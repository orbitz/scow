open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Vote_store.node
                 and  type elt    = Log.elt) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let send_append_entries self state node log_index term entries =
    Log.get_term
      (State.log state)
      (Scow_log_index.pred log_index)
    >>=? fun prev_term ->
    let append_entries =
      Scow_rpc.Append_entries.(
        { term           = term
        ; prev_log_index = Scow_log_index.pred log_index
        ; prev_log_term  = prev_term
        ; leader_commit  = State.commit_idx state
        ; entries        = entries
        })
    in
    Transport.append_entries
      (State.transport state)
      node
      append_entries
    >>= fun result ->
    let next_idx = Scow_log_index.succ log_index in
    Gen_server.send self (Msg.Op (Msg.Append_entries_resp (node, next_idx, result)))

  let replicate_log_to_node self state node log_index =
    Log.get_entry
      (State.log state)
      log_index
    >>=? fun (term, entry) ->
    send_append_entries self state node log_index term [entry]

  let replicate_log self state log_index =
    List.iter
      ~f:(fun node ->
        match State.next_idx node state with
          | Some next_idx when Scow_log_index.is_equal log_index next_idx ->
            ignore (replicate_log_to_node self state node log_index)
          | Some _
          | None ->
            ())
      (State.nodes state)

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare (State.current_term state) append_entries.Ae.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.set_heartbeat_timeout self
      in
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
    else begin
      Transport.resp_append_entries
        (State.transport state)
        ctx
        ~term:(State.current_term state)
        ~success:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    if Scow_term.compare (State.current_term state) request_vote.Rv.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.cancel_election_timeout
        |> State.cancel_heartbeat_timeout
      in
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))
    else begin
      Transport.resp_request_vote
        (State.transport state)
        ctx
        ~term:(State.current_term state)
        ~granted:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_append_entry self state ret entry =
    Log.append
      (State.log state)
      (State.current_term state)
      [entry]
    >>= function
      | Ok log_index ->
        let ae =
          State.Append_entry.(
            { log_index = log_index
            ; op        = entry
            ; ret       = ret
            })
        in
        let state = State.add_append_entry ae state in
        ignore (replicate_log self state log_index);
        Deferred.return (Ok state)
      | Error `Invalid_log -> begin
        Ivar.fill ret (Error `Invalid_log);
        Deferred.return (Ok state)
      end
      | Error `Append_failed -> begin
        Ivar.fill ret (Error `Append_failed);
        Deferred.return (Ok state)
      end

  let handle_timeout self state =
    failwith "nyi"

  let cancel_pending_append_entries state =
    let (entries, state) = State.remove_all_append_entries state in
    List.iter
      ~f:State.Append_entry.(fun ae -> Ivar.fill ae.ret (Error `Not_master))
      entries;
    state

  let reply_any_pending_append_entries state =
    let (entries, state) =
      State.remove_append_entries (State.commit_idx state) state
    in
    let entries =
      List.sort
        ~cmp:(fun l r ->
          State.Append_entry.(Scow_log_index.compare l.log_index r.log_index))
        entries
    in
    Deferred.List.iter
      ~f:(fun ae ->
        let op  = ae.State.Append_entry.op in
        let ret = ae.State.Append_entry.ret in
        Statem.apply (State.statem state) op
        >>= fun result ->
        Ivar.fill ret (Ok result);
        Deferred.unit)
      entries
    >>= fun _ ->
    Deferred.return state

  let do_handle_append_entries_resp self state node next_idx curr_next_idx = function
    | Ok (term, _) when Scow_term.compare (State.current_term state) term < 0 -> begin
      (* This node is ahead of us *)
      let state =
        state
        |> State.set_state_follower
        |> State.cancel_heartbeat_timeout
        |> State.cancel_election_timeout
        |> State.set_heartbeat_timeout self
        |> State.clear_next_idx
        |> State.clear_match_idx
        |> cancel_pending_append_entries
      in
      Deferred.return (Ok state)
    end
    | Ok (term, true) -> begin
      let state =
        state
        |> State.set_next_idx node next_idx
        |> State.set_match_idx node (Scow_log_index.pred next_idx)
        |> State.update_commit_index
      in
      reply_any_pending_append_entries state
      >>= fun state ->
      ignore (replicate_log_to_node self state node next_idx);
      Deferred.return (Ok state)
    end
    | Ok (term, false) -> begin
      let next_idx = Scow_log_index.pred curr_next_idx in
      let state = State.set_next_idx node next_idx state in
      ignore (replicate_log_to_node self state node next_idx);
      Deferred.return (Ok state)
    end
    | Error `Transport_error -> begin
      ignore (replicate_log_to_node self state node curr_next_idx);
      Deferred.return (Ok state)
    end

  let handle_append_entries_resp self state node next_id resp =
    let get_curr_next_idx () =
      match State.next_idx node state with
        | Some next_idx ->
          Deferred.return (Ok next_idx)
        | None -> begin
          Log.get_log_index_range (State.log state)
          >>=? fun (_low, high) ->
          (* Consider pushing back to state *)
          Deferred.return (Ok high)
        end
    in
    get_curr_next_idx ()
    >>=? fun curr_next_idx ->
    do_handle_append_entries_resp self state node next_id curr_next_idx resp

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, request_vote, ctx))
    | Msg.Append_entry (ret, entry) ->
      ignore_error (handle_append_entry self state ret entry)
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      ignore_error (handle_timeout self state)
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp (node, next_idx, resp) ->
      ignore_error (handle_append_entries_resp self state node next_idx resp)
end

