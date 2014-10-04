open Core.Std
open Async.Std

(*
 * How state is transfered to followers:
 *
 * Heartbeats
 *   - A heartbeat (empty append entries) is sent only if
 *     match_idx equals [pred next_idx].  This means that,
 *     at the time of performing the heartbeat the node is
 *     completely caught up.
 *
 *   - The heartbeat message will kick off replication of the log
 *     to other nodes if the node does not exist in the next_idx or
 *     match_idx maps.  This means that no other peice of code has
 *     attempted to initiate an append entries on that node.
 *
 * Append_entry
 *   - If the next_idx equals the new log index for the entry, replicate it.
 *     Otherwise it is assumed that somebody is already trying to bring the
 *     node up to date.
 *)

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Store.node
                 and  type elt    = Log.elt) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Store)(Transport).t
  type errors = Scow_server_state.Make(Statem)(Log)(Store)(Transport).errors

  module Msg = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Store)(Transport)

  let load_term store =
    Store.load_term store
    >>=? fun current_term_opt ->
    current_term_opt
    |> Option.value ~default:(Scow_term.zero ())
    |> Result.return
    |> Deferred.return

  let replication_caught_up ~match_idx ~high =
    Scow_log_index.is_equal match_idx high

  let ongoing_replication ~match_idx ~next_idx =
    not (Scow_log_index.is_equal
           (Scow_log_index.pred next_idx)
           match_idx)

  let should_replicate ~match_idx ~next_idx ~high =
    not (replication_caught_up ~match_idx ~high)
    && not (ongoing_replication ~match_idx ~next_idx)

  let count_entries idx entries =
    List.fold_left
      ~f:(fun acc _ -> Scow_log_index.succ acc)
      ~init:idx
      entries

  let send_entries self state node start_idx entries =
    Log.get_term (State.log state) (Scow_log_index.pred start_idx)
    >>=? fun prev_term ->
    load_term (State.store state)
    >>=? fun current_term ->
    let next_idx = count_entries start_idx entries in
    let append_entries =
      Scow_rpc.Append_entries.(
        { term           = current_term
        ; prev_log_index = Scow_log_index.pred start_idx
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
    Gen_server.send self (Msg.Op (Msg.Append_entries_resp (node, next_idx, result)))

  let get_next_log_entries log next_idx =
    Log.get_log_index_range log
    >>=? function
      | (_low, high) when Scow_log_index.compare high next_idx > 0 -> begin
        Log.get_entry log next_idx
        >>=? fun term_and_entry ->
        Deferred.return (Ok [term_and_entry])
      end
      | (_log, _high) ->
        Deferred.return (Ok [])

  let do_replicate self next_idx node state =
    get_next_log_entries (State.log state) next_idx
    >>=? fun entries ->
    send_entries self state node next_idx entries

  let replicate_to_node self next_idx node state =
    do_replicate self next_idx node state
    >>| function
      | Ok _ ->
        ()
      | Error `Invalid_log
      | Error `Closed
      | Error `Invalid_term_store ->
        ()
      | Error (`Not_found idx) ->
        failwith "nyi"

  let maybe_replicate_to_node self high state node =
    let next_idx =
      Option.value
        (State.next_idx node state)
        ~default:high
    in
    let match_idx =
      Option.value
        (State.match_idx node state)
        ~default:(Scow_log_index.zero ())
    in
    if should_replicate ~match_idx ~next_idx ~high then
      ignore (replicate_to_node self next_idx node state);
    Deferred.return state

  let maybe_replicate self state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    Deferred.List.fold
      ~f:(maybe_replicate_to_node self high)
      ~init:state
      (State.nodes state)
    >>= fun state ->
    let state =
      state
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    Deferred.return (Ok state)

  let append_entry_in_future self node append_entries ctx state =
    let state =
      state
      |> State.set_leader (Some node)
      |> State.set_state_follower
      |> State.set_heartbeat_timeout self
    in
    State.notify state Scow_notify.Event.(State_change (Leader, Follower))
    >>= fun () ->
    let module Ae = Scow_rpc.Append_entries in
    Store.store_term (State.store state) append_entries.Ae.term
    >>=? fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    State.handler
      state
      self
      state
      (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    load_term (State.store state)
    >>=? fun current_term ->
    if Scow_term.compare current_term append_entries.Ae.term < 0 then
      append_entry_in_future self node append_entries ctx state
    else begin
      Transport.resp_append_entries
        (State.transport state)
        ctx
        ~term:current_term
        ~success:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let request_vote_in_future self node request_vote ctx state =
    let state =
      state
      |> State.set_leader (Some node)
      |> State.set_state_follower
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    State.notify state Scow_notify.Event.(State_change (Leader, Follower))
    >>= fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    let module Rv = Scow_rpc.Request_vote in
    Store.store_term (State.store state) request_vote.Rv.term
    >>=? fun () ->
    State.handler
      state
      self
      state
      (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    load_term (State.store state)
    >>=? fun current_term ->
    if Scow_term.compare current_term request_vote.Rv.term < 0 then
      request_vote_in_future self node request_vote ctx state
    else begin
      Transport.resp_request_vote
        (State.transport state)
        ctx
        ~term:current_term
        ~granted:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let add_append_entry log_index ret state =
    let ae = State.Append_entry.({log_index; ret}) in
    State.add_append_entry ae state

  let handle_append_entry self state ret entry =
    load_term (State.store state)
    >>=? fun current_term ->
    Log.append
      (State.log state)
      [(current_term, entry)]
    >>= function
      | Ok log_index -> begin
        let state = add_append_entry log_index ret state in
        State.notify state (Scow_notify.Event.Append_entry (log_index, 1))
        >>= fun () ->
        maybe_replicate self state
      end
      | Error `Invalid_log -> begin
        Ivar.fill ret (Error `Invalid_log);
        Deferred.return (Ok state)
      end
      | Error `Append_failed -> begin
        Ivar.fill ret (Error `Append_failed);
        Deferred.return (Ok state)
      end

  let handle_heartbeat = maybe_replicate

  let cancel_pending_append_entries state =
    let (entries, state) = State.remove_all_append_entries state in
    List.iter
      ~f:State.Append_entry.(fun ae ->
        Ivar.fill ae.ret (Error `Not_master))
      entries;
    state

  let reply_to_pending_append_entry res log_index state =
    match State.remove_append_entry log_index state with
      | (Some ae, state) -> begin
        let ret = ae.State.Append_entry.ret in
        Ivar.fill ret (Ok res);
        state
      end
      | (None, state) ->
        state

  let rec apply_state_machine state =
    let commit_idx = State.commit_idx state in
    match State.last_applied state with
      | last_applied when Scow_log_index.compare last_applied commit_idx < 0 -> begin
        let to_apply = Scow_log_index.succ last_applied in
        Log.get_entry (State.log state) to_apply
        >>= function
          | Ok (_term, elt) -> begin
            Statem.apply (State.statem state) elt
            >>= fun res ->
            let state =
              state
              |> State.set_last_applied to_apply
              |> reply_to_pending_append_entry res to_apply
            in
            apply_state_machine state
          end
          | Error _ ->
            failwith "nyi"
      end
      | _ ->
        Deferred.return state

  let maybe_notify_commit_idx state highest_match_idx =
    if not (Scow_log_index.is_equal (State.commit_idx state) highest_match_idx) then
      State.notify state (Scow_notify.Event.Commit_idx highest_match_idx)
    else
      Deferred.unit

  let update_commit_idx state =
    let highest_match_idx = State.compute_highest_match_idx state in
    load_term (State.store state)
    >>= fun current_term_res ->
    let current_term =
      current_term_res
      |> Result.ok
      |> Option.value ~default:(Scow_term.zero ())
    in
    Log.get_term (State.log state) highest_match_idx
    >>= function
      | Ok term when Scow_term.is_equal current_term term -> begin
        maybe_notify_commit_idx state highest_match_idx
        >>= fun () ->
        Deferred.return (State.set_commit_idx highest_match_idx state)
      end
      | Ok _ ->
        Deferred.return state
      | Error _ ->
        failwith "nyi"

  let append_entries_resp_in_future self term state node =
    let state =
      state
      |> State.set_leader (Some node)
      |> State.set_state_follower
      |> State.cancel_election_timeout
      |> State.set_heartbeat_timeout self
      |> cancel_pending_append_entries
    in
    State.notify state Scow_notify.Event.(State_change (Leader, Follower))
    >>= fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    Store.store_term (State.store state) term
    >>=? fun () ->
    Deferred.return (Ok state)

  let append_entries_resp_success self state node new_next_idx =
    let state =
      state
      |> State.set_next_idx node new_next_idx
      |> State.set_match_idx node (Scow_log_index.pred new_next_idx)
    in
    update_commit_idx state
    >>= fun state ->
    apply_state_machine state
    >>= fun state ->
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    maybe_replicate_to_node self high state node
    >>= fun state ->
    Deferred.return (Ok state)

  let append_entries_resp_failed self state node =
    let curr_next_idx =
      Option.value
        ~default:(Scow_log_index.zero ())
        (State.next_idx node state)
    in
    let next_idx = Scow_log_index.pred curr_next_idx in
    let state = State.set_next_idx node next_idx state in
    ignore (replicate_to_node self next_idx node state);
    Deferred.return (Ok state)

  let append_entries_resp_error self state node =
    let sleep_and_retry () =
      after (sec 0.1)
      >>| fun () ->
      let next_idx =
        Option.value
          ~default:(Scow_log_index.zero ())
          (State.next_idx node state)
      in
      ignore (replicate_to_node self next_idx node state)
    in
    (* Background this so we don't hold up event processing *)
    ignore (sleep_and_retry ());
    Deferred.return (Ok state)

  let do_handle_append_entries_resp self state node new_next_idx current_term = function
    | Ok (term, _) when Scow_term.compare current_term term < 0 ->
      (* The node is ahead of us, become a follower and update state *)
      append_entries_resp_in_future self term state node
    | Ok (term, true) ->
      append_entries_resp_success self state node new_next_idx
    | Ok (term, false) ->
      append_entries_resp_failed self state node
    | Error `Transport_error ->
      append_entries_resp_error self state node

  let handle_append_entries_resp self state node new_next_idx resp =
    load_term (State.store state)
    >>=? fun current_term ->
    do_handle_append_entries_resp self state node new_next_idx current_term resp

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_rpc_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      handle_rpc_request_vote self state (node, request_vote, ctx)
    | Msg.Append_entry (ret, entry) ->
      handle_append_entry self state ret entry
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      handle_heartbeat self state
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp (node, next_idx, resp) ->
      handle_append_entries_resp self state node next_idx resp
end
