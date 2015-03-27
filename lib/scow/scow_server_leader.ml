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

  let replication_caught_up ~next_idx ~match_idx =
    Scow_log_index.is_equal (Scow_log_index.pred next_idx) match_idx

  let count_entries idx entries =
    List.fold_left
      ~f:(fun acc _ -> Scow_log_index.succ acc)
      ~init:idx
      entries

  let send_entries self state node start_idx entries =
    Log.get_term (State.log state) (Scow_log_index.pred start_idx)
    >>=? fun prev_term ->
    Store.load_term (State.store state)
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

  let do_replicate_log self node state =
    let next_idx = Option.value_exn (State.next_idx node state) in
    get_next_log_entries (State.log state) next_idx
    >>=? fun entries ->
    send_entries self state node next_idx entries

  let replicate_log self node state =
    do_replicate_log self node state
    >>| function
      | Ok _ ->
        ()
      | Error `Invalid_term_store
      | Error `Invalid_log
      | Error `Closed ->
        ()
      | Error (`Not_found idx) -> begin
        failwith "nyi"
      end

  let maybe_replicate_log self state node =
    (*
     * If the (pred next_idx) equals match_idx then
     * it means we have sent the entire log to the report and
     * we need to send a heartbeat.  Also, if next_idx or match_idx
     * are not present.
     *
     * Otherwise, we assume that something is already replicating values
     * and we do not need to do anything.
     *)
    match (State.next_idx node state, State.match_idx node state) with
      | (Some next_idx, Some match_idx) when replication_caught_up ~next_idx ~match_idx -> begin
        ignore (replicate_log self node state);
        Deferred.return state
      end
      | (Some _, Some _) ->
        Deferred.return state
      | (_, _) -> begin
        Log.get_log_index_range (State.log state)
        >>= function
          | Ok (_low, high) -> begin
            let next_idx = Scow_log_index.succ high in
            let state =
              state
              |> State.set_next_idx node next_idx
              |> State.set_match_idx node (Scow_log_index.zero ())
            in
            ignore (replicate_log self node state);
            Deferred.return state
          end
          | Error `Invalid_log ->
            failwith "nyi"
      end

  let maybe_replicate_next_log self node state =
    let next_idx =
      Option.value
        (State.next_idx node state)
        ~default:(Scow_log_index.zero ())
    in
    Log.get_log_index_range (State.log state)
    >>= function
      | Ok (_low, high) when Scow_log_index.compare next_idx high < 0 ->
        maybe_replicate_log self state node
      | Ok (_, _) ->
        Deferred.return state
      | Error _ ->
        failwith "nyi"

  let maybe_send_to_all_nodes self state =
    Deferred.List.fold
      ~f:(maybe_replicate_log self)
      ~init:state
      (State.nodes state)
    >>= fun state ->
    let state =
      state
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    Deferred.return (Ok state)

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    Store.load_term (State.store state)
    >>=? fun current_term ->
    if Scow_term.compare current_term append_entries.Ae.term < 0 then begin
      let state =
        state
        |> State.set_leader (Some node)
        |> State.set_state_follower
        |> State.set_heartbeat_timeout self
      in
      Store.store_term (State.store state) append_entries.Ae.term
      >>=? fun () ->
      State.notify state Scow_notify.Event.(State_change (Leader, Follower))
      >>= fun () ->
      Store.store_vote (State.store state) None
      >>=? fun () ->
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
    end
    else begin
      Transport.resp_append_entries
        (State.transport state)
        ctx
        ~term:current_term
        ~success:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    Store.load_term (State.store state)
    >>=? fun current_term ->
    if Scow_term.compare current_term request_vote.Rv.term < 0 then begin
      let state =
        state
        |> State.set_leader (Some node)
        |> State.set_state_follower
        |> State.cancel_election_timeout
        |> State.cancel_heartbeat_timeout
        |> State.set_heartbeat_timeout self
      in
      Store.store_term (State.store state) request_vote.Rv.term
      >>=? fun () ->
      State.notify state Scow_notify.Event.(State_change (Leader, Follower))
      >>= fun () ->
      Store.store_vote (State.store state) None
      >>=? fun () ->
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))
    end
    else begin
      Transport.resp_request_vote
        (State.transport state)
        ctx
        ~term:current_term
        ~granted:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_append_entry self state ret entry =
    Store.load_term (State.store state)
    >>=? fun current_term ->
    Log.append (State.log state) [(current_term, entry)]
    >>= function
      | Ok log_index -> begin
        let ae =
          State.Append_entry.(
            { log_index = log_index
            ; ret       = ret
            })
        in
        let state = State.add_append_entry ae state in
        State.notify state (Scow_notify.Event.Append_entry (log_index, 1))
        >>= fun () ->
        maybe_send_to_all_nodes self state
      end
      | Error `Invalid_log -> begin
        Ivar.fill ret (Error `Invalid_log);
        Deferred.return (Ok state)
      end
      | Error `Append_failed -> begin
        Ivar.fill ret (Error `Append_failed);
        Deferred.return (Ok state)
      end

  let handle_heartbeat = maybe_send_to_all_nodes

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
    Store.load_term (State.store state)
    >>= function
      | Ok current_term -> begin
	let highest_match_idx = State.compute_highest_match_idx state in
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
      end
      | Error _ ->
	failwith "nyi"

  let get_node_next_idx node state =
    match State.next_idx node state with
      | Some next_idx ->
	Deferred.return (Ok next_idx)
      | None -> begin
	Log.get_log_index_range (State.log state)
	>>=? fun (_low, high) ->
	Deferred.return (Ok high)
      end

  let remote_node_ahead self node term state =
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
    Store.store_term (State.store state) term
    >>=? fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    Deferred.return (Ok state)

  let append_entries_succeeded self node next_next_idx state =
    let state =
      state
      |> State.set_next_idx node next_next_idx
      |> State.set_match_idx node (Scow_log_index.pred next_next_idx)
    in
    update_commit_idx state
    >>= apply_state_machine
    >>= maybe_replicate_next_log self node
    >>= fun state ->
    Deferred.return (Ok state)

  let append_entries_failed self node next_idx state =
    let next_idx = Scow_log_index.pred next_idx in
    let state = State.set_next_idx node next_idx state in
    ignore (replicate_log self node state);
    Deferred.return (Ok state)

  let retry_replicate self node state =
    after (sec 0.1)
    >>= fun () ->
    ignore (replicate_log self node state);
    Deferred.return (Ok state)

  let do_handle_append_entries_resp self state node next_next_idx resp =
    get_node_next_idx node state
    >>=? fun next_idx ->
    Store.load_term (State.store state)
    >>=? fun current_term ->
    match resp with
      | Ok (term, _) when Scow_term.compare current_term term < 0 -> begin
	remote_node_ahead self node term state
      end
      | Ok (term, true) -> begin
	append_entries_succeeded self node next_next_idx state
      end
      | Ok (term, false) -> begin
	append_entries_failed self node next_idx state
      end
      | Error `Transport_error -> begin
	retry_replicate self node state
      end

  (* let do_handle_append_entries_resp self state node next_idx curr_next_idx = function *)
  (*   | Ok (term, _) when Scow_term.compare (State.current_term state) term < 0 -> begin *)
  (*     (\* This node is ahead of us *\) *)
  (*     let state = *)
  (*       state *)
  (*       |> State.set_leader (Some node) *)
  (*       |> State.set_current_term term *)
  (*       |> State.set_state_follower *)
  (*       |> State.cancel_election_timeout *)
  (*       |> State.set_heartbeat_timeout self *)
  (*       |> cancel_pending_append_entries *)
  (*     in *)
  (*     State.notify state Scow_notify.Event.(State_change (Leader, Follower)) *)
  (*     >>= fun () -> *)
  (*     Store.store_vote (State.store state) None *)
  (*     >>=? fun () -> *)
  (*     Deferred.return (Ok state) *)
  (*   end *)
  (*   | Ok (term, true) -> begin *)
  (*     let state = *)
  (*       state *)
  (*       |> State.set_next_idx node next_idx *)
  (*       |> State.set_match_idx node (Scow_log_index.pred next_idx) *)
  (*     in *)
  (*     update_commit_idx state *)
  (*     >>= fun state -> *)
  (*     apply_state_machine state *)
  (*     >>= fun state -> *)
  (*     maybe_replicate_next_log self state node *)
  (*     >>= fun state -> *)
  (*     Deferred.return (Ok state) *)
  (*   end *)
  (*   | Ok (term, false) -> begin *)
  (*     let next_idx = Scow_log_index.pred curr_next_idx in *)
  (*     let state = State.set_next_idx node next_idx state in *)
  (*     ignore (replicate_log self state node); *)
  (*     Deferred.return (Ok state) *)
  (*   end *)
  (*   | Error `Transport_error -> begin *)
  (*     after (sec 0.1) *)
  (*     >>= fun () -> *)
  (*     ignore (replicate_log self state node); *)
  (*     Deferred.return (Ok state) *)
  (*   end *)

  let handle_append_entries_resp self state node next_next_idx resp =
    do_handle_append_entries_resp self state node next_next_idx resp

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) -> begin
      handle_rpc_append_entries self state (node, append_entries, ctx)
    end
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) -> begin
      handle_rpc_request_vote self state (node, request_vote, ctx)
    end
    | Msg.Append_entry (ret, entry) -> begin
      handle_append_entry self state ret entry
    end
    | Msg.Election_timeout
    | Msg.Heartbeat -> begin
      handle_heartbeat self state
    end
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp (node, next_idx, resp) -> begin
      handle_append_entries_resp self state node next_idx resp
    end
end

