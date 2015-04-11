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

  let do_replicate_log self next_idx node state =
    get_next_log_entries (State.log state) next_idx
    >>=? fun entries ->
    send_entries self state node next_idx entries

  let replicate_log self next_idx node state =
    do_replicate_log self next_idx node state
    >>= function
      | Ok _ ->
	Deferred.unit
      | Error `Invalid_term_store
      | Error `Invalid_log
      | Error `Closed ->
	Deferred.unit
      | Error (`Not_found idx) -> begin
        failwith "nyi"
      end

  let get_next_idx latest_log_idx node state =
    Option.value
      ~default:latest_log_idx
      (State.next_idx node state)

  let maybe_replicate_new_log self latest_log_idx node state =
    let next_idx = get_next_idx latest_log_idx node state in
    if Scow_log_index.is_equal latest_log_idx next_idx then
      (* TODO: Propagate errors properly *)
      ignore (replicate_log self next_idx node state)
    else
      ()

  let replicate_to_caught_up_nodes self state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    List.iter
      ~f:(Fn.flip (maybe_replicate_new_log self high) state)
      (State.nodes state);
    Deferred.return (Ok ())

  let replicate_next_log_entries self node state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    maybe_replicate_new_log self high node state;
    Deferred.return (Ok state)

  let maybe_heartbeat_node self latest_log_idx node state =
    let next_idx = get_next_idx latest_log_idx node state in
    if Scow_log_index.(<) latest_log_idx next_idx then
      ignore (send_entries self state node latest_log_idx [])
    else
      ()

  let heartbeat_to_caught_up_nodes self state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    List.iter
      ~f:(Fn.flip (maybe_heartbeat_node self high) state)
      (State.nodes state);
    Deferred.return (Ok ())

  let remote_node_in_future node remote_term state =
    Store.load_term (State.store state)
    >>=? fun current_term ->
    if Scow_term.(<) current_term remote_term then
      Deferred.return (Ok ())
    else
      Deferred.return (Error (`Remote_node_in_past node))

  let do_handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    remote_node_in_future node append_entries.Ae.term state
    >>=? fun () ->
    Store.store_term (State.store state) append_entries.Ae.term
    >>=? fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    let state =
      state
      |> State.set_leader (Some node)
      |> State.set_state_follower
      |> State.set_heartbeat_timeout self
    in
    State.handler
      state
      self
      state
      (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	(*
	 * This little bit is because the type system doesn't handle
	 * the variants as well as we'd hope
	 *)
	Deferred.return err

  let respond_rpc_append_entries_fail state (node, append_entries, ctx) =
    Store.load_term (State.store state)
    >>= function
      | Ok current_term -> begin
	Transport.resp_append_entries
	  (State.transport state)
	  ctx
	  ~term:current_term
	  ~success:false
	>>= fun _ ->
	Deferred.return (Ok state)
      end
      | Error `Invalid_term_store ->
	Deferred.return (Error `Invalid_term_store)

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
      | last_applied when Scow_log_index.(<) last_applied commit_idx -> begin
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
    >>= fun state ->
    apply_state_machine state
    >>= fun state ->
    replicate_next_log_entries self node state
    >>=? fun state ->
    Deferred.return (Ok state)

  let append_entries_failed self node next_idx state =
    let next_idx = Scow_log_index.pred next_idx in
    let state    = State.set_next_idx node next_idx state in
    replicate_next_log_entries self node state
    >>=? fun state ->
    Deferred.return (Ok state)

  let retry_replicate self next_idx node state =
    after (sec 0.1)
    >>= fun () ->
    replicate_next_log_entries self node state
    >>=? fun state ->
    Deferred.return (Ok state)

  let do_handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    remote_node_in_future node request_vote.Rv.term state
    >>=? fun () ->
    Store.store_term (State.store state) request_vote.Rv.term
    >>=? fun () ->
    Store.store_vote (State.store state) None
    >>=? fun () ->
    let state =
      state
      |> State.set_leader (Some node)
      |> State.set_state_follower
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    State.handler
      state
      self
      state
      (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	Deferred.return err

  let respond_rpc_request_vote_fail state (node, request_vote, ctx) =
    Store.load_term (State.store state)
    >>= function
      | Ok current_term -> begin
	Transport.resp_request_vote
          (State.transport state)
          ctx
          ~term:current_term
          ~granted:false
	>>= fun _ ->
	Deferred.return (Ok state)
      end
      | Error `Invalid_term_store ->
	Deferred.return (Error `Invalid_term_store)

  let handle_rpc_append_entries self state append_entries_data =
    do_handle_rpc_append_entries self state append_entries_data
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Remote_node_in_past _ ->
	respond_rpc_append_entries_fail state append_entries_data
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	Deferred.return err

  let handle_rpc_request_vote self state request_vote_data =
    do_handle_rpc_request_vote self state request_vote_data
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Remote_node_in_past _ ->
	respond_rpc_request_vote_fail state request_vote_data
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	Deferred.return err

  let do_handle_append_entry self state ret entry =
    Store.load_term (State.store state)
    >>=? fun current_term ->
    Log.append (State.log state) [(current_term, entry)]
    >>=? fun log_index ->
    let ae =
      State.Append_entry.(
        { log_index = log_index
        ; ret       = ret
        })
    in
    let state = State.add_append_entry ae state in
    State.notify state (Scow_notify.Event.Append_entry (log_index, 1))
    >>= fun () ->
    replicate_to_caught_up_nodes self state
    >>=? fun () ->
    Deferred.return (Ok state)

  let handle_append_entry self state ret entry =
    do_handle_append_entry self state ret entry
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Append_failed as err -> begin
        Ivar.fill ret (err);
        Deferred.return (Ok state)
      end
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	Deferred.return err

  let do_handle_heartbeat self state =
    heartbeat_to_caught_up_nodes self state
    >>=? fun () ->
    let state = State.set_heartbeat_timeout self state in
    Deferred.return (Ok state)

  let handle_heartbeat self state =
    do_handle_heartbeat self state
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Invalid_log
      | Error `Invalid_term_store
      | Error `Invalid_vote_store as err ->
	Deferred.return err

  let do_handle_append_entries_resp self state node next_next_idx resp =
    get_node_next_idx node state
    >>=? fun next_idx ->
    Store.load_term (State.store state)
    >>=? fun current_term ->
    match resp with
      | Ok (term, _) when Scow_term.(<) current_term term ->
	remote_node_ahead self node term state
      | Ok (term, true) ->
	append_entries_succeeded self node next_next_idx state
      | Ok (term, false) ->
	append_entries_failed self node next_idx state
      | Error `Transport_error ->
	retry_replicate self next_idx node state

  let handle_append_entries_resp self state node next_next_idx resp =
    do_handle_append_entries_resp self state node next_next_idx resp
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error `Invalid_log
      | Error `Invalid_vote_store
      | Error `Invalid_term_store as err ->
	Deferred.return err

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
