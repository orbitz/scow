open Core.Std
open Async.Std

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

  module Msg   = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Store)(Transport)

  let term_or_zero = function
    | Some term -> term
    | None      -> Scow_term.zero ()

  (*
   *************************************************************
   * Requesting votes
   *************************************************************
   *)
  let request_votes self vote_request transport nodes =
    let send_request_vote node =
      Transport.request_vote
        transport
        node
        vote_request
      >>=? fun (term, success) ->
      Gen_server.send self (Msg.Op (Msg.Received_vote (node, term, success)))
    in
    List.iter
      ~f:(fun node -> ignore (send_request_vote node))
      nodes

  let send_vote_requests self term state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    Log.get_term (State.log state) high
    >>=? fun last_term ->
    let vote_request =
      Scow_rpc.({ Request_vote.term           = term
                ;              last_log_index = high
                ;              last_log_term  = last_term
                })
    in
    request_votes
      self
      vote_request
      (State.transport state)
      (State.nodes state);
    Deferred.return (Ok ())

  let vote_for_self state =
    Store.store_vote
      (State.store state)
      (Some (State.me state))

  let save_term term state =
    Store.store_term
      (State.store state)
      term

  let term_is_valid term state =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term = term_or_zero current_term_opt in
    let term_in_past =
      Scow_term.compare term current_term < 0
    in
    if term_in_past then
      Deferred.return (Error (`Term_less_than_current (term, current_term)))
    else
      Deferred.return (Ok ())

  let maybe_update_term vote_term state =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term = term_or_zero current_term_opt in
    if Scow_term.is_equal vote_term current_term then
      Deferred.return (Ok state)
    else begin
      Store.store_vote (State.store state) None
      >>=? fun () ->
      save_term vote_term state
      >>=? fun () ->
      state
	|> State.set_leader None
	|> Result.return
	|> Deferred.return
    end

  let update_leader node state =
    let state = State.set_leader (Some node) state in
    Deferred.return (Ok state)

  let reset_heartbeat self state =
    state
    |> State.cancel_election_timeout
    |> State.cancel_heartbeat_timeout
    |> State.set_heartbeat_timeout self
    |> Result.return
    |> Deferred.return

  let get_latest_commit_idx state =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    Deferred.return (Ok high)

  let candidate_commit_idx_up_to_date latest_commit_idx request_vote state =
    let module Rv = Scow_rpc.Request_vote in
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term = term_or_zero current_term_opt in
    Log.get_term (State.log state) latest_commit_idx
    >>=? fun last_log_term ->
    let i_am_in_future =
      Scow_term.compare current_term request_vote.Rv.term > 0 ||
	Scow_term.compare last_log_term request_vote.Rv.last_log_term > 0 ||
	Scow_log_index.compare latest_commit_idx request_vote.Rv.last_log_index > 0
    in
    if i_am_in_future then
      Deferred.return (Ok ())
    else
      Deferred.return (Error `Candidate_commit_not_up_to_date)

  let can_give_vote node state =
    Store.load_vote (State.store state)
    >>=? function
      | Some voted_for when Transport.Node.compare voted_for node = 0 ->
	Deferred.return (Ok ())
      | None ->
	Deferred.return (Ok ())
      | Some voted_for ->
	Deferred.return (Error (`Already_voted_for voted_for))

  let give_vote ctx node state =
    Store.store_vote (State.store state) (Some node)
    >>=? fun () ->
    Store.load_term (State.store state)
    >>=? function
      | Some current_term ->
	Transport.resp_request_vote
	  (State.transport state)
	  ctx
	  ~term:current_term
	  ~granted:true
      | None ->
	Deferred.return (Error `No_term_stored)

  let previous_index_matches_term state append_entries =
    let module Ae      = Scow_rpc.Append_entries in
    let prev_log_index = append_entries.Ae.prev_log_index in
    let prev_log_term  = append_entries.Ae.prev_log_term in
    Log.get_term (State.log state) prev_log_index
    >>=? function
      | term when Scow_term.is_equal term prev_log_term ->
        Deferred.return (Ok ())
      | term -> begin
        Deferred.return (Error `Prev_term_does_not_match)
      end

  let rec check_entries_match log log_index = function
    | [] ->
      Deferred.return (Ok [])
    | (elt_term, elt)::elts -> begin
      let entry_equal term log_elt =
        Scow_term.is_equal elt_term term && Log.is_elt_equal log_elt elt
      in
      Log.get_entry log log_index
      >>= function
        | Ok (term, log_elt) when entry_equal term log_elt ->
          check_entries_match log (Scow_log_index.succ log_index) elts
        | Ok (_term, _log_elt) -> begin
          Log.delete_from_log_index log log_index
          >>=? fun () ->
          Deferred.return (Ok ((elt_term, elt)::elts))
        end
        | Error (`Not_found _) ->
          (* We assume there are no gaps in the log *)
          Deferred.return (Ok ((elt_term, elt)::elts))
        | Error `Invalid_log ->
          Deferred.return (Error `Invalid_log)
    end

  let delete_if_not_equal state append_entries =
    let module Ae = Scow_rpc.Append_entries in
    let log_index = Scow_log_index.succ (append_entries.Ae.prev_log_index) in
    check_entries_match (State.log state) log_index append_entries.Ae.entries

  let can_apply_log append_entries state =
    (*
     * If this function returns successfully it means that the log is in
     * a valid state for calling [append]
     *)
    let module Ae = Scow_rpc.Append_entries in
    let term = append_entries.Ae.term in
    term_is_valid term state
    >>=? fun () ->
    previous_index_matches_term state append_entries
    >>=? fun () ->
    delete_if_not_equal state append_entries

  let do_append_entries ctx entries state =
    Log.append (State.log state) entries
    >>=? fun log_index ->
    State.notify
      state
      (Scow_notify.Event.Append_entry (log_index, List.length entries))
    >>= fun () ->
    Deferred.return (Ok state)

  let apply_state_machine leader_commit state =
    (* Log index less than *)
    let lile l1 l2 = Scow_log_index.compare l1 l2 < 0 in
    let rec apply_state_machine' state = function
      | leader_commit when lile (State.last_applied state) leader_commit -> begin
	let to_apply = Scow_log_index.succ (State.last_applied state) in
	Log.get_entry (State.log state) to_apply
	>>= function
          | Ok (_term, elt) -> begin
	    Statem.apply (State.statem state) elt
	    >>= fun _ ->
	    let state = State.set_last_applied to_apply state in
	    apply_state_machine' state leader_commit
          end
          | Error (`Not_found _) ->
	    Deferred.return (Ok state)
          | Error err ->
	    Deferred.return (Error err)
      end
      | _ ->
	Deferred.return (Ok state)
    in
    apply_state_machine' state leader_commit

  let reply_append_entries_success ctx term state =
    Transport.resp_append_entries
      (State.transport state)
      ctx
      ~term:term
      ~success:true

  let do_handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    let term = append_entries.Ae.term in
    term_is_valid term state
    >>=? fun () ->
    maybe_update_term term state
    >>=? update_leader node
    >>=? reset_heartbeat self
    >>=? can_apply_log append_entries
    >>=? fun entries ->
    do_append_entries ctx entries state
    >>=? apply_state_machine append_entries.Ae.leader_commit
    >>=? reply_append_entries_success ctx term
    >>=? fun () ->
    Deferred.return (Ok state)

  let handle_rpc_append_entries self state append_entries_data =
    do_handle_rpc_append_entries self state append_entries_data
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error _ ->
	failwith "nyi"

  let do_handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    let vote_term = request_vote.Rv.term in
    term_is_valid vote_term state
    >>=? fun () ->
    maybe_update_term vote_term state
    >>=? reset_heartbeat self
    >>=? get_latest_commit_idx
    >>=? fun latest_commit_idx ->
    candidate_commit_idx_up_to_date latest_commit_idx request_vote state
    >>=? fun () ->
    can_give_vote node state
    >>=? fun () ->
    give_vote ctx node state
    >>=? fun () ->
    Deferred.return (Ok state)

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    do_handle_rpc_request_vote self state (node, request_vote, ctx)
    >>= function
      | Ok state ->
	Deferred.return (Ok state)
      | Error _ ->
	failwith "nyi"

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  let handle_election_timeout self state =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term = term_or_zero current_term_opt in
    let term = Scow_term.succ current_term in
    send_vote_requests self term state
    >>=? fun () ->
    vote_for_self state
    >>=? fun () ->
    save_term term state
    >>=? fun () ->
    State.notify state Scow_notify.Event.(State_change (Follower, Candidate))
    >>= fun () ->
    state
    |> State.set_state_candidate
    |> State.set_heartbeat_timeout self
    |> Result.return
    |> Deferred.return

  let handle_heartbeat self state =
    state
    |> State.set_election_timeout self
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_rpc_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      handle_rpc_request_vote self state (node, request_vote, ctx)
    | Msg.Append_entry (ret, _) ->
      handle_append_entries self state ret
    | Msg.Election_timeout ->
      handle_election_timeout self state
    | Msg.Heartbeat ->
      handle_heartbeat self state
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp _ ->
      Deferred.return (Ok state)
end

