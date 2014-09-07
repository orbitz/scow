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
  type state  = Scow_server_state.Make(Statem)(Log)(Store)(Transport).t
  type errors = Scow_server_state.Make(Statem)(Log)(Store)(Transport).errors

  module Msg   = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Store)(Transport)

  let is_term_gte_current state append_entries =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let module Ae    = Scow_rpc.Append_entries in
    let ae_term      = append_entries.Ae.term in
    let current_term = Option.value ~default:(Scow_term.zero ()) current_term_opt in
    if Scow_term.compare current_term ae_term <= 0 then
      Deferred.return (Ok ())
    else
      Deferred.return (Error `Term_less_than_current)

  let previous_index_matches_term state append_entries =
    let module Ae = Scow_rpc.Append_entries in
    let prev_log_index = append_entries.Ae.prev_log_index in
    let prev_log_term  = append_entries.Ae.prev_log_term in
    Log.get_term (State.log state) prev_log_index
    >>=? function
      | term when Scow_term.is_equal term prev_log_term ->
        Deferred.return (Ok ())
      | _ ->
        Deferred.return (Error `Prev_term_does_not_match)

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

  (*
   * It's possible that the AppendEntries contains values
   * that are already in our log or that need to replace entries
   * in our log.  So check that and if so, delete.
   *
   * Not that this cannot replace values that have already been commited.
   * No take backs if it's been committed.
   *)
  let delete_if_not_equal state append_entries =
    let module Ae = Scow_rpc.Append_entries in
    let log_index = Scow_log_index.succ (append_entries.Ae.prev_log_index) in
    check_entries_match (State.log state) log_index append_entries.Ae.entries

  (*
   * Do everything to determine if the log can be applied
   *)
  let can_apply_log state append_entries =
    is_term_gte_current state append_entries
    >>=? fun () ->
    previous_index_matches_term state append_entries
    >>=? fun () ->
    delete_if_not_equal state append_entries

  (*
   * Just append to the log and notify anyone that cares
   *)
  let do_append_entries state ctx entries =
    Log.append (State.log state) entries
    >>=? fun log_index ->
    State.notify state (Scow_notify.Event.Append_entry (log_index, List.length entries))
    >>= fun () ->
    Deferred.return (Ok ())

  (*
   * Keep on iterating over [last_applied] until it is equal
   * to [leader_commit]
   *)
  let rec apply_state_machine state leader_commit =
    let last_applied = State.last_applied state in
    let i_is_behind =
      Scow_log_index.compare last_applied leader_commit < 0
    in
    if i_is_behind then begin
      let to_apply = Scow_log_index.succ last_applied in
      Log.get_entry (State.log state) to_apply
      >>= function
        | Ok (_term, elt) -> begin
          Statem.apply (State.statem state) elt
          >>= fun _ ->
          let state = State.set_last_applied to_apply state in
          apply_state_machine state leader_commit
        end
        | Error (`Not_found _) ->
          (* This is ok just means we aren't caught up yet in the log *)
          Deferred.return (Ok state)
        | Error err ->
          Deferred.return (Error err)
    end
    else
      Deferred.return (Ok state)

  (*
   * [apply_append_entries] is the happypath of performing
   * and AppendEntries calls.
   *)
  let apply_append_entries state append_entries ctx =
    let module Ae = Scow_rpc.Append_entries in
    can_apply_log state append_entries
    >>=? fun entries ->
    do_append_entries state ctx entries
    >>=? fun () ->
    apply_state_machine state append_entries.Ae.leader_commit
    >>=? fun state ->
    Store.store_term (State.store state) append_entries.Ae.term
    >>=? fun () ->
    Transport.resp_append_entries
      (State.transport state)
      ctx
      ~term:append_entries.Ae.term
      ~success:true
    >>=? fun () ->
    Deferred.return (Ok state)

  (*
   * Apply the append entries data.  In the happy path the leader
   * will have been notified of the successful AppendEntries and all
   * the local state will be updated.
   *
   * On any failure, simply eat it and reply to the leader that it failed.
   *
   * This will cause a failure to reply to the leader to result in replying
   * again with the failure, despite evertyhing being successful, but this
   * is OK.  We might do some extra work as the leader sends the previous
   * entry again but the assumption is that such failures are unlikely.
   *)
  let do_handle_rpc_append_entries self state (node, append_entries, ctx) =
    apply_append_entries state append_entries ctx
    >>= function
      | Ok state ->
        Deferred.return (Ok state)
      | Error _ -> begin
        Store.load_term (State.store state)
        >>=? fun current_term_opt ->
        Transport.resp_append_entries
          (State.transport state)
          ctx
          ~term:(Option.value ~default:(Scow_term.zero ()) current_term_opt)
          ~success:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end

  let handle_rpc_append_entries self state node append_entries_data =
    (* Cancel timers first incase applying the entries takes awhile *)
    let state =
      state
      |> State.set_leader (Some node)
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    do_handle_rpc_append_entries self state append_entries_data
    >>= fun res ->
    (*
     * Don't care about any errors here, so pull the new state out
     * or return the existing state
     *)
    res
    |> Result.ok
    |> Option.value ~default:state
    |> Result.return
    |> Deferred.return

  (*
   * Give a vote only if we are in the past
   * OR if we have not given vote
   * OR if we have given a vote and it's the same node
   *)
  let should_grant_vote i_am_in_future voted_for_opt node =
    (not i_am_in_future)
    || (Option.value_map
          ~f:(fun voted_node -> Transport.Node.compare node voted_node = 0)
          ~default:false
          voted_for_opt)

  (*
   * Only update local information if the vote is actually granted
   *)
  let update_term_and_vote_if_granted store node request_vote_term granted =
    if granted then begin
      Store.store_vote store (Some node)
      >>=? fun () ->
      Store.store_term store request_vote_term
    end
    else
      Deferred.return (Ok ())

  (*
   * Handling a RequestVote comes down to determining if a vote can be granted
   * and if so updating some local information if so.
   * Almost all the work involved is just determining if a vote can be granted
   *)
  let do_handle_rpc_request_vote self state (node, request_vote, ctx) =
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, last_log_index) ->
    Log.get_term (State.log state) last_log_index
    >>=? fun last_log_term ->
    Store.load_vote (State.store state)
    >>=? fun voted_for_opt ->
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term   = Option.value ~default:(Scow_term.zero ()) current_term_opt in
    let module Rv      = Scow_rpc.Request_vote in
    let i_am_in_future =
      Scow_term.compare current_term request_vote.Rv.term > 0 ||
        Scow_term.compare last_log_term request_vote.Rv.last_log_term > 0 ||
        Scow_log_index.compare last_log_index request_vote.Rv.last_log_index > 0
    in
    let granted = should_grant_vote i_am_in_future voted_for_opt node in
    update_term_and_vote_if_granted
      (State.store state)
      node
      request_vote.Rv.term
      granted
    >>=? fun () ->
    Transport.resp_request_vote
      (State.transport state)
      ctx
      ~term:current_term
      ~granted
    >>=? fun () ->
    Deferred.return (Ok state)

  (*
   * If the incoming request for a vote has a term ahead of ours
   * then do all the necessary updates to our state.
   *)
  let update_term self state node request_vote =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term   = Option.value ~default:(Scow_term.zero ()) current_term_opt in
    let module Rv      = Scow_rpc.Request_vote in
    let vote_term      = request_vote.Rv.term in
    let vote_in_future = Scow_term.compare current_term vote_term < 0 in
    if vote_in_future then begin
      Store.store_vote (State.store state) None
      >>=? fun () ->
      Store.store_term (State.store state) request_vote.Rv.term
      >>=? fun () ->
      state
      |> State.set_leader (Some node)
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
      |> Result.return
      |> Deferred.return
    end
    else
      Deferred.return (Ok state)

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    update_term self state node request_vote
    >>=? fun state ->
    do_handle_rpc_request_vote self state (node, request_vote, ctx)

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  (*
   * Send a request vote to every node and queue the response back in
   * as a message.  If the RequestVote fails for some reason, that's ok
   * because a timeout will kick in and the process will start over again.
   *)
  let request_votes self state term =
    let send_request_vote node vote_request =
      Transport.request_vote
        (State.transport state)
        node
        vote_request
      >>=? fun (term, success) ->
      Gen_server.send self (Msg.Op (Msg.Received_vote (node, term, success)))
    in
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, high) ->
    Log.get_term (State.log state) high
    >>=? fun last_term ->
    let vote_request =
      Scow_rpc.Request_vote.({ term           = term
                             ; last_log_index = high
                             ; last_log_term  = last_term
                             })
    in
    List.iter
      ~f:(fun node -> ignore (send_request_vote node vote_request))
      (State.nodes state);
    Deferred.return (Ok ())

  (*
   * Have not received an AppendEntries or RequestVote, so become a
   * candidate send RequestVote's to everyone else.
   *)
  let handle_election_timeout self state =
    Store.load_term (State.store state)
    >>=? fun current_term_opt ->
    let current_term = Option.value ~default:(Scow_term.zero ()) current_term_opt in
    let term         = Scow_term.succ current_term in
    request_votes self state term
    >>=? fun () ->
    Store.store_vote (State.store state) (Some (State.me state))
    >>=? fun () ->
    Store.store_term (State.store state) term
    >>=? fun () ->
    State.notify state Scow_notify.Event.(State_change (Follower, Candidate))
    >>= fun () ->
    state
    |> State.set_state_candidate
    |> State.set_heartbeat_timeout self
    |> Result.return
    |> Deferred.return

  (*
   * Receiving a heartbeat means there has been no activity within
   * the heartbeat timeout.  So kick off an election timeout.
   *)
  let handle_heartbeat self state =
    state
    |> State.set_election_timeout self
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_rpc_append_entries self state node (node, append_entries, ctx)
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
