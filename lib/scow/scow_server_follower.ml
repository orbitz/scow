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

  let is_valid_term state append_entries =
    let module Ae    = Scow_rpc.Append_entries in
    let current_term = State.current_term state in
    let ae_term      = append_entries.Ae.term in
    if Scow_term.compare current_term ae_term <= 0 then
      Deferred.return (Ok ())
    else
      Deferred.return (Error `Term_less_than_current)

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

  let can_apply_log state append_entries =
    (*
     * If this function returns successfully it means that the log is in
     * a valid state for calling [append]
     *)
    is_valid_term state append_entries
    >>=? fun () ->
    previous_index_matches_term state append_entries
    >>=? fun () ->
    delete_if_not_equal state append_entries

  let do_append_entries state ctx entries =
    Log.append (State.log state) entries
    >>=? fun log_index ->
    Transport.resp_append_entries
      (State.transport state)
      ctx
      ~term:(State.current_term state)
      ~success:true
    >>=? fun () ->
    Deferred.return (Ok state)

  let rec apply_state_machine state = function
    | leader_commit
        when Scow_log_index.compare (State.last_applied state) leader_commit < 0 -> begin
      let to_apply = Scow_log_index.succ (State.last_applied state) in
      Log.get_entry (State.log state) to_apply
      >>= function
        | Ok (_term, elt) -> begin
          Statem.apply (State.statem state) elt
          >>= fun _ ->
          let state = State.set_last_applied to_apply state in
          apply_state_machine state leader_commit
        end
        | Error (`Not_found _) ->
          Deferred.return (Ok state)
        | Error err ->
          Deferred.return (Error err)
    end
    | _ ->
      Deferred.return (Ok state)

  let apply_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    let test_and_do () =
      can_apply_log state append_entries
      >>=? fun entries ->
      do_append_entries state ctx entries
      >>=? fun state ->
      apply_state_machine state append_entries.Ae.leader_commit
      >>=? fun state ->
      let term = append_entries.Ae.term in
      let state = State.set_current_term term state in
      Deferred.return (Ok (State.set_current_term term state))
    in
    test_and_do ()
    >>= function
      | Ok state ->
        Deferred.return (Ok state)
      | Error err -> begin
        let string_of_error = function
          | `Not_found idx -> sprintf "Not_found %d" (Scow_log_index.to_int idx)
          | `Append_failed -> "Append_failed"
          | `Transport_error -> "Transport_error"
          | `Invalid_log -> "Invalid_log"
          | `Term_less_than_current -> "Term_less_than_current"
          | `Prev_term_does_not_match -> "Prev_term_does_not_match"
        in
        (* printf "Follower: %s Failed %s\n%!" *)
        (*   (Transport.Node.to_string (State.me state)) *)
        (*   (string_of_error err); *)
        Transport.resp_append_entries
          (State.transport state)
          ctx
          ~term:(State.current_term state)
          ~success:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end

  let handle_rpc_append_entries self state node append_entries_data =
    let state =
      state
      |> State.set_leader (Some node)
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    apply_rpc_append_entries self state append_entries_data
    >>= function
      | Ok state ->
        Deferred.return (Ok state)
      | Error _ ->
        Deferred.return (Ok state)

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    Log.get_log_index_range (State.log state)
    >>=? fun (_low, last_log_index) ->
    Log.get_term (State.log state) last_log_index
    >>=? fun last_log_term ->
    let i_am_in_future =
      Scow_term.compare (State.current_term state) request_vote.Rv.term > 0 ||
        Scow_term.compare last_log_term request_vote.Rv.last_log_term > 0 ||
        Scow_log_index.compare last_log_index request_vote.Rv.last_log_index > 0
    in
    let reply granted =
      Transport.resp_request_vote
        (State.transport state)
        ctx
        ~term:(State.current_term state)
        ~granted
    in
    match State.voted_for state with
      | Some _ | None when i_am_in_future -> begin
        reply false
        >>= fun _ ->
        Deferred.return (Ok state)
      end
      | Some voted_node when Transport.Node.compare node voted_node = 0 -> begin
        reply true
        >>= fun _ ->
        Deferred.return (Ok state)
      end
      | Some voted_for -> begin
        reply false
        >>= fun _ ->
        Deferred.return (Ok state)
      end
      | None -> begin
        let state =
          state
          |> State.set_voted_for (Some node)
          |> State.set_current_term request_vote.Rv.term
        in
        Store.store_vote (State.store state) (Some node)
        >>=? fun () ->
        reply true
        >>= fun _ ->
        Deferred.return (Ok state)
      end

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  let handle_election_timeout self state =
    let term = Scow_term.succ (State.current_term state) in
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
    request_votes self vote_request (State.transport state) (State.nodes state);
    Store.store_vote (State.store state) (Some (State.me state))
    >>=? fun () ->
    Store.store_term (State.store state) term
    >>=? fun () ->
    state
    |> State.set_state_candidate
    |> State.set_current_term term
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

