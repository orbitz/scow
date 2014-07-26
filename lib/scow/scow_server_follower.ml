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

  module Msg   = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let is_valid_term state append_entries =
    let module Ae    = Scow_rpc.Append_entries in
    let current_term = State.current_term state in
    let ae_term      = append_entries.Ae.term in
    if Scow_term.compare current_term ae_term >= 0 then
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
      | _ ->
        Deferred.return (Error `Prev_term_does_not_match)

  let rec check_entries_match log log_index = function
    | [] ->
      Deferred.return (Ok [])
    | elt::elts -> begin
      Log.get_entry log log_index
      >>= function
        | Ok (_term, log_elt) when Log.is_elt_equal log_elt elt ->
          check_entries_match log (Scow_log_index.succ log_index) elts
        | Ok (_term, _log_elt) -> begin
          Log.delete_from_log_index log log_index
          >>=? fun () ->
          Deferred.return (Ok (elt::elts))
        end
        | Error `Not_found ->
          (* We assume there are no gaps in the log *)
          Deferred.return (Ok (elt::elts))
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

  let do_append_entries state ctx term entries =
    let state = State.set_current_term term state in
    Log.append (State.log state) term entries
    >>=? fun () ->
    Transport.resp_append_entries
      (State.transport state)
      ctx
      ~term:(State.current_term state)
      ~success:true
    >>=? fun () ->
    Deferred.return (Ok state)

  let rec apply_state_machine state = function
    | leader_commit when (State.commit_idx state) < leader_commit -> begin
      let commit_idx = Scow_log_index.succ (State.commit_idx state) in
      Log.get_entry (State.log state) commit_idx
      >>=? fun (_term, elt) ->
      Statem.apply (State.statem state) elt
      >>= fun _ ->
      let state = State.set_commit_idx commit_idx state in
      apply_state_machine state leader_commit
    end
    | _ ->
      Deferred.return (Ok state)

  let apply_rpc_append_entries self state (node, append_entries, ctx) =
    let test_and_do () =
      let module Ae = Scow_rpc.Append_entries in
      can_apply_log state append_entries
      >>=? fun entries ->
      do_append_entries state ctx append_entries.Ae.term entries
      >>=? fun state ->
      apply_state_machine state append_entries.Ae.leader_commit
    in
    test_and_do ()
    >>= function
      | Ok state ->
        Deferred.return (Ok state)
      | Error `Append_failed
      | Error `Transport_error
      | Error `Not_found
      | Error `Invalid_log
      | Error `Term_less_than_current
      | Error `Prev_term_does_not_match -> begin
        Transport.resp_append_entries
          (State.transport state)
          ctx
          ~term:(State.current_term state)
          ~success:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end

  let handle_rpc_append_entries self state append_entries_data =
    let state =
      state
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
      |> State.set_heartbeat_timeout self
    in
    apply_rpc_append_entries self state append_entries_data

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    match State.voted_for state with
      | Some _ -> begin
        Transport.resp_request_vote
          (State.transport state)
          ctx
          ~term:(State.current_term state)
          ~granted:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end
      | None -> begin
        let state = State.set_voted_for (Some node) state in
        Transport.resp_request_vote
          (State.transport state)
          ctx
          ~term:(State.current_term state)
          ~granted:true
        >>=? fun () ->
        Vote_store.store (State.vote_store state) (Some node)
        >>=? fun () ->
        Deferred.return (Ok state)
      end

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  let request_votes self vote_request transport nodes =
    let send_request_vote node =
      Transport.request_vote
        transport
        node
        vote_request
      >>=? fun (term, success) -> begin
      Gen_server.send self (Msg.Op (Msg.Received_vote (node, term, success)))
      end
    in
    List.iter
      ~f:(fun node -> ignore (send_request_vote node))
      nodes

  let handle_timeout self state =
    let term = Scow_term.succ (State.current_term state) in
    let vote_request =
      Scow_rpc.({ Request_vote.term           = term
                ;              last_log_index = failwith "nyi"
                ;              last_log_term  = failwith "nyi"
                })
    in
    request_votes self vote_request (State.transport state) (State.nodes state);
    state
    |> State.set_state_candidate
    |> State.set_current_term term
    |> State.set_heartbeat_timeout self
    |> State.clear_votes
    |> State.record_vote (State.me state)
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, request_vote, ctx))
    | Msg.Append_entry (ret, _) ->
      ignore_error (handle_append_entries self state ret)
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      ignore_error (handle_timeout self state)
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp _ ->
      failwith "nyi"
end

