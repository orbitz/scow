open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Vote_store.node) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg   = Scow_server_msg.Make(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let apply_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term <= 0 then begin
      failwith "nyi"
    end
    else begin
      Transport.resp_append_entries
        state.State.transport
        ctx
        ~term:state.State.current_term
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
    match state.State.voted_for with
      | Some _ -> begin
        Transport.resp_request_vote
          state.State.transport
          ctx
          ~term:state.State.current_term
          ~granted:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end
      | None -> begin
        let state = State.({ state with voted_for = Some node }) in
        Transport.resp_request_vote
          state.State.transport
          ctx
          ~term:state.State.current_term
          ~granted:true
        >>=? fun () ->
        Vote_store.store state.State.vote_store (Some node)
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
    let term = Scow_term.succ state.State.current_term in
    let vote_request =
      Scow_rpc.({ Request_vote.term           = term
                ;              last_log_index = failwith "nyi"
                ;              last_log_term  = failwith "nyi"
                })
    in
    request_votes self vote_request state.State.transport state.State.nodes;
    state
    |> State.set_state_candidate
    |> fun state -> { state with State.current_term = term }
    |> State.set_heartbeat_timeout self
    |> State.clear_votes
    |> State.record_vote state.State.me
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_rpc_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      handle_rpc_request_vote self state (node, request_vote, ctx)
    | Msg.Append_entries (ret, _) ->
      handle_append_entries self state ret
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      handle_timeout self state
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
end

