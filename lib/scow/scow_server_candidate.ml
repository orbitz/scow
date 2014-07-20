open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg   = Scow_server_msg.Make(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term <= 0 then begin
      let state = State.set_state_follower state in
      state.State.handler
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
      (* TODO Probably don't want to do this *)
      >>= function
        | Ok state -> Deferred.return (Ok state)
        | Error () -> Deferred.return (Ok state)
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

  let handle_rpc_request_vote _self state (node, ctx) =
    Transport.resp_request_vote
      state.State.transport
      ctx
      ~term:state.State.current_term
      ~granted:false
    >>=? fun () ->
    Deferred.return (Ok state)

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  let transition_to_leader self state =
    let state =
      state
      |> State.set_state_leader
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
    in
    state.State.handler
      self
      state
      Msg.Heartbeat

  let handle_received_yes_vote self state node =
    let state = State.record_vote node state in
    if State.count_votes state > (List.length state.State.nodes / 2) then begin
      transition_to_leader self state
    end
    else
      Deferred.return (Ok state)

  let handle_received_no_vote _self state term =
    (* Unclear what to do in this case *)
    Deferred.return (Ok state)

  let handle_heartbeat_timeout self state =
    state
    |> State.set_state_follower
    |> State.clear_votes
    |> State.set_election_timeout self
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, _request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, ctx))
    | Msg.Append_entries (ret, _) ->
      ignore_error (handle_append_entries self state ret)
    | Msg.Received_vote (node, _term, true) ->
      ignore_error (handle_received_yes_vote self state node)
    | Msg.Received_vote (_node, term, false) ->
      ignore_error (handle_received_no_vote self state term)
    | Msg.Election_timeout ->
      Deferred.return (Ok state)
    | Msg.Heartbeat ->
      ignore_error (handle_heartbeat_timeout self state)

end

