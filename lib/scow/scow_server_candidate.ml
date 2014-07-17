open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg   = Scow_server_msg.Make(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term <= 0 then begin
      let state = State.set_state_follower state in
      state.State.handler
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
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

  let handle_heartbeat_timeout self state =
    state
    |> State.set_state_follower
    |> State.clear_votes
    |> State.set_election_timeout self
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_rpc_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, _request_vote), ctx) ->
      handle_rpc_request_vote self state (node, ctx)
    | Msg.Append_entries (ret, _) ->
      handle_append_entries self state ret
    | Msg.Received_vote (node, true) ->
      handle_received_yes_vote self state node
    | Msg.Received_vote (_node, false) ->
      Deferred.return (Ok state)
    | Msg.Election_timeout ->
      Deferred.return (Ok state)
    | Msg.Heartbeat ->
      handle_heartbeat_timeout self state

end

