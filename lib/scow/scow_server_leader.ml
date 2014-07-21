open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg = Scow_server_msg.Make(Log)(Transport)
  module TMsg = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.set_heartbeat_timeout self
      in
      state.State.handler
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
    else begin
      Transport.resp_append_entries
        state.State.transport
        ctx
        ~term:state.State.current_term
        ~success:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    if Scow_term.compare state.State.current_term request_vote.Rv.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.cancel_election_timeout
        |> State.cancel_heartbeat_timeout
      in
      state.State.handler
        self
        state
        (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))
    else begin
      Transport.resp_request_vote
        state.State.transport
        ctx
        ~term:state.State.current_term
        ~granted:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_append_entries self state ret =
    failwith "nyi"

  let handle_timeout self state =
    failwith "nyi"

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, request_vote, ctx))
    | Msg.Append_entries (ret, _) ->
      ignore_error (handle_append_entries self state ret)
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      ignore_error (handle_timeout self state)
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
end

