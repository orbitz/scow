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

  let handle_recv_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term <= 0 then begin
      let state = State.({ state with handler = state.states.States.follower }) in
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

  let schedule_heartbeat self state =
    failwith "nyi"

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_recv_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) -> begin
      Transport.resp_request_vote
        state.State.transport
        ctx
        ~term:state.State.current_term
        ~granted:false
      >>=? fun () ->
      Deferred.return (Ok state)
    end
    | Msg.Append_entries (ret, _) -> begin
      Ivar.fill ret (Error `Not_master);
      Deferred.return (Ok state)
    end
    | Msg.Election_timeout ->
      let state = State.({ state with handler = state.states.States.follower }) in
      ignore (schedule_heartbeat self state);
      Deferred.return (Ok state)

end

