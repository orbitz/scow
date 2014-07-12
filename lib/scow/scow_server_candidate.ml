open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Transport).t

  module Msg = Scow_server_msg.Make(Log)(Transport)
  module TMsg = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Transport)

  let handle_call self state = function
    | Msg.Recv_msg (TMsg.Append_entries (node, append_entries), ctx) ->
      Deferred.return (Ok state)
    | Msg.Recv_msg (TMsg.Request_vote (node, request_vote), ctx) -> begin
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
    | _ ->
      Deferred.return (Ok state)

end

