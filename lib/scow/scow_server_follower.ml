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

  let handle_call self _state _msg =
    failwith "nyi"
end

