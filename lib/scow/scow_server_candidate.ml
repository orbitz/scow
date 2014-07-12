open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
struct

  module Msg = Scow_server_msg.Make(Log)(Transport)
  module TMsg = Scow_transport.Msg

  let handle_call _self _state _msg =
    failwith "nyi"

end

