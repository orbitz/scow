open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S) ->
sig
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  val handle_call :
    Scow_server_msg.Make(Log)(Transport).t Gen_server.t ->
    state ->
    Scow_server_msg.Make(Log)(Transport).op ->
    (state, unit) Deferred.Result.t

end

