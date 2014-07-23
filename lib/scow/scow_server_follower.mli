open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Vote_store.node
                 and  type elt    = Log.elt) ->
sig
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  val handle_call :
    Statem.ret Scow_server_msg.Make(Log)(Transport).t Gen_server.t ->
    state ->
    Statem.ret Scow_server_msg.Make(Log)(Transport).op ->
    (state, unit) Deferred.Result.t

end

