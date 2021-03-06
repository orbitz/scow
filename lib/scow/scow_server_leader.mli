open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Store.node
                 and  type elt    = Log.elt) ->
sig
  type state = Scow_server_state.Make(Statem)(Log)(Store)(Transport).t
  type errors = Scow_server_state.Make(Statem)(Log)(Store)(Transport).errors

  val handle_call :
    Scow_server_msg.Make(Statem)(Log)(Transport).t Gen_server.t ->
    state ->
    Scow_server_msg.Make(Statem)(Log)(Transport).op ->
    (state, errors) Deferred.Result.t

end

