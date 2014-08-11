open Core.Std
open Async.Std

module Make : functor (Transport : Scow_transport.S) -> sig
  type elt = Transport.elt
  type ctx = Transport.ctx
  type t

  module Node : sig
    type t = Transport.Node.t
    val compare : t -> t -> int
  end

  val create : Time.Span.t -> Transport.t -> t

  val listen :
    t ->
    (((Node.t, elt) Scow_transport.Msg.t * ctx), [> `Transport_error ]) Deferred.Result.t

  val resp_append_entries :
    t ->
    ctx ->
    term:Scow_term.t ->
    success:bool ->
    (unit, [> `Transport_error ]) Deferred.Result.t

  val resp_request_vote :
    t ->
    ctx ->
    term:Scow_term.t ->
    granted:bool ->
    (unit, [> `Transport_error ]) Deferred.Result.t

  val request_vote :
    t ->
    Node.t ->
    Scow_rpc.Request_vote.t ->
    ((Scow_term.t * bool), [> `Transport_error ]) Deferred.Result.t

  val append_entries :
    t ->
    Node.t ->
    elt Scow_rpc.Append_entries.t ->
    ((Scow_term.t * bool), [> `Transport_error ]) Deferred.Result.t

end
