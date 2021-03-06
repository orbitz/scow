open Async.Std

module Msg = struct
  type ('n, 'elt) t =
    | Append_entries of ('n * 'elt Scow_rpc.Append_entries.t)
    | Request_vote   of ('n * Scow_rpc.Request_vote.t)
end

module type S = sig
  type ctx
  type elt
  type t

  module Node : sig
    type t
    val compare   : t -> t -> int
    val to_string : t -> string
  end

  val listen : t -> (((Node.t, elt) Msg.t * ctx), [> `Transport_error ]) Deferred.Result.t

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
