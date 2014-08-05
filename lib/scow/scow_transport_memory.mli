open Async.Std

module type ELT = sig
  type t
end

module Make : functor (Elt : ELT) -> sig
  type elt = Elt.t
  type ctx
  type t

  module Node : sig
    type t = string
    val compare : t -> t -> int
  end

  module Router : sig
    type t
    type msg = ((Node.t, elt) Scow_transport.Msg.t * ctx)

    val create    : unit -> t
    val add_node  : t -> Node.t
    val route_msg : t -> Node.t -> msg -> unit
    val reader    : t -> Node.t -> msg Pipe.Reader.t option
  end

  val create : Node.t -> Router.t -> t

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
