open Async.Std

module type NODE = sig
  type t
end

module Make : functor (Node : NODE) -> sig
  type node = Node.t
  type t

  val create     : unit -> t

  val store_vote : t -> node option -> (unit, [> `Invalid_vote_store ]) Deferred.Result.t
  val load_vote  : t -> (node option, [> `Invalid_vote_store ]) Deferred.Result.t

  val store_term : t -> Scow_term.t -> (unit, [> `Invalid_term_store ]) Deferred.Result.t
  val load_term  : t -> (Scow_term.t option, [> `Invalid_term_store ]) Deferred.Result.t
end
