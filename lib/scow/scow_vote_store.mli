open Async.Std

module type S = sig
  type node
  type t

  val store : t -> node option -> (unit, [> `Invalid_vote_store ]) Deferred.Result.t
  val load  : t -> (node option, [> `Invalid_vote_store ]) Deferred.Result.t
end
