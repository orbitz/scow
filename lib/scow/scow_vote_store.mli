open Async.Std

module type S = sig
  type node
  type t

  val store : t -> node option -> (unit, unit) Deferred.Result.t
  val load  : t -> (node option, unit) Deferred.Result.t
end
