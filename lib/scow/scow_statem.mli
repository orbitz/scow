open Async.Std

module type T = sig
  type op
  type t

  val apply : t -> op -> (unit, unit) Deferred.Result.t
end
