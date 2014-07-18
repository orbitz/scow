open Async.Std

module type S = sig
  type op
  type t

  val apply : t -> op -> (unit, unit) Deferred.Result.t
end
