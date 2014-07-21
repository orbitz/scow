open Async.Std

module type S = sig
  type op
  type ret
  type t

  val apply : t -> op -> ret Deferred.t
end
