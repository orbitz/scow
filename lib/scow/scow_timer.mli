open Core.Std

type t

val create       : Time.Span.t -> (unit -> unit) -> t
val cancel       : t -> unit
val is_cancelled : t -> bool
