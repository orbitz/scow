type t

val zero   : unit -> t
val succ   : t -> t
val pred   : t -> t
val of_int : int -> t option
val to_int : t -> int
