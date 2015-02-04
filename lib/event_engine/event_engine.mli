open Async.Std

module O : sig
  val (&&) : ('a -> bool) -> ('a -> bool) -> 'a -> bool
  val (||) : ('a -> bool) -> ('a -> bool) -> 'a -> bool
  val not  : ('a -> bool) -> 'a -> bool
end

type ('e, 's) t

module Event : sig
  type ('e, 's) t = { event : 'e
                    ; state : 's
                    }
end

module Rule : sig
  type ('e, 's) t = ('e, 's) Event.t -> bool
end

module Action : sig
  type ('e, 's) t = ('e, 's) Event.t -> 's Deferred.t
end

val create : (('e, 's) Rule.t * ('e, 's) Action.t) list -> ('e, 's) t
val run    : ('e, 's) t -> ('e, 's) Event.t -> 's Deferred.t
