open Async.Std

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

module Rule_table : sig
  type ('e, 's) t = (('e, 's) Rule.t * ('e, 's) Action.t) list
end

val create : ('e, 's) Rule_table.t -> ('e, 's) t
val run    : ('e, 's) t -> ('e, 's) Event.t -> 's Deferred.t
