open Async.Std
open Core.Std

module Event = struct
  type ('e, 's) t = { event : 'e
                    ; state : 's
                    }
end

module Rule = struct
  type ('e, 's) t = ('e, 's) Event.t -> bool
end

module Action = struct
  type ('e, 's) t = ('e, 's) Event.t -> 's Deferred.t
end

type ('e, 's) t = (('e, 's) Rule.t * ('e, 's) Action.t) list

let create ruleset = ruleset

let run t event =
  Deferred.List.fold
    ~f:(fun event (r, a) ->
      if r event then begin
        a event
        >>= fun s ->
        Deferred.return Event.({ event with state = s })
      end
      else
        Deferred.return event)
    ~init:event
    t
  >>= fun event ->
  Deferred.return event.Event.state
