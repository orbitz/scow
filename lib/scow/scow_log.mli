open Async.Std

module type S = sig
  type elt
  type t

  val append :
    t ->
    Scow_term.t ->
    elt list ->
    (unit, [> `Append_failed ]) Deferred.Result.t

  val get_entry :
    t ->
    Scow_log_index.t ->
    ((Scow_term.t * elt), [> `Not_found ]) Deferred.Result.t
end
