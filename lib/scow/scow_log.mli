open Async.Std

module type T = sig
  type elt
  type t

  val append_log :
    t ->
    'elt Scow_rpc.Append_entries.t ->
    (unit, [> `Append_failed ]) Deferred.Result.t

  val get_entry :
    t ->
    Scow_log_index.t ->
    ((Scow_term.t * elt), [> `Not_found ]) Deferred.Result.t
end
