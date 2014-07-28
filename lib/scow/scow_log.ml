open Async.Std

module type S = sig
  type elt
  type t

  val append :
    t ->
    Scow_term.t ->
    elt list ->
    (Scow_log_index.t, [> `Append_failed | `Invalid_log ]) Deferred.Result.t

  val get_entry :
    t ->
    Scow_log_index.t ->
    ((Scow_term.t * elt), [> `Not_found | `Invalid_log ]) Deferred.Result.t

  val get_term :
    t ->
    Scow_log_index.t ->
    (Scow_term.t, [> `Not_found | `Invalid_log ]) Deferred.Result.t

  val get_log_index_range :
    t ->
    ((Scow_log_index.t * Scow_log_index.t), [> `Invalid_log ]) Deferred.Result.t

  val delete_from_log_index :
    t ->
    Scow_log_index.t ->
    (unit, [> `Invalid_log | `Not_found ]) Deferred.Result.t

  val is_elt_equal : elt -> elt -> bool
end
