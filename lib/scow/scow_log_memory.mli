open Async.Std

module type ELT = sig
  type t
  val compare : t -> t -> int
end

module Make : functor (Elt : ELT) -> sig
  type elt = Elt.t
  type t

  val create : unit -> t

  val append :
    t ->
    Scow_term.t ->
    elt list ->
    (Scow_log_index.t, [> `Append_failed | `Invalid_log ]) Deferred.Result.t

  val get_entry :
    t ->
    Scow_log_index.t ->
    ((Scow_term.t * elt), [> `Not_found of Scow_log_index.t | `Invalid_log ]) Deferred.Result.t

  val get_term :
    t ->
    Scow_log_index.t ->
    (Scow_term.t, [> `Not_found of Scow_log_index.t | `Invalid_log ]) Deferred.Result.t

  val get_log_index_range :
    t ->
    ((Scow_log_index.t * Scow_log_index.t), [> `Invalid_log ]) Deferred.Result.t

  val delete_from_log_index :
    t ->
    Scow_log_index.t ->
    (unit, [> `Invalid_log | `Not_found of Scow_log_index.t ]) Deferred.Result.t

  val is_elt_equal : elt -> elt -> bool
end
