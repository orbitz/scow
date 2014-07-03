open Async.Std

module Term : sig
  type t

  val zero   : unit -> t
  val succ   : t -> t
  val pred   : t -> t
  val of_int : int -> t option
  val to_int : t -> int
end

module Log_index : sig
  type t

  val zero   : unit -> t
  val succ   : t -> t
  val pred   : t -> t
  val of_int : int -> t option
  val to_int : t -> int
end

module Append_entries : sig
  type 'elt t = { term          : Term.t
                ; prev_log_idx  : Log_index.t
                ; prev_log_term : Term.t
                ; leader_commit : Log_index.t
                ; entries       : 'elt list
                }
end

module Request_vote : sig
  type t = { term           : Term.t
           ; last_log_index : Log_index.t
           ; last_log_term  : Term.t
           }
end

module Msg : sig
  type ('n, 'elt) t =
    | Append_entries      of ('n * 'elt Append_entries.t)
    | Resp_append_entries of 'n
    | Request_vote        of ('n * Request_vote.t)
    | Resp_request_vote   of 'n
end

module type TRANSPORT = sig
  type elt
  type t

  module Node : sig
    type t
    val compare : t -> t -> int
  end

  val listen : t -> ((Node.t, elt) Msg.t, unit) Deferred.Result.t

  val resp_append_entries : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t
  val resp_request_vote   : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t

  val request_vote        : t -> Node.t -> (unit, unit) Deferred.Result.t
  val append_entries      : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t
end

module type LOG = sig
  type elt
  type t

  val append_log : t -> 'elt Append_entries.t -> (unit, [> `Append_failed ]) Deferred.Result.t
  val get_entry  : t -> Log_index.t -> ((Term.t * elt), [> `Not_found ]) Deferred.Result.t
end

module type STATEM = sig
  type op
  type t

  val apply : t -> op -> (unit, unit) Deferred.Result.t
end

module Make : functor (Statem : STATEM) -> functor (Log : LOG) -> functor (Transport : TRANSPORT) -> sig
  type t

  module Init_args : sig
    type t = { me                       : Transport.Node.t
             ; nodes                    : Transport.Node.t list
             ; statem                   : Statem.t
             ; transport                : Transport.t
             ; log                      : Log.t
             ; max_parallel_replication : int
             }
  end

  val start        : Init_args.t -> (t, unit) Deferred.Result.t
  val stop         : t -> unit Deferred.t

  val append_log   :
    t ->
    Log.elt list ->
    (unit, [> `Not_master | `Append_failed | `Closed ]) Deferred.Result.t

  val nodes        : t -> (Transport.Node.t list, [> `Closed ]) Deferred.Result.t
  val current_term : t -> (Term.t, [> `Closed ]) Deferred.Result.t
  val voted_for    : t -> (Transport.Node.t option, [> `Closed ]) Deferred.Result.t
  val leader       : t -> (Transport.Node.t option, [> `Closed ]) Deferred.Result.t
end
