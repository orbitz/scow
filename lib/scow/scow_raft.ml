open Core.Std
open Async.Std

module Term = struct
  type t = int

  let zero () = 0

  let succ t = t + 1

  let pred t = t - 1

  let of_int = function
    | n when n >= 0 -> Some n
    | _             -> None

  let to_int t = t
end

module Log_index = struct
  (* Semantically thing for now *)
  type t = Term.t

  let zero   = Term.zero
  let succ   = Term.succ
  let pred   = Term.pred
  let of_int = Term.of_int
  let to_int = Term.to_int
end

module Append_entries = struct
  type 'elt t = { term          : Term.t
                ; prev_log_idx  : Log_index.t
                ; prev_log_term : Term.t
                ; leader_commit : Log_index.t
                ; entries       : 'elt list
                }
end

module Request_vote = struct
  type t = { term           : Term.t
           ; last_log_index : Log_index.t
           ; last_log_term  : Term.t
           }
end

module Msg = struct
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

module Make = functor (Statem : STATEM) -> functor (Log : LOG) -> functor (Transport : TRANSPORT) -> struct
  type t = unit

  module Init_args = struct
    type t = { me                       : Transport.Node.t
             ; nodes                    : Transport.Node.t list
             ; transport                : Transport.t
             ; log                      : Log.t
             ; max_parallel_replication : int
             }
  end

  let start init_args = failwith "nyi"
  let stop t = failwith "nyi"
  let append_log t entries = failwith "nyi"

  let nodes t = failwith "nyi"
  let current_term t = failwith "nyi"
  let voted_for t = failwith "nyi"
  let leader t = failwith "nyi"
end
