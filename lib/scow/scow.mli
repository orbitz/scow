open Core.Std
open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Vote_store.node
                 and  type elt    = Log.elt) ->
sig
  type t

  module Init_args : sig
    type t = { me                       : Transport.Node.t
             ; nodes                    : Transport.Node.t list
             ; statem                   : Statem.t
             ; transport                : Transport.t
             ; log                      : Log.t
             ; vote_store               : Vote_store.t
             ; max_parallel_replication : int
             ; timeout                  : Time.Span.t
             ; timeout_rand             : Time.Span.t
             }
  end

  val start        : Init_args.t -> (t, [> `Invalid_vote_store | `Unknown]) Deferred.Result.t
  val stop         : t -> unit Deferred.t

  val append_log   :
    t ->
    Log.elt list ->
    (unit, [> `Not_master | `Append_failed | `Closed ]) Deferred.Result.t

  val nodes        : t -> (Transport.Node.t list, [> `Closed ]) Deferred.Result.t
  val current_term : t -> (Scow_term.t, [> `Closed ]) Deferred.Result.t
  val voted_for    : t -> (Transport.Node.t option, [> `Closed ]) Deferred.Result.t
  val leader       : t -> (Transport.Node.t option, [> `Closed ]) Deferred.Result.t
end
