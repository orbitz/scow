open Async.Std

module Make :
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T  with type Node.t = Vote_store.node) ->
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
             }
  end

  val start        : Init_args.t -> (t, unit) Deferred.Result.t
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
