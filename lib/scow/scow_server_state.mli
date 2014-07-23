open Core.Std
open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Vote_store.node) ->
sig
  type msg = Statem.ret Scow_server_msg.Make(Log)(Transport).t
  type op  = Statem.ret Scow_server_msg.Make(Log)(Transport).op

  type 's handler =
      msg Gen_server.t ->
      's ->
      op ->
      ('s, unit) Deferred.Result.t

  type t

  module Init_args : sig
    type t_ = { me           : Transport.Node.t
              ; nodes        : Transport.Node.t list
              ; statem       : Statem.t
              ; transport    : Transport.t
              ; log          : Log.t
              ; vote_store   : Vote_store.t
              ; max_par_repl : int
              ; timeout      : Time.Span.t
              ; timeout_rand : Time.Span.t
              ; follower     : t handler
              ; candidate    : t handler
              ; leader       : t handler
              }

    type t = t_
  end

  val create : Init_args.t -> (t, [> `Invalid_vote_store ]) Deferred.Result.t

  val handler : t -> t handler

  val current_term : t -> Scow_term.t
  val set_current_term : Scow_term.t -> t -> t

  val transport : t -> Transport.t
  val log : t -> Log.t
  val vote_store : t -> Vote_store.t
  val statem : t -> Statem.t

  val commit_idx : t -> Scow_log_index.t
  val set_commit_idx : Scow_log_index.t -> t -> t

  val me : t -> Transport.Node.t
  val nodes : t -> Transport.Node.t list

  val voted_for : t -> Transport.Node.t option
  val set_voted_for : Transport.Node.t option -> t -> t

  val set_heartbeat_timeout    : msg Gen_server.t -> t -> t
  val set_election_timeout     : msg Gen_server.t -> t -> t
  val cancel_election_timeout  : t -> t
  val cancel_heartbeat_timeout : t -> t

  val set_state_follower  : t -> t
  val set_state_candidate : t -> t
  val set_state_leader    : t -> t

  val record_vote : Transport.Node.t -> t -> t
  val count_votes : t -> int
  val clear_votes : t -> t
end
