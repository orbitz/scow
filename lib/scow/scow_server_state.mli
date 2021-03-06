open Core.Std
open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
sig
  type msg = Scow_server_msg.Make(Statem)(Log)(Transport).t
  type op  = Scow_server_msg.Make(Statem)(Log)(Transport).op

  type errors =
    [ `Invalid_log
    | `Invalid_term_store
    | `Invalid_vote_store
    | `Not_found of Scow_log_index.t
    | `Transport_error
    ]

  type 's handler =
      msg Gen_server.t ->
      's ->
      op ->
      ('s, errors) Deferred.Result.t

  type t

  module Append_entry : sig
    type errors = [ `Not_master | `Append_failed | `Invalid_log ]
    type ret = (Statem.ret, errors) Result.t
    type t = { log_index : Scow_log_index.t
             ; ret       : ret Ivar.t
             }
  end

  module Init_args : sig
    type t_ = { me           : Transport.Node.t
              ; nodes        : Transport.Node.t list
              ; statem       : Statem.t
              ; transport    : Transport.t
              ; log          : Log.t
              ; store        : Store.t
              ; timeout      : Time.Span.t
              ; timeout_rand : Time.Span.t
              ; notify       : Scow_notify.t
              ; follower     : t handler
              ; candidate    : t handler
              ; leader       : t handler
              }

    type t = t_
  end

  val create : Init_args.t -> (t, [> `Invalid_vote_store | `Invalid_term_store ]) Deferred.Result.t

  val handler : t -> t handler

  val current_term : t -> Scow_term.t
  val set_current_term : Scow_term.t -> t -> t

  val transport : t -> Transport.t
  val log : t -> Log.t
  val store : t -> Store.t
  val statem : t -> Statem.t

  val notify : t -> Scow_notify.t

  val commit_idx : t -> Scow_log_index.t
  val set_commit_idx : Scow_log_index.t -> t -> t
  val compute_highest_match_idx : t -> Scow_log_index.t

  val last_applied     : t -> Scow_log_index.t
  val set_last_applied : Scow_log_index.t -> t -> t

  val me : t -> Transport.Node.t
  val nodes : t -> Transport.Node.t list
  val leader : t -> Transport.Node.t option
  val set_leader : Transport.Node.t option -> t -> t

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

  val add_append_entry          : Append_entry.t -> t -> t
  val remove_append_entry       : Scow_log_index.t -> t -> (Append_entry.t option * t)
  val remove_all_append_entries : t -> (Append_entry.t list * t)

  val next_idx       : Transport.Node.t -> t -> Scow_log_index.t option
  val set_next_idx   : Transport.Node.t -> Scow_log_index.t -> t -> t
  val clear_next_idx : t -> t

  val match_idx       : Transport.Node.t -> t -> Scow_log_index.t option
  val set_match_idx   : Transport.Node.t -> Scow_log_index.t -> t -> t
  val clear_match_idx : t -> t
end
