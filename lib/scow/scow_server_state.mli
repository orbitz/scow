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
    | `Not_found
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
             ; op        : Statem.op
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
              ; max_par_repl : int
              ; timeout      : Time.Span.t
              ; timeout_rand : Time.Span.t
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

  val commit_idx : t -> Scow_log_index.t
  val set_commit_idx : Scow_log_index.t -> t -> t
  val update_commit_index : t -> t

  val max_par : t -> int

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

  val add_append_entry          : Append_entry.t -> t -> t
  val remove_append_entries     : Scow_log_index.t -> t -> (Append_entry.t list * t)
  val remove_all_append_entries : t -> (Append_entry.t list * t)

  val next_idx       : Transport.Node.t -> t -> Scow_log_index.t option
  val set_next_idx   : Transport.Node.t -> Scow_log_index.t -> t -> t
  val clear_next_idx : t -> t

  val match_idx       : Transport.Node.t -> t -> Scow_log_index.t option
  val set_match_idx   : Transport.Node.t -> Scow_log_index.t -> t -> t
  val clear_match_idx : t -> t
end
