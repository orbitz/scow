open Core.Std
open Async.Std

module Make :
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
sig
  type msg = Scow_server_msg.Make(Log)(Transport).t
  type op  = Scow_server_msg.Make(Log)(Transport).op

  type 's handler =
      msg Gen_server.t ->
      's ->
      op ->
      ('s, unit) Deferred.Result.t

  module States : sig
    type 's t = { follower  : 's handler
                ; candidate : 's handler
                ; leader    : 's handler
                }
  end

  type t = { me           : Transport.Node.t
           ; nodes        : Transport.Node.t list
           ; statem       : Statem.t
           ; transport    : Transport.t
           ; log          : Log.t
           ; vote_store   : Vote_store.t
           ; max_par_repl : int
           ; current_term : Scow_term.t
           ; commit_idx   : Scow_log_index.t
           ; last_applied : Scow_log_index.t
           ; leader       : Transport.Node.t option
           ; voted_for    : Transport.Node.t option
           ; handler      : t handler
           ; states       : t States.t
           ; timer        : Scow_timer.t option
           ; timeout      : Time.Span.t
           ; timeout_rand : Time.Span.t
           }


  val set_timeout        : msg Gen_server.t -> t -> t
  val set_random_timeout : msg Gen_server.t -> t -> t
  val cancel_timeout     : t -> t

  val set_state_follower  : t -> t
  val set_state_candidate : t -> t
  val set_state_leader    : t -> t
end
