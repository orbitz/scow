open Core.Std
open Async.Std

module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
sig
  module Append_entry : sig
    type errors = [ `Not_master | `Append_failed | `Invalid_log ]
    type ret = (Statem.ret, errors) Result.t
    type t = { log_index : Scow_log_index.t
             ; ret       : ret Ivar.t
             }
  end

  module Role : sig
    type t =
      | Leader
      | Candidate
      | Follower
  end

  module Init_args : sig
    type t = { me           : Transport.Node.t
             ; nodes        : Transport.Node.t list
             ; statem       : Statem.t
             ; transport    : Transport.t
             ; log          : Log.t
             ; store        : Store.t
             ; timeout      : Time.Span.t
             ; timeout_rand : Time.Span.t
             ; notify       : Scow_notify.t
             }
  end

  type t

  type create_err = [ `Invalid_term_store ]

  val create : Init_args.t -> (t, [> create_err ]) Deferred.Result.t

  (* val notify : t -> Scow_notify.t *)

  val me         : t -> Transport.Node.t
  val nodes      : t -> Transport.Node.t list
  val leader     : t -> Transport.Node.t option
  val set_leader : Transport.Node.t option -> t -> t

  val current_term : t -> Scow_term.t

  val role     : t -> Role.t
  val set_role : Role.t -> t -> t

end
