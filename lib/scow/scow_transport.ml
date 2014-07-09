open Async.Std

module Msg = struct
  type ('n, 'elt) t =
    | Append_entries      of ('n * 'elt Scow_rpc.Append_entries.t)
    | Resp_append_entries of 'n
    | Request_vote        of ('n * Scow_rpc.Request_vote.t)
    | Resp_request_vote   of 'n
end

module type T = sig
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
