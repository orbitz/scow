open Core.Std
open Async.Std

module Make : functor (Log : Scow_log.S) -> functor (Transport : Scow_transport.S) -> sig
  type 'a append_entries = ('a, [ `Not_master | `Append_failed ] as 'e) Result.t
  type rpc = (Transport.Node.t, Transport.elt) Scow_transport.Msg.t * Transport.ctx

  type 'a op =
    | Election_timeout
    | Heartbeat
    | Rpc              of rpc
    | Append_entries   of ('a append_entries Ivar.t * Log.elt list)
    | Received_vote    of (Transport.Node.t * Scow_term.t * bool)

  type getter =
    | Get_nodes        of Transport.Node.t list Ivar.t
    | Get_current_term of Scow_term.t Ivar.t
    | Get_voted_for    of Transport.Node.t option Ivar.t
    | Get_leader       of Transport.Node.t option Ivar.t

  type 'a t =
    | Op  of 'a op
    | Get of getter
end
