open Core.Std
open Async.Std

module Make = functor (Log : Scow_log.T) -> functor (Transport : Scow_transport.T) -> struct
  type append_entries = (unit, [ `Not_master | `Append_failed ] as 'e) Result.t
  type rpc = (Transport.Node.t, Transport.elt) Scow_transport.Msg.t * Transport.ctx

  type op =
    | Election_timeout
    | Rpc              of rpc
    | Append_entries   of (append_entries Ivar.t * Log.elt list)

  type getter =
    | Get_nodes        of Transport.Node.t list Ivar.t
    | Get_current_term of Scow_term.t Ivar.t
    | Get_voted_for    of Transport.Node.t option Ivar.t
    | Get_leader       of Transport.Node.t option Ivar.t

  type t =
    | Op  of op
    | Get of getter
end
