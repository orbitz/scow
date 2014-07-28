open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Transport : Scow_transport.S) ->
struct
  type append_entries = (Statem.ret, [ `Not_master | `Append_failed | `Invalid_log ]) Result.t
  type rpc = (Transport.Node.t, Transport.elt) Scow_transport.Msg.t * Transport.ctx
  type append_entries_resp =
      (Transport.Node.t * Scow_log_index.t * ((Scow_term.t * bool), [ `Transport_error ]) Result.t)

  type op =
    | Election_timeout
    | Heartbeat
    | Rpc                 of rpc
    | Append_entry        of (append_entries Ivar.t * Statem.op)
    | Received_vote       of (Transport.Node.t * Scow_term.t * bool)
    | Append_entries_resp of append_entries_resp

  type getter =
    | Get_nodes        of Transport.Node.t list Ivar.t
    | Get_current_term of Scow_term.t Ivar.t
    | Get_voted_for    of Transport.Node.t option Ivar.t
    | Get_leader       of Transport.Node.t option Ivar.t

  type t =
    | Op  of op
    | Get of getter
end
