open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Transport : Scow_transport.S) ->
struct
  type getter =
    | Get_me           of Transport.Node.t Ivar.t
    | Get_nodes        of Transport.Node.t list Ivar.t
    | Get_current_term of Scow_term.t Ivar.t
    | Get_voted_for    of Transport.Node.t option Ivar.t
    | Get_leader       of Transport.Node.t option Ivar.t

  type t =
    | Op  of Scow_replication_msg.Make(Statem)(Log)(Transport).t
    | Get of getter
end
