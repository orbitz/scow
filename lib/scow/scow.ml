open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
struct
  module Init_args = struct
    type t = { me                       : Transport.Node.t
             ; nodes                    : Transport.Node.t list
             ; statem                   : Statem.t
             ; transport                : Transport.t
             ; log                      : Log.t
             ; max_parallel_replication : int
             }
  end

  module TMsg = Scow_transport.Msg

  module Msg = struct
    type append_entries = (unit, [ `Not_master | `Append_failed ] as 'e) Result.t

    type t =
      | Append_entries   of (append_entries Ivar.t * Log.elt list)
      | Recv_msg         of (Transport.Node.t, Transport.elt) TMsg.t
      | Get_nodes        of Transport.Node.t list Ivar.t
      | Get_current_term of Scow_term.t Ivar.t
      | Get_voted_for    of Transport.Node.t option Ivar.t
      | Get_leader       of Transport.Node.t option Ivar.t
  end

  type t = Msg.t Gen_server.t

  module Server = struct
    module Resp = Gen_server.Response

    type t = { me           : Transport.Node.t
             ; nodes        : Transport.Node.t list
             ; statem       : Statem.t
             ; transport    : Transport.t
             ; log          : Log.t
             ; max_par_repl : int
             ; current_term : Scow_term.t
             ; commit_idx   : Scow_log_index.t
             ; last_applied : Scow_log_index.t
             ; leader       : Transport.Node.t option
             ; voted_for    : Transport.Node.t option
             }


    let rec transport_listener_loop server transport =
      Transport.listen transport
      >>=? fun msg -> begin
        Gen_server.send server (Msg.Recv_msg msg)
        >>= function
          | Ok _ ->
            transport_listener_loop server transport
          | Error _ ->
            Deferred.return (Error ())
      end

    let start_transport_listener server transport =
      ignore (transport_listener_loop server transport)

    let handle_request_vote state node params =
      failwith "nyi"

    let init self init_args =
      start_transport_listener self init_args.Init_args.transport;
      Deferred.return (Ok { me           = init_args.Init_args.me
                          ; nodes        = init_args.Init_args.nodes
                          ; statem       = init_args.Init_args.statem
                          ; transport    = init_args.Init_args.transport
                          ; log          = init_args.Init_args.log
                          ; max_par_repl = init_args.Init_args.max_parallel_replication
                          ; current_term = Scow_term.zero ()
                          ; commit_idx   = Scow_log_index.zero ()
                          ; last_applied = Scow_log_index.zero ()
                          ; leader       = None
                          ; voted_for    = None
                          })

    let handle_call _self state = function
      | Msg.Append_entries (ret, entries) -> begin
        Deferred.return (Resp.Ok state)
      end
      | Msg.Recv_msg (TMsg.Append_entries (node, params)) -> begin
        Deferred.return (Resp.Ok state)
      end
      | Msg.Recv_msg (TMsg.Request_vote (node, params)) -> begin
        handle_request_vote state node params >>= fun state' ->
        Deferred.return (Resp.Ok state')
      end
      | Msg.Recv_msg (TMsg.Resp_append_entries node) -> begin
        Deferred.return (Resp.Ok state)
      end
      | Msg.Recv_msg (TMsg.Resp_request_vote node) -> begin
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get_nodes ret -> begin
        Ivar.fill ret state.nodes;
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get_current_term ret -> begin
        Ivar.fill ret state.current_term;
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get_voted_for ret -> begin
        Ivar.fill ret state.voted_for;
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get_leader ret -> begin
        Ivar.fill ret state.leader;
        Deferred.return (Resp.Ok state)
      end

    let terminate _reason state =
      failwith "nyi"

    let callbacks = { Gen_server.Server.init; handle_call; terminate }
  end

  (*
   ******************************************************************
   * API Functions
   ******************************************************************
   *)
  let start init_args =
    Gen_server.start init_args Server.callbacks
    >>= function
      | Ok t    -> Deferred.return (Ok t)
      | Error _ -> Deferred.return (Error ())

  let stop t =
    Gen_server.stop t
    >>= fun _ ->
    Deferred.unit

  let append_log t entries =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Append_entries (ret, entries))
    >>=? fun _ ->
    Ivar.read ret
    >>= function
      | Ok ()                -> Deferred.return (Ok ())
      | Error `Not_master    -> Deferred.return (Error `Not_master)
      | Error `Append_failed -> Deferred.return (Error `Append_failed)

  let send_with_ret t ret msg =
    Gen_server.send t msg
    >>=? fun _ ->
    Ivar.read ret
    >>= fun result ->
    Deferred.return (Ok result)

  let nodes t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get_nodes ret)

  let current_term t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get_current_term ret)

  let voted_for t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get_voted_for ret)

  let leader t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get_leader ret)
end
