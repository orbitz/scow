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

  module Msg       = Scow_server_msg.Make(Log)(Transport)
  module State     = Scow_server_state.Make(Statem)(Log)(Transport)

  module Candidate = Scow_server_candidate.Make(Statem)(Log)(Vote_store)(Transport)
  module Follower  = Scow_server_follower.Make(Statem)(Log)(Vote_store)(Transport)
  module Leader    = Scow_server_leader.Make(Statem)(Log)(Vote_store)(Transport)

  type t = Msg.t Gen_server.t

  module Server = struct
    module Resp = Gen_server.Response

    let rec transport_listener_loop server transport =
      Transport.listen transport
      >>=? fun msg_with_ctx -> begin
        Gen_server.send server (Msg.Recv_msg msg_with_ctx)
        >>= function
          | Ok _ ->
            transport_listener_loop server transport
          | Error _ ->
            Deferred.return (Error ())
      end

    let start_transport_listener server transport =
      ignore (transport_listener_loop server transport)

    let init self init_args =
      start_transport_listener self init_args.Init_args.transport;
      Deferred.return
        State.(Ok { me           = init_args.Init_args.me
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
                  ; handler      = Follower.handle_call
                  ; states       = { States.follower  = Follower.handle_call
                                   ;        candidate = Candidate.handle_call
                                   ;        leader    = Leader.handle_call
                                   }
                  })

    let handle_call self state msg =
      state.State.handler self state msg
      >>= function
        | Ok state ->
          Deferred.return (Resp.Ok state)
        | Error _ ->
          Deferred.return (Resp.Error ((), state))

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
