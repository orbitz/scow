open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S with type elt = Statem.op) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Store.node
                 and  type elt    = Log.elt) ->
struct
  module Init_args = struct
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

  module RState     = Scow_replication_state.Make(Statem)(Log)(Store)(Transport)
  module Msg        = Scow_msg.Make(Statem)(Log)(Transport)
  module Op         = Scow_replication_msg.Make(Statem)(Log)(Transport)
  module Rule_table = Scow_rule_table.Make(Statem)(Log)(Store)(Transport)

  type t = Msg.t Gen_server.t

  module Server = struct
    module Resp = Gen_server.Response

    type state = { event_engine : (Op.t, RState.t) Event_engine.t
                 ; rstate       : RState.t
                 }

    let rec transport_listener_loop server transport =
      Transport.listen transport
      >>=? fun msg_with_ctx -> begin
      Gen_server.send server (Msg.Op (Op.Rpc msg_with_ctx))
      >>= function
        | Ok _ ->
          transport_listener_loop server transport
        | Error _ ->
          Deferred.return (Error `Transport_error)
      end

    let start_transport_listener server transport =
      ignore (transport_listener_loop server transport)

    let init self init_args =
      start_transport_listener self init_args.Init_args.transport;
      ignore (Gen_server.send self (Msg.Op Op.Heartbeat));
      let init_args =
        RState.Init_args.({ me           = init_args.Init_args.me
                          ; nodes        = init_args.Init_args.nodes
                          ; statem       = init_args.Init_args.statem
                          ; transport    = init_args.Init_args.transport
                          ; log          = init_args.Init_args.log
                          ; store        = init_args.Init_args.store
                          ; timeout      = init_args.Init_args.timeout
                          ; timeout_rand = init_args.Init_args.timeout_rand
                          ; notify       = init_args.Init_args.notify
                          })
      in
      RState.create init_args
      >>=? fun rstate ->
      let event_engine = Event_engine.create (Rule_table.table ()) in
      Deferred.return (Ok { event_engine; rstate })

    let handle_call self state = function
      | Msg.Op op -> begin
        let event = Event_engine.Event.({ event = op; state = state.rstate }) in
        Event_engine.run state.event_engine event
        >>= fun rstate ->
        Deferred.return (Resp.Ok { state with rstate })
      end
      | Msg.Get (Msg.Get_leader ret) -> begin
        Ivar.fill ret (RState.leader state.rstate);
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get (Msg.Get_me ret) -> begin
        Ivar.fill ret (RState.me state.rstate);
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get (Msg.Get_current_term ret) -> begin
        Ivar.fill ret (RState.current_term state.rstate);
        Deferred.return (Resp.Ok state)
      end
      | Msg.Get _ ->
        Deferred.return (Resp.Ok state)

    let terminate reason state =
      let string_of_error = function
        | `Invalid_log        -> "Invalid_log"
        | `Invalid_term_store -> "Invalid_term_store"
        | `Invalid_vote_store -> "Invalid_vote_store"
        | `Not_found idx      -> sprintf "Not_found %d" (Scow_log_index.to_int idx)
        | `Transport_error    -> "Transport_error"
      in
      let open Gen_server.Server in
      match reason with
        | Normal -> Deferred.unit
        | Exn exn  -> begin
          printf "Exn %s\n%!" (Exn.to_string exn);
          Deferred.unit
        end
        | Error err -> begin
          printf "Error: %s - %s\n"
            (string_of_error err)
            (Transport.Node.to_string (RState.me state.rstate));
          Deferred.unit
        end

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
      | Ok t ->
        Deferred.return (Ok t)
      | Error (`Error `Invalid_vote_store) ->
        Deferred.return (Error `Invalid_vote_store)
      | Error (`Error `Invalid_term_store) ->
        Deferred.return (Error `Invalid_term_store)
      | Error (`Exn _) ->
        Deferred.return (Error `Unknown)

  let stop t =
    Gen_server.stop t
    >>= fun _ ->
    Deferred.unit

  let append_log t entry =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Op (Op.Append_entry (ret, entry)))
    >>=? fun _ ->
    Ivar.read ret
    >>= function
      | Ok result            -> Deferred.return (Ok result)
      | Error `Not_master    -> Deferred.return (Error `Not_master)
      | Error `Append_failed -> Deferred.return (Error `Append_failed)
      | Error `Invalid_log   -> Deferred.return (Error `Invalid_log)

  let send_with_ret t ret msg =
    Gen_server.send t msg
    >>=? fun _ ->
    Ivar.read ret
    >>= fun result ->
    Deferred.return (Ok result)

  let me t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get (Msg.Get_me ret))

  let nodes t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get (Msg.Get_nodes ret))

  let current_term t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get (Msg.Get_current_term ret))

  let voted_for t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get (Msg.Get_voted_for ret))

  let leader t =
    let ret = Ivar.create () in
    send_with_ret t ret (Msg.Get (Msg.Get_leader ret))
end
