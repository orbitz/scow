open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Vote_store.node) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg   = Scow_server_msg.Make(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare (State.current_term state) append_entries.Ae.term <= 0 then begin
      let state = State.set_state_follower state in
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
    end
    else begin
      Transport.resp_append_entries
        (State.transport state)
        ctx
        ~term:(State.current_term state)
        ~success:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_rpc_request_vote self state (node, request_vote, ctx) =
    let module Rv = Scow_rpc.Request_vote in
    if Scow_term.compare (State.current_term state) request_vote.Rv.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.cancel_election_timeout
        |> State.cancel_heartbeat_timeout
      in
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx))
    else begin
      Transport.resp_request_vote
        (State.transport state)
        ctx
        ~term:(State.current_term state)
        ~granted:false
      >>= fun _ ->
      Deferred.return (Ok state)
    end

  let handle_append_entries _self state ret =
    Ivar.fill ret (Error `Not_master);
    Deferred.return (Ok state)

  let transition_to_leader self state =
    (*
     * Turn self into leader and then kick off a
     * heartbeat to tell the world
     *)
    let state =
      state
      |> State.set_state_leader
      |> State.cancel_election_timeout
      |> State.cancel_heartbeat_timeout
    in
    State.handler
      state
      self
      state
      Msg.Heartbeat

  let handle_received_yes_vote self state node =
    let state = State.record_vote node state in
    if State.count_votes state > (List.length (State.nodes state) / 2) then begin
      transition_to_leader self state
    end
    else
      Deferred.return (Ok state)

  let handle_received_no_vote _self state term =
    (* Unclear what to do in this case *)
    Deferred.return (Ok state)

  let handle_heartbeat_timeout self state =
    state
    |> State.set_state_follower
    |> State.clear_votes
    |> State.set_election_timeout self
    |> Result.return
    |> Deferred.return

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, request_vote, ctx))
    | Msg.Append_entries (ret, _) ->
      ignore_error (handle_append_entries self state ret)
    | Msg.Received_vote (node, _term, true) ->
      ignore_error (handle_received_yes_vote self state node)
    | Msg.Received_vote (_node, term, false) ->
      ignore_error (handle_received_no_vote self state term)
    | Msg.Election_timeout ->
      Deferred.return (Ok state)
    | Msg.Heartbeat ->
      ignore_error (handle_heartbeat_timeout self state)

end

