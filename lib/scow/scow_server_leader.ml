open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S
                 with type Node.t = Vote_store.node
                 and  type elt    = Log.elt) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg = Scow_server_msg.Make(Statem)(Log)(Transport)
  module TMsg = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let ignore_error deferred =
    deferred
    >>= function
      | Ok anything -> Deferred.return (Ok anything)
      | Error _     -> Deferred.return (Error ())

  let is_node_equal n1 n2 = Transport.Node.compare n1 n2 = 0

  let rec send_until' max_par f nodes test in_flight remaining_nodes results =
    match (in_flight, remaining_nodes) with
      | ([], []) ->
        Deferred.return (results, [])
      | (in_flight, n::ns) when List.length in_flight < max_par ->
        let missing_amount = max_par - List.length in_flight in
        let additional =
          List.map
            ~f:(fun n -> (n, f n >>= fun result -> Deferred.return (n, result)))
            (List.take remaining_nodes missing_amount)
        in
        let remaining_nodes = List.drop remaining_nodes missing_amount in
        let in_flight = in_flight @ additional in
        send_until' max_par f nodes test in_flight remaining_nodes results
      | (in_flight, remaining_nodes) ->
        Deferred.any (List.map ~f:snd in_flight)
        >>= fun (node, result) ->
        let in_flight = List.Assoc.remove ~equal:is_node_equal in_flight node in
        let results = (node, result)::results in
        if test results then
          Deferred.return (results, in_flight)
        else
          send_until' max_par f nodes test in_flight remaining_nodes results

  let send_until ~max_par ~f ~nodes ~test =
    send_until' max_par f nodes test [] nodes []

  let send_majority_sync max_par f nodes =
    let test results =
      let length =
        results
        |> List.map ~f:snd
        |> List.filter ~f:Result.is_ok
        |> List.length
      in
      length > List.length nodes / 2
    in
    send_until ~max_par ~f ~nodes ~test
    >>= fun (results, in_flight) ->
    let (success, failed) =
      List.partition_tf
        ~f:(Fn.compose Result.is_ok snd)
        results
    in
    Deferred.return (success, failed, in_flight)

  let send_append_entries state entries =
    let append_entries_rpc node =
      let append_entries =
        Scow_rpc.Append_entries.(
          { term = State.current_term state
          ; prev_log_index = failwith "nyi"
          ; prev_log_term  = failwith "nyi"
          ; leader_commit  = State.commit_idx state
          ; entries        = entries
          })
      in
      Transport.append_entries
        (State.transport state)
        node
        append_entries
    in
    send_majority_sync (State.max_par state) append_entries_rpc (State.nodes state)
    >>= function
      | (success, [], in_flight) ->
        failwith "nyi"
      | ([], failed, []) ->
        (* Everything failed *)
        failwith "nyi"
      | (success, failed, in_flight) ->
        failwith "nyi"


  let handle_rpc_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare (State.current_term state) append_entries.Ae.term < 0 then
      let state =
        state
        |> State.set_state_follower
        |> State.set_heartbeat_timeout self
      in
      State.handler
        state
        self
        state
        (Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx))
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

  let handle_append_entries self state ret entries =
    (* send_append_entries state entries *)
    failwith "nyi"

  let handle_timeout self state =
    failwith "nyi"

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      ignore_error (handle_rpc_append_entries self state (node, append_entries, ctx))
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      ignore_error (handle_rpc_request_vote self state (node, request_vote, ctx))
    | Msg.Append_entry (ret, entry) ->
      ignore_error (handle_append_entries self state ret entry)
    | Msg.Election_timeout
    | Msg.Heartbeat ->
      ignore_error (handle_timeout self state)
    | Msg.Received_vote _ ->
      Deferred.return (Ok state)
    | Msg.Append_entries_resp _ ->
      failwith "nyi"
end

