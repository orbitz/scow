open Async.Std
open Core.Std

module Event = Event_engine.Event

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
struct
  module RState = Scow_replication_state.Make(Statem)(Log)(Store)(Transport)
  module Msg    = Scow_replication_msg.Make(Statem)(Log)(Transport)
  module Rpc    = Scow_transport.Msg

  module Follower = Scow_follower_rule_table.Make(Statem)(Log)(Store)(Transport)

  type state = RState.t
  type msg   = Msg.t

  let leader_rule_table =
    Event_engine.create
      []

  let candidate_rule_table =
    Event_engine.create
      []

  let follower_rule_table =
    Event_engine.create
      (Follower.table ())

  let get_term_from_append_entries append_entries =
    append_entries.Scow_rpc.Append_entries.term

  let get_term_from_request_vote request_vote =
    request_vote.Scow_rpc.Request_vote.term

  let get_term_from_event = function
    | Msg.Rpc (Rpc.Append_entries (_node, append_entries), _ctx) ->
      Some (get_term_from_append_entries append_entries)
    | Msg.Rpc (Rpc.Request_vote (_node, request_vote), _ctx) ->
      Some (get_term_from_request_vote request_vote)
    | Msg.Received_vote (_node, term, _granted) ->
      Some term
    | Msg.Append_entries_resp (_node, _log_idx, Ok (term, _success)) ->
      Some term
    | Msg.Append_entries_resp _
    | Msg.Election_timeout
    | Msg.Heartbeat
    | Msg.Append_entry _ ->
      None

  let rpc_in_future event =
    let rstate = event.Event.state in
    let current_term = RState.current_term rstate in
    let term_opt = get_term_from_event event.Event.event in
    Option.value_map
      ~default:false
      ~f:(fun term -> Scow_term.compare current_term term >= 0)
      term_opt

  let become_follower_with_term event =
    let term_opt = get_term_from_event event.Event.event in
    let current_term =
      Option.value
        ~default:(RState.current_term event.Event.state)
        term_opt
    in
    let rstate =
      event.Event.state
      |> RState.set_role RState.Role.Follower
      |> RState.set_current_term current_term
    in
    Store.store_vote (RState.store event.Event.state) None
    >>= function
      | Ok () ->
        Deferred.return rstate
      | Error _ ->
        failwith "nyi - become_follower_with_term"

  let is_leader event =
    let rstate = event.Event.state in
    RState.Role.Follower = RState.role rstate

  let is_candidate event =
    let rstate = event.Event.state in
    RState.Role.Candidate = RState.role rstate

  let is_follower event =
    let rstate = event.Event.state in
    RState.Role.Follower = RState.role rstate

  let table () =
    [ (rpc_in_future, become_follower_with_term)
    ; (is_leader,     Event_engine.run leader_rule_table)
    ; (is_candidate,  Event_engine.run candidate_rule_table)
    ; (is_follower,   Event_engine.run follower_rule_table)
    ]
end


