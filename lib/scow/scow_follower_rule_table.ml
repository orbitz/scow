module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
struct
  module RState = Scow_replication_state.Make(Statem)(Log)(Store)(Transport)
  module Msg    = Scow_replication_msg.Make(Statem)(Log)(Transport)
  module Rpc    = Scow_transport.Msg

  type state = RState.t
  type msg   = Msg.t

  module O = struct
    let (&&) l r e = l e && r e
    let (||) l r e = l e || r e
    let not c e    = not (c e)
  end

  let is_heartbeat event =
    event.Event.event = Msg.Heartbeat

  let is_election_timeout event =
    event.Event.event = Msg.Election_timeout

  let is_append_entries event =
    match event.Event.event with
      | Msg.Append_entry _ -> true
      | _                  -> false

  let is_rpc_request_vote event =
    match event.Event.event with
      | Msg.Rpc (Rpc.Request_vote _) -> true
      | _                            -> false

  let is_rpc_request_vote_can =
    O.(is_rpc_request_vote &&
         (has_not_voted || vote_for_same_node))

  let is_rpc_request_vote_cannot =
    O.(is_rpc_request_vote && not is_rpc_request_vote_can)

  let is_rpc_append_entries event =
    match event.Event.event with
      | Msg.Rpc (Rpc.Append_entries _) -> true
      | _                              -> false

  let has_empty_leader event =
    None = RState.leader event.Event.state

  let is_rpc event =
    match event.Event.event with
      | Msg.Rpc _ -> true
      | _         -> false

  let can_set_leader =
    O.(is_rpc && has_empty_leader)

  let set_election_timeout event =
    let state = RState.set_election_timeout event.Event.state in
    Deferred.return state

  let begin_votes event =
    failwith "nyi"

  let reply_append_entries event =
    failwith "nyi"

  let give_vote event =
    failwith "nyi"

  let do_not_give_vote event =
    failwith "nyi"

  let apply_rpc_append_entries event =
    failwith "nyi"

  let set_leader event =
    let node  = event.Event.event in
    let state = RState.set_leader node event.Event.state in
    Deferred.return state

  let reset_timeout event =
    let state =
      event.Event.state
      |> RState.cancel_election_timeout
      |> RState.cancel_heartbeat_timeout
      |> RState.set_heartbeat_timeout
    in
    Deferred.return state

  let rpc_request_vote_table =
    Event_engine.create
      [ (can_grant_vote,    grant_vote)
      ; (cannot_grant_vote, do_not_grant_vote)
      ]

  let table () =
    [ (is_heartbeat,          set_election_timeout)
    ; (can_set_leader,        set_leader)
    ; (is_rpc,                reset_timeout)
    ; (is_election_timeout,   become_candidate)
    ; (is_append_entries,     reply_append_entries)
    ; (is_rpc_request_vote,   Event_engine.run rpc_request_vote_table)
    ; (is_rpc_append_entries, rpc_append_entries_table)
    ]
end

