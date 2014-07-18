open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Vote_store : Scow_vote_store.S) ->
        functor (Transport : Scow_transport.S) ->
struct
  module Msg = Scow_server_msg.Make(Log)(Transport)

  type msg = Msg.t
  type op  = Msg.op

  type 's handler =
      msg Gen_server.t ->
      's ->
      op ->
      ('s, unit) Deferred.Result.t

  module States = struct
    type 's t = { follower  : 's handler
                ; candidate : 's handler
                ; leader    : 's handler
                }
  end

  type t = { me              : Transport.Node.t
           ; nodes           : Transport.Node.t list
           ; statem          : Statem.t
           ; transport       : Transport.t
           ; log             : Log.t
           ; vote_store      : Vote_store.t
           ; max_par_repl    : int
           ; current_term    : Scow_term.t
           ; commit_idx      : Scow_log_index.t
           ; last_applied    : Scow_log_index.t
           ; leader          : Transport.Node.t option
           ; voted_for       : Transport.Node.t option
           ; votes_for_me    : Transport.Node.t list
           ; handler         : t handler
           ; states          : t States.t
           ; election_timer  : Scow_timer.t option
           ; heartbeat_timer : Scow_timer.t option
           ; timeout         : Time.Span.t
           ; timeout_rand    : Time.Span.t
           }

  let create_timer timeout server =
    let f =
      fun () ->
        ignore (Gen_server.send server (Msg.Op Msg.Election_timeout))
    in
    Scow_timer.create timeout f

  let set_heartbeat_timeout server t =
    { t with heartbeat_timer = Some (create_timer t.timeout server) }

  let set_election_timeout server t =
    let rand_diff = Random.float (Time.Span.to_ms t.timeout_rand) in
    let timeout   = Time.Span.of_ms (Time.Span.to_ms t.timeout +. rand_diff) in
    { t with election_timer = Some (create_timer timeout server) }

  let cancel_timeout timer_opt =
    match timer_opt with
      | Some timer -> begin
        Scow_timer.cancel timer;
        ()
      end
      | None ->
        ()

  let cancel_election_timeout t =
    cancel_timeout t.election_timer;
    { t with election_timer = None }

  let cancel_heartbeat_timeout t =
    cancel_timeout t.heartbeat_timer;
    { t with heartbeat_timer = None }

  let set_state_follower t =
    { t with handler = t.states.States.follower }

  let set_state_candidate t =
    { t with handler = t.states.States.candidate }

  let set_state_leader t =
    { t with handler = t.states.States.leader }

  let record_vote node t =
    { t with votes_for_me = node::t.votes_for_me }

  let count_votes t =
    List.length
      (List.dedup ~compare:Transport.Node.compare t.votes_for_me)

  let clear_votes t = { t with votes_for_me = [] }
end
