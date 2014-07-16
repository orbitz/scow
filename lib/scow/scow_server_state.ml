open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T) ->
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

  type t = { me           : Transport.Node.t
           ; nodes        : Transport.Node.t list
           ; statem       : Statem.t
           ; transport    : Transport.t
           ; log          : Log.t
           ; vote_store   : Vote_store.t
           ; max_par_repl : int
           ; current_term : Scow_term.t
           ; commit_idx   : Scow_log_index.t
           ; last_applied : Scow_log_index.t
           ; leader       : Transport.Node.t option
           ; voted_for    : Transport.Node.t option
           ; handler      : t handler
           ; states       : t States.t
           ; timer        : Scow_timer.t option
           ; timeout      : Time.Span.t
           ; timeout_rand : Time.Span.t
           }

  let create_timer timeout server t =
    let f =
      fun () ->
        ignore (Gen_server.send server (Msg.Op Msg.Election_timeout))
    in
    { t with timer = Some (Scow_timer.create timeout f) }

  let set_timeout server t =
    create_timer t.timeout server t

  let set_random_timeout server t =
    let rand_diff = Random.float (Time.Span.to_ms t.timeout_rand) in
    let timeout   = Time.Span.of_ms (Time.Span.to_ms t.timeout +. rand_diff) in
    create_timer timeout server t

  let cancel_timeout t =
    match t.timer with
      | Some timer -> begin
        Scow_timer.cancel timer;
        { t with timer = None }
      end
      | None ->
        t

  let set_state_follower t =
    { t with handler = t.states.States.follower }

  let set_state_candidate t =
    { t with handler = t.states.States.candidate }

  let set_state_leader t =
    { t with handler = t.states.States.leader }
end
