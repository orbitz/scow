open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
struct
  module Msg = Scow_server_msg.Make(Statem)(Log)(Transport)

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

  module Append_entry = struct
    type errors = [ `Not_master | `Append_failed | `Invalid_log ]
    type ret = (Statem.ret, errors) Result.t
    type t = { log_index : Scow_log_index.t
             ; op        : Statem.op
             ; ret       : ret Ivar.t
             }
  end

  module Node_map = Map.Make(
    struct
      type t        = Transport.Node.t
      let compare   = Transport.Node.compare
      let t_of_sexp = failwith "nyi"
      let sexp_of_t = failwith "nyi"
    end)

  type t = { me              : Transport.Node.t
           ; nodes           : Transport.Node.t list
           ; statem          : Statem.t
           ; transport       : Transport.t
           ; log             : Log.t
           ; store           : Store.t
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
           ; next_idx        : Scow_log_index.t Node_map.t
           ; match_idx       : Scow_log_index.t Node_map.t
           ; append_entries  : Append_entry.t list
           }

  module Init_args = struct
    type t_ = { me           : Transport.Node.t
              ; nodes        : Transport.Node.t list
              ; statem       : Statem.t
              ; transport    : Transport.t
              ; log          : Log.t
              ; store        : Store.t
              ; max_par_repl : int
              ; timeout      : Time.Span.t
              ; timeout_rand : Time.Span.t
              ; follower     : t handler
              ; candidate    : t handler
              ; leader       : t handler
              }

    type t = t_
  end

  let create init_args =
    let module Ia = Init_args in
    Store.load init_args.Ia.store
    >>=? fun voted_for ->
    Deferred.return
      (Ok { me              = init_args.Ia.me
          ; nodes           = init_args.Ia.nodes
          ; statem          = init_args.Ia.statem
          ; transport       = init_args.Ia.transport
          ; log             = init_args.Ia.log
          ; store           = init_args.Ia.store
          ; max_par_repl    = init_args.Ia.max_par_repl
          ; current_term    = Scow_term.zero ()
          ; commit_idx      = Scow_log_index.zero ()
          ; last_applied    = Scow_log_index.zero ()
          ; leader          = None
          ; voted_for       = voted_for
          ; votes_for_me    = []
          ; handler         = init_args.Ia.follower
          ; states          = { States.follower  = init_args.Ia.follower
                              ;        candidate = init_args.Ia.candidate
                              ;        leader    = init_args.Ia.leader
                              }
          ; election_timer  = None
          ; heartbeat_timer = None
          ; timeout         = init_args.Ia.timeout
          ; timeout_rand    = init_args.Ia.timeout_rand
          ; next_idx        = Node_map.empty
          ; match_idx       = Node_map.empty
          ; append_entries  = []
          })

  let handler t = t.handler

  let me t = t.me
  let nodes t = t.nodes

  let current_term t = t.current_term
  let set_current_term term t = { t with current_term = term }

  let transport t = t.transport
  let log t = t.log
  let store t = t.store
  let statem t = t.statem

  let commit_idx t = t.commit_idx
  let set_commit_idx commit_idx t = { t with commit_idx }

  let update_commit_index t =
    let majority = List.length (nodes t) / 2 + 1 in
    let highest_committed_majority =
      Node_map.data t.match_idx
      |> List.sort ~cmp:Scow_log_index.compare
      |> List.rev
      |> Fn.flip List.take majority
      |> List.rev
    in
    match highest_committed_majority with
      | [] -> t
      | (commit_idx::_) as all when List.length all = majority ->
        { t with commit_idx }
      | _ ->
        (* In this case, match_idx is not full of values yet *)
        t

  let max_par t = t.max_par_repl

  let voted_for t = t.voted_for
  let set_voted_for voted_for t = { t with voted_for }

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

  let add_append_entry append_entry t =
    { t with append_entries = append_entry::t.append_entries }

  let remove_append_entries log_index t =
    let gt ae =
      Scow_log_index.compare ae.Append_entry.log_index log_index <= 0
    in
    match List.filter ~f:gt t.append_entries with
      | [] ->
        ([], t)
      | aes ->
        let append_entries =
          List.filter ~f:(Fn.compose not gt) t.append_entries
        in
        (aes, { t with append_entries })

  let remove_all_append_entries t =
    (t.append_entries, { t with append_entries = [] })

  let next_idx node t =
    Map.find t.next_idx node

  let set_next_idx node log_index t =
    { t with next_idx = Map.add ~key:node ~data:log_index t.next_idx }

  let clear_next_idx t =
    { t with next_idx = Node_map.empty }

  let match_idx node t =
    Map.find t.match_idx node

  let set_match_idx node log_index t =
    { t with match_idx = Map.add ~key:node ~data:log_index t.match_idx }

  let clear_match_idx t =
    { t with match_idx = Node_map.empty }
end
