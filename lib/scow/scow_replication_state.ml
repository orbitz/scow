open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
struct
  module Append_entry = struct
    type errors = [ `Not_master | `Append_failed | `Invalid_log ]
    type ret = (Statem.ret, errors) Result.t
    type t = { log_index : Scow_log_index.t
             ; ret       : ret Ivar.t
             }
  end

  module Role = struct
    type t =
      | Leader
      | Candidate
      | Follower
  end

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

  module Node_map = Map.Make(
    struct
      type t          = Transport.Node.t
      let compare     = Transport.Node.compare
      let t_of_sexp _ = failwith "nyi"
      let sexp_of_t _ = failwith "nyi"
    end)

  type t = { me              : Transport.Node.t
           ; nodes           : Transport.Node.t list
           ; statem          : Statem.t
           ; transport       : Transport.t
           ; log             : Log.t
           ; store           : Store.t
           ; role            : Role.t
           ; current_term    : Scow_term.t
           ; commit_idx      : Scow_log_index.t
           ; last_applied    : Scow_log_index.t
           ; leader          : Transport.Node.t option
           ; votes_for_me    : Transport.Node.t list
           ; election_timer  : Scow_timer.t option
           ; heartbeat_timer : Scow_timer.t option
           ; timeout         : Time.Span.t
           ; timeout_rand    : Time.Span.t
           ; notify          : Scow_notify.t
           ; next_idx        : Scow_log_index.t Node_map.t
           ; match_idx       : Scow_log_index.t Node_map.t
           ; append_entries  : (Scow_log_index.t * Append_entry.t) list
           }

  type create_err = [ `Invalid_term_store ]

  let create init_args =
    let module Ia = Init_args in
    Store.load_term init_args.Ia.store
    >>=? fun current_term_opt ->
    let current_term = Option.value current_term_opt ~default:(Scow_term.zero ()) in
    let nodes =
      List.filter
        ~f:(fun n -> Transport.Node.compare n init_args.Ia.me <> 0)
        init_args.Ia.nodes
    in
    Deferred.return
      (Ok { me              = init_args.Ia.me
          ; nodes           = nodes
          ; statem          = init_args.Ia.statem
          ; transport       = init_args.Ia.transport
          ; log             = init_args.Ia.log
          ; store           = init_args.Ia.store
          ; role            = Role.Follower
          ; current_term    = current_term
          ; commit_idx      = Scow_log_index.zero ()
          ; last_applied    = Scow_log_index.zero ()
          ; leader          = None
          ; votes_for_me    = []
          ; election_timer  = None
          ; heartbeat_timer = None
          ; timeout         = init_args.Ia.timeout
          ; timeout_rand    = init_args.Ia.timeout_rand
          ; notify          = init_args.Ia.notify
          ; next_idx        = Node_map.empty
          ; match_idx       = Node_map.empty
          ; append_entries  = []
          })

  (* val notify : t -> Scow_notify.t *)

  let me t = t.me
  let nodes t = t.nodes
  let leader t = t.leader
  let set_leader leader t = { t with leader }

  let current_term t = t.current_term

  let role t = t.role
  let set_role role t = { t with role }
end
