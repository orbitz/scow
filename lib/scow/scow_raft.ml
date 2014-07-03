open Core.Std
open Async.Std

module Term = struct
  type t = int

  let zero () = 0

  let succ t = t + 1

  let pred t = t - 1

  let of_int = function
    | n when n >= 0 -> Some n
    | _             -> None

  let to_int t = t
end

module Log_index = struct
  (* Semantically same thing for now *)
  type t = Term.t

  let zero   = Term.zero
  let succ   = Term.succ
  let pred   = Term.pred
  let of_int = Term.of_int
  let to_int = Term.to_int
end

module Append_entries = struct
  type 'elt t = { term          : Term.t
                ; prev_log_idx  : Log_index.t
                ; prev_log_term : Term.t
                ; leader_commit : Log_index.t
                ; entries       : 'elt list
                }
end

module Request_vote = struct
  type t = { term           : Term.t
           ; last_log_index : Log_index.t
           ; last_log_term  : Term.t
           }
end

module Msg = struct
  type ('n, 'elt) t =
    | Append_entries      of ('n * 'elt Append_entries.t)
    | Resp_append_entries of 'n
    | Request_vote        of ('n * Request_vote.t)
    | Resp_request_vote   of 'n
end

module type TRANSPORT = sig
  type elt
  type t

  module Node : sig
    type t
    val compare : t -> t -> int
  end

  val listen : t -> ((Node.t, elt) Msg.t, unit) Deferred.Result.t

  val resp_append_entries : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t
  val resp_request_vote   : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t

  val request_vote        : t -> Node.t -> (unit, unit) Deferred.Result.t
  val append_entries      : t -> Node.t -> unit -> (unit, unit) Deferred.Result.t
end

module type LOG = sig
  type elt
  type t

  val append_log : t -> 'elt Append_entries.t -> (unit, [> `Append_failed ]) Deferred.Result.t
  val get_entry  : t -> Log_index.t -> ((Term.t * elt), [> `Not_found ]) Deferred.Result.t
end

module type STATEM = sig
  type op
  type t

  val apply : t -> op -> (unit, unit) Deferred.Result.t
end

module Make = functor (Statem : STATEM) -> functor (Log : LOG) -> functor (Transport : TRANSPORT) -> struct
  module Init_args = struct
    type t = { me                       : Transport.Node.t
             ; nodes                    : Transport.Node.t list
             ; statem                   : Statem.t
             ; transport                : Transport.t
             ; log                      : Log.t
             ; max_parallel_replication : int
             }
  end

  module Msg = struct
    type append_entries = (unit, [ `Not_master | `Append_failed ] as 'e) Result.t

    type t =
      | Append_entries   of (append_entries Ivar.t * Log.elt list)
      | Get_nodes        of Transport.Node.t list Ivar.t
      | Get_current_term of Term.t Ivar.t
      | Get_voted_for    of Transport.Node.t option Ivar.t
      | Get_leader       of Transport.Node.t option Ivar.t
  end

  type t = Msg.t Gen_server.t

  module Server = struct
    let init self init_args =
      failwith "nyi"

    let handle_call self state msg =
      failwith "nyi"

    let terminate _reason state =
      failwith "nyi"

    let callbacks = { Gen_server.Server.init; handle_call; terminate }
  end

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

  let nodes t =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Get_nodes ret)
    >>=? fun _ ->
    Ivar.read ret
    >>= fun nodes ->
    Deferred.return (Ok nodes)

  let current_term t =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Get_current_term ret)
    >>=? fun _ ->
    Ivar.read ret
    >>= fun current_term ->
    Deferred.return (Ok current_term)

  let voted_for t =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Get_voted_for ret)
    >>=? fun _ ->
    Ivar.read ret
    >>= fun voted_for ->
    Deferred.return (Ok voted_for)

  let leader t =
    let ret = Ivar.create () in
    Gen_server.send t (Msg.Get_leader ret)
    >>=? fun _ ->
    Ivar.read ret
    >>= fun leader ->
    Deferred.return (Ok leader)
end
