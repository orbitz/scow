module Term = struct
  type t = Num.num

  let zero      = Num.num_of_int 0
  let succ      = Num.succ_num
  let compare   = Num.compare_num
  let of_string = Num.num_of_string
  let to_string = Num.string_of_num
end

module type LOG = sig
  type elt
  type t

  val append    : elt -> t -> t option
  val last_term : t -> Term.t
end

module type SERVER = sig
  type t

  val equal : t -> t -> bool
end

module Make = functor (Server : SERVER) -> functor (Log : LOG) -> struct
  type 'a t = { voted_for    : Server.t option
              ; votes        : Server.t list
              ; me           : Server.t
              ; servers      : Server.t list
              ; current_term : Term.t
              ; log          : Log.t
              ; leader       : Server.t option
              }


  type error = [ `Bad_term | `Bad_previous ]

  let create ~me ~log servers =
    { voted_for    = None
    ; votes        = []
    ; me           = me
    ; servers      = servers
    ; current_term = Log.last_term log
    ; log          = log
    ; leader       = None
    }

  let servers t = t.servers

  let current_term t = t.current_term

  let voted_for t = t.voted_for

  let log t = t.log

  let set_log log t = { t with log = log }

  let leader t = t.leader

  let heartbeat_timeout t = { t with voted_for = None; leader = None }

  let election_timeout t = { t with votes = [] }

  let request_vote server = function
    | { voted_for = None } as t -> Some { t with voted_for = Some server }
    | _                         -> None

  let receive_vote server t = { t with votes = server::t.votes }

  let is_now_leader t =
    if List.length t.votes > (List.length t.servers / 2) then
      Some { t with votes = []; leader = Some t.me }
    else
      None


  let receive_log _ _ = failwith "nyi"
  let append_log _ _ = failwith "nyi"

end
