module Term : sig
  type t

  val zero      : t
  val succ      : t -> t
  val compare   : t -> t -> int
  val of_string : string -> t
  val to_string : t -> string
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

module Make : functor (Server : SERVER) -> functor (Log : LOG) -> sig
  type 'a t
  type error            = [ `Bad_term | `Bad_previous ]

  val create            : me:Server.t -> log:Log.t -> Server.t list -> [ `Follower ] t

  val servers           : 'a t -> Server.t list

  val current_term      : 'a t -> Term.t
  val voted_for         : 'a t -> Server.t option

  val log               : 'a t -> Log.t
  val set_log           : Log.t -> 'a t -> 'a t

  val leader            : 'a t -> Server.t option

  (*
   * Triggered when we are a follower and have not heard from the leader
   * in some timeout period
   *)
  val heartbeat_timeout : [ `Follower] t -> [ `Candidate ] t

  (*
   * This set of functions are used during a voting round
   *
   * request_vote - A server is requesting a vote from us.  Returns [t option]
   * if it is granted and None if it is not
   *
   * receive_vote - A vote has been requested and received from the server
   *
   * is_now_leader - Tests if the current state machine can become a leader.
   * If so, return it.
   *)
  val election_timeout  : [ `Candidate ] t -> [ `Candidate ] t
  val request_vote      : Server.t -> [ `Follower ] t -> [ `Follower ] t option
  val receive_vote      : Server.t -> [ `Candidate ] t -> [ `Candidate ] t
  val is_now_leader     : [ `Candidate ] t -> [ `Leader ] t option

  val receive_log       : Log.t -> [ `Candidate | `Follower ] t -> ([ `Follower ] t, [> error ]) Core.Result.t
  val append_log        : Log.t -> [ `Leader ] t -> [ `Leader ] t

end
