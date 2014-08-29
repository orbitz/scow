open Core.Std
open Async.Std

module Event = struct
  type state =
    | Follower
    | Candidate
    | Leader

  type t =
    | Started
    | State_change of (state * state)
    | Append_entry of (Scow_log_index.t * int)
    | Commit_idx   of Scow_log_index.t
end

type t = Event.t -> unit Deferred.t

let dummy _ = Deferred.unit

