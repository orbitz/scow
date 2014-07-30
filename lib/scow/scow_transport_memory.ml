open Core.Std
open Async.Std

type ctx = unit
type elt = unit
type t = unit

module Node = struct
  type t = unit
  let compare = failwith "nyi"
end

let listen t = failwith "nyi"

let resp_append_entries t ctx ~term ~success = failwith "nyi"

let resp_request_vote t ctx ~term ~granted = failwith "nyi"

let request_vote t node request_vote = failwith "nyi"

let append_entries t node entries = failwith "nyi"
