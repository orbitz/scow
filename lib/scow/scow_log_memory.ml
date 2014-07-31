open Core.Std
open Async.Std

type elt = Scow_transport_memory.elt

module Log_map = Map.Make(
  struct
    type t = Scow_log_index.t
    let compare = Scow_log_index.compare
    let t_of_sexp = failwith "nyi"
    let sexp_of_t = failwith "nyi"
  end)

type t = { mutable log      : (Scow_term.t * elt) Log_map.t
         ; mutable next_idx : Scow_log_index.t
         }

let create () =
  { log = Log_map.empty; next_idx = Scow_log_index.zero () }

let append t term elts =
  let (log, next_idx) =
    List.fold_left
      ~f:(fun (map, next_idx) elt ->
        let m = Map.add ~key:next_idx ~data:(term, elt) map in
        (m, Scow_log_index.succ next_idx))
      ~init:(t.log, t.next_idx)
      elts
  in
  t.log <- log;
  t.next_idx <- next_idx;
  Deferred.return (Ok next_idx)

let get_entry t log_index =
  match Map.find t.log log_index with
    | Some data ->
      Deferred.return (Ok data)
    | None ->
      Deferred.return (Error `Not_found)

let get_term t log_index =
  get_entry t log_index
  >>=? fun (term, _) ->
  Deferred.return (Ok term)

let get_log_index_range t =
  match (Map.min_elt t.log, Map.max_elt t.log) with
    | (Some (low, _), Some (high, _)) ->
      Deferred.return (Ok (low, high))
    | _ ->
      Deferred.return (Ok (Scow_log_index.zero (), Scow_log_index.zero ()))

let rec delete_from_log_index t log_index =
  t.log <- Map.remove t.log log_index;
  match Map.next_key t.log log_index with
    | Some (next_idx, _) ->
      delete_from_log_index t next_idx
    | None ->
      Deferred.return (Ok ())

let is_elt_equal elt1 elt2 = failwith "nyi"
