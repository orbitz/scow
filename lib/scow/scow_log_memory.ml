open Core.Std
open Async.Std

module type ELT = sig
  type t
  val compare : t -> t -> int
end

module Make = functor (Elt : ELT) -> struct
  type elt = Elt.t

  module Log_map = Map.Make(
    struct
      type t          = Scow_log_index.t
      let compare     = Scow_log_index.compare
      let t_of_sexp _ = failwith "nyi"
      let sexp_of_t _ = failwith "nyi"
    end)

  type t = { mutable log      : (Scow_term.t * elt) Log_map.t
           ; mutable next_idx : Scow_log_index.t
           }

  let create () =
    { log = Log_map.empty; next_idx = Scow_log_index.succ (Scow_log_index.zero ()) }

  let append t elts =
    let (log, next_idx) =
      List.fold_left
        ~f:(fun (map, next_idx) elt ->
          let m = Map.add ~key:next_idx ~data:elt map in
          (m, Scow_log_index.succ next_idx))
        ~init:(t.log, t.next_idx)
        elts
    in
    t.log <- log;
    t.next_idx <- next_idx;
    Deferred.return (Ok (Scow_log_index.pred next_idx))

  let get_entry t log_index =
    match Map.find t.log log_index with
      | Some data ->
        Deferred.return (Ok data)
      | None ->
        Deferred.return (Error (`Not_found log_index))

  let get_term t log_index =
    if Scow_log_index.compare log_index (Scow_log_index.zero ()) = 0 then
      Deferred.return (Ok (Scow_term.zero ()))
    else begin
      get_entry t log_index
      >>=? fun (term, _) ->
      Deferred.return (Ok term)
    end

  let get_log_index_range t =
    let min = Scow_log_index.zero () in
    let max = Scow_log_index.pred t.next_idx in
    Deferred.return (Ok (min, max))

  let rec do_delete_from_log_index t log_index =
    t.log <- Map.remove t.log log_index;
    match Map.next_key t.log log_index with
      | Some (next_idx, _) ->
        do_delete_from_log_index t next_idx
      | None ->
        Deferred.return (Ok ())

  let delete_from_log_index t log_index =
    t.next_idx <- log_index;
    do_delete_from_log_index t log_index

  let is_elt_equal elt1 elt2 = Elt.compare elt1 elt2 = 0
end
