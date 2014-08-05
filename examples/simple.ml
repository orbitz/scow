open Core.Std
open Async.Std

module Elt = struct
  type t = int
  let compare = Int.compare
end

module Statem = struct
  type op = Elt.t
  type ret = unit
  type t = unit

  let apply () op = Deferred.unit
end

module Log = Scow_log_memory.Make(Elt)
module Transport = Scow_transport_memory.Make(Elt)
module Store = Scow_store_memory.Make(Transport.Node)

module Scow = Scow.Make(Statem)(Log)(Store)(Transport)

let create_scow router nodes me =
  let me = Transport.Router.add_node router in
  let transport = Transport.create me router in
  let log = Log.create () in
  let store = Store.create () in
  let statem = () in
  let module Ia = Scow.Init_args in
  let init_args = { Ia.me                       = me
                  ;    nodes                    = nodes
                  ;    statem                   = statem
                  ;    transport                = transport
                  ;    log                      = log
                  ;    store                    = store
                  ;    max_parallel_replication = 3
                  ;    timeout                  = sec 1.0
                  ;    timeout_rand             = sec 10.0
                  }
  in
  Scow.start init_args
  >>= function
    | Ok scow -> Deferred.return scow
    | Error _ -> failwith "nyi"

let main () =
  let router = Transport.Router.create () in
  let node1 = Transport.Router.add_node router in
  let node2 = Transport.Router.add_node router in
  let node3 = Transport.Router.add_node router in
  let nodes = [ node1; node2; node3 ] in
  Deferred.List.map
    ~f:(create_scow router nodes)
    nodes
  >>| fun scows ->
  every
    (sec 1.0)
    (fun () ->
      ignore
        (Deferred.List.iter
           ~f:(fun scow ->
             Scow.leader scow
             >>| function
               | Ok (Some leader) ->
                 printf "Leader - %s\n" leader
               | Ok None ->
                 printf "No leader\n"
               | Error `Closed ->
                 printf "Closed\n")
           scows))

let () =
  ignore (main ());
  never_returns (Scheduler.go ());
