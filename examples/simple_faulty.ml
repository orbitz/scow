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
module Memory_transport = Scow_transport_memory.Make(Elt)
module Transport = Scow_transport_faulty.Make(Memory_transport)
module Store = Scow_store_memory.Make(Transport.Node)

module Scow = Scow.Make(Statem)(Log)(Store)(Transport)

let create_scow router nodes me =
  let memory_transport = Memory_transport.create me router in
  let transport = Transport.create (Int.of_string Sys.argv.(2)) memory_transport in
  let log = Log.create () in
  let store = Store.create () in
  let statem = () in
  let module Ia = Scow.Init_args in
  let init_args = { Ia.me           = me
                  ;    nodes        = nodes
                  ;    statem       = statem
                  ;    transport    = transport
                  ;    log          = log
                  ;    store        = store
                  ;    timeout      = sec 1.0
                  ;    timeout_rand = sec 2.0
                  }
  in
  Scow.start init_args
  >>= function
    | Ok scow -> Deferred.return scow
    | Error _ -> failwith "nyi"

let print_leader me term = function
  | Some leader ->
    printf "%s: Leader - %s %d\n%!" me leader (Scow_term.to_int term)
  | None ->
    printf "%s: No leader %d\n%!" me (Scow_term.to_int term)

let print_leader_info scows () =
  let print scow =
    Scow.me scow
    >>=? fun me ->
    Scow.current_term scow
    >>=? fun term ->
    Scow.leader scow
    >>= function
      | Ok result -> begin
        print_leader me term result;
        Deferred.return (Ok ())
      end
      | Error `Closed -> begin
        printf "%s: Closed\n%!" me;
        Deferred.return (Ok ())
      end
  in
  Deferred.List.iter
    ~f:(fun scow ->
      print scow
      >>= fun _ ->
      Deferred.unit)
    scows
  >>= fun _ ->
  printf "---\n";
  Deferred.unit

let rec create_nodes router = function
  | 0 -> []
  | n -> Memory_transport.Router.add_node router :: create_nodes router (n - 1)

let main () =
  let router = Memory_transport.Router.create () in
  let nodes  = create_nodes router (Int.of_string Sys.argv.(1)) in
  Deferred.List.map
    ~f:(create_scow router nodes)
    nodes
  >>| fun scows ->
  every
    (sec 5.0)
    (Fn.compose ignore (print_leader_info scows))

let () =
  Random.self_init ();
  ignore (main ());
  never_returns (Scheduler.go ());
