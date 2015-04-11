open Core.Std
open Async.Std

module Make = functor (Transport : Scow_transport.S) -> struct
  module Node = Transport.Node

  type ctx = Transport.ctx
  type elt = Transport.elt

  type t = { faultyness             : int
           ; duration               : Time.Span.t
           ; transport              : Transport.t
           ; mutable partition_over : unit Ivar.t
           ; me                     : Transport.Node.t
           }

  let rec partitioner t =
    let next_partition = Random.int t.faultyness in
    let sleep = Float.of_int next_partition in
    after (sec sleep)
    >>| fun () ->
    printf "%s: Partitioning\n%!" (Transport.Node.to_string t.me);
    let partition_over = Ivar.create () in
    t.partition_over <- partition_over;
    after t.duration
    >>| fun () ->
    printf "%s: Partition over\n%!" (Transport.Node.to_string t.me);
    Ivar.fill partition_over ();
    ignore (partitioner t)

  let rec dev_null partition_over transport =
    if Ivar.is_full partition_over then
      Deferred.unit
    else begin
      choose
        [ choice (Ivar.read partition_over) (fun () -> `Partition_over)
        ; choice (Transport.listen transport) (fun _ -> `Continue)
        ]
      >>= function
        | `Partition_over -> Deferred.return ()
        | `Continue       -> dev_null partition_over transport
    end

  let create ~faultyness ~duration me transport =
    let partition_over = Ivar.create () in
    Ivar.fill partition_over ();
    let t = { faultyness; duration; me; partition_over; transport } in
    ignore (partitioner t);
    t

  let listen t =
    dev_null t.partition_over t.transport
    >>= fun () ->
    Transport.listen t.transport

  let resp_append_entries t ctx ~term ~success =
    if Ivar.is_full t.partition_over then
      Transport.resp_append_entries t.transport ctx ~term ~success
    else
      Deferred.return (Error `Transport_error)

  let resp_request_vote t ctx ~term ~granted =
    if Ivar.is_full t.partition_over then
      Transport.resp_request_vote t.transport ctx ~term ~granted
    else
      Deferred.return (Error `Transport_error)

  let request_vote t node request_vote =
    if Ivar.is_full t.partition_over then
      Transport.request_vote t.transport node request_vote
    else
      Deferred.return (Error `Transport_error)

  let append_entries t node append_entries =
    if Ivar.is_full t.partition_over then
      Transport.append_entries t.transport node append_entries
    else
      Deferred.return (Error `Transport_error)
end
