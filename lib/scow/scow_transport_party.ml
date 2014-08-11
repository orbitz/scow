open Core.Std
open Async.Std

module Make = functor (Transport : Scow_transport.S) -> struct
  module Node = Transport.Node

  type ctx = Transport.ctx
  type elt = Transport.elt

  type t = { faultyness : int
           ; duration   : Time.Span.t
           ; transport  : Transport.t
           }

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

  let create faultyness duration transport =
    { faultyness; duration; transport }

  let listen t =
    if Random.int 1000 < t.faultyness then begin
      let partition_over = Ivar.create () in
      ignore (after t.duration >>| fun () -> Ivar.fill partition_over ());
      dev_null partition_over t.transport
      >>= fun () ->
      Transport.listen t.transport
    end
    else
      Transport.listen t.transport


  let resp_append_entries t =
    Transport.resp_append_entries t.transport

  let resp_request_vote t =
    Transport.resp_request_vote t.transport

  let request_vote t node request_vote =
    Transport.request_vote t.transport node request_vote

  let append_entries t node append_entries =
    Transport.append_entries t.transport node append_entries
end
