open Core.Std
open Async.Std

module Make = functor (Transport : Scow_transport.S) -> struct
  module Node = Transport.Node

  type ctx = Transport.ctx
  type elt = Transport.elt

  type t = { faultyness : int
           ; transport  : Transport.t
           }

  let create faultyness transport =
    { faultyness; transport }

  let listen t =
    Transport.listen t.transport

  let resp_append_entries t =
    Transport.resp_append_entries t.transport

  let resp_request_vote t =
    Transport.resp_request_vote t.transport

  let request_vote t node request_vote =
    if Random.int 1000 < t.faultyness then
      Deferred.return (Error `Transport_error)
    else
      Transport.request_vote t.transport node request_vote

  let append_entries t node append_entries =
    if Random.int 1000 < t.faultyness then
      Deferred.return (Error `Transport_error)
    else
      Transport.append_entries t.transport node append_entries
end
