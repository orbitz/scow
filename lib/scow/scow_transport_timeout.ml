open Core.Std
open Async.Std

module Make = functor (Transport : Scow_transport.S) -> struct
  module Node = Transport.Node

  type ctx = Transport.ctx
  type elt = Transport.elt

  type t = { timeout   : Time.Span.t
           ; transport : Transport.t
           }

  let with_timeout timeout work =
    let timeout =
      choice
        (after timeout)
        (fun () -> `Timeout)
    in
    let work =
      choice
        work
        (fun result -> `Ok result)
    in
    choose [timeout; work]
    >>= function
      | `Ok result -> Deferred.return result
      | `Timeout   -> Deferred.return (Error `Transport_error)

  let create timeout transport =
    { timeout; transport }

  let listen t =
    Transport.listen t.transport

  let resp_append_entries t =
    Transport.resp_append_entries t.transport

  let resp_request_vote t =
    Transport.resp_request_vote t.transport

  let request_vote t node request_vote =
    with_timeout
      t.timeout
      (Transport.request_vote t.transport node request_vote)

  let append_entries t node append_entries =
    with_timeout
      t.timeout
      (Transport.append_entries t.transport node append_entries)
end
