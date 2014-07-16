open Core.Std
open Async.Std

type cancel =
  | Triggered
  | Cancelled

type t = { canceller            : unit Ivar.t
         ; mutable is_cancelled : bool
         }

let create span f =
  let canceller = Ivar.create () in
  let t         = { canceller; is_cancelled = false } in
  let timer =
    choice
      (after span)
      (fun () -> Triggered)
  in
  let cancelled =
    choice
      (Ivar.read canceller)
      (fun () -> Cancelled)
  in
  ignore
    (choose [timer; cancelled]
     >>| function
       | Triggered -> f ()
       | Cancelled -> ());
  t

let cancel t =
  t.is_cancelled <- true;
  Ivar.fill t.canceller ()

let is_cancelled t = t.is_cancelled
