open Core.Std
open Async.Std

module type ELT = sig
  type t
end

module Make = functor (Elt : ELT) -> struct
  module Node = struct
    type t = string
    let compare = String.compare
  end

  type ctx = (Scow_term.t * bool) Ivar.t
  type elt = Elt.t

  module Router = struct
    module Node_map = String.Map
    type msg = ((Node.t, elt) Scow_transport.Msg.t * ctx)

    type t = { mutable router    : (msg Pipe.Reader.t * msg Pipe.Writer.t) Node_map.t
             ; mutable next_node : int
             }

    let create () =
      { router = String.Map.empty; next_node = 0 }

    let add_node t =
      let node = "node-" ^ Int.to_string t.next_node in
      t.next_node <- t.next_node + 1;

      let pipe = Pipe.create () in
      let router = Map.add ~key:node ~data:pipe t.router in
      t.router <- router;
      node

    let route_msg t node msg =
      match Map.find t.router node with
        | Some (_, writer) ->
          Pipe.write_without_pushback writer msg
        | None ->
          ()

    let reader t node =
      match Map.find t.router node with
        | Some (reader, _) -> Some reader
        | None             -> None
  end

  type t = { router : Router.t; self : Node.t }

  let create self router =
    { router; self }

  let listen t =
    match Router.reader t.router t.self with
      | Some reader -> begin
        Pipe.read reader
        >>= function
          | `Ok msg -> Deferred.return (Ok msg)
          | `Eof    -> Deferred.return (Error `Transport_error)
      end
      | None ->
        Deferred.return (Error `Transport_error)

  let resp_append_entries t ctx ~term ~success =
    Ivar.fill ctx (term, success);
    Deferred.return (Ok ())

  let resp_request_vote t ctx ~term ~granted =
    Ivar.fill ctx (term, granted);
    Deferred.return (Ok ())

  let request_vote t node request_vote =
    let ctx = Ivar.create () in
    let msg =
      (Scow_transport.Msg.Request_vote (t.self, request_vote), ctx)
    in
    Router.route_msg
      t.router
      node
      msg;
    Ivar.read ctx
    >>= fun result ->
    Deferred.return (Ok result)

  let append_entries t node append_entries =
    let ctx = Ivar.create () in
    let msg =
      (Scow_transport.Msg.Append_entries (t.self, append_entries), ctx)
    in
    Router.route_msg
      t.router
      node
      msg;
    Ivar.read ctx
    >>= fun result ->
    Deferred.return (Ok result)
end
