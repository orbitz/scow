open Core.Std
open Async.Std

module type NODE = sig
  type t
end

module Make = functor (Node : NODE) -> struct
  type node = Node.t
  type t = { mutable vote : node option
           ; mutable term : Scow_term.t option
           }

  let create () = { vote = None; term = None }

  let store_vote t node_opt =
    t.vote <- node_opt;
    Deferred.return (Ok ())

  let load_vote t = Deferred.return (Ok t.vote)

  let store_term t term =
    t.term <- Some term;
    Deferred.return (Ok ())

  let load_term t = Deferred.return (Ok t.term)
end
