open Core.Std
open Async.Std

module Make =
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Vote_store : Scow_vote_store.T) ->
        functor (Transport : Scow_transport.T with type Node.t = Vote_store.node) ->
struct
  type state = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport).t

  module Msg   = Scow_server_msg.Make(Log)(Transport)
  module TMsg  = Scow_transport.Msg
  module State = Scow_server_state.Make(Statem)(Log)(Vote_store)(Transport)

  let store_vote node =
    failwith "nyi"

  let handle_recv_append_entries self state (node, append_entries, ctx) =
    let module Ae = Scow_rpc.Append_entries in
    if Scow_term.compare state.State.current_term append_entries.Ae.term <= 0 then begin
      failwith "nyi"
    end
    else begin
      failwith "nyi"
    end

  let handle_recv_request_vote self state (node, request_vote, ctx) =
    match state.State.voted_for with
      | Some _ -> begin
        Transport.resp_request_vote
          state.State.transport
          ctx
          ~term:state.State.current_term
          ~granted:false
        >>=? fun () ->
        Deferred.return (Ok state)
      end
      | None -> begin
        let state = State.({ state with voted_for = Some node }) in
        Transport.resp_request_vote
          state.State.transport
          ctx
          ~term:state.State.current_term
          ~granted:true
        >>=? fun () ->
        Vote_store.store state.State.vote_store (Some node)
        >>=? fun () ->
        Deferred.return (Ok state)
      end

  let handle_call self state = function
    | Msg.Rpc (TMsg.Append_entries (node, append_entries), ctx) ->
      handle_recv_append_entries self state (node, append_entries, ctx)
    | Msg.Rpc (TMsg.Request_vote (node, request_vote), ctx) ->
      handle_recv_request_vote self state (node, request_vote, ctx)
    | Msg.Append_entries (ret, _) -> begin
      Ivar.fill ret (Error `Not_master);
      Deferred.return (Ok state)
    end
    | Msg.Election_timeout ->
      Deferred.return (Ok state)
    | _ ->
      Deferred.return (Ok state)
end

