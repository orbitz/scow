module Make :
  functor (Statem : Scow_statem.S) ->
    functor (Log : Scow_log.S) ->
      functor (Store : Scow_store.S) ->
        functor (Transport : Scow_transport.S with type Node.t = Store.node) ->
sig
  type state = Scow_replication_state.Make(Statem)(Log)(Store)(Transport).t
  type msg   = Scow_replication_msg.Make(Statem)(Log)(Transport).t

  val table : unit -> (msg, state) Event_engine.Rule_table.t
end

