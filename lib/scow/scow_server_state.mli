open Async.Std

module Make :
  functor (Statem : Scow_statem.T) ->
    functor (Log : Scow_log.T) ->
      functor (Transport : Scow_transport.T) ->
sig
  type t = { me           : Transport.Node.t
           ; nodes        : Transport.Node.t list
           ; statem       : Statem.t
           ; transport    : Transport.t
           ; log          : Log.t
           ; max_par_repl : int
           ; current_term : Scow_term.t
           ; commit_idx   : Scow_log_index.t
           ; last_applied : Scow_log_index.t
           ; leader       : Transport.Node.t option
           ; voted_for    : Transport.Node.t option
           ; handler      : Scow_server_msg.Make(Log)(Transport).t Gen_server.t ->
                            t ->
                            Scow_server_msg.Make(Log)(Transport).t ->
                            (t, unit) Deferred.Result.t
           }
end
