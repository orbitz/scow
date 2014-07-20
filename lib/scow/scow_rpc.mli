module Append_entries : sig
  type 'elt t = { term           : Scow_term.t
                ; prev_log_index : Scow_log_index.t
                ; prev_log_term  : Scow_term.t
                ; leader_commit  : Scow_log_index.t
                ; entries        : 'elt list
                }
end

module Request_vote : sig
  type t = { term           : Scow_term.t
           ; last_log_index : Scow_log_index.t
           ; last_log_term  : Scow_term.t
           }
end
