(* Semantically same thing for now *)
type t = Scow_term.t

let zero     = Scow_term.zero
let succ     = Scow_term.succ
let pred     = Scow_term.pred
let of_int   = Scow_term.of_int
let to_int   = Scow_term.to_int
let compare  = Scow_term.compare
let is_equal = Scow_term.is_equal
