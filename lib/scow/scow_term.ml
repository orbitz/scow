type t = int

let zero () = 0

let succ t = t + 1

let pred t = t - 1

let of_int = function
  | n when n >= 0 -> Some n
  | _             -> None

let to_int t = t
