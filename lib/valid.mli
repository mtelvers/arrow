type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type t

val create : int -> t
val create_all_valid : int -> t
val create_ba : int -> ba
val get : t -> int -> bool
val set : t -> int -> bool -> unit
val length : t -> int
val set_invalid : t -> int -> unit
val set_valid : t -> int -> unit
val is_valid : t -> int -> bool
val from_array : bool array -> t
val to_array : t -> bool array
val to_ba : t -> int -> ba
val num_true : t -> int
val num_false : t -> int
val bigarray : t -> ba