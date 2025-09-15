type t
type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

val create : int -> t
val create_ba : int -> ba
val set_invalid : t -> int -> unit
val set_valid : t -> int -> unit
val is_valid : t -> int -> bool
val from_array : bool array -> t
val to_ba : t -> int -> ba