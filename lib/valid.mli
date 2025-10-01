(** Validity bitmaps for tracking null values in arrays

    Arrow uses compact bitmaps (1 bit per value) to track which values are null.
    This is more memory-efficient than OCaml option types for large datasets.

    {1 Why Validity Bitmaps?}

    Consider an array of 1 million integers where 10% are null:

    - Option array: [int option array] = 8 bytes/value × 1M = 8 MB
    - Validity bitmap: [int array] + [Valid.t] = 8 MB + 125 KB = 8.125 MB

    The bitmap uses only ~1.5% additional space vs 100% for options.

    {1 Bit Layout}

    Bits are packed into bytes with LSB first:
    {v
      Byte 0: [bit7 bit6 bit5 bit4 bit3 bit2 bit1 bit0]
              value7  6     5     4     3     2     1     0
    v}

    A bit value of 1 means the value is valid (not null).
    A bit value of 0 means the value is null.

    {1 Usage Examples}

    {2 Creating validity bitmaps}
    {[
      (* All values initially invalid *)
      let valid = Valid.create 100 in

      (* All values initially valid *)
      let valid = Valid.create_all_valid 100 in

      (* From a boolean array *)
      let valid = Valid.from_array [| true; false; true; true |]
    ]}

    {2 Reading and setting validity}
    {[
      let valid = Valid.create 10 in
      Valid.set_valid valid 0;      (* Mark index 0 as valid *)
      Valid.set_invalid valid 1;    (* Mark index 1 as invalid *)

      if Valid.is_valid valid 0 then
        Printf.printf "Value at 0 is valid\n";

      Printf.printf "%d valid values\n" (Valid.num_true valid)
    ]}

    {2 Using with column data}
    {[
      (* Read column with nullability information *)
      let data, valid = Wrapper.Column.read_i64_ba_opt table ~column:(`Name "age") in
      (* data is a bigarray of int64, valid is a bitmap *)

      for i = 0 to Bigarray.Array1.dim data - 1 do
        if Valid.is_valid valid i then
          Printf.printf "%Ld\n" data.{i}
        else
          Printf.printf "null\n"
      done
    ]}
*)

type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
(** Bigarray type for validity bitmaps - unsigned bytes in C layout *)

type t
(** Validity bitmap - compact representation of null/valid flags *)

(** {1 Creating Validity Bitmaps} *)

val create : int -> t
(** Create a validity bitmap with all values initially marked as invalid (null).

    @param length Number of values
*)

val create_all_valid : int -> t
(** Create a validity bitmap with all values initially marked as valid (not null).

    @param length Number of values
*)

val create_ba : int -> ba
(** Create a bigarray for a validity bitmap (all bits initially zero).

    Size in bytes: (length + 7) / 8

    @param length Number of values
*)

val from_array : bool array -> t
(** Create a validity bitmap from a boolean array.

    [true] means valid (not null), [false] means invalid (null).
*)

(** {1 Querying and Modifying} *)

val get : t -> int -> bool
(** Get the validity bit at an index.

    Returns [true] if valid (not null), [false] if invalid (null).

    @param bitmap The validity bitmap
    @param index Zero-based index
    @raise Invalid_argument if index is out of bounds
*)

val set : t -> int -> bool -> unit
(** Set the validity bit at an index.

    [true] marks as valid (not null), [false] marks as invalid (null).

    @param bitmap The validity bitmap
    @param index Zero-based index
    @param value Validity flag
    @raise Invalid_argument if index is out of bounds
*)

val set_valid : t -> int -> unit
(** Mark a value as valid (not null) at an index.

    Equivalent to [set bitmap index true].
*)

val set_invalid : t -> int -> unit
(** Mark a value as invalid (null) at an index.

    Equivalent to [set bitmap index false].
*)

val is_valid : t -> int -> bool
(** Check if a value is valid (not null) at an index.

    Same as {!get}.

    @raise Invalid_argument if index is out of bounds
*)

(** {1 Conversions} *)

val to_array : t -> bool array
(** Convert validity bitmap to a boolean array.

    Returns an array where [true] = valid, [false] = invalid/null.
*)

val to_ba : t -> int -> ba
(** Extract the underlying bigarray from a validity bitmap.

    @param bitmap The validity bitmap
    @param length The number of values (determines byte array size)
    @return Bigarray of bytes containing the packed bits
*)

val bigarray : t -> ba
(** Get direct access to the underlying bigarray.

    Use with caution - modifying this directly bypasses safety checks.
*)

(** {1 Statistics} *)

val length : t -> int
(** Get the number of values represented by this validity bitmap *)

val num_true : t -> int
(** Count the number of valid (non-null) values.

    This is the number of 1 bits in the bitmap.
*)

val num_false : t -> int
(** Count the number of invalid (null) values.

    This is the number of 0 bits in the bitmap.
    Equivalent to [length bitmap - num_true bitmap].
*)