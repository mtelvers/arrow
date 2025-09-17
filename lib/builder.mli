module type Intf = sig
  type t
  type elem

  val create : unit -> t
  val append : t -> elem -> unit
  val append_null : ?n:int -> t -> unit
  val append_opt : t -> elem option -> unit
  val length : t -> int
  val null_count : t -> int
end

module Double : sig
  include Intf with type elem := float and type t = Wrapper.DoubleBuilder.t
end

module Int32 : sig
  include Intf with type elem := Int32.t and type t = Wrapper.Int32Builder.t
end

module Int64 : sig
  include Intf with type elem := Int64.t and type t = Wrapper.Int64Builder.t
end

module NativeInt : sig
  include Intf with type elem := int and type t = Wrapper.Int64Builder.t
end

module String : sig
  include Intf with type elem := string and type t = Wrapper.StringBuilder.t
end

val make_table : (string * Wrapper.Builder.t) list -> Table.t

type ('row, 'elem, 'col_type) col =
  { name : string
  ; get : 'row -> 'elem
  ; col_type : 'col_type Table.col_type
  }

type 'row packed_col =
  | P : ('row, 'elem, 'elem) col -> 'row packed_col
  | O : ('row, 'elem option, 'elem) col -> 'row packed_col

type 'row packed_cols = 'row packed_col list

val col : ?name:string -> 'a Table.col_type -> ('row -> 'a) -> 'row packed_cols
val col_opt : ?name:string -> 'a Table.col_type -> ('row -> 'a option) -> 'row packed_cols

val array_to_table : 'row packed_cols -> 'row array -> Table.t

module type Row_intf = sig
  type row
  val array_to_table : row array -> Table.t
end

module type Row_builder_intf = sig
  type t
  type row

  val create : unit -> t
  val append : t -> row -> unit
  val length : t -> int
  val reset : t -> unit
  val to_table : t -> Table.t
end

module Row (R : Row_intf) : Row_builder_intf with type row = R.row