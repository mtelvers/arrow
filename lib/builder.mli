(** Type-safe builders for constructing Arrow arrays incrementally

    Builders allow efficient construction of columnar data with proper handling
    of null values. Each builder is specialized for a specific data type.

    {1 Two Ways to Build Tables}

    {2 Recommended: High-level array-to-table API}

    For most use cases, the simplest approach is to use {!array_to_table}
    which converts OCaml arrays directly to tables:

    {[
      type person = { name : string; age : int; score : float option }

      let people = [|
        { name = "Alice"; age = 25; score = Some 95.5 };
        { name = "Bob"; age = 30; score = None };
      |]

      let table = Builder.array_to_table
        [ Builder.col Table.Utf8 ~name:"name" (fun p -> p.name)
        ; Builder.col Table.Int ~name:"age" (fun p -> p.age)
        ; Builder.col_opt Table.Float ~name:"score" (fun p -> p.score)
        ]
        people
    ]}

    {2 Advanced: Manual builder management}

    For streaming data or when you don't have all data in memory, use builders directly:

    {[
      let builder = Builder.Int64.create () in
      Builder.Int64.append builder 42L;
      Builder.Int64.append_null builder;
      Builder.Int64.append_opt builder (Some 100L);

      let table = Builder.make_table [
        ("my_column", Wrapper.Builder.Int64 builder)
      ]
    ]}

    {1 When to Use Builders}

    - Building data row-by-row from streaming sources
    - When you don't have all data in memory at once
    - When you need precise control over memory allocation
    - For incremental construction with complex logic

    For static data already in arrays, prefer {!array_to_table} or {!Table.create}.

    {1 Available Builder Types}

    Builders exist for all major Arrow types:
    - Integers: {!Int8}, {!Int16}, {!Int32}, {!Int64}, {!UInt8}, {!UInt16}, {!UInt32}, {!UInt64}, {!NativeInt}
    - Floats: {!Float}, {!Double}
    - Strings: {!String}
    - Boolean: {!Boolean}
    - Temporal: {!Date32}, {!Date64}, {!Time32}, {!Time64}, {!Timestamp}, {!Duration}
*)

module type Intf = sig
  type t
  (** The builder type *)

  type elem
  (** The element type being built *)

  val create : unit -> t
  (** Create a new empty builder.

      Builders start with a small initial capacity and grow automatically
      as elements are appended.
  *)

  val append : t -> elem -> unit
  (** Append a non-null value to the builder.

      Amortized O(1) complexity due to buffer resizing strategy.
  *)

  val append_null : ?n:int -> t -> unit
  (** Append n null values to the builder (default: n=1).

      Nulls are tracked in a validity bitmap. Amortized O(n) complexity.
  *)

  val append_opt : t -> elem option -> unit
  (** Append a value or null based on the option.

      Equivalent to:
      {[
        match opt with
        | Some v -> append t v
        | None -> append_null t
      ]}
  *)

  val length : t -> int
  (** Total number of elements (including nulls) *)

  val null_count : t -> int
  (** Number of null elements *)
end

(** {1 Typed Builder Modules} *)

module Double : sig
  include Intf with type elem := float and type t = Wrapper.DoubleBuilder.t
  (** Builder for 64-bit floating point numbers (Float64 in Arrow) *)
end

module Int32 : sig
  include Intf with type elem := int32 and type t = Wrapper.Int32Builder.t
  (** Builder for 32-bit signed integers *)
end

module Int64 : sig
  include Intf with type elem := int64 and type t = Wrapper.Int64Builder.t
  (** Builder for 64-bit signed integers *)
end

module NativeInt : sig
  include Intf with type elem := int and type t = Wrapper.Int64Builder.t
  (** Builder for OCaml native ints (stored as Int64 in Arrow) *)
end

module String : sig
  include Intf with type elem := string and type t = Wrapper.StringBuilder.t
  (** Builder for UTF-8 strings *)
end

module Int8 : sig
  include Intf with type elem := int and type t = C_wrapper.Int8Builder.t
  (** Builder for 8-bit signed integers (range: -128 to 127) *)
end

module Int16 : sig
  include Intf with type elem := int and type t = C_wrapper.Int16Builder.t
  (** Builder for 16-bit signed integers (range: -32768 to 32767) *)
end

module UInt8 : sig
  include Intf with type elem := int and type t = C_wrapper.UInt8Builder.t
  (** Builder for 8-bit unsigned integers (range: 0 to 255) *)
end

module UInt16 : sig
  include Intf with type elem := int and type t = C_wrapper.UInt16Builder.t
  (** Builder for 16-bit unsigned integers (range: 0 to 65535) *)
end

module UInt32 : sig
  include Intf with type elem := int32 and type t = C_wrapper.UInt32Builder.t
  (** Builder for 32-bit unsigned integers (stored in int32) *)
end

module UInt64 : sig
  include Intf with type elem := int64 and type t = C_wrapper.UInt64Builder.t
  (** Builder for 64-bit unsigned integers (stored in int64) *)
end

module Float : sig
  include Intf with type elem := float and type t = C_wrapper.FloatBuilder.t
  (** Builder for 32-bit floating point numbers (Float32 in Arrow) *)
end

module Boolean : sig
  include Intf with type elem := bool and type t = C_wrapper.BooleanBuilder.t
  (** Builder for boolean values (stored as bits in Arrow) *)
end

module Date32 : sig
  include Intf with type elem := int32 and type t = C_wrapper.Date32Builder.t
  (** Builder for dates stored as days since Unix epoch (1970-01-01) *)
end

module Date64 : sig
  include Intf with type elem := int64 and type t = C_wrapper.Date64Builder.t
  (** Builder for dates stored as milliseconds since Unix epoch *)
end

module Time32 : sig
  include Intf with type elem := int32 and type t = C_wrapper.Time32Builder.t
  (** Builder for time of day (seconds or milliseconds since midnight) *)
end

module Time64 : sig
  include Intf with type elem := int64 and type t = C_wrapper.Time64Builder.t
  (** Builder for time of day (microseconds or nanoseconds since midnight) *)
end

module Timestamp : sig
  include Intf with type elem := int64 and type t = C_wrapper.TimestampBuilder.t
  (** Builder for timestamps (nanoseconds since Unix epoch) *)
end

module Duration : sig
  include Intf with type elem := int64 and type t = C_wrapper.DurationBuilder.t
  (** Builder for time durations (nanoseconds) *)
end

(** {1 Creating Tables from Builders} *)

val make_table : (string * Wrapper.Builder.t) list -> Table.t
(** Create a table from a list of named builders.

    Example:
    {[
      let id_builder = Builder.Int32.create () in
      let name_builder = Builder.String.create () in
      Builder.Int32.append id_builder 1l;
      Builder.String.append name_builder "Alice";

      let table = Builder.make_table [
        ("id", Wrapper.Builder.Int32 id_builder);
        ("name", Wrapper.Builder.String name_builder);
      ]
    ]}
*)

(** {1 High-Level Array-to-Table API} *)

type ('row, 'elem, 'col_type) col =
  { name : string
  ; get : 'row -> 'elem
  ; col_type : 'col_type Table.col_type
  }
(** Column specification: name, accessor function, and type *)

type 'row packed_col =
  | P : ('row, 'elem, 'elem) col -> 'row packed_col
  (** Non-nullable column *)
  | O : ('row, 'elem option, 'elem) col -> 'row packed_col
  (** Nullable column *)
(** Existentially packed column specification *)

type 'row packed_cols = 'row packed_col list
(** List of column specifications for a row type *)

val col : ?name:string -> 'a Table.col_type -> ('row -> 'a) -> 'row packed_cols
(** Specify a non-nullable column with an accessor function.

    Example:
    {[
      type person = { name : string; age : int }

      let columns = Builder.col Table.Utf8 ~name:"name" (fun p -> p.name)
    ]}
*)

val col_opt : ?name:string -> 'a Table.col_type -> ('row -> 'a option) -> 'row packed_cols
(** Specify a nullable column with an accessor function.

    Example:
    {[
      type person = { name : string; score : float option }

      let columns = Builder.col_opt Table.Float ~name:"score" (fun p -> p.score)
    ]}
*)

val array_to_table : 'row packed_cols -> 'row array -> Table.t
(** Convert an array of OCaml records/tuples to an Arrow table.

    This is the recommended way to build tables from in-memory OCaml data.

    Example:
    {[
      type person = { name : string; age : int; score : float option }

      let people = [|
        { name = "Alice"; age = 25; score = Some 95.5 };
        { name = "Bob"; age = 30; score = None };
        { name = "Charlie"; age = 35; score = Some 87.3 };
      |]

      let table = Builder.array_to_table
        [ Builder.col Table.Utf8 ~name:"name" (fun p -> p.name)
        ; Builder.col Table.Int ~name:"age" (fun p -> p.age)
        ; Builder.col_opt Table.Float ~name:"score" (fun p -> p.score)
        ]
        people
    ]}
*)

(** {1 Row-Based Builder Pattern} *)

module type Row_intf = sig
  type row
  (** The row type *)

  val array_to_table : row array -> Table.t
  (** Convert an array of rows to a table *)
end
(** Interface for row-based table construction *)

module type Row_builder_intf = sig
  type t
  (** The builder type *)

  type row
  (** The row type being built *)

  val create : unit -> t
  (** Create a new empty row builder *)

  val append : t -> row -> unit
  (** Append a row to the builder *)

  val length : t -> int
  (** Number of rows in the builder *)

  val reset : t -> unit
  (** Clear the builder, removing all rows *)

  val to_table : t -> Table.t
  (** Convert the builder to a table *)
end
(** Interface for incremental row-based building *)

module Row (R : Row_intf) : Row_builder_intf with type row = R.row
(** Create an incremental row builder from a Row_intf implementation.

    Example:
    {[
      type person = { name : string; age : int }

      module Person_row = struct
        type row = person

        let array_to_table arr =
          Builder.array_to_table
            [ Builder.col Table.Utf8 ~name:"name" (fun p -> p.name)
            ; Builder.col Table.Int ~name:"age" (fun p -> p.age)
            ]
            arr
      end

      module Person_builder = Builder.Row(Person_row)

      let builder = Person_builder.create () in
      Person_builder.append builder { name = "Alice"; age = 25 };
      Person_builder.append builder { name = "Bob"; age = 30 };
      let table = Person_builder.to_table builder
    ]}
*)