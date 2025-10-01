(** Low-level bindings to Arrow C++ API

    {b Warning:} This module exposes internal implementation details and direct
    C++ bindings. Most users should use the higher-level modules: {!Table},
    {!Builder}, {!Parquet_reader}, etc.

    This module provides direct access to Arrow internals for advanced use cases:
    - Schema inspection and manipulation
    - Direct bigarray access for zero-copy performance
    - Custom column read/write operations
    - Low-level table and array construction

    {1 When to Use This Module}

    - You need maximum performance with zero-copy bigarrays
    - You're working with data types not exposed in high-level API
    - You need fine-grained control over memory layout
    - You're implementing custom serialization/deserialization

    {1 Performance Note}

    The [*_ba] functions return bigarrays that directly reference Arrow's
    internal memory buffers. This is zero-copy and very fast, but requires
    understanding Arrow's memory management.
*)

module Schema : sig
  (** Arrow schema representation - describes table structure *)

  module Flags : sig
    type t
    (** Schema field flags *)

    val all : t list -> t
    (** Combine multiple flags *)

    val dictionary_ordered_ : t
    (** Flag: dictionary values are ordered *)

    val nullable_ : t
    (** Flag: field can contain null values *)

    val map_keys_sorted_ : t
    (** Flag: map keys are sorted *)

    val dictionary_ordered : t -> bool
    (** Check if dictionary_ordered flag is set *)

    val nullable : t -> bool
    (** Check if nullable flag is set *)

    val map_keys_sorted : t -> bool
    (** Check if map_keys_sorted flag is set *)
  end

  type t =
    { format : Datatype.t
      (** Arrow data type of this field *)
    ; name : string
      (** Field name (empty for root) *)
    ; metadata : (string * string) list
      (** Key-value metadata *)
    ; flags : Flags.t
      (** Field flags (nullable, etc.) *)
    ; children : t list
      (** Child fields (for nested types like Struct) *)
    }
  (** Schema field - describes one column or nested field

      For a top-level table, this represents the entire schema with
      one child per column.
  *)
end

module ChunkedArray : sig
  (** ChunkedArray - a column that may be split into multiple contiguous chunks

      Large columns may be split into chunks for memory management.
      Most operations work transparently across chunks.
  *)

  type t
  (** Opaque handle to a chunked array *)
end

module Table : sig
  (** Low-level table operations - prefer {!Arrow.Table} for high-level API *)

  type t
  (** Opaque handle to an Arrow table *)

  val concatenate : t list -> t
  (** Concatenate tables vertically (must have same schema) *)

  val slice : t -> offset:int -> length:int -> t
  (** Extract a range of rows (zero-copy) *)

  val num_rows : t -> int
  (** Get number of rows *)

  val schema : t -> Schema.t
  (** Get table schema *)

  val read_csv : string -> t
  (** Read CSV file with automatic type inference *)

  val read_json : string -> t
  (** Read JSON file (line-delimited or array) *)

  val write_parquet : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
  (** Write to Parquet format *)

  val write_feather : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
  (** Write to Feather/IPC format *)

  val to_string_debug : t -> string
  (** Debug string representation *)

  val add_column : t -> string -> ChunkedArray.t -> t
  (** Add a column (must have same row count) *)

  val get_column : t -> string -> ChunkedArray.t
  (** Get column by name *)

  val add_all_columns : t -> t -> t
  (** Horizontally concatenate columns from two tables *)
end

module Parquet_reader : sig
  (** Low-level Parquet reader - prefer {!Arrow.Parquet_reader} *)

  type t
  (** Streaming Parquet reader handle *)

  val create
    :  ?use_threads:bool
    -> ?column_idxs:int list
    -> ?mmap:bool
    -> ?buffer_size:int
    -> ?batch_size:int
    -> string
    -> t

  val next : t -> Table.t option
  val close : t -> unit
  val schema : string -> Schema.t
  val schema_and_num_rows : string -> Schema.t * int

  val table
    :  ?only_first:int
    -> ?use_threads:bool
    -> ?column_idxs:int list
    -> string
    -> Table.t
end

module Feather_reader : sig
  (** Low-level Feather/IPC reader - prefer {!Arrow.File_reader} *)

  val schema : string -> Schema.t
  (** Read schema from Feather file *)

  val table : ?column_idxs:int list -> string -> Table.t
  (** Read Feather file, optionally selecting columns *)
end

module Column : sig
  (** Column reading operations - extract data from tables

      This module provides the primary way to read data from Arrow tables.
      Two patterns are available:

      1. {b Zero-copy bigarrays} ([*_ba] functions): Maximum performance,
         returns bigarrays that reference Arrow's internal buffers
      2. {b OCaml arrays} ([read_*] functions): More convenient, copies
         data into OCaml-managed arrays
  *)

  type column =
    [ `Index of int
      (** Select column by position (0-indexed) *)
    | `Name of string
      (** Select column by name *)
    ]
  (** Column selector - by index or name *)

  (** {1 Zero-Copy Bigarray Reads} *)

  val read_i32_ba
    :  Table.t
    -> column:column
    -> (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
  (** Read int32 column as bigarray (zero-copy). Maximum performance. *)

  val read_i64_ba
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
  (** Read int64 column as bigarray (zero-copy). Maximum performance. *)

  val read_f64_ba
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
  (** Read float64 column as bigarray (zero-copy). Maximum performance. *)

  val read_f32_ba
    :  Table.t
    -> column:column
    -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t
  (** Read float32 column as bigarray (zero-copy). Maximum performance. *)

  (** {1 Array Reads (Non-Nullable)} *)

  val read_int : Table.t -> column:column -> int array
  (** Read int64 column as native int array *)

  val read_int32 : Table.t -> column:column -> Int32.t array
  (** Read int32 column as int32 array *)

  val read_float : Table.t -> column:column -> float array
  (** Read float64 column as float array *)

  val read_utf8 : Table.t -> column:column -> string array
  (** Read UTF-8 string column as string array *)

  val read_date : Table.t -> column:column -> Datetime.Date.t array
  (** Read date column as Ptime.date array *)

  val read_time_ns : Table.t -> column:column -> Datetime.Time_ns.t array
  (** Read timestamp column as Ptime.t array *)

  val read_ofday_ns : Table.t -> column:column -> Datetime.Time_ns.Ofday.t array
  (** Read time-of-day column as (hour, minute, second, nanosecond) tuples *)

  val read_span_ns : Table.t -> column:column -> Datetime.Time_ns.Span.t array
  (** Read duration column as Ptime.span array *)

  (** {1 Bigarray Reads with Validity (Nullable)} *)

  val read_i32_ba_opt
    :  Table.t
    -> column:column
    -> (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t
  (** Read nullable int32 column as (bigarray, validity_bitmap) *)

  val read_i64_ba_opt
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t
  (** Read nullable int64 column as (bigarray, validity_bitmap) *)

  val read_f64_ba_opt
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t
  (** Read nullable float64 column as (bigarray, validity_bitmap) *)

  val read_f32_ba_opt
    :  Table.t
    -> column:column
    -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t
  (** Read nullable float32 column as (bigarray, validity_bitmap) *)

  (** {1 Array Reads (Nullable)} *)

  val read_int_opt : Table.t -> column:column -> int option array
  (** Read nullable int64 column as option array *)

  val read_int32_opt : Table.t -> column:column -> Int32.t option array
  (** Read nullable int32 column as option array *)

  val read_float_opt : Table.t -> column:column -> float option array
  (** Read nullable float64 column as option array *)

  val read_utf8_opt : Table.t -> column:column -> string option array
  (** Read nullable UTF-8 string column as option array *)

  val read_date_opt : Table.t -> column:column -> Datetime.Date.t option array
  (** Read nullable date column as option array *)

  val read_time_ns_opt : Table.t -> column:column -> Datetime.Time_ns.t option array
  (** Read nullable timestamp column as option array *)

  val read_ofday_ns_opt : Table.t -> column:column -> Datetime.Time_ns.Ofday.t option array
  (** Read nullable time-of-day column as option array *)

  val read_span_ns_opt : Table.t -> column:column -> Datetime.Time_ns.Span.t option array
  (** Read nullable duration column as option array *)

  (** {1 Boolean Column Reads} *)

  val read_bitset : Table.t -> column:column -> Valid.t
  (** Read boolean column as validity bitmap *)

  val read_bitset_opt : Table.t -> column:column -> Valid.t * Valid.t
  (** Read nullable boolean column as (data_bitmap, validity_bitmap) *)

  (** {1 Dynamic Type Reading} *)

  type t =
    | Unsupported_type
    (** Column type not supported by fast_read *)
    | String of string array
    (** String column data *)
    | String_option of string option array
    (** Nullable string column data *)
    | Int64 of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    (** Int64 column as bigarray *)
    | Int64_option of
        (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba
    (** Nullable int64 column as (bigarray, validity) *)
    | Double of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    (** Float64 column as bigarray *)
    | Double_option of
        (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba
    (** Nullable float64 column as (bigarray, validity) *)
  (** Dynamically-typed column data *)

  val fast_read : Table.t -> int -> t
  (** Read a column by index with automatic type detection.

      This is useful when you don't know the column type at compile time.
      Returns typed variant based on the actual Arrow column type.

      @param table The table to read from
      @param column_index Zero-based column index
      @return Typed variant containing the column data
  *)
end

module Writer : sig
  (** Low-level column writing from bigarrays

      This module provides zero-copy table construction from bigarray data.
      It's the fastest way to create Arrow tables but requires manual memory
      management.

      For most use cases, prefer {!Arrow.Table.create} or {!Arrow.Builder}.
  *)

  type col
  (** Opaque column type for Writer *)

  val fixed_ba
    : format:string
    -> ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col
  (** Create column from bigarray of fixed-width values *)

  val fixed_ba_opt
    : format:string
    -> ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val string_ba
    : format:string
    -> offsets:('a, 'b, Bigarray.c_layout) Bigarray.Array1.t
    -> data:(char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val string_ba_opt
    : format:string
    -> offsets:('a, 'b, Bigarray.c_layout) Bigarray.Array1.t
    -> data:(char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> valid:Valid.t
    -> name:string
    -> col

  val int64_ba
    : (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val int64_ba_opt
    : (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val int32_ba
    : (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val int32_ba_opt
    : (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val int : int array -> name:string -> col
  val int_opt : int option array -> name:string -> col

  val float64_ba
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val float64_ba_opt
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val float : float array -> name:string -> col
  val float_opt : float option array -> name:string -> col
  val utf8 : string array -> name:string -> col
  val utf8_opt : string option array -> name:string -> col
  val date : Datetime.Date.t array -> name:string -> col
  val date_opt : Datetime.Date.t option array -> name:string -> col

  val time_ns : Datetime.Time_ns.t array -> name:string -> col
  val time_ns_opt : Datetime.Time_ns.t option array -> name:string -> col
  val ofday_ns : Datetime.Time_ns.Ofday.t array -> name:string -> col
  val ofday_ns_opt : Datetime.Time_ns.Ofday.t option array -> name:string -> col
  val span_ns : Datetime.Time_ns.Span.t array -> name:string -> col
  val span_ns_opt : Datetime.Time_ns.Span.t option array -> name:string -> col
  val bitset : Valid.t -> name:string -> col
  val bitset_opt : Valid.t -> valid:Valid.t -> name:string -> col

  val write
    :  ?chunk_size:int
    -> ?compression:Compression.t
    -> string
    -> cols:col list
    -> unit

  val create_table : cols:col list -> Table.t
end

module DoubleBuilder : sig
  type t

  val create : unit -> t
  val append : t -> float -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Int32Builder : sig
  type t

  val create : unit -> t
  val append : t -> Int32.t -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Int64Builder : sig
  type t

  val create : unit -> t
  val append : t -> Int64.t -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module StringBuilder : sig
  type t

  val create : unit -> t
  val append : t -> string -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Builder : sig
  type t =
    | Double of DoubleBuilder.t
    | Int32 of Int32Builder.t
    | Int64 of Int64Builder.t
    | String of StringBuilder.t

  val make_table : (string * t) list -> Table.t
end
