(** Tables: Collections of named columns with a common schema

    A Table is Arrow's primary data structure, similar to a DataFrame in pandas
    or a data.frame in R. Tables are immutable and optimized for columnar analytics.

    {1 Key Concepts}

    - {b Columnar storage}: Data is stored by column, not by row
    - {b Zero-copy}: Operations like slicing don't copy data
    - {b Chunked}: Large columns may be split into chunks internally
    - {b Type-safe}: Each column has a specific Arrow data type

    {1 Common Operations}

    {2 Creating tables from OCaml data}
    {[
      let table = Table.create [
        Table.col [| 1; 2; 3 |] Table.Int ~name:"id";
        Table.col [| "Alice"; "Bob"; "Charlie" |] Table.Utf8 ~name:"name";
        Table.col_opt [| Some 1.5; None; Some 2.7 |] Table.Float ~name:"score";
      ]
    ]}

    {2 Reading from files}
    {[
      let table1 = Table.read_csv "data.csv" in
      let table2 = Table.read_json "data.json" in
      let table3 = Parquet_reader.table "data.parquet"
    ]}

    {2 Processing columns}
    {[
      let ids = Table.read table Table.Int ~column:(`Name "id") in
      let scores = Table.read_opt table Table.Float ~column:(`Name "score") in

      (* Or using Wrapper for more types *)
      let names = Wrapper.Column.read_utf8 table ~column:(`Name "name")
    ]}

    {2 Table operations}
    {[
      (* Combine tables vertically *)
      let combined = Table.concatenate [table1; table2; table3] in

      (* Extract a subset of rows *)
      let subset = Table.slice combined ~offset:100 ~length:50 in

      (* Add a new column *)
      let with_col = Table.add_column table "new_col" chunked_array
    ]}
*)

type t = Wrapper.Table.t
(** Immutable table of columnar data *)

(** {1 Table Operations} *)

val concatenate : t list -> t
(** Concatenate multiple tables vertically (stack rows).

    All tables must have the same schema (column names and types).
    This is a zero-copy operation when possible.

    Example:
    {[
      let combined = Table.concatenate [jan_data; feb_data; mar_data]
    ]}

    @raise Failure if schemas don't match
*)

val slice : t -> offset:int -> length:int -> t
(** Extract a subset of rows from a table.

    This is a zero-copy operation - no data is copied, only metadata is updated.

    Example:
    {[
      (* Get rows 10-19 *)
      let subset = Table.slice table ~offset:10 ~length:10
    ]}

    @raise Invalid_argument if offset or length are out of bounds
*)

val num_rows : t -> int
(** Get the total number of rows in the table *)

val schema : t -> Wrapper.Schema.t
(** Get the schema (column names, types, and metadata) of the table *)

(** {1 Reading and Writing Files} *)

val read_csv : string -> t
(** Read a CSV file into a table.

    Schema is automatically inferred from the data. The first row is treated
    as header names. Type inference examines the data to determine column types.

    Example:
    {[
      let table = Table.read_csv "sales.csv"
    ]}

    @raise Failure if file cannot be read or parsed
*)

val read_json : string -> t
(** Read a JSON file into a table.

    Supports both line-delimited JSON (one object per line) and a single
    JSON array of objects. Schema is inferred from the data.

    Example:
    {[
      let table = Table.read_json "data.json"
    ]}

    @raise Failure if file cannot be read or parsed
*)

val write_parquet : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
(** Write table to Parquet format.

    Parquet is a columnar storage format with efficient compression and encoding.
    It's the recommended format for long-term storage and analytics.

    @param chunk_size Rows per row group (default: 64k). Larger values give
                      better compression but require more memory.
    @param compression Compression codec (default: Snappy). Snappy is fast;
                       Zstd gives better compression.

    Example:
    {[
      Table.write_parquet table "output.parquet";
      Table.write_parquet table "compressed.parquet"
        ~compression:Compression.Zstd
        ~chunk_size:100_000
    ]}

    @raise Failure if write fails
*)

val write_feather : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
(** Write table to Feather/IPC format.

    Feather is Arrow's native binary format - faster to read/write than Parquet
    but with less compression. Good for intermediate storage or fast data exchange.

    @param chunk_size Rows per record batch (default: 64k)
    @param compression Compression codec (default: None)

    Example:
    {[
      Table.write_feather table "cache.feather"
    ]}

    @raise Failure if write fails
*)

val to_string_debug : t -> string
(** Get a debug string representation of the table.

    Shows schema, number of rows, and sample data. Useful for debugging
    and interactive exploration.
*)

(** {1 Column Operations} *)

val add_column : t -> string -> Wrapper.ChunkedArray.t -> t
(** Add a new column to the table.

    The chunked array must have the same number of rows as the table.
    Returns a new table with the added column.

    Example:
    {[
      let table_with_id = Table.add_column table "id" id_column
    ]}

    @raise Failure if row counts don't match
*)

val get_column : t -> string -> Wrapper.ChunkedArray.t
(** Get a column by name as a ChunkedArray.

    @raise Failure if column doesn't exist
*)

val add_all_columns : t -> t -> t
(** Merge all columns from the second table into the first.

    Both tables must have the same number of rows. This performs a horizontal
    join (adds columns).

    @raise Failure if row counts don't match or column names collide
*)

(** {1 High-Level Type-Safe API} *)

type _ col_type =
  | Int : int col_type
  (** Native int column (stored as Int64 in Arrow) *)
  | Float : float col_type
  (** Float64 column *)
  | Utf8 : string col_type
  (** UTF-8 string column *)
  | Date : Datetime.Date.t col_type
  (** Date column (days since Unix epoch) *)
  | Time_ns : Datetime.Time_ns.t col_type
  (** Timestamp column (nanoseconds since Unix epoch) *)
  | Span_ns : Datetime.Time_ns.Span.t col_type
  (** Duration column (nanoseconds) *)
  | Ofday_ns : Datetime.Time_ns.Ofday.t col_type
  (** Time of day column (nanoseconds since midnight) *)
  | Bool : bool col_type
  (** Boolean column *)
(** GADT for type-safe column operations.

    This ensures you can't accidentally read a string column as integers, etc.
*)

type packed_col =
  | P : 'a col_type * 'a array -> packed_col
  (** Non-nullable column *)
  | O : 'a col_type * 'a option array -> packed_col
  (** Nullable column *)
(** Existentially packed column for building tables *)

type writer_col
(** Opaque type for columns being added to a table *)

val create : writer_col list -> t
(** Create a table from a list of columns.

    This is the recommended way to build tables from OCaml data.

    Example:
    {[
      let table = Table.create [
        Table.col [| 1; 2; 3 |] Table.Int ~name:"id";
        Table.col [| "a"; "b"; "c" |] Table.Utf8 ~name:"label";
        Table.col_opt [| Some 1.5; None; Some 2.7 |] Table.Float ~name:"score";
      ]
    ]}
*)

val named_col : packed_col -> name:string -> writer_col
(** Convert a packed column to a named column for table creation *)

val col : 'a array -> 'a col_type -> name:string -> writer_col
(** Create a non-nullable column from an array.

    Example:
    {[
      Table.col [| 1; 2; 3 |] Table.Int ~name:"count"
    ]}
*)

val col_opt : 'a option array -> 'a col_type -> name:string -> writer_col
(** Create a nullable column from an option array.

    Example:
    {[
      Table.col_opt [| Some 1.5; None; Some 2.7 |] Table.Float ~name:"score"
    ]}
*)

val read : t -> column:Wrapper.Column.column -> 'a col_type -> 'a array
(** Read a non-nullable column from a table.

    Type-safe: the column type determines the return array type.

    Example:
    {[
      let ages = Table.read table ~column:(`Name "age") Table.Int in
      let names = Table.read table ~column:(`Index 0) Table.Utf8
    ]}

    @raise Failure if column doesn't exist or type doesn't match
*)

val read_opt : t -> column:Wrapper.Column.column -> 'a col_type -> 'a option array
(** Read a nullable column from a table.

    Returns an option array where None represents null values.

    Example:
    {[
      let scores = Table.read_opt table ~column:(`Name "score") Table.Float
    ]}

    @raise Failure if column doesn't exist or type doesn't match
*)