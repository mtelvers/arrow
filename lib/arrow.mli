(** OCaml bindings for Apache Arrow - a columnar in-memory data format

    Apache Arrow is a cross-language development platform for in-memory data.
    It specifies a standardized language-independent columnar memory format for
    flat and hierarchical data, organized for efficient analytic operations.
    It also supports Parquet, a column-oriented data file format designed for
    efficient data storage and retrieval.

    {1 Quick Start}

    Most users will want to:
    - Read files: {!Parquet_reader} or {!File_reader} (for Feather/IPC)
    - Build tables: {!Builder} or {!Table}
    - Process data: {!Wrapper.Column} for reading columns
    - Write files: {!Table.write_parquet} or {!Table.write_feather}

    {1 Common Patterns}

    {2 Reading a Parquet file and processing columns}
    {[
      let table = Parquet_reader.table "data.parquet" in
      let ages = Wrapper.Column.read_int table ~column:(`Name "age") in
      let names = Wrapper.Column.read_utf8 table ~column:(`Name "name") in
      Array.iter2 (fun name age ->
        Printf.printf "%s is %d years old\n" name age
      ) names ages
    ]}

    {2 Building a table from OCaml data}
    {[
      let table = Table.create [
        Table.col [| 1; 2; 3 |] Table.Int ~name:"id";
        Table.col [| "Alice"; "Bob"; "Charlie" |] Table.Utf8 ~name:"name";
      ]
    ]}

    {2 Streaming large Parquet files}
    {[
      Parquet_reader.iter_batches "large.parquet"
        ~batch_size:10_000
        ~f:(fun batch ->
          (* Process each batch incrementally *)
          let col = Wrapper.Column.read_int batch ~column:(`Index 0) in
          Array.iter print_int col
        )
    ]}

    {2 Using builders for incremental construction}
    {[
      let builder = Builder.Int64.create () in
      Builder.Int64.append builder 42L;
      Builder.Int64.append_null builder;
      Builder.Int64.append_opt builder (Some 100L);

      let table = Builder.make_table [
        ("my_column", Wrapper.Builder.Int64 builder)
      ]
    ]}
*)

(** {1 Reading and Writing Data} *)

module Parquet_reader = Parquet_reader
(** Read Apache Parquet files - the recommended columnar format for analytics.

    Parquet is a columnar storage format that provides efficient compression
    and encoding schemes. Use this for most data analytics workloads.

    Supports both full-file loading and streaming for memory-efficient
    processing of large datasets.
*)

module File_reader = File_reader
(** Read Arrow IPC/Feather files - fast binary format for Arrow data.

    Feather/IPC format is faster than Parquet for reading but less compressed.
    Use this for intermediate data exchange or when speed is critical.
*)

module Table = Table
(** Tables are the primary data structure - collections of named columns.

    Tables support:
    - Reading from CSV/JSON files
    - Writing to Parquet/Feather formats
    - Slicing and concatenation
    - Column operations (add, get, merge)

    Tables are immutable and use zero-copy operations where possible.
*)

(** {1 Building Data} *)

module Builder = Builder
(** Typed builders for constructing Arrow arrays incrementally.

    Builders provide efficient, type-safe construction of columnar data
    with support for null values. Each builder is specialized for a specific
    data type (Int32, Int64, Float, String, etc.).

    For convenience, also provides high-level {!Builder.array_to_table} to
    convert OCaml arrays directly to tables without manual builder management.
*)

(** {1 Type System} *)

module Datatype = Datatype
(** Arrow data types - the type system for columnar data.

    Defines all supported Arrow types including integers (8-64 bit, signed/unsigned),
    floats (16-64 bit), strings (UTF-8), binary data, decimals, dates, timestamps,
    durations, and complex types like structs and maps.

    Each type specifies the physical layout and interpretation of data in memory.
*)

module Datetime = Datetime
(** Date and time types with nanosecond precision.

    Arrow stores temporal data as integers with specific units (days, milliseconds,
    microseconds, nanoseconds). This module provides conversions to/from Ptime,
    OCaml's standard time library.
*)

(** {1 Supporting Types} *)

module Compression = Compression
(** Compression codecs for Parquet and Feather files.

    Supported: None, Snappy, Gzip, Brotli, LZ4, Zstd.
    Snappy provides good balance of speed and compression for most workloads.
*)

module Valid = Valid
(** Validity bitmaps for tracking null values in arrays.

    Arrow uses compact bitmaps (1 bit per value) to track which values are null.
    This is more memory-efficient than OCaml option types for large datasets.
*)

(** {1 Advanced / Internal API} *)

module Wrapper = Wrapper
(** Low-level bindings to Arrow C++ API.

    {b Note:} This module exposes internal implementation details and direct
    C++ bindings. Most users should use the higher-level modules above.

    Provides direct access to Arrow internals for advanced use cases:
    - Schema inspection and manipulation
    - Direct bigarray access for zero-copy performance
    - Custom column read/write operations
    - Low-level table and array construction

    Use this module when you need maximum performance or capabilities not
    exposed by the high-level API.
*)

(** {1 Library Information} *)

val version : string
(** Version of this OCaml Arrow binding library *)
