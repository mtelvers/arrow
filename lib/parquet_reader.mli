(** Reading Apache Parquet files efficiently

    Parquet is a columnar storage format optimized for analytics workloads.
    This module provides both streaming (for large files) and batch reading.

    {1 Usage Patterns}

    {2 Simple: Load entire file into memory}
    {[
      let table = Parquet_reader.table "data.parquet" in
      let num_rows = Wrapper.Table.num_rows table
    ]}

    {2 Memory-efficient: Stream large files in batches}
    {[
      let sum = Parquet_reader.fold_batches "large.parquet"
        ~batch_size:10_000
        ~init:0
        ~f:(fun acc batch ->
          let col = Wrapper.Column.read_int batch ~column:(`Index 0) in
          acc + Array.fold_left (+) 0 col
        )
    ]}

    {2 Process with side effects}
    {[
      Parquet_reader.iter_batches "data.parquet"
        ~batch_size:50_000
        ~f:(fun batch ->
          (* Process each batch, e.g., write to database *)
          let ids = Wrapper.Column.read_int batch ~column:(`Name "id") in
          Array.iter insert_to_db ids
        )
    ]}

    {2 Parallel: Multi-threaded decoding}
    {[
      (* Use multiple threads for decompression and decoding *)
      let table = Parquet_reader.table "data.parquet"
        ~use_threads:true
    ]}

    {2 Selective: Read only specific columns}
    {[
      (* Only decode columns 0 and 2, skip others *)
      let table = Parquet_reader.table "data.parquet"
        ~column_idxs:[0; 2]
    ]}

    {1 Performance Tips}

    - Use [~use_threads:true] for large files with compression
    - Use [~column_idxs] to skip unnecessary columns (saves I/O and decode time)
    - Use [~mmap:true] on fast storage (SSD) for better performance
    - Choose [batch_size] based on available memory (10k-100k rows typical)
    - For small files, [table] is simplest and fastest
*)

type t
(** Handle for streaming Parquet file reading *)

val create
  :  ?use_threads:bool        (** Enable multi-threaded decoding (default: false) *)
  -> ?column_idxs:int list    (** Only read specific columns by index (default: all) *)
  -> ?mmap:bool               (** Use memory-mapped I/O (default: false) *)
  -> ?buffer_size:int         (** I/O buffer size in bytes (default: system dependent) *)
  -> ?batch_size:int          (** Rows per batch for streaming (default: 64k) *)
  -> string                   (** File path *)
  -> t
(** Create a streaming reader for incremental processing of Parquet files.

    Use {!next} to read batches one at a time. Remember to {!close} when done
    to free resources.

    The reader is stateful - each call to {!next} advances to the next batch.

    @raise Failure if file cannot be opened or is not valid Parquet
*)

val next : t -> Wrapper.Table.t option
(** Read the next batch from the Parquet file.

    Returns [Some table] with the next batch of rows, or [None] when the
    end of file is reached.

    The number of rows in each batch is controlled by [batch_size] parameter
    in {!create}.
*)

val close : t -> unit
(** Close the reader and free associated resources.

    After calling [close], the reader cannot be used anymore.
    It's safe to call [close] multiple times.
*)

val iter_batches
  :  ?use_threads:bool        (** Enable multi-threaded decoding (default: false) *)
  -> ?column_idxs:int list    (** Only read specific columns by index (default: all) *)
  -> ?mmap:bool               (** Use memory-mapped I/O (default: false) *)
  -> ?buffer_size:int         (** I/O buffer size in bytes *)
  -> ?batch_size:int          (** Rows per batch (default: 64k) *)
  -> string                   (** File path *)
  -> f:(Wrapper.Table.t -> unit)  (** Function to process each batch *)
  -> unit
(** Iterate over batches of a Parquet file with a side-effecting function.

    Automatically handles opening, reading, and closing the file.
    Useful for processing large files that don't fit in memory.

    Example:
    {[
      Parquet_reader.iter_batches "large.parquet"
        ~batch_size:10_000
        ~f:(fun batch ->
          Printf.printf "Processing batch with %d rows\n"
            (Wrapper.Table.num_rows batch)
        )
    ]}

    @raise Failure if file cannot be opened or is not valid Parquet
*)

val fold_batches
  :  ?use_threads:bool        (** Enable multi-threaded decoding (default: false) *)
  -> ?column_idxs:int list    (** Only read specific columns by index (default: all) *)
  -> ?mmap:bool               (** Use memory-mapped I/O (default: false) *)
  -> ?buffer_size:int         (** I/O buffer size in bytes *)
  -> ?batch_size:int          (** Rows per batch (default: 64k) *)
  -> string                   (** File path *)
  -> init:'a                  (** Initial accumulator value *)
  -> f:('a -> Wrapper.Table.t -> 'a)  (** Fold function *)
  -> 'a
(** Fold over batches of a Parquet file, accumulating a result.

    Similar to {!iter_batches} but functional style with an accumulator.
    Automatically handles opening, reading, and closing the file.

    Example - compute sum of a column:
    {[
      let total = Parquet_reader.fold_batches "data.parquet"
        ~init:0
        ~f:(fun acc batch ->
          let values = Wrapper.Column.read_int batch ~column:(`Name "amount") in
          acc + Array.fold_left (+) 0 values
        )
    ]}

    @raise Failure if file cannot be opened or is not valid Parquet
*)

val schema : string -> Wrapper.Schema.t
(** Read only the schema (column names and types) from a Parquet file.

    This is very fast as it doesn't read any data, only metadata.
    Useful for inspecting file structure before deciding what to read.

    @raise Failure if file cannot be opened or is not valid Parquet
*)

val schema_and_num_rows : string -> Wrapper.Schema.t * int
(** Read schema and total row count from a Parquet file.

    Like {!schema} but also returns the total number of rows in the file.
    Still very fast as it only reads metadata.

    @raise Failure if file cannot be opened or is not valid Parquet
*)

val table
  :  ?only_first:int          (** Read only first N rows (default: all) *)
  -> ?use_threads:bool        (** Enable multi-threaded decoding (default: false) *)
  -> ?column_idxs:int list    (** Only read specific columns by index (default: all) *)
  -> string                   (** File path *)
  -> Wrapper.Table.t
(** Read an entire Parquet file into memory as a single table.

    This is the simplest way to read Parquet files when the data fits in memory.

    Use [~only_first] to read just a preview of the data (useful for testing).
    Use [~column_idxs] to read only specific columns by position.

    Example:
    {[
      (* Read everything *)
      let table = Parquet_reader.table "data.parquet" in

      (* Read only first 1000 rows *)
      let preview = Parquet_reader.table "data.parquet" ~only_first:1000 in

      (* Read only columns 0, 2, and 5 *)
      let subset = Parquet_reader.table "data.parquet" ~column_idxs:[0; 2; 5]
    ]}

    @raise Failure if file cannot be opened or is not valid Parquet
*)