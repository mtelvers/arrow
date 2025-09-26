(** Reading Arrow IPC/Feather files

    Feather (also called Arrow IPC format) is Arrow's native binary format.
    It's designed for fast reading and writing with minimal overhead.

    {1 When to Use Feather vs Parquet}

    - {b Feather}: Faster read/write, larger files, good for temporary/cache data
    - {b Parquet}: Better compression, widely compatible, good for long-term storage

    {1 Usage}

    {2 Read entire file}
    {[
      let table = File_reader.table "data.feather"
    ]}

    {2 Read specific columns}
    {[
      (* By column index *)
      let table = File_reader.table "data.feather"
        ~columns:(`Indexes [0; 2; 5]) in

      (* By column name *)
      let table = File_reader.table "data.feather"
        ~columns:(`Names ["id"; "name"; "score"])
    ]}

    {2 Inspect schema}
    {[
      let schema = File_reader.schema "data.feather" in
      Printf.printf "File has %d columns\n" (List.length schema.children)
    ]}
*)

val schema : string -> Wrapper.Schema.t
(** Read only the schema (column names and types) from a Feather file.

    This is very fast as it doesn't read any data, only metadata.

    @raise Failure if file cannot be opened or is not valid Feather/IPC format
*)

val table : ?columns:[ `Indexes of int list | `Names of string list ] -> string -> Table.t
(** Read a Feather/IPC file into memory as a table.

    @param columns Optionally select specific columns to read:
                   - [`Indexes [0; 2; 5]] reads columns 0, 2, and 5
                   - [`Names ["id"; "name"]] reads named columns
                   Default: read all columns

    Example:
    {[
      (* Read all columns *)
      let table = File_reader.table "cache.feather" in

      (* Read only specific columns by name *)
      let subset = File_reader.table "cache.feather"
        ~columns:(`Names ["user_id"; "timestamp"; "event"])
    ]}

    @raise Failure if file cannot be opened or is not valid Feather/IPC format
*)