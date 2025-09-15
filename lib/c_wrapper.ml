open Ctypes
module C = C_api.C

let add_compact = false

external use_value : 'a -> unit = "ctypes_use" [@@noalloc]

let ptr_of_string str =
  let len = String.length str in
  let carray = CArray.make char (1 + len) in
  for i = 0 to String.length str - 1 do
    CArray.set carray i str.[i]
  done;
  CArray.set carray len '\x00';
  CArray.start carray

let ptr_of_strings strings =
  let strings = List.map ptr_of_string strings in
  let start = CArray.(of_list (ptr char) strings |> start) in
  Gc.finalise (fun _ -> use_value strings) start;
  start

let get_string ptr_char =
  let rec loop acc p =
    let c = !@p in
    if Char.equal c '\000'
    then List.rev acc |> List.to_seq |> String.of_seq
    else loop (c :: acc) (p +@ 1)
  in
  loop [] ptr_char

module Schema = struct
  module Flags = struct
    type t = int

    let none = 0
    let all ts = List.fold_left (lor) 0 ts
    let dictionary_ordered_ = 1
    let nullable_ = 2
    let map_keys_sorted_ = 4
    let of_cint = Int64.to_int
    let to_cint = Int64.of_int
    let dictionary_ordered t = t land dictionary_ordered_ <> 0
    let nullable t = t land nullable_ <> 0
    let map_keys_sorted t = t land map_keys_sorted_ <> 0
  end

  type t =
    { format : Datatype.t
    ; name : string
    ; metadata : (string * string) list
    ; flags : Flags.t
    ; children : t list
    }

  let dereference_int32 ptr =
    if is_null ptr
    then failwith "got null ptr"
    else
      from_voidp int32_t (to_voidp ptr)
      |> ( !@ )
      |> Int32.to_int

  let metadata p =
    if is_null p
    then []
    else (
      let nfields = dereference_int32 p in
      let rec loop p acc = function
        | 0 -> List.rev acc
        | n ->
          let key_len = dereference_int32 p in
          let p = p +@ 4 in
          let key = string_from_ptr p ~length:key_len in
          let p = p +@ key_len in
          let value_len = dereference_int32 p in
          let p = p +@ 4 in
          let value = string_from_ptr p ~length:value_len in
          let p = p +@ value_len in
          loop p ((key, value) :: acc) (n - 1)
      in
      loop (p +@ 4) [] nfields)

  let of_c c_schema =
    Gc.finalise C.ArrowSchema.free c_schema;
    let rec loop c_schema =
      if is_null c_schema then failwith "Got a null schema";
      let schema = !@ c_schema in
      let n_children = getf schema C.ArrowSchema.n_children |> Int64.to_int in
      let children = getf schema C.ArrowSchema.children in
      let children = List.init n_children (fun i -> loop (!@(children +@ i))) in
      { format =
          getf schema C.ArrowSchema.format |> get_string |> Datatype.of_cstring
      ; name = getf schema C.ArrowSchema.name |> get_string
      ; metadata = getf schema C.ArrowSchema.metadata |> metadata
      ; flags = getf schema C.ArrowSchema.flags |> Flags.of_cint
      ; children
      }
    in
    loop c_schema
end

module ChunkedArray = struct
  type t = C.ChunkedArray.t

  let with_free t =
    Gc.finalise C.ChunkedArray.free t;
    t
end

module Table = struct
  type t = C.Table.t

  let schema t = C.Table.schema t |> Schema.of_c
  let num_rows t = C.Table.num_rows t |> Int64.to_int
  let to_string_debug = C.Table.to_string

  let with_free t =
    Gc.finalise C.Table.free t;
    t

  let concatenate ts =
    let array = CArray.of_list C.Table.t ts in
    let t =
      C.Table.concatenate (CArray.start array) (CArray.length array)
      |> with_free
    in
    use_value array;
    t

  let slice t ~offset ~length =
    C.Table.slice t (Int64.of_int offset) (Int64.of_int length) |> with_free

  let read_csv filename = C.csv_read_table filename |> with_free
  let read_json filename = C.json_read_table filename |> with_free

  let write_parquet
      ?(chunk_size = 1024 * 1024)
      ?(compression = Compression.Snappy)
      t
      filename
    =
    C.Table.parquet_write filename t chunk_size (Compression.to_int compression)

  let write_feather
      ?(chunk_size = 1024 * 1024)
      ?(compression = Compression.Snappy)
      t
      filename
    =
    C.Table.feather_write filename t chunk_size (Compression.to_int compression)

  let get_column t col_name = C.Table.get_column t col_name |> ChunkedArray.with_free
  let add_column t col_name array = C.Table.add_column t col_name array |> with_free
  let add_all_columns t t' = C.Table.add_all_columns t t' |> with_free
end

module Parquet_reader = struct
  type t = C.Parquet_reader.t

  let create
      ?use_threads
      ?(column_idxs = [])
      ?(mmap = false)
      ?(buffer_size = 0)
      ?(batch_size = 0)
      filename
    =
    let use_threads =
      match use_threads with
      | None -> -1
      | Some false -> 0
      | Some true -> 1
    in
    let column_idxs = CArray.of_list int column_idxs in
    let t =
      C.Parquet_reader.open_
        filename
        (CArray.start column_idxs)
        (CArray.length column_idxs)
        use_threads
        (if mmap then 1 else 0)
        buffer_size
        batch_size
    in
    Gc.finalise C.Parquet_reader.free t;
    t

  let next t =
    let table_ptr = C.Parquet_reader.next t in
    if is_null table_ptr then None else Table.with_free table_ptr |> Option.some

  let close = C.Parquet_reader.close

  let schema_and_num_rows filename =
    let num_rows = CArray.make int64_t 1 in
    let schema =
      C.Parquet_reader.schema filename (CArray.start num_rows) |> Schema.of_c
    in
    let num_rows = CArray.get num_rows 0 |> Int64.to_int in
    schema, num_rows

  let schema filename = schema_and_num_rows filename |> fst

  let table ?(only_first = -1) ?use_threads ?(column_idxs = []) filename =
    let use_threads =
      match use_threads with
      | None -> -1
      | Some false -> 0
      | Some true -> 1
    in
    let column_idxs = CArray.of_list int column_idxs in
    C.Parquet_reader.read_table
      filename
      (CArray.start column_idxs)
      (CArray.length column_idxs)
      use_threads
      (Int64.of_int only_first)
    |> Table.with_free
end

(* Stub implementations for missing modules - to be completed later *)
module Writer = struct
  type col = unit ptr

  let fixed_ba ~format:_ _array ~name:_ = from_voidp void null
  let fixed_ba_opt ~format:_ _array _valid ~name:_ = from_voidp void null
  let string_ba ~format:_ ~offsets:_ ~data:_ ~name:_ = from_voidp void null
  let string_ba_opt ~format:_ ~offsets:_ ~data:_ ~valid:_ ~name:_ = from_voidp void null
  let int64_ba _array ~name:_ = from_voidp void null
  let int64_ba_opt _array _valid ~name:_ = from_voidp void null
  let int32_ba _array ~name:_ = from_voidp void null
  let int32_ba_opt _array _valid ~name:_ = from_voidp void null
  let float64_ba _array ~name:_ = from_voidp void null
  let float64_ba_opt _array _valid ~name:_ = from_voidp void null
  let date _array ~name:_ = from_voidp void null
  let date_opt _array ~name:_ = from_voidp void null
  let time_ns _array ~name:_ = from_voidp void null
  let time_ns_opt _array ~name:_ = from_voidp void null
  let span_ns _array ~name:_ = from_voidp void null
  let span_ns_opt _array ~name:_ = from_voidp void null
  let ofday_ns _array ~name:_ = from_voidp void null
  let ofday_ns_opt _array ~name:_ = from_voidp void null
  let bitset _valid ~name:_ = from_voidp void null
  let bitset_opt _content ~valid:_ ~name:_ = from_voidp void null
  let utf8 _array ~name:_ = from_voidp void null
  let utf8_opt _array ~name:_ = from_voidp void null
  let int _array ~name:_ = from_voidp void null
  let int_opt _array ~name:_ = from_voidp void null
  let float _array ~name:_ = from_voidp void null
  let float_opt _array ~name:_ = from_voidp void null
  let write ?chunk_size:_ ?compression:_ _filename ~cols:_ = ()
  let create_table ~cols:_ = from_voidp void null
end

module DoubleBuilder = struct
  type t = unit ptr
  
  let create () = from_voidp void null
  let append _t _value = ()
  let append_null ?n:_ _t = ()
  let length _t = 0L
  let null_count _t = 0L
end

module Int32Builder = struct
  type t = unit ptr
  
  let create () = from_voidp void null
  let append _t _value = ()
  let append_null ?n:_ _t = ()
  let length _t = 0L
  let null_count _t = 0L
end

module Int64Builder = struct
  type t = unit ptr
  
  let create () = from_voidp void null
  let append _t _value = ()
  let append_null ?n:_ _t = ()
  let length _t = 0L
  let null_count _t = 0L
end

module StringBuilder = struct
  type t = unit ptr
  
  let create () = from_voidp void null
  let append _t _value = ()
  let append_null ?n:_ _t = ()
  let length _t = 0L
  let null_count _t = 0L
end

module Builder = struct
  type t =
    | Double of DoubleBuilder.t
    | Int32 of Int32Builder.t
    | Int64 of Int64Builder.t
    | String of StringBuilder.t

  let make_table _named_builders = from_voidp void null
end


(* Column data extraction - simplified version *)
module Column = struct
  type column = [`Name of string | `Index of int]
  
  module Datatype = struct
    type t = 
      | Int64 | Float64 | Utf8 | Date32 | Timestamp | Bool | Float32 | Int32 | Time64 | Duration
    
    let to_int = function
      | Int64 -> 0 | Float64 -> 1 | Utf8 -> 2 | Date32 -> 3 | Timestamp -> 4
      | Bool -> 5 | Float32 -> 6 | Int32 -> 7 | Time64 -> 8 | Duration -> 9
  end

  let with_column table dt ~column ~f =
    let n_chunks = CArray.make int 1 in
    let chunked_column =
      match column with
      | `Name column_name ->
        C.Table.chunked_column_by_name
          table
          column_name
          (CArray.start n_chunks)
          (Datatype.to_int dt)
      | `Index column_idx ->
        C.Table.chunked_column
          table
          column_idx
          (CArray.start n_chunks)
          (Datatype.to_int dt)
    in
    let n_chunks = CArray.get n_chunks 0 in
    Fun.protect
      ~finally:(fun () -> C.free_chunked_column chunked_column n_chunks)
      (fun () ->
        let chunks =
          List.init n_chunks (fun chunk_idx ->
              !@(chunked_column +@ chunk_idx))
        in
        f chunks)

  let num_rows chunks =
    List.fold_left (fun acc chunk ->
        let length = getf chunk C.ArrowArray.length in
        acc + Int64.to_int length) 0 chunks

  module Chunk = struct
    type t = 
      { offset : int
      ; buffers : unit ptr list  
      ; length : int
      ; null_count : int
      }

    let create chunk ~fail_on_null ~fail_on_offset =
      let null_count = getf chunk C.ArrowArray.null_count |> Int64.to_int in
      if fail_on_null && null_count <> 0 then
        failwith ("expected no null item but got " ^ string_of_int null_count);
      let offset = getf chunk C.ArrowArray.offset |> Int64.to_int in
      if fail_on_offset && offset <> 0 then
        failwith ("offsets are not supported for this column type, got " ^ string_of_int offset);
      let n_buffers = getf chunk C.ArrowArray.n_buffers |> Int64.to_int in
      let buffers_ptr = getf chunk C.ArrowArray.buffers in
      let buffers = List.init n_buffers (fun i -> !@(buffers_ptr +@ i)) in
      let length = getf chunk C.ArrowArray.length |> Int64.to_int in
      { offset; buffers; length; null_count }

    let primitive_data_ptr t ~ctype =
      match t.buffers with
      (* The first array is for the (optional) validity bitmap. *)
      | _bitmap :: data :: _ -> from_voidp ctype data +@ t.offset
      | buffers ->
        failwith ("expected 2 columns or more, got " ^ string_of_int (List.length buffers))
  end

  let read_utf8 table ~column =
    with_column table Utf8 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then
          [||]
        else (
          let dst = Array.make num_rows "" in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                (* Arrow UTF8 format:
                   - Buffer 0: validity bitmap (optional) 
                   - Buffer 1: offsets array (int32)
                   - Buffer 2: data array (char)
                *)
                let offsets = Chunk.primitive_data_ptr chunk ~ctype:int32_t in
                let data =
                  match chunk.buffers with
                  | [ _; _; data ] -> from_voidp char data
                  | _ -> failwith "expected 3 buffers for utf8"
                in
                for idx = 0 to chunk.length - 1 do
                  let str_offset = !@(offsets +@ idx) |> Int32.to_int in
                  let next_str_offset = !@(offsets +@ (idx + 1)) |> Int32.to_int in
                  let str =
                    string_from_ptr (data +@ str_offset)
                      ~length:(next_str_offset - str_offset)
                  in
                  dst.(dst_offset + idx) <- str
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))
        
  let read_i32_ba _table ~column:_ = Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout 0
  let read_i64_ba _table ~column:_ = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout 0 
  let read_f64_ba _table ~column:_ = Bigarray.Array1.create Bigarray.float64 Bigarray.c_layout 0
  let read_f32_ba _table ~column:_ = Bigarray.Array1.create Bigarray.float32 Bigarray.c_layout 0
  
  let read_int _table ~column:_ = [||]
  let read_int32 _table ~column:_ = [||] 
  let read_float _table ~column:_ = [||]
  let read_date _table ~column:_ = [||]
  let read_time_ns _table ~column:_ = [||]
  let read_ofday_ns _table ~column:_ = [||]
  let read_span_ns _table ~column:_ = [||]

  let read_i32_ba_opt _table ~column:_ = 
    (Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout 0, ())
  let read_i64_ba_opt _table ~column:_ = 
    (Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout 0, ())
  let read_f64_ba_opt _table ~column:_ = 
    (Bigarray.Array1.create Bigarray.float64 Bigarray.c_layout 0, ())
  let read_f32_ba_opt _table ~column:_ = 
    (Bigarray.Array1.create Bigarray.float32 Bigarray.c_layout 0, ())

  let read_int_opt _table ~column:_ = [||]
  let read_int32_opt _table ~column:_ = [||]
  let read_float_opt _table ~column:_ = [||]
  let read_utf8_opt _table ~column:_ = [||]
  let read_date_opt _table ~column:_ = [||]
  let read_time_ns_opt _table ~column:_ = [||]
  let read_ofday_ns_opt _table ~column:_ = [||] 
  let read_span_ns_opt _table ~column:_ = [||]

  type t =
    | Unsupported_type
    | String of string array
    | String_option of string option array
    | Int64 of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Int64_option of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * unit
    | Double of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Double_option of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * unit

  let fast_read _table _column_index = Unsupported_type
end