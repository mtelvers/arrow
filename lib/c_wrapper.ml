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

(* Writer module - mirrors Jane Street implementation *)
module Writer = struct
  (* Release functions for Arrow C Data Interface *)
  module Release_array_fn_ptr =
    (val Foreign.dynamic_funptr (ptr C.ArrowArray.t @-> returning void))

  module Release_schema_fn_ptr =
    (val Foreign.dynamic_funptr (ptr C.ArrowSchema.t @-> returning void))

  (* Proper release callbacks that follow Arrow C Data Interface spec *)
  let release_schema =
    let release_schema schema_ptr =
      (* Check if already released *)
      if not (is_null (getf (!@schema_ptr) C.ArrowSchema.release)) then (
        (* Mark as released by setting release to NULL *)
        setf (!@schema_ptr) C.ArrowSchema.release (null |> from_voidp void)
      )
    in
    Release_schema_fn_ptr.of_fun release_schema

  let release_array =
    let release_array array_ptr =
      (* Check if already released *)
      if not (is_null (getf (!@array_ptr) C.ArrowArray.release)) then (
        (* Mark as released by setting release to NULL *)
        setf (!@array_ptr) C.ArrowArray.release (null |> from_voidp void)
      )
    in
    Release_array_fn_ptr.of_fun release_array

  (* Keep the function pointers alive to prevent GC *)
  let keep_alive_internal = ref []
  let () = keep_alive_internal := [Obj.repr release_schema; Obj.repr release_array]

  let release_array_ptr = coerce Release_array_fn_ptr.t (ptr void) release_array
  let release_schema_ptr = coerce Release_schema_fn_ptr.t (ptr void) release_schema

  let empty_schema_l = CArray.of_list (ptr C.ArrowSchema.t) []
  let empty_array_l = CArray.of_list (ptr C.ArrowArray.t) []

  type col = C.ArrowArray.t * C.ArrowSchema.t

  let schema_struct ~format ~name ~children ~flag =
    let format = CArray.of_string format in
    let name = CArray.of_string name in
    let s = make C.ArrowSchema.t ~finalise:(fun _ ->
        use_value format;
        use_value name;
        use_value children)
    in
    setf s C.ArrowSchema.format (CArray.start format);
    setf s C.ArrowSchema.name (CArray.start name);
    setf s C.ArrowSchema.metadata (null |> from_voidp char);
    setf s C.ArrowSchema.flags (Schema.Flags.to_cint flag);
    setf s C.ArrowSchema.n_children (CArray.length children |> Int64.of_int);
    setf s C.ArrowSchema.children (CArray.start children);
    setf s C.ArrowSchema.dictionary (null |> from_voidp C.ArrowSchema.t);
    setf s C.ArrowSchema.release release_schema_ptr;
    s

  let array_struct ~null_count ~buffers ~children ~length ~finalise =
    let a = make ~finalise:(fun _ ->
        finalise ();
        use_value buffers;
        use_value children)
      C.ArrowArray.t
    in
    setf a C.ArrowArray.length (Int64.of_int length);
    setf a C.ArrowArray.null_count (Int64.of_int null_count);
    setf a C.ArrowArray.offset Int64.zero;
    setf a C.ArrowArray.n_buffers (CArray.length buffers |> Int64.of_int);
    setf a C.ArrowArray.buffers (CArray.start buffers);
    setf a C.ArrowArray.n_children (CArray.length children |> Int64.of_int);
    setf a C.ArrowArray.children (CArray.start children);
    setf a C.ArrowArray.dictionary (null |> from_voidp C.ArrowArray.t);
    setf a C.ArrowArray.release release_array_ptr;
    a

  (* Helper functions - Jane Street pattern *)
  let fixed_ba ~format array ~name =
    let buffers = CArray.of_list (ptr void)
        [ null; bigarray_start array1 array |> to_voidp ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct = schema_struct ~format ~name
        ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let fixed_ba_opt ~format array valid ~name =
    if Bigarray.Array1.dim array <> Valid.length valid then failwith "incoherent lengths";
    let buffers = CArray.of_list (ptr void)
        [ bigarray_start array1 (Valid.bigarray valid) |> to_voidp
        ; bigarray_start array1 array |> to_voidp
        ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct = schema_struct ~format ~name
        ~children:empty_schema_l ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let string_ba ~format ~offsets ~data ~name =
    let buffers = CArray.of_list (ptr void)
        [ null
        ; bigarray_start array1 offsets |> to_voidp
        ; bigarray_start array1 data |> to_voidp
        ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ ->
          use_value offsets;
          use_value data)
        ~length:(Bigarray.Array1.dim offsets - 1)
    in
    let schema_struct = schema_struct ~format ~name
        ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let string_ba_opt ~format ~offsets ~data ~valid ~name =
    if Bigarray.Array1.dim offsets - 1 <> Valid.length valid then failwith "incoherent lengths";
    let buffers = CArray.of_list (ptr void)
        [ bigarray_start array1 (Valid.bigarray valid) |> to_voidp
        ; bigarray_start array1 offsets |> to_voidp
        ; bigarray_start array1 data |> to_voidp
        ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value offsets;
          use_value data;
          use_value valid)
        ~length:(Bigarray.Array1.dim offsets - 1)
    in
    let schema_struct = schema_struct ~format ~name
        ~children:empty_schema_l ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  (* Real implementations using Jane Street pattern *)
  let int array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length array) in
    Array.iteri (fun i x -> ba.{i} <- Int64.of_int x) array;
    fixed_ba ~format:"l" ba ~name

  let int_opt array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length array) in
    let valid = Valid.create (Array.length array) in
    Array.iteri (fun i -> function
      | Some x -> ba.{i} <- Int64.of_int x; Valid.set_valid valid i
      | None -> ba.{i} <- 0L; Valid.set_invalid valid i
    ) array;
    fixed_ba_opt ~format:"l" ba valid ~name

  let float array ~name =
    let ba = Bigarray.Array1.create Bigarray.float64 Bigarray.c_layout (Array.length array) in
    Array.iteri (fun i x -> ba.{i} <- x) array;
    fixed_ba ~format:"g" ba ~name

  let float_opt array ~name =
    let ba = Bigarray.Array1.create Bigarray.float64 Bigarray.c_layout (Array.length array) in
    let valid = Valid.create (Array.length array) in
    Array.iteri (fun i -> function
      | Some x -> ba.{i} <- x; Valid.set_valid valid i
      | None -> ba.{i} <- 0.0; Valid.set_invalid valid i
    ) array;
    fixed_ba_opt ~format:"g" ba valid ~name

  let utf8 array ~name =
    let total_length = Array.fold_left (fun acc s -> acc + String.length s) 0 array in
    let offsets = Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout (Array.length array + 1) in
    let data = Bigarray.Array1.create Bigarray.char Bigarray.c_layout total_length in
    let current_offset = ref 0 in
    offsets.{0} <- 0l;
    Array.iteri (fun i s ->
      let len = String.length s in
      String.iteri (fun j c -> data.{!current_offset + j} <- c) s;
      current_offset := !current_offset + len;
      offsets.{i + 1} <- Int32.of_int !current_offset
    ) array;
    string_ba ~format:"u" ~offsets ~data ~name

  let utf8_opt array ~name =
    let total_length = Array.fold_left (fun acc -> function
      | Some s -> acc + String.length s | None -> acc) 0 array in
    let offsets = Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout (Array.length array + 1) in
    let data = Bigarray.Array1.create Bigarray.char Bigarray.c_layout total_length in
    let valid = Valid.create (Array.length array) in
    let current_offset = ref 0 in
    offsets.{0} <- 0l;
    Array.iteri (fun i -> function
      | Some s ->
        let len = String.length s in
        String.iteri (fun j c -> data.{!current_offset + j} <- c) s;
        current_offset := !current_offset + len;
        offsets.{i + 1} <- Int32.of_int !current_offset;
        Valid.set_valid valid i
      | None ->
        offsets.{i + 1} <- Int32.of_int !current_offset;
        Valid.set_invalid valid i
    ) array;
    string_ba_opt ~format:"u" ~offsets ~data ~valid ~name

  (* Bigarray convenience functions *)
  let int64_ba array ~name = fixed_ba ~format:"l" array ~name
  let int64_ba_opt array valid ~name = fixed_ba_opt ~format:"l" array valid ~name
  let int32_ba array ~name = fixed_ba ~format:"i" array ~name
  let int32_ba_opt array valid ~name = fixed_ba_opt ~format:"i" array valid ~name
  let float64_ba array ~name = fixed_ba ~format:"g" array ~name
  let float64_ba_opt array valid ~name = fixed_ba_opt ~format:"g" array valid ~name
  let date date_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout (Array.length date_array) in
    Array.iteri (fun i date ->
      let days = Datetime.Date.to_unix_days date |> Int32.of_int in
      ba.{i} <- days
    ) date_array;
    fixed_ba ~format:"tdD" ba ~name
  let date_opt date_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int32 Bigarray.c_layout (Array.length date_array) in
    let valid = Valid.create (Array.length date_array) in
    Array.iteri (fun i -> function
      | Some date ->
        let days = Datetime.Date.to_unix_days date |> Int32.of_int in
        ba.{i} <- days; Valid.set_valid valid i
      | None -> ba.{i} <- 0l; Valid.set_invalid valid i
    ) date_array;
    fixed_ba_opt ~format:"tdD" ba valid ~name
  let time_ns time_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length time_array) in
    Array.iteri (fun i time ->
      let ns = Datetime.Time_ns.to_int64_ns_since_epoch time in
      ba.{i} <- ns
    ) time_array;
    fixed_ba ~format:"tsn:UTC" ba ~name
  let time_ns_opt time_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length time_array) in
    let valid = Valid.create (Array.length time_array) in
    Array.iteri (fun i -> function
      | Some time ->
        let ns = Datetime.Time_ns.to_int64_ns_since_epoch time in
        ba.{i} <- ns; Valid.set_valid valid i
      | None -> ba.{i} <- 0L; Valid.set_invalid valid i
    ) time_array;
    fixed_ba_opt ~format:"tsn:UTC" ba valid ~name
  let span_ns span_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length span_array) in
    Array.iteri (fun i span ->
      let ns = Datetime.Time_ns.Span.to_ns span in
      ba.{i} <- ns
    ) span_array;
    fixed_ba ~format:"tDn" ba ~name
  let span_ns_opt span_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length span_array) in
    let valid = Valid.create (Array.length span_array) in
    Array.iteri (fun i -> function
      | Some span ->
        let ns = Datetime.Time_ns.Span.to_ns span in
        ba.{i} <- ns; Valid.set_valid valid i
      | None -> ba.{i} <- 0L; Valid.set_invalid valid i
    ) span_array;
    fixed_ba_opt ~format:"tDn" ba valid ~name
  let ofday_ns ofday_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length ofday_array) in
    Array.iteri (fun i ofday ->
      let ns = Datetime.Time_ns.Ofday.to_ns_since_midnight ofday in
      ba.{i} <- ns
    ) ofday_array;
    fixed_ba ~format:"ttn" ba ~name
  let ofday_ns_opt ofday_array ~name =
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout (Array.length ofday_array) in
    let valid = Valid.create (Array.length ofday_array) in
    Array.iteri (fun i -> function
      | Some ofday ->
        let ns = Datetime.Time_ns.Ofday.to_ns_since_midnight ofday in
        ba.{i} <- ns; Valid.set_valid valid i
      | None -> ba.{i} <- 0L; Valid.set_invalid valid i
    ) ofday_array;
    fixed_ba_opt ~format:"ttn" ba valid ~name
  let bitset valid ~name =
    let array = Valid.bigarray valid in
    let buffers = CArray.of_list (ptr void)
        [ null; bigarray_start array1 array |> to_voidp ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Valid.length valid)
    in
    let schema_struct = schema_struct ~format:"b" ~name
        ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)
  let bitset_opt content ~valid ~name =
    if Valid.length content <> Valid.length valid then failwith "incoherent lengths";
    let array = Valid.bigarray content in
    let buffers = CArray.of_list (ptr void)
        [ bigarray_start array1 (Valid.bigarray valid) |> to_voidp
        ; bigarray_start array1 array |> to_voidp
        ]
    in
    let array_struct = array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Valid.length content)
    in
    let schema_struct = schema_struct ~format:"b" ~name
        ~children:empty_schema_l ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let rec write ?chunk_size ?compression filename ~cols =
    (* Use the actual C++ parquet_write_table function *)
    let chunk_size = Option.value chunk_size ~default:(1024 * 1024) in
    let compression = Option.value compression ~default:Compression.Snappy |> Compression.to_int in

    (* Create table from columns using make_table *)
    let table = create_table ~cols in

    (* Write table using C++ function *)
    C.Table.parquet_write filename table chunk_size compression

  and create_table ~cols =
    (* Create a proper table with struct schema containing columns as children *)
    let n_cols = List.length cols in
    if n_cols = 0 then
      C.csv_read_table "" |> Table.with_free
    else (
      (* Create arrays and schemas for all columns *)
      let (arrays, schemas) = List.split cols in

      (* Create child schema array *)
      let schema_ptrs = List.map addr schemas in
      let children_schemas = CArray.of_list (ptr C.ArrowSchema.t) schema_ptrs in

      (* Create child array array *)
      let array_ptrs = List.map addr arrays in
      let children_arrays = CArray.of_list (ptr C.ArrowArray.t) array_ptrs in

      (* Create table-level struct schema *)
      let table_schema = schema_struct
        ~format:"+s"
        ~name:""
        ~children:children_schemas
        ~flag:Schema.Flags.none in

      (* Get length from first array (all should have same length) *)
      let length = match arrays with
        | [] -> 0
        | array :: _ -> getf array C.ArrowArray.length |> Int64.to_int
      in

      (* Create table-level array struct - struct types need validity buffer *)
      let table_array = array_struct
        ~buffers:(CArray.of_list (ptr void) [null])
        ~children:children_arrays
        ~null_count:0
        ~length
        ~finalise:(fun _ -> ()) in

      C.Table.create (addr table_array) (addr table_schema) |> Table.with_free
    )
end

module DoubleBuilder = struct
  type t = C.DoubleBuilder.t
  
  let create () = 
    let builder = C.DoubleBuilder.create () in
    Gc.finalise C.DoubleBuilder.free builder;
    builder
    
  let append t value = C.DoubleBuilder.append t value
  let append_null ?(n=1) t = C.DoubleBuilder.append_null t n
  let length t = C.DoubleBuilder.length t
  let null_count t = C.DoubleBuilder.null_count t
end

module Int32Builder = struct
  type t = C.Int32Builder.t
  
  let create () = 
    let builder = C.Int32Builder.create () in
    Gc.finalise C.Int32Builder.free builder;
    builder
    
  let append t value = C.Int32Builder.append t value
  let append_null ?(n=1) t = C.Int32Builder.append_null t n
  let length t = C.Int32Builder.length t
  let null_count t = C.Int32Builder.null_count t
end

module Int64Builder = struct
  type t = C.Int64Builder.t
  
  let create () = 
    let builder = C.Int64Builder.create () in
    Gc.finalise C.Int64Builder.free builder;
    builder
    
  let append t value = C.Int64Builder.append t value
  let append_null ?(n=1) t = C.Int64Builder.append_null t n
  let length t = C.Int64Builder.length t
  let null_count t = C.Int64Builder.null_count t
end

module StringBuilder = struct
  type t = C.StringBuilder.t
  
  let create () = 
    let builder = C.StringBuilder.create () in
    Gc.finalise C.StringBuilder.free builder;
    builder
    
  let append t value = C.StringBuilder.append t value
  let append_null ?(n=1) t = C.StringBuilder.append_null t n
  let length t = C.StringBuilder.length t
  let null_count t = C.StringBuilder.null_count t
end

module Builder = struct
  type t =
    | Double of DoubleBuilder.t
    | Int32 of Int32Builder.t
    | Int64 of Int64Builder.t
    | String of StringBuilder.t

  let make_table named_builders =
    let n = List.length named_builders in
    if n = 0 then
      (* Return empty table for no builders *)
      C.csv_read_table "" |> Table.with_free
    else (
      let builders = Array.make n (from_voidp void null) in
      let names = Array.make n (from_voidp char null) in
      List.iteri (fun i (name, builder) ->
        let builder_ptr = match builder with
          | Double b -> to_voidp b
          | Int32 b -> to_voidp b
          | Int64 b -> to_voidp b
          | String b -> to_voidp b
        in
        builders.(i) <- builder_ptr;
        let name_ptr = ptr_of_string name in
        names.(i) <- name_ptr
      ) named_builders;
      let builders_array = CArray.of_list (ptr void) (Array.to_list builders) in
      let names_array = CArray.of_list (ptr char) (Array.to_list names) in
      C.make_table 
        (CArray.start builders_array)
        (CArray.start names_array)
        n
      |> Table.with_free
    )
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

  let read_int table ~column =
    with_column table Int64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows 0 in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                for idx = 0 to chunk.length - 1 do
                  let value = !@(data +@ idx) |> Int64.to_int in
                  dst.(dst_offset + idx) <- value
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_int32 table ~column =
    with_column table Int32 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows Int32.zero in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int32_t in
                for idx = 0 to chunk.length - 1 do
                  let value = !@(data +@ idx) in
                  dst.(dst_offset + idx) <- value
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_float table ~column =
    with_column table Float64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows 0.0 in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:double in
                for idx = 0 to chunk.length - 1 do
                  let value = !@(data +@ idx) in
                  dst.(dst_offset + idx) <- value
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_int_opt table ~column = 
    with_column table Int64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  (* All nulls - dst already initialized with None *)
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                  for idx = 0 to chunk.length - 1 do
                    let value = !@(data +@ idx) |> Int64.to_int in
                    dst.(dst_offset + idx) <- Some value
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_int32_opt table ~column =
    with_column table Int32 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  (* All nulls - dst already initialized with None *)
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int32_t in
                  for idx = 0 to chunk.length - 1 do
                    let value = !@(data +@ idx) in
                    dst.(dst_offset + idx) <- Some value
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_float_opt table ~column = 
    with_column table Float64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  (* All nulls - dst already initialized with None *)
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:double in
                  for idx = 0 to chunk.length - 1 do
                    let value = !@(data +@ idx) in
                    dst.(dst_offset + idx) <- Some value
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_utf8_opt table ~column = 
    with_column table Utf8 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  (* All nulls - dst already initialized with None *)
                  dst_offset + chunk.length
                else (
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
                    dst.(dst_offset + idx) <- Some str
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

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

  let of_chunks chunks ~kind ~ctype =
    let num_rows = num_rows chunks in
    let dst = Bigarray.Array1.create kind Bigarray.c_layout num_rows in
    let _num_rows =
      List.fold_left (fun dst_offset chunk ->
          let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
          let ptr = Chunk.primitive_data_ptr chunk ~ctype in
          let dst_sub = Bigarray.Array1.sub dst dst_offset chunk.length in
          let src = bigarray_of_ptr array1 chunk.length kind ptr in
          Bigarray.Array1.blit src dst_sub;
          dst_offset + chunk.length) 0 chunks
    in
    dst

  let read_ba table ~datatype ~kind ~ctype ~column =
    with_column table datatype ~column ~f:(of_chunks ~kind ~ctype)

  let read_ba_opt table ~datatype ~kind ~ctype ~column =
    with_column table datatype ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Bigarray.Array1.create kind Bigarray.c_layout num_rows in
        let valid = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold_left (fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:true in
              if chunk.null_count = chunk.length then (
                for i = 0 to chunk.length - 1 do
                  Valid.set valid (dst_offset + i) false
                done;
                dst_offset + chunk.length
              ) else (
                let ptr = Chunk.primitive_data_ptr chunk ~ctype in
                let dst_sub = Bigarray.Array1.sub dst dst_offset chunk.length in
                let src = bigarray_of_ptr array1 chunk.length kind ptr in
                Bigarray.Array1.blit src dst_sub;
                dst_offset + chunk.length
              )) 0 chunks
        in
        dst, valid)

  let read_i32_ba table ~column = read_ba table ~datatype:Int32 ~kind:Bigarray.int32 ~ctype:int32_t ~column
  let read_i64_ba table ~column = read_ba table ~datatype:Int64 ~kind:Bigarray.int64 ~ctype:int64_t ~column 
  let read_f64_ba table ~column = read_ba table ~datatype:Float64 ~kind:Bigarray.float64 ~ctype:double ~column
  let read_f32_ba table ~column = read_ba table ~datatype:Float32 ~kind:Bigarray.float32 ~ctype:float ~column
  

  let read_i32_ba_opt table ~column = read_ba_opt table ~datatype:Int32 ~kind:Bigarray.int32 ~ctype:int32_t ~column
  let read_i64_ba_opt table ~column = read_ba_opt table ~datatype:Int64 ~kind:Bigarray.int64 ~ctype:int64_t ~column
  let read_f64_ba_opt table ~column = read_ba_opt table ~datatype:Float64 ~kind:Bigarray.float64 ~ctype:double ~column
  let read_f32_ba_opt table ~column = read_ba_opt table ~datatype:Float32 ~kind:Bigarray.float32 ~ctype:float ~column


  type t =
    | Unsupported_type
    | String of string array
    | String_option of string option array
    | Int64 of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Int64_option of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba
    | Double of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Double_option of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba

  let read_date table ~column =
    with_column table Date32 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows (Datetime.Date.of_unix_days 0) in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int32_t in
                for idx = 0 to chunk.length - 1 do
                  let days = !@(data +@ idx) |> Int32.to_int in
                  dst.(dst_offset + idx) <- Datetime.Date.of_unix_days days
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_time_ns table ~column =
    with_column table Time64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows (Datetime.Time_ns.of_int64_ns_since_epoch 0L) in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                for idx = 0 to chunk.length - 1 do
                  let ns = !@(data +@ idx) in
                  dst.(dst_offset + idx) <- Datetime.Time_ns.of_int64_ns_since_epoch ns
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_span_ns table ~column =
    with_column table Duration ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows (Datetime.Time_ns.Span.of_ns 0L) in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                for idx = 0 to chunk.length - 1 do
                  let ns = !@(data +@ idx) in
                  dst.(dst_offset + idx) <- Datetime.Time_ns.Span.of_ns ns
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_ofday_ns table ~column =
    with_column table Time64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows (Datetime.Time_ns.Ofday.of_ns_since_midnight 0L) in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
                let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                for idx = 0 to chunk.length - 1 do
                  let ns = !@(data +@ idx) in
                  dst.(dst_offset + idx) <- Datetime.Time_ns.Ofday.of_ns_since_midnight ns
                done;
                dst_offset + chunk.length) 0 chunks
          in
          dst
        ))

  let read_date_opt table ~column =
    with_column table Date32 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int32_t in
                  for idx = 0 to chunk.length - 1 do
                    let days = !@(data +@ idx) |> Int32.to_int in
                    dst.(dst_offset + idx) <- Some (Datetime.Date.of_unix_days days)
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_time_ns_opt table ~column =
    with_column table Time64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                  for idx = 0 to chunk.length - 1 do
                    let ns = !@(data +@ idx) in
                    dst.(dst_offset + idx) <- Some (Datetime.Time_ns.of_int64_ns_since_epoch ns)
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_span_ns_opt table ~column =
    with_column table Duration ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                  for idx = 0 to chunk.length - 1 do
                    let ns = !@(data +@ idx) in
                    dst.(dst_offset + idx) <- Some (Datetime.Time_ns.Span.of_ns ns)
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_ofday_ns_opt table ~column =
    with_column table Time64 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        if num_rows = 0 then [||]
        else (
          let dst = Array.make num_rows None in
          let _num_rows =
            List.fold_left (fun dst_offset chunk ->
                let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:false in
                if chunk.null_count = chunk.length then
                  dst_offset + chunk.length
                else (
                  let data = Chunk.primitive_data_ptr chunk ~ctype:int64_t in
                  for idx = 0 to chunk.length - 1 do
                    let ns = !@(data +@ idx) in
                    dst.(dst_offset + idx) <- Some (Datetime.Time_ns.Ofday.of_ns_since_midnight ns)
                  done;
                  dst_offset + chunk.length
                )) 0 chunks
          in
          dst
        ))

  let read_bitset table ~column =
    with_column table Bool ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let bitset = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold_left (fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:true in
              let ptr_ = Chunk.primitive_data_ptr chunk ~ctype:uint8_t in
              for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                let byte = !@(ptr_ +@ bidx) |> Unsigned.UInt8.to_int in
                let max_idx = min 8 (chunk.length - (8 * bidx)) in
                for bit_idx = 0 to max_idx - 1 do
                  let global_idx = dst_offset + (8 * bidx) + bit_idx in
                  let bit_value = (byte lsr bit_idx) land 1 = 1 in
                  Valid.set bitset global_idx bit_value
                done
              done;
              dst_offset + chunk.length) 0 chunks
        in
        bitset)

  let read_bitset_opt table ~column =
    with_column table Bool ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let bitset = Valid.create_all_valid num_rows in
        let valid = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold_left (fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:true in
              if chunk.null_count = chunk.length then (
                (* All nulls - mark all as invalid *)
                for i = 0 to chunk.length - 1 do
                  Valid.set valid (dst_offset + i) false
                done;
                dst_offset + chunk.length
              ) else (
                let ptr_ = Chunk.primitive_data_ptr chunk ~ctype:uint8_t in
                for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                  let byte = !@(ptr_ +@ bidx) |> Unsigned.UInt8.to_int in
                  let max_idx = min 8 (chunk.length - (8 * bidx)) in
                  for bit_idx = 0 to max_idx - 1 do
                    let global_idx = dst_offset + (8 * bidx) + bit_idx in
                    let bit_value = (byte lsr bit_idx) land 1 = 1 in
                    Valid.set bitset global_idx bit_value;
                    Valid.set valid global_idx true
                  done
                done;
                dst_offset + chunk.length
              )) 0 chunks
        in
        bitset, valid)

  let fast_read table column_index =
    try
      (* Try to determine column type by attempting reads *)
      let column = `Index column_index in

      (* First try reading as String *)
      try
        let str_array = read_utf8 table ~column in
        String str_array
      with _ ->
        (* Try reading as Int64 *)
        try
          let int64_ba = read_i64_ba table ~column in
          Int64 int64_ba
        with _ ->
          (* Try reading as Double *)
          try
            let double_ba = read_f64_ba table ~column in
            Double double_ba
          with _ ->
            (* Try reading as optional String *)
            try
              let str_opt_array = read_utf8_opt table ~column in
              String_option str_opt_array
            with _ ->
              (* Try reading as optional Int64 *)
              try
                let int64_ba, valid = read_i64_ba_opt table ~column in
                Int64_option (int64_ba, Valid.bigarray valid)
              with _ ->
                (* Try reading as optional Double *)
                try
                  let double_ba, valid = read_f64_ba_opt table ~column in
                  Double_option (double_ba, Valid.bigarray valid)
                with _ ->
                  Unsupported_type
    with _ ->
      Unsupported_type
end