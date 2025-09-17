type t = Wrapper.Table.t

type _ col_type =
  | Int : int col_type
  | Float : float col_type
  | Utf8 : string col_type
  | Date : Datetime.Date.t col_type
  | Time_ns : Datetime.Time_ns.t col_type
  | Span_ns : Datetime.Time_ns.Span.t col_type
  | Ofday_ns : Datetime.Time_ns.Ofday.t col_type
  | Bool : bool col_type

type packed_col =
  | P : 'a col_type * 'a array -> packed_col
  | O : 'a col_type * 'a option array -> packed_col

type writer_col = Wrapper.Writer.col

let concatenate = Wrapper.Table.concatenate
let slice = Wrapper.Table.slice
let num_rows = Wrapper.Table.num_rows
let schema = Wrapper.Table.schema
let read_csv = Wrapper.Table.read_csv
let read_json = Wrapper.Table.read_json
let write_parquet = Wrapper.Table.write_parquet
let write_feather = Wrapper.Table.write_feather
let to_string_debug = Wrapper.Table.to_string_debug
let add_column = Wrapper.Table.add_column
let get_column = Wrapper.Table.get_column
let add_all_columns = Wrapper.Table.add_all_columns

let create (cols : writer_col list) : t = 
  (* Use the actual Writer.create_table function *)
  Wrapper.Writer.create_table ~cols

let col (type a) (data : a array) (col_type : a col_type) ~name =
  match col_type with
  | Int -> Wrapper.Writer.int data ~name
  | Float -> Wrapper.Writer.float data ~name
  | Utf8 -> Wrapper.Writer.utf8 data ~name
  | Date -> Wrapper.Writer.date data ~name
  | Time_ns -> Wrapper.Writer.time_ns data ~name
  | Span_ns -> Wrapper.Writer.span_ns data ~name
  | Ofday_ns -> Wrapper.Writer.ofday_ns data ~name
  | Bool -> Wrapper.Writer.bitset (Valid.from_array data) ~name

let col_opt (type a) (data : a option array) (col_type : a col_type) ~name =
  match col_type with
  | Int -> Wrapper.Writer.int_opt data ~name
  | Float -> Wrapper.Writer.float_opt data ~name
  | Utf8 -> Wrapper.Writer.utf8_opt data ~name
  | Date -> Wrapper.Writer.date_opt data ~name
  | Time_ns -> Wrapper.Writer.time_ns_opt data ~name
  | Span_ns -> Wrapper.Writer.span_ns_opt data ~name
  | Ofday_ns -> Wrapper.Writer.ofday_ns_opt data ~name
  | Bool -> Wrapper.Writer.bitset_opt (Valid.from_array (Array.map (Option.value ~default:false) data)) ~valid:(Valid.from_array (Array.map Option.is_some data)) ~name

let named_col packed_col ~name =
  match packed_col with
  | P (typ_, data) -> col data typ_ ~name
  | O (typ_, data) -> col_opt data typ_ ~name

let read (type a) table ~column (col_type : a col_type) : a array =
  match col_type with
  | Int -> Wrapper.Column.read_int table ~column
  | Float -> Wrapper.Column.read_float table ~column
  | Utf8 -> Wrapper.Column.read_utf8 table ~column
  | Date -> Wrapper.Column.read_date table ~column
  | Time_ns -> Wrapper.Column.read_time_ns table ~column
  | Span_ns -> Wrapper.Column.read_span_ns table ~column
  | Ofday_ns -> Wrapper.Column.read_ofday_ns table ~column
  | Bool ->
    let bitset = Wrapper.Column.read_bitset table ~column in
    Valid.to_array bitset

let read_opt (type a) table ~column (col_type : a col_type) : a option array =
  match col_type with
  | Int -> Wrapper.Column.read_int_opt table ~column
  | Float -> Wrapper.Column.read_float_opt table ~column
  | Utf8 -> Wrapper.Column.read_utf8_opt table ~column
  | Date -> Wrapper.Column.read_date_opt table ~column
  | Time_ns -> Wrapper.Column.read_time_ns_opt table ~column
  | Span_ns -> Wrapper.Column.read_span_ns_opt table ~column
  | Ofday_ns -> Wrapper.Column.read_ofday_ns_opt table ~column
  | Bool ->
    let bitset, valid = Wrapper.Column.read_bitset_opt table ~column in
    let length = Valid.length bitset in
    Array.init length (fun i ->
      if Valid.get valid i then Some (Valid.get bitset i) else None)