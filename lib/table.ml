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

type writer_col = unit

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

let create (_cols : writer_col list) : t = Wrapper.Table.read_csv ""

let named_col _packed_col ~name:_ = ()

let col _array _col_type ~name:_ = ()

let col_opt _option_array _col_type ~name:_ = ()

let read _table ~column:_ _col_type = [||]

let read_opt _table ~column:_ _col_type = [||]