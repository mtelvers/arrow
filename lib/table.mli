type t = Wrapper.Table.t

val concatenate : t list -> t
val slice : t -> offset:int -> length:int -> t
val num_rows : t -> int
val schema : t -> Wrapper.Schema.t
val read_csv : string -> t
val read_json : string -> t
val write_parquet : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
val write_feather : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
val to_string_debug : t -> string
val add_column : t -> string -> Wrapper.ChunkedArray.t -> t
val get_column : t -> string -> Wrapper.ChunkedArray.t
val add_all_columns : t -> t -> t

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

type writer_col

val create : writer_col list -> t
val named_col : packed_col -> name:string -> writer_col
val col : 'a array -> 'a col_type -> name:string -> writer_col
val col_opt : 'a option array -> 'a col_type -> name:string -> writer_col
val read : t -> column:Wrapper.Column.column -> 'a col_type -> 'a array
val read_opt : t -> column:Wrapper.Column.column -> 'a col_type -> 'a option array