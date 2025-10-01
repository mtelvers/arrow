module type Intf = sig
  type t
  type elem

  val create : unit -> t
  val append : t -> elem -> unit
  val append_null : ?n:int -> t -> unit
  val append_opt : t -> elem option -> unit
  val length : t -> int
  val null_count : t -> int
end

module Double = struct
  include Wrapper.DoubleBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int
  let null_count t = null_count t |> Int64.to_int
end

module String = struct
  include Wrapper.StringBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int
  let null_count t = null_count t |> Int64.to_int
end

module NativeInt = struct
  include Wrapper.Int64Builder

  let append t v = append t (Int64.of_int v)

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int
  let null_count t = null_count t |> Int64.to_int
end

module Int32 = struct
  include Wrapper.Int32Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int
  let null_count t = null_count t |> Int64.to_int
end

module Int64 = struct
  include Wrapper.Int64Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int
  let null_count t = null_count t |> Int64.to_int
end

module Int8 = struct
  include C_wrapper.Int8Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Int8Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Int8Builder.null_count t |> Stdlib.Int64.to_int
end

module Int16 = struct
  include C_wrapper.Int16Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Int16Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Int16Builder.null_count t |> Stdlib.Int64.to_int
end

module UInt8 = struct
  include C_wrapper.UInt8Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.UInt8Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.UInt8Builder.null_count t |> Stdlib.Int64.to_int
end

module UInt16 = struct
  include C_wrapper.UInt16Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.UInt16Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.UInt16Builder.null_count t |> Stdlib.Int64.to_int
end

module UInt32 = struct
  include C_wrapper.UInt32Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.UInt32Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.UInt32Builder.null_count t |> Stdlib.Int64.to_int
end

module UInt64 = struct
  include C_wrapper.UInt64Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.UInt64Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.UInt64Builder.null_count t |> Stdlib.Int64.to_int
end

module Float = struct
  include C_wrapper.FloatBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.FloatBuilder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.FloatBuilder.null_count t |> Stdlib.Int64.to_int
end

module Boolean = struct
  include C_wrapper.BooleanBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.BooleanBuilder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.BooleanBuilder.null_count t |> Stdlib.Int64.to_int
end

module Date32 = struct
  include C_wrapper.Date32Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Date32Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Date32Builder.null_count t |> Stdlib.Int64.to_int
end

module Date64 = struct
  include C_wrapper.Date64Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Date64Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Date64Builder.null_count t |> Stdlib.Int64.to_int
end

module Time32 = struct
  type t = C_wrapper.Time32Builder.t

  let create () = C_wrapper.Time32Builder.create ()
  let append = C_wrapper.Time32Builder.append
  let append_null = C_wrapper.Time32Builder.append_null

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Time32Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Time32Builder.null_count t |> Stdlib.Int64.to_int
end

module Time64 = struct
  type t = C_wrapper.Time64Builder.t

  let create () = C_wrapper.Time64Builder.create ()
  let append = C_wrapper.Time64Builder.append
  let append_null = C_wrapper.Time64Builder.append_null

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.Time64Builder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.Time64Builder.null_count t |> Stdlib.Int64.to_int
end

module Timestamp = struct
  type t = C_wrapper.TimestampBuilder.t

  let create () = C_wrapper.TimestampBuilder.create ()
  let append = C_wrapper.TimestampBuilder.append
  let append_null = C_wrapper.TimestampBuilder.append_null

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.TimestampBuilder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.TimestampBuilder.null_count t |> Stdlib.Int64.to_int
end

module Duration = struct
  type t = C_wrapper.DurationBuilder.t

  let create () = C_wrapper.DurationBuilder.create ()
  let append = C_wrapper.DurationBuilder.append
  let append_null = C_wrapper.DurationBuilder.append_null

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = C_wrapper.DurationBuilder.length t |> Stdlib.Int64.to_int
  let null_count t = C_wrapper.DurationBuilder.null_count t |> Stdlib.Int64.to_int
end

let make_table = Wrapper.Builder.make_table

(* Simple row-based construction - no PPX dependencies *)
type ('row, 'elem, 'col_type) col =
  { name : string
  ; get : 'row -> 'elem
  ; col_type : 'col_type Table.col_type
  }

type 'row packed_col =
  | P : ('row, 'elem, 'elem) col -> 'row packed_col
  | O : ('row, 'elem option, 'elem) col -> 'row packed_col

type 'row packed_cols = 'row packed_col list

let col ?name col_type get_fn =
  let name = match name with Some n -> n | None -> "col" in
  [ P { name; get = get_fn; col_type } ]

let col_opt ?name col_type get_fn =
  let name = match name with Some n -> n | None -> "col" in
  [ O { name; get = get_fn; col_type } ]

let array_to_table packed_cols rows =
  let cols =
    List.map (fun packed_col ->
      match packed_col with
      | P { name; get; col_type } -> Table.col (Array.map get rows) col_type ~name
      | O { name; get; col_type } ->
        Table.col_opt (Array.map get rows) col_type ~name
    ) packed_cols
  in
  Table.create cols

module type Row_intf = sig
  type row
  val array_to_table : row array -> Table.t
end

module type Row_builder_intf = sig
  type t
  type row

  val create : unit -> t
  val append : t -> row -> unit
  val length : t -> int
  val reset : t -> unit
  val to_table : t -> Table.t
end

module Row (R : Row_intf) = struct
  type row = R.row

  type t =
    { mutable data : row list
    ; mutable length : int
    }

  let create () = { data = []; length = 0 }

  let append t row =
    t.data <- row :: t.data;
    t.length <- t.length + 1

  let to_table t = Array.of_list (List.rev t.data) |> R.array_to_table
  let length t = t.length

  let reset t =
    t.data <- [];
    t.length <- 0
end