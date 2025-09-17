let unknown_suffix filename =
  failwith (Printf.sprintf
    "cannot infer the file format from suffix %s (supported suffixes are csv/json/feather/parquet)"
    filename)

let get_file_extension filename =
  try
    let dot_index = String.rindex filename '.' in
    let extension = String.sub filename (dot_index + 1) (String.length filename - dot_index - 1) in
    Some extension
  with Not_found -> None

let schema filename =
  match get_file_extension filename with
  | Some "csv" -> Table.read_csv filename |> Table.schema
  | Some "json" -> Table.read_json filename |> Table.schema
  | Some "feather" -> Wrapper.Feather_reader.schema filename
  | Some "parquet" -> Wrapper.Parquet_reader.schema filename
  | Some _ | None -> unknown_suffix filename

let column_names_to_indexes col_names schema =
  let col_set = List.fold_left (fun acc name ->
    if List.mem name acc then acc else name :: acc
  ) [] col_names in
  let rec loop acc i = function
    | [] -> List.rev acc
    | child :: rest ->
      let col_name = child.Wrapper.Schema.name in
      let new_acc = if List.mem col_name col_set then i :: acc else acc in
      loop new_acc (i + 1) rest
  in
  loop [] 0 schema.Wrapper.Schema.children

let indexes columns ~filename =
  match columns with
  | `indexes indexes -> indexes
  | `names col_names ->
    let schema = schema filename in
    column_names_to_indexes col_names schema

let table ?columns filename =
  match get_file_extension filename with
  | Some "csv" -> Table.read_csv filename
  | Some "json" -> Table.read_json filename
  | Some "feather" ->
    let column_idxs = match columns with
      | Some cols -> Some (indexes cols ~filename)
      | None -> None
    in
    Wrapper.Feather_reader.table ?column_idxs filename
  | Some "parquet" ->
    let column_idxs = match columns with
      | Some cols -> Some (indexes cols ~filename)
      | None -> None
    in
    Wrapper.Parquet_reader.table ?column_idxs filename
  | Some _ | None -> unknown_suffix filename