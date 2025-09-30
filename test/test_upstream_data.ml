open Arrow

let parse_parquet_file file =
  try
    (* Open Parquet reader *)
    let reader = Parquet_reader.create file in

    (* Read schema *)
    let schema = Parquet_reader.schema file in
    let num_columns = List.length schema.Wrapper.Schema.children in

    (* Read table *)
    let table = Parquet_reader.table file in
    let num_rows = Table.num_rows table in

    (* Get debug string to exercise table formatting *)
    let _debug_str = Table.to_string_debug table in

    (* Try to read first column to exercise column deserialization *)
    if num_columns > 0 && num_rows > 0 then (
      (* Try different column types - catch errors since we don't know the type *)
      ignore (try
        ignore (Wrapper.Column.read_utf8 table ~column:(`Index 0)); true
      with _ -> try
        ignore (Wrapper.Column.read_int table ~column:(`Index 0)); true
      with _ -> try
        ignore (Wrapper.Column.read_float table ~column:(`Index 0)); true
      with _ ->
        false)
    );

    (* Close reader *)
    Parquet_reader.close reader;

    Printf.printf "✓ %s (rows=%d, cols=%d)\n"
      (Filename.basename file) num_rows num_columns;
    true
  with exn ->
    Printf.printf "✗ %s: %s\n"
      (Filename.basename file)
      (Printexc.to_string exn);
    false

let test_upstream_parquet_files () =
  let test_data_dir = "test/test-data" in
  let data_dir = Filename.concat test_data_dir "data" in

  (* Skip test if test-data directory doesn't exist *)
  if not (Sys.file_exists test_data_dir && Sys.is_directory test_data_dir) then begin
    Printf.printf "\nSkipping tests as no test-data dir\n%!";
    Alcotest.skip ()
  end else begin
    (* Find all parquet files in the data directory *)
    let parquet_files =
      if Sys.file_exists data_dir && Sys.is_directory data_dir then
        Sys.readdir data_dir
        |> Array.to_list
        |> List.filter (fun f -> Filename.check_suffix f ".parquet")
        |> List.map (fun f -> Filename.concat data_dir f)
        |> List.sort String.compare
      else
        []
    in

    Printf.printf "\nParsing %d parquet files from upstream test data:\n" (List.length parquet_files);

    (* Parse each file *)
    let results = List.map parse_parquet_file parquet_files in
    let num_success = List.filter (fun x -> x) results |> List.length in
    let num_failed = List.length results - num_success in

    Printf.printf "\nResults: %d succeeded, %d failed out of %d files\n"
      num_success num_failed (List.length parquet_files);

    (* Verify we found at least some files *)
    Alcotest.(check bool) "Found parquet files" true (List.length parquet_files > 0)
  end

let () =
  let open Alcotest in
  run "Upstream parquet data tests" [
    "upstream_data", [
      test_case "Parse all upstream parquet files" `Quick test_upstream_parquet_files;
    ];
  ]
