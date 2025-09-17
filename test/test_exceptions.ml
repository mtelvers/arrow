open Arrow2

let catch_and_extract_message f =
  try
    f ();
    failwith "Expected exception but none was raised"
  with
  | Failure msg -> msg
  | exn -> Printexc.to_string exn

let test_parquet_file_not_found () =
  let error_msg = catch_and_extract_message (fun () ->
      ignore (Parquet_reader.table "does-not-exist.parquet" : Wrapper.Table.t)) in

  (* Jane Street expects: "IOError: Failed to open local file 'does-not-exist.parquet'. Detail: [errno 2] No such file or directory" *)
  Alcotest.(check string) "File not found error message"
    "IOError: Failed to open local file 'does-not-exist.parquet'. Detail: [errno 2] No such file or directory"
    error_msg

let test_empty_parquet_file () =
  let filename = Filename.temp_file "test" ".parquet" in
  let cleanup () = if Sys.file_exists filename then Sys.remove filename in
  try
    let error_msg = catch_and_extract_message (fun () ->
        ignore (Parquet_reader.table filename : Wrapper.Table.t)) in
    cleanup ();

    (* Jane Street expects: "Invalid: Parquet file size is 0 bytes" *)
    Alcotest.(check string) "Empty parquet file error message"
      "Invalid: Parquet file size is 0 bytes"
      error_msg
  with exn ->
    cleanup ();
    raise exn

let test_column_errors () =
  (* Create the same test table as Jane Street *)
  let table =
    List.init 3 (fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.int [| i; 5 * i; 10 * i |] ~name:"bar"
          ; Wrapper.Writer.int_opt [| Some ((i * 2) + 1); None; None |] ~name:"baz"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in

  (* Test 1: Column not found *)
  let error_msg1 = catch_and_extract_message (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "foobar") in
      ()) in

  (* Jane Street expects: "cannot find column foobar" *)
  Alcotest.(check string) "Column not found error message"
    "cannot find column foobar"
    error_msg1;

  (* Test 2: Wrong column type *)
  let error_msg2 = catch_and_extract_message (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "baz") in
      ()) in

  (* Jane Street expects: "expected type with utf8 (id 13) got int64" *)
  Alcotest.(check string) "Column type mismatch error message"
    "expected type with utf8 (id 13) got int64"
    error_msg2;

  (* Test 3: Invalid column index *)
  let error_msg3 = catch_and_extract_message (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Index 0) in
      let _col = Wrapper.Column.read_utf8 table ~column:(`Index 123) in
      ()) in

  (* Jane Street expects: "invalid column index 123 (ncols: 3)" *)
  Alcotest.(check string) "Invalid column index error message"
    "invalid column index 123 (ncols: 3)"
    error_msg3

let () =
  let open Alcotest in
  run "Exception handling tests" [
    "exceptions", [
      test_case "Parquet file not found" `Quick test_parquet_file_not_found;
      test_case "Empty parquet file" `Quick test_empty_parquet_file;
      test_case "Column access errors" `Quick test_column_errors;
    ];
  ]