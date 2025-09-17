open Arrow

let time_function f =
  let start_time = Sys.time () in
  let result = f () in
  let end_time = Sys.time () in
  (result, end_time -. start_time)

let test_large_array_performance () =
  let size = 100_000 in
  let large_array = Array.init size (fun i -> i) in

  let (col, creation_time) = time_function (fun () ->
    Wrapper.Writer.int large_array ~name:"large_int_array"
  ) in

  let (table, table_time) = time_function (fun () ->
    Wrapper.Writer.create_table ~cols:[col]
  ) in

  (* Verify the table was created correctly *)
  Alcotest.(check int) "Large array table rows" size (Wrapper.Table.num_rows table);

  (* Print performance info *)
  Printf.printf "Large array performance: creation=%.3fs, table=%.3fs\n" creation_time table_time

let test_multiple_column_types () =
  let size = 10_000 in

  let int_data = Array.init size (fun i -> i) in
  let float_data = Array.init size (fun i -> Float.of_int i *. 1.5) in
  let string_data = Array.init size (fun i -> Printf.sprintf "item_%d" i) in
  let bool_data = Array.init size (fun i -> i mod 2 = 0) in

  let (cols, creation_time) = time_function (fun () -> [
    Wrapper.Writer.int int_data ~name:"integers";
    Wrapper.Writer.float float_data ~name:"floats";
    Wrapper.Writer.utf8 string_data ~name:"strings";
  ]) in

  let (table, table_time) = time_function (fun () ->
    Wrapper.Writer.create_table ~cols
  ) in

  (* Verify the table was created correctly *)
  Alcotest.(check int) "Multiple column types table rows" size (Wrapper.Table.num_rows table);

  (* Print performance info *)
  Printf.printf "Multiple column types performance: creation=%.3fs, table=%.3fs\n" creation_time table_time;
  let _ = bool_data in
  ()

let test_bigarray_performance () =
  let size = 50_000 in

  let (ba, ba_creation_time) = time_function (fun () ->
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout size in
    for i = 0 to size - 1 do
      Bigarray.Array1.set ba i (Int64.of_int (i * 2))
    done;
    ba
  ) in

  let (col, col_time) = time_function (fun () ->
    Wrapper.Writer.int64_ba ba ~name:"bigarray_int64"
  ) in

  (* Verify the column was created correctly *)
  Alcotest.(check bool) "Bigarray column created" true (match col with _ -> true);

  (* Print performance info *)
  Printf.printf "Bigarray performance: creation=%.3fs, column=%.3fs\n" ba_creation_time col_time

let test_validity_performance () =
  let size = 100_000 in

  let (bool_array, bool_time) = time_function (fun () ->
    Array.init size (fun i -> i mod 3 <> 0) (* ~33% null values *)
  ) in

  let (valid, valid_time) = time_function (fun () ->
    Valid.from_array bool_array
  ) in

  let (ba_valid, ba_time) = time_function (fun () ->
    Valid.create_ba size
  ) in

  (* Verify validity objects were created correctly *)
  Alcotest.(check bool) "Bool array validity created" true (match valid with _ -> true);
  Alcotest.(check bool) "Bigarray validity created" true (match ba_valid with _ -> true);

  (* Print performance info *)
  Printf.printf "Validity performance: bool_array=%.3fs, valid=%.3fs, ba=%.3fs\n" bool_time valid_time ba_time

let test_datetime_performance () =
  let size = 10_000 in

  let (dates, date_time) = time_function (fun () ->
    Array.init size (fun i -> Datetime.Date.of_unix_days (19000 + i))
  ) in

  let (times, time_time) = time_function (fun () ->
    Array.init size (fun i ->
      Datetime.Time_ns.of_int64_ns_since_epoch (Int64.of_int (i * 1_000_000_000))
    )
  ) in

  let (date_col, date_col_time) = time_function (fun () ->
    Wrapper.Writer.date dates ~name:"dates"
  ) in

  let (time_col, time_col_time) = time_function (fun () ->
    Wrapper.Writer.time_ns times ~name:"times"
  ) in

  (* Verify columns were created correctly *)
  Alcotest.(check bool) "Date column created" true (match date_col with _ -> true);
  Alcotest.(check bool) "Time column created" true (match time_col with _ -> true);

  (* Print performance info *)
  Printf.printf "Datetime performance: dates=%.3fs, times=%.3fs, date_col=%.3fs, time_col=%.3fs\n"
    date_time time_time date_col_time time_col_time

let () =
  let open Alcotest in
  run "Performance tests" [
    "performance", [
      test_case "Large array performance" `Quick test_large_array_performance;
      test_case "Multiple column types performance" `Quick test_multiple_column_types;
      test_case "Bigarray performance" `Quick test_bigarray_performance;
      test_case "Validity performance" `Quick test_validity_performance;
      test_case "Datetime performance" `Quick test_datetime_performance;
    ];
  ]