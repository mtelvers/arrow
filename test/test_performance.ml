(* Performance and stress tests for Arrow2 library *)

let time_function f =
  let start_time = Sys.time () in
  let result = f () in
  let end_time = Sys.time () in
  (result, end_time -. start_time)

let test_large_array_performance () =
  Printf.printf "Testing large array performance... ";
  let size = 100_000 in
  let large_array = Array.init size (fun i -> i) in
  
  let (col, creation_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.int large_array ~name:"large_int_array"
  ) in
  
  let (table, table_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.create_table ~cols:[col]
  ) in
  
  Printf.printf "✓ PASS (creation: %.3fs, table: %.3fs)\n" creation_time table_time;
  let _ = table in
  ()

let test_multiple_column_types () =
  Printf.printf "Testing multiple column types... ";
  let size = 10_000 in
  
  let int_data = Array.init size (fun i -> i) in
  let float_data = Array.init size (fun i -> Float.of_int i *. 1.5) in
  let string_data = Array.init size (fun i -> Printf.sprintf "item_%d" i) in
  let bool_data = Array.init size (fun i -> i mod 2 = 0) in
  
  let (cols, creation_time) = time_function (fun () -> [
    Arrow2.Wrapper.Writer.int int_data ~name:"integers";
    Arrow2.Wrapper.Writer.float float_data ~name:"floats";
    Arrow2.Wrapper.Writer.utf8 string_data ~name:"strings";
  ]) in
  
  let (table, table_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.create_table ~cols
  ) in
  
  Printf.printf "✓ PASS (creation: %.3fs, table: %.3fs)\n" creation_time table_time;
  let _ = table in
  let _ = bool_data in
  ()

let test_bigarray_performance () =
  Printf.printf "Testing bigarray performance... ";
  let size = 50_000 in
  
  let (ba, ba_creation_time) = time_function (fun () ->
    let ba = Bigarray.Array1.create Bigarray.int64 Bigarray.c_layout size in
    for i = 0 to size - 1 do
      Bigarray.Array1.set ba i (Int64.of_int (i * 2))
    done;
    ba
  ) in
  
  let (col, col_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.int64_ba ba ~name:"bigarray_int64"
  ) in
  
  Printf.printf "✓ PASS (bigarray: %.3fs, column: %.3fs)\n" ba_creation_time col_time;
  let _ = col in
  ()

let test_validity_performance () =
  Printf.printf "Testing validity performance... ";
  let size = 100_000 in
  
  let (bool_array, bool_time) = time_function (fun () ->
    Array.init size (fun i -> i mod 3 <> 0) (* ~33% null values *)
  ) in
  
  let (valid, valid_time) = time_function (fun () ->
    Arrow2.Valid.from_array bool_array
  ) in
  
  let (ba_valid, ba_time) = time_function (fun () ->
    Arrow2.Valid.create_ba size
  ) in
  
  Printf.printf "✓ PASS (bool_array: %.3fs, valid: %.3fs, ba: %.3fs)\n" bool_time valid_time ba_time;
  let _ = valid in
  let _ = ba_valid in
  ()

let test_datetime_performance () =
  Printf.printf "Testing datetime performance... ";
  let size = 10_000 in
  
  let (dates, date_time) = time_function (fun () ->
    Array.init size (fun i -> Arrow2.Datetime.Date.of_unix_days (19000 + i))
  ) in
  
  let (times, time_time) = time_function (fun () ->
    Array.init size (fun i -> 
      Arrow2.Datetime.Time_ns.of_int64_ns_since_epoch (Int64.of_int (i * 1_000_000_000))
    )
  ) in
  
  let (date_col, date_col_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.date dates ~name:"dates"
  ) in
  
  let (time_col, time_col_time) = time_function (fun () ->
    Arrow2.Wrapper.Writer.time_ns times ~name:"times"
  ) in
  
  Printf.printf "✓ PASS (dates: %.3fs, times: %.3fs, date_col: %.3fs, time_col: %.3fs)\n" 
    date_time time_time date_col_time time_col_time;
  let _ = date_col in
  let _ = time_col in
  ()

let run_performance_tests () =
  Printf.printf "🚀 Running Arrow2 Performance Tests\n";
  Printf.printf "====================================\n\n";
  
  test_large_array_performance ();
  test_multiple_column_types ();
  test_bigarray_performance ();
  test_validity_performance ();
  test_datetime_performance ();
  
  Printf.printf "\n🎯 Performance tests completed successfully!\n"

let () = run_performance_tests ()