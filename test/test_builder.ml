open Arrow

let test_double_builder () =
  let builder = Builder.Double.create () in

  (* Test basic append *)
  Builder.Double.append builder 1.5;
  Builder.Double.append builder 2.7;
  Builder.Double.append_null builder;
  Builder.Double.append builder 3.14;

  Alcotest.(check int) "Length after appends" 4 (Builder.Double.length builder);
  Alcotest.(check int) "Null count" 1 (Builder.Double.null_count builder);

  (* Test append_opt *)
  Builder.Double.append_opt builder (Some 4.2);
  Builder.Double.append_opt builder None;

  Alcotest.(check int) "Length after opt appends" 6 (Builder.Double.length builder);
  Alcotest.(check int) "Null count after opt appends" 2 (Builder.Double.null_count builder)

let test_string_builder () =
  let builder = Builder.String.create () in

  Builder.String.append builder "hello";
  Builder.String.append builder "world";
  Builder.String.append_null builder;
  Builder.String.append_opt builder (Some "test");
  Builder.String.append_opt builder None;

  Alcotest.(check int) "String builder length" 5 (Builder.String.length builder);
  Alcotest.(check int) "String builder null count" 2 (Builder.String.null_count builder)

let test_int_builders () =
  (* Test NativeInt builder *)
  let int_builder = Builder.NativeInt.create () in
  Builder.NativeInt.append int_builder 42;
  Builder.NativeInt.append int_builder 100;
  Builder.NativeInt.append_null int_builder;

  Alcotest.(check int) "NativeInt builder length" 3 (Builder.NativeInt.length int_builder);
  Alcotest.(check int) "NativeInt builder null count" 1 (Builder.NativeInt.null_count int_builder);

  (* Test Int32 builder *)
  let int32_builder = Builder.Int32.create () in
  Builder.Int32.append int32_builder 42l;
  Builder.Int32.append_opt int32_builder (Some 100l);
  Builder.Int32.append_opt int32_builder None;

  Alcotest.(check int) "Int32 builder length" 3 (Builder.Int32.length int32_builder);
  Alcotest.(check int) "Int32 builder null count" 1 (Builder.Int32.null_count int32_builder);

  (* Test Int64 builder *)
  let int64_builder = Builder.Int64.create () in
  Builder.Int64.append int64_builder 42L;
  Builder.Int64.append_opt int64_builder None;
  Builder.Int64.append int64_builder 100L;

  Alcotest.(check int) "Int64 builder length" 3 (Builder.Int64.length int64_builder);
  Alcotest.(check int) "Int64 builder null count" 1 (Builder.Int64.null_count int64_builder)

type person = { name : string; age : int; height : float option }

let test_row_based_builder () =
  (* Test row-based table construction *)

  let people = [|
    { name = "Alice"; age = 30; height = Some 1.65 };
    { name = "Bob"; age = 25; height = None };
    { name = "Charlie"; age = 35; height = Some 1.80 };
  |] in

  let cols =
    Builder.col ~name:"name" Table.Utf8 (fun p -> p.name) @
    Builder.col ~name:"age" Table.Int (fun p -> p.age) @
    Builder.col_opt ~name:"height" Table.Float (fun p -> p.height) in

  let table = Builder.array_to_table cols people in

  (* Verify the table was created correctly *)
  Alcotest.(check int) "Table rows" 3 (Wrapper.Table.num_rows table);

  let names = Table.read table Table.Utf8 ~column:(`Name "name") in
  let ages = Table.read table Table.Int ~column:(`Name "age") in
  let heights = Table.read_opt table Table.Float ~column:(`Name "height") in

  let expected_names = [| "Alice"; "Bob"; "Charlie" |] in
  let expected_ages = [| 30; 25; 35 |] in
  let expected_heights = [| Some 1.65; None; Some 1.80 |] in

  Alcotest.(check (array string)) "Names column" expected_names names;
  Alcotest.(check (array int)) "Ages column" expected_ages ages;
  Alcotest.(check (array (option (float 1e-6)))) "Heights column" expected_heights heights

type simple_row = { id : int; value : string }

let test_row_builder () =
  (* Test the Row builder module *)

  let module SimpleRow = struct
    type row = simple_row
    let array_to_table rows =
      let cols =
        Builder.col ~name:"id" Table.Int (fun r -> r.id) @
        Builder.col ~name:"value" Table.Utf8 (fun r -> r.value) in
      Builder.array_to_table cols rows
  end in

  let module SimpleRowBuilder = Builder.Row(SimpleRow) in

  let builder = SimpleRowBuilder.create () in
  SimpleRowBuilder.append builder { id = 1; value = "first" };
  SimpleRowBuilder.append builder { id = 2; value = "second" };

  Alcotest.(check int) "Row builder length" 2 (SimpleRowBuilder.length builder);

  let table = SimpleRowBuilder.to_table builder in
  Alcotest.(check int) "Row builder table rows" 2 (Wrapper.Table.num_rows table);

  let ids = Table.read table Table.Int ~column:(`Name "id") in
  let values = Table.read table Table.Utf8 ~column:(`Name "value") in

  Alcotest.(check (array int)) "Row builder IDs" [| 1; 2 |] ids;
  Alcotest.(check (array string)) "Row builder values" [| "first"; "second" |] values;

  (* Test reset *)
  SimpleRowBuilder.reset builder;
  Alcotest.(check int) "Row builder length after reset" 0 (SimpleRowBuilder.length builder)

let test_float_and_boolean_builders () =
  (* Test Float builder *)
  let float_builder = Builder.Float.create () in
  Builder.Float.append float_builder 1.5;
  Builder.Float.append float_builder 2.7;
  Builder.Float.append_null float_builder;
  Builder.Float.append float_builder 3.14;
  Builder.Float.append_opt float_builder (Some 4.2);
  Builder.Float.append_opt float_builder None;

  Alcotest.(check int) "Float builder length" 6 (Builder.Float.length float_builder);
  Alcotest.(check int) "Float builder null count" 2 (Builder.Float.null_count float_builder);

  (* Test Boolean builder *)
  let bool_builder = Builder.Boolean.create () in
  Builder.Boolean.append bool_builder true;
  Builder.Boolean.append bool_builder false;
  Builder.Boolean.append_null bool_builder;
  Builder.Boolean.append_opt bool_builder (Some true);
  Builder.Boolean.append_opt bool_builder None;

  Alcotest.(check int) "Boolean builder length" 5 (Builder.Boolean.length bool_builder);
  Alcotest.(check int) "Boolean builder null count" 2 (Builder.Boolean.null_count bool_builder)

let test_small_int_builders () =
  (* Test Int8 builder *)
  let int8_builder = Builder.Int8.create () in
  Builder.Int8.append int8_builder 127;
  Builder.Int8.append int8_builder (-128);
  Builder.Int8.append_null int8_builder;
  Builder.Int8.append_opt int8_builder (Some 0);
  Builder.Int8.append_opt int8_builder None;

  Alcotest.(check int) "Int8 builder length" 5 (Builder.Int8.length int8_builder);
  Alcotest.(check int) "Int8 builder null count" 2 (Builder.Int8.null_count int8_builder);

  (* Test Int16 builder *)
  let int16_builder = Builder.Int16.create () in
  Builder.Int16.append int16_builder 32767;
  Builder.Int16.append int16_builder (-32768);
  Builder.Int16.append_null int16_builder;
  Builder.Int16.append_opt int16_builder (Some 1234);

  Alcotest.(check int) "Int16 builder length" 4 (Builder.Int16.length int16_builder);
  Alcotest.(check int) "Int16 builder null count" 1 (Builder.Int16.null_count int16_builder)

let test_unsigned_int_builders () =
  (* Test UInt8 builder *)
  let uint8_builder = Builder.UInt8.create () in
  Builder.UInt8.append uint8_builder 0;
  Builder.UInt8.append uint8_builder 255;
  Builder.UInt8.append_null uint8_builder;
  Builder.UInt8.append_opt uint8_builder (Some 128);

  Alcotest.(check int) "UInt8 builder length" 4 (Builder.UInt8.length uint8_builder);
  Alcotest.(check int) "UInt8 builder null count" 1 (Builder.UInt8.null_count uint8_builder);

  (* Test UInt16 builder *)
  let uint16_builder = Builder.UInt16.create () in
  Builder.UInt16.append uint16_builder 0;
  Builder.UInt16.append uint16_builder 65535;
  Builder.UInt16.append_null uint16_builder;
  Builder.UInt16.append_opt uint16_builder (Some 32768);

  Alcotest.(check int) "UInt16 builder length" 4 (Builder.UInt16.length uint16_builder);
  Alcotest.(check int) "UInt16 builder null count" 1 (Builder.UInt16.null_count uint16_builder);

  (* Test UInt32 builder *)
  let uint32_builder = Builder.UInt32.create () in
  Builder.UInt32.append uint32_builder 0l;
  Builder.UInt32.append uint32_builder (-1l); (* max uint32 as signed int32 *)
  Builder.UInt32.append_null uint32_builder;
  Builder.UInt32.append_opt uint32_builder (Some 123456l);

  Alcotest.(check int) "UInt32 builder length" 4 (Builder.UInt32.length uint32_builder);
  Alcotest.(check int) "UInt32 builder null count" 1 (Builder.UInt32.null_count uint32_builder);

  (* Test UInt64 builder *)
  let uint64_builder = Builder.UInt64.create () in
  Builder.UInt64.append uint64_builder 0L;
  Builder.UInt64.append uint64_builder Int64.max_int;
  Builder.UInt64.append_null uint64_builder;
  Builder.UInt64.append_opt uint64_builder (Some 123456789L);

  Alcotest.(check int) "UInt64 builder length" 4 (Builder.UInt64.length uint64_builder);
  Alcotest.(check int) "UInt64 builder null count" 1 (Builder.UInt64.null_count uint64_builder)

let test_builder_edge_cases () =
  (* Test empty builders *)
  let empty_float = Builder.Float.create () in
  Alcotest.(check int) "Empty float builder length" 0 (Builder.Float.length empty_float);
  Alcotest.(check int) "Empty float builder null count" 0 (Builder.Float.null_count empty_float);

  let empty_bool = Builder.Boolean.create () in
  Alcotest.(check int) "Empty boolean builder length" 0 (Builder.Boolean.length empty_bool);
  Alcotest.(check int) "Empty boolean builder null count" 0 (Builder.Boolean.null_count empty_bool);

  (* Test multiple nulls *)
  let multi_null_int8 = Builder.Int8.create () in
  Builder.Int8.append multi_null_int8 1;
  Builder.Int8.append_null ~n:3 multi_null_int8;
  Builder.Int8.append multi_null_int8 2;

  Alcotest.(check int) "Multiple nulls length" 5 (Builder.Int8.length multi_null_int8);
  Alcotest.(check int) "Multiple nulls count" 3 (Builder.Int8.null_count multi_null_int8);

  (* Test all nulls *)
  let all_nulls = Builder.UInt16.create () in
  Builder.UInt16.append_null ~n:5 all_nulls;
  Alcotest.(check int) "All nulls length" 5 (Builder.UInt16.length all_nulls);
  Alcotest.(check int) "All nulls null count" 5 (Builder.UInt16.null_count all_nulls)

let test_datetime_builders () =
  (* Test Date32 builder - days since Unix epoch *)
  let date32_builder = Builder.Date32.create () in
  Builder.Date32.append date32_builder 0l;  (* 1970-01-01 *)
  Builder.Date32.append date32_builder 18628l;  (* ~2021-01-01 *)
  Builder.Date32.append_null date32_builder;
  Builder.Date32.append_opt date32_builder (Some 19000l);

  Alcotest.(check int) "Date32 builder length" 4 (Builder.Date32.length date32_builder);
  Alcotest.(check int) "Date32 builder null count" 1 (Builder.Date32.null_count date32_builder);

  (* Test Date64 builder - milliseconds since Unix epoch *)
  let date64_builder = Builder.Date64.create () in
  Builder.Date64.append date64_builder 0L;  (* 1970-01-01 *)
  Builder.Date64.append date64_builder 1609459200000L;  (* 2021-01-01 *)
  Builder.Date64.append_null date64_builder;
  Builder.Date64.append_opt date64_builder None;

  Alcotest.(check int) "Date64 builder length" 4 (Builder.Date64.length date64_builder);
  Alcotest.(check int) "Date64 builder null count" 2 (Builder.Date64.null_count date64_builder);

  (* Test Time32 builder - seconds or milliseconds since midnight *)
  let time32_builder = Builder.Time32.create () in
  Builder.Time32.append time32_builder 0l;  (* midnight *)
  Builder.Time32.append time32_builder 3600l;  (* 1 hour *)
  Builder.Time32.append time32_builder 43200l;  (* noon *)
  Builder.Time32.append_null time32_builder;

  Alcotest.(check int) "Time32 builder length" 4 (Builder.Time32.length time32_builder);
  Alcotest.(check int) "Time32 builder null count" 1 (Builder.Time32.null_count time32_builder);

  (* Test Time64 builder - microseconds or nanoseconds since midnight *)
  let time64_builder = Builder.Time64.create () in
  Builder.Time64.append time64_builder 0L;  (* midnight *)
  Builder.Time64.append time64_builder 3600000000L;  (* 1 hour in microseconds *)
  Builder.Time64.append_null time64_builder;
  Builder.Time64.append_opt time64_builder (Some 43200000000L);  (* noon in microseconds *)

  Alcotest.(check int) "Time64 builder length" 4 (Builder.Time64.length time64_builder);
  Alcotest.(check int) "Time64 builder null count" 1 (Builder.Time64.null_count time64_builder);

  (* Test Timestamp builder - nanoseconds since Unix epoch *)
  let timestamp_builder = Builder.Timestamp.create () in
  Builder.Timestamp.append timestamp_builder 0L;
  Builder.Timestamp.append timestamp_builder 1609459200000000000L;  (* 2021-01-01 in nanoseconds *)
  Builder.Timestamp.append_null timestamp_builder;
  Builder.Timestamp.append_opt timestamp_builder (Some 1640995200000000000L);  (* 2022-01-01 *)

  Alcotest.(check int) "Timestamp builder length" 4 (Builder.Timestamp.length timestamp_builder);
  Alcotest.(check int) "Timestamp builder null count" 1 (Builder.Timestamp.null_count timestamp_builder);

  (* Test Duration builder - time duration *)
  let duration_builder = Builder.Duration.create () in
  Builder.Duration.append duration_builder 1000000000L;  (* 1 second in nanoseconds *)
  Builder.Duration.append duration_builder 60000000000L;  (* 1 minute in nanoseconds *)
  Builder.Duration.append_null duration_builder;
  Builder.Duration.append_opt duration_builder (Some 3600000000000L);  (* 1 hour *)

  Alcotest.(check int) "Duration builder length" 4 (Builder.Duration.length duration_builder);
  Alcotest.(check int) "Duration builder null count" 1 (Builder.Duration.null_count duration_builder);

  (* Test multiple nulls *)
  let date_null_test = Builder.Date32.create () in
  Builder.Date32.append date_null_test 100l;
  Builder.Date32.append_null ~n:3 date_null_test;
  Builder.Date32.append date_null_test 200l;

  Alcotest.(check int) "Date32 multiple nulls length" 5 (Builder.Date32.length date_null_test);
  Alcotest.(check int) "Date32 multiple nulls count" 3 (Builder.Date32.null_count date_null_test)

let test_comprehensive_builders () =
  (* Comprehensive test of multiple builder types working together *)
  let col1 = Wrapper.StringBuilder.create () in
  let col2 = Wrapper.DoubleBuilder.create () in
  let col3 = Wrapper.Int64Builder.create () in
  let col4 = Wrapper.Int32Builder.create () in
  for i = 1 to 3 do
    Wrapper.StringBuilder.append col1 "v1";
    Wrapper.StringBuilder.append col1 "v2";
    Wrapper.StringBuilder.append col1 "v3";
    Wrapper.DoubleBuilder.append col2 (Float.of_int i +. 0.5);
    Wrapper.DoubleBuilder.append col2 (Float.of_int i +. 1.5);
    Wrapper.DoubleBuilder.append_null col2;
    Wrapper.Int64Builder.append col3 (Int64.of_int (2 * i));
    Wrapper.Int64Builder.append_null col3;
    Wrapper.Int64Builder.append_null col3;
    Wrapper.Int32Builder.append col4 (Int32.of_int (2 * i));
    Wrapper.Int32Builder.append col4 (Int32.of_int (i * i));
    Wrapper.Int32Builder.append_null col4
  done;
  let table =
    Wrapper.Builder.make_table
      [ "foo", Wrapper.Builder.String col1; "bar", Wrapper.Builder.Double col2; "baz", Wrapper.Builder.Int64 col3; "baz32", Wrapper.Builder.Int32 col4 ]
  in
  let foo = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
  let bar = Wrapper.Column.read_float_opt table ~column:(`Name "bar") in
  let baz = Wrapper.Column.read_int_opt table ~column:(`Name "baz") in
  let baz32 = Wrapper.Column.read_int32_opt table ~column:(`Name "baz32") in

  (* Verify expected outputs with nulls and type mixing *)
  let expected_foo = [| "v1"; "v2"; "v3"; "v1"; "v2"; "v3"; "v1"; "v2"; "v3" |] in
  let expected_bar = [| Some 1.5; Some 2.5; None; Some 2.5; Some 3.5; None; Some 3.5; Some 4.5; None |] in
  let expected_baz = [| Some 2; None; None; Some 4; None; None; Some 6; None; None |] in
  let expected_baz32 = [| Some 2l; Some 1l; None; Some 4l; Some 4l; None; Some 6l; Some 9l; None |] in

  Alcotest.(check (array string)) "Comprehensive foo column" expected_foo foo;
  Alcotest.(check (array (option (float 1e-6)))) "Comprehensive bar column" expected_bar bar;
  Alcotest.(check (array (option int))) "Comprehensive baz column" expected_baz baz;
  Alcotest.(check (array (option int32))) "Comprehensive baz32 column" expected_baz32 baz32

let () =
  let open Alcotest in
  run "Builder tests" [
    "builders", [
      test_case "Double builder" `Quick test_double_builder;
      test_case "String builder" `Quick test_string_builder;
      test_case "Int builders" `Quick test_int_builders;
      test_case "Float and Boolean builders" `Quick test_float_and_boolean_builders;
      test_case "Small int builders (Int8/Int16)" `Quick test_small_int_builders;
      test_case "Unsigned int builders" `Quick test_unsigned_int_builders;
      test_case "Datetime builders" `Quick test_datetime_builders;
      test_case "Builder edge cases" `Quick test_builder_edge_cases;
      test_case "Row-based builder" `Quick test_row_based_builder;
      test_case "Row builder module" `Quick test_row_builder;
      test_case "Comprehensive builders test" `Quick test_comprehensive_builders;
    ];
  ]