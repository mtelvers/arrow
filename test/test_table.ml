open Arrow

let test_table_basic () =
  (* Test basic table creation with multiple data types and concatenation *)
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
  let foo = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
  let bar = Wrapper.Column.read_int table ~column:(`Name "bar") in
  let baz = Wrapper.Column.read_int_opt table ~column:(`Name "baz") in

  (* Expected outputs for concatenated table *)
  let expected_foo = [| "v1"; "v2"; "v3"; "v1"; "v2"; "v3"; "v1"; "v2"; "v3" |] in
  let expected_bar = [| 0; 0; 0; 1; 5; 10; 2; 10; 20 |] in
  let expected_baz = [| Some 1; None; None; Some 3; None; None; Some 5; None; None |] in

  Alcotest.(check (array string)) "foo column" expected_foo foo;
  Alcotest.(check (array int)) "bar column" expected_bar bar;
  Alcotest.(check (array (option int))) "baz column" expected_baz baz

let test_table_with_high_level_api () =
  (* Test high-level Table.col API with float and optional columns *)
  let table =
    List.init 3 (fun i ->
        let f = Float.of_int i in
        let cols =
          [ Table.col [| "v1"; "v2"; "v3" |] Table.Utf8 ~name:"foo"
          ; Table.col [| f; 5. *. f; 0.5 *. f |] Table.Float ~name:"bar"
          ; Table.col_opt [| Some ((f *. 2.) +. 0.1); None; None |] Table.Float ~name:"baz"
          ]
        in
        Table.create cols)
    |> Wrapper.Table.concatenate
  in
  let foo = Table.read table Table.Utf8 ~column:(`Name "foo") in
  let bar = Table.read table Table.Float ~column:(`Name "bar") in
  let baz = Table.read_opt table Table.Float ~column:(`Name "baz") in

  (* Expected outputs for high-level API test *)
  let expected_foo = [| "v1"; "v2"; "v3"; "v1"; "v2"; "v3"; "v1"; "v2"; "v3" |] in
  let expected_bar = [| 0.0; 0.0; 0.0; 1.0; 5.0; 0.5; 2.0; 10.0; 1.0 |] in
  let expected_baz = [| Some 0.1; None; None; Some 2.1; None; None; Some 4.1; None; None |] in

  Alcotest.(check (array string)) "foo column" expected_foo foo;
  Alcotest.(check (array (float 1e-6))) "bar column" expected_bar bar;
  Alcotest.(check (array (option (float 1e-6)))) "baz column" expected_baz baz

let test_table_operations () =
  (* Test column manipulation operations: add_column and add_all_columns *)
  let table =
    let cols = [ Table.col [| "v1"; "v2"; "v3" |] Table.Utf8 ~name:"foo" ] in
    Table.create cols
  in
  let col_foo = Wrapper.Table.get_column table "foo" in

  let table2 =
    let cols = [ Table.col (Array.init 3 (Printf.sprintf "w%d")) Table.Utf8 ~name:"woo" ] in
    Table.create cols
  in
  let table3 = Wrapper.Table.add_column table2 "foo2" col_foo in
  let table3 = Wrapper.Table.add_column table3 "foo3" col_foo in

  let foo3 = Table.read table3 Table.Utf8 ~column:(`Name "foo3") in
  let woo = Table.read table3 Table.Utf8 ~column:(`Name "woo") in

  let expected_foo3 = [| "v1"; "v2"; "v3" |] in
  let expected_woo = [| "w0"; "w1"; "w2" |] in

  Alcotest.(check (array string)) "foo3 column" expected_foo3 foo3;
  Alcotest.(check (array string)) "woo column" expected_woo woo;

  (* Test add_all_columns *)
  let combined_table = Wrapper.Table.add_all_columns table table3 in
  let foo3_combined = Table.read combined_table Table.Utf8 ~column:(`Name "foo3") in
  let woo_combined = Table.read combined_table Table.Utf8 ~column:(`Name "woo") in

  Alcotest.(check (array string)) "combined foo3 column" expected_foo3 foo3_combined;
  Alcotest.(check (array string)) "combined woo column" expected_woo woo_combined

let () =
  let open Alcotest in
  run "Table tests" [
    "basic", [
      test_case "Table creation and reading" `Quick test_table_basic;
      test_case "Table with high-level API" `Quick test_table_with_high_level_api;
      test_case "Table operations" `Quick test_table_operations;
    ];
  ]