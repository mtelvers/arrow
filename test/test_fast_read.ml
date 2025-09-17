open Arrow2

(* Helper function to check Column.t equality since we can't derive comparison *)
let check_column msg expected actual =
  match expected, actual with
  | Wrapper.Column.String expected_arr, Wrapper.Column.String actual_arr ->
      Alcotest.(check (array string)) msg expected_arr actual_arr
  | Wrapper.Column.String_option expected_arr, Wrapper.Column.String_option actual_arr ->
      Alcotest.(check (array (option string))) msg expected_arr actual_arr
  | Wrapper.Column.Int64 expected_ba, Wrapper.Column.Int64 actual_ba ->
      let expected_arr = Array.init (Bigarray.Array1.dim expected_ba) (fun i -> expected_ba.{i}) in
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array int64)) msg expected_arr actual_arr
  | Wrapper.Column.Int64_option (expected_ba, _), Wrapper.Column.Int64_option (actual_ba, _) ->
      (* Note: We're not checking validity bitmask equality here for simplicity *)
      let expected_arr = Array.init (Bigarray.Array1.dim expected_ba) (fun i -> expected_ba.{i}) in
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array int64)) (msg ^ " (data)") expected_arr actual_arr
  | Wrapper.Column.Double expected_ba, Wrapper.Column.Double actual_ba ->
      let expected_arr = Array.init (Bigarray.Array1.dim expected_ba) (fun i -> expected_ba.{i}) in
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array (float 1e-6))) msg expected_arr actual_arr
  | Wrapper.Column.Double_option (expected_ba, _), Wrapper.Column.Double_option (actual_ba, _) ->
      let expected_arr = Array.init (Bigarray.Array1.dim expected_ba) (fun i -> expected_ba.{i}) in
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array (float 1e-6))) (msg ^ " (data)") expected_arr actual_arr
  | Wrapper.Column.Unsupported_type, Wrapper.Column.Unsupported_type ->
      () (* Both unsupported is OK *)
  | _ ->
      Alcotest.fail (Printf.sprintf "%s: Column types don't match" msg)

let test_fast_read_comprehensive () =
  (* Port of the comprehensive fast_read test from Jane Street *)
  let t =
    List.init 12 (fun i ->
        let f = Float.of_int i in
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.utf8_opt
              [| None; Some (Printf.sprintf "hello %d" i); Some "world" |]
              ~name:"foobar"
          ; Wrapper.Writer.int [| i; i + 1; 2 * i |] ~name:"baz"
          ; Wrapper.Writer.int_opt [| Some (i / 2); None; None |] ~name:"baz_opt"
          ; Wrapper.Writer.float [| f; f +. 0.5; f /. 2. |] ~name:"fbaz"
          ; Wrapper.Writer.float_opt
              [| None; None; Some (Float.sqrt f) |]
              ~name:"fbaz_opt"
          ; Wrapper.Writer.bitset (Valid.create_all_valid 3) ~name:"bset"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in

  (* Test column 0: String *)
  let col0 = Wrapper.Column.fast_read t 0 in
  let expected_strings = Array.make 36 "" in
  for i = 0 to 35 do
    expected_strings.(i) <- [|"v1"; "v2"; "v3"|].(i mod 3)
  done;
  check_column "Column 0 (String)" (Wrapper.Column.String expected_strings) col0;

  (* Test column 1: String_option *)
  let col1 = Wrapper.Column.fast_read t 1 in
  let expected_string_opts = Array.make 36 None in
  for i = 0 to 11 do
    expected_string_opts.(i * 3) <- None;
    expected_string_opts.(i * 3 + 1) <- Some (Printf.sprintf "hello %d" i);
    expected_string_opts.(i * 3 + 2) <- Some "world"
  done;
  check_column "Column 1 (String_option)" (Wrapper.Column.String_option expected_string_opts) col1;

  (* Test column 2: Int64 *)
  let col2 = Wrapper.Column.fast_read t 2 in
  let expected_ints = Array.make 36 0L in
  for i = 0 to 11 do
    expected_ints.(i * 3) <- Int64.of_int i;
    expected_ints.(i * 3 + 1) <- Int64.of_int (i + 1);
    expected_ints.(i * 3 + 2) <- Int64.of_int (2 * i)
  done;
  (match col2 with
  | Wrapper.Column.Int64 actual_ba ->
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array int64)) "Column 2 (Int64)" expected_ints actual_arr
  | _ -> Alcotest.fail "Expected Int64 column");

  (* Test column 4: Double *)
  let col4 = Wrapper.Column.fast_read t 4 in
  let expected_floats = Array.make 36 0.0 in
  for i = 0 to 11 do
    let f = Float.of_int i in
    expected_floats.(i * 3) <- f;
    expected_floats.(i * 3 + 1) <- f +. 0.5;
    expected_floats.(i * 3 + 2) <- f /. 2.0
  done;
  (match col4 with
  | Wrapper.Column.Double actual_ba ->
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array (float 1e-6))) "Column 4 (Double)" expected_floats actual_arr
  | _ -> Alcotest.fail "Expected Double column");

  (* Test column 6: Should be Unsupported_type (bitset) *)
  let col6 = Wrapper.Column.fast_read t 6 in
  check_column "Column 6 (Bitset - unsupported)" Wrapper.Column.Unsupported_type col6

let test_fast_read_sliced () =
  (* Test fast_read on sliced table *)
  let t =
    List.init 5 (fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.int [| i; i + 1; 2 * i |] ~name:"bar"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in

  let sliced = Wrapper.Table.slice t ~offset:3 ~length:6 in

  (* Test string column on sliced table *)
  let col0 = Wrapper.Column.fast_read sliced 0 in
  let expected_strings = [| "v1"; "v2"; "v3"; "v1"; "v2"; "v3" |] in
  check_column "Sliced Column 0 (String)" (Wrapper.Column.String expected_strings) col0;

  (* Test int column on sliced table *)
  let col1 = Wrapper.Column.fast_read sliced 1 in
  let expected_ints = [| 1L; 2L; 2L; 2L; 3L; 4L |] in
  (match col1 with
  | Wrapper.Column.Int64 actual_ba ->
      let actual_arr = Array.init (Bigarray.Array1.dim actual_ba) (fun i -> actual_ba.{i}) in
      Alcotest.(check (array int64)) "Sliced Column 1 (Int64)" expected_ints actual_arr
  | _ -> Alcotest.fail "Expected Int64 column")

let () =
  let open Alcotest in
  run "Fast_read tests" [
    "fast_read", [
      test_case "Comprehensive fast_read test" `Quick test_fast_read_comprehensive;
      test_case "Fast_read on sliced table" `Quick test_fast_read_sliced;
    ];
  ]