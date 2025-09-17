open Arrow

let bitset_to_string bitset =
  let chars = ref [] in
  for i = Valid.length bitset - 1 downto 0 do
    chars := (if Valid.get bitset i then '1' else '0') :: !chars
  done;
  String.concat "" (List.map (String.make 1) !chars)

let test_valid_creation () =
  let len = 10 in
  let valid_all = Valid.create_all_valid len in
  let valid_none = Valid.create len in

  Alcotest.(check int) "All valid length" len (Valid.length valid_all);
  Alcotest.(check int) "None valid length" len (Valid.length valid_none);

  (* Check all valid bitset *)
  for i = 0 to len - 1 do
    Alcotest.(check bool) (Printf.sprintf "All valid bit %d" i) true (Valid.get valid_all i)
  done;

  (* Check none valid bitset *)
  for i = 0 to len - 1 do
    Alcotest.(check bool) (Printf.sprintf "None valid bit %d" i) false (Valid.get valid_none i)
  done;

  Alcotest.(check string) "All valid string" "1111111111" (bitset_to_string valid_all);
  Alcotest.(check string) "None valid string" "0000000000" (bitset_to_string valid_none)

let test_valid_operations () =
  let len = 8 in
  let valid = Valid.create len in

  (* Set some bits *)
  Valid.set_valid valid 1;
  Valid.set_valid valid 3;
  Valid.set_valid valid 5;
  Valid.set_invalid valid 7; (* Should already be invalid, but test the function *)

  let expected = "01010100" in
  Alcotest.(check string) "Pattern setting" expected (bitset_to_string valid);

  (* Test num_true and num_false *)
  Alcotest.(check int) "Num true" 3 (Valid.num_true valid);
  Alcotest.(check int) "Num false" 5 (Valid.num_false valid)

let test_valid_from_array () =
  let bool_array = [| true; false; true; true; false |] in
  let valid = Valid.from_array bool_array in

  Alcotest.(check int) "From array length" 5 (Valid.length valid);
  Alcotest.(check string) "From array pattern" "10110" (bitset_to_string valid);

  let converted_back = Valid.to_array valid in
  Alcotest.(check (array bool)) "Round trip conversion" bool_array converted_back

let test_bitset_writer_reader () =
  (* Test writing and reading bitsets *)
  let bool_data = [| true; false; true; false; true |] in
  let valid = Valid.from_array bool_data in

  let table =
    let cols = [ Wrapper.Writer.bitset valid ~name:"bits" ] in
    Wrapper.Writer.create_table ~cols
  in

  Alcotest.(check int) "Bitset table rows" 5 (Wrapper.Table.num_rows table);

  let read_bitset = Wrapper.Column.read_bitset table ~column:(`Name "bits") in
  let read_bool_data = Valid.to_array read_bitset in

  Alcotest.(check (array bool)) "Bitset round trip" bool_data read_bool_data

let test_optional_bitset () =
  (* Test optional bitset (bitset with validity) *)
  let content = Valid.from_array [| true; false; true; false |] in
  let validity = Valid.create_all_valid 4 in
  Valid.set_invalid validity 1; (* Make second element invalid *)

  let table =
    let cols = [ Wrapper.Writer.bitset_opt content ~valid:validity ~name:"opt_bits" ] in
    Wrapper.Writer.create_table ~cols
  in

  let read_content, read_validity = Wrapper.Column.read_bitset_opt table ~column:(`Name "opt_bits") in

  (* Check that the content matches (for valid positions) *)
  Alcotest.(check bool) "Opt content pos 0" true (Valid.get read_content 0);
  Alcotest.(check bool) "Opt content pos 2" true (Valid.get read_content 2);
  Alcotest.(check bool) "Opt content pos 3" false (Valid.get read_content 3);

  (* Check validity *)
  Alcotest.(check bool) "Opt validity pos 0" true (Valid.get read_validity 0);
  Alcotest.(check bool) "Opt validity pos 1" false (Valid.get read_validity 1);
  Alcotest.(check bool) "Opt validity pos 2" true (Valid.get read_validity 2);
  Alcotest.(check bool) "Opt validity pos 3" true (Valid.get read_validity 3)

let () =
  let open Alcotest in
  run "Bitset tests" [
    "valid", [
      test_case "Valid creation" `Quick test_valid_creation;
      test_case "Valid operations" `Quick test_valid_operations;
      test_case "Valid from/to array" `Quick test_valid_from_array;
      test_case "Bitset writer/reader" `Quick test_bitset_writer_reader;
      test_case "Optional bitset" `Quick test_optional_bitset;
    ];
  ]