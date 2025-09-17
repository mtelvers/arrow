type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

type t = {
  length : int;
  data : ba;
}

let create len =
  let data = Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout ((len + 7) / 8) in
  Bigarray.Array1.fill data 255; (* All bits set to 1 (valid) *)
  { length = len; data }

let create_all_valid = create

let create_ba len =
  let ba = Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout ((len + 7) / 8) in
  Bigarray.Array1.fill ba 255;
  ba

let mask i = 1 lsl (i land 0b111)
let unmask i = lnot (mask i) land 255

let get t i =
  if i >= t.length then false
  else t.data.{i / 8} land (mask i) <> 0

let set t i b =
  if i < t.length then (
    let index = i / 8 in
    if b then
      t.data.{index} <- t.data.{index} lor (mask i)
    else
      t.data.{index} <- t.data.{index} land (unmask i)
  )

let length t = t.length

let set_invalid t idx = set t idx false
let set_valid t idx = set t idx true
let is_valid t idx = get t idx

let from_array bool_array =
  let len = Array.length bool_array in
  let t = create len in
  Array.iteri (fun i b -> set t i b) bool_array;
  t

let to_array t =
  Array.init t.length (fun i -> get t i)

let to_ba t len =
  let ba = create_ba len in
  for i = 0 to min (t.length - 1) (len - 1) do
    let byte_idx = i / 8 in
    let bit_idx = i mod 8 in
    if not (get t i) then (
      let old_byte = Bigarray.Array1.get ba byte_idx in
      let new_byte = old_byte land (lnot (1 lsl bit_idx)) in
      Bigarray.Array1.set ba byte_idx new_byte
    )
  done;
  ba

let popcount n =
  let rec count acc x =
    if x = 0 then acc
    else count (acc + 1) (x land (x - 1))
  in
  count 0 n

let num_true t =
  let res = ref 0 in
  let length = t.length in
  for byte_index = 0 to (length / 8) - 1 do
    res := !res + popcount t.data.{byte_index}
  done;
  let last_byte_index = length / 8 in
  let last_bits = length mod 8 in
  if last_bits > 0 then (
    let mask = (1 lsl last_bits) - 1 in
    res := !res + popcount (t.data.{last_byte_index} land mask)
  );
  !res

let num_false t = length t - num_true t

let bigarray t = t.data