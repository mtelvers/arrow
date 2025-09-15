open Ctypes

type t = unit ptr
type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

let create len = 
  let ba = Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout ((len + 7) / 8) in
  Bigarray.Array1.fill ba 255; (* All bits set to 1 (valid) *)
  from_voidp void null

let create_ba len =
  let ba = Bigarray.Array1.create Bigarray.int8_unsigned Bigarray.c_layout ((len + 7) / 8) in
  Bigarray.Array1.fill ba 255;
  ba

let set_invalid _t _idx = ()
let set_valid _t _idx = ()
let is_valid _t _idx = true

let from_array bool_array =
  let len = Array.length bool_array in
  let ba = create_ba len in
  for i = 0 to len - 1 do
    let byte_idx = i / 8 in
    let bit_idx = i mod 8 in
    if not bool_array.(i) then
      let old_byte = Bigarray.Array1.get ba byte_idx in
      let new_byte = old_byte land (lnot (1 lsl bit_idx)) in
      Bigarray.Array1.set ba byte_idx new_byte
  done;
  from_voidp void null

let to_ba _t len = create_ba len