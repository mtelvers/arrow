include C_wrapper

module Feather_reader = struct
  let schema filename = C.Feather_reader.schema filename |> Schema.of_c

  let table ?(column_idxs = []) filename =
    let column_idxs = Ctypes.CArray.of_list Ctypes.int column_idxs in
    let t = C.Feather_reader.read_table
      filename
      (Ctypes.CArray.start column_idxs)
      (Ctypes.CArray.length column_idxs)
    in
    Gc.finalise C.Table.free t;
    t
end


let keep_alive = ref []