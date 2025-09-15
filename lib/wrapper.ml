include C_wrapper

module Feather_reader = struct
  let schema _filename = { Schema.format = Datatype.Null; name = ""; metadata = []; flags = 0; children = [] }
  let table ?column_idxs:_ _filename = Ctypes.(from_voidp void null)
end


let keep_alive = ref []