type t = 
  | None
  | Snappy
  | Gzip
  | Brotli
  | Lz4
  | Lz4_raw
  | Zstd

val to_int : t -> int
val to_string : t -> string