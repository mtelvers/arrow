type t = 
  | None
  | Snappy
  | Gzip
  | Brotli
  | Lz4
  | Lz4_raw
  | Zstd

let to_int = function
  | None -> 0
  | Snappy -> 1
  | Gzip -> 2
  | Brotli -> 3
  | Lz4 -> 4
  | Lz4_raw -> 5
  | Zstd -> 6

let to_string = function
  | None -> "none"
  | Snappy -> "snappy"
  | Gzip -> "gzip" 
  | Brotli -> "brotli"
  | Lz4 -> "lz4"
  | Lz4_raw -> "lz4_raw"
  | Zstd -> "zstd"