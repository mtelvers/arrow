type t =
  | Null
  | Boolean
  | Int8
  | Uint8
  | Int16
  | Uint16
  | Int32
  | Uint32
  | Int64
  | Uint64
  | Float16
  | Float32
  | Float64
  | Binary
  | Large_binary
  | Utf8_string
  | Large_utf8_string
  | Decimal128 of
      { precision : int
      ; scale : int
      }
  | Fixed_width_binary of { bytes : int }
  | Date32 of [ `Days ]
  | Date64 of [ `Milliseconds ]
  | Time32 of [ `Seconds | `Milliseconds ]
  | Time64 of [ `Microseconds | `Nanoseconds ]
  | Timestamp of
      { precision : [ `Seconds | `Milliseconds | `Microseconds | `Nanoseconds ]
      ; timezone : string
      }
  | Duration of [ `Seconds | `Milliseconds | `Microseconds | `Nanoseconds ]
  | Interval of [ `Months | `Days_time ]
  | Struct
  | Map
  | Unknown of string

let of_cstring = function
  | "n" -> Null
  | "b" -> Boolean
  | "c" -> Int8
  | "C" -> Uint8
  | "s" -> Int16
  | "S" -> Uint16
  | "i" -> Int32
  | "I" -> Uint32
  | "l" -> Int64
  | "L" -> Uint64
  | "e" -> Float16
  | "f" -> Float32
  | "g" -> Float64
  | "z" -> Binary
  | "Z" -> Large_binary
  | "u" -> Utf8_string
  | "U" -> Large_utf8_string
  | "tdD" -> Date32 `Days
  | "tdm" -> Date64 `Milliseconds
  | "tts" -> Time32 `Seconds
  | "ttm" -> Time32 `Milliseconds
  | "ttu" -> Time64 `Microseconds
  | "ttn" -> Time64 `Nanoseconds
  | "tDs" -> Duration `Seconds
  | "tDm" -> Duration `Milliseconds
  | "tDu" -> Duration `Microseconds
  | "tDn" -> Duration `Nanoseconds
  | "tiM" -> Interval `Months
  | "tiD" -> Interval `Days_time
  | "+s" -> Struct
  | "+m" -> Map
  | unknown ->
    (match String.split_on_char ':' unknown with
    | [ "tss"; timezone ] -> Timestamp { precision = `Seconds; timezone }
    | [ "tsm"; timezone ] -> Timestamp { precision = `Milliseconds; timezone }
    | [ "tsu"; timezone ] -> Timestamp { precision = `Microseconds; timezone }
    | [ "tsn"; timezone ] -> Timestamp { precision = `Nanoseconds; timezone }
    | [ "w"; bytes ] ->
      (match int_of_string bytes with
      | bytes -> Fixed_width_binary { bytes }
      | exception _ -> Unknown unknown)
    | [ "d"; precision_scale ] ->
      (match String.split_on_char ',' precision_scale with
      | [ precision; scale ] ->
        (match int_of_string precision, int_of_string scale with
        | precision, scale -> Decimal128 { precision; scale }
        | exception _ -> Unknown unknown)
      | _ -> Unknown unknown)
    | _ -> Unknown unknown)