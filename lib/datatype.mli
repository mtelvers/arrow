(** Arrow data types - the type system for columnar data

    Arrow's type system specifies the physical layout and logical interpretation
    of data in memory. Each type defines:
    - Physical representation (bytes per value, alignment)
    - Logical interpretation (signed/unsigned, precision, units)
    - Null representation (validity bitmap)

    {1 Type Categories}

    {2 Primitive Types}
    - Integers: Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64
    - Floating-point: Float16, Float32, Float64
    - Boolean: Single bit per value

    {2 Binary Types}
    - Variable-length: Binary, Utf8_string
    - Large (64-bit offsets): Large_binary, Large_utf8_string
    - Fixed-length: Fixed_width_binary

    {2 Temporal Types}
    - Dates: Date32 (days), Date64 (milliseconds)
    - Times: Time32, Time64 (time of day)
    - Timestamps: With timezone support
    - Durations: Time spans
    - Intervals: Calendar intervals

    {2 Decimal Type}
    - Decimal128: Fixed-point decimal with configurable precision and scale

    {2 Nested Types}
    - Struct: Named fields (like records)
    - Map: Key-value pairs

    See {{:https://arrow.apache.org/docs/format/Columnar.html}Arrow Format Specification}
    for complete details on physical layouts.
*)

type t =
  | Null
  (** Null type - all values are null.
      Size: 0 bytes per value. *)

  | Boolean
  (** Boolean type - true or false.
      Size: 1 bit per value (packed into bytes). *)

  | Int8
  (** 8-bit signed integer.
      Range: -128 to 127.
      Size: 1 byte per value. *)

  | Uint8
  (** 8-bit unsigned integer.
      Range: 0 to 255.
      Size: 1 byte per value. *)

  | Int16
  (** 16-bit signed integer.
      Range: -32,768 to 32,767.
      Size: 2 bytes per value. *)

  | Uint16
  (** 16-bit unsigned integer.
      Range: 0 to 65,535.
      Size: 2 bytes per value. *)

  | Int32
  (** 32-bit signed integer.
      Range: -2³¹ to 2³¹-1.
      Size: 4 bytes per value. *)

  | Uint32
  (** 32-bit unsigned integer.
      Range: 0 to 2³²-1.
      Size: 4 bytes per value. *)

  | Int64
  (** 64-bit signed integer.
      Range: -2⁶³ to 2⁶³-1.
      Size: 8 bytes per value. *)

  | Uint64
  (** 64-bit unsigned integer.
      Range: 0 to 2⁶⁴-1.
      Size: 8 bytes per value. *)

  | Float16
  (** 16-bit floating point (half precision).
      Format: IEEE 754 binary16.
      Size: 2 bytes per value. *)

  | Float32
  (** 32-bit floating point (single precision).
      Format: IEEE 754 binary32.
      Size: 4 bytes per value. *)

  | Float64
  (** 64-bit floating point (double precision).
      Format: IEEE 754 binary64.
      Size: 8 bytes per value. *)

  | Binary
  (** Variable-length binary data.
      Layout: 32-bit offsets + data buffer.
      Max size per value: 2GB. *)

  | Large_binary
  (** Variable-length binary data with 64-bit offsets.
      Layout: 64-bit offsets + data buffer.
      Max size per value: 2⁶³ bytes. *)

  | Utf8_string
  (** Variable-length UTF-8 encoded strings.
      Layout: 32-bit offsets + UTF-8 data buffer.
      Max size per value: 2GB.
      Most commonly used string type. *)

  | Large_utf8_string
  (** Variable-length UTF-8 strings with 64-bit offsets.
      Layout: 64-bit offsets + UTF-8 data buffer.
      Max size per value: 2⁶³ bytes. *)

  | Decimal128 of
      { precision : int  (** Total number of decimal digits *)
      ; scale : int      (** Number of digits after decimal point *)
      }
  (** 128-bit fixed-point decimal number.
      Precision: 1-38 decimal digits.
      Scale: 0-precision.
      Example: Decimal128 {precision=10; scale=2} can represent -99999999.99 to 99999999.99.
      Size: 16 bytes per value. *)

  | Fixed_width_binary of { bytes : int }
  (** Fixed-length binary data.
      All values have exactly the same byte length.
      Size: bytes per value. *)

  | Date32 of [ `Days ]
  (** Date stored as days since Unix epoch (1970-01-01).
      Range: ±5.8 million years.
      Size: 4 bytes per value. *)

  | Date64 of [ `Milliseconds ]
  (** Date stored as milliseconds since Unix epoch.
      Includes time of day.
      Size: 8 bytes per value. *)

  | Time32 of [ `Seconds | `Milliseconds ]
  (** Time of day (no date component).
      [`Seconds]: 0 to 86,399 (4 bytes)
      [`Milliseconds]: 0 to 86,399,999 (4 bytes) *)

  | Time64 of [ `Microseconds | `Nanoseconds ]
  (** Time of day with sub-millisecond precision.
      [`Microseconds]: 0 to 86,399,999,999 (8 bytes)
      [`Nanoseconds]: 0 to 86,399,999,999,999 (8 bytes) *)

  | Timestamp of
      { precision : [ `Seconds | `Milliseconds | `Microseconds | `Nanoseconds ]
      ; timezone : string  (** IANA timezone string, or "" for unspecified *)
      }
  (** Timestamp: date and time since Unix epoch.
      Timezone-aware if timezone is specified.
      Precision determines the unit of the stored integer.
      Size: 8 bytes per value. *)

  | Duration of [ `Seconds | `Milliseconds | `Microseconds | `Nanoseconds ]
  (** Time duration/span.
      Stored as signed integer in the specified unit.
      Size: 8 bytes per value. *)

  | Interval of [ `Months | `Days_time ]
  (** Calendar interval.
      [`Months]: Number of months (4 bytes)
      [`Days_time]: Days and milliseconds (8 bytes: 4 + 4) *)

  | Struct
  (** Structured type with named fields.
      Like an OCaml record - contains multiple child columns.
      Null representation is at struct level (all fields null together). *)

  | Map
  (** Map type: list of key-value pairs.
      Keys must not be null.
      Layout: nested struct with "key" and "value" fields. *)

  | Unknown of string
  (** Unknown/unsupported Arrow type.
      String contains the Arrow type name.
      Returned when encountering types not yet implemented in this binding. *)
(** Arrow data type specification *)

val of_cstring : string -> t
(** Parse an Arrow type from its C string representation.

    Used internally when reading schemas from files.

    @raise Failure if the type string is invalid
*)