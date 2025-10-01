(** Date and time types with nanosecond precision

    Arrow stores temporal data as integers with specific units. This module
    provides conversions between Arrow's integer representations and Ptime,
    OCaml's standard time library.

    {1 Arrow Temporal Storage}

    - {b Dates}: Stored as days since Unix epoch (1970-01-01)
    - {b Timestamps}: Stored as nanoseconds since Unix epoch
    - {b Durations}: Stored as nanoseconds
    - {b Time of day}: Stored as nanoseconds since midnight (0-86,399,999,999,999)

    {1 Usage Examples}

    {[
      (* Working with dates *)
      let date = Datetime.Date.of_unix_days 19000 in  (* 52 years after epoch *)
      let days = Datetime.Date.to_unix_days date in

      (* Working with timestamps *)
      let now = Ptime_clock.now () in
      let ns = Datetime.Time_ns.to_int64_ns_since_epoch now in
      let timestamp = Datetime.Time_ns.of_int64_ns_since_epoch ns in

      (* Working with durations *)
      let one_hour = Datetime.Time_ns.Span.of_ns 3_600_000_000_000L in
      let ns = Datetime.Time_ns.Span.to_ns one_hour
    ]}
*)

module Date : sig
  type t = Ptime.date
  (** Date type (no time component) using Ptime *)

  val of_unix_days : int -> t
  (** Convert days since Unix epoch (1970-01-01) to a date.

      Arrow's Date32 type stores dates as signed 32-bit integers representing
      days since epoch. This allows representing dates from year -5877641 to 5881580.

      @raise Invalid_argument if the day count is out of Ptime's valid range
  *)

  val to_unix_days : t -> int
  (** Convert a date to days since Unix epoch (1970-01-01).

      Returns a signed integer suitable for storage in Arrow's Date32 type.
  *)
end

module Time_ns : sig
  type t = Ptime.t
  (** Timestamp with nanosecond precision using Ptime *)

  val of_int64_ns_since_epoch : int64 -> t
  (** Convert nanoseconds since Unix epoch to a timestamp.

      Arrow's Timestamp type (with nanosecond precision) stores time as a
      64-bit signed integer of nanoseconds since 1970-01-01 00:00:00 UTC.

      Range: approximately years 1677 to 2262.

      @raise Invalid_argument if the nanosecond value is out of Ptime's valid range
  *)

  val to_int64_ns_since_epoch : t -> int64
  (** Convert a timestamp to nanoseconds since Unix epoch.

      Returns a signed 64-bit integer suitable for Arrow's Timestamp type.
  *)

  module Span : sig
    type t = Ptime.span
    (** Time duration/span using Ptime *)

    val of_ns : int64 -> t
    (** Convert nanoseconds to a time span.

        Arrow's Duration type stores durations as signed 64-bit integers.
        Negative values represent spans backwards in time.

        @raise Invalid_argument if the nanosecond value is out of Ptime.Span's valid range
    *)

    val to_ns : t -> int64
    (** Convert a time span to nanoseconds.

        Returns a signed 64-bit integer suitable for Arrow's Duration type.
    *)
  end

  module Ofday : sig
    type t = int * int * int * int64
    (** Time of day: (hour, minute, second, nanosecond).
        - hour: 0-23
        - minute: 0-59
        - second: 0-59
        - nanosecond: 0-999,999,999
    *)

    val of_ns_since_midnight : int64 -> t
    (** Convert nanoseconds since midnight to time of day components.

        Arrow's Time64 type (with nanosecond precision) stores time of day
        as a 64-bit integer of nanoseconds since midnight.

        Range: 0 to 86,399,999,999,999 (just under 24 hours).

        Example:
        {[
          let tod = Datetime.Time_ns.Ofday.of_ns_since_midnight 3_661_000_000_000L in
          (* tod = (1, 1, 1, 0L)  -- 01:01:01.000000000 *)
        ]}

        @raise Invalid_argument if value is negative or >= 24 hours
    *)

    val to_ns_since_midnight : t -> int64
    (** Convert time of day components to nanoseconds since midnight.

        Returns a value in range 0 to 86,399,999,999,999, suitable for
        Arrow's Time64 type.

        @raise Invalid_argument if any component is out of valid range
    *)
  end
end