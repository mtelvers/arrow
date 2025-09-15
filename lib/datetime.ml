module Date = struct
  type t = Ptime.date
  
  let of_unix_days days =
    let epoch = Ptime.epoch in
    match Ptime.Span.of_d_ps (days, 0L) with
    | None -> failwith "Invalid span from days"
    | Some span ->
      match Ptime.add_span epoch span with
      | Some ptime -> Ptime.to_date ptime
      | None -> failwith "Invalid date from unix days"
  
  let to_unix_days date =
    let ptime = match Ptime.of_date date with
    | Some p -> p
    | None -> failwith "Invalid date"
    in
    let span = Ptime.diff ptime Ptime.epoch in
    match Ptime.Span.to_d_ps span with
    | (days, _) -> days
end

module Time_ns = struct
  type t = Ptime.t
  
  let of_int64_ns_since_epoch ns =
    let ns_per_s = 1_000_000_000L in
    let s = Int64.div ns ns_per_s in
    let remainder_ns = Int64.rem ns ns_per_s in
    let ps = Int64.mul remainder_ns 1000L in
    let days = Int64.to_int s / 86400 in
    match Ptime.Span.of_d_ps (days, ps) with
    | None -> failwith "Invalid span from nanoseconds"
    | Some span ->
      match Ptime.of_span span with
      | Some ptime -> ptime
      | None -> failwith "Invalid time from nanoseconds"
  
  let to_int64_ns_since_epoch ptime =
    let span = Ptime.to_span ptime in
    match Ptime.Span.to_d_ps span with
    | (days, ps) ->
      let s = Int64.of_int (days * 86400) in
      let ns_from_s = Int64.mul s 1_000_000_000L in
      let ns_from_ps = Int64.div ps 1000L in
      Int64.add ns_from_s ns_from_ps
  
  module Span = struct
    type t = Ptime.span
    
    let of_ns ns =
      match Ptime.Span.of_d_ps (0, Int64.mul ns 1000L) with
      | None -> failwith "Invalid span from nanoseconds"
      | Some span -> span
    
    let to_ns span =
      match Ptime.Span.to_d_ps span with
      | (days, ps) ->
        let ns_from_days = Int64.mul (Int64.of_int days) (Int64.mul 86400L 1_000_000_000L) in
        let ns_from_ps = Int64.div ps 1000L in
        Int64.add ns_from_days ns_from_ps
  end
  
  module Ofday = struct
    type t = int * int * int * int64
    
    let of_ns_since_midnight ns =
      let ns_per_hour = Int64.mul 3600L 1_000_000_000L in
      let ns_per_minute = Int64.mul 60L 1_000_000_000L in
      let ns_per_second = 1_000_000_000L in
      
      let hours = Int64.to_int (Int64.div ns ns_per_hour) in
      let remaining = Int64.rem ns ns_per_hour in
      let minutes = Int64.to_int (Int64.div remaining ns_per_minute) in
      let remaining = Int64.rem remaining ns_per_minute in
      let seconds = Int64.to_int (Int64.div remaining ns_per_second) in
      let nanoseconds = Int64.rem remaining ns_per_second in
      
      (hours, minutes, seconds, nanoseconds)
    
    let to_ns_since_midnight (h, m, s, ns) =
      let ns_per_hour = Int64.mul 3600L 1_000_000_000L in
      let ns_per_minute = Int64.mul 60L 1_000_000_000L in
      let ns_per_second = 1_000_000_000L in
      
      Int64.add (Int64.mul (Int64.of_int h) ns_per_hour)
        (Int64.add (Int64.mul (Int64.of_int m) ns_per_minute)
           (Int64.add (Int64.mul (Int64.of_int s) ns_per_second) ns))
  end
end