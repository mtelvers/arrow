module Date : sig
  type t = Ptime.date
  
  val of_unix_days : int -> t
  val to_unix_days : t -> int
end

module Time_ns : sig
  type t = Ptime.t
  
  val of_int64_ns_since_epoch : int64 -> t
  val to_int64_ns_since_epoch : t -> int64
  
  module Span : sig
    type t = Ptime.span
    
    val of_ns : int64 -> t
    val to_ns : t -> int64
  end
  
  module Ofday : sig
    type t = int * int * int * int64
    
    val of_ns_since_midnight : int64 -> t
    val to_ns_since_midnight : t -> int64
  end
end