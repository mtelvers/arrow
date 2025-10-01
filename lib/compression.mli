(** Compression codecs for Parquet and Feather files

    Arrow supports multiple compression algorithms for reducing file size.
    Different codecs offer different trade-offs between compression ratio,
    speed, and CPU usage.

    {1 Codec Comparison}

    {v
    Codec      Speed    Ratio    CPU    Use Case
    ────────────────────────────────────────────────
    None       ★★★★★    ☆☆☆☆☆    ★☆☆☆☆  Already compressed data
    Snappy     ★★★★★    ★★☆☆☆    ★★☆☆☆  Default - balanced
    LZ4        ★★★★★    ★★☆☆☆    ★★☆☆☆  Fast operations
    Gzip       ★★★☆☆    ★★★☆☆    ★★★☆☆  Better compression
    Zstd       ★★★☆☆    ★★★★☆    ★★★★☆  Best compression
    Brotli     ★★☆☆☆    ★★★★★    ★★★★★  Maximum compression
    v}

    {1 Recommendations}

    - {b Default}: Use [Snappy] - good balance of speed and compression
    - {b Storage}: Use [Zstd] for long-term storage (better compression)
    - {b Speed}: Use [Lz4] or [Snappy] for fast read/write
    - {b Size}: Use [Brotli] or [Zstd] for maximum compression
    - {b None}: Use when data is already compressed (images, video)

    {1 Example}

    {[
      (* Default - fast and reasonable compression *)
      Table.write_parquet table "data.parquet"
        ~compression:Compression.Snappy;

      (* Storage - better compression, slower *)
      Table.write_parquet table "archive.parquet"
        ~compression:Compression.Zstd;

      (* No compression - fastest, largest files *)
      Table.write_parquet table "uncompressed.parquet"
        ~compression:Compression.None
    ]}
*)

type t =
  | None
  (** No compression. Fastest but largest files.
      Use when data is already compressed or I/O is not a bottleneck. *)

  | Snappy
  (** Snappy compression - the default and recommended codec.
      Very fast compression and decompression with moderate compression ratio.
      Widely supported across Arrow implementations. *)

  | Gzip
  (** Gzip compression (DEFLATE algorithm).
      Slower than Snappy but better compression ratio.
      Universally compatible. Compression level not configurable in this binding. *)

  | Brotli
  (** Brotli compression.
      Excellent compression ratio but slower compression.
      Good for archival or when file size is critical. *)

  | Lz4
  (** LZ4 compression (with frame format).
      Extremely fast, similar to Snappy. Compression ratio comparable to Snappy.
      Good alternative to Snappy. *)

  | Lz4_raw
  (** LZ4 raw block format (without frame headers).
      Slightly faster than Lz4 but less portable.
      Use only when compatibility with frame format is not needed. *)

  | Zstd
  (** Zstandard (Zstd) compression.
      Excellent compression ratio with good speed.
      Recommended for storage and archival. Modern and actively developed.
      Compression level not configurable in this binding (uses default level). *)
(** Compression codec type *)

val to_int : t -> int
(** Convert compression codec to its integer representation.
    Used internally for C++ interop. *)

val to_string : t -> string
(** Convert compression codec to a human-readable string.

    Returns: "none", "snappy", "gzip", "brotli", "lz4", "lz4_raw", or "zstd"
*)