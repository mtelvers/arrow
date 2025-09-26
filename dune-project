(lang dune 3.19)

(name arrow)

(generate_opam_files true)

(source
 (github mtelvers/arrow))

(authors "Laurent Mazare")

(maintainers "Mark Elvers" "Anil Madhavapeddy")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name arrow)
 (synopsis "Bindings for Apache Arrow and Parquet")
 (description "OCaml bindings for Apache Arrow and the Parquet file format")
 (depends
  ocaml
  dune
  ctypes
  ctypes-foreign
  dune-configurator
  ptime
  (alcotest :with-test)))
