(lang dune 3.19)

(name arrow2)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name <author@example.com>")

(maintainers "Maintainer Name <maintainer@example.com>")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name arrow2)
 (synopsis "OCaml bindings for Apache Arrow")
 (description "OCaml bindings for Apache Arrow using standard OCaml libraries")
 (depends
  ocaml
  dune
  ctypes
  ctypes-foreign
  dune-configurator
  ptime
  (alcotest :with-test))
 (tags
  ("arrow" "parquet" "data" "analytics")))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
