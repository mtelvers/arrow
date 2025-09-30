This project is entirely derived from
[LaurentMazare/ocaml-arrow](https://github.com/LaurentMazare/ocaml-arrow).

This is a reimplementation using the OCaml Standard Library and updated to
[Apache Arrow](https://arrow.apache.org/) version 20 and C++ 17.

Some of the `ocaml-arrow` features around PPX have not yet been ported.

Add the Apache apt repository

```sh
apt install -y -V ca-certificates lsb-release wget
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
```

Install the Arrow development library for C++

1. libarrow-dev - Apache Arrow C++ development libraries and headers
2. libparquet-dev - Apache Parquet C++ development libraries and headers

```sh
apt install -y libarrow-dev libparquet-dev
```

Then create a switch and build the project.

```sh
opam switch create . 5.3.0 --deps-only --with-test
dune build
```

For extra tests, do `git clone https://github.com/apache/parquet-testing
test/test-data` and all the test data will be parsed using this library.
