This project is entirely derived from
[LaurentMazare/ocaml-arrow](https://github.com/LaurentMazare/ocaml-arrow).

This is a reimplementation using the OCaml Standard Library and updated to
[Apache Arrow](https://arrow.apache.org/) version 21 and C++ 17.

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

# Install Apache Arrow from source

On Ubuntu Plucky the Apache repository does not yet have a prebuild binary, therefore we must build Apache Arrow from source

Install build dependencies
```sh
sudo apt update
sudo apt install -y build-essential cmake git \
    libboost-all-dev libssl-dev libcurl4-openssl-dev \
    libbz2-dev zlib1g-dev liblz4-dev libzstd-dev \
    libsnappy-dev libre2-dev libthrift-dev
```

Clone Arrow repository
```sh
git clone https://github.com/apache/arrow.git
cd arrow
```

Checkout latest stable release (optional, or use main branch)
```sh
git checkout apache-arrow-21.0.0
```

Create build directory
```sh
cd cpp
mkdir build
cd build
```

Configure with CMake (basic options)
```sh
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_ZSTD=ON
```

Build (use all CPU cores)
```sh
make -j$(nproc)
```

Install
```sh
sudo make install
```

Update library cache
```sh
sudo ldconfig
```

# Tests

For extra tests, do `git clone https://github.com/apache/parquet-testing
test/test-data` and all the test data will be parsed using this library.
