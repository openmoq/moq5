# Rust bindings

Two crates over libmoq's service tier:

- [`moq5-sys`](moq5-sys) -- raw FFI bindings, generated from the installed
  headers with bindgen.
- [`moq5`](moq5) -- a safe API on top: endpoint, media sender and receiver,
  CMAF parsing. Ownership, teardown order and threading rules that the C API
  states in comments are enforced by the types.

Build and test against an install of this checkout:

```sh
cmake --preset install -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build/install && cmake --install build/install
cd bindings/rust
cargo test        # MOQ_PREFIX=<prefix> for an install elsewhere
```

The tests run without a relay: `moq5`'s `testing` feature swaps libmoq's
service calls for fakes.
