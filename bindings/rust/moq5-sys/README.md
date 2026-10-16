# moq5-sys

Raw FFI bindings to libmoq, the MOQ5 Media over QUIC library, generated with
bindgen from libmoq's installed headers. For a safe API, use the
[`moq5`](../moq5) crate, which re-exports these bindings as `moq5::sys`.

The crate declares `links = "moq"` and links the static libmoq archives
(`moq-service`, `moq-media-object`, `moq-loc`, `moq-cmaf`, `moq-msf`,
`moq-core`) from an install prefix:

- `MOQ_PREFIX` -- the prefix holding `include/moq` and `lib/libmoq-*.a`.
  Defaults to `../local-prefix` beside the moq5 checkout, where
  `cmake --preset install` installs.
- `MOQ_EXTRA_LIBS` -- further `cargo:rustc-link-lib` values, space-separated,
  for a libmoq built against other static libraries (e.g.
  `"static=picoquic-core static=picotls-core ssl crypto"`).

A shared library that links these archives (a GStreamer plugin, say) needs
them built position-independent: `-DCMAKE_POSITION_INDEPENDENT_CODE=ON`.
bindgen needs libclang.

## License

Apache-2.0
