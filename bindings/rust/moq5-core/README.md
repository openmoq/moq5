# moq5-core

A safe Rust API for the sans-I/O core of libmoq, the MOQ5 Media over QUIC
library.

The core tier holds no socket, thread or clock. A consumer that owns its
transport drives a session through this crate and takes on the host
contract the core documents: feeding input, draining actions and events,
custody of refused input, and time. Media applications normally use the
service crate, [`moq5`](../moq5), which re-exports the shared names below.

The vocabulary every tier shares lives here:

- `Buffer`: an exclusively owned libmoq refcounted buffer; it moves between
  threads and is never shared, and a slice consumes it. One adopted from
  native code (`Buffer::from_raw`, `Buffer::wrap_raw`) must bring an
  ownership graph, allocators and release callbacks valid on whichever
  thread drops it; the safe constructors always do.
- `Error` and `Result`: every libmoq result code by name, an unknown code
  kept as it is.
- `Namespace`: a track namespace of non-empty byte parts. The root has zero
  parts (`Namespace::root()`, or an empty parts list); the text form has no
  spelling for it, so `Namespace::parse("")` is an error. Where a draft
  cannot carry the root (draft 16 requires a part), the operation refuses
  it, not the constructor.
- `Version`, `Location`, `LocationFilter`, `GroupOrder` and `ObjectStatus`:
  protocol values in their native domain (`ObjectStatus` is the semantic
  `MOQ_OBJECT_*` status libmoq reports, not a draft's wire code).

Each converts to and from its raw form through narrow, documented methods
(`as_raw`/`from_raw`, `code`/`from_code`): producing a raw value is safe,
and a conversion that reads through a raw value's pointers is `unsafe`,
with the caller's obligations stated. The raw C API is re-exported as
`moq5_core::sys` (the `moq5-sys` crate with its default, core-only
features).

The session itself, `Session` (created with `Session::try_new`), has no
thread affinity: it is `Send` and may move between threads between calls,
and `!Sync`, so one thread calls into it at a time. What it lends borrows it
and stays with it, and payloads leave it only as copies. Its module
documentation states the host contract, the
request families and which of a record's views a scoped thread may read.

## Linking

With only this crate in a dependency graph, `moq5-sys` probes
`libmoq-core.pc` and links the core library alone; the installed-consumer
rows prove it from the build's link lines and the binary's symbols. Point
the build at the SDK with `MOQ_PREFIX=<prefix>`; see
[`moq5-sys`](../moq5-sys) for the tiers, the static/shared selection and
cross builds.

## License

Apache-2.0
