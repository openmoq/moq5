# moq5

A safe Rust API for libmoq, the MOQ5 Media over QUIC library.

It wraps libmoq's service tier: a managed `Endpoint` (connection, MoQ session
and network thread), a `Sender` that publishes a namespace with an MSF/CMSF
catalog, a `Receiver` that discovers and receives one (track events, owned
media objects, the stream-terminal lane, SAP and media timeline records), and
CMAF parsing. The raw C API is re-exported as `moq5::sys` (the `moq5-sys`
crate); its header comments are the contract.

```rust,no_run
use moq5::{
    Buffer, Endpoint, EndpointConfig, MediaType, Namespace, Packaging, SendObject,
    SendTrackConfig, Sender, SenderConfig,
};

let ep = Endpoint::connect(&EndpointConfig::new("https://relay.example:4443/moq"))?;
let sender = Sender::attach(&ep, SenderConfig::live(Namespace::parse("live/demo")?))?;
let video = sender.add_track(
    &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
        .bitrate(2_000_000),
)?;
let mut obj = SendObject::new(Buffer::from_vec(vec![0u8; 100])?);
obj.is_sync = true;
obj.starts_group = true;
sender.write(video, obj)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## What the types enforce

- **Teardown order.** Senders and receivers hold their endpoint; the endpoint
  is stopped and destroyed when its last handle drops.
- **Handle ownership.** Track handles are checked against the sender or
  receiver that issued them; a foreign one fails with `Error::INVAL` before
  reaching libmoq.
- **Buffer ownership.** `Sender::write` takes a `Buffer` and hands it back on
  failure; received objects release their buffers on drop, on the polling
  thread.
- **Sized records.** Every configuration is built with the library's sized
  initializer; every sized output is read under the `struct_size` the library
  stamped, a field it did not report is `None`, and a stamp outside the
  contract is `Error::ABI_MISMATCH` (an owning record is released first).
- **Snapshots.** A `TrackDescription` is the track's current state, copied
  under the receiver's lock; nothing reads the mutable native description
  concurrently with the service.
- **Threading.** `Endpoint` is `Send + Sync`; `Sender` and `Receiver` are
  `Send` but not `Sync`; `MediaObject` is neither; `Buffer` moves and is never
  shared.

No application code runs on the service's threads: the crate installs no C
callbacks, posts no tasks, and adopts no buffer whose release runs
application code. Demand is observed through the sender's queries;
`Sender::wait` waits for the write level, not for a subscriber. Every one of
the 17 native result codes is a named `Error` constant.

Not in this crate yet: the media player, `poll_object_within`, the receiver
info snapshot, fill cancellation, fill and location subscribe options,
per-track sender statistics, explicit draft-22 object metadata and track
properties, `Buffer::slice`, and MPEG-TS.

## Building

`moq5-sys` finds the installed SDK through pkg-config (`libmoq-service`); see
its README for `MOQ_PREFIX`, static/shared selection, cross builds and the
supported SDK configurations. bindgen needs libclang.

## License

Apache-2.0
