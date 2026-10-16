# moq5

A safe Rust API for libmoq, the MOQ5 Media over QUIC library.

It wraps libmoq's service tier: a managed `Endpoint` (connection, MoQ session
and network thread), a `MediaSender` that publishes a namespace with an
MSF/CMSF catalog, and a `MediaReceiver` that discovers and receives one, plus
CMAF parsing. The raw C API is re-exported as `moq5::sys` (the `moq5-sys`
crate).

```rust,no_run
use std::time::Duration;
use moq5::{
    Endpoint, EndpointConfig, MediaSender, MediaType, Namespace, Packaging,
    RcBuf, SendObject, SenderConfig, StartupStatus, TrackConfig,
};

let ep = Endpoint::connect(&EndpointConfig::new("https://relay.example:4443/moq"))?;
let cfg = SenderConfig::live(Namespace::parse("live/demo")?);
let video = TrackConfig::new("video", MediaType::Video, Packaging::Loc, "avc1.42e01e")
    .bitrate(2_000_000);

// Attach and add the track in one network turn, so the first catalog has it.
let mut startup = MediaSender::start(&ep, cfg, vec![video])?;
let (sender, tracks) = loop {
    match startup.wait(Duration::from_millis(100)) {
        StartupStatus::Done(r) => break r?,
        StartupStatus::Pending(s) => startup = s,
    }
};

let mut obj = SendObject::new(RcBuf::from_owner(vec![0u8; 100])?);
obj.is_sync = true;
obj.starts_group = true;
sender.write(tracks[0], obj)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## What it enforces

- **Teardown order.** Senders and receivers hold their endpoint; the endpoint
  is stopped and destroyed when its last handle drops.
- **Handle ownership.** Track handles are checked against the sender or
  receiver that issued them.
- **Payload ownership.** `MediaSender::write` takes an `RcBuf` and hands it
  back on failure; received objects release their buffers on drop.
- **Lifetimes.** Track descriptions borrow the receiver that owns them.
- **Threading.** `Endpoint` is `Send + Sync`, so another thread can set its
  interrupt latch; senders and receivers are `Send` but not `Sync`.

## Building

`moq5-sys` links the static libmoq archives from an install prefix given by
`MOQ_PREFIX`, and generates its bindings with bindgen, so libclang must be
installed.

## Testing without a relay

With the `testing` feature, `moq5::testing::install` replaces libmoq's
service calls with a table of fakes until the returned guard drops.

## License

Apache-2.0
