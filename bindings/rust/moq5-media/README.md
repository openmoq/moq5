# moq5-media

A safe Rust API for the media parsers of libmoq, the MOQ5 Media over QUIC
library: stateless, sans-I/O functions and value types over bytes the
application holds.

- `cmaf`: CMAF init segments and media fragments, and CMSF object
  packaging checks.
- LOC-04 object metadata: what a sender writes with a draft-22 object
  (`LocMetadata`, `Marking`) and what a receiver reports for one
  (`ObjectMetadata`).
- The MPEG-TS catalog record of draft-gregoire-moq-msfts: what a track
  declares (`Mpeg2tsRecord`) and what a received catalog carries
  (`Mpeg2tsRecordRef`, `TrackExtension`); likewise the LOCMAF catalog
  record of draft-einarsson-moq-locmaf-01 (`LocmafRecordRef`).
- The media value types: `MediaType`, `Packaging` (`Packaging::Locmaf` for
  packaging code 4, which earlier releases of this crate reported as
  `Packaging::Other(4)`), `SapType`, `CompanionProfile` and `TimeDomain`.
- With the `locmaf` feature, the `locmaf` module: the LOCMAF encoder and
  decoder, each owning its state and the storage the library works in, and
  the canonical CMAF writer, for a host that handles LOCMAF Objects
  itself.

Errors are [`moq5-core`](../moq5-core)'s `Error`. The service crate,
[`moq5`](../moq5), re-exports every name here but the `locmaf` module, so
existing users of `moq5::cmaf` or `moq5::LocMetadata` compile unchanged; a
service application receives and sends LOCMAF tracks as CMAF chunks and
does not need the module. A conversion that
reads through a raw record's pointers (`ObjectMetadata::from_raw`,
`Mpeg2tsRecordRef::from_raw`, `CmafSample::from_raw_parts`) is `unsafe`,
with the caller's obligations stated.

## Linking

With this crate and the core crate in a graph, `moq5-sys` probes
`libmoq-media.pc`, which links the five media parsers (LOC, CMAF, MPEG-TS,
media object, MSF) and the core, and nothing of the service tier or a
transport adapter. An SDK installs that package only when it builds all
five parsers (`MOQ_BUILD_MPEG2TS` among them); without it the build is
refused with the remedy. The `locmaf` feature adds `libmoq-locmaf.pc`
(LOCMAF and the CMAF library it uses, installed by an SDK built with
`MOQ_BUILD_LOCMAF=ON`), and is refused the same way without it.

## License

Apache-2.0
