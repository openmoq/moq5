## moq/locmaf.h

```text
LOCMAF: Low Overhead CMAF for Media over QUIC
(draft-einarsson-moq-locmaf-01; Object encoding version "0.3").

A LOCMAF Object carries one CMAF chunk (section 7): the boxes before its
moof as generic box elements, the moof as tagged fields -- absolute in a
full header, differences against the previous chunk of the same MOQT group
in a delta header -- and the mdat payload unchanged. This library encodes a
CMAF chunk into a LOCMAF Object, decodes a LOCMAF Object into the chunk's
effective values (section 15.1), and writes the canonical CMAF chunk those
values define (section 15).

Sans-I/O and allocation-free. The encoder and decoder are caller-owned
state over caller storage sized by their _storage_size function; every
other output is written into a caller buffer or borrows from the caller's
input. One encoder or decoder serves one track; neither is thread-safe, and
calls on one instance must be serialized. The canonical writer and the
object parser are pure.

Integers in a LOCMAF Object are the MoQT vi64 of
draft-ietf-moq-transport-18 section 1.4.1, whatever draft the session
carrying the Object negotiated: the payload is end to end and is never
re-encoded by a relay. Decoders accept any valid vi64 length; encoders
write the shortest.

Link against moq::locmaf (depends on moq::core and moq::cmaf).
```
