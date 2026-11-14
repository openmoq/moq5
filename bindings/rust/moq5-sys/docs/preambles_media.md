## moq/loc.h

```text
LOC (Low Overhead Container) metadata parse/encode helpers.

Parses and encodes LOC metadata carried in raw object property
bytes for the selected LOC profile. Each profile maps typed C
fields to the property IDs defined by that LOC draft version.

Pure helper library: no sessions, no I/O, no threads. A
moq_version_t is taken by value to select the wire codec; no
session is touched.
Link against moq::loc (depends only on moq::core).

Unknown well-formed properties are silently ignored.

-- Transport version, LOC revision, and the KVP integer codec --------

The caller explicitly selects the companion LOC profile, separately from
the transport version. Transport selects only the KVP integer encoding:
    MOQ_VERSION_DRAFT_16  section 1.4/1.4.2: fields marked (i) use the
      RFC 9000 QUIC variable-length integer (1/2/4/8 bytes, max 2^62-1);
    MOQ_VERSION_DRAFT_18  section 1.4.1/1.4.3 and
    MOQ_VERSION_DRAFT_22  section 8.3: fields marked (vi64) use MOQT's
      own encoding (1..9 bytes, max 2^64-1).

The Delta Type, the odd-type Length, and the even-type value are all
encoded with that draft's integer. Values 0..63 encode identically in
both, so a codec confusion is invisible below 64 and produces wrong
bytes at or above it.

Supported explicit pairs are LOC-01 with D16/18, and LOC-04 or LOC-05
with D22. LOC-05 is LOC-04 plus the Codec String property (0x11); its
other property definitions are unchanged, and its Key-Value-Pair grammar
is draft-22 section 8.3's. Pairing it with draft 22 claims nothing about
draft 23. The LOC revision is not negotiated, so the caller names it.
There is no default: an unsupported or unknown
version -- including 0 -- FAILS CLOSED with MOQ_ERR_INVAL rather than
guessing a codec, and so does a revision paired with a version it is not
registered for (LOC-01 ids on draft 22, LOC-04 or LOC-05 ids on draft 16
or 18). A future draft becomes usable here only when its row is
deliberately registered in the table.
```

## moq/cmaf.h

```text
CMAF (Common Media Application Format) segment parser.

Parse-only: extracts codec configuration from init segments
(ftyp+moov) and sample tables from media fragments (moof+mdat).
No muxer, no decoder, no session integration.

All string/byte fields in parsed output borrow from the input
buffer. The input must remain valid while the output is in use.
Parsing never allocates.

Link against moq::cmaf (depends only on moq::core).
```

## moq/media_object.h

```text
Stateless MoQ media object normalizer.

Parses one MoQ media object into timing, keyframe, and payload
metadata. Handles RAW (LOC properties), CMAF (fragment parsing) and, in a
build with moq::mpeg2ts, MPEG-2 TS packaging. No state, no heap allocation, no retained
refs — output spans borrow from input rcbufs.

Usable standalone by downstream integrations without requiring
moq_playback_t. Link against moq::media-object (depends on
moq::core, moq::loc, moq::cmaf).
```

## moq/msf.h

```text
MSF (MOQT Streaming Format) catalog parser and encoder.

Converts between MSF catalog JSON documents and typed C structs.
No network, no sessions.

Link against moq::msf (depends on moq::core; when built with
moq::media-object and moq::cmaf, also provides the
moq_msf_track_to_media_info_sized() helper via <moq/msf_media.h>).

Lifetime: string fields in the parsed catalog borrow from the
internal JSON DOM, which itself is allocated via the caller's
moq_alloc_t. All borrows are valid until moq_msf_catalog_cleanup()
is called. The original JSON input need not be kept alive after
moq_msf_catalog_parse() returns.

@see draft-ietf-moq-msf-01
```
