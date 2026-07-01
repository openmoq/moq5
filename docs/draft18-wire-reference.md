# Draft-18 wire reference

In-repo reference for the draft-18 wire primitives implemented in libmoq, so
the implementation does not depend on any external/scratch document. It records
the minimal facts the code relies on, verified against the local spec
(`draft-ietf-moq-transport-18`, §1.4.1) and the draft-16→18 delta. Sections are
added as each wire slice lands.

## vi64 — variable-length integer

Draft-18 replaces the QUIC varint with a MoQT-specific encoding, denoted
`(vi64)` in the spec. The number of leading 1-bits in the first byte gives the
total encoded length; the first 0-bit terminates the length prefix. The
remaining bits of the first byte plus the following bytes form the value in
network byte order (big-endian).

| First byte    | Length | Value bits | Max value                    |
|---------------|--------|------------|------------------------------|
| `0xxxxxxx`    | 1      | 7          | 127                          |
| `10xxxxxx`    | 2      | 14         | 16,383                       |
| `110xxxxx`    | 3      | 21         | 2,097,151                    |
| `1110xxxx`    | 4      | 28         | 268,435,455                  |
| `11110xxx`    | 5      | 35         | 34,359,738,367               |
| `111110xx`    | 6      | 42         | 4,398,046,511,103            |
| `1111110x`    | 7      | 49         | 562,949,953,421,311          |
| `11111110`    | 8      | 56         | 72,057,594,037,927,935       |
| `11111111`    | 9      | 64         | 18,446,744,073,709,551,615   |

Invariants:

- **Range:** 0 .. 2^64-1 (the full `uint64_t` range; the 9-byte form's first
  byte is all 1-bits and the value occupies the following 8 bytes).
- **Non-minimal encodings are valid.** Any length that can represent the value
  is a legal encoding (e.g. `0x25` and `0x8025` both decode to 37). A decoder
  MUST accept any valid encoding; an encoder SHOULD emit the minimum length.
- **Big-endian** value bytes.
- This differs from the QUIC varint (RFC 9000), which is 2-bit-tagged, capped at
  2^62-1, and minimal-only. The two codecs coexist; draft-16 uses the QUIC
  varint, draft-18 uses vi64.

Decode (byte level): read the first byte, take `length` from the leading-1-bit
count (`+1`, capped at 9 for `0xFF`); the low `8-length` bits of the first byte
are the value's high bits (none for lengths 8 and 9), followed by `length-1`
big-endian bytes (8 bytes for length 9).

Encode (minimum length): pick the smallest length whose value-bit budget holds
the value; the first byte is the `(length-1)`-ones-then-0 prefix OR'd with the
value's high `8-length` bits; then the remaining value bytes big-endian.

### Worked examples (spec §1.4.1)

| Encoding             | Value                      |
|----------------------|----------------------------|
| `0x25`               | 37                         |
| `0x8025`             | 37 (non-minimal)           |
| `0xbbbd`             | 15,293                     |
| `0xed7f3e7d`         | 226,442,877                |
| `0xfaa1a0e403d8`     | 2,893,212,287,960          |
| `0xfc8998abc66bc0`   | 151,288,809,941,952        |
| `0xfefa318fa8e3ca11` | 70,423,237,261,249,041     |
| `0xff` + 8×`0xff`    | 18,446,744,073,709,551,615 |

### API

`<moq/vi64.h>` (wire/tooling tier, not the application API):

- `moq_vi64_len(value)` — minimum encoded length (1..9).
- `moq_vi64_encode(value, buf, buf_len)` — minimum-length encode; returns bytes
  written, or 0 if `buf_len` is too small.
- `moq_vi64_decode(buf, buf_len, &value)` — decode any valid encoding; returns
  bytes consumed, or 0 if the buffer is empty or shorter than the indicated
  length.
- `moq_vi64_encoded_len(first_byte)` — length from the first byte without a full
  decode.
- `MOQ_VI64_MAX` (`UINT64_MAX`), `MOQ_VI64_MAX_LEN` (9).

`<moq/buf.h>` cursor helpers mirroring the varint ones:
`moq_buf_write_vi64` / `moq_buf_read_vi64`.
