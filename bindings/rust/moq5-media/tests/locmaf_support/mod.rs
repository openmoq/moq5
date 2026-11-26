//! Test material for the LOCMAF rows: a CMAF Header for one audio track,
//! and the first two Objects of a group with the canonical CMAF chunks they
//! stand for (draft-einarsson-moq-locmaf-01 sections 7 to 15).
//!
//! The track: track_ID 1, trex sample description index 1, duration 1024,
//! size 0, flags 0x02000000; clear. The Objects: a full header (BMDT 0, one
//! sample) and a delta header with no field (BMDT 1024 by section 12.2),
//! each with a four-byte mdat payload.

#![allow(dead_code)]

use moq5_media::cmaf::CmafInit;
use moq5_media::locmaf::Track;

/// The Object at (0, 0): genBox-free full header with BMDT 0 (ID 10) and
/// sample count 1 (ID 14), then the mdat payload.
pub const FULL: [u8; 10] = [0x02, 0x04, 0x0a, 0x00, 0x0e, 0x01, 0xde, 0xad, 0xbe, 0xef];
/// The Object at (0, 1): a delta header with no field, then the payload.
pub const DELTA: [u8; 6] = [0x03, 0x00, 0xca, 0xfe, 0xba, 0xbe];
/// The mdat payloads of the two Objects.
pub const FULL_PAYLOAD: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];
pub const DELTA_PAYLOAD: [u8; 4] = [0xca, 0xfe, 0xba, 0xbe];

fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut out = u32::try_from(8 + body.len())
        .expect("a test box fits a 32-bit size")
        .to_be_bytes()
        .to_vec();
    out.extend_from_slice(kind);
    out.extend_from_slice(body);
    out
}

fn words(values: &[u32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_be_bytes()).collect()
}

/// The canonical CMAF chunk of one single-sample chunk of the track (section
/// 15): moof(mfhd, traf(tfhd, tfdt v1, trun)) and the mdat.
pub fn canonical(bmdt: u64, payload: &[u8; 4]) -> Vec<u8> {
    let mfhd = boxed(b"mfhd", &words(&[0, 0]));
    // default-base-is-moof and default-sample-size-present; track 1, size 4.
    let tfhd = boxed(b"tfhd", &words(&[0x0002_0010, 1, 4]));
    let mut tfdt_body = words(&[0x0100_0000]);
    tfdt_body.extend_from_slice(&bmdt.to_be_bytes());
    let tfdt = boxed(b"tfdt", &tfdt_body);
    // data-offset-present; one sample; the payload follows moof and the
    // mdat header (92 + 8 bytes).
    let trun = boxed(b"trun", &words(&[0x0000_0001, 1, 100]));
    let traf = boxed(b"traf", &[tfhd, tfdt, trun].concat());
    let moof = boxed(b"moof", &[mfhd, traf].concat());
    assert_eq!(moof.len(), 92, "the pinned moof layout");
    [moof, boxed(b"mdat", payload)].concat()
}

/// ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(entry))))) mvex(trex)):
/// one track with an unrecognized sample entry, which the facts do not
/// depend on.
pub fn init_segment() -> Vec<u8> {
    let stsd = boxed(
        b"stsd",
        &[
            &[0, 0, 0, 0, 0, 0, 0, 1][..],
            &boxed(b"zzzz", &[0u8; 16])[..],
        ]
        .concat(),
    );
    let mdhd = boxed(
        b"mdhd",
        &[&[0u8; 12][..], &48000u32.to_be_bytes()[..]].concat(),
    );
    let tkhd = boxed(b"tkhd", &[&[0u8; 12][..], &1u32.to_be_bytes()[..]].concat());
    let mdia = boxed(
        b"mdia",
        &[mdhd, boxed(b"minf", &boxed(b"stbl", &stsd))].concat(),
    );
    let trak = boxed(b"trak", &[tkhd, mdia].concat());
    let trex = boxed(b"trex", &words(&[0, 1, 1, 1024, 0, 0x0200_0000]));
    let mvex = boxed(b"mvex", &trex);
    [
        boxed(b"ftyp", b"isom\0\0\0\0isom"),
        boxed(b"moov", &[trak, mvex].concat()),
    ]
    .concat()
}

/// The track's facts, from its parsed header.
pub fn track() -> Track {
    let init = CmafInit::parse(init_segment()).expect("the test header parses");
    Track::from_init(init.info()).expect("the header is one LOCMAF can use")
}
