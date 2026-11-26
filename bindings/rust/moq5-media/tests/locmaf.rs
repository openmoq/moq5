//! The LOCMAF codec through the public API: track facts from a CMAF
//! Header, a group decoded to its effective values and written as the
//! canonical CMAF chunks it stands for, the same chunks encoded back to the
//! same Objects with an explicit commit, rawBoxes both ways, the bound, the
//! refusals and their reasons, and the counters.

mod locmaf_support;

use locmaf_support::{canonical, track, DELTA, DELTA_PAYLOAD, FULL, FULL_PAYLOAD};
use moq5_core::Error;
use moq5_media::cmaf::{CmafInit, Trex};
use moq5_media::locmaf::{write_cmaf, Decoder, Encoder, Limits, ObjectKind, Reason, Track};
use moq5_media::sys;

#[test]
fn track_facts_come_from_the_header() {
    let track = track();
    assert_eq!(track.track_id(), 1);
    assert_eq!(
        track.trex(),
        Trex {
            default_sample_description_index: 1,
            default_sample_duration: 1024,
            default_sample_size: 0,
            default_sample_flags: 0x0200_0000,
        }
    );
    assert!(!track.is_protected());
    assert_eq!(track.default_per_sample_iv_size(), 0);
    assert!(format!("{track:?}").contains("1024"));

    // A header without the track's trex is not one LOCMAF can use.
    let mut segment = locmaf_support::init_segment();
    let at = segment
        .windows(4)
        .position(|w| w == b"trex")
        .expect("the header has its trex");
    segment[at + 8..at + 12].copy_from_slice(&9u32.to_be_bytes());
    let init = CmafInit::parse(segment).expect("a header with a foreign trex parses");
    assert!(init.info().trex().is_none(), "no trex for track 1");
    assert_eq!(
        Track::from_init(init.info()).expect_err("refused"),
        Error::PROTO
    );
}

#[test]
fn a_group_decodes_to_its_canonical_chunks() {
    let track = track();
    let mut decoder = Decoder::new(&track, Limits::default()).expect("a decoder with defaults");
    assert!(!decoder.expects(0, 0), "no reference yet");

    let full = FULL;
    let chunk = decoder.push(0, 0, &full).expect("the full header decodes");
    assert_eq!(chunk.kind(), ObjectKind::Full);
    assert_eq!(chunk.base_media_decode_time(), 0);
    assert_eq!(chunk.sample_description_index(), 1);
    assert_eq!(chunk.samples().len(), 1);
    let sample = chunk.samples()[0];
    assert_eq!(
        (sample.duration(), sample.size(), sample.flags()),
        (1024, 4, 0x0200_0000)
    );
    assert_eq!(sample.offset(), 0);
    assert_eq!(sample.composition_offset(), 0);
    assert_eq!(sample.subsample_count(), 0);
    assert!(sample.is_sync());
    assert!(!chunk.has_aux_info() && !chunk.has_subsamples());
    assert!(chunk.ivs().is_empty() && chunk.subsamples().is_empty());
    assert_eq!(chunk.gen_boxes().count(), 0);
    assert_eq!(chunk.mdat_payload(), FULL_PAYLOAD);
    assert!(
        std::ptr::eq(chunk.mdat_payload().as_ptr(), full[6..].as_ptr()),
        "borrowed"
    );
    assert!(chunk.raw_boxes().is_empty());
    assert_eq!(
        write_cmaf(&track, &chunk).expect("a decoded chunk is consistent"),
        canonical(0, &FULL_PAYLOAD)
    );
    assert!(decoder.expects(0, 1));
    assert!(!decoder.expects(0, 2), "only the next Location");

    let chunk = decoder
        .push(0, 1, &DELTA)
        .expect("the delta header decodes");
    assert_eq!(chunk.kind(), ObjectKind::Delta);
    assert_eq!(chunk.base_media_decode_time(), 1024, "section 12.2");
    assert_eq!(
        write_cmaf(&track, &chunk).expect("a decoded chunk is consistent"),
        canonical(1024, &DELTA_PAYLOAD)
    );

    let stats = decoder.stats().expect("the counters are read");
    assert_eq!(stats.objects_decoded, 2);
    assert_eq!(stats.full_headers_decoded, 1);
    assert_eq!(stats.delta_headers_decoded, 1);
    assert_eq!(stats.last_reason, None);
}

#[test]
fn refusals_carry_their_codes_and_reasons() {
    let track = track();
    let mut decoder = Decoder::new(&track, Limits::default()).expect("a decoder with defaults");

    // Joined mid-group: a delta with no reference is not malformed, but it
    // cannot be decoded.
    assert_eq!(
        decoder.push(0, 1, &DELTA).expect_err("refused"),
        Error::WRONG_STATE
    );
    let stats = decoder.stats().expect("the counters are read");
    assert_eq!(stats.objects_unreferenced, 1);
    let reason = stats.last_reason.expect("the refusal has a reason");
    assert_eq!(reason.code(), sys::MOQ_LOCMAF_REASON_NO_REFERENCE);
    assert_eq!(reason.name(), "no_reference");

    // An Object ID below the next one expected changes nothing.
    assert_eq!(
        decoder.push(0, 0, &FULL).expect_err("refused"),
        Error::INVAL
    );
    assert_eq!(
        decoder
            .stats()
            .expect("the counters are read")
            .objects_unreferenced,
        1
    );

    // A malformed Object, then the next group decodes.
    assert_eq!(
        decoder.push(0, 2, &[0x05, 0x00]).expect_err("refused"),
        Error::PROTO
    );
    let stats = decoder.stats().expect("the counters are read");
    assert_eq!(stats.objects_malformed, 1);
    assert_eq!(
        stats.last_reason.map(Reason::code),
        Some(sys::MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT)
    );
    decoder
        .push(1, 0, &FULL)
        .expect("a new group starts afresh");

    // A reset forgets the reference: the next delta has none.
    decoder.reset();
    assert!(!decoder.expects(1, 1));
    assert_eq!(
        decoder.push(1, 1, &DELTA).expect_err("refused"),
        Error::WRONG_STATE
    );

    // Limits above their maxima are refused before any storage is sized.
    let mut limits = Limits::default();
    limits.max_samples = 65537;
    assert_eq!(
        Decoder::new(&track, limits).expect_err("refused"),
        Error::INVAL
    );
    assert_eq!(
        Encoder::new(&track, limits).expect_err("refused"),
        Error::INVAL
    );

    // A chunk over the configured limits.
    let mut limits = Limits::default();
    limits.max_samples = 1;
    let mut small = Decoder::new(&track, limits).expect("a one-sample decoder");
    small.push(0, 0, &FULL).expect("one sample fits");
    // A full header with BMDT 0 and two samples (ID 14), no mdat payload.
    let two_samples = [0x02, 0x04, 0x0a, 0x00, 0x0e, 0x02];
    assert_eq!(
        small.push(1, 0, &two_samples).expect_err("refused"),
        Error::LIMIT
    );
    assert_eq!(
        small
            .stats()
            .expect("the counters are read")
            .objects_limited,
        1
    );

    assert_eq!(Reason::from_code(9999).name(), "", "an unknown reason");
    assert_eq!(Reason::from_code(9999).code(), 9999);
}

#[test]
fn chunks_encode_to_the_same_objects_after_a_commit() {
    let track = track();
    let mut encoder = Encoder::new(&track, Limits::default()).expect("an encoder with defaults");
    let first = canonical(0, &FULL_PAYLOAD);
    let second = canonical(1024, &DELTA_PAYLOAD);

    let bound = encoder.check(&first).expect("the chunk is encodable");
    let object = encoder.encode_chunk(0, 0, &first).expect("the full header");
    assert_eq!(object, FULL);
    assert!(object.len() <= bound, "the bound holds");

    // Without a commit the reference is unchanged: the same Location
    // encodes to the same bytes, and the next chunk is a full header too.
    assert_eq!(
        encoder.encode_chunk(0, 0, &first).expect("a retry"),
        FULL,
        "a refused send can be retried unchanged"
    );
    encoder.commit();
    let object = encoder
        .encode_chunk(0, 1, &second)
        .expect("the delta header");
    assert_eq!(object, DELTA);
    encoder.commit();

    let stats = encoder.stats().expect("the counters are read");
    assert_eq!(stats.chunks_encoded, 3);
    assert_eq!(stats.full_headers_written, 2);
    assert_eq!(stats.delta_headers_written, 1);
    assert_eq!(stats.stages_committed, 2);
    assert_eq!(
        stats.stages_replaced, 1,
        "the retry replaced the first stage"
    );
    assert_eq!(stats.last_reason, None);

    // A malformed chunk (a moof running past its bytes) is refused by the
    // check and by the encode alike.
    let truncated = [0, 0, 0, 0x10, b'm', b'o', b'o', b'f'];
    assert_eq!(
        encoder.check(&truncated).expect_err("refused"),
        Error::PROTO
    );
    assert_eq!(
        encoder.encode_chunk(0, 2, &truncated).expect_err("refused"),
        Error::PROTO
    );
    let stats = encoder.stats().expect("the counters are read");
    assert_eq!(stats.chunks_refused, 2);
    assert_eq!(
        stats.last_reason.map(Reason::code),
        Some(sys::MOQ_LOCMAF_REASON_MALFORMED_CMAF)
    );

    // An Object ID at or below the committed position is refused.
    assert_eq!(
        encoder.encode_chunk(0, 1, &second).expect_err("refused"),
        Error::INVAL
    );
    // A reset forgets the reference: the next chunk is a full header.
    encoder.reset();
    let object = encoder
        .encode_chunk(0, 2, &second)
        .expect("a full header again");
    assert_eq!(object[0], 0x02);
}

#[test]
fn raw_boxes_travel_verbatim() {
    let track = track();
    let chunk_bytes = canonical(0, &FULL_PAYLOAD);
    let mut encoder = Encoder::new(&track, Limits::default()).expect("an encoder with defaults");
    let object = encoder
        .encode_raw_boxes(0, 0, &chunk_bytes)
        .expect("a CMAF chunk travels as rawBoxes");
    encoder.commit();
    assert_eq!(
        encoder
            .stats()
            .expect("the counters are read")
            .raw_boxes_written,
        1
    );
    assert_eq!(
        encoder
            .encode_chunk(0, 1, &canonical(1024, &DELTA_PAYLOAD))
            .expect("the next chunk")[0],
        0x02,
        "a full header follows rawBoxes"
    );

    let mut decoder = Decoder::new(&track, Limits::default()).expect("a decoder with defaults");
    let chunk = decoder
        .push(0, 0, &object)
        .expect("the rawBoxes Object decodes");
    assert_eq!(chunk.kind(), ObjectKind::RawBoxes);
    assert_eq!(chunk.raw_boxes(), chunk_bytes);
    assert_eq!(
        write_cmaf(&track, &chunk).expect("rawBoxes write verbatim"),
        chunk_bytes
    );
    assert!(!decoder.expects(0, 1), "rawBoxes leave no reference");

    let mut malformed = chunk_bytes.clone();
    malformed[3] = 0xff;
    assert_eq!(
        encoder
            .encode_raw_boxes(1, 0, &malformed)
            .expect_err("refused"),
        Error::PROTO
    );
    assert_eq!(
        encoder
            .stats()
            .expect("the counters are read")
            .last_reason
            .map(Reason::code),
        Some(sys::MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED)
    );
}

#[test]
fn a_header_from_an_older_record_is_refused() {
    // A record stamped before the header facts (an older library's) does
    // not carry what the track facts need.
    let init = CmafInit::parse(locmaf_support::init_segment()).expect("the test header parses");
    let mut raw = *init.info().raw();
    raw.struct_size = (std::mem::offset_of!(sys::moq_cmaf_init_info_t, _reserved_sample_entry)
        + std::mem::size_of::<u32>()) as u32;
    // SAFETY: the record's spans point into `init`, alive here.
    let older = unsafe { moq5_media::cmaf::InitInfo::from_raw(&raw) };
    assert_eq!(Track::from_init(older).expect_err("refused"), Error::INVAL);
}
