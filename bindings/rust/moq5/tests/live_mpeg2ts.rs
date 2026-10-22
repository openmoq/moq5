//! Relay rows: MPEG-TS through the service tier. A generated transport
//! stream is written with `write_mpeg2ts` in uneven chunks whose sizes are
//! not multiples of the packet size, so packets span calls; it arrives at
//! an auto-subscribing receiver as Objects in Location order whose payloads
//! are the source, byte for byte; every Group opens with the PAT; every
//! Object reports its Location continuity, with no gap; the description
//! carries the record the sender declared; the sender's per-track counters
//! count every packet; the track ends with its END_OF_TRACK. 192-octet
//! packets arrive as 188-octet packets with the receiver's `strip_prefix`.
//! On an SDK without MPEG-TS the track is refused instead.
//!
//! The writer resumes after whatever each call took, retries on
//! WOULD_BLOCK and flushes held Objects, but the rows do not require that
//! a short write, a WOULD_BLOCK or a held Object occurs: sixty packets need
//! not meet any backpressure. How those results are reported is the
//! fake-provider rows' coverage (`mpeg2ts_writes_report_what_they_took`),
//! not these rows'.

mod live;

use std::time::{Duration, Instant};

use moq5::{
    Error, Location, MediaType, Mpeg2tsInput, Mpeg2tsMode, Mpeg2tsRecord, Mpeg2tsWriteError,
    Namespace, ObjectStatus, Packaging, Receiver, ReceiverConfig, SendTrack, SendTrackConfig,
    Sender, SenderConfig, StreamTerminalKind,
};

/// Every byte of `source`, in the chunk sizes given round robin, resuming
/// after what each call took and retrying on WOULD_BLOCK; then any Objects
/// the track holds.
fn write_all(sender: &Sender, track: SendTrack, source: &[u8], chunks: &[usize]) {
    let started = Instant::now();
    let mut offset = 0;
    let mut next = 0;
    while offset < source.len() {
        let len = chunks[next % chunks.len()].min(source.len() - offset);
        match sender.write_mpeg2ts(track, &source[offset..offset + len], false) {
            Ok(consumed) => {
                assert!(consumed <= len, "took {consumed} of {len}");
                offset += consumed;
                if consumed == len {
                    next += 1;
                }
            }
            Err(Mpeg2tsWriteError {
                error: Error::WOULD_BLOCK,
                consumed,
            }) => {
                assert_eq!(consumed, 0, "a write that made no progress took nothing");
                let _ = sender.wait(live::STEP);
            }
            Err(failure) => panic!("write_mpeg2ts at {offset}: {failure}"),
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "wrote {offset} of {}",
            source.len()
        );
    }
    // Held Objects go out on empty writes until none is held.
    while sender
        .track_stats(track)
        .expect("stats of the sender's track")
        .mpeg2ts
        .objects_parked
        > 0
    {
        match sender.write_mpeg2ts(track, &[], false) {
            Ok(consumed) => assert_eq!(consumed, 0),
            Err(failure) if failure.error == Error::WOULD_BLOCK => {
                let _ = sender.wait(live::STEP);
            }
            Err(failure) => panic!("write_mpeg2ts of held Objects: {failure}"),
        }
        assert!(started.elapsed() < live::DEADLINE, "Objects still held");
    }
}

fn end_track(sender: &Sender, track: SendTrack) {
    let started = Instant::now();
    loop {
        match sender.end_track(track) {
            Ok(()) => return,
            Err(Error::WOULD_BLOCK) => {
                let _ = sender.wait(live::STEP);
            }
            Err(error) => panic!("end_track: {error}"),
        }
        assert!(started.elapsed() < live::DEADLINE);
    }
}

fn run(row: &str, port: u16, packet_size: usize, strip: bool) {
    let mut relay = live::Relay::start(row, port);
    let namespace = Namespace::parse(&format!("live/rust-{row}")).expect("a two-part namespace");

    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::lossless(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let mut record = Mpeg2tsRecord::new(Mpeg2tsMode::UnmodifiedProgram, packet_size as u32);
    record.program_number = Some(1);
    let config = SendTrackConfig::new("prog", MediaType::Video, Packaging::Mpeg2ts, "")
        .bitrate(4_000_000)
        .mpeg2ts(
            record,
            Mpeg2tsInput::ByteStream {
                object_packets: 4,
                min_group: Duration::ZERO,
                forward_before_random_access: false,
            },
        );
    if !live::expect_mpeg2ts() {
        assert_eq!(
            sender.add_track(&config).err(),
            Some(Error::UNSUPPORTED),
            "an SDK without MPEG-TS refuses the track"
        );
        drop(sender);
        drop(sender_endpoint);
        relay.stop();
        return;
    }
    let track = sender
        .add_track(&config)
        .expect("an MPEG-TS track is added");
    live::ready(&sender);

    let receiver_endpoint = live::established(&relay.config());
    let mut receiver_config = ReceiverConfig::flow_control(namespace)
        .auto_subscribe(true)
        .stream_terminals(64);
    if strip {
        receiver_config = receiver_config.mpeg2ts_strip_prefix(true);
    }
    let receiver = Receiver::attach(&receiver_endpoint, &receiver_config)
        .expect("the receiver attaches to an established endpoint");
    let mut seen = Vec::new();
    let announced = live::announced_track(&receiver, &mut seen);
    let description = receiver
        .track_description(announced)
        .expect("the announced track has a description");
    assert_eq!(description.name(), b"prog");
    assert_eq!(description.packaging(), Some(Packaging::Mpeg2ts));
    assert_eq!(description.packaging_name(), b"mpeg2ts");
    assert_eq!(description.mime_type(), b"video/mp2t");
    let ts = description
        .mpeg2ts()
        .expect("the catalog carries the track's record");
    assert_eq!(ts.mode(), Some(Mpeg2tsMode::UnmodifiedProgram));
    assert_eq!(
        ts.packet_size(),
        Some(packet_size as u32),
        "the catalog's size"
    );
    assert_eq!(ts.program_number(), Some(1));
    assert_eq!(
        ts.random_access(),
        Some(true),
        "declared by the service in byte-stream mode"
    );
    assert_eq!(ts.timestamp_mode(), None);
    assert!(ts.malformed().is_empty(), "{:?}", ts.malformed());
    live::subscribed(&sender, track);

    // Six GOPs of ten packets, one second of PCR time each.
    let mut generator = live::ts::Generator::new(packet_size, 1);
    let mut source = Vec::new();
    for _ in 0..6 {
        source.extend(generator.gop(6));
    }
    let packets = source.len() / packet_size;
    assert_eq!(packets, 60);
    write_all(
        &sender,
        track,
        &source,
        &[packet_size * 3, 1000, 77, packet_size * 7],
    );
    end_track(&sender, track);

    // The Objects, then the lane, until the track's END_OF_TRACK.
    let started = Instant::now();
    let mut objects: Vec<(Location, Vec<u8>)> = Vec::new();
    let mut ended = None;
    while ended.is_none() {
        live::drain_queued(&receiver, &mut seen);
        while let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            assert_eq!(object.track(), Some(announced));
            assert_eq!(object.packaging(), Some(Packaging::Mpeg2ts));
            assert_eq!(object.status(), ObjectStatus::Normal);
            let id = object
                .identity()
                .expect("a relayed object is stamped with its identity");
            let loss = object
                .loss_report()
                .expect("an MPEG-TS track reports Location continuity");
            assert!(
                !loss.gap,
                "no Object is missing: {loss:?} at {:?}",
                id.location
            );
            objects.push((id.location, object.payload().to_vec()));
        }
        while let Some(record) = receiver
            .poll_stream_terminal()
            .expect("poll_stream_terminal on a receiver with the lane enabled")
        {
            if record.kind == StreamTerminalKind::EndOfTrack && record.track == announced {
                ended = Some(record);
            }
        }
        if ended.is_none() {
            let _ = receiver.wait(live::STEP);
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "no END_OF_TRACK after {} Objects",
            objects.len()
        );
    }

    // Location order, and the source byte for byte (each packet without
    // its prefix when the receiver strips it).
    assert!(
        objects.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "Objects surface in Location order"
    );
    let delivered: Vec<u8> = objects
        .iter()
        .flat_map(|(_, payload)| payload.iter().copied())
        .collect();
    let expected: Vec<u8> = if strip {
        live::ts::packets(&source, packet_size).concat()
    } else {
        source.clone()
    };
    assert_eq!(delivered.len(), expected.len(), "every byte arrived");
    assert!(delivered == expected, "the payloads are the source");
    let delivered_size = if strip { 188 } else { packet_size };
    // Every Group opens with the PAT, before its random access point.
    let group_starts: Vec<&(Location, Vec<u8>)> = objects
        .iter()
        .filter(|(location, _)| location.object_id == 0)
        .collect();
    assert_eq!(group_starts.len(), 6, "a Group per random access point");
    for (location, payload) in group_starts {
        let first = &payload[delivered_size - 188..delivered_size];
        assert_eq!(
            live::ts::pid(first),
            live::ts::PAT_PID,
            "the Group at {location:?} opens with the PAT"
        );
    }
    assert_eq!(
        ended.map(|record| record.group_id),
        objects.last().map(|(location, _)| location.group_id + 1),
        "the END_OF_TRACK opens the next Group"
    );

    let stats = sender
        .track_stats(track)
        .expect("stats of the sender's track");
    assert_eq!(
        stats.mpeg2ts.packets_in, packets as u64,
        "every packet counted"
    );
    assert_eq!(stats.mpeg2ts.groups, 6);
    assert_eq!(stats.mpeg2ts.objects, objects.len() as u64);
    assert_eq!(stats.objects_written, stats.mpeg2ts.objects);
    assert_eq!(stats.objects_dropped, 0);
    assert_eq!(stats.mpeg2ts.packets_dropped_before_random_access, 0);
    assert_eq!(
        stats.mpeg2ts.partial_bytes_discarded, 0,
        "partial packets were completed"
    );
    assert_eq!(stats.mpeg2ts.objects_parked, 0);
    assert_eq!(stats.mpeg2ts.source_pcr_discontinuities, 0);

    drop(receiver);
    drop(sender);
    drop(receiver_endpoint);
    drop(sender_endpoint);
    relay.stop();
}

#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_EXPECT_MPEG2TS; run with --ignored"]
fn a_188_octet_stream_arrives_byte_for_byte() {
    run("mpeg2ts-188", 14510, 188, false);
}

#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_EXPECT_MPEG2TS; run with --ignored"]
fn a_192_octet_stream_arrives_without_its_prefix() {
    run("mpeg2ts-192", 14520, 192, true);
}
