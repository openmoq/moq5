//! The transport host of `examples/transport_host` run through its
//! scenarios on drafts 16, 18 and 22, in the tagged mode and without it:
//! setup, a subscription with objects and its end, a publication, fetches
//! (with objects and empty), partial writes, event pressure, a refused
//! stream offered again, fragmented and truncated stream types, a peer stop
//! on a data stream, retirement with no peer terminal, the end of a control
//! stream, and cleanup releasing every held record and every payload once.
//! Then every further request family on each draft that has it, and the
//! refusal on each draft that does not: TRACK_STATUS, SUBSCRIBE_TRACKS with
//! PUBLISH_BLOCKED, a REDIRECT with its target, a per-request GOAWAY,
//! joining FETCHes, a subscription fill, update answers, publication
//! updates and cancels, publisher state and initial parameters, FETCH
//! updates, filter and window queries, request-capacity grants, a cancelled
//! advertisement, namespace-request updates, cluster metadata, and a
//! publication's built Track Properties and datagrams. Last, a FETCH's
//! request after a STOP of its response stream (RESET_FINAL_ON_STOP, drafts
//! 18 and 22, and its absence on draft 16): the request's final message
//! replaced by a CANCELLED reset, never a FIN; a reset the session makes of
//! the request itself standing alone; the first cause of a held claim kept;
//! one effect per STOP; and an ordinary completion carrying the policy.
//! Refused writes hand back the very buffer, intact and the caller's alone;
//! object properties reach the receiver exactly, on whole objects, streamed
//! chunks, datagrams and FETCH objects.
//! In the tagged mode the host reports each FIN the transport accepted:
//! the report keeps its result order; a reported hand-off gives its place in
//! a pool of one back, and a held object with the FIN behind it then
//! expires into one terminal with the exact identity, on a subscription and
//! on a publication; a report that a reset (queued, claimed, or owed behind
//! a waiting terminal) made obsolete changes nothing; and a stale report
//! leaves the subgroup that took the place alone. A FIN the host accepts
//! after the session closed is reported as part of its shutdown.
//! On drafts 18 and 22, early object and request streams that fill the
//! pending bound before setup do not hide the peer's control stream, and
//! neither does any number of streams whose type is incomplete or that have
//! sent nothing; a stream whose type completes after setup waits with that
//! prefix while the pending streams are full.
//! A live pair moves between threads mid-exchange and completes.
//! The seeded case prints its seed; set `MOQ5_HOST_SEED` to replay it.

use std::cell::Cell;
use std::ffi::c_void;

use moq5_core::{
    AcceptFetch, AcceptPublish, AcceptSubscribe, AcceptTrackStatus, Announcement, AuthToken,
    Buffer, ClusterPath, Error, EventKind, Fetch, FetchConfig, FetchObjectWrite, FetchRange,
    FetchUpdate, FillConfig, FillEnd, FilterWindow, GroupOrder, Instant, Joining, JoiningStart,
    Location, LocationFilter, Namespace, NamespaceInterest, NamespaceUpdateConfig, ObjectStatus,
    ObjectWrite, Perspective, Publication, PublishConfig, PublishNamespaceConfig,
    PublishStateConfig, RedirectTarget, Rejection, RequestHandle, Session, SessionConfig,
    SessionState, StreamRef, SubgroupConfig, SubscribeConfig, SubscribeFilter,
    SubscribeNamespaceConfig, SubscribeTracksConfig, Subscription, SubscriptionUpdate,
    TrackDoneConfig, TrackStatusConfig, UpdateOutcome, Version,
};

// The host and its transport are shared with the example, which uses parts
// of them these tests do not.
#[allow(dead_code)]
#[path = "../examples/transport_host/host.rs"]
mod host;
#[allow(dead_code)]
#[path = "../examples/transport_host/wire.rs"]
mod wire;

use host::{Options, Pair, PeerEnd, Seen, CLIENT, SERVER};

const PASSES: u32 = 400;
const VERSIONS: [Version; 3] = [Version::DRAFT_16, Version::DRAFT_18, Version::DRAFT_22];
const PROTOCOL_VIOLATION: u64 = 0x3;

// -- Payloads that count their release -------------------------------------

thread_local! {
    static CREATED: Cell<usize> = const { Cell::new(0) };
    static RELEASED: Cell<usize> = const { Cell::new(0) };
}

unsafe extern "C" fn release(context: *mut c_void, _data: *const u8, _len: usize) {
    // SAFETY: the context is the box `counted` leaked; libmoq calls this
    // once, on the final release. Freeing a Vec and a counter update cannot
    // unwind or reach a session.
    drop(unsafe { Box::from_raw(context.cast::<Vec<u8>>()) });
    RELEASED.with(|released| released.set(released.get() + 1));
}

/// A payload whose final release is counted on this thread.
fn counted(bytes: &[u8]) -> Buffer {
    let owner = Box::new(bytes.to_vec());
    let (data, len) = (owner.as_ptr(), owner.len());
    let context = Box::into_raw(owner);
    // SAFETY: the Vec's heap block stays valid and unchanged until `release`
    // frees it; `release` only frees it and counts.
    let buffer = unsafe { Buffer::wrap_raw(data, len, Some(release), context.cast()) }
        .expect("a wrapped payload");
    CREATED.with(|created| created.set(created.get() + 1));
    buffer
}

fn outstanding() -> usize {
    CREATED.with(Cell::get) - RELEASED.with(Cell::get)
}

// -- Helpers -------------------------------------------------------------------

fn modes() -> [(&'static str, Options); 2] {
    [
        ("untagged", Options::default()),
        (
            "tagged",
            Options {
                tagged: [true, true],
                ..Options::default()
            },
        ),
    ]
}

fn open(version: Version, options: &Options) -> Pair {
    let mut pair = Pair::new(version, options).expect("a pair");
    assert!(pair.run(PASSES), "setup settles");
    assert!(pair.established(), "both sides established");
    pair.drain(CLIENT);
    pair.drain(SERVER);
    pair
}

/// The client closes; both sides settle; nothing is left: no held record,
/// no live payload.
fn close(mut pair: Pair) {
    let now = pair.now;
    pair.session(CLIENT).close(0, "end", now).expect("a close");
    assert!(pair.run(PASSES));
    pair.drain(CLIENT);
    let server = pair.drain(SERVER);
    assert_eq!(pair.wire.closed[CLIENT], Some(0));
    assert_eq!(pair.hosts[SERVER].session.state(), SessionState::Closed);
    assert_eq!(count(&server, EventKind::SessionClosed), 1);
    assert_eq!(pair.hosts[CLIENT].held(), 0);
    assert_eq!(pair.hosts[SERVER].held(), 0);
    assert_eq!(pair.live_records(), 0, "every held record released");
    assert_eq!(
        pair.wire.overclaimed,
        [0, 0],
        "no reset promised unwritten bytes"
    );
    drop(pair);
}

fn count(seen: &[Seen], kind: EventKind) -> usize {
    seen.iter().filter(|s| s.kind == Some(kind)).count()
}

fn first(seen: &[Seen], kind: EventKind) -> Option<&Seen> {
    seen.iter().find(|s| s.kind == Some(kind))
}

fn index(seen: &[Seen], kind: EventKind) -> Option<usize> {
    seen.iter().position(|s| s.kind == Some(kind))
}

fn live() -> Namespace {
    Namespace::parse("live").expect("a namespace")
}

/// Retry `op` on `side` across runs while it would block (an end waits for
/// the host to report its streams' opens in the tagged mode).
fn retry(
    pair: &mut Pair,
    side: usize,
    mut op: impl FnMut(&mut moq5_core::Session, moq5_core::Instant) -> moq5_core::Result<()>,
) {
    for _ in 0..64 {
        let now = pair.now;
        match op(pair.session(side), now) {
            Err(moq5_core::Error::WOULD_BLOCK) => {
                pair.run(PASSES);
            }
            other => {
                other.expect("the call succeeds once the host has run");
                return;
            }
        }
    }
    panic!("still blocked after 64 runs");
}

struct Track {
    client: Subscription,
    server: Subscription,
}

fn subscribe(pair: &mut Pair, name: &str) -> Track {
    let now = pair.now;
    let config = SubscribeConfig::new(
        live(),
        name,
        LocationFilter::AbsoluteStart(Location::default()),
    );
    let client = pair
        .session(CLIENT)
        .subscribe(&config, now)
        .expect("subscribe");
    assert!(pair.run(PASSES));
    let seen = pair.drain(SERVER);
    let request = first(&seen, EventKind::SubscribeRequest).expect("a subscribe request");
    assert_eq!(request.text, name.as_bytes());
    let server = request.subscription.expect("its subscription");
    let now = pair.now;
    pair.session(SERVER)
        .accept_subscribe(server, &AcceptSubscribe::default(), now)
        .expect("accept");
    assert!(pair.run(PASSES));
    let seen = pair.drain(CLIENT);
    assert_eq!(count(&seen, EventKind::SubscribeOk), 1);
    assert_eq!(
        first(&seen, EventKind::SubscribeOk).and_then(|s| s.subscription),
        Some(client)
    );
    Track { client, server }
}

fn group(pair: &mut Pair, track: &Track, group: u64) -> moq5_core::Subgroup {
    let now = pair.now;
    pair.session(SERVER)
        .open_subgroup(track.server, &SubgroupConfig::new(group), now)
        .expect("a subgroup")
}

fn write(pair: &mut Pair, subgroup: moq5_core::Subgroup, object: u64, text: &str) {
    let now = pair.now;
    pair.session(SERVER)
        .write_object(subgroup, object, counted(text.as_bytes()), now)
        .expect("a write");
}

fn objects(seen: &[Seen]) -> Vec<(u64, u64, Vec<u8>)> {
    seen.iter()
        .filter(|s| s.kind == Some(EventKind::ObjectReceived))
        .map(|s| (s.group, s.object, s.payload.clone()))
        .collect()
}

// -- Scenarios -------------------------------------------------------------------

#[test]
fn setup_on_every_draft() {
    for version in VERSIONS {
        for (write_limit, read_limit) in [(0, 0), (1, 0), (0, 1), (3, 2)] {
            let options = Options {
                write_limit,
                read_limit,
                ..Options::default()
            };
            let mut pair = Pair::new(version, &options).expect("a pair");
            assert!(pair.run(PASSES), "draft {} settles", version.draft());
            assert!(pair.established());
            let c = pair.drain(CLIENT);
            let s = pair.drain(SERVER);
            assert_eq!(count(&c, EventKind::SetupComplete), 1);
            assert_eq!(count(&s, EventKind::SetupComplete), 1);
            // Where each side sends control on its own unidirectional
            // stream, each routed exactly one peer stream there.
            let want = u32::from(version != Version::DRAFT_16);
            for side in [CLIENT, SERVER] {
                let n = pair.hosts[side].counters();
                assert_eq!(n.routed_control, want, "draft {}", version.draft());
                assert_eq!(n.violations, 0);
                if want == 1 && read_limit == 1 {
                    // A two-byte stream type read one byte at a time.
                    assert!(n.need_more >= 1);
                }
            }
            if write_limit == 1 {
                assert!(pair.hosts[CLIENT].counters().partial_writes >= 1);
            }
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_subscription_delivers_its_objects_and_ends() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            for write_limit in [0, 5] {
                let options = Options {
                    write_limit,
                    ..base.clone()
                };
                let mut pair = open(version, &options);
                let track = subscribe(&mut pair, "t");
                let sg = group(&mut pair, &track, 0);
                write(&mut pair, sg, 0, "public host");
                write(&mut pair, sg, 1, "second");
                let now = pair.now;
                pair.session(SERVER)
                    .close_subgroup(sg, now)
                    .expect("close subgroup");
                let done = TrackDoneConfig::new(0).stream_count(1).reason("over");
                retry(&mut pair, SERVER, |s, now| {
                    s.done_subscribe(track.server, &done, now)
                });
                assert!(pair.run(PASSES));
                let c = pair.drain(CLIENT);
                let what = format!("draft {} {mode} limit {write_limit}", version.draft());
                assert_eq!(
                    objects(&c),
                    [(0, 0, b"public host".to_vec()), (0, 1, b"second".to_vec())],
                    "{what}"
                );
                assert!(c
                    .iter()
                    .filter(|s| s.kind == Some(EventKind::ObjectReceived))
                    .all(|s| s.subscription == Some(track.client) && s.publication.is_none()));
                let finished = index(&c, EventKind::SubgroupFinished).expect("the stream's end");
                let done = index(&c, EventKind::SubscribeDone).expect("the done");
                assert!(finished < done, "{what}");
                assert_eq!(c[done].subscription, Some(track.client));
                assert_eq!(c[done].count, 1);
                assert_eq!(c[done].text, b"over");
                if write_limit != 0 {
                    assert!(pair.hosts[SERVER].counters().partial_writes >= 1, "{what}");
                    assert!(pair.hosts[SERVER].counters().held_records >= 1, "{what}");
                }
                if options.tagged[SERVER] {
                    // The host reported its stream's open; the session's
                    // own count went into the done.
                    assert_eq!(pair.hosts[SERVER].counters().opens_reported, 1, "{what}");
                }
                assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
                assert_eq!(pair.hosts[CLIENT].counters().routed_data, 1);
                close(pair);
            }
        }
    }
    assert_eq!(outstanding(), 0, "every payload released once");
}

#[test]
fn a_server_publication_carries_its_identity() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let mut pair = open(version, &options);
            let now = pair.now;
            let server_pub = pair
                .session(SERVER)
                .publish(&PublishConfig::new(live(), "pub"), now)
                .expect("publish");
            assert!(pair.run(PASSES));
            let c = pair.drain(CLIENT);
            let request = first(&c, EventKind::PublishRequest).expect("a publish request");
            assert_eq!(request.text, b"pub");
            let client_pub = request.publication.expect("its publication");
            let now = pair.now;
            pair.session(CLIENT)
                .accept_publish(client_pub, &AcceptPublish::default(), now)
                .expect("accept publish");
            assert!(pair.run(PASSES));
            let s = pair.drain(SERVER);
            assert_eq!(
                first(&s, EventKind::PublishOk).and_then(|e| e.publication),
                Some(server_pub)
            );
            let now = pair.now;
            let sg = pair
                .session(SERVER)
                .open_publication_subgroup(server_pub, &SubgroupConfig::new(5), now)
                .expect("a subgroup");
            write(&mut pair, sg, 0, "p-zero");
            write(&mut pair, sg, 1, "p-one");
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(sg, now)
                .expect("close subgroup");
            let done = TrackDoneConfig::new(0).stream_count(1).reason("end");
            retry(&mut pair, SERVER, |s, now| {
                s.finish_publish(server_pub, &done, now)
            });
            assert!(pair.run(PASSES));
            let c = pair.drain(CLIENT);
            let what = format!("draft {} {mode}", version.draft());
            assert_eq!(
                objects(&c),
                [(5, 0, b"p-zero".to_vec()), (5, 1, b"p-one".to_vec())],
                "{what}"
            );
            assert!(c
                .iter()
                .filter(|s| s.kind == Some(EventKind::ObjectReceived))
                .all(|s| s.publication == Some(client_pub) && s.subscription.is_none()));
            let finished = first(&c, EventKind::PublishFinished).expect("its end");
            assert_eq!(finished.publication, Some(client_pub));
            assert_eq!(finished.text, b"end");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn fetches_with_objects_and_empty() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            for empty in [false, true] {
                let what = format!("draft {} {mode} empty {empty}", version.draft());
                let mut pair = open(version, &options);
                let now = pair.now;
                let config = FetchConfig::new(live(), "past", Location::default(), 0, 3);
                let client_fetch = pair.session(CLIENT).fetch(&config, now).expect("fetch");
                assert!(pair.run(PASSES));
                let s = pair.drain(SERVER);
                let request = first(&s, EventKind::FetchRequest).expect("a fetch request");
                assert_eq!(request.text, b"past");
                assert_eq!((request.group, request.object), (0, 3));
                let server_fetch = request.fetch.expect("its fetch");
                let now = pair.now;
                let mut accept = AcceptFetch::new(0, 3);
                accept.empty = empty;
                pair.session(SERVER)
                    .accept_fetch(server_fetch, &accept, now)
                    .expect("accept fetch");
                if !empty {
                    for id in 0..2u64 {
                        let mut object =
                            FetchObjectWrite::new(0, id, counted(format!("f-{id}").as_bytes()));
                        object.publisher_priority = 100;
                        let now = pair.now;
                        pair.session(SERVER)
                            .write_fetch_object(server_fetch, object, now)
                            .expect("a fetch object");
                    }
                    retry(&mut pair, SERVER, |s, now| s.end_fetch(server_fetch, now));
                }
                assert!(pair.run(PASSES));
                let c = pair.drain(CLIENT);
                let ok = first(&c, EventKind::FetchOk).expect("the fetch's answer");
                assert_eq!(ok.fetch, Some(client_fetch), "{what}");
                assert_eq!((ok.group, ok.object), (0, 3), "{what}");
                let got: Vec<(u64, Vec<u8>)> = c
                    .iter()
                    .filter(|s| s.kind == Some(EventKind::FetchObject))
                    .map(|s| (s.object, s.payload.clone()))
                    .collect();
                if empty {
                    assert!(got.is_empty(), "{what}");
                } else {
                    assert_eq!(got, [(0, b"f-0".to_vec()), (1, b"f-1".to_vec())], "{what}");
                }
                let complete = index(&c, EventKind::FetchComplete).expect("its completion");
                assert_eq!(c[complete].fetch, Some(client_fetch));
                assert_eq!(count(&c, EventKind::FetchError), 0, "{what}");
                assert_eq!(count(&c, EventKind::FetchReset), 0, "{what}");
                assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
                close(pair);
            }
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn event_pressure_resumes_each_delivery_once() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let mut options = base.clone();
            options.max_events[CLIENT] = 2;
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "t");
            let sg = group(&mut pair, &track, 0);
            let text = ["a", "b", "c", "d", "e", "f"];
            for (id, t) in text.iter().enumerate() {
                write(&mut pair, sg, id as u64, t);
            }
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(sg, now)
                .expect("close subgroup");
            // Two events of room: every object still arrives once, in order.
            let mut seen = Vec::new();
            for _ in 0..64 {
                pair.run(PASSES);
                seen.extend(pair.drain(CLIENT));
                if count(&seen, EventKind::SubgroupFinished) == 1 {
                    break;
                }
            }
            let what = format!("draft {} {mode}", version.draft());
            let got: Vec<Vec<u8>> = objects(&seen).into_iter().map(|o| o.2).collect();
            let want: Vec<Vec<u8>> = text.iter().map(|t| t.as_bytes().to_vec()).collect();
            assert_eq!(got, want, "{what}");
            let n = pair.hosts[CLIENT].counters();
            assert!(n.owned_retries >= 1, "{what}: a kept delivery was resumed");
            assert_eq!(n.refused_inputs, 0, "{what}");
            assert!(pair.hosts[CLIENT].fault().is_none());
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_refused_stream_is_offered_again_once() {
    for version in VERSIONS {
        for feed_buffers in [false, true] {
            for (mode, base) in modes() {
                let mut options = base.clone();
                options.max_data_streams[CLIENT] = 1;
                options.feed_buffers[CLIENT] = feed_buffers;
                let what = format!("draft {} {mode} buffers {feed_buffers}", version.draft());
                let mut pair = open(version, &options);
                let track = subscribe(&mut pair, "t");
                let g0 = group(&mut pair, &track, 0);
                write(&mut pair, g0, 0, "zero");
                assert!(pair.run(PASSES));
                let g1 = group(&mut pair, &track, 1);
                write(&mut pair, g1, 0, "one");
                assert!(pair.run(PASSES));
                // One receive entry, held by group 0: group 1's stream is not
                // admitted, and the host keeps its bytes.
                let mut c = pair.drain(CLIENT);
                assert_eq!(objects(&c).len(), 1, "{what}");
                assert_eq!(pair.hosts[CLIENT].counters().refused_inputs, 1, "{what}");
                assert!(!pair.hosts[CLIENT].session.can_admit_data_stream());
                // Control keeps moving under the data pressure: a request and
                // its answer still pass.
                let now = pair.now;
                let fetch = FetchConfig::new(live(), "other", Location::default(), 0, 1);
                let client_fetch = pair.session(CLIENT).fetch(&fetch, now).expect("a fetch");
                assert!(pair.run(PASSES));
                let s = pair.drain(SERVER);
                let server_fetch = first(&s, EventKind::FetchRequest)
                    .and_then(|r| r.fetch)
                    .expect("the request arrived under data pressure");
                let now = pair.now;
                pair.session(SERVER)
                    .reject_fetch(server_fetch, &Rejection::new(0x10), now)
                    .expect("a refusal");
                assert!(pair.run(PASSES));
                let refusal = pair.drain(CLIENT);
                assert_eq!(
                    first(&refusal, EventKind::FetchError).and_then(|e| e.fetch),
                    Some(client_fetch),
                    "{what}"
                );
                c.extend(refusal);
                assert_eq!(
                    pair.hosts[CLIENT].counters().redelivered,
                    0,
                    "{what}: still refused"
                );
                // Group 0 ends; its entry frees; the kept input is taken.
                let now = pair.now;
                pair.session(SERVER)
                    .close_subgroup(g0, now)
                    .expect("close group 0");
                assert!(pair.run(PASSES));
                c.extend(pair.drain(CLIENT));
                let n = pair.hosts[CLIENT].counters();
                assert_eq!(n.redelivered, 1, "{what}: offered again exactly once");
                if feed_buffers {
                    assert_eq!(n.same_buffer_redelivered, 1, "{what}: the very buffer");
                }
                assert_eq!(
                    objects(&c),
                    [(0, 0, b"zero".to_vec()), (1, 0, b"one".to_vec())],
                    "{what}"
                );
                let now = pair.now;
                pair.session(SERVER)
                    .close_subgroup(g1, now)
                    .expect("close group 1");
                assert!(pair.run(PASSES));
                c.extend(pair.drain(CLIENT));
                assert_eq!(count(&c, EventKind::SubgroupFinished), 2, "{what}");
                assert_eq!(objects(&c).len(), 2, "{what}: nothing twice");
                assert!(pair.hosts[CLIENT].fault().is_none());
                close(pair);
            }
        }
    }
    assert_eq!(outstanding(), 0);
}

/// The first byte of the two-byte vi64 of the draft-18/22 SETUP stream type
/// (0x2F00): a 10 prefix, then the value's high bits.
const SETUP_TYPE_FIRST: u8 = 0xaf;

#[test]
fn streams_that_end_inside_their_type() {
    struct Case {
        name: &'static str,
        bytes: &'static [u8],
        end: PeerEnd,
        closes: bool,
    }
    let cases = [
        Case {
            name: "empty-reset",
            bytes: &[],
            end: PeerEnd::Reset(9),
            closes: false,
        },
        Case {
            name: "empty-fin",
            bytes: &[],
            end: PeerEnd::Fin,
            closes: false,
        },
        Case {
            name: "prefix-reset",
            bytes: &[SETUP_TYPE_FIRST],
            end: PeerEnd::Reset(9),
            closes: false,
        },
        Case {
            name: "prefix-fin",
            bytes: &[SETUP_TYPE_FIRST],
            end: PeerEnd::Fin,
            closes: true,
        },
        Case {
            name: "unknown-type",
            bytes: &[0x06],
            end: PeerEnd::None,
            closes: true,
        },
    ];
    for version in VERSIONS {
        let multibyte = version != Version::DRAFT_16;
        for case in &cases {
            if !multibyte && case.bytes.len() == 1 && case.bytes[0] != 0x06 {
                continue; // draft 16 has no multi-byte stream types to cut
            }
            let what = format!("draft {} {}", version.draft(), case.name);
            let mut pair = open(version, &Options::default());
            let before = pair.hosts[SERVER].counters().clone();
            assert!(pair.peer_opens_uni(SERVER, case.bytes, case.end));
            assert!(pair.run(PASSES), "{what}");
            let n = pair.hosts[SERVER].counters().clone();
            // Never taken for the control channel.
            assert_eq!(n.routed_control, before.routed_control, "{what}");
            assert_eq!(n.control_ended, before.control_ended, "{what}");
            assert!(pair.hosts[SERVER].fault().is_none(), "{what}");
            let s = pair.drain(SERVER);
            if case.closes {
                assert_eq!(
                    pair.hosts[SERVER].session.state(),
                    SessionState::Closed,
                    "{what}"
                );
                assert_eq!(pair.wire.closed[SERVER], Some(PROTOCOL_VIOLATION), "{what}");
                assert_eq!(count(&s, EventKind::SessionClosed), 1, "{what}");
                assert_eq!(
                    first(&s, EventKind::SessionClosed).map(|e| e.code),
                    Some(PROTOCOL_VIOLATION)
                );
                // On draft 16 the session closed itself; elsewhere this host
                // closed it on the route's answer.
                assert_eq!(
                    n.violations,
                    before.violations + u32::from(multibyte),
                    "{what}"
                );
                pair.drain(CLIENT);
                drop(pair);
                continue;
            }
            assert_eq!(
                pair.hosts[SERVER].session.state(),
                SessionState::Established,
                "{what}: {s:?} {n:?}"
            );
            assert_eq!(n.violations, before.violations, "{what}");
            if multibyte || matches!(case.end, PeerEnd::Reset(_)) {
                assert_eq!(n.ended_unrouted, before.ended_unrouted + 1, "{what}");
            }
            if multibyte && case.bytes.len() == 1 {
                assert!(n.need_more > before.need_more, "{what}");
            }
            assert_eq!(count(&s, EventKind::SessionClosed), 0, "{what}");
            close(pair);
        }
    }
}

#[test]
fn a_peer_stop_on_a_data_stream_resets_it() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "t");
            let sg = group(&mut pair, &track, 0);
            write(&mut pair, sg, 0, "before the stop");
            assert!(pair.run(PASSES));
            let c = pair.drain(CLIENT);
            assert_eq!(objects(&c).len(), 1, "{what}");
            let streams = pair.hosts[SERVER].local_data_streams();
            assert_eq!(streams.len(), 1, "{what}");
            // The subscriber's transport stops the publisher's data stream.
            assert!(pair.peer_stops_data(SERVER, streams[0], 0x42));
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let n = pair.hosts[SERVER].counters();
            assert_eq!(n.peer_stops, 1, "{what}");
            let terminated =
                first(&s, EventKind::OutgoingSubgroupTerminated).expect("the subgroup's terminal");
            assert_eq!(terminated.subscription, Some(track.server), "{what}");
            assert_eq!(
                terminated.code, 0x42,
                "{what}: the reset keeps the STOP's code"
            );
            if options.tagged[SERVER] {
                assert!(
                    n.claims >= 1 && n.commits >= 1,
                    "{what}: the reset transaction ran"
                );
            }
            assert_eq!(pair.wire.last_reset_code[SERVER], 0x42, "{what}");
            // The subgroup is ended: a write now is refused.
            let now = pair.now;
            assert!(pair
                .session(SERVER)
                .write_object(sg, 1, counted(b"late"), now)
                .is_err());
            assert!(pair.run(PASSES));
            let c = pair.drain(CLIENT);
            assert_eq!(count(&c, EventKind::SubgroupReset), 1, "{what}");
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_stopped_stream_is_retired_without_a_peer_terminal() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                drop_after_stop: true,
                ..base.clone()
            };
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "t");
            let sg = group(&mut pair, &track, 0);
            write(&mut pair, sg, 0, "kept open");
            assert!(pair.run(PASSES));
            let c = pair.drain(CLIENT);
            assert_eq!(objects(&c), [(0, 0, b"kept open".to_vec())], "{what}");
            let data = pair.hosts[CLIENT].open_peer_data_streams();
            assert_eq!(data.len(), 1, "{what}");
            assert!(pair.hosts[CLIENT].session.has_transport_stream(data[0]));
            let fins = pair.hosts[CLIENT].counters().peer_fins;
            let resets = pair.hosts[CLIENT].counters().peer_resets;
            let now = pair.now;
            pair.session(CLIENT)
                .unsubscribe(track.client, now)
                .expect("unsubscribe");
            assert!(pair.run(PASSES), "{what}");
            let n = pair.hosts[CLIENT].counters();
            assert_eq!(n.data_retired, 1, "{what}");
            assert_eq!(n.retire_stale, 0, "{what}");
            assert_eq!(n.peer_fins, fins, "{what}: nothing arrived after the stop");
            assert_eq!(n.peer_resets, resets, "{what}");
            if version != Version::DRAFT_16 {
                assert_eq!(n.bidi_retired, 1, "{what}: the request stream too");
            }
            assert!(
                !pair.hosts[CLIENT].session.has_transport_stream(data[0]),
                "{what}"
            );
            assert!(pair.hosts[CLIENT].session.can_admit_data_stream());
            assert!(pair.hosts[SERVER].counters().peer_stops >= 1, "{what}");
            // No peer terminal is fabricated, and the session lives on.
            let after = pair.drain(CLIENT);
            assert_eq!(count(&after, EventKind::SubgroupFinished), 0, "{what}");
            assert_eq!(count(&after, EventKind::SubgroupReset), 0, "{what}");
            assert_eq!(
                pair.hosts[CLIENT].session.state(),
                SessionState::Established
            );
            assert!(
                pair.hosts.iter().all(|h| h.fault().is_none()),
                "{what}: {:?} {:?}",
                pair.hosts[CLIENT].fault(),
                pair.hosts[SERVER].fault()
            );
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn the_end_of_a_control_stream_closes_the_session() {
    for version in VERSIONS {
        for victim in [CLIENT, SERVER] {
            for by_reset in [false, true] {
                let what = format!("draft {} victim {victim} reset {by_reset}", version.draft());
                let mut pair = open(version, &Options::default());
                // The peer of `victim` ends the control stream it sends on.
                let index = pair
                    .wire
                    .streams
                    .iter()
                    .position(|s| {
                        if version == Version::DRAFT_16 {
                            s.bidi && s.opener == CLIENT
                        } else {
                            !s.bidi && s.opener == 1 - victim
                        }
                    })
                    .expect("the control stream");
                if by_reset {
                    pair.wire.reset(index, 1 - victim, 0x55, 0);
                } else {
                    pair.wire.fin(index, 1 - victim);
                }
                assert!(pair.run(PASSES), "{what}");
                assert_eq!(pair.hosts[victim].counters().control_ended, 1, "{what}");
                assert_eq!(
                    pair.hosts[victim].session.state(),
                    SessionState::Closed,
                    "{what}"
                );
                assert_eq!(pair.wire.closed[victim], Some(PROTOCOL_VIOLATION), "{what}");
                let v = pair.drain(victim);
                assert_eq!(
                    first(&v, EventKind::SessionClosed).map(|e| e.code),
                    Some(PROTOCOL_VIOLATION)
                );
                let peer = pair.drain(1 - victim);
                assert_eq!(
                    count(&peer, EventKind::SessionClosed),
                    1,
                    "{what}: the peer learns"
                );
            }
        }
    }
}

/// A session GOAWAY is a control message after setup: on drafts 18 and 22
/// it continues the unidirectional control stream the setup opened.
#[test]
fn a_goaway_reaches_the_peer() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let now = pair.now;
            pair.session(SERVER)
                .goaway(b"moqt://next.example/moq", now)
                .expect("a GOAWAY");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let goaway = first(&c, EventKind::Goaway).expect("the GOAWAY");
            assert_eq!(goaway.text, b"moqt://next.example/moq", "{what}");
            assert_eq!(
                pair.hosts[SERVER].session.state(),
                SessionState::Draining,
                "{what}"
            );
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
}

#[test]
fn namespaces_are_advertised_and_discovered() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            // The client advertises a namespace; the server accepts it.
            let now = pair.now;
            let ann = pair
                .session(CLIENT)
                .publish_namespace(&live(), now)
                .expect("an advertisement");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let published = first(&s, EventKind::NamespacePublished).expect("the advertisement");
            assert_eq!(published.text, b"live", "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace(published.announcement.expect("its handle"), now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::NamespaceAccepted).and_then(|e| e.announcement),
                Some(ann),
                "{what}"
            );
            // The client subscribes to namespaces under "live"; the server
            // accepts and announces one.
            let now = pair.now;
            let ns_sub = pair
                .session(CLIENT)
                .subscribe_namespace(&live(), moq5_core::NamespaceInterest::NamespaceState, now)
                .expect("a namespace subscription");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::NamespaceSubscriptionRequest).expect("the request");
            assert_eq!(request.text, b"live", "{what}");
            let server_sub = request.namespace_subscription.expect("its handle");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace_subscription(server_sub, now)
                .expect("accept");
            let suffix = Namespace::parse("room1").expect("a suffix");
            pair.session(SERVER)
                .send_namespace(server_sub, &suffix, now)
                .expect("a NAMESPACE");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::NamespaceSubscriptionOk)
                    .and_then(|e| e.namespace_subscription),
                Some(ns_sub),
                "{what}"
            );
            let found = first(&c, EventKind::NamespaceFound).expect("the found namespace");
            assert_eq!(found.namespace_subscription, Some(ns_sub), "{what}");
            assert_eq!(found.text, b"room1", "{what}");
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
}

#[test]
fn streamed_objects_and_chunks() {
    for version in VERSIONS {
        for streaming in [false, true] {
            let what = format!("draft {} chunks {streaming}", version.draft());
            let options = Options {
                tagged: [true, true],
                streaming_objects: [streaming, false],
                ..Options::default()
            };
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "streamed");
            let sg = group(&mut pair, &track, 0);
            let now = pair.now;
            let server = pair.session(SERVER);
            server.begin_object(sg, 0, 10, now).expect("begin");
            server
                .write_object_data(sg, counted(b"hello"), now)
                .expect("first part");
            server
                .write_object_data(sg, counted(b"world"), now)
                .expect("second part");
            server.end_object(sg, now).expect("end");
            server.close_subgroup(sg, now).expect("close subgroup");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            if streaming {
                let chunks: Vec<&Seen> = c
                    .iter()
                    .filter(|s| s.kind == Some(EventKind::ObjectChunk))
                    .collect();
                let bytes: Vec<u8> = chunks.iter().flat_map(|s| s.payload.clone()).collect();
                assert_eq!(bytes, b"helloworld", "{what}");
                assert!(
                    chunks.last().is_some_and(|s| s.flag),
                    "{what}: the last chunk ends it"
                );
            } else {
                assert_eq!(objects(&c), [(0, 0, b"helloworld".to_vec())], "{what}");
            }
            assert_eq!(count(&c, EventKind::SubgroupFinished), 1, "{what}");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn an_object_datagram_arrives_as_one() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "dgram");
            let now = pair.now;
            let location = Location {
                group_id: 4,
                object_id: 2,
            };
            pair.session(SERVER)
                .send_object_datagram(
                    track.server,
                    location,
                    7,
                    false,
                    counted(b"a datagram"),
                    &[],
                    now,
                )
                .expect("a datagram");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let object = first(&c, EventKind::ObjectReceived).expect("the datagram's object");
            assert!(object.flag, "{what}: delivered as a datagram");
            assert_eq!((object.group, object.object), (4, 2), "{what}");
            assert_eq!(object.payload, b"a datagram", "{what}");
            assert_eq!(object.subscription, Some(track.client), "{what}");
            assert!(pair.hosts[SERVER].counters().datagrams_sent >= 1, "{what}");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

/// Seeded fragmentation of every write and read: the same flows, the same
/// outcomes. The seed is printed; `MOQ5_HOST_SEED` replays one.
#[test]
fn seeded_fragmentation() {
    let seeds: Vec<u64> = match std::env::var("MOQ5_HOST_SEED") {
        Ok(seed) => vec![seed.parse().expect("MOQ5_HOST_SEED is a number")],
        Err(_) => vec![0x5eed_0001, 0x5eed_0002, 0x5eed_0003],
    };
    for seed in seeds {
        println!("seed {seed} (replay with MOQ5_HOST_SEED={seed})");
        for version in VERSIONS {
            for (mode, base) in modes() {
                let what = format!("seed {seed} draft {} {mode}", version.draft());
                let options = Options {
                    seed: Some(seed),
                    ..base.clone()
                };
                let mut pair = Pair::new(version, &options).expect("a pair");
                assert!(pair.run(PASSES * 4), "{what}: setup settles");
                assert!(pair.established(), "{what}");
                pair.drain(CLIENT);
                pair.drain(SERVER);
                let track = subscribe(&mut pair, "seeded");
                let sg = group(&mut pair, &track, 3);
                for id in 0..5u64 {
                    write(&mut pair, sg, id, &format!("object {id} of a seeded run"));
                }
                let now = pair.now;
                pair.session(SERVER)
                    .close_subgroup(sg, now)
                    .expect("close subgroup");
                let mut seen = Vec::new();
                for _ in 0..16 {
                    pair.run(PASSES);
                    seen.extend(pair.drain(CLIENT));
                }
                let got: Vec<u64> = objects(&seen).iter().map(|o| o.1).collect();
                assert_eq!(got, [0, 1, 2, 3, 4], "{what}");
                assert_eq!(count(&seen, EventKind::SubgroupFinished), 1, "{what}");
                assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
                close(pair);
            }
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn cleanup_releases_every_held_record_once() {
    for version in VERSIONS {
        let options = Options {
            tagged: [true, true],
            write_limit: 3,
            ..Options::default()
        };
        let mut pair = open(version, &options);
        let track = subscribe(&mut pair, "t");
        let sg = group(&mut pair, &track, 0);
        for id in 0..4u64 {
            write(&mut pair, sg, id, "some payload bytes");
        }
        // Held mid-flight: the transport refuses every byte for a while.
        pair.wire.refuse_bytes[SERVER] = true;
        pair.run(8);
        assert!(
            pair.hosts[SERVER].held() > 0,
            "draft {}: output held",
            version.draft()
        );
        assert!(pair.live_records() > 0);
        // Held records own copies; payloads still queued in the session are
        // the caller's buffers, released when the session goes.
        write(&mut pair, sg, 4, "queued in the session");
        write(&mut pair, sg, 5, "queued as well");
        assert!(outstanding() >= 2);
        // Tear down with output still held: every record and payload goes
        // exactly once (a second release would fail the count or crash).
        let records = pair.live_counter();
        drop(pair);
        assert_eq!(
            *records.lock().unwrap_or_else(|e| e.into_inner()),
            0,
            "draft {}: every held record released",
            version.draft()
        );
        assert_eq!(
            outstanding(),
            0,
            "draft {}: every payload released once",
            version.draft()
        );
    }
}

// -- Request families ----------------------------------------------------------

/// A session of `version` that never met the pair: every handle of the pair
/// is foreign to it.
fn stranger(version: Version) -> Session {
    let config = SessionConfig::new(Perspective::Server, version);
    Session::try_new(&config, Instant::from_micros(1)).expect("a session")
}

/// The server PUBLISHes `name` and the client accepts: (server, client).
fn publication(pair: &mut Pair, name: &str) -> (Publication, Publication) {
    let now = pair.now;
    let server = pair
        .session(SERVER)
        .publish(&PublishConfig::new(live(), name), now)
        .expect("publish");
    assert!(pair.run(PASSES));
    let c = pair.drain(CLIENT);
    let client = first(&c, EventKind::PublishRequest)
        .and_then(|e| e.publication)
        .expect("a publish request");
    let now = pair.now;
    pair.session(CLIENT)
        .accept_publish(client, &AcceptPublish::default(), now)
        .expect("accept publish");
    assert!(pair.run(PASSES));
    let s = pair.drain(SERVER);
    assert_eq!(
        first(&s, EventKind::PublishOk).and_then(|e| e.publication),
        Some(server)
    );
    (server, client)
}

/// The client advertises `namespace` and the server accepts: (client,
/// server).
fn advertise(pair: &mut Pair, namespace: &Namespace) -> (Announcement, Announcement) {
    let now = pair.now;
    let client = pair
        .session(CLIENT)
        .publish_namespace(namespace, now)
        .expect("an advertisement");
    assert!(pair.run(PASSES));
    let s = pair.drain(SERVER);
    let server = first(&s, EventKind::NamespacePublished)
        .and_then(|e| e.announcement)
        .expect("the advertisement");
    let now = pair.now;
    pair.session(SERVER)
        .accept_namespace(server, now)
        .expect("accept");
    assert!(pair.run(PASSES));
    pair.drain(CLIENT);
    (client, server)
}

fn object_write(group_id: u64, object_id: u64, payload: Buffer) -> FetchObjectWrite {
    let mut object = FetchObjectWrite::new(group_id, object_id, payload);
    object.publisher_priority = 5;
    object
}

fn at(group_id: u64, object_id: u64) -> Location {
    Location {
        group_id,
        object_id,
    }
}

#[test]
fn track_status_is_answered_and_refused() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let now = pair.now;
            let asked = pair
                .session(CLIENT)
                .track_status(&TrackStatusConfig::new(live(), "st"), now)
                .expect("a TRACK_STATUS");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::TrackStatusRequest).expect("the request");
            assert_eq!(request.text, b"st", "{what}");
            let server_req = request.track_status.expect("its handle");
            let mut answer = AcceptTrackStatus::default();
            answer.largest = Some(at(4, 2));
            assert_eq!(
                stranger(version)
                    .accept_track_status(server_req, &answer, now)
                    .err(),
                Some(Error::WRONG_SESSION),
                "{what}"
            );
            let now = pair.now;
            pair.session(SERVER)
                .accept_track_status(server_req, &answer, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let ok = first(&c, EventKind::TrackStatusOk).expect("the answer");
            assert_eq!(ok.track_status, Some(asked), "{what}");
            assert_eq!(ok.largest, Some(at(4, 2)), "{what}");

            // A second request, refused.
            let now = pair.now;
            let asked = pair
                .session(CLIENT)
                .track_status(&TrackStatusConfig::new(live(), "gone"), now)
                .expect("a TRACK_STATUS");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_req = first(&s, EventKind::TrackStatusRequest)
                .and_then(|e| e.track_status)
                .expect("the request");
            let now = pair.now;
            pair.session(SERVER)
                .reject_track_status(server_req, &Rejection::new(0x10).reason("none"), now)
                .expect("reject");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let error = first(&c, EventKind::TrackStatusError).expect("the refusal");
            assert_eq!(error.track_status, Some(asked), "{what}");
            assert_eq!(error.code, 0x10, "{what}");

            // INCLUDE_PROPERTIES travels on draft 22 only.
            let now = pair.now;
            let config = TrackStatusConfig::new(live(), "props").include_properties(false);
            let result = pair.session(CLIENT).track_status(&config, now);
            if version == Version::DRAFT_22 {
                result.expect("INCLUDE_PROPERTIES on draft 22");
                assert!(pair.run(PASSES), "{what}");
            } else {
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
            }
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
}

#[test]
fn track_discovery_is_answered_blocked_and_refused() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let prefix = Namespace::parse("tracks").expect("a prefix");
            let now = pair.now;
            let result = pair
                .session(CLIENT)
                .subscribe_tracks(&SubscribeTracksConfig::new(prefix.clone()), now);
            if version == Version::DRAFT_16 {
                assert_eq!(result.err(), Some(Error::INVAL), "{what}");
                close(pair);
                continue;
            }
            let request = result.expect("a SUBSCRIBE_TRACKS");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let seen = first(&s, EventKind::SubscribeTracksRequest).expect("the request");
            assert_eq!(seen.text, b"tracks", "{what}");
            assert_eq!(seen.forward, Some(true), "{what}");
            let server_req = seen.track_subscription.expect("its handle");
            let now = pair.now;
            // The refusal has no retry interval to carry.
            assert_eq!(
                pair.session(SERVER)
                    .reject_subscribe_tracks(
                        server_req,
                        &Rejection::new(0x10).retry_after_ms(5),
                        now
                    )
                    .err(),
                Some(Error::INVAL),
                "{what}"
            );
            assert_eq!(
                stranger(version)
                    .accept_subscribe_tracks(server_req, now)
                    .err(),
                Some(Error::WRONG_SESSION),
                "{what}"
            );
            pair.session(SERVER)
                .accept_subscribe_tracks(server_req, now)
                .expect("accept");
            let room = Namespace::parse("room").expect("a suffix");
            pair.session(SERVER)
                .send_publish_blocked(server_req, &room, b"cam", now)
                .expect("PUBLISH_BLOCKED");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::SubscribeTracksOk).and_then(|e| e.track_subscription),
                Some(request),
                "{what}"
            );
            let blocked = first(&c, EventKind::PublishBlocked).expect("the blocked track");
            assert_eq!(blocked.track_subscription, Some(request), "{what}");
            assert_eq!(blocked.text, b"room", "{what}");
            assert_eq!(blocked.name, b"cam", "{what}");

            // A second request, without forwarding, refused.
            let other = Namespace::parse("other").expect("a prefix");
            let now = pair.now;
            let refused = pair
                .session(CLIENT)
                .subscribe_tracks(&SubscribeTracksConfig::new(other).forward(false), now)
                .expect("a SUBSCRIBE_TRACKS");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let seen = first(&s, EventKind::SubscribeTracksRequest).expect("the request");
            assert_eq!(seen.forward, Some(false), "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .reject_subscribe_tracks(
                    seen.track_subscription.expect("its handle"),
                    &Rejection::new(0x10),
                    now,
                )
                .expect("reject");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let error = first(&c, EventKind::SubscribeTracksError).expect("the refusal");
            assert_eq!(error.track_subscription, Some(refused), "{what}");
            assert_eq!(error.code, 0x10, "{what}");

            // The options travel on draft 22 only.
            let third = Namespace::parse("third").expect("a prefix");
            let config = SubscribeTracksConfig::new(third).group_order(GroupOrder::Descending);
            let now = pair.now;
            let result = pair.session(CLIENT).subscribe_tracks(&config, now);
            if version == Version::DRAFT_22 {
                result.expect("GROUP_ORDER on draft 22");
                assert!(pair.run(PASSES), "{what}");
            } else {
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
            }
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
}

#[test]
fn a_redirect_names_its_target() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let now = pair.now;
            let config = SubscribeConfig::new(live(), "moved", LocationFilter::NextObject);
            let client_sub = pair
                .session(CLIENT)
                .subscribe(&config, now)
                .expect("subscribe");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_sub = first(&s, EventKind::SubscribeRequest)
                .and_then(|e| e.subscription)
                .expect("the request");
            let target = RedirectTarget {
                connect_uri: b"moqt://other.example/moq".to_vec(),
                namespace: Namespace::parse("elsewhere").expect("a namespace"),
                track_name: b"there".to_vec(),
            };
            let redirect = Rejection::redirect(target.clone()).retry_after_ms(250);
            let now = pair.now;
            let result = pair
                .session(SERVER)
                .reject_subscribe(server_sub, &redirect, now);
            if version == Version::DRAFT_16 {
                // Draft 16 has no REDIRECT: refused, and refused plainly.
                assert_eq!(result.err(), Some(Error::INVAL), "{what}");
                pair.session(SERVER)
                    .reject_subscribe(server_sub, &Rejection::new(0x10), now)
                    .expect("a plain refusal");
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let error = first(&c, EventKind::SubscribeError).expect("the refusal");
                assert_eq!(error.subscription, Some(client_sub), "{what}");
                assert_eq!(error.code, 0x10, "{what}");
                close(pair);
                continue;
            }
            result.expect("a REDIRECT");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let moved = first(&c, EventKind::RequestRedirect).expect("the redirect");
            assert_eq!(
                moved.request,
                Some(RequestHandle::Subscription(client_sub)),
                "{what}"
            );
            assert_eq!(moved.code, 0x34, "{what}");
            assert_eq!(moved.target.as_ref(), Some(&target), "{what}");
            assert_eq!(count(&c, EventKind::SubscribeError), 0, "{what}");

            // The empty target goes literally, and arrives as sent: on draft
            // 18 it means the original track, on draft 22 the root and the
            // empty name. Neither side fills it in.
            let now = pair.now;
            let config = SubscribeConfig::new(live(), "again", LocationFilter::NextObject);
            let again = pair
                .session(CLIENT)
                .subscribe(&config, now)
                .expect("subscribe");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_again = first(&s, EventKind::SubscribeRequest)
                .and_then(|e| e.subscription)
                .expect("the request");
            let now = pair.now;
            pair.session(SERVER)
                .reject_subscribe(
                    server_again,
                    &Rejection::redirect(RedirectTarget::empty()),
                    now,
                )
                .expect("an empty REDIRECT");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let moved = first(&c, EventKind::RequestRedirect).expect("the redirect");
            assert_eq!(
                moved.request,
                Some(RequestHandle::Subscription(again)),
                "{what}"
            );
            assert_eq!(moved.target, Some(RedirectTarget::empty()), "{what}");

            // A PUBLISH refusal: the native configuration has no target
            // field, so a nonempty target is refused before anything is
            // sent; the empty REDIRECT is the draft-22 one.
            let now = pair.now;
            let server_pub = pair
                .session(SERVER)
                .publish(&PublishConfig::new(live(), "pushed"), now)
                .expect("publish");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let client_pub = first(&c, EventKind::PublishRequest)
                .and_then(|e| e.publication)
                .expect("a publish request");
            let now = pair.now;
            assert_eq!(
                pair.session(CLIENT)
                    .reject_publish(client_pub, &Rejection::redirect(target.clone()), now)
                    .err(),
                Some(Error::INVAL),
                "{what}: no target field"
            );
            let empty = pair.session(CLIENT).reject_publish(
                client_pub,
                &Rejection::redirect(RedirectTarget::empty()),
                now,
            );
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            if version == Version::DRAFT_22 {
                empty.expect("an empty REDIRECT of a PUBLISH");
                let moved = first(&s, EventKind::RequestRedirect).expect("the redirect");
                assert_eq!(
                    moved.request,
                    Some(RequestHandle::Publication(server_pub)),
                    "{what}"
                );
                assert_eq!(moved.target, Some(RedirectTarget::empty()), "{what}");
            } else {
                assert_eq!(
                    empty.err(),
                    Some(Error::INVAL),
                    "{what}: REDIRECT is draft 22's"
                );
                assert_eq!(count(&s, EventKind::RequestRedirect), 0, "{what}");
                assert_eq!(count(&s, EventKind::PublishError), 0, "{what}");
            }
            close(pair);
        }
    }
}

#[test]
fn a_request_goaway_migrates_one_request() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "moving");
            let request = RequestHandle::Subscription(track.server);
            let uri = b"moqt://next.example/moq";
            let now = pair.now;
            assert_eq!(
                stranger(version)
                    .request_goaway(request, uri, 500, now)
                    .err(),
                Some(Error::WRONG_SESSION),
                "{what}"
            );
            let result = pair.session(SERVER).request_goaway(request, uri, 500, now);
            if version == Version::DRAFT_16 {
                assert_eq!(result.err(), Some(Error::INVAL), "{what}");
                close(pair);
                continue;
            }
            result.expect("a request GOAWAY");
            assert_eq!(
                pair.session(SERVER)
                    .request_goaway(request, b"", 0, now)
                    .err(),
                Some(Error::WRONG_STATE),
                "{what}: one per request"
            );
            // The publisher keeps producing until the subscriber lets the
            // request go.
            let sg = group(&mut pair, &track, 0);
            write(&mut pair, sg, 0, "late");
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(sg, now)
                .expect("close subgroup");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let goaway = first(&c, EventKind::RequestGoaway).expect("the GOAWAY");
            assert_eq!(
                goaway.request,
                Some(RequestHandle::Subscription(track.client)),
                "{what}"
            );
            assert_eq!(goaway.text, uri, "{what}");
            assert_eq!(goaway.count, 500, "{what}");
            // The request is terminal on the subscriber, which closes its
            // stream; the publisher retires it without an event.
            let s = pair.drain(SERVER);
            assert_eq!(count(&s, EventKind::Unsubscribed), 0, "{what}");
            for (side, sub) in [(CLIENT, track.client), (SERVER, track.server)] {
                assert_eq!(
                    pair.hosts[side].session.subscription_filter(sub).err(),
                    Some(Error::STALE_HANDLE),
                    "{what} side {side}"
                );
            }
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn joining_fetches_start_relative_or_absolute() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let now = pair.now;
            let config = SubscribeConfig::new(live(), "join", LocationFilter::NextObject);
            let client_sub = pair
                .session(CLIENT)
                .subscribe(&config, now)
                .expect("subscribe");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_sub = first(&s, EventKind::SubscribeRequest)
                .and_then(|e| e.subscription)
                .expect("the request");
            let mut accept = AcceptSubscribe::default();
            accept.largest = Some(at(10, 5));
            let now = pair.now;
            pair.session(SERVER)
                .accept_subscribe(server_sub, &accept, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            let joined = Joining::Subscription(client_sub);
            let relative = FetchConfig::joining(joined, JoiningStart::Relative(3));
            let now = pair.now;
            let result = pair.session(CLIENT).fetch(&relative, now);
            if version == Version::DRAFT_22 {
                assert!(
                    !pair.hosts[CLIENT].session.supports_joining_fetch(),
                    "{what}"
                );
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
                close(pair);
                continue;
            }
            assert!(
                pair.hosts[CLIENT].session.supports_joining_fetch(),
                "{what}"
            );
            let fetch = result.expect("a relative joining fetch");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::FetchRequest).expect("the request");
            assert_eq!(request.start, Some(at(7, 0)), "{what}");
            assert_eq!(request.group, 10, "{what}");
            let server_fetch = request.fetch.expect("its handle");
            let mut served = AcceptFetch::new(request.group, request.object);
            served.empty = true;
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(server_fetch, &served, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::FetchOk).and_then(|e| e.fetch),
                Some(fetch),
                "{what}"
            );
            assert_eq!(
                first(&c, EventKind::FetchComplete).and_then(|e| e.fetch),
                Some(fetch),
                "{what}"
            );

            // An absolute start, refused.
            let absolute = FetchConfig::joining(joined, JoiningStart::Absolute(4));
            let now = pair.now;
            let fetch = pair
                .session(CLIENT)
                .fetch(&absolute, now)
                .expect("an absolute joining fetch");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::FetchRequest).expect("the request");
            assert_eq!(request.start, Some(at(4, 0)), "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .reject_fetch(
                    request.fetch.expect("its handle"),
                    &Rejection::new(0x10),
                    now,
                )
                .expect("reject");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let error = first(&c, EventKind::FetchError).expect("the refusal");
            assert_eq!(error.fetch, Some(fetch), "{what}");
            assert_eq!(error.code, 0x10, "{what}");
            close(pair);
        }
    }
}

#[test]
fn a_subscription_fill_delivers_its_range() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let mut fill = FillConfig::default();
            fill.subscriber_priority = Some(9);
            fill.fill_timeout_ms = Some(250);
            fill.location = Some(LocationFilter::AbsoluteStart(at(1, 0)));
            let config =
                SubscribeConfig::new(live(), "filled", LocationFilter::NextObject).fill(fill);
            let now = pair.now;
            let result = pair.session(CLIENT).subscribe(&config, now);
            if version != Version::DRAFT_22 {
                assert!(!pair.hosts[CLIENT].session.supports_fill(), "{what}");
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
                close(pair);
                continue;
            }
            let client_sub = result.expect("a subscription with a fill");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_sub = first(&s, EventKind::SubscribeRequest)
                .and_then(|e| e.subscription)
                .expect("the request");
            let mut accept = AcceptSubscribe::default();
            accept.largest = Some(at(3, 7));
            let now = pair.now;
            pair.session(SERVER)
                .accept_subscribe(server_sub, &accept, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let created = first(&c, EventKind::FillCreated).expect("the fill");
            assert_eq!(created.subscription, Some(client_sub), "{what}");
            let client_fill = created.fill.expect("its handle");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::FillRequest).expect("the fill request");
            assert_eq!(request.subscription, Some(server_sub), "{what}");
            assert_eq!(request.start, Some(at(1, 0)), "{what}");
            assert_eq!(request.end, Some(at(3, 7)), "{what}");
            let server_fill = request.fill.expect("its handle");

            // Before its stream opens a write is refused, and the payload
            // comes back whole.
            let now = pair.now;
            let refused = pair
                .session(SERVER)
                .write_fill_object(server_fill, object_write(1, 0, counted(b"one")), now)
                .expect_err("not open yet");
            assert_eq!(refused.error, Error::WRONG_STATE, "{what}");
            let one = refused.payload.expect("the payload back");
            assert_eq!(
                stranger(version).open_fill(server_fill, now).err(),
                Some(Error::WRONG_SESSION),
                "{what}"
            );
            pair.session(SERVER)
                .open_fill(server_fill, now)
                .expect("open the fill");
            pair.session(SERVER)
                .write_fill_object(server_fill, object_write(1, 0, one), now)
                .expect("a fill object");
            pair.session(SERVER)
                .write_fill_object(server_fill, object_write(1, 1, counted(b"two")), now)
                .expect("a fill object");
            pair.session(SERVER)
                .write_fill_range(server_fill, FetchRange::NonExistent, at(2, 0), now)
                .expect("a fill gap");
            retry(&mut pair, SERVER, |s, now| s.end_fill(server_fill, now));
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let delivered: Vec<_> = c
                .iter()
                .filter(|e| e.kind == Some(EventKind::FillObject))
                .map(|e| (e.fill, e.group, e.object, e.payload.clone()))
                .collect();
            assert_eq!(
                delivered,
                [
                    (Some(client_fill), 1, 0, b"one".to_vec()),
                    (Some(client_fill), 1, 1, b"two".to_vec())
                ],
                "{what}"
            );
            assert_eq!(
                first(&c, EventKind::FillGap).and_then(|e| e.end),
                Some(at(2, 0)),
                "{what}"
            );
            let ended = first(&c, EventKind::FillTerminal).expect("the fill's end");
            assert_eq!(ended.fill, Some(client_fill), "{what}");
            assert_eq!(ended.fill_end, Some(FillEnd::Complete), "{what}");
            let s = pair.drain(SERVER);
            let ended = first(&s, EventKind::FillTerminal).expect("the fill's end");
            assert_eq!(ended.fill, Some(server_fill), "{what}");
            assert_eq!(ended.fill_end, Some(FillEnd::Complete), "{what}");

            // A fill requested by an update, aborted by its publisher.
            let mut fill = FillConfig::default();
            fill.location = Some(LocationFilter::AbsoluteStart(at(2, 0)));
            let mut update = SubscriptionUpdate::default();
            update.fill = Some(fill);
            let now = pair.now;
            pair.session(CLIENT)
                .update_subscription(client_sub, &update, now)
                .expect("an update with a fill");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let client_fill = first(&c, EventKind::FillCreated)
                .and_then(|e| e.fill)
                .expect("the second fill");
            let s = pair.drain(SERVER);
            let server_fill = first(&s, EventKind::FillRequest)
                .and_then(|e| e.fill)
                .expect("its request");
            retry(&mut pair, SERVER, |s, now| {
                s.abort_fill(server_fill, 0x7, now)
            });
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let ended = first(&s, EventKind::FillTerminal).expect("the fill's end");
            assert_eq!(ended.fill, Some(server_fill), "{what}");
            assert_eq!(ended.fill_end, Some(FillEnd::LocalAborted), "{what}");
            let c = pair.drain(CLIENT);
            let ended = first(&c, EventKind::FillTerminal).expect("the fill's end");
            assert_eq!(ended.fill, Some(client_fill), "{what}");
            let now = pair.now;
            assert_eq!(
                pair.session(CLIENT).cancel_fill(client_fill, now).err(),
                Some(Error::STALE_HANDLE),
                "{what}: identifier only after its end"
            );
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn subscription_updates_are_answered_by_the_application() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                defer_updates: [false, true],
                ..base
            };
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "u");
            // Draft 22 carries a replacement filter in the update's typed
            // options; the older drafts refuse the typed form.
            let replacement = LocationFilter::AbsoluteStart(at(0, 1));
            let mut typed = SubscriptionUpdate::default();
            typed.location = Some(replacement);
            let mut update = if version == Version::DRAFT_22 {
                typed.clone()
            } else {
                SubscriptionUpdate::default()
            };
            update.subscriber_priority = Some(9);
            let now = pair.now;
            if version != Version::DRAFT_22 {
                assert_eq!(
                    pair.session(CLIENT)
                        .update_subscription(track.client, &typed, now)
                        .err(),
                    Some(Error::UNSUPPORTED),
                    "{what}"
                );
            }
            pair.session(CLIENT)
                .update_subscription(track.client, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let updated = first(&s, EventKind::SubscribeUpdated).expect("the update");
            assert_eq!(updated.subscription, Some(track.server), "{what}");
            assert_eq!(updated.owed, Some(true), "{what}");
            let owed = pair.hosts[SERVER]
                .session
                .subscription_owed_update(track.server)
                .expect("an owed update");
            assert_eq!(Some(owed.update_request_id), updated.id, "{what}");
            let resolution = pair.hosts[SERVER]
                .session
                .subscription_update_resolution(track.server, owed.update_request_id);
            if version == Version::DRAFT_22 {
                assert_eq!(updated.location, Some(replacement), "{what}");
                assert_eq!(owed.filter, Some(replacement), "{what}");
                let resolution = resolution.expect("its resolution");
                assert_eq!(
                    resolution.update_request_id, owed.update_request_id,
                    "{what}"
                );
                assert_eq!(resolution.location_replacement, Some(replacement), "{what}");
            } else {
                assert_eq!(updated.location, None, "{what}");
                assert_eq!(owed.filter, None, "{what}");
                assert_eq!(resolution.err(), Some(Error::UNSUPPORTED), "{what}");
            }
            let now = pair.now;
            pair.session(SERVER)
                .accept_subscription_update(track.server, owed.update_request_id, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::SubscriptionUpdateOk).and_then(|e| e.subscription),
                Some(track.client),
                "{what}"
            );

            if version == Version::DRAFT_22 {
                for (side, sub) in [(CLIENT, track.client), (SERVER, track.server)] {
                    assert_eq!(
                        pair.hosts[side].session.subscription_filter(sub),
                        Ok(replacement),
                        "{what} side {side}: the accepted filter is in effect"
                    );
                }
            }

            // A second update, refused: the subscription ends UPDATE_FAILED.
            let mut update = SubscriptionUpdate::default();
            update.forward = Some(false);
            let now = pair.now;
            pair.session(CLIENT)
                .update_subscription(track.client, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let id = first(&s, EventKind::SubscribeUpdated)
                .and_then(|e| e.id)
                .expect("the update's ID");
            let now = pair.now;
            assert_eq!(
                pair.session(SERVER)
                    .reject_subscription_update(
                        track.server,
                        id,
                        &Rejection::new(0x3).retry_after_ms(1),
                        now
                    )
                    .err(),
                Some(Error::INVAL),
                "{what}: no retry interval on an update refusal"
            );
            pair.session(SERVER)
                .reject_subscription_update(track.server, id, &Rejection::new(0x3), now)
                .expect("reject");
            assert_eq!(
                pair.hosts[SERVER]
                    .session
                    .subscription_owed_update(track.server)
                    .err(),
                Some(Error::STALE_HANDLE),
                "{what}"
            );
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let done = first(&c, EventKind::SubscribeDone).expect("the end");
            assert_eq!(done.subscription, Some(track.client), "{what}");
            assert_eq!(done.code, 0x8, "{what}: UPDATE_FAILED");
            close(pair);
        }
    }
}

#[test]
fn a_publication_is_updated_answered_and_cancelled() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                defer_updates: [false, true],
                ..base
            };
            let mut pair = open(version, &options);
            let (server_pub, client_pub) = publication(&mut pair, "pub");
            let now = pair.now;
            let mut legacy = SubscriptionUpdate::default();
            legacy.filter = Some(SubscribeFilter::NextGroup);
            assert_eq!(
                pair.session(CLIENT)
                    .update_publication(client_pub, &legacy, now)
                    .err(),
                Some(Error::INVAL),
                "{what}: a publication has no legacy filter"
            );
            let mut update = SubscriptionUpdate::default();
            update.subscriber_priority = Some(9);
            pair.session(CLIENT)
                .update_publication(client_pub, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let updated = first(&s, EventKind::PublishUpdated).expect("the update");
            assert_eq!(updated.publication, Some(server_pub), "{what}");
            assert_eq!(updated.owed, Some(true), "{what}");
            let owed = pair.hosts[SERVER]
                .session
                .publication_owed_update(server_pub)
                .expect("an owed update");
            assert_eq!(Some(owed.update_request_id), updated.id, "{what}");
            let resolution = pair.hosts[SERVER]
                .session
                .publication_update_resolution(server_pub, owed.update_request_id);
            if version == Version::DRAFT_22 {
                assert_eq!(
                    resolution.map(|r| r.update_request_id),
                    Ok(owed.update_request_id),
                    "{what}"
                );
            } else {
                assert_eq!(resolution.err(), Some(Error::UNSUPPORTED), "{what}");
            }
            let now = pair.now;
            pair.session(SERVER)
                .accept_publication_update(server_pub, owed.update_request_id, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::PublicationUpdateOk).and_then(|e| e.publication),
                Some(client_pub),
                "{what}"
            );

            // A second update, refused: the publication ends UPDATE_FAILED.
            let now = pair.now;
            pair.session(CLIENT)
                .update_publication(client_pub, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let id = first(&s, EventKind::PublishUpdated)
                .and_then(|e| e.id)
                .expect("the update's ID");
            let now = pair.now;
            pair.session(SERVER)
                .reject_publication_update(server_pub, id, &Rejection::new(0x3), now)
                .expect("reject");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let finished = first(&c, EventKind::PublishFinished).expect("the end");
            assert_eq!(finished.publication, Some(client_pub), "{what}");
            assert_eq!(finished.code, 0x8, "{what}: UPDATE_FAILED");

            // Another publication, cancelled by its subscriber.
            let (server_pub, client_pub) = publication(&mut pair, "other");
            let now = pair.now;
            pair.session(CLIENT)
                .cancel_publication(client_pub, 0x20, now)
                .expect("cancel");
            assert_eq!(
                pair.session(CLIENT)
                    .cancel_publication(client_pub, 0x20, now)
                    .err(),
                Some(Error::STALE_HANDLE),
                "{what}"
            );
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            assert_eq!(
                first(&s, EventKind::PublishUnsubscribed).and_then(|e| e.publication),
                Some(server_pub),
                "{what}"
            );
            close(pair);
        }
    }
}

#[test]
fn publisher_state_and_initial_parameters() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "state");
            let mut state = PublishStateConfig::default();
            state.forward = Some(true);
            let now = pair.now;
            let result = pair
                .session(SERVER)
                .notify_subscription_state(track.server, &state, now);
            if version != Version::DRAFT_22 {
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
            } else {
                result.expect("a state notification");
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let seen = first(&c, EventKind::PublishState).expect("the notification");
                assert_eq!(
                    seen.request,
                    Some(RequestHandle::Subscription(track.client)),
                    "{what}"
                );
                assert_eq!(seen.forward, Some(true), "{what}");
            }
            // A PUBLISH's initial parameters, observable while pending.
            let now = pair.now;
            let server_pub = pair
                .session(SERVER)
                .publish(&PublishConfig::new(live(), "pub"), now)
                .expect("publish");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let client_pub = first(&c, EventKind::PublishRequest)
                .and_then(|e| e.publication)
                .expect("a publish request");
            let initial = pair.hosts[CLIENT]
                .session
                .publish_initial_parameters(client_pub);
            if version != Version::DRAFT_22 {
                assert_eq!(initial.err(), Some(Error::UNSUPPORTED), "{what}");
                close(pair);
                continue;
            }
            initial.expect("the initial parameters");
            let now = pair.now;
            pair.session(CLIENT)
                .accept_publish(client_pub, &AcceptPublish::default(), now)
                .expect("accept publish");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(SERVER);
            let now = pair.now;
            pair.session(SERVER)
                .notify_publication_state(server_pub, &state, now)
                .expect("a state notification");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::PublishState).and_then(|e| e.request),
                Some(RequestHandle::Publication(client_pub)),
                "{what}"
            );
            close(pair);
        }
    }
}

#[test]
fn a_fetch_is_updated_on_draft_22() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let now = pair.now;
            let config = FetchConfig::new(live(), "f", Location::default(), 0, 3);
            let fetch = pair.session(CLIENT).fetch(&config, now).expect("a fetch");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_fetch = first(&s, EventKind::FetchRequest)
                .and_then(|e| e.fetch)
                .expect("the request");
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(server_fetch, &AcceptFetch::new(0, 3), now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            let mut update = FetchUpdate::default();
            update.subscriber_priority = Some(7);
            let now = pair.now;
            let result = pair.session(CLIENT).update_fetch(fetch, &update, now);
            if version != Version::DRAFT_22 {
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
            } else {
                result.expect("a fetch update");
                assert_eq!(
                    pair.session(CLIENT).update_fetch(fetch, &update, now).err(),
                    Some(Error::WRONG_STATE),
                    "{what}: one outstanding"
                );
                assert!(pair.run(PASSES), "{what}");
                let now = pair.now;
                pair.session(CLIENT)
                    .update_fetch(fetch, &update, now)
                    .expect("another, once answered");
                assert!(pair.run(PASSES), "{what}");
            }
            let now = pair.now;
            pair.session(SERVER)
                .write_fetch_object(server_fetch, object_write(0, 0, counted(b"f-0")), now)
                .expect("a fetch object");
            retry(&mut pair, SERVER, |s, now| s.end_fetch(server_fetch, now));
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::FetchObject).map(|e| e.payload.clone()),
                Some(b"f-0".to_vec()),
                "{what}"
            );
            assert_eq!(
                first(&c, EventKind::FetchComplete).and_then(|e| e.fetch),
                Some(fetch),
                "{what}"
            );
            close(pair);
        }
    }
}

#[test]
fn filters_and_windows_are_observed() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "w");
            for (side, sub) in [(CLIENT, track.client), (SERVER, track.server)] {
                let session = &pair.hosts[side].session;
                assert_eq!(
                    session.subscription_filter(sub),
                    Ok(LocationFilter::AbsoluteStart(Location::default())),
                    "{what} side {side}"
                );
                assert_eq!(
                    session.subscription_window(sub),
                    Ok(FilterWindow::Range {
                        start: Location::default(),
                        end: None
                    }),
                    "{what} side {side}"
                );
                assert_eq!(
                    session.subscription_group_order(sub),
                    Ok(GroupOrder::Ascending),
                    "{what} side {side}"
                );
            }
            assert_eq!(
                pair.hosts[SERVER].session.subscription_filter(track.client),
                Err(Error::WRONG_SESSION),
                "{what}"
            );
            let (server_pub, client_pub) = publication(&mut pair, "p");
            for (side, publication) in [(CLIENT, client_pub), (SERVER, server_pub)] {
                let session = &pair.hosts[side].session;
                assert_eq!(
                    session.publication_filter(publication),
                    Ok(LocationFilter::Unconstrained),
                    "{what} side {side}"
                );
                assert!(
                    session.publication_window(publication).is_ok(),
                    "{what} side {side}"
                );
                assert!(
                    session.publication_group_order(publication).is_ok(),
                    "{what} side {side}"
                );
            }
            close(pair);
        }
    }
}

#[test]
fn request_capacity_is_granted_on_draft_16() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let local = pair.hosts[CLIENT].session.local_request_capacity();
            assert_eq!(
                pair.hosts[SERVER].session.peer_request_capacity(),
                local,
                "{what}"
            );
            let now = pair.now;
            if version != Version::DRAFT_16 {
                // Drafts 18 and 22 have no request IDs to grant.
                assert_eq!(local, 0, "{what}");
                assert_eq!(
                    pair.session(CLIENT).grant_request_capacity(64, now).err(),
                    Some(Error::INVAL),
                    "{what}"
                );
                close(pair);
                continue;
            }
            assert_eq!(
                pair.session(CLIENT)
                    .grant_request_capacity(local, now)
                    .err(),
                Some(Error::INVAL),
                "{what}: not above the current capacity"
            );
            pair.session(CLIENT)
                .grant_request_capacity(local + 64, now)
                .expect("a grant");
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[CLIENT].session.local_request_capacity(),
                local + 64,
                "{what}"
            );
            assert_eq!(
                pair.hosts[SERVER].session.peer_request_capacity(),
                local + 64,
                "{what}"
            );
            close(pair);
        }
    }
}

#[test]
fn an_accepted_advertisement_is_cancelled() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let (client_ann, server_ann) = advertise(&mut pair, &live());
            let now = pair.now;
            if version == Version::DRAFT_16 {
                // A control message: the code and the reason travel.
                pair.session(SERVER)
                    .cancel_namespace(server_ann, 0x10, b"gone", now)
                    .expect("cancel");
            } else {
                // A cancelled request stream carries no reason.
                assert_eq!(
                    pair.session(SERVER)
                        .cancel_namespace(server_ann, 0x10, b"gone", now)
                        .err(),
                    Some(Error::INVAL),
                    "{what}"
                );
                pair.session(SERVER)
                    .cancel_namespace(server_ann, 0x10, b"", now)
                    .expect("cancel");
            }
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            // Draft 22 reports the request stream's end; the older drafts
            // the cancel itself.
            let kind = if version == Version::DRAFT_22 {
                EventKind::NamespaceTerminated
            } else {
                EventKind::NamespaceCancelled
            };
            let cancelled = first(&c, kind).expect("the cancel");
            assert_eq!(cancelled.announcement, Some(client_ann), "{what}");
            if version == Version::DRAFT_16 {
                assert_eq!(cancelled.code, 0x10, "{what}");
            }
            close(pair);
        }
    }
}

#[test]
fn namespace_requests_are_updated_on_draft_22() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            pair.session(SERVER)
                .set_defer_namespace_updates(true)
                .expect("defer");
            let (client_ann, server_ann) = advertise(&mut pair, &live());
            let now = pair.now;
            let mut prefixed = NamespaceUpdateConfig::default();
            prefixed.prefix = Some(live());
            let refused = pair
                .session(CLIENT)
                .update_namespace(client_ann, &prefixed, now);
            let result = pair.session(CLIENT).update_namespace(
                client_ann,
                &NamespaceUpdateConfig::default(),
                now,
            );
            if version != Version::DRAFT_22 {
                assert_eq!(refused.err(), Some(Error::UNSUPPORTED), "{what}");
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
                close(pair);
                continue;
            }
            assert_eq!(
                refused.err(),
                Some(Error::INVAL),
                "{what}: an advertisement has no prefix to update"
            );
            let id = result.expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::NamespaceUpdateRequest).expect("the update");
            assert_eq!(request.announcement, Some(server_ann), "{what}");
            assert_eq!(request.id, Some(id), "{what}");
            let owed = pair.hosts[SERVER]
                .session
                .namespace_owed_update(server_ann)
                .map(|u| u.update_request_id);
            assert_eq!(owed, Ok(id), "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .reject_namespace_update(server_ann, id, &Rejection::new(0x3), now)
                .expect("reject");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let result = first(&c, EventKind::NamespaceUpdateResult).expect("the answer");
            assert_eq!(result.announcement, Some(client_ann), "{what}");
            assert_eq!(result.id, Some(id), "{what}");
            assert_eq!(result.outcome, Some(UpdateOutcome::Error), "{what}");
            assert_eq!(result.code, 0x3, "{what}");

            // A namespace subscription narrows its prefix.
            let now = pair.now;
            let ns_sub = pair
                .session(CLIENT)
                .subscribe_namespace(&live(), NamespaceInterest::NamespaceState, now)
                .expect("a namespace subscription");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_sub = first(&s, EventKind::NamespaceSubscriptionRequest)
                .and_then(|e| e.namespace_subscription)
                .expect("the request");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace_subscription(server_sub, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            let narrower = Namespace::parse("live/a").expect("a prefix");
            let mut update = NamespaceUpdateConfig::default();
            update.prefix = Some(narrower.clone());
            let now = pair.now;
            let id = pair
                .session(CLIENT)
                .update_namespace_subscription(ns_sub, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request =
                first(&s, EventKind::NamespaceSubscriptionUpdateRequest).expect("the update");
            assert_eq!(request.namespace_subscription, Some(server_sub), "{what}");
            assert_eq!(request.id, Some(id), "{what}");
            assert_eq!(request.text, b"livea", "{what}");
            let owed = pair.hosts[SERVER]
                .session
                .namespace_subscription_owed_update(server_sub)
                .map(|u| u.prefix.map(|p| p.to_namespace()));
            assert_eq!(owed, Ok(Some(Ok(narrower))), "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace_subscription_update(server_sub, id, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let result =
                first(&c, EventKind::NamespaceSubscriptionUpdateResult).expect("the answer");
            assert_eq!(result.id, Some(id), "{what}");
            assert_eq!(result.outcome, Some(UpdateOutcome::Ok), "{what}");

            // SUBSCRIBE_TRACKS stops forwarding.
            let prefix = Namespace::parse("tracks").expect("a prefix");
            let now = pair.now;
            let tracks = pair
                .session(CLIENT)
                .subscribe_tracks(&SubscribeTracksConfig::new(prefix), now)
                .expect("a SUBSCRIBE_TRACKS");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_req = first(&s, EventKind::SubscribeTracksRequest)
                .and_then(|e| e.track_subscription)
                .expect("the request");
            let now = pair.now;
            pair.session(SERVER)
                .accept_subscribe_tracks(server_req, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            let mut update = NamespaceUpdateConfig::default();
            update.forward = Some(false);
            let now = pair.now;
            let id = pair
                .session(CLIENT)
                .update_subscribe_tracks(tracks, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::SubscribeTracksUpdateRequest).expect("the update");
            assert_eq!(request.forward, Some(false), "{what}");
            let owed = pair.hosts[SERVER]
                .session
                .subscribe_tracks_owed_update(server_req)
                .map(|u| (u.update_request_id, u.forward));
            assert_eq!(owed, Ok((id, Some(false))), "{what}");
            let now = pair.now;
            pair.session(SERVER)
                .accept_subscribe_tracks_update(server_req, id, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let result = first(&c, EventKind::SubscribeTracksUpdateResult).expect("the answer");
            assert_eq!(result.track_subscription, Some(tracks), "{what}");
            assert_eq!(result.outcome, Some(UpdateOutcome::Ok), "{what}");
            close(pair);
        }
    }
}

#[test]
fn cluster_metadata_travels_on_draft_22() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                cluster: [Some((0, Some(0))), Some((u64::MAX, None))],
                ..base
            };
            if version != Version::DRAFT_22 {
                assert_eq!(
                    Pair::new(version, &options).err(),
                    Some(Error::UNSUPPORTED),
                    "{what}"
                );
                continue;
            }
            let mut pair = open(version, &options);
            let info = pair.hosts[CLIENT]
                .session
                .cluster_info()
                .expect("cluster info");
            assert!(info.enabled && info.negotiated, "{what}");
            assert_eq!(info.peer_hop_id, u64::MAX, "{what}");
            assert_eq!(
                (info.peer_relay_cost, info.peer_relay_cost_stated),
                (1, false),
                "{what}"
            );
            let info = pair.hosts[SERVER]
                .session
                .cluster_info()
                .expect("cluster info");
            assert_eq!(
                (
                    info.peer_hop_id,
                    info.peer_relay_cost,
                    info.peer_relay_cost_stated
                ),
                (0, 0, true),
                "{what}"
            );

            // An advertisement with its path.
            let path = ClusterPath {
                hop_ids: vec![0, 0, u64::MAX],
                route_cost: Some(u64::MAX),
            };
            let mut config =
                PublishNamespaceConfig::new(Namespace::parse("cluster").expect("a namespace"));
            config.cluster = Some(path.clone());
            let now = pair.now;
            let client_ann = pair
                .session(CLIENT)
                .publish_namespace_with(&config, now)
                .expect("an advertisement");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let published = first(&s, EventKind::NamespacePublished).expect("the advertisement");
            assert_eq!(published.cluster.as_ref(), Some(&path), "{what}");
            let server_ann = published.announcement.expect("its handle");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace(server_ann, now)
                .expect("accept");
            pair.session(SERVER)
                .set_defer_namespace_updates(true)
                .expect("defer");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);

            // A partial update: the cost only.
            let mut update = NamespaceUpdateConfig::default();
            update.cluster = Some(ClusterPath {
                hop_ids: Vec::new(),
                route_cost: Some(0),
            });
            let now = pair.now;
            let id = pair
                .session(CLIENT)
                .update_namespace(client_ann, &update, now)
                .expect("an update");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            assert_eq!(
                first(&s, EventKind::NamespaceUpdateRequest).and_then(|e| e.id),
                Some(id),
                "{what}"
            );
            let owed = pair.hosts[SERVER]
                .session
                .namespace_owed_update(server_ann)
                .map(|u| u.cluster.map(|c| c.to_path()));
            assert_eq!(
                owed,
                Ok(Some(ClusterPath {
                    hop_ids: Vec::new(),
                    route_cost: Some(0)
                })),
                "{what}"
            );
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace_update(server_ann, id, now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::NamespaceUpdateResult).and_then(|e| e.outcome),
                Some(UpdateOutcome::Ok),
                "{what}"
            );
            let current = pair.hosts[SERVER]
                .session
                .namespace_cluster(server_ann)
                .map(|c| c.to_path());
            assert_eq!(
                current,
                Ok(ClusterPath {
                    hop_ids: vec![0, 0, u64::MAX],
                    route_cost: Some(0)
                }),
                "{what}"
            );

            // Discovery with a path.
            let now = pair.now;
            let ns_sub = pair
                .session(CLIENT)
                .subscribe_namespace(&live(), NamespaceInterest::NamespaceState, now)
                .expect("a namespace subscription");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let server_sub = first(&s, EventKind::NamespaceSubscriptionRequest)
                .and_then(|e| e.namespace_subscription)
                .expect("the request");
            let found_path = ClusterPath {
                hop_ids: vec![0, 2],
                route_cost: Some(9),
            };
            let mesh = Namespace::parse("mesh").expect("a suffix");
            let now = pair.now;
            pair.session(SERVER)
                .accept_namespace_subscription(server_sub, now)
                .expect("accept");
            pair.session(SERVER)
                .send_namespace_cluster(server_sub, &mesh, &found_path, now)
                .expect("a NAMESPACE with its path");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let found = first(&c, EventKind::NamespaceFound).expect("the namespace");
            assert_eq!(found.namespace_subscription, Some(ns_sub), "{what}");
            assert_eq!(found.text, b"mesh", "{what}");
            assert_eq!(found.cluster.as_ref(), Some(&found_path), "{what}");
            close(pair);
        }
    }
}

#[test]
fn a_publication_carries_built_properties_and_datagrams() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let server = &pair.hosts[SERVER].session;
            let ordered = server
                .track_properties_with_group_order(&[], GroupOrder::Descending)
                .expect("a group order");
            assert!(!ordered.is_empty(), "{what}");
            let properties = server
                .track_properties_with_delivery_timeouts(&ordered, Some(500), Some(500))
                .expect("delivery timeouts");
            assert!(properties.len() > ordered.len(), "{what}");
            assert_eq!(
                server
                    .track_properties_with_delivery_timeouts(&properties, None, None)
                    .err(),
                Some(Error::INVAL),
                "{what}: nothing selected"
            );
            assert_eq!(
                server
                    .track_properties_with_group_order(&properties, GroupOrder::Ascending)
                    .err(),
                Some(Error::INVAL),
                "{what}: a conflicting order"
            );
            let config = PublishConfig::new(live(), "dg").track_properties(properties);
            let now = pair.now;
            let server_pub = pair.session(SERVER).publish(&config, now).expect("publish");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let client_pub = first(&c, EventKind::PublishRequest)
                .and_then(|e| e.publication)
                .expect("a publish request");
            let now = pair.now;
            pair.session(CLIENT)
                .accept_publish(client_pub, &AcceptPublish::default(), now)
                .expect("accept publish");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(SERVER);
            assert_eq!(
                pair.hosts[CLIENT]
                    .session
                    .publication_group_order(client_pub),
                Ok(GroupOrder::Descending),
                "{what}: the publisher's advertised order"
            );
            let now = pair.now;
            pair.session(SERVER)
                .send_publication_datagram(
                    server_pub,
                    at(1, 0),
                    3,
                    false,
                    counted(b"pub dg"),
                    &[],
                    now,
                )
                .expect("a datagram");
            pair.session(SERVER)
                .send_publication_status_datagram(
                    server_pub,
                    at(1, 1),
                    3,
                    ObjectStatus::EndOfGroup,
                    now,
                )
                .expect("a status datagram");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let object = first(&c, EventKind::ObjectReceived).expect("the datagram");
            assert!(object.flag, "{what}: delivered as a datagram");
            assert_eq!(object.publication, Some(client_pub), "{what}");
            assert_eq!((object.group, object.object), (1, 0), "{what}");
            assert_eq!(object.payload, b"pub dg", "{what}");
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

// -- A FETCH's request after a STOP of its response stream ---------------------
//
// Drafts 18 and 22 tag a FETCH response stream RESET_FINAL_ON_STOP: a STOP of
// it that finds it open ends the FETCH, and the publisher resets both of its
// streams. Draft 16 has no request stream and no such tag.

const STREAM_CANCELLED: u64 = moq5_core::sys::MOQ_STREAM_ERROR_CANCELLED as u64;
const STREAM_PROFILES: [Version; 2] = [Version::DRAFT_18, Version::DRAFT_22];

/// The server in the tagged mode, the client not.
fn fence_server() -> Options {
    Options {
        tagged: [false, true],
        ..Options::default()
    }
}

/// The client FETCHes `name`; the server sees it: (client, server).
fn request_fetch(pair: &mut Pair, name: &str) -> (Fetch, Fetch) {
    let now = pair.now;
    let config = FetchConfig::new(live(), name, Location::default(), 0, 9);
    let client = pair.session(CLIENT).fetch(&config, now).expect("a fetch");
    assert!(pair.run(PASSES));
    let s = pair.drain(SERVER);
    let server = first(&s, EventKind::FetchRequest)
        .and_then(|e| e.fetch)
        .expect("the fetch request");
    (client, server)
}

fn write_fetch(pair: &mut Pair, fetch: Fetch, object: u64, text: &str) {
    let now = pair.now;
    pair.session(SERVER)
        .write_fetch_object(
            fetch,
            object_write(0, object, counted(text.as_bytes())),
            now,
        )
        .expect("a fetch object");
}

/// A FETCH of `name` the server accepted, one object delivered.
fn serve_fetch(pair: &mut Pair, name: &str) -> (Fetch, Fetch) {
    let (client, server) = request_fetch(pair, name);
    let now = pair.now;
    pair.session(SERVER)
        .accept_fetch(server, &AcceptFetch::new(0, 9), now)
        .expect("accept");
    write_fetch(pair, server, 0, "f-zero");
    assert!(pair.run(PASSES));
    pair.drain(CLIENT);
    (client, server)
}

/// The server's response stream and request stream of its one FETCH.
fn fetch_streams(pair: &Pair) -> (StreamRef, StreamRef) {
    let data = pair.hosts[SERVER].local_data_streams();
    let requests = pair.hosts[SERVER].served_requests();
    assert_eq!((data.len(), requests.len()), (1, 1));
    (data[0], requests[0])
}

/// What the server's transport took on one of its streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Trace {
    writes: u32,
    taken: u64,
    fin: Option<u64>,
    reset: Option<u64>,
    reset_at: Option<u64>,
    reset_reliable: Option<u64>,
}

fn trace(pair: &Pair, stream: StreamRef) -> Trace {
    let pipe = pair.trace(SERVER, stream).expect("a stream of the server");
    Trace {
        writes: pipe.writes,
        taken: pipe.taken,
        fin: pipe.fin_at,
        reset: pipe.reset,
        reset_at: pipe.reset_at,
        reset_reliable: pipe.reset_reliable,
    }
}

fn server_settled(pair: &Pair, what: &str) {
    assert_eq!(pair.hosts[SERVER].held(), 0, "{what}: nothing held");
    assert!(
        pair.hosts.iter().all(|h| h.fault().is_none()),
        "{what}: no fault"
    );
}

#[test]
fn a_stop_of_an_ended_fetch_resets_its_request() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        assert_eq!(pair.hosts[SERVER].counters().final_policies, 1, "{what}");
        assert!(pair.refuse_data_stream(SERVER, Some(data)));
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d.fin, d.reset), (None, None), "{what}");
        assert_eq!((r.fin, r.reset), (None, None), "{what}: the fence holds it");
        assert_eq!(pair.hosts[SERVER].held(), 2, "{what}: both FINs");
        let host_resets = pair.hosts[SERVER].counters().host_resets;

        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d.reset, d.fin), (Some(0x2A), None), "{what}");
        assert_eq!(
            (r.reset, r.fin),
            (Some(STREAM_CANCELLED), None),
            "{what}: never the request FIN"
        );
        let n = pair.hosts[SERVER].counters();
        assert_eq!(n.finals_cancelled, 1, "{what}");
        assert_eq!(n.host_resets, host_resets + 1, "{what}");
        server_settled(&pair, &what);
        let s = pair.drain(SERVER);
        assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");

        pair.lift_data_refusal(SERVER);
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(trace(&pair, data), d, "{what}");
        assert_eq!(
            (trace(&pair, request).fin, trace(&pair, request).reset_at),
            (None, r.reset_at),
            "{what}"
        );
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_stop_of_a_blocked_fetch_ends_it_in_the_session() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let writes = trace(&pair, data).writes;
        assert!(pair.refuse_data_stream(SERVER, Some(data)));
        write_fetch(&mut pair, fetch, 1, "f-one");
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(trace(&pair, data).writes, writes, "{what}");
        assert_eq!(pair.hosts[SERVER].held(), 1, "{what}: the object");

        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(
            (d.reset, d.fin, d.writes),
            (Some(0x2A), None, writes),
            "{what}"
        );
        assert_eq!((r.reset, r.fin), (Some(STREAM_CANCELLED), None), "{what}");
        // The session's teardown resets the request itself (REQUEST_ENDED).
        assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 0, "{what}");
        server_settled(&pair, &what);
        let s = pair.drain(SERVER);
        let cancelled = first(&s, EventKind::FetchCancelled).expect("the end");
        assert_eq!(cancelled.fetch, Some(fetch), "{what}");
        assert_eq!(cancelled.code, 0x2A, "{what}");
        assert_eq!(count(&s, EventKind::FetchCancelled), 1, "{what}");

        pair.lift_data_refusal(SERVER);
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(trace(&pair, data), d, "{what}");
        assert_eq!(trace(&pair, request).reset_at, r.reset_at, "{what}");
        assert_eq!(trace(&pair, request).fin, None, "{what}");
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_stop_before_any_byte_of_an_ended_fetch() {
    for version in STREAM_PROFILES {
        for empty in [false, true] {
            let what = format!("draft {} empty {empty}", version.draft());
            let mut pair = open(version, &fence_server());
            let (_, fetch) = request_fetch(&mut pair, "past");
            assert!(pair.refuse_data_stream(SERVER, None));
            let mut accept = AcceptFetch::new(0, if empty { 0 } else { 9 });
            accept.empty = empty;
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(fetch, &accept, now)
                .expect("accept");
            if !empty {
                write_fetch(&mut pair, fetch, 0, "f-zero");
                let now = pair.now;
                pair.session(SERVER).end_fetch(fetch, now).expect("end");
            }
            assert!(pair.run(PASSES), "{what}");
            let (data, request) = fetch_streams(&pair);
            let (d, r) = (trace(&pair, data), trace(&pair, request));
            assert_eq!((d.writes, d.fin, d.reset), (0, None, None), "{what}");
            assert!(r.writes >= 1, "{what}: FETCH_OK alone");
            assert_eq!((r.fin, r.reset), (None, None), "{what}");
            assert_eq!(pair.hosts[SERVER].counters().final_policies, 1, "{what}");

            assert!(pair.peer_stops_data(SERVER, data, 0x2A));
            assert!(pair.run(PASSES), "{what}");
            let (d, r) = (trace(&pair, data), trace(&pair, request));
            assert_eq!((d.reset, d.writes, d.fin), (Some(0x2A), 0, None), "{what}");
            assert_eq!((r.reset, r.fin), (Some(STREAM_CANCELLED), None), "{what}");
            assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 1, "{what}");
            server_settled(&pair, &what);
            let s = pair.drain(SERVER);
            assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");

            pair.lift_data_refusal(SERVER);
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(trace(&pair, data), d, "{what}");
            assert_eq!(trace(&pair, request).reset_at, r.reset_at, "{what}");
            assert_eq!(trace(&pair, request).fin, None, "{what}");
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_stop_before_the_final_message_is_polled() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let writes = trace(&pair, data).writes;
        let host_resets = pair.hosts[SERVER].counters().host_resets;
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(
            (d.reset, d.fin, d.writes),
            (Some(0x2A), None, writes),
            "{what}"
        );
        assert_eq!((r.reset, r.fin), (Some(STREAM_CANCELLED), None), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!(n.finals_cancelled, 1, "{what}");
        assert_eq!(n.host_resets, host_resets + 1, "{what}");
        server_settled(&pair, &what);
        let s = pair.drain(SERVER);
        assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_held_claim_keeps_the_first_cause() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);
        let (claims, commits) = {
            let n = pair.hosts[SERVER].counters();
            (n.claims, n.commits)
        };
        // The client reads nothing until the server's resets are out.
        pair.wire.hold_input[CLIENT] = true;
        pair.wire.refuse_resets[SERVER] = 1000;
        let now = pair.now;
        pair.session(SERVER)
            .abort_fetch(fetch, 0x11, now)
            .expect("abort");
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(pair.hosts[SERVER].counters().claims, claims + 1, "{what}");
        assert_eq!(trace(&pair, data).reset, None, "{what}");
        assert_eq!(trace(&pair, request).reset, None, "{what}");

        // The STOP meets the held claim, which keeps the abort's code; the
        // request's own reset stands.
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d.reset, r.reset, r.fin), (None, None, None), "{what}");
        assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 0, "{what}");

        pair.wire.refuse_resets[SERVER] = 0;
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d.reset, d.fin), (Some(0x11), None), "{what}");
        assert_eq!((r.reset, r.fin), (Some(0x11), None), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!(n.commits, commits + 1, "{what}");
        assert_eq!(n.finals_cancelled, 0, "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        assert_eq!(pair.wire.reset_repeats[SERVER], repeats, "{what}");
        pair.wire.hold_input[CLIENT] = false;
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        let s = pair.drain(SERVER);
        assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");

        // A later FETCH completes ordinarily: its data FIN, then its
        // request FIN, nothing reset.
        let (next_client, next) = serve_fetch(&mut pair, "more");
        let now = pair.now;
        pair.session(SERVER).end_fetch(next, now).expect("end");
        assert!(pair.run(PASSES), "{what}");
        let data_streams = pair.hosts[SERVER].local_data_streams();
        let requests = pair.hosts[SERVER].served_requests();
        assert_eq!((data_streams.len(), requests.len()), (2, 2), "{what}");
        assert_eq!((data_streams[0], requests[0]), (data, request), "{what}");
        let (nd, nr) = (trace(&pair, data_streams[1]), trace(&pair, requests[1]));
        assert!(
            nd.fin.is_some() && nr.fin.is_some() && nd.fin < nr.fin,
            "{what}"
        );
        assert_eq!((nd.reset, nr.reset), (None, None), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!((n.final_policies, n.finals_cancelled), (2, 0), "{what}");
        let c = pair.drain(CLIENT);
        assert_eq!(
            first(&c, EventKind::FetchComplete).and_then(|e| e.fetch),
            Some(next_client),
            "{what}"
        );
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_queued_native_reset_of_the_request_stands() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);
        let claims = pair.hosts[SERVER].counters().claims;
        pair.wire.hold_input[CLIENT] = true;
        let now = pair.now;
        pair.session(SERVER)
            .abort_fetch(fetch, 0x11, now)
            .expect("abort");
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        // The STOP the session took lets the abort's reset be claimed
        // WAIVED: it keeps 0x11 and delivers what the transport took.
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d.reset, d.fin), (Some(0x11), None), "{what}");
        assert_eq!(d.reset_reliable, Some(d.taken), "{what}");
        assert_eq!((r.reset, r.fin), (Some(0x11), None), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!(n.claims, claims + 1, "{what}: claimed once");
        assert_eq!(n.finals_cancelled, 0, "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        assert_eq!(pair.wire.reset_repeats[SERVER], repeats, "{what}");
        server_settled(&pair, &what);
        pair.wire.hold_input[CLIENT] = false;
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

/// FETCH terminal events in a log: one FETCH ends once.
fn fetch_terminals(seen: &[Seen]) -> usize {
    [
        EventKind::FetchError,
        EventKind::FetchCancelled,
        EventKind::FetchComplete,
        EventKind::FetchReset,
    ]
    .into_iter()
    .map(|kind| count(seen, kind))
    .sum()
}

#[test]
fn an_earlier_reset_then_a_stop_waives_what_was_not_taken() {
    for version in STREAM_PROFILES {
        for partial in [false, true] {
            let what = format!("draft {} partial {partial}", version.draft());
            let mut pair = open(version, &fence_server());
            let (_, fetch) = request_fetch(&mut pair, "past");
            // The transport refuses the response stream from its open, or
            // once it took one byte of the header.
            let taken = u64::from(partial);
            assert!(pair.refuse_data_stream(SERVER, None));
            pair.wire.refusal_allowance[SERVER] = taken;
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(fetch, &AcceptFetch::new(0, 9), now)
                .expect("accept");
            write_fetch(&mut pair, fetch, 0, "f-zero");
            assert!(pair.run(PASSES), "{what}");
            let (data, request) = fetch_streams(&pair);
            let d = trace(&pair, data);
            assert_eq!((d.taken, d.reset), (taken, None), "{what}");
            let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);
            let (claims, commits) = {
                let n = pair.hosts[SERVER].counters();
                (n.claims, n.commits)
            };

            // An earlier native reset, then the STOP, before this side polls.
            let now = pair.now;
            pair.session(SERVER)
                .abort_fetch(fetch, 0x11, now)
                .expect("abort");
            assert!(pair.peer_stops_data(SERVER, data, 0x2A));
            assert!(pair.run(PASSES), "{what}");
            let (d, r) = (trace(&pair, data), trace(&pair, request));
            let n = pair.hosts[SERVER].counters();
            assert_eq!((n.claims, n.commits), (claims + 1, commits + 1), "{what}");
            assert_eq!(
                (d.reset, d.fin),
                (Some(0x11), None),
                "{what}: the abort's code"
            );
            assert_eq!(
                (d.reset_reliable, d.taken),
                (Some(taken), taken),
                "{what}: exactly what the transport took"
            );
            assert_eq!((r.reset, r.fin), (Some(0x11), None), "{what}");
            assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}: one each");
            assert_eq!(pair.wire.reset_repeats[SERVER], repeats, "{what}");
            assert_eq!(n.finals_cancelled, 0, "{what}: nothing replaced");
            assert_eq!(pair.hosts[SERVER].held(), 0, "{what}");
            assert_eq!(fetch_terminals(&pair.drain(SERVER)), 0, "{what}");

            // Nothing held is written after the STOP, the refusal lifted.
            pair.lift_data_refusal(SERVER);
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(trace(&pair, data), d, "{what}");
            assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
            assert_eq!(fetch_terminals(&pair.drain(CLIENT)), 1, "{what}");
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

/// A REQUEST_UPDATE of the client's FETCH that the server refuses: on draft
/// 22 the client's update carrying one token twice; on draft 18 a
/// REQUEST_UPDATE naming a token alias never registered, written on the
/// served request stream.
fn refused_fetch_update(pair: &mut Pair, version: Version, fetch: Fetch, request: StreamRef) {
    if version == Version::DRAFT_22 {
        let token = AuthToken {
            token_type: 1,
            value: vec![0x74],
        };
        let mut update = FetchUpdate::default();
        update.tokens = vec![token.clone(), token];
        let now = pair.now;
        pair.session(CLIENT)
            .update_fetch(fetch, &update, now)
            .expect("an update");
        return;
    }
    // REQUEST_UPDATE (0x02), 6 bytes: Request ID 10, one parameter,
    // AUTHORIZATION_TOKEN (0x03) of 2 bytes: USE_ALIAS (2), alias 99. Every
    // integer here fits one byte of draft 18's variable-length encoding.
    const UPDATE: [u8; 9] = [0x02, 0x00, 0x06, 0x0a, 0x01, 0x03, 0x02, 0x02, 0x63];
    assert!(pair.peer_writes_request(SERVER, request, &UPDATE));
}

#[test]
fn a_held_claim_replaces_a_waiting_final() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (client_fetch, _) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let request_writes = trace(&pair, request).writes;
        let (waits, claims, commits) = {
            let n = pair.hosts[SERVER].counters();
            (n.final_waits, n.claims, n.commits)
        };
        // A refused REQUEST_UPDATE ends the FETCH. Polling one action a
        // pass, this side takes the final message (REQUEST_ERROR with FIN),
        // which its fence holds, before the response stream's reset is
        // offered; it claims that reset and the transport refuses it.
        pair.wire.refuse_resets[SERVER] = 1000;
        pair.set_poll_budget(SERVER, 1);
        refused_fetch_update(&mut pair, version, client_fetch, request);
        assert!(pair.run(PASSES), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!(n.claims, claims + 1, "{what}: the claim is held");
        assert!(n.final_waits > waits, "{what}: and the final message waits");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(
            (d.reset, r.reset, r.fin, r.writes),
            (None, None, None, request_writes),
            "{what}"
        );
        assert_eq!(n.finals_cancelled, 0, "{what}");

        // The STOP meets the held claim with no session call: the claim
        // keeps its code, and the final message becomes the request
        // stream's CANCELLED reset, never written.
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(d.reset, None, "{what}");
        assert_eq!(
            (r.reset, r.fin, r.writes),
            (Some(STREAM_CANCELLED), None, request_writes),
            "{what}: the final message replaced"
        );
        assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 1, "{what}");

        pair.wire.refuse_resets[SERVER] = 0;
        assert!(pair.run(PASSES), "{what}");
        let (d2, r2) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d2.reset, d2.fin), (Some(STREAM_CANCELLED), None), "{what}");
        assert_eq!(d2.reset_reliable, Some(d2.taken), "{what}: what was taken");
        assert_eq!(
            (r2.reset_at, r2.fin, r2.writes),
            (r.reset_at, None, request_writes),
            "{what}: once"
        );
        let n = pair.hosts[SERVER].counters();
        assert_eq!((n.commits, n.finals_cancelled), (commits + 1, 1), "{what}");
        pair.set_poll_budget(SERVER, 0);
        server_settled(&pair, &what);
        pair.drain(SERVER);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_redelivered_stop_adds_no_obligation() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        assert!(pair.refuse_data_stream(SERVER, Some(data)));
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        assert!(pair.run(PASSES), "{what}");
        let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);
        let (entries, repeated) = {
            let n = pair.hosts[SERVER].counters();
            (n.stop_entries, n.stop_repeats)
        };
        // The transport refuses this side's reset of the response stream:
        // the first STOP's obligation stays owed.
        pair.wire.refuse_resets[SERVER] = 1000;
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(d.reset, None, "{what}: this side's reset is owed");
        assert_eq!(r.reset, Some(STREAM_CANCELLED), "{what}");
        assert_eq!(
            pair.hosts[SERVER].counters().stop_entries,
            entries + 1,
            "{what}"
        );

        // The same STOP reaches the host twice more while that reset is
        // owed, then the peer stops the request stream, and that STOP too
        // reaches the host twice more.
        assert!(pair.deliver_peer_stop(SERVER, data, 0x2B));
        assert!(pair.run(PASSES), "{what}");
        assert!(pair.deliver_peer_stop(SERVER, data, 0x2C));
        assert!(pair.peer_stops_request(SERVER, request, 0x6));
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(
            pair.hosts[SERVER].counters().stop_entries,
            entries + 4,
            "{what}"
        );
        assert!(pair.deliver_peer_stop(SERVER, request, 0x5));
        assert!(pair.run(PASSES), "{what}");
        assert!(pair.deliver_peer_stop(SERVER, request, 0x7));
        assert!(pair.run(PASSES), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!(
            (n.stop_entries, n.stop_repeats),
            (entries + 6, repeated + 4),
            "{what}: each reached it, none added an obligation"
        );
        assert_eq!(trace(&pair, data).reset, None, "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 1, "{what}");

        // Once resets are taken, each stream is reset once.
        pair.wire.refuse_resets[SERVER] = 0;
        assert!(pair.run(PASSES), "{what}");
        let (d2, r2) = (trace(&pair, data), trace(&pair, request));
        assert_eq!((d2.reset, d2.fin), (Some(0x2A), None), "{what}");
        assert_eq!(r2, r, "{what}");
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        assert_eq!(pair.wire.reset_repeats[SERVER], repeats, "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!((n.finals_cancelled, n.stops_answered), (1, 0), "{what}");
        assert_eq!(
            count(&pair.drain(SERVER), EventKind::FetchCancelled),
            0,
            "{what}"
        );
        pair.lift_data_refusal(SERVER);
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_second_stop_changes_nothing() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        assert!(pair.refuse_data_stream(SERVER, Some(data)));
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        assert!(pair.run(PASSES), "{what}");
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert!(d.reset.is_some() && r.reset.is_some(), "{what}");
        let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);

        // The transport reports one STOP per stream: a second one of the
        // response stream is absorbed there; the request stream's reaches
        // the session.
        assert!(pair.peer_stops_data(SERVER, data, 0x2B));
        assert!(pair.peer_stops_request(SERVER, request, 0x5));
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(trace(&pair, data), d, "{what}");
        let r2 = trace(&pair, request);
        assert_eq!(
            (r2.reset, r2.reset_at, r2.fin),
            (Some(STREAM_CANCELLED), r.reset_at, None),
            "{what}"
        );
        assert_eq!(pair.wire.resets[SERVER], resets, "{what}");
        assert_eq!(pair.wire.reset_repeats[SERVER], repeats, "{what}");
        assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 1, "{what}");
        let s = pair.drain(SERVER);
        assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");
        pair.lift_data_refusal(SERVER);
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_draft_16_fetch_has_no_stop_policy() {
    let what = "draft 16";
    let mut pair = open(Version::DRAFT_16, &fence_server());
    let (_, fetch) = serve_fetch(&mut pair, "past");
    let data = pair.hosts[SERVER].local_data_streams();
    assert_eq!(data.len(), 1, "{what}");
    let data = data[0];
    assert!(pair.hosts[SERVER].served_requests().is_empty(), "{what}");
    assert_eq!(pair.hosts[SERVER].counters().final_policies, 0, "{what}");
    assert!(pair.refuse_data_stream(SERVER, Some(data)));
    let now = pair.now;
    pair.session(SERVER).end_fetch(fetch, now).expect("end");
    assert!(pair.run(PASSES), "{what}");
    let resets = pair.wire.resets[SERVER];
    assert!(pair.peer_stops_data(SERVER, data, 0x2A));
    assert!(pair.run(PASSES), "{what}");
    let d = trace(&pair, data);
    assert_eq!((d.reset, d.fin), (Some(0x2A), None), "{what}");
    assert_eq!(
        pair.wire.resets[SERVER],
        resets + 1,
        "{what}: that stream alone"
    );
    let n = pair.hosts[SERVER].counters();
    assert_eq!((n.final_policies, n.finals_cancelled), (0, 0), "{what}");
    assert_eq!(
        pair.hosts[SERVER].session.state(),
        SessionState::Established,
        "{what}"
    );
    let s = pair.drain(SERVER);
    assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");
    pair.lift_data_refusal(SERVER);
    server_settled(&pair, what);
    close(pair);
    assert_eq!(outstanding(), 0);
}

#[test]
fn an_ended_fetch_completes_with_its_policy() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (client_fetch, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert!(
            d.fin.is_some() && r.fin.is_some() && d.fin < r.fin,
            "{what}"
        );
        assert_eq!((d.reset, r.reset), (None, None), "{what}");
        let c = pair.drain(CLIENT);
        assert_eq!(
            first(&c, EventKind::FetchComplete).and_then(|e| e.fetch),
            Some(client_fetch),
            "{what}"
        );
        // A STOP after the response stream's FIN was accepted has no effect
        // on the request.
        assert!(pair.peer_stops_data(SERVER, data, 0x2A));
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(trace(&pair, data), d, "{what}");
        assert_eq!(trace(&pair, request), r, "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!((n.final_policies, n.finals_cancelled), (1, 0), "{what}");
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

// -- Buffer custody of refused writes, and object properties -------------------

/// The native count of references to `buffer`.
fn refcount(buffer: &Buffer) -> u32 {
    // SAFETY: a live rcbuf this test holds.
    unsafe { moq5_core::sys::moq_rcbuf_refcount(buffer.as_ptr()) }
}

/// A refused write's payload: the very buffer passed, intact, and the
/// caller's alone.
fn returned(
    payload: Option<Buffer>,
    passed: *mut moq5_core::sys::moq_rcbuf_t,
    bytes: &[u8],
    what: &str,
) -> Buffer {
    let buffer = payload.expect("the payload back");
    assert_eq!(buffer.as_ptr(), passed, "{what}: the very buffer");
    assert_eq!(buffer.as_slice(), bytes, "{what}: intact");
    assert_eq!(refcount(&buffer), 1, "{what}: the caller's alone");
    buffer
}

#[test]
fn refused_writes_hand_back_the_very_buffer() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);

            // A FETCH object before the FETCH is accepted.
            let (_, fetch) = request_fetch(&mut pair, "past");
            let buffer = counted(b"f-zero");
            let passed = buffer.as_ptr();
            let now = pair.now;
            let refused = pair
                .session(SERVER)
                .write_fetch_object(fetch, object_write(0, 0, buffer), now)
                .expect_err("the fetch is not accepted");
            assert_eq!(refused.error, Error::WRONG_STATE, "{what}");
            let buffer = returned(refused.payload, passed, b"f-zero", &what);
            pair.session(SERVER)
                .accept_fetch(fetch, &AcceptFetch::new(0, 9), now)
                .expect("accept");
            pair.session(SERVER)
                .write_fetch_object(fetch, object_write(0, 0, buffer), now)
                .expect("the same buffer, written");
            retry(&mut pair, SERVER, |s, now| s.end_fetch(fetch, now));
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::FetchObject).map(|e| e.payload.clone()),
                Some(b"f-zero".to_vec()),
                "{what}"
            );

            // A subgroup object whose ID is not above the last.
            let track = subscribe(&mut pair, "w");
            let sg = group(&mut pair, &track, 0);
            write(&mut pair, sg, 0, "zero");
            let buffer = counted(b"one");
            let passed = buffer.as_ptr();
            let now = pair.now;
            let refused = pair
                .session(SERVER)
                .write_object(sg, 0, buffer, now)
                .expect_err("not above the last");
            assert_eq!(refused.error, Error::INVAL, "{what}");
            let buffer = returned(refused.payload, passed, b"one", &what);
            pair.session(SERVER)
                .write_object(sg, 1, buffer, now)
                .expect("the same buffer, written");
            pair.session(SERVER)
                .close_subgroup(sg, now)
                .expect("close subgroup");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                objects(&c),
                [(0, 0, b"zero".to_vec()), (0, 1, b"one".to_vec())],
                "{what}"
            );

            // A datagram on a subscription not yet accepted.
            let now = pair.now;
            let config = SubscribeConfig::new(
                live(),
                "dg",
                LocationFilter::AbsoluteStart(Location::default()),
            );
            pair.session(CLIENT)
                .subscribe(&config, now)
                .expect("subscribe");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let pending = first(&s, EventKind::SubscribeRequest)
                .and_then(|e| e.subscription)
                .expect("the request");
            let buffer = counted(b"dg");
            let passed = buffer.as_ptr();
            let now = pair.now;
            let refused = pair
                .session(SERVER)
                .send_object_datagram(pending, at(2, 0), 3, false, buffer, &[], now)
                .expect_err("not established");
            assert_eq!(refused.error, Error::WRONG_STATE, "{what}");
            let buffer = returned(refused.payload, passed, b"dg", &what);
            pair.session(SERVER)
                .accept_subscribe(pending, &AcceptSubscribe::default(), now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            let now = pair.now;
            pair.session(SERVER)
                .send_object_datagram(pending, at(2, 0), 3, false, buffer, &[], now)
                .expect("the same buffer, sent");
            assert!(pair.run(PASSES), "{what}");
            let c = pair.drain(CLIENT);
            let object = first(&c, EventKind::ObjectReceived).expect("the datagram");
            assert!(object.flag, "{what}");
            assert_eq!(object.payload, b"dg", "{what}");

            // A publication datagram after its subscriber cancelled it.
            let (server_pub, client_pub) = publication(&mut pair, "p");
            let now = pair.now;
            pair.session(CLIENT)
                .cancel_publication(client_pub, 0x20, now)
                .expect("cancel");
            assert!(pair.run(PASSES), "{what}");
            pair.drain(SERVER);
            let buffer = counted(b"late");
            let passed = buffer.as_ptr();
            let now = pair.now;
            let refused = pair
                .session(SERVER)
                .send_publication_datagram(server_pub, at(0, 0), 3, false, buffer, &[], now)
                .expect_err("the publication ended");
            assert!(
                [Error::STALE_HANDLE, Error::WRONG_STATE].contains(&refused.error),
                "{what}: {:?}",
                refused.error
            );
            drop(returned(refused.payload, passed, b"late", &what));
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

/// One even-type object property (type 2, value 0x21): the same bytes on
/// every draft, a varint value below 64.
const PROPERTIES: [u8; 2] = [0x02, 0x21];
const OTHER_PROPERTIES: [u8; 2] = [0x02, 0x22];

#[test]
fn object_properties_reach_the_receiver() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            for streaming in [false, true] {
                let what = format!("draft {} {mode} chunks {streaming}", version.draft());
                let options = Options {
                    streaming_objects: [streaming, false],
                    ..base.clone()
                };
                let mut pair = open(version, &options);
                let track = subscribe(&mut pair, "props");
                let mut config = SubgroupConfig::new(0);
                config.object_properties = true;
                let now = pair.now;
                let server = pair.session(SERVER);
                let sg = server
                    .open_subgroup(track.server, &config, now)
                    .expect("a subgroup");
                let whole = ObjectWrite::new(0, counted(b"whole")).properties(counted(&PROPERTIES));
                server
                    .write_object_with(sg, whole, now)
                    .expect("a whole object");
                server
                    .begin_object_with_properties(sg, 1, 6, counted(&OTHER_PROPERTIES), now)
                    .expect("a streamed object");
                server
                    .write_object_data(sg, counted(b"stream"), now)
                    .expect("its bytes");
                server.end_object(sg, now).expect("its end");
                server.close_subgroup(sg, now).expect("close subgroup");
                server
                    .send_object_datagram(
                        track.server,
                        at(1, 0),
                        3,
                        false,
                        counted(b"dg"),
                        &PROPERTIES,
                        now,
                    )
                    .expect("a datagram");
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let mut carried: Vec<(u64, u64, Vec<u8>)> = c
                    .iter()
                    .filter(|e| {
                        matches!(
                            e.kind,
                            Some(EventKind::ObjectReceived | EventKind::ObjectChunk)
                        ) && !e.properties.is_empty()
                    })
                    .map(|e| (e.group, e.object, e.properties.clone()))
                    .collect();
                carried.sort();
                assert_eq!(
                    carried,
                    [
                        (0, 0, PROPERTIES.to_vec()),
                        (0, 1, OTHER_PROPERTIES.to_vec()),
                        (1, 0, PROPERTIES.to_vec())
                    ],
                    "{what}: each object's exact properties, once"
                );
                if streaming {
                    assert!(
                        c.iter().any(|e| e.kind == Some(EventKind::ObjectChunk)
                            && (e.group, e.object) == (0, 1)
                            && e.properties == OTHER_PROPERTIES),
                        "{what}: on the streamed object's first chunk"
                    );
                } else {
                    assert_eq!(
                        objects(&c),
                        [
                            (0, 0, b"whole".to_vec()),
                            (0, 1, b"stream".to_vec()),
                            (1, 0, b"dg".to_vec())
                        ],
                        "{what}"
                    );
                }
                let datagram = c
                    .iter()
                    .find(|e| (e.group, e.object) == (1, 0) && !e.properties.is_empty())
                    .expect("the datagram");
                if datagram.kind == Some(EventKind::ObjectReceived) {
                    assert!(datagram.flag, "{what}: delivered as a datagram");
                }

                // A FETCH object with its properties.
                let (_, fetch) = request_fetch(&mut pair, "past");
                let now = pair.now;
                pair.session(SERVER)
                    .accept_fetch(fetch, &AcceptFetch::new(0, 9), now)
                    .expect("accept");
                let mut object = object_write(0, 0, counted(b"fetched"));
                object.properties = Some(counted(&PROPERTIES));
                pair.session(SERVER)
                    .write_fetch_object(fetch, object, now)
                    .expect("a fetch object");
                retry(&mut pair, SERVER, |s, now| s.end_fetch(fetch, now));
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let fetched = first(&c, EventKind::FetchObject).expect("the fetch object");
                assert_eq!(fetched.payload, b"fetched", "{what}");
                assert_eq!(fetched.properties, PROPERTIES, "{what}");
                assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
                close(pair);
            }
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_blocked_streamed_object_returns_its_properties() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                max_actions: [0, 16],
                ..base
            };
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "props");
            let mut config = SubgroupConfig::new(0);
            config.object_properties = true;
            let now = pair.now;
            let sg = pair
                .session(SERVER)
                .open_subgroup(track.server, &config, now)
                .expect("a subgroup");

            // Whole objects fill the action queue, nothing polled.
            let mut next = None;
            for object in 0..64u64 {
                let buffer = counted(b"o");
                let passed = buffer.as_ptr();
                match pair.session(SERVER).write_object(sg, object, buffer, now) {
                    Ok(()) => {}
                    Err(refused) => {
                        assert_eq!(refused.error, Error::WOULD_BLOCK, "{what}");
                        drop(returned(refused.payload, passed, b"o", &what));
                        next = Some(object);
                        break;
                    }
                }
            }
            let next = next.expect("the queue fills");
            assert!(next > 0, "{what}");

            // The streamed object would block: its properties come back,
            // the very buffer and the caller's alone, and nothing begins.
            let properties = counted(&OTHER_PROPERTIES);
            let passed = properties.as_ptr();
            let refused = pair
                .session(SERVER)
                .begin_object_with_properties(sg, next, 6, properties, now)
                .expect_err("a full queue");
            assert_eq!(refused.error, Error::WOULD_BLOCK, "{what}");
            assert!(refused.payload.is_none(), "{what}");
            let properties = returned(refused.properties, passed, &OTHER_PROPERTIES, &what);
            let refused = pair
                .session(SERVER)
                .write_object_data(sg, counted(b"stream"), now)
                .expect_err("no streamed object is active");
            assert_eq!(refused.error, Error::WRONG_STATE, "{what}");
            drop(refused);

            // Drained, the same buffer begins the object.
            assert!(pair.run(PASSES), "{what}");
            let now = pair.now;
            let server = pair.session(SERVER);
            server
                .begin_object_with_properties(sg, next, 6, properties, now)
                .expect("the retry");
            server
                .write_object_data(sg, counted(b"stream"), now)
                .expect("its bytes");
            server.end_object(sg, now).expect("its end");
            server.close_subgroup(sg, now).expect("close subgroup");
            let mut seen = Vec::new();
            for _ in 0..4 {
                assert!(pair.run(PASSES), "{what}");
                seen.extend(pair.drain(CLIENT));
            }
            let carried: Vec<_> = seen
                .iter()
                .filter(|e| !e.properties.is_empty())
                .map(|e| (e.group, e.object, e.properties.clone()))
                .collect();
            assert_eq!(
                carried,
                [(0, next, OTHER_PROPERTIES.to_vec())],
                "{what}: the properties, once"
            );
            let got = objects(&seen);
            assert_eq!(got.len() as u64, next + 1, "{what}: every object");
            assert_eq!(got.last(), Some(&(0, next, b"stream".to_vec())), "{what}");
            assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
            close(pair);
            assert_eq!(outstanding(), 0, "{what}: each buffer released once");
        }
    }
}

// -- Native selections -----------------------------------------------------------

fn setup_selections(config: SessionConfig) -> SessionConfig {
    config
        .setup_auth_token(AuthToken {
            token_type: 1,
            value: b"setup".to_vec(),
        })
        .auth_token_cache_size(1024)
}

#[test]
fn native_selections_reach_the_peer() {
    for version in VERSIONS {
        for (mode, base) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let options = Options {
                configure: [Some(setup_selections), None],
                ..base
            };
            // The setup's tokens and cache size.
            let mut pair = Pair::new(version, &options).expect("a pair");
            assert!(pair.run(PASSES), "{what}");
            assert!(pair.established(), "{what}");
            let s = pair.drain(SERVER);
            let setup = first(&s, EventKind::SetupComplete).expect("the setup");
            assert_eq!(setup.tokens, [(1, b"setup".to_vec())], "{what}");
            assert_eq!(
                pair.hosts[SERVER].session.peer_auth_token_cache_size(),
                1024,
                "{what}"
            );
            pair.drain(CLIENT);

            // A namespace subscription's own tokens.
            let config = SubscribeNamespaceConfig::new(live(), NamespaceInterest::NamespaceState)
                .auth_token(AuthToken {
                    token_type: 1,
                    value: b"ns".to_vec(),
                });
            let now = pair.now;
            pair.session(CLIENT)
                .subscribe_namespace_with(&config, now)
                .expect("a namespace subscription");
            assert!(pair.run(PASSES), "{what}");
            let s = pair.drain(SERVER);
            let request = first(&s, EventKind::NamespaceSubscriptionRequest).expect("the request");
            assert_eq!(request.tokens, [(1, b"ns".to_vec())], "{what}");

            // A PUBLISH's draft-22 selections.
            let selected = PublishConfig::new(live(), "chosen")
                .expires_ms(5000)
                .subscriber_priority(9)
                .group_order(GroupOrder::Descending)
                .location(LocationFilter::AbsoluteStart(at(1, 0)))
                .object_delivery_timeout_ms(100)
                .subgroup_delivery_timeout_ms(200);
            let now = pair.now;
            let result = pair.session(SERVER).publish(&selected, now);
            if version != Version::DRAFT_22 {
                assert_eq!(result.err(), Some(Error::UNSUPPORTED), "{what}");
            } else {
                result.expect("a PUBLISH with its selections");
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let request = first(&c, EventKind::PublishRequest).expect("the PUBLISH");
                assert_eq!(request.expires_ms, Some(5000), "{what}");
                let client_pub = request.publication.expect("its handle");
                let initial = pair.hosts[CLIENT]
                    .session
                    .publish_initial_parameters(client_pub)
                    .expect("the initial parameters");
                assert_eq!(
                    (
                        initial.subscriber_priority,
                        initial.subscriber_priority_carried
                    ),
                    (9, true),
                    "{what}"
                );
                assert_eq!(
                    (initial.group_order, initial.group_order_carried),
                    (Some(GroupOrder::Descending), true),
                    "{what}"
                );
                assert_eq!(
                    initial.location,
                    Some(LocationFilter::AbsoluteStart(at(1, 0))),
                    "{what}"
                );
                assert_eq!(
                    (
                        initial.object_delivery_timeout_ms,
                        initial.subgroup_delivery_timeout_ms
                    ),
                    (Some(100), Some(200)),
                    "{what}"
                );
                // Draft 22's answer carries EXPIRES and refuses the legacy
                // overrides before anything is sent.
                let now = pair.now;
                assert_eq!(
                    pair.session(CLIENT)
                        .accept_publish(
                            client_pub,
                            &AcceptPublish::new().subscriber_priority(7),
                            now
                        )
                        .err(),
                    Some(Error::INVAL),
                    "{what}"
                );
                pair.session(CLIENT)
                    .accept_publish(client_pub, &AcceptPublish::new().expires_ms(3000), now)
                    .expect("accept");
                assert!(pair.run(PASSES), "{what}");
                let s = pair.drain(SERVER);
                let ok = first(&s, EventKind::PublishOk).expect("the acceptance");
                assert_eq!(ok.expires_ms, Some(3000), "{what}");
            }

            // Drafts 16 and 18: the acceptance's own priority and forward
            // state; EXPIRES is draft 22's.
            if version != Version::DRAFT_22 {
                let now = pair.now;
                let server_pub = pair
                    .session(SERVER)
                    .publish(&PublishConfig::new(live(), "plain"), now)
                    .expect("publish");
                assert!(pair.run(PASSES), "{what}");
                let c = pair.drain(CLIENT);
                let client_pub = first(&c, EventKind::PublishRequest)
                    .and_then(|e| e.publication)
                    .expect("the PUBLISH");
                let now = pair.now;
                assert_eq!(
                    pair.session(CLIENT)
                        .accept_publish(client_pub, &AcceptPublish::new().expires_ms(1), now)
                        .err(),
                    Some(Error::UNSUPPORTED),
                    "{what}"
                );
                let accept = AcceptPublish::new().subscriber_priority(7).forward(false);
                pair.session(CLIENT)
                    .accept_publish(client_pub, &accept, now)
                    .expect("accept");
                assert!(pair.run(PASSES), "{what}");
                let s = pair.drain(SERVER);
                let ok = first(&s, EventKind::PublishOk).expect("the acceptance");
                assert_eq!(ok.publication, Some(server_pub), "{what}");
                assert_eq!(ok.priority, Some(7), "{what}");
                assert_eq!(ok.forward, Some(false), "{what}");
            }

            // Forward omitted or chosen, from a paused PUBLISH and from a
            // forwarding one. Drafts 16 and 18 forward when the answer omits
            // Forward; draft 22 keeps the PUBLISH's own state and refuses
            // the override.
            let legacy = version != Version::DRAFT_22;
            for (name, paused, chosen) in [
                ("paused-omitted", true, None),
                ("paused-false", true, Some(false)),
                ("open-omitted", false, None),
            ] {
                let mut config = PublishConfig::new(live(), name);
                if paused {
                    config = config.forward(false);
                }
                let now = pair.now;
                let server_pub = pair.session(SERVER).publish(&config, now).expect("publish");
                assert!(pair.run(PASSES), "{what} {name}");
                let client_pub = first(&pair.drain(CLIENT), EventKind::PublishRequest)
                    .and_then(|e| e.publication)
                    .expect("the PUBLISH");
                let mut accept = AcceptPublish::new();
                if let Some(forward) = chosen {
                    accept = accept.forward(forward);
                }
                let now = pair.now;
                let result = pair
                    .session(CLIENT)
                    .accept_publish(client_pub, &accept, now);
                if chosen.is_some() && !legacy {
                    assert_eq!(result.err(), Some(Error::INVAL), "{what} {name}");
                    pair.session(CLIENT)
                        .accept_publish(client_pub, &AcceptPublish::new(), now)
                        .expect("accept");
                } else {
                    result.expect("accept");
                }
                assert!(pair.run(PASSES), "{what} {name}");
                let ok = first(&pair.drain(SERVER), EventKind::PublishOk)
                    .map(|e| (e.publication, e.forward))
                    .expect("the acceptance");
                let forwarding = match (legacy, chosen) {
                    (true, Some(forward)) => forward,
                    (true, None) => true,
                    (false, _) => !paused,
                };
                assert_eq!(ok, (Some(server_pub), Some(forwarding)), "{what} {name}");
            }
            close(pair);
        }
    }
}

#[test]
fn bounded_selections_are_refused_at_creation() {
    for version in VERSIONS {
        let what = format!("draft {}", version.draft());
        let token = |value: Vec<u8>| AuthToken {
            token_type: 1,
            value,
        };
        let mut many = SessionConfig::new(Perspective::Client, version);
        for _ in 0..17 {
            many = many.setup_auth_token(token(b"t".to_vec()));
        }
        assert_eq!(
            Session::try_new(&many, Instant::from_micros(1)).err(),
            Some(Error::INVAL),
            "{what}: at most 16 setup tokens"
        );
        let long = SessionConfig::new(Perspective::Client, version)
            .setup_auth_token(token(vec![0x61; 65536]));
        assert_eq!(
            Session::try_new(&long, Instant::from_micros(1)).err(),
            Some(Error::INVAL),
            "{what}: a token value of at most 65535 bytes"
        );
        let fills = SessionConfig::new(Perspective::Client, version).max_fills(65537);
        assert_eq!(
            Session::try_new(&fills, Instant::from_micros(1)).err(),
            Some(Error::INVAL),
            "{what}: at most 65536 fills"
        );
        let sixteen = (0..16).fold(SessionConfig::new(Perspective::Client, version), |c, _| {
            c.setup_auth_token(token(b"t".to_vec()))
        });
        assert!(
            Session::try_new(&sixteen, Instant::from_micros(1)).is_ok(),
            "{what}: sixteen are taken"
        );
        // Creation checks that the setup encodes, not that it fits the send
        // buffer: a 128-byte token with a 64-byte buffer is created, and its
        // start is refused; the same token with the default buffer starts.
        let small = Options {
            configure: [Some(small_send_buffer), None],
            ..Options::default()
        };
        assert!(
            Session::try_new(
                &small_send_buffer(SessionConfig::new(Perspective::Client, version)),
                Instant::from_micros(1)
            )
            .is_ok(),
            "{what}: created"
        );
        assert_eq!(
            Pair::new(version, &small).err(),
            Some(Error::BUFFER),
            "{what}: the setup does not fit the send buffer"
        );
        let roomy = Options {
            configure: [Some(long_setup_token), None],
            ..Options::default()
        };
        let mut pair = Pair::new(version, &roomy).expect("a pair");
        assert!(pair.run(PASSES), "{what}");
        assert!(pair.established(), "{what}: the default buffer holds it");
        pair.drain(SERVER);
        close(pair);
    }
}

fn long_setup_token(config: SessionConfig) -> SessionConfig {
    config.setup_auth_token(AuthToken {
        token_type: 1,
        value: vec![0x61; 128],
    })
}

fn small_send_buffer(config: SessionConfig) -> SessionConfig {
    long_setup_token(config).send_buffer_size(64)
}

// -- STOP and teardown at the host boundary ---------------------------------------

#[test]
fn a_request_stop_keeps_a_queued_native_reset() {
    for version in STREAM_PROFILES {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let (_, fetch) = serve_fetch(&mut pair, "past");
            let (data, request) = fetch_streams(&pair);
            let (resets, repeats) = (pair.wire.resets[SERVER], pair.wire.reset_repeats[SERVER]);
            // The native abort is queued, not polled, when the peer's STOP
            // of the request stream, with another code, arrives.
            let now = pair.now;
            pair.session(SERVER)
                .abort_fetch(fetch, 0x11, now)
                .expect("abort");
            assert!(pair.peer_stops_request(SERVER, request, 0x5));
            assert!(pair.run(PASSES), "{what}");
            let (d, r) = (trace(&pair, data), trace(&pair, request));
            assert_eq!(
                (r.reset, r.fin),
                (Some(0x11), None),
                "{what}: the native code stands"
            );
            assert_eq!((d.reset, d.fin), (Some(0x11), None), "{what}");
            assert_eq!(
                pair.wire.resets[SERVER],
                resets + 2,
                "{what}: one reset each"
            );
            assert_eq!(
                pair.wire.reset_repeats[SERVER], repeats,
                "{what}: no second one"
            );
            assert_eq!(pair.hosts[SERVER].counters().stops_answered, 0, "{what}");
            let s = pair.drain(SERVER);
            assert_eq!(
                count(&s, EventKind::FetchCancelled),
                0,
                "{what}: no extra terminal"
            );
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_request_stop_the_session_leaves_is_answered_once() {
    for version in STREAM_PROFILES {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let (_, _fetch) = serve_fetch(&mut pair, "past");
            let (_, request) = fetch_streams(&pair);
            let resets = pair.wire.resets[SERVER];
            assert!(pair.peer_stops_request(SERVER, request, 0x5));
            assert!(pair.run(PASSES), "{what}");
            let r = trace(&pair, request);
            assert_eq!(r.fin, None, "{what}");
            assert!(r.reset.is_some(), "{what}: the STOP is answered");
            let answered = pair.hosts[SERVER].counters().stops_answered;
            assert_eq!(
                (answered, r.reset),
                (1, Some(0x5)),
                "{what}: answered once, with the STOP's code"
            );
            assert!(pair.wire.resets[SERVER] > resets, "{what}");
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_repeated_stop_reaches_the_host_and_changes_nothing() {
    for version in STREAM_PROFILES {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &fence_server());
        let (_, fetch) = serve_fetch(&mut pair, "past");
        let (data, request) = fetch_streams(&pair);
        let now = pair.now;
        pair.session(SERVER).end_fetch(fetch, now).expect("end");
        let resets = pair.wire.resets[SERVER];
        let taken = pair.hosts[SERVER].counters().peer_stops;
        // Both STOPs reach the host before the final message is polled:
        // the first obligation is still pending when the second arrives.
        assert!(pair.deliver_peer_stop(SERVER, data, 0x2A));
        assert!(pair.deliver_peer_stop(SERVER, data, 0x2B));
        let n = pair.hosts[SERVER].counters();
        assert_eq!(
            (n.stop_entries, n.stop_repeats),
            (2, 1),
            "{what}: both reached it"
        );
        assert_eq!(n.peer_stops, taken + 1, "{what}: the session saw one");
        assert!(pair.run(PASSES), "{what}");
        let (d, r) = (trace(&pair, data), trace(&pair, request));
        assert_eq!(
            (d.reset, d.fin),
            (Some(0x2A), None),
            "{what}: the first code"
        );
        assert_eq!((r.reset, r.fin), (Some(STREAM_CANCELLED), None), "{what}");
        assert_eq!(
            pair.wire.resets[SERVER],
            resets + 2,
            "{what}: one reset each"
        );
        assert_eq!(pair.hosts[SERVER].counters().finals_cancelled, 1, "{what}");
        // And once more after both resets went.
        assert!(pair.deliver_peer_stop(SERVER, data, 0x2C));
        assert!(pair.run(PASSES), "{what}");
        let n = pair.hosts[SERVER].counters();
        assert_eq!((n.stop_repeats, n.peer_stops), (2, taken + 1), "{what}");
        assert_eq!(
            (trace(&pair, data), trace(&pair, request)),
            (d, r),
            "{what}"
        );
        assert_eq!(pair.wire.resets[SERVER], resets + 2, "{what}");
        let s = pair.drain(SERVER);
        assert_eq!(count(&s, EventKind::FetchCancelled), 0, "{what}");
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_native_abort_does_not_wait_behind_held_request_bytes() {
    for version in STREAM_PROFILES {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let (_, fetch) = request_fetch(&mut pair, "past");
            let requests = pair.hosts[SERVER].served_requests();
            assert_eq!(requests.len(), 1, "{what}");
            let request = requests[0];
            // FETCH_OK is refused by the transport and held.
            assert!(pair.refuse_request_stream(SERVER, request));
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(fetch, &AcceptFetch::new(0, 9), now)
                .expect("accept");
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(trace(&pair, request).writes, 0, "{what}: FETCH_OK held");
            assert!(pair.hosts[SERVER].held() >= 1, "{what}");
            // The abort tears the request down at once, the refusal still
            // standing: the held bytes are dropped, never written.
            let now = pair.now;
            pair.session(SERVER)
                .abort_fetch(fetch, 0x11, now)
                .expect("abort");
            assert!(pair.run(PASSES), "{what}");
            let r = trace(&pair, request);
            assert_eq!((r.reset, r.fin, r.writes), (Some(0x11), None, 0), "{what}");
            let index = pair.hosts[SERVER]
                .wire_of_stream(request)
                .expect("the request stream");
            assert_eq!(
                pair.wire.streams[index].dir[CLIENT].stop,
                Some(0x11),
                "{what}: the abort stops the peer's half too"
            );
            assert!(
                pair.hosts[SERVER].counters().teardown_discards >= 1,
                "{what}"
            );
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn an_ordinary_request_fin_waits_behind_held_bytes() {
    for version in STREAM_PROFILES {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let (client_fetch, fetch) = request_fetch(&mut pair, "past");
            let requests = pair.hosts[SERVER].served_requests();
            assert_eq!(requests.len(), 1, "{what}");
            let request = requests[0];
            assert!(pair.refuse_request_stream(SERVER, request));
            let now = pair.now;
            pair.session(SERVER)
                .accept_fetch(fetch, &AcceptFetch::new(0, 9), now)
                .expect("accept");
            write_fetch(&mut pair, fetch, 0, "f-zero");
            assert!(pair.run(PASSES), "{what}");
            retry(&mut pair, SERVER, |s, now| s.end_fetch(fetch, now));
            assert!(pair.run(PASSES), "{what}");
            // The FIN stays behind the held bytes while they are refused.
            let r = trace(&pair, request);
            assert_eq!((r.writes, r.fin, r.reset), (0, None, None), "{what}");
            assert!(pair.hosts[SERVER].held() >= 1, "{what}");
            pair.lift_data_refusal(SERVER);
            assert!(pair.run(PASSES), "{what}");
            let r = trace(&pair, request);
            assert!(
                r.writes >= 1 && r.fin.is_some(),
                "{what}: then follows them"
            );
            assert_eq!(r.reset, None, "{what}");
            let c = pair.drain(CLIENT);
            assert_eq!(
                first(&c, EventKind::FetchComplete).and_then(|e| e.fetch),
                Some(client_fetch),
                "{what}"
            );
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_burst_of_new_streams_stays_within_the_refused_bound() {
    for version in VERSIONS {
        // Draft 22 also takes a stream type in its nonminimal two-byte
        // form, so the type itself can arrive split across reads.
        let schedules: &[bool] = if version == Version::DRAFT_22 {
            &[false, true]
        } else {
            &[false]
        };
        for &split in schedules {
            for (mode, base) in modes() {
                let what = format!("draft {} {mode} split {split}", version.draft());
                burst_within_the_bound(version, base, split, &what);
            }
        }
    }
    assert_eq!(outstanding(), 0);
}

fn burst_within_the_bound(version: Version, base: Options, split: bool, what: &str) {
    let options = Options {
        max_data_streams: [1, 0],
        ..base
    };
    let mut pair = open(version, &options);
    let track = subscribe(&mut pair, "burst");
    let first_group = group(&mut pair, &track, 0);
    write(&mut pair, first_group, 0, "g0");
    assert!(pair.run(PASSES), "{what}");
    assert_eq!(
        objects(&pair.drain(CLIENT)),
        [(0, 0, b"g0".to_vec())],
        "{what}"
    );

    // Six more subgroups reach the client's transport while its one entry
    // is taken.
    pair.wire.hold_input[CLIENT] = true;
    let mut groups = Vec::new();
    for g in 1..=6u64 {
        let sg = group(&mut pair, &track, g);
        write(&mut pair, sg, 0, &format!("g{g}"));
        groups.push(sg);
    }
    assert!(pair.run(PASSES), "{what}");
    let fresh: Vec<usize> = pair.hosts[SERVER]
        .local_data_streams()
        .iter()
        .skip(1)
        .map(|&s| pair.hosts[SERVER].wire_of_stream(s).expect("a stream"))
        .collect();
    assert_eq!(fresh.len(), 6, "{what}");
    assert!(
        fresh.windows(2).all(|w| w[0] < w[1]),
        "{what}: read in this order"
    );

    // Split: each stream first shows only the leading byte of its type in
    // the two-byte form (`80`); the type's second byte and the header follow
    // on a later wake.
    let mut rest = Vec::new();
    let mut sizes = Vec::new();
    for &index in &fresh {
        let pipe = &mut pair.wire.streams[index].dir[SERVER];
        assert_eq!(pipe.read_off, 0, "{what}: nothing read yet");
        if split {
            let bytes: Vec<u8> = pipe.buf.drain(..).collect();
            assert!(bytes[0] < 0x80, "{what}: a one-byte stream type");
            pipe.buf.push_back(0x80);
            pipe.taken += 1;
            sizes.push(1 + bytes.len());
            rest.push(bytes);
        } else {
            sizes.push(pipe.buf.len());
        }
    }
    pair.wire.hold_input[CLIENT] = false;
    assert!(pair.run(PASSES), "{what}");
    if split {
        // Four streams are read and pending, their types incomplete; the
        // other two are left unread.
        assert_eq!(pair.hosts[CLIENT].held_reads(), (0, 4, 4), "{what}");
        assert_eq!(pair.hosts[CLIENT].pending_streams(), 4, "{what}");
        for (&index, bytes) in fresh.iter().zip(rest) {
            pair.wire.streams[index].dir[SERVER].buf.extend(bytes);
        }
        assert!(pair.run(PASSES), "{what}");
    }
    let held: usize = sizes[..4].iter().sum();
    assert_eq!(
        pair.hosts[CLIENT].held_reads(),
        (host::MAX_REFUSED, host::MAX_REFUSED, held),
        "{what}: the bound reached, the first four held whole"
    );
    assert_eq!(pair.hosts[CLIENT].pending_streams(), 4, "{what}");
    for &index in &fresh[4..] {
        assert_eq!(
            pair.wire.streams[index].dir[SERVER].read_off, 0,
            "{what}: the rest left unread"
        );
    }
    assert!(
        pair.hosts[CLIENT].counters().admissions_deferred >= 1,
        "{what}: the rest deferred"
    );

    // Control still flows.
    let now = pair.now;
    let asked = pair
        .session(CLIENT)
        .track_status(&TrackStatusConfig::new(live(), "st"), now)
        .expect("a TRACK_STATUS");
    assert!(pair.run(PASSES), "{what}");
    let s = pair.drain(SERVER);
    let server_req = first(&s, EventKind::TrackStatusRequest)
        .and_then(|e| e.track_status)
        .expect("the request");
    let now = pair.now;
    pair.session(SERVER)
        .accept_track_status(server_req, &AcceptTrackStatus::default(), now)
        .expect("accept");
    assert!(pair.run(PASSES), "{what}");
    let c = pair.drain(CLIENT);
    assert_eq!(
        first(&c, EventKind::TrackStatusOk).and_then(|e| e.track_status),
        Some(asked),
        "{what}"
    );

    // Ending the subgroups frees the entry: every object arrives.
    let now = pair.now;
    pair.session(SERVER)
        .close_subgroup(first_group, now)
        .expect("close");
    for sg in groups {
        pair.session(SERVER).close_subgroup(sg, now).expect("close");
    }
    let mut delivered = objects(&c);
    for _ in 0..16 {
        assert!(pair.run(PASSES), "{what}");
        delivered.extend(objects(&pair.drain(CLIENT)));
    }
    delivered.sort();
    let expected: Vec<_> = (1..=6u64)
        .map(|g| (g, 0, format!("g{g}").into_bytes()))
        .collect();
    assert_eq!(delivered, expected, "{what}");
    assert_eq!(pair.hosts[CLIENT].pending_streams(), 0, "{what}");
    assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
    close(pair);
}

// -- FIN reports ---------------------------------------------------------------
//
// In the tagged mode the host reports each FIN of a local data stream the
// transport accepted. While output of a closed subgroup with a delivery
// deadline may still be held, the session keeps that subgroup, and its
// place in the pool, so a held expiry still commits its one terminal with
// the exact identity; the accepted FIN ends that hold.

const TIMEOUT_MS: u64 = 50;
const RESET_DELIVERY_TIMEOUT: u64 = moq5_core::sys::MOQ_RESET_DELIVERY_TIMEOUT as u64;

/// One place in the server's subgroup pool.
fn pool_of_one(config: SessionConfig) -> SessionConfig {
    config.max_open_subgroups(1)
}

/// The server in the tagged mode, the client not; `pool` gives the server
/// one place for subgroups.
fn timed_server(pool: bool) -> Options {
    let mut options = fence_server();
    if pool {
        options.configure[SERVER] = Some(pool_of_one);
    }
    options
}

/// What the server's subgroups belong to.
#[derive(Clone, Copy, Debug)]
enum Timed {
    Subscription(Subscription),
    Publication(Publication),
}

/// A subscription the server accepted, or a publication it began, with an
/// object delivery timeout of `TIMEOUT_MS` in its Track Properties.
fn timed(pair: &mut Pair, publication: bool) -> Timed {
    let properties = pair.hosts[SERVER]
        .session
        .track_properties_with_delivery_timeouts(&[], Some(TIMEOUT_MS), None)
        .expect("a delivery timeout");
    if publication {
        let now = pair.now;
        let config = PublishConfig::new(live(), "timed-pub").track_properties(properties);
        let server = pair.session(SERVER).publish(&config, now).expect("publish");
        assert!(pair.run(PASSES));
        let c = pair.drain(CLIENT);
        let client = first(&c, EventKind::PublishRequest)
            .and_then(|e| e.publication)
            .expect("a publish request");
        let now = pair.now;
        pair.session(CLIENT)
            .accept_publish(client, &AcceptPublish::default(), now)
            .expect("accept publish");
        assert!(pair.run(PASSES));
        pair.drain(SERVER);
        return Timed::Publication(server);
    }
    let now = pair.now;
    let config = SubscribeConfig::new(
        live(),
        "timed",
        LocationFilter::AbsoluteStart(Location::default()),
    );
    pair.session(CLIENT)
        .subscribe(&config, now)
        .expect("subscribe");
    assert!(pair.run(PASSES));
    let s = pair.drain(SERVER);
    let server = first(&s, EventKind::SubscribeRequest)
        .and_then(|e| e.subscription)
        .expect("a subscribe request");
    let mut accept = AcceptSubscribe::default();
    accept.track_properties = properties;
    let now = pair.now;
    pair.session(SERVER)
        .accept_subscribe(server, &accept, now)
        .expect("accept");
    assert!(pair.run(PASSES));
    pair.drain(CLIENT);
    Timed::Subscription(server)
}

fn open_timed(pair: &mut Pair, owner: Timed, group: u64) -> moq5_core::Result<moq5_core::Subgroup> {
    let now = pair.now;
    let config = SubgroupConfig::new(group);
    match owner {
        Timed::Subscription(s) => pair.session(SERVER).open_subgroup(s, &config, now),
        Timed::Publication(p) => pair
            .session(SERVER)
            .open_publication_subgroup(p, &config, now),
    }
}

/// The stream the server opened last.
fn last_stream(pair: &Pair) -> StreamRef {
    *pair.hosts[SERVER]
        .local_data_streams()
        .last()
        .expect("a stream of the server")
}

/// An advancing call with nothing due. Polling is not one, and with no
/// input and no timer due the host makes none while its output waits:
/// without it the pool would not be swept, and a kept subgroup could not
/// be told from an unswept one.
fn sweep(pair: &mut Pair) {
    let now = pair.now;
    // What the tick found is not the point: only that the session advanced.
    let _ = pair.session(SERVER).tick(now).expect("a tick");
}

/// Whether the server's one place in the pool is taken: a subgroup of
/// `group` cannot open. A refused open changes nothing.
fn pool_taken(pair: &mut Pair, owner: Timed, group: u64) -> bool {
    match open_timed(pair, owner, group) {
        Err(Error::WOULD_BLOCK) => true,
        Ok(subgroup) => {
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(subgroup, now)
                .expect("close the probe");
            false
        }
        Err(e) => panic!("an open refused with {e:?}"),
    }
}

/// The one terminal in `seen` names the subgroup exactly: its owner, its
/// stream, and (group, first cause, code).
fn check_terminal(
    seen: &[Seen],
    subgroup: moq5_core::Subgroup,
    owner: Timed,
    stream: StreamRef,
    (group, cause, code): (u64, moq5_core::SubgroupTerminalCause, u64),
    what: &str,
) {
    assert_eq!(
        count(seen, EventKind::OutgoingSubgroupTerminated),
        1,
        "{what}: one terminal"
    );
    let t = first(seen, EventKind::OutgoingSubgroupTerminated).expect("the terminal");
    assert_eq!(t.subgroup, Some(subgroup), "{what}");
    match owner {
        Timed::Subscription(s) => {
            assert_eq!((t.subscription, t.publication), (Some(s), None), "{what}");
        }
        Timed::Publication(p) => {
            assert_eq!((t.subscription, t.publication), (None, Some(p)), "{what}");
        }
    }
    assert_eq!(t.stream, Some(stream), "{what}");
    assert_eq!(t.group, group, "{what}");
    assert_eq!(t.terminal_cause, Some(cause), "{what}");
    assert_eq!(t.code, code, "{what}");
}

/// What a FIN report must leave as it was, as the public API and the host
/// show it: the session's state and next deadline, the reset the host
/// would be offered (with its state), the held output and its deadline, and
/// the host's counts.
#[derive(Debug, PartialEq)]
struct Untouched {
    state: SessionState,
    deadline: Option<Instant>,
    offer: (
        u8,
        Option<moq5_core::DataResetOffer>,
        Option<moq5_core::ResetState>,
    ),
    held: usize,
    held_deadline: Option<Instant>,
    claims: u32,
    commits: u32,
    held_expired: u32,
    fins_reported: u32,
    host_resets: u32,
    transport_resets: u32,
}

fn untouched(pair: &Pair) -> Untouched {
    let host = &pair.hosts[SERVER];
    let n = host.counters();
    let offer = match host.session.offer_data_reset().expect("an offer poll") {
        moq5_core::ResetOfferPoll::Offer(o) => (0, Some(o), Some(o.state())),
        moq5_core::ResetOfferPoll::Empty => (1, None, None),
        moq5_core::ResetOfferPoll::Waiting => (2, None, None),
    };
    Untouched {
        state: host.session.state(),
        deadline: host.session.next_deadline(),
        offer,
        held: host.held(),
        held_deadline: host.next_held_deadline(),
        claims: n.claims,
        commits: n.commits,
        held_expired: n.held_expired,
        fins_reported: n.fins_reported,
        host_resets: n.host_resets,
        transport_resets: pair.wire.resets[SERVER],
    }
}

/// A report a reset made obsolete, or one of a stream the session no
/// longer keeps: `Ok`, and nothing moves.
fn report_changes_nothing(pair: &mut Pair, stream: StreamRef, what: &str) {
    let before = untouched(pair);
    assert_eq!(
        pair.session(SERVER).report_data_fin(stream),
        Ok(()),
        "{what}"
    );
    assert_eq!(untouched(pair), before, "{what}: nothing moved");
}

fn server_fins(pair: &Pair) -> u32 {
    pair.hosts[SERVER].counters().fins_reported
}

#[test]
fn fin_reports_keep_their_result_order() {
    for version in VERSIONS {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &timed_server(false));
        let owner = timed(&mut pair, false);
        let peer = StreamRef::peer(0).expect("a peer ref");
        let local = StreamRef::from_raw(1).expect("a local ref");
        // Input first, then the mode: the client is not tagged.
        assert_eq!(
            pair.session(CLIENT).report_data_fin(peer),
            Err(Error::INVAL),
            "{what}"
        );
        assert_eq!(
            pair.session(CLIENT).report_data_fin(local),
            Err(Error::WRONG_STATE),
            "{what}: no report outside the tagged mode"
        );
        assert_eq!(
            pair.session(SERVER).report_data_fin(peer),
            Err(Error::INVAL),
            "{what}"
        );

        let subgroup = open_timed(&mut pair, owner, 0).expect("a subgroup");
        write(&mut pair, subgroup, 0, "first");
        assert!(pair.run(PASSES), "{what}");
        let stream = last_stream(&pair);
        let before = untouched(&pair);
        assert_eq!(
            pair.session(SERVER).report_data_fin(stream),
            Err(Error::WRONG_STATE),
            "{what}: open, its FIN never handed out"
        );
        let now = pair.now;
        pair.session(SERVER)
            .close_subgroup(subgroup, now)
            .expect("close");
        assert_eq!(
            pair.session(SERVER).report_data_fin(stream),
            Err(Error::WRONG_STATE),
            "{what}: its FIN queued, not handed out"
        );
        assert_eq!(server_fins(&pair), before.fins_reported, "{what}");
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(
            server_fins(&pair),
            before.fins_reported + 1,
            "{what}: the host's own report, once"
        );
        assert!(trace(&pair, stream).fin.is_some(), "{what}");
        // A duplicate or late report, and one for a ref never handed out,
        // change nothing.
        report_changes_nothing(&mut pair, stream, &what);
        let unknown = StreamRef::from_raw(stream.as_raw() + 1000).expect("a local ref");
        report_changes_nothing(&mut pair, unknown, &what);
        let s = pair.drain(SERVER);
        assert_eq!(
            count(&s, EventKind::OutgoingSubgroupTerminated),
            0,
            "{what}"
        );
        assert_eq!(
            objects(&pair.drain(CLIENT)),
            [(0, 0, b"first".to_vec())],
            "{what}"
        );

        // Closed: after the mode, input validation still first.
        let now = pair.now;
        pair.session(SERVER).close(0, "end", now).expect("a close");
        assert_eq!(
            pair.session(SERVER).report_data_fin(stream),
            Err(Error::CLOSED),
            "{what}"
        );
        assert_eq!(
            pair.session(SERVER).report_data_fin(peer),
            Err(Error::INVAL),
            "{what}"
        );
        assert!(pair.run(PASSES), "{what}");
        pair.drain(CLIENT);
        pair.drain(SERVER);
        assert!(pair.hosts.iter().all(|h| h.fault().is_none()), "{what}");
        assert_eq!(pair.hosts[SERVER].counters().fins_after_close, 0, "{what}");
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_held_object_and_its_fin_expire_after_a_reported_hand_off() {
    for version in VERSIONS {
        for publication in [false, true] {
            let what = format!(
                "draft {} {}",
                version.draft(),
                if publication {
                    "publication"
                } else {
                    "subscription"
                }
            );
            let mut pair = open(version, &timed_server(true));
            let owner = timed(&mut pair, publication);

            // A healthy hand-off: the object and the FIN are accepted, the
            // FIN reported once, no terminal.
            let handed = open_timed(&mut pair, owner, 0).expect("a subgroup");
            write(&mut pair, handed, 0, "handed");
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(handed, now)
                .expect("close");
            assert!(pair.run(PASSES), "{what}");
            let handed_stream = last_stream(&pair);
            assert!(trace(&pair, handed_stream).fin.is_some(), "{what}");
            let s = pair.drain(SERVER);
            assert_eq!(
                count(&s, EventKind::OutgoingSubgroupTerminated),
                0,
                "{what}"
            );

            // The report gave the one place in the pool back.
            sweep(&mut pair);
            let subgroup =
                open_timed(&mut pair, owner, 1).expect("the place the reported hand-off left");
            assert_eq!(server_fins(&pair), 1, "{what}: the accepted FIN reported");
            write(&mut pair, subgroup, 0, "in time");
            assert!(pair.run(PASSES), "{what}");
            let stream = last_stream(&pair);
            assert_ne!(stream, handed_stream, "{what}");
            assert_eq!(
                objects(&pair.drain(CLIENT)),
                [(0, 0, b"handed".to_vec()), (1, 0, b"in time".to_vec())],
                "{what}"
            );

            // The next object and the FIN behind it are held past the
            // object's deadline.
            assert!(pair.refuse_data_stream(SERVER, Some(stream)));
            write(&mut pair, subgroup, 1, "too late");
            let now = pair.now;
            pair.session(SERVER)
                .close_subgroup(subgroup, now)
                .expect("close");
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(server_fins(&pair), 1, "{what}: a held FIN is not reported");
            let due = pair.next_held_deadline(SERVER).expect("a held deadline");
            assert!(due > pair.now, "{what}");
            sweep(&mut pair);
            assert!(
                pool_taken(&mut pair, owner, 2),
                "{what}: the held output keeps it"
            );
            let resets = pair.wire.resets[SERVER];
            assert!(
                pair.wake(Instant::from_micros(due.as_micros() - 1), PASSES),
                "{what}"
            );
            assert_eq!(pair.hosts[SERVER].counters().held_expired, 0, "{what}");
            assert!(pair.wake(due, PASSES), "{what}");
            let n = pair.hosts[SERVER].counters().clone();
            assert_eq!(n.held_expired, 1, "{what}");
            assert_eq!(n.host_resets, 0, "{what}: the session owns the reset");
            assert_eq!((n.claims, n.commits), (1, 1), "{what}");
            assert_eq!(pair.wire.resets[SERVER], resets + 1, "{what}");
            assert_eq!(
                pair.wire.last_reset_code[SERVER], RESET_DELIVERY_TIMEOUT,
                "{what}"
            );
            let s = pair.drain(SERVER);
            check_terminal(
                &s,
                subgroup,
                owner,
                stream,
                (
                    1,
                    moq5_core::SubgroupTerminalCause::DeliveryTimeout,
                    RESET_DELIVERY_TIMEOUT,
                ),
                &what,
            );

            // Writes are taken again: nothing of the expired object, and no
            // FIN, follows.
            pair.lift_data_refusal(SERVER);
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(server_fins(&pair), 1, "{what}: the FIN never went out");
            let t = trace(&pair, stream);
            assert_eq!(
                (t.fin, t.reset),
                (None, Some(RESET_DELIVERY_TIMEOUT)),
                "{what}"
            );
            let c = pair.drain(CLIENT);
            assert!(objects(&c).is_empty(), "{what}");
            assert_eq!(count(&c, EventKind::SubgroupReset), 1, "{what}");
            assert_eq!(
                first(&c, EventKind::SubgroupReset).map(|e| e.code),
                Some(RESET_DELIVERY_TIMEOUT),
                "{what}"
            );
            sweep(&mut pair);
            assert!(
                !pool_taken(&mut pair, owner, 2),
                "{what}: the place is free again"
            );
            assert!(pair.run(PASSES), "{what}");
            pair.drain(CLIENT);
            pair.drain(SERVER);
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

/// How a reset makes the FIN report of the server's stream obsolete.
#[derive(Clone, Copy, Debug)]
enum Obsolete {
    /// The owner resets the subgroup before its FIN: the reset queued.
    Queued,
    /// The same, with the reset claimed while the transport refuses it.
    Claimed,
    /// The peer stops the stream while the server's events are full: the
    /// terminal waits for capacity (a debt), the FIN not yet queued or
    /// handed out and held.
    Debt { fin_handed: bool },
}

#[test]
fn a_fin_report_a_reset_made_obsolete_changes_nothing() {
    let kinds = [
        Obsolete::Queued,
        Obsolete::Claimed,
        Obsolete::Debt { fin_handed: false },
        Obsolete::Debt { fin_handed: true },
    ];
    for version in VERSIONS {
        for kind in kinds {
            let what = format!("draft {} {kind:?}", version.draft());
            let mut options = timed_server(false);
            if matches!(kind, Obsolete::Debt { .. }) {
                options.max_events[SERVER] = 2;
            }
            let mut pair = open(version, &options);
            let owner = timed(&mut pair, false);
            let subgroup = open_timed(&mut pair, owner, 0).expect("a subgroup");
            write(&mut pair, subgroup, 0, "a");
            assert!(pair.run(PASSES), "{what}");
            let stream = last_stream(&pair);
            if let Obsolete::Debt { fin_handed: true } = kind {
                assert!(pair.refuse_data_stream(SERVER, Some(stream)));
                write(&mut pair, subgroup, 1, "held");
                let now = pair.now;
                pair.session(SERVER)
                    .close_subgroup(subgroup, now)
                    .expect("close");
                assert!(pair.run(PASSES), "{what}");
            }
            let resets = pair.wire.resets[SERVER];
            let n0 = pair.hosts[SERVER].counters().clone();
            let now = pair.now;
            let code = match kind {
                Obsolete::Queued | Obsolete::Claimed => {
                    pair.session(SERVER)
                        .reset_subgroup(subgroup, 0x7, now)
                        .expect("a reset");
                    if let Obsolete::Claimed = kind {
                        pair.wire.refuse_resets[SERVER] = 1000;
                        assert!(pair.run(PASSES), "{what}");
                        let n = pair.hosts[SERVER].counters();
                        assert_eq!((n.claims, n.commits), (n0.claims + 1, n0.commits), "{what}");
                    }
                    0x7
                }
                Obsolete::Debt { .. } => {
                    for name in ["u", "v"] {
                        let config = SubscribeConfig::new(
                            Namespace::parse("other").expect("a namespace"),
                            name,
                            LocationFilter::AbsoluteStart(Location::default()),
                        );
                        let now = pair.now;
                        pair.session(CLIENT)
                            .subscribe(&config, now)
                            .expect("subscribe");
                    }
                    assert!(pair.run(PASSES), "{what}");
                    assert!(pair.peer_stops_data(SERVER, stream, 0x9));
                    assert!(pair.run(PASSES), "{what}");
                    0x9
                }
            };
            let offered = pair.hosts[SERVER]
                .session
                .offer_data_reset()
                .expect("an offer poll");
            let mut reliable = None;
            match kind {
                Obsolete::Queued | Obsolete::Claimed => {
                    let moq5_core::ResetOfferPoll::Offer(offer) = offered else {
                        panic!("{what}: the reset is offered, not {offered:?}");
                    };
                    let state = if let Obsolete::Claimed = kind {
                        moq5_core::ResetState::Claimed
                    } else {
                        moq5_core::ResetState::Ready
                    };
                    assert_eq!(offer.state(), state, "{what}");
                    assert_eq!(
                        (offer.stream(), offer.error_code()),
                        (stream, 0x7),
                        "{what}"
                    );
                    assert!(offer.reliable_size() > 0, "{what}: the header");
                    reliable = Some(offer.reliable_size());
                    assert_eq!(
                        offer.cause(),
                        moq5_core::SubgroupTerminalCause::None,
                        "{what}"
                    );
                }
                Obsolete::Debt { .. } => {
                    assert!(
                        matches!(offered, moq5_core::ResetOfferPoll::Waiting),
                        "{what}: the terminal waits for event capacity, not {offered:?}"
                    );
                    assert_eq!(pair.hosts[SERVER].counters().claims, n0.claims, "{what}");
                }
            }
            report_changes_nothing(&mut pair, stream, &what);
            report_changes_nothing(&mut pair, stream, &what);

            // The reset settles once.
            let mut terminals = Vec::new();
            match kind {
                Obsolete::Claimed => pair.wire.refuse_resets[SERVER] = 0,
                Obsolete::Debt { .. } => terminals.extend(pair.drain(SERVER)),
                Obsolete::Queued => {}
            }
            pair.lift_data_refusal(SERVER);
            assert!(pair.run(PASSES), "{what}");
            terminals.extend(pair.drain(SERVER));
            if let Obsolete::Debt { .. } = kind {
                check_terminal(
                    &terminals,
                    subgroup,
                    owner,
                    stream,
                    (0, moq5_core::SubgroupTerminalCause::PeerStopSending, 0x9),
                    &what,
                );
            } else {
                assert_eq!(
                    count(&terminals, EventKind::OutgoingSubgroupTerminated),
                    0,
                    "{what}"
                );
            }
            let n = pair.hosts[SERVER].counters().clone();
            assert_eq!(
                (n.claims, n.commits),
                (n0.claims + 1, n0.commits + 1),
                "{what}"
            );
            assert_eq!(pair.wire.resets[SERVER], resets + 1, "{what}: one reset");
            assert_eq!(pair.wire.reset_repeats[SERVER], 0, "{what}");
            let t = trace(&pair, stream);
            assert_eq!((t.fin, t.reset), (None, Some(code)), "{what}");
            if reliable.is_some() {
                assert_eq!(t.reset_reliable, reliable, "{what}: the header delivered");
            }
            assert_eq!(n.fins_reported, n0.fins_reported, "{what}: no FIN went out");
            assert_eq!(n.held_expired, 0, "{what}");
            if let Obsolete::Queued | Obsolete::Claimed = kind {
                let c = pair.drain(CLIENT);
                assert_eq!(
                    first(&c, EventKind::SubgroupReset).map(|e| e.code),
                    Some(0x7),
                    "{what}"
                );
            }
            // After the reset settled, and once the entry is gone.
            report_changes_nothing(&mut pair, stream, &what);
            sweep(&mut pair);
            report_changes_nothing(&mut pair, stream, &what);
            pair.drain(CLIENT);
            pair.drain(SERVER);
            server_settled(&pair, &what);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_stale_fin_report_leaves_the_next_subgroup_alone() {
    for version in VERSIONS {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &timed_server(true));
        let owner = timed(&mut pair, false);
        let first_subgroup = open_timed(&mut pair, owner, 0).expect("a subgroup");
        write(&mut pair, first_subgroup, 0, "a");
        let now = pair.now;
        pair.session(SERVER)
            .close_subgroup(first_subgroup, now)
            .expect("close");
        assert!(pair.run(PASSES), "{what}");
        let old = last_stream(&pair);
        sweep(&mut pair);

        // The second subgroup takes the one place, on a new stream, and
        // holds an object and its FIN.
        let subgroup = open_timed(&mut pair, owner, 1).expect("the freed place");
        assert_eq!(server_fins(&pair), 1, "{what}");
        write(&mut pair, subgroup, 0, "b");
        assert!(pair.run(PASSES), "{what}");
        let stream = last_stream(&pair);
        assert_ne!(stream, old, "{what}");
        assert!(pair.refuse_data_stream(SERVER, Some(stream)));
        write(&mut pair, subgroup, 1, "held");
        let now = pair.now;
        pair.session(SERVER)
            .close_subgroup(subgroup, now)
            .expect("close");
        assert!(pair.run(PASSES), "{what}");
        sweep(&mut pair);
        assert!(pool_taken(&mut pair, owner, 2), "{what}");

        // A stale or duplicate report of the first stream touches nothing.
        report_changes_nothing(&mut pair, old, &what);
        report_changes_nothing(&mut pair, old, &what);
        sweep(&mut pair);
        assert!(
            pool_taken(&mut pair, owner, 2),
            "{what}: the second keeps it"
        );

        // The second subgroup's held expiry still commits its own terminal.
        let due = pair.next_held_deadline(SERVER).expect("a held deadline");
        assert!(due > pair.now, "{what}");
        assert!(pair.wake(due, PASSES), "{what}");
        assert_eq!(pair.hosts[SERVER].counters().held_expired, 1, "{what}");
        let s = pair.drain(SERVER);
        check_terminal(
            &s,
            subgroup,
            owner,
            stream,
            (
                1,
                moq5_core::SubgroupTerminalCause::DeliveryTimeout,
                RESET_DELIVERY_TIMEOUT,
            ),
            &what,
        );
        pair.lift_data_refusal(SERVER);
        assert!(pair.run(PASSES), "{what}");
        assert_eq!(server_fins(&pair), 1, "{what}");
        sweep(&mut pair);
        assert!(!pool_taken(&mut pair, owner, 2), "{what}");
        assert!(pair.run(PASSES), "{what}");
        pair.drain(CLIENT);
        pair.drain(SERVER);
        server_settled(&pair, &what);
        close(pair);
    }
    assert_eq!(outstanding(), 0);
}

/// The host holds a FIN while the transport refuses its stream, and so much
/// other output that it polls nothing more. The session closes; the host
/// writes what it holds before it reaches the close, and the FIN accepted
/// then is reported into the closed session: shutdown, `CLOSED`, no fault.
#[test]
fn a_fin_accepted_after_the_close_is_part_of_shutdown() {
    for version in VERSIONS {
        let what = format!("draft {}", version.draft());
        let mut pair = open(version, &timed_server(false));
        let track = subscribe(&mut pair, "drain");
        assert!(pair.refuse_data_stream(SERVER, None));
        let ended = group(&mut pair, &track, 0);
        write(&mut pair, ended, 0, "a");
        let now = pair.now;
        pair.session(SERVER)
            .close_subgroup(ended, now)
            .expect("close");
        assert!(pair.run(PASSES), "{what}");
        let stream = last_stream(&pair);
        assert!(pair.hosts[SERVER].held() > 0, "{what}: its FIN held");

        // Another refused stream fills the hold: the host stops polling.
        assert!(pair.refuse_data_stream(SERVER, None));
        let filler = group(&mut pair, &track, 1);
        for object in 0..64 {
            if pair.hosts[SERVER].held() >= host::OUT_SLOTS {
                break;
            }
            write(&mut pair, filler, object, "filler");
            assert!(pair.run(PASSES), "{what}");
        }
        assert_eq!(
            pair.hosts[SERVER].held(),
            host::OUT_SLOTS,
            "{what}: the hold is full"
        );
        assert_eq!(server_fins(&pair), 0, "{what}");

        let now = pair.now;
        pair.session(SERVER).close(0, "end", now).expect("a close");
        pair.lift_data_refusal(SERVER);
        assert!(pair.run(PASSES), "{what}");
        assert!(
            pair.hosts.iter().all(|h| h.fault().is_none()),
            "{what}: no fault"
        );
        let n = pair.hosts[SERVER].counters();
        assert_eq!(
            (n.fins_reported, n.fins_after_close),
            (0, 1),
            "{what}: reported into the closed session"
        );
        assert!(trace(&pair, stream).fin.is_some(), "{what}");
        assert_eq!(pair.wire.closed[SERVER], Some(0), "{what}");
        assert_eq!(pair.hosts[SERVER].held(), 0, "{what}");
        pair.drain(CLIENT);
        pair.drain(SERVER);
    }
    assert_eq!(outstanding(), 0);
}

// -- Early streams before setup (drafts 18 and 22) -----------------------------

/// The longest encoding of a stream type on drafts 18 and 22, whose
/// variable-length integers take one to nine bytes: the most the host reads
/// of a stream past its pending bound before the stream is classified.
const TYPE_PREFIX: usize = 9;

/// What another pair's server wrote on a data stream of `version`: a
/// subgroup header for a track alias no session of a new pair knows, and
/// one object. Longer than a stream type.
fn foreign_data_stream(version: Version) -> Vec<u8> {
    let mut pair = open(version, &Options::default());
    let track = subscribe(&mut pair, "foreign");
    let subgroup = group(&mut pair, &track, 0);
    write(&mut pair, subgroup, 0, "an early object");
    pair.wire.hold_input[CLIENT] = true;
    assert!(pair.run(PASSES));
    let stream = last_stream(&pair);
    let index = pair.hosts[SERVER]
        .wire_of_local(stream)
        .expect("its transport stream");
    let bytes: Vec<u8> = pair.wire.streams[index].dir[SERVER]
        .buf
        .iter()
        .copied()
        .collect();
    assert!(bytes.len() > TYPE_PREFIX, "longer than a stream type");
    bytes
}

/// Before the server reads anything, `MAX_REFUSED` early object streams
/// fill its pending bound (one cut inside its stream type), one more object
/// stream and a request stream arrive past it, and only then the client's
/// control stream, its type cut after the first byte. The control stream
/// is still found: setup completes with no violation, the two streams past
/// the bound are refused with EXCESSIVE_LOAD, the object stream after no
/// more than `TYPE_PREFIX` bytes of it were read, what the server holds stays
/// within the pending bound and the control stream's one byte, and every
/// early stream settles once setup is complete, leaving nothing held.
#[test]
fn early_streams_do_not_hide_the_control_stream() {
    for version in STREAM_PROFILES {
        let early = foreign_data_stream(version);
        assert!(early[0] < 0x80, "a one-byte stream type");
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = Pair::new(version, &options).expect("a pair");
            for _ in 0..host::MAX_REFUSED - 1 {
                assert!(pair.peer_opens_uni(SERVER, &early, PeerEnd::Fin));
            }
            // An incomplete type: `80` alone. These drafts' integers give
            // their length by their leading 1 bits, so `10xxxxxx` begins a
            // two-byte form, and `80` followed by a one-byte type is that
            // type again.
            assert!(pair.peer_opens_uni(SERVER, &[0x80], PeerEnd::None));
            let cut = pair.wire.streams.len() - 1;
            assert!(pair.peer_opens_uni(SERVER, &early, PeerEnd::Fin));
            let extra = pair.wire.streams.len() - 1;
            let request = pair
                .peer_opens_bidi(SERVER, b"early request")
                .expect("a request stream");

            // The client's first pump opens its control stream while the
            // server reads nothing; the server then sees only its first
            // byte.
            pair.wire.hold_input[SERVER] = true;
            assert!(pair.run(PASSES), "{what}");
            let control = (request + 1..pair.wire.streams.len())
                .find(|&i| pair.wire.streams[i].opener == CLIENT && !pair.wire.streams[i].bidi)
                .expect("the client's control stream");
            let rest: Vec<u8> = pair.wire.streams[control].dir[CLIENT]
                .buf
                .drain(1..)
                .collect();
            assert!(!rest.is_empty(), "{what}");
            pair.wire.hold_input[SERVER] = false;
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[SERVER].session.state(),
                SessionState::SetupSent,
                "{what}"
            );
            assert_eq!(
                pair.hosts[SERVER].pending_streams(),
                host::MAX_REFUSED,
                "{what}"
            );
            // Held: the pending streams (three whole, one a single byte)
            // and the first byte of the control stream's type.
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (
                    0,
                    host::MAX_REFUSED + 1,
                    (host::MAX_REFUSED - 1) * early.len() + 2
                ),
                "{what}"
            );
            // Of the object stream past the bound no more than the longest
            // stream type was read before it was refused.
            let read = pair.wire.streams[extra].dir[CLIENT].read_off;
            assert!(
                read > 0 && read <= TYPE_PREFIX as u64,
                "{what}: {read} bytes read of the stream past the bound"
            );

            // The rest of the control stream's type, and its SETUP.
            pair.wire.streams[control].dir[CLIENT].buf.extend(rest);
            assert!(pair.run(PASSES), "{what}");
            assert!(pair.established(), "{what}: the control stream was found");
            let n = pair.hosts[SERVER].counters().clone();
            assert_eq!(n.violations, 0, "{what}: no peer violation");
            assert_eq!(n.early_refused, 2, "{what}: the two streams past the bound");
            assert!(n.discoveries >= 2, "{what}");
            assert_eq!(
                pair.wire.streams[extra].dir[CLIENT].stop,
                Some(host::EXCESSIVE_LOAD),
                "{what}"
            );
            let refused = &pair.wire.streams[request].dir;
            assert_eq!(
                (refused[CLIENT].stop, refused[SERVER].reset),
                (Some(host::EXCESSIVE_LOAD), Some(host::EXCESSIVE_LOAD)),
                "{what}: the request stream refused, not left waiting"
            );

            // The cut stream's type completes after setup.
            let pipe = &mut pair.wire.streams[cut].dir[CLIENT];
            pipe.buf.extend(&early);
            pipe.taken += early.len() as u64;
            pipe.fin = true;
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(pair.hosts[SERVER].pending_streams(), 0, "{what}: settled");
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, 0, 0),
                "{what}: nothing held"
            );
            assert!(pair.established(), "{what}");
            assert_eq!(pair.hosts[SERVER].counters().violations, 0, "{what}");
            assert_eq!(pair.hosts[CLIENT].counters().violations, 0, "{what}");
            pair.drain(CLIENT);
            pair.drain(SERVER);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

/// The streams whose type the server cannot yet know, opened after the
/// pending bound is full and before the client's control stream.
#[derive(Clone, Copy, Debug)]
enum Untyped {
    /// One stream holding the first byte of a two-byte type.
    Partial,
    /// One stream that has sent nothing.
    Empty,
    /// More of both than the pending bound, and a control stream whose own
    /// type arrives in two pieces.
    Several,
}

/// `MAX_REFUSED` complete early object streams wait for setup. Then come
/// streams whose type is incomplete or absent, none of them ended, and last
/// the client's control stream. The control stream is found and setup
/// completes while every one of those types is still incomplete: no stream
/// waits for another to be classified. Nothing is refused and no violation
/// is raised; before setup the server holds the pending streams and at most
/// the bytes of each incomplete type; once those streams complete, every
/// stream settles and nothing is left held.
fn untyped_streams_do_not_hide_the_control_stream(case: Untyped) {
    let shapes: &[&[u8]] = match case {
        Untyped::Partial => &[&[0x80]],
        Untyped::Empty => &[&[]],
        Untyped::Several => &[&[0x80], &[], &[0x80], &[], &[0x80], &[]],
    };
    let partial = shapes.iter().filter(|bytes| !bytes.is_empty()).count();
    for version in STREAM_PROFILES {
        let early = foreign_data_stream(version);
        assert!(early[0] < 0x80, "a one-byte stream type");
        for (mode, options) in modes() {
            let what = format!("draft {} {mode} {case:?}", version.draft());
            let mut pair = Pair::new(version, &options).expect("a pair");
            for _ in 0..host::MAX_REFUSED {
                assert!(pair.peer_opens_uni(SERVER, &early, PeerEnd::Fin));
            }
            let mut untyped = Vec::new();
            for bytes in shapes {
                assert!(pair.peer_opens_uni(SERVER, bytes, PeerEnd::None));
                untyped.push(pair.wire.streams.len() - 1);
            }

            // The client's first pump opens its control stream while the
            // server reads nothing; its bytes are then kept back, so the
            // server meets every other stream first.
            pair.wire.hold_input[SERVER] = true;
            assert!(pair.run(PASSES), "{what}");
            let after = untyped[untyped.len() - 1] + 1;
            let control = (after..pair.wire.streams.len())
                .find(|&i| pair.wire.streams[i].opener == CLIENT && !pair.wire.streams[i].bidi)
                .expect("the client's control stream");
            let setup: Vec<u8> = pair.wire.streams[control].dir[CLIENT]
                .buf
                .drain(..)
                .collect();
            assert!(setup.len() > 1, "{what}: a type and a SETUP");
            pair.wire.hold_input[SERVER] = false;
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[SERVER].session.state(),
                SessionState::SetupSent,
                "{what}"
            );
            assert_eq!(
                pair.hosts[SERVER].pending_streams(),
                host::MAX_REFUSED,
                "{what}: the early object streams wait for setup"
            );
            // Held: the pending streams, whole, and one byte of each
            // incomplete type.
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (
                    0,
                    host::MAX_REFUSED + partial,
                    host::MAX_REFUSED * early.len() + partial
                ),
                "{what}"
            );

            // The control stream's bytes arrive: whole, or its type's first
            // byte and then the rest. No other stream has changed.
            if let Untyped::Several = case {
                pair.wire.streams[control].dir[CLIENT]
                    .buf
                    .push_back(setup[0]);
                assert!(pair.run(PASSES), "{what}");
                assert_eq!(
                    pair.hosts[SERVER].session.state(),
                    SessionState::SetupSent,
                    "{what}"
                );
                pair.wire.streams[control].dir[CLIENT]
                    .buf
                    .extend(&setup[1..]);
            } else {
                pair.wire.streams[control].dir[CLIENT].buf.extend(&setup);
            }
            assert!(pair.run(PASSES), "{what}");
            assert!(
                pair.established(),
                "{what}: the control stream was found behind incomplete types"
            );
            let n = pair.hosts[SERVER].counters().clone();
            assert_eq!(n.violations, 0, "{what}: no peer violation");
            assert_eq!(n.early_refused, 0, "{what}: nothing refused");
            assert_eq!(pair.hosts[CLIENT].counters().violations, 0, "{what}");
            // Every incomplete type is still incomplete, read as far as it
            // goes and held; the early object streams settled with setup.
            for (&index, bytes) in untyped.iter().zip(shapes) {
                let pipe = &pair.wire.streams[index].dir[CLIENT];
                assert_eq!(
                    (pipe.stop, pipe.read_off, pipe.fin),
                    (None, bytes.len() as u64, false),
                    "{what}"
                );
            }
            assert_eq!(pair.hosts[SERVER].pending_streams(), 0, "{what}");
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, partial, partial),
                "{what}: only the incomplete types are held"
            );

            // The unrelated streams complete: each is the early object
            // stream, the partial ones with its type in the two-byte form.
            for &index in &untyped {
                let pipe = &mut pair.wire.streams[index].dir[CLIENT];
                pipe.buf.extend(&early);
                pipe.taken += early.len() as u64;
                pipe.fin = true;
            }
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(pair.hosts[SERVER].pending_streams(), 0, "{what}: settled");
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, 0, 0),
                "{what}: nothing held"
            );
            assert!(pair.established(), "{what}");
            assert_eq!(pair.hosts[SERVER].counters().violations, 0, "{what}");
            assert_eq!(pair.hosts[CLIENT].counters().violations, 0, "{what}");
            pair.drain(CLIENT);
            pair.drain(SERVER);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

#[test]
fn a_partial_early_type_does_not_hide_the_control_stream() {
    untyped_streams_do_not_hide_the_control_stream(Untyped::Partial);
}

#[test]
fn an_empty_early_stream_does_not_hide_the_control_stream() {
    untyped_streams_do_not_hide_the_control_stream(Untyped::Empty);
}

#[test]
fn several_untyped_early_streams_do_not_hide_the_control_stream() {
    untyped_streams_do_not_hide_the_control_stream(Untyped::Several);
}

/// A stream whose type was still incomplete when setup completed stays
/// apart from the pending streams afterwards. When its type completes
/// while `MAX_REFUSED` other streams are pending, no more than
/// `TYPE_PREFIX` bytes of it are read, it is fed nothing, and it waits: the
/// pending bound holds.
/// Once the pending streams settle it joins them and settles too.
#[test]
fn a_stream_typed_after_setup_waits_with_its_prefix() {
    for version in STREAM_PROFILES {
        let early = foreign_data_stream(version);
        assert!(early[0] < 0x80, "a one-byte stream type");
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = Pair::new(version, &options).expect("a pair");
            let finish = |pair: &mut Pair, index: usize| {
                let pipe = &mut pair.wire.streams[index].dir[CLIENT];
                pipe.buf.extend(&early);
                pipe.taken += early.len() as u64;
                pipe.fin = true;
            };

            // The server classifies the client's control stream but has not
            // the whole SETUP yet: its last byte is kept back.
            pair.wire.hold_input[SERVER] = true;
            assert!(pair.run(PASSES), "{what}");
            let control = (0..pair.wire.streams.len())
                .find(|&i| pair.wire.streams[i].opener == CLIENT && !pair.wire.streams[i].bidi)
                .expect("the client's control stream");
            let last = pair.wire.streams[control].dir[CLIENT]
                .buf
                .pop_back()
                .expect("a SETUP");
            pair.wire.hold_input[SERVER] = false;
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[SERVER].session.state(),
                SessionState::SetupSent,
                "{what}"
            );

            // Early object streams fill the pending bound; one more stream
            // shows the first byte of its type.
            for _ in 0..host::MAX_REFUSED {
                assert!(pair.peer_opens_uni(SERVER, &early, PeerEnd::Fin));
            }
            assert!(pair.peer_opens_uni(SERVER, &[0x80], PeerEnd::None));
            let late = pair.wire.streams.len() - 1;
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (
                    0,
                    host::MAX_REFUSED + 1,
                    host::MAX_REFUSED * early.len() + 1
                ),
                "{what}"
            );

            // Setup completes; the early streams settle; the one byte stays.
            pair.wire.streams[control].dir[CLIENT].buf.push_back(last);
            assert!(pair.run(PASSES), "{what}");
            assert!(pair.established(), "{what}");
            assert_eq!(pair.hosts[SERVER].held_reads(), (0, 1, 1), "{what}");

            // Established: incomplete types fill the pending bound.
            let mut pending = Vec::new();
            for _ in 0..host::MAX_REFUSED {
                assert!(pair.peer_opens_uni(SERVER, &[0x80], PeerEnd::None));
                pending.push(pair.wire.streams.len() - 1);
            }
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                pair.hosts[SERVER].pending_streams(),
                host::MAX_REFUSED,
                "{what}"
            );
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, host::MAX_REFUSED + 1, host::MAX_REFUSED + 1),
                "{what}"
            );

            // The first stream's type completes while they are pending.
            let deferred = pair.hosts[SERVER].counters().admissions_deferred;
            finish(&mut pair, late);
            assert!(pair.run(PASSES), "{what}");
            let read = pair.wire.streams[late].dir[CLIENT].read_off;
            assert_eq!(
                read, TYPE_PREFIX as u64,
                "{what}: `TYPE_PREFIX` bytes read and no more"
            );
            assert_eq!(
                pair.hosts[SERVER].pending_streams(),
                host::MAX_REFUSED,
                "{what}: the pending bound holds"
            );
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, host::MAX_REFUSED + 1, host::MAX_REFUSED + TYPE_PREFIX),
                "{what}: its prefix is all it holds"
            );
            assert!(
                pair.hosts[SERVER].counters().admissions_deferred > deferred,
                "{what}: it waits"
            );

            // The pending streams complete and settle; the waiting stream
            // joins them and settles.
            for &index in &pending {
                finish(&mut pair, index);
            }
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(pair.hosts[SERVER].pending_streams(), 0, "{what}: settled");
            assert_eq!(
                pair.hosts[SERVER].held_reads(),
                (0, 0, 0),
                "{what}: nothing held"
            );
            assert_eq!(
                pair.wire.streams[late].dir[CLIENT].read_off,
                1 + early.len() as u64,
                "{what}: read to its end"
            );
            assert!(pair.established(), "{what}");
            assert_eq!(pair.hosts[SERVER].counters().violations, 0, "{what}");
            assert_eq!(pair.hosts[CLIENT].counters().violations, 0, "{what}");
            pair.drain(CLIENT);
            pair.drain(SERVER);
            close(pair);
        }
    }
    assert_eq!(outstanding(), 0);
}

// -- A session moves between threads ---------------------------------------

/// The pair, both sessions inside it, is handed to another thread in the
/// middle of an exchange: a subscription established and its first object
/// delivered on the first thread, the second object written, delivered and
/// the subgroup closed on the second, the close on a third. Nothing is lent
/// across a handoff (every polled record was dropped by `drain`), each
/// `join` is the happens-before edge, and every count the hosts keep is the
/// same as on one thread.
#[test]
fn a_live_session_moves_to_another_thread_mid_exchange() {
    for version in VERSIONS {
        for (mode, options) in modes() {
            let what = format!("draft {} {mode}", version.draft());
            let mut pair = open(version, &options);
            let track = subscribe(&mut pair, "moved");
            let sg = group(&mut pair, &track, 0);
            write(&mut pair, sg, 0, "on the first thread");
            assert!(pair.run(PASSES), "{what}");
            assert_eq!(
                objects(&pair.drain(CLIENT)),
                [(0, 0, b"on the first thread".to_vec())],
                "{what}"
            );

            let what2 = what.clone();
            let (mut pair, delivered) = std::thread::spawn(move || {
                let mut pair = pair;
                write(&mut pair, sg, 1, "on the second thread");
                let now = pair.now;
                pair.session(SERVER).close_subgroup(sg, now).expect("close");
                assert!(pair.run(PASSES), "{what2}");
                let c = pair.drain(CLIENT);
                let delivered = objects(&c);
                assert_eq!(count(&c, EventKind::SubgroupFinished), 1, "{what2}");
                (pair, delivered)
            })
            .join()
            .expect("the second thread");
            assert_eq!(
                delivered,
                [(0, 1, b"on the second thread".to_vec())],
                "{what}"
            );
            assert!(pair.established(), "{what}");

            std::thread::spawn(move || {
                let s = pair.drain(SERVER);
                assert_eq!(count(&s, EventKind::OutgoingSubgroupTerminated), 0);
                close(pair);
            })
            .join()
            .expect("the third thread");
        }
    }
    assert_eq!(outstanding(), 0);
}
