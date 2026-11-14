//! A transport host over the safe session API: two sessions joined by an
//! in-memory transport, on drafts 16, 18 and 22.
//!
//! It runs setup, a subscription with objects and its end, and a fetch,
//! each side in the tagged host mode, with every transport write taking at
//! most a few bytes so partial writes and held output are exercised. The
//! host itself is `host.rs`; the transport is `wire.rs`. Pass a seed as the
//! first argument to fragment every write and read with it instead.
//!
//! ```text
//! cargo run -p moq5-core --example transport_host [seed]
//! ```

use std::process::ExitCode;

use moq5_core::{
    AcceptFetch, AcceptSubscribe, Buffer, EventKind, FetchConfig, FetchObjectWrite, Location,
    LocationFilter, Namespace, SubgroupConfig, SubscribeConfig, TrackDoneConfig, Version,
};

// The host and the transport are shared with the host tests, which use
// parts of them this example does not.
#[allow(dead_code)]
mod host;
#[allow(dead_code)]
mod wire;

use host::{Options, Pair, Seen, CLIENT, SERVER};

const PASSES: u32 = 4096;

fn run(version: Version, seed: Option<u64>) -> Result<(), String> {
    let options = Options {
        tagged: [true, true],
        write_limit: if seed.is_some() { 0 } else { 7 },
        seed,
        ..Options::default()
    };
    let mut pair = Pair::new(version, &options).map_err(|e| format!("pair: {e}"))?;
    if !pair.run(PASSES) || !pair.established() {
        return Err("setup did not complete".into());
    }
    pair.drain(CLIENT);
    pair.drain(SERVER);
    let now = pair.now;

    // Subscribe; the server accepts.
    let namespace = Namespace::parse("live").map_err(|e| e.to_string())?;
    let config = SubscribeConfig::new(
        namespace.clone(),
        "video",
        LocationFilter::AbsoluteStart(Location::default()),
    );
    let csub = pair
        .session(CLIENT)
        .subscribe(&config, now)
        .map_err(|e| format!("subscribe: {e}"))?;
    pair.run(PASSES);
    let request = find(&pair.drain(SERVER), EventKind::SubscribeRequest)?;
    let ssub = request.subscription.ok_or("no subscription")?;
    let now = pair.now;
    pair.session(SERVER)
        .accept_subscribe(ssub, &AcceptSubscribe::default(), now)
        .map_err(|e| format!("accept: {e}"))?;
    pair.run(PASSES);
    find(&pair.drain(CLIENT), EventKind::SubscribeOk)?;

    // Three objects on one subgroup, then the subscription's end.
    let now = pair.now;
    let server = pair.session(SERVER);
    let group = server
        .open_subgroup(ssub, &SubgroupConfig::new(0), now)
        .map_err(|e| format!("subgroup: {e}"))?;
    for (id, text) in ["first object", "second", "third and last"]
        .iter()
        .enumerate()
    {
        let payload = Buffer::copy_from(text.as_bytes()).map_err(|e| e.to_string())?;
        server
            .write_object(group, id as u64, payload, now)
            .map_err(|e| format!("write: {e}"))?;
    }
    server
        .close_subgroup(group, now)
        .map_err(|e| format!("close subgroup: {e}"))?;
    // In the tagged mode the end waits until the host has reported the
    // stream's open, so it may have to be retried after a run.
    let done = TrackDoneConfig::new(0).stream_count(1).reason("done");
    retry(&mut pair, |session, now| {
        session.done_subscribe(ssub, &done, now)
    })
    .map_err(|e| format!("done: {e}"))?;
    pair.run(PASSES);
    let seen = pair.drain(CLIENT);
    let objects: Vec<&Seen> = seen
        .iter()
        .filter(|s| s.kind == Some(EventKind::ObjectReceived))
        .collect();
    if objects.len() != 3 || objects[2].payload != b"third and last" {
        return Err(format!("objects: {objects:?}"));
    }
    if objects.iter().any(|s| s.subscription != Some(csub)) {
        return Err("an object of another subscription".into());
    }
    find(&seen, EventKind::SubscribeDone)?;

    // A fetch of two objects.
    let now = pair.now;
    let fetch = FetchConfig::new(namespace, "past", Location::default(), 0, 3);
    let cfetch = pair
        .session(CLIENT)
        .fetch(&fetch, now)
        .map_err(|e| e.to_string())?;
    pair.run(PASSES);
    let request = find(&pair.drain(SERVER), EventKind::FetchRequest)?;
    let sfetch = request.fetch.ok_or("no fetch")?;
    let now = pair.now;
    let server = pair.session(SERVER);
    server
        .accept_fetch(sfetch, &AcceptFetch::new(0, 3), now)
        .map_err(|e| e.to_string())?;
    for id in 0..2u64 {
        let payload =
            Buffer::copy_from(format!("fetched {id}").as_bytes()).map_err(|e| e.to_string())?;
        let mut object = FetchObjectWrite::new(0, id, payload);
        object.publisher_priority = 100;
        server
            .write_fetch_object(sfetch, object, now)
            .map_err(|e| e.to_string())?;
    }
    retry(&mut pair, |session, now| session.end_fetch(sfetch, now))
        .map_err(|e| format!("end fetch: {e}"))?;
    pair.run(PASSES);
    let seen = pair.drain(CLIENT);
    let fetched = seen
        .iter()
        .filter(|s| s.kind == Some(EventKind::FetchObject) && s.fetch == Some(cfetch))
        .count();
    if fetched != 2 {
        return Err(format!("fetched {fetched} objects"));
    }
    find(&seen, EventKind::FetchComplete)?;

    // Close; nothing held is left behind.
    let now = pair.now;
    pair.session(CLIENT)
        .close(0, "bye", now)
        .map_err(|e| e.to_string())?;
    pair.run(PASSES);
    pair.drain(CLIENT);
    pair.drain(SERVER);
    let partial = pair.hosts[SERVER].counters().partial_writes;
    if pair.hosts.iter().any(|h| h.fault().is_some()) {
        return Err("a side faulted".into());
    }
    if pair.hosts.iter().any(|h| h.held() != 0) {
        return Err("output still held after close".into());
    }
    drop(pair);
    println!(
        "draft {}: ok ({partial} partial writes on the server)",
        version.draft()
    );
    Ok(())
}

/// Call `op` on the server, running the pair between attempts while it
/// would block.
fn retry(
    pair: &mut Pair,
    mut op: impl FnMut(&mut moq5_core::Session, moq5_core::Instant) -> moq5_core::Result<()>,
) -> moq5_core::Result<()> {
    for _ in 0..64 {
        let now = pair.now;
        match op(pair.session(SERVER), now) {
            Err(moq5_core::Error::WOULD_BLOCK) => {
                pair.run(PASSES);
            }
            other => return other,
        }
    }
    Err(moq5_core::Error::WOULD_BLOCK)
}

fn find(seen: &[Seen], kind: EventKind) -> Result<Seen, String> {
    seen.iter()
        .find(|s| s.kind == Some(kind))
        .cloned()
        .ok_or_else(|| format!("no {kind:?} in {seen:?}"))
}

fn main() -> ExitCode {
    let seed = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    if let Some(seed) = seed {
        println!("seed {seed}");
    }
    let mut failed = false;
    for version in [Version::DRAFT_16, Version::DRAFT_18, Version::DRAFT_22] {
        if let Err(error) = run(version, seed) {
            eprintln!("draft {}: FAILED: {error}", version.draft());
            failed = true;
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
