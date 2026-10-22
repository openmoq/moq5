//! Relay row: teardown of a receiver, a sender and their endpoints leaves
//! no thread of the service behind and a balanced allocator.
//!
//! The endpoints are built through `sys` with a counting allocator (an
//! unsafe fixture meeting the service tier's thread-safety requirement) and
//! adopted with `Endpoint::from_raw`, so every allocation the service made
//! for them is counted.

mod live;

use std::alloc::{alloc_zeroed, dealloc, realloc, Layout};
use std::ffi::c_void;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

use moq5::sys;
use moq5::{
    Buffer, Endpoint, EndpointState, MediaType, Namespace, Packaging, Receiver, ReceiverConfig,
    SendObject, SendTrackConfig, Sender, SenderConfig, TrackEventKind, VersionOffer,
};

static LIVE: AtomicIsize = AtomicIsize::new(0);

fn layout(size: usize) -> Layout {
    Layout::from_size_align(size.max(1), 16).expect("a non-zero size aligned to 16 is a layout")
}

unsafe extern "C" fn count_alloc(size: usize, _ctx: *mut c_void) -> *mut c_void {
    // SAFETY: a non-zero layout.
    let p = unsafe { alloc_zeroed(layout(size)) };
    if !p.is_null() {
        LIVE.fetch_add(1, Ordering::SeqCst);
    }
    p as *mut c_void
}

unsafe extern "C" fn count_realloc(
    ptr: *mut c_void,
    old_size: usize,
    new_size: usize,
    ctx: *mut c_void,
) -> *mut c_void {
    if ptr.is_null() {
        // SAFETY: as count_alloc.
        return unsafe { count_alloc(new_size, ctx) };
    }
    // SAFETY: ptr came from count_alloc with old_size.
    unsafe { realloc(ptr as *mut u8, layout(old_size), new_size.max(1)) as *mut c_void }
}

unsafe extern "C" fn count_free(ptr: *mut c_void, size: usize, _ctx: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: ptr came from count_alloc/count_realloc with this size.
    unsafe { dealloc(ptr as *mut u8, layout(size)) };
    LIVE.fetch_sub(1, Ordering::SeqCst);
}

/// A `moq_alloc_t` in a static: its `ctx` is a raw pointer, which `Sync`
/// does not cover; it is NULL and never read.
struct StaticAlloc(sys::moq_alloc_t);

// SAFETY: the table's ctx is NULL and the functions are thread-safe.
unsafe impl Sync for StaticAlloc {}

static ALLOC: StaticAlloc = StaticAlloc(sys::moq_alloc_t {
    ctx: std::ptr::null_mut(),
    alloc: Some(count_alloc),
    realloc: Some(count_realloc),
    free: Some(count_free),
});

/// An endpoint on the counting allocator, built through `sys` and adopted.
fn counted_endpoint(relay: &live::Relay) -> Endpoint {
    let url = format!("moqt://127.0.0.1:{}", relay.port);
    let sni = b"localhost";
    let ca = relay
        .cert
        .to_str()
        .expect("the certificate path is UTF-8")
        .as_bytes();
    let versions = [live::draft().draft()];
    let mut cfg = sys::moq_endpoint_cfg_t::default();
    let mut raw = std::ptr::null_mut();
    // SAFETY: a sized configuration whose pointers outlive the call; the
    // allocator is a static meeting the thread-safety contract; no callback
    // is installed, so the crate's invariant holds for the adopted endpoint.
    unsafe {
        sys::moq_endpoint_cfg_init_sized(&mut cfg, std::mem::size_of_val(&cfg));
        cfg.url = sys::moq_bytes_t {
            data: url.as_ptr(),
            len: url.len(),
        };
        cfg.sni = sys::moq_bytes_t {
            data: sni.as_ptr(),
            len: sni.len(),
        };
        cfg.ca_file = sys::moq_bytes_t {
            data: ca.as_ptr(),
            len: ca.len(),
        };
        cfg.versions.struct_size = std::mem::size_of_val(&cfg.versions) as u32;
        cfg.versions.policy = sys::MOQ_VERSION_POLICY_EXACT;
        cfg.versions.versions = versions.as_ptr();
        cfg.versions.version_count = 1;
        cfg.alloc = &ALLOC.0;
        assert_eq!(sys::moq_endpoint_connect(&cfg, &mut raw), sys::MOQ_OK);
        Endpoint::from_raw(raw)
    }
}

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn teardown_leaves_no_thread_and_a_balanced_allocator() {
    let mut relay = live::Relay::start("teardown", 14490);
    let _ = VersionOffer::Auto;
    let threads_before = live::thread_count();
    LIVE.store(0, Ordering::SeqCst);

    let namespace = Namespace::parse("live/rust-teardown").expect("a two-part namespace parses");
    let sender_endpoint = counted_endpoint(&relay);
    let receiver_endpoint = counted_endpoint(&relay);
    for endpoint in [&sender_endpoint, &receiver_endpoint] {
        let started = Instant::now();
        while endpoint.state() != EndpointState::Established {
            endpoint
                .wait(live::STEP)
                .unwrap_or_else(|e| panic!("{e}: {:?}", endpoint.terminal()));
            assert!(
                started.elapsed() < live::DEADLINE,
                "{:?}",
                endpoint.terminal()
            );
        }
    }
    assert!(
        LIVE.load(Ordering::SeqCst) > 0,
        "the service allocated through the fixture"
    );

    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to the counted endpoint");
    let video = sender
        .add_track(
            &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(1),
        )
        .expect("a raw video track is added");
    live::ready(&sender);
    let receiver = Receiver::attach(
        &receiver_endpoint,
        &ReceiverConfig::live(namespace).auto_subscribe(true),
    )
    .expect("the receiver attaches to the counted endpoint");
    let mut seen = Vec::new();
    live::until_event(&receiver, &mut seen, |k| k == TrackEventKind::CatalogReady);
    let mut object =
        SendObject::new(Buffer::from_vec(vec![9u8; 100]).expect("a 100-byte vector is adopted"));
    object.is_sync = true;
    object.starts_group = true;
    sender
        .write(video, object)
        .expect("the first object is accepted");
    let started = Instant::now();
    loop {
        if let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            assert_eq!(object.payload(), [9u8; 100]);
            break;
        }
        receiver
            .wait(live::STEP)
            .expect("the receiver's wait is not interrupted");
        assert!(
            started.elapsed() < live::DEADLINE,
            "the object did not arrive"
        );
    }

    drop(receiver);
    drop(sender);
    drop(receiver_endpoint);
    drop(sender_endpoint);
    relay.stop();

    // Threads of a joined endpoint are gone when stop() returns; give any
    // detached helper of the transport a moment, then insist.
    let started = Instant::now();
    while live::thread_count() > threads_before && started.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(live::STEP);
    }
    assert_eq!(
        live::thread_count(),
        threads_before,
        "service threads remain"
    );
    assert_eq!(LIVE.load(Ordering::SeqCst), 0, "outstanding allocations");
}
