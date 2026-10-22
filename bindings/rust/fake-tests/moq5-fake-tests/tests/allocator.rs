//! Allocation and release balance: an endpoint built through `sys` with a
//! counting allocator and adopted by `Endpoint::from_raw`, a sender and a
//! receiver attached to it, buffers written and dropped; after teardown no
//! allocation is outstanding and every buffer release ran once.
//!
//! The counting allocator is an unsafe test fixture meeting the service
//! tier's thread-safety requirement for allocators; no safe allocator API
//! is offered.

use std::alloc::{alloc_zeroed, dealloc, realloc, Layout};
use std::ffi::c_void;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

use moq5::{
    Buffer, Endpoint, MediaType, Namespace, Packaging, Receiver, ReceiverConfig, SendObject,
    SendTrackConfig, Sender, SenderConfig,
};
use moq5_fake_tests as fake;
use moq5_fake_tests::sys;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static BYTES: AtomicIsize = AtomicIsize::new(0);
static RELEASES: AtomicUsize = AtomicUsize::new(0);

fn layout(size: usize) -> Layout {
    Layout::from_size_align(size.max(1), 16)
        .expect("a non-zero size aligned to 16 is a valid layout")
}

unsafe extern "C" fn count_alloc(size: usize, _ctx: *mut c_void) -> *mut c_void {
    // SAFETY: a non-zero layout.
    let p = unsafe { alloc_zeroed(layout(size)) };
    if !p.is_null() {
        LIVE.fetch_add(1, Ordering::SeqCst);
        BYTES.fetch_add(size as isize, Ordering::SeqCst);
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
    let p = unsafe { realloc(ptr as *mut u8, layout(old_size), new_size.max(1)) };
    if !p.is_null() {
        BYTES.fetch_add(new_size as isize - old_size as isize, Ordering::SeqCst);
    }
    p as *mut c_void
}

unsafe extern "C" fn count_free(ptr: *mut c_void, size: usize, _ctx: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: ptr came from count_alloc/count_realloc with this size.
    unsafe { dealloc(ptr as *mut u8, layout(size)) };
    LIVE.fetch_sub(1, Ordering::SeqCst);
    BYTES.fetch_sub(size as isize, Ordering::SeqCst);
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

unsafe extern "C" fn count_release(_ctx: *mut c_void, _data: *const u8, _len: usize) {
    RELEASES.fetch_add(1, Ordering::SeqCst);
}

static FRAME: [u8; 3] = [7, 8, 9];

fn counted_buffer() -> Buffer {
    // SAFETY: static bytes; the release only counts.
    unsafe { Buffer::wrap_raw(FRAME.as_ptr(), 3, Some(count_release), std::ptr::null_mut()) }
        .expect("a static frame wraps")
}

#[test]
fn teardown_leaves_no_allocation_and_releases_every_buffer_once() {
    let s = fake::suite();
    LIVE.store(0, Ordering::SeqCst);
    BYTES.store(0, Ordering::SeqCst);
    RELEASES.store(0, Ordering::SeqCst);

    let url = b"moqt://relay:4433";
    let mut config = sys::moq_endpoint_cfg_t::default();
    let mut raw = std::ptr::null_mut();
    // SAFETY: a sized, zeroed configuration whose pointers outlive the call;
    // the allocator is a static that meets the thread-safety contract.
    unsafe {
        sys::moq_endpoint_cfg_init_sized(&mut config, std::mem::size_of_val(&config));
        config.url = sys::moq_bytes_t {
            data: url.as_ptr(),
            len: url.len(),
        };
        config.alloc = &ALLOC.0;
        assert_eq!(sys::moq_endpoint_connect(&config, &mut raw), sys::MOQ_OK);
    }
    assert!(
        LIVE.load(Ordering::SeqCst) >= 1,
        "the endpoint came from the allocator"
    );
    // SAFETY: a live endpoint nothing else owns, built with no callbacks.
    let endpoint = unsafe { Endpoint::from_raw(raw) };

    let sender = Sender::attach(
        &endpoint,
        &SenderConfig::live(Namespace::parse("ns").expect("a one-part namespace parses")),
    )
    .expect("the provider accepts the sender attach");
    let receiver = Receiver::attach(
        &endpoint,
        &ReceiverConfig::live(Namespace::parse("ns").expect("a one-part namespace parses")),
    )
    .expect("the provider accepts the receiver attach");
    let live_with_attachments = LIVE.load(Ordering::SeqCst);
    assert!(live_with_attachments >= 3, "{live_with_attachments}");
    let track = sender
        .add_track(&SendTrackConfig::new("v", MediaType::Video, Packaging::Raw, "c").bitrate(1))
        .expect("the provider accepts the track");
    sender
        .write(track, SendObject::new(counted_buffer()))
        .expect("the provider accepts the write");
    s.set("write_result", i64::from(sys::MOQ_ERR_WOULD_BLOCK));
    let failure = sender
        .write(track, SendObject::new(counted_buffer()))
        .unwrap_err();
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "only the transferred one so far"
    );
    drop(failure);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 2);
    let dropped = counted_buffer();
    drop(dropped);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 3);

    drop(receiver);
    drop(sender);
    drop(endpoint);
    assert_eq!(s.get("destroy"), 1);
    assert_eq!(s.get("alloc_live"), 0, "the provider's own count");
    assert_eq!(LIVE.load(Ordering::SeqCst), 0, "outstanding allocations");
    assert_eq!(BYTES.load(Ordering::SeqCst), 0, "outstanding bytes");
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        3,
        "every release ran exactly once"
    );
}
