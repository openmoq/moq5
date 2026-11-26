//! The LOCMAF decoder's and encoder's storage, observed through a counting
//! global allocator: each owner allocates its storage once, with the size
//! the library asks for and its storage alignment, and frees it once with
//! that same layout when it is dropped. The state and its storage move
//! with their owner without relocating, so a chain survives a move into a
//! container and out again, and a move to another thread. A copied
//! decoder continues from the same point with storage of its own.
//!
//! The allocator watches allocations of exactly the storage layout under
//! test; the rows of this binary run one at a time, so no other row's
//! owner is counted.

mod locmaf_support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use locmaf_support::{canonical, track, DELTA, DELTA_PAYLOAD, FULL, FULL_PAYLOAD};
use moq5_media::locmaf::{write_cmaf, Decoder, Encoder, Limits, ObjectKind, Track};
use moq5_media::sys;

/// The storage alignment the library requires.
const ALIGN: usize = sys::MOQ_LOCMAF_STORAGE_ALIGN as usize;
/// Blocks tracked at once.
const SLOTS: usize = 8;

/// The size of the layout watched; zero watches nothing.
static WATCHED: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);
/// Watched blocks freed with a layout other than the one they had.
static FREED_MISMATCHED: AtomicUsize = AtomicUsize::new(0);
/// The addresses of the watched blocks still allocated (zero: free slot).
static LIVE: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];

struct Counting;

// SAFETY: every request is forwarded unchanged to the system allocator;
// the bookkeeping only reads layouts and stores integers in atomics, and
// never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract, forwarded.
        let block = unsafe { System.alloc(layout) };
        let watched = WATCHED.load(Ordering::SeqCst);
        if !block.is_null() && watched != 0 && layout.size() == watched && layout.align() == ALIGN {
            ALLOCATED.fetch_add(1, Ordering::SeqCst);
            let address = block as usize;
            // A slot left unclaimed only loses the layout check of a block
            // beyond SLOTS, which no row allocates.
            let _ = LIVE.iter().find(|slot| {
                slot.compare_exchange(0, address, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            });
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        let address = block as usize;
        if let Some(slot) = LIVE
            .iter()
            .find(|slot| slot.load(Ordering::SeqCst) == address)
        {
            slot.store(0, Ordering::SeqCst);
            if layout.size() == WATCHED.load(Ordering::SeqCst) && layout.align() == ALIGN {
                FREED.fetch_add(1, Ordering::SeqCst);
            } else {
                FREED_MISMATCHED.fetch_add(1, Ordering::SeqCst);
            }
        }
        // SAFETY: the caller's contract, forwarded.
        unsafe { System.dealloc(block, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

static SERIAL: Mutex<()> = Mutex::new(());

/// One row at a time, watching `size`; the counters start at zero.
struct Watch {
    _serial: MutexGuard<'static, ()>,
}

impl Watch {
    fn new(size: usize) -> Watch {
        // The lock guards no data: a row that panicked holding it left
        // nothing behind, so the poison is cleared and the lock taken again.
        let serial = loop {
            match SERIAL.lock() {
                Ok(guard) => break guard,
                Err(poisoned) => {
                    drop(poisoned);
                    SERIAL.clear_poison();
                }
            }
        };
        for slot in &LIVE {
            slot.store(0, Ordering::SeqCst);
        }
        ALLOCATED.store(0, Ordering::SeqCst);
        FREED.store(0, Ordering::SeqCst);
        FREED_MISMATCHED.store(0, Ordering::SeqCst);
        WATCHED.store(size, Ordering::SeqCst);
        Watch { _serial: serial }
    }

    /// (allocated, freed, freed with another layout).
    fn counts(&self) -> (usize, usize, usize) {
        (
            ALLOCATED.load(Ordering::SeqCst),
            FREED.load(Ordering::SeqCst),
            FREED_MISMATCHED.load(Ordering::SeqCst),
        )
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        WATCHED.store(0, Ordering::SeqCst);
    }
}

/// The storage the library sizes for a decoder or an encoder of `track`
/// with the default limits.
fn storage_size(track: &Track, encoder: bool) -> usize {
    let mut bytes = 0usize;
    let code = if encoder {
        let mut cfg = sys::moq_locmaf_encoder_cfg_t::default();
        // SAFETY: a writable cfg of the size passed; the track is borrowed
        // for the call.
        unsafe {
            sys::moq_locmaf_encoder_cfg_init_sized(&mut cfg, std::mem::size_of_val(&cfg));
            cfg.track = track.raw();
            sys::moq_locmaf_encoder_storage_size(&cfg, &mut bytes)
        }
    } else {
        let mut cfg = sys::moq_locmaf_decoder_cfg_t::default();
        // SAFETY: as above.
        unsafe {
            sys::moq_locmaf_decoder_cfg_init_sized(&mut cfg, std::mem::size_of_val(&cfg));
            cfg.track = track.raw();
            sys::moq_locmaf_decoder_storage_size(&cfg, &mut bytes)
        }
    };
    assert_eq!(code, sys::MOQ_OK);
    assert!(bytes > 0, "the defaults need storage");
    bytes
}

fn decoder(track: &Track) -> Decoder {
    Decoder::new(track, Limits::default()).expect("a decoder with defaults")
}

fn encoder(track: &Track) -> Encoder {
    Encoder::new(track, Limits::default()).expect("an encoder with defaults")
}

/// The canonical bytes of the delta at (0, `object`), whose BMDT is
/// `bmdt`, pushed on `decoder`.
fn push_delta(track: &Track, decoder: &mut Decoder, object: u64, bmdt: u64) -> Vec<u8> {
    let chunk = decoder
        .push(0, object, &DELTA)
        .expect("the delta continues the chain");
    assert_eq!(chunk.kind(), ObjectKind::Delta);
    assert_eq!(chunk.base_media_decode_time(), bmdt);
    write_cmaf(track, &chunk).expect("a decoded chunk is consistent")
}

#[test]
fn decoder_drop_frees_storage_once() {
    let track = track();
    let watch = Watch::new(storage_size(&track, false));
    let mut decoder = decoder(&track);
    assert_eq!(watch.counts(), (1, 0, 0), "one storage allocation");
    decoder.push(0, 0, &FULL).expect("the full header decodes");
    drop(decoder);
    assert_eq!(watch.counts(), (1, 1, 0), "freed once, with its own layout");
}

#[test]
fn decoder_move_keeps_state() {
    let track = track();
    let watch = Watch::new(storage_size(&track, false));
    let mut decoder = decoder(&track);
    decoder.push(0, 0, &FULL).expect("the full header decodes");
    let mut held = Vec::new();
    held.push(decoder);
    held.push(self::decoder(&track));
    let mut decoder = held.swap_remove(0);
    assert_eq!(
        push_delta(&track, &mut decoder, 1, 1024),
        canonical(1024, &DELTA_PAYLOAD),
        "the chain survived the moves"
    );
    drop(held);
    assert_eq!(watch.counts(), (2, 1, 0), "nothing reallocated");
    drop(decoder);
    assert_eq!(watch.counts(), (2, 2, 0));
}

#[test]
fn decoder_send() {
    let track = track();
    let watch = Watch::new(storage_size(&track, false));
    let mut decoder = decoder(&track);
    decoder.push(0, 0, &FULL).expect("the full header decodes");
    let moved = track;
    let continued = std::thread::spawn(move || {
        let bytes = push_delta(&moved, &mut decoder, 1, 1024);
        drop(decoder);
        bytes
    })
    .join()
    .expect("the other thread decodes");
    assert_eq!(continued, canonical(1024, &DELTA_PAYLOAD));
    assert_eq!(watch.counts(), (1, 1, 0), "freed once, on the other thread");
}

#[test]
fn copy_from_independent() {
    let track = track();
    let watch = Watch::new(storage_size(&track, false));
    let mut first = decoder(&track);
    first.push(0, 0, &FULL).expect("the full header decodes");
    let mut second = decoder(&track);
    second.copy_from(&first).expect("same track, same limits");
    assert!(
        second.expects(0, 1),
        "the copy continues where the source stands"
    );
    assert_eq!(
        push_delta(&track, &mut first, 1, 1024),
        push_delta(&track, &mut second, 1, 1024),
        "the same chunk from either"
    );
    drop(first);
    assert_eq!(watch.counts(), (2, 1, 0), "only the source's storage freed");
    assert_eq!(
        push_delta(&track, &mut second, 2, 2048),
        canonical(2048, &DELTA_PAYLOAD),
        "the copy is independent of the source"
    );
    drop(second);
    assert_eq!(watch.counts(), (2, 2, 0));

    // A copy between decoders of different tracks is refused.
    let mut other_track_decoder = decoder(&track);
    let segment = locmaf_support::init_segment();
    let mut changed = segment.clone();
    let at = changed
        .windows(4)
        .position(|w| w == b"trex")
        .expect("the header has its trex");
    changed[at + 16..at + 20].copy_from_slice(&512u32.to_be_bytes());
    let init = moq5_media::cmaf::CmafInit::parse(changed).expect("the changed header parses");
    let other = Track::from_init(init.info()).expect("the changed header is usable");
    let source = Decoder::new(&other, Limits::default()).expect("a decoder of the other track");
    assert_eq!(
        other_track_decoder.copy_from(&source).expect_err("refused"),
        moq5_core::Error::INVAL
    );
}

#[test]
fn encoder_drop_frees_storage_once() {
    let track = track();
    let watch = Watch::new(storage_size(&track, true));
    let mut encoder = encoder(&track);
    assert_eq!(watch.counts(), (1, 0, 0), "one storage allocation");
    encoder
        .encode_chunk(0, 0, &canonical(0, &FULL_PAYLOAD))
        .expect("the chunk encodes");
    encoder.commit();
    drop(encoder);
    assert_eq!(watch.counts(), (1, 1, 0), "freed once, with its own layout");
}

#[test]
fn encoder_move_keeps_state() {
    let track = track();
    let watch = Watch::new(storage_size(&track, true));
    let mut encoder = encoder(&track);
    encoder
        .encode_chunk(0, 0, &canonical(0, &FULL_PAYLOAD))
        .expect("the chunk encodes");
    encoder.commit();
    let mut held = Vec::new();
    held.push(encoder);
    held.push(self::encoder(&track));
    let mut encoder = held.swap_remove(0);
    assert_eq!(
        encoder
            .encode_chunk(0, 1, &canonical(1024, &DELTA_PAYLOAD))
            .expect("the next chunk encodes"),
        DELTA,
        "a delta against the committed reference: the chain survived"
    );
    drop(held);
    drop(encoder);
    assert_eq!(watch.counts(), (2, 2, 0));
}

#[test]
fn encoder_send() {
    let track = track();
    let watch = Watch::new(storage_size(&track, true));
    let mut encoder = encoder(&track);
    encoder
        .encode_chunk(0, 0, &canonical(0, &FULL_PAYLOAD))
        .expect("the chunk encodes");
    encoder.commit();
    let object = std::thread::spawn(move || {
        let object = encoder
            .encode_chunk(0, 1, &canonical(1024, &DELTA_PAYLOAD))
            .expect("the next chunk encodes");
        drop(encoder);
        object
    })
    .join()
    .expect("the other thread encodes");
    assert_eq!(object, DELTA);
    assert_eq!(watch.counts(), (1, 1, 0), "freed once, on the other thread");
}
