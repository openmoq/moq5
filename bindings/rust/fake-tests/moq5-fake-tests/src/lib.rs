//! Drives the scripted service provider (fake_service.c) and the layout
//! probe (layout_probe.c). The provider is process-global: every test takes
//! [`suite`] first, and the controls that reach the provider's globals are
//! methods of the [`Suite`] it returns, so holding it is the only way to use
//! them.

#![deny(unsafe_op_in_unsafe_fn)]

use std::cell::Cell;
use std::ffi::{c_char, CString};
use std::marker::PhantomData;
use std::sync::{Mutex, MutexGuard};

pub use moq5_sys as sys;

// The provider's raw controls. Every one of them reads or writes the
// provider's process globals without synchronization: a caller must hold the
// [`Suite`] for the whole call (exclusive access), and pass records that are
// valid for the call, as each SAFETY comment at the call site states.
extern "C" {
    fn moq5_fake_reset();
    fn moq5_fake_log_take(buf: *mut c_char, cap: usize) -> usize;
    fn moq5_fake_set(name: *const c_char, value: i64);
    fn moq5_fake_get(name: *const c_char) -> i64;
    pub fn moq5_fake_set_goaway(uri: *const u8, len: usize, present: bool);
    pub fn moq5_fake_set_content_protection(cp: *const sys::moq_cmsf_content_protection_t);
    pub fn moq5_fake_push_track_event(ev: *const sys::moq_media_track_event_t);
    pub fn moq5_fake_push_object(obj: *const sys::moq_media_object_t);
    pub fn moq5_fake_push_stream_terminal(t: *const sys::moq_media_stream_terminal_t);
    pub fn moq5_fake_push_sap(r: *const sys::moq_media_sap_record_t);
    pub fn moq5_fake_push_timeline(r: *const sys::moq_media_timeline_record_t);
    pub fn moq5_fake_receiver_track(
        r: *mut sys::moq_media_receiver_t,
        index: usize,
    ) -> *mut sys::moq_media_track_t;
    pub fn moq5_fake_set_track_desc(
        r: *mut sys::moq_media_receiver_t,
        index: usize,
        desc: *const sys::moq_media_track_desc_t,
    );
    pub fn moq5_fake_set_track_state(r: *mut sys::moq_media_receiver_t, index: usize, state: u32);
}

// The player's controls: the provider queues what the tests push for the
// player attached last.
#[cfg(feature = "player")]
extern "C" {
    pub fn moq5_fake_push_player_output(out: *const sys::moq_media_player_output_t);
    pub fn moq5_fake_push_player_event(ev: *const sys::moq_media_player_event_t);
}

/// One entry of the C layout table.
#[repr(C)]
pub struct LayoutEntry {
    pub name: *const c_char,
    pub size: usize,
    pub align: usize,
    pub offset: usize,
}

extern "C" {
    pub fn moq5_layout_entries(count: *mut usize) -> *const LayoutEntry;
}

static SERIAL: Mutex<()> = Mutex::new(());

/// Exclusive access to the provider for one test: the suite lock, held
/// until the value is dropped. The knob, counter and log controls are its
/// methods, and the value is neither `Send` (the guard) nor `Sync` (the
/// marker), so the thread that took the suite is the only one that can
/// reach the provider through it; two threads sharing a `&Suite` do not
/// compile.
pub struct Suite {
    _serial: MutexGuard<'static, ()>,
    _one_thread: PhantomData<Cell<()>>,
}

/// Serializes the tests of one binary and resets the provider.
pub fn suite() -> Suite {
    // The lock guards no data, so a row that panicked while holding it left
    // nothing inconsistent behind: the poison is cleared and the lock taken
    // again, never reached into.
    let serial = loop {
        match SERIAL.lock() {
            Ok(guard) => break guard,
            Err(poisoned) => {
                drop(poisoned);
                SERIAL.clear_poison();
            }
        }
    };
    // SAFETY: the provider's reset, under the suite lock just taken.
    unsafe { moq5_fake_reset() };
    Suite {
        _serial: serial,
        _one_thread: PhantomData,
    }
}

impl Suite {
    /// Set a provider knob.
    pub fn set(&self, name: &str, value: i64) {
        let name = CString::new(name).expect("a knob name has no NUL");
        // SAFETY: a NUL-terminated name; `self` holds the suite lock.
        unsafe { moq5_fake_set(name.as_ptr(), value) }
    }

    /// Read a provider counter or knob.
    pub fn get(&self, name: &str) -> i64 {
        let name = CString::new(name).expect("a knob name has no NUL");
        // SAFETY: a NUL-terminated name; `self` holds the suite lock.
        unsafe { moq5_fake_get(name.as_ptr()) }
    }

    /// Take the provider's log, one line per call.
    pub fn take_log(&self) -> Vec<String> {
        // SAFETY: a NULL buffer with zero capacity only reports the length;
        // `self` holds the suite lock.
        let len = unsafe { moq5_fake_log_take(std::ptr::null_mut(), 0) };
        let mut buf = vec![0u8; len + 1];
        // SAFETY: `buf` holds `len + 1` bytes, the NUL included; the suite
        // lock is still held, so the log is the one just measured.
        let copied = unsafe { moq5_fake_log_take(buf.as_mut_ptr() as *mut c_char, buf.len()) };
        assert_eq!(copied, len);
        buf.truncate(len);
        String::from_utf8(buf)
            .expect("the provider logs ASCII")
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

/// A log line with a matching prefix, or a panic listing the log.
pub fn line(log: &[String], prefix: &str) -> String {
    log.iter()
        .find(|l| l.starts_with(prefix))
        .cloned()
        .unwrap_or_else(|| panic!("no `{prefix}` line in {log:#?}"))
}

/// `key=value` from a log line.
pub fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let key = format!(" {key}=");
    let start = line
        .find(&key)
        .unwrap_or_else(|| panic!("no `{key}` in `{line}`"))
        + key.len();
    let rest = &line[start..];
    // A bracketed list or a quoted string runs to its closing delimiter;
    // anything else to the next space.
    let end = match rest.as_bytes().first() {
        Some(b'[') => rest.find(']').map(|i| i + 1).unwrap_or(rest.len()),
        Some(b'"') => rest[1..].find('"').map(|i| i + 2).unwrap_or(rest.len()),
        _ => rest.find(' ').unwrap_or(rest.len()),
    };
    &rest[..end]
}

/// A span over `s` for records handed to the provider.
pub fn span(s: &[u8]) -> sys::moq_bytes_t {
    if s.is_empty() {
        sys::moq_bytes_t::default()
    } else {
        sys::moq_bytes_t {
            data: s.as_ptr(),
            len: s.len(),
        }
    }
}
