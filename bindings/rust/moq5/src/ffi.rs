//! The libmoq service calls this crate makes, as one replaceable table.
//!
//! Every call into the service tier (endpoint, media sender, media receiver)
//! goes through [`api`]. A normal build resolves it to [`REAL`], a constant
//! table of the libmoq functions themselves, so the call is direct. With the
//! `testing` feature, [`crate::testing::install`] swaps in a table of fakes
//! for the duration of a test. Pure parsers (CMAF), rcbufs and the
//! version/error strings are not in the table: tests keep the real ones.

use moq5_sys::*;
use std::ffi::c_void;

macro_rules! api_table {
    ($($name:ident : fn($($arg:ident : $ty:ty),*) $(-> $ret:ty)?;)*) => {
        /// The libmoq service calls, as replaceable entries. Build a fake
        /// table with struct update syntax: `Api { moq_endpoint_connect: fake,
        /// ..REAL }`.
        #[derive(Clone, Copy)]
        #[allow(missing_docs)]
        pub struct Api {
            $(pub $name: unsafe extern "C" fn($($ty),*) $(-> $ret)?,)*
        }

        /// The real libmoq.
        pub const REAL: Api = Api { $($name: moq5_sys::$name,)* };

        $(
            #[inline]
            pub(crate) unsafe fn $name($($arg: $ty),*) $(-> $ret)? {
                (api().$name)($($arg),*)
            }
        )*
    };
}

api_table! {
    moq_endpoint_cfg_init: fn(cfg: *mut moq_endpoint_cfg_t);
    moq_endpoint_cfg_init_sized: fn(cfg: *mut moq_endpoint_cfg_t, cfg_size: usize);
    moq_endpoint_connect: fn(cfg: *const moq_endpoint_cfg_t, out: *mut *mut moq_endpoint_t) -> moq_result_t;
    moq_endpoint_stop: fn(ep: *mut moq_endpoint_t) -> moq_result_t;
    moq_endpoint_destroy: fn(ep: *mut moq_endpoint_t);
    moq_endpoint_wait: fn(ep: *mut moq_endpoint_t, timeout_us: u64) -> moq_result_t;
    moq_endpoint_wake: fn(ep: *mut moq_endpoint_t);
    moq_endpoint_drain: fn(ep: *mut moq_endpoint_t, timeout_us: u64) -> moq_result_t;
    moq_endpoint_state: fn(ep: *const moq_endpoint_t) -> moq_endpoint_state_t;
    moq_endpoint_is_closed: fn(ep: *const moq_endpoint_t) -> bool;
    moq_endpoint_is_fatal: fn(ep: *const moq_endpoint_t) -> bool;
    moq_endpoint_fatal_code: fn(ep: *const moq_endpoint_t) -> u64;
    moq_endpoint_get_terminal: fn(ep: *const moq_endpoint_t, out: *mut moq_endpoint_terminal_t, out_size: usize) -> moq_result_t;
    moq_endpoint_negotiated_version: fn(ep: *const moq_endpoint_t) -> moq_version_t;
    moq_endpoint_set_interrupted: fn(ep: *mut moq_endpoint_t, interrupted: bool);
    moq_endpoint_post: fn(ep: *mut moq_endpoint_t, fn_: moq_endpoint_task_fn, ctx: *mut c_void) -> moq_result_t;

    moq_media_sender_callbacks_init_sized: fn(cb: *mut moq_media_sender_callbacks_t, cb_size: usize);
    moq_media_sender_cfg_init_live_sized: fn(cfg: *mut moq_media_sender_cfg_t, cfg_size: usize);
    moq_media_sender_cfg_init_lossless_sized: fn(cfg: *mut moq_media_sender_cfg_t, cfg_size: usize);
    moq_media_track_cfg_init_sized: fn(cfg: *mut moq_media_track_cfg_t, cfg_size: usize);
    moq_media_sender_attach: fn(ep: *mut moq_endpoint_t, cfg: *const moq_media_sender_cfg_t, out: *mut *mut moq_media_sender_t) -> moq_result_t;
    moq_media_sender_destroy: fn(s: *mut moq_media_sender_t);
    moq_media_sender_add_track: fn(s: *mut moq_media_sender_t, cfg: *const moq_media_track_cfg_t, out: *mut *mut moq_media_track_t) -> moq_result_t;
    moq_media_sender_remove_track: fn(s: *mut moq_media_sender_t, track: *mut moq_media_track_t) -> moq_result_t;
    moq_media_sender_write: fn(s: *mut moq_media_sender_t, track: *mut moq_media_track_t, obj: *const moq_media_send_object_t) -> moq_result_t;
    moq_media_sender_end_track: fn(s: *mut moq_media_sender_t, track: *mut moq_media_track_t) -> moq_result_t;
    moq_media_sender_complete: fn(s: *mut moq_media_sender_t) -> moq_result_t;
    moq_media_sender_convert_to_vod: fn(s: *mut moq_media_sender_t, items: *const moq_media_vod_track_t, count: usize) -> moq_result_t;
    moq_media_sender_get_stats: fn(s: *const moq_media_sender_t, out: *mut moq_media_sender_stats_t, out_size: usize) -> moq_result_t;
    moq_media_sender_is_ready: fn(s: *const moq_media_sender_t) -> bool;
    moq_media_sender_wait: fn(s: *mut moq_media_sender_t, timeout_us: u64) -> moq_result_t;
    moq_media_sender_track_subscriptions: fn(s: *const moq_media_sender_t, track: *const moq_media_track_t) -> usize;
    moq_media_sender_track_has_subscriber: fn(s: *const moq_media_sender_t, track: *const moq_media_track_t) -> bool;
    moq_media_sender_has_media_subscriber: fn(s: *const moq_media_sender_t) -> bool;
    moq_media_sender_is_closed: fn(s: *const moq_media_sender_t) -> bool;
    moq_media_sender_is_fatal: fn(s: *const moq_media_sender_t) -> bool;
    moq_media_sender_fatal_code: fn(s: *const moq_media_sender_t) -> u64;

    moq_media_receiver_cfg_init_live: fn(cfg: *mut moq_media_receiver_cfg_t);
    moq_media_receiver_cfg_init_flow_control: fn(cfg: *mut moq_media_receiver_cfg_t);
    moq_media_receiver_attach: fn(ep: *mut moq_endpoint_t, cfg: *const moq_media_receiver_cfg_t, out: *mut *mut moq_media_receiver_t) -> moq_result_t;
    moq_media_receiver_destroy: fn(r: *mut moq_media_receiver_t);
    moq_media_track_desc_get: fn(track: *const moq_media_track_t) -> *const moq_media_track_desc_t;
    moq_media_receiver_find_content_protection: fn(r: *const moq_media_receiver_t, ref_id: moq_bytes_t) -> *const moq_cmsf_content_protection_t;
    moq_media_receiver_track_subscribe_cfg_init: fn(cfg: *mut moq_media_receiver_track_subscribe_cfg_t);
    moq_media_receiver_subscribe_track: fn(r: *mut moq_media_receiver_t, track: *mut moq_media_track_t, cfg: *const moq_media_receiver_track_subscribe_cfg_t) -> moq_result_t;
    moq_media_receiver_unsubscribe_track: fn(r: *mut moq_media_receiver_t, track: *mut moq_media_track_t) -> moq_result_t;
    moq_media_receiver_track_state: fn(r: *const moq_media_receiver_t, track: *const moq_media_track_t, out: *mut moq_media_track_state_t) -> moq_result_t;
    moq_media_receiver_poll_track: fn(r: *mut moq_media_receiver_t, ev: *mut moq_media_track_event_t, ev_size: usize) -> moq_result_t;
    moq_media_receiver_poll_object: fn(r: *mut moq_media_receiver_t, obj: *mut moq_media_object_t, obj_size: usize) -> moq_result_t;
    moq_media_receiver_poll_sap: fn(r: *mut moq_media_receiver_t, out: *mut moq_media_sap_record_t, out_size: usize) -> moq_result_t;
    moq_media_receiver_poll_media_timeline: fn(r: *mut moq_media_receiver_t, out: *mut moq_media_timeline_record_t, out_size: usize) -> moq_result_t;
    moq_media_receiver_wait: fn(r: *mut moq_media_receiver_t, timeout_us: u64) -> moq_result_t;
    moq_media_receiver_get_stats: fn(r: *const moq_media_receiver_t, out: *mut moq_media_receiver_stats_t, out_size: usize) -> moq_result_t;
    moq_media_receiver_is_closed: fn(r: *const moq_media_receiver_t) -> bool;
    moq_media_receiver_is_fatal: fn(r: *const moq_media_receiver_t) -> bool;
    moq_media_receiver_fatal_code: fn(r: *const moq_media_receiver_t) -> u64;
    moq_media_object_cleanup: fn(obj: *mut moq_media_object_t);
}

#[cfg(not(any(test, feature = "testing")))]
#[inline(always)]
fn api() -> Api {
    REAL
}

#[cfg(any(test, feature = "testing"))]
pub(crate) use seam::api;

#[cfg(any(test, feature = "testing"))]
pub(crate) mod seam {
    use super::{Api, REAL};
    use std::sync::RwLock;

    static ACTIVE: RwLock<Api> = RwLock::new(REAL);

    pub(crate) fn api() -> Api {
        *ACTIVE.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Restores the previous table when dropped.
    #[must_use = "the fake table is uninstalled when the guard drops"]
    pub struct ApiGuard(Api);

    impl Drop for ApiGuard {
        fn drop(&mut self) {
            *ACTIVE.write().unwrap_or_else(|e| e.into_inner()) = self.0;
        }
    }

    /// Replace the table until the returned guard drops.
    ///
    /// The table is process-wide: tests that install one must not run
    /// concurrently with other tests that call into libmoq.
    pub fn install(table: Api) -> ApiGuard {
        let mut active = ACTIVE.write().unwrap_or_else(|e| e.into_inner());
        ApiGuard(std::mem::replace(&mut *active, table))
    }
}
