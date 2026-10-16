use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::endpoint::{Endpoint, ServiceScope, TaskContext};
use crate::error::{check, Error, Result};
use crate::ffi;
use crate::namespace::Namespace;
use crate::rcbuf::RcBuf;
use crate::sys;
use crate::types::{micros, span, wait_result, MediaType, Packaging, SapType, Wait};

/// Distinguishes the senders (and receivers) of this process, so a track
/// handle can be checked against the object it came from.
pub(crate) fn next_owner_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn opt_span(s: &Option<String>) -> sys::moq_bytes_t {
    s.as_deref().map(|s| span(s.as_bytes())).unwrap_or_default()
}

// -- content protection (CMSF 4) ----------------------------------------------

/// A URL in a DRM system entry (laURL, certURL, authURL).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CmsfUrl {
    /// The URL.
    pub url: String,
    /// Its optional `type`.
    pub url_type: Option<String>,
}

impl CmsfUrl {
    /// A URL with no type.
    pub fn new(url: impl Into<String>) -> CmsfUrl {
        CmsfUrl {
            url: url.into(),
            url_type: None,
        }
    }
}

/// A CMSF 4.1.1.4 drmSystem.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrmSystem {
    /// systemID, a "8-4-4-4-12" UUID string.
    pub system_id: String,
    /// Licence acquisition URL.
    pub la_url: Option<CmsfUrl>,
    /// Certificate URL.
    pub cert_url: Option<CmsfUrl>,
    /// Authorization URL.
    pub auth_url: Option<CmsfUrl>,
    /// Base64 PSSH box.
    pub pssh: Option<String>,
    /// Robustness level.
    pub robustness: Option<String>,
}

/// A CMSF 4.1.1 root contentProtections entry that tracks reference by
/// `ref_id`. libmoq checks the syntax (UUIDs, scheme enum, unique refIDs) but
/// carries the DRM metadata opaquely.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentProtection {
    /// refID, unique across the entries.
    pub ref_id: String,
    /// defaultKID UUID strings; at least one.
    pub default_kids: Vec<String>,
    /// `cenc` or `cbcs`.
    pub scheme: String,
    /// The DRM system this entry is offered under.
    pub drm_system: DrmSystem,
}

// -- callbacks -------------------------------------------------------------------

type Callback<A> = Option<Box<dyn Fn(A) + Send + Sync>>;

/// Sender event callbacks. They run on the endpoint's network thread, must
/// not block, and cannot reach the sender itself.
#[derive(Default)]
struct Callbacks {
    on_ready: Callback<()>,
    on_closed: Callback<(bool, u64)>,
    on_subscriber_joined: Callback<(MediaTrack, usize)>,
    on_subscriber_left: Callback<(MediaTrack, usize)>,
    on_track_closed: Callback<MediaTrack>,
}

impl Callbacks {
    fn any(&self) -> bool {
        self.on_ready.is_some()
            || self.on_closed.is_some()
            || self.on_subscriber_joined.is_some()
            || self.on_subscriber_left.is_some()
            || self.on_track_closed.is_some()
    }
}

struct CallbackCtx {
    callbacks: Callbacks,
    owner: u64,
}

fn call<A>(
    ctx: *mut c_void,
    pick: impl FnOnce(&Callbacks) -> &Callback<A>,
    arg: impl FnOnce(u64) -> A,
) {
    let _scope = ServiceScope::enter();
    // SAFETY: the ctx outlives the sender (MediaSender drops it after destroy).
    let ctx = unsafe { &*(ctx as *const CallbackCtx) };
    if let Some(f) = pick(&ctx.callbacks) {
        let _ = catch_unwind(AssertUnwindSafe(|| f(arg(ctx.owner))));
    }
}

fn track_arg(raw: *mut sys::moq_media_track_t, owner: u64) -> MediaTrack {
    MediaTrack {
        raw: NonNull::new(raw).unwrap_or(NonNull::dangling()),
        owner,
    }
}

unsafe extern "C" fn cb_ready(ctx: *mut c_void, _s: *mut sys::moq_media_sender_t) {
    call(ctx, |c| &c.on_ready, |_| ());
}
unsafe extern "C" fn cb_closed(
    ctx: *mut c_void,
    _s: *mut sys::moq_media_sender_t,
    fatal: bool,
    code: u64,
) {
    call(ctx, |c| &c.on_closed, |_| (fatal, code));
}
unsafe extern "C" fn cb_joined(
    ctx: *mut c_void,
    _s: *mut sys::moq_media_sender_t,
    t: *mut sys::moq_media_track_t,
    n: usize,
) {
    call(ctx, |c| &c.on_subscriber_joined, |o| (track_arg(t, o), n));
}
unsafe extern "C" fn cb_left(
    ctx: *mut c_void,
    _s: *mut sys::moq_media_sender_t,
    t: *mut sys::moq_media_track_t,
    n: usize,
) {
    call(ctx, |c| &c.on_subscriber_left, |o| (track_arg(t, o), n));
}
unsafe extern "C" fn cb_track_closed(
    ctx: *mut c_void,
    _s: *mut sys::moq_media_sender_t,
    t: *mut sys::moq_media_track_t,
) {
    call(ctx, |c| &c.on_track_closed, |o| track_arg(t, o));
}

// -- sender configuration -------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SenderPreset {
    Live,
    Lossless,
}

/// What to do when the network cannot keep up with the writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    /// Evict whole groups back to the newest keyframe (the live preset).
    DropToKeyframe,
    /// Evict whole groups.
    DropGroup,
    /// Block the writer, then fail with [`Error::WOULD_BLOCK`] (the lossless
    /// preset).
    BlockTimeout,
    /// Fail with [`Error::WOULD_BLOCK`] at once.
    ReturnWouldBlock,
}

/// How a [`MediaSender`] publishes. Start from a preset, then adjust; only
/// the fields set here override the preset's.
pub struct SenderConfig {
    namespace: Namespace,
    preset: SenderPreset,
    catalog_track: Option<String>,
    backpressure: Option<Backpressure>,
    block_timeout: Option<Duration>,
    queue_max_objects: Option<u32>,
    queue_max_bytes: Option<u32>,
    pre_ready_max_objects: Option<u32>,
    pre_ready_max_bytes: Option<u32>,
    validate_cmaf: Option<bool>,
    content_protections: Vec<ContentProtection>,
    callbacks: Callbacks,
    publish_tracks: Option<bool>,
    drop_without_demand: Option<bool>,
    catalog_refresh_interval: Option<Duration>,
}

impl SenderConfig {
    fn with_preset(namespace: Namespace, preset: SenderPreset) -> SenderConfig {
        SenderConfig {
            namespace,
            preset,
            catalog_track: None,
            backpressure: None,
            block_timeout: None,
            queue_max_objects: None,
            queue_max_bytes: None,
            pre_ready_max_objects: None,
            pre_ready_max_bytes: None,
            validate_cmaf: None,
            content_protections: Vec::new(),
            callbacks: Callbacks::default(),
            publish_tracks: None,
            drop_without_demand: None,
            catalog_refresh_interval: None,
        }
    }

    /// Live: never block the encoder; drop to the newest keyframe under
    /// backpressure.
    pub fn live(namespace: Namespace) -> SenderConfig {
        SenderConfig::with_preset(namespace, SenderPreset::Live)
    }

    /// Lossless: block the writer under congestion, then surface
    /// [`Error::WOULD_BLOCK`] so the drop decision is the caller's.
    pub fn lossless(namespace: Namespace) -> SenderConfig {
        SenderConfig::with_preset(namespace, SenderPreset::Lossless)
    }

    /// Catalog track name; defaults to `catalog`.
    pub fn catalog_track(mut self, name: impl Into<String>) -> Self {
        self.catalog_track = Some(name.into());
        self
    }
    /// Override the preset's backpressure policy.
    pub fn backpressure(mut self, policy: Backpressure) -> Self {
        self.backpressure = Some(policy);
        self
    }
    /// How long [`Backpressure::BlockTimeout`] blocks.
    pub fn block_timeout(mut self, timeout: Duration) -> Self {
        self.block_timeout = Some(timeout);
        self
    }
    /// Send-queue bounds.
    pub fn queue_limits(mut self, max_objects: u32, max_bytes: u32) -> Self {
        self.queue_max_objects = Some(max_objects);
        self.queue_max_bytes = Some(max_bytes);
        self
    }
    /// Pre-ready queue bounds.
    pub fn pre_ready_limits(mut self, max_objects: u32, max_bytes: u32) -> Self {
        self.pre_ready_max_objects = Some(max_objects);
        self.pre_ready_max_bytes = Some(max_bytes);
        self
    }
    /// Validate CMAF objects against CMSF 3.3/3.4 on write (on by default).
    pub fn validate_cmaf(mut self, validate: bool) -> Self {
        self.validate_cmaf = Some(validate);
        self
    }
    /// CMSF 4 root contentProtections, frozen at attach.
    pub fn content_protections(mut self, entries: Vec<ContentProtection>) -> Self {
        self.content_protections = entries;
        self
    }
    /// Also PUBLISH every track and the catalog (push), instead of only
    /// announcing and answering SUBSCRIBE.
    pub fn publish_tracks(mut self, publish: bool) -> Self {
        self.publish_tracks = Some(publish);
        self
    }
    /// Drop media written while its track has no subscriber, instead of
    /// queueing it.
    pub fn drop_without_demand(mut self, drop: bool) -> Self {
        self.drop_without_demand = Some(drop);
        self
    }
    /// Republish the catalog this often while it has subscribers. Off by
    /// default; meant for relays that serve late joiners from cache only.
    pub fn catalog_refresh_interval(mut self, interval: Duration) -> Self {
        self.catalog_refresh_interval = Some(interval);
        self
    }
    /// Called once the namespace is accepted and the catalog published.
    pub fn on_ready(mut self, f: impl Fn() + Send + Sync + 'static) -> Self {
        self.callbacks.on_ready = Some(Box::new(move |()| f()));
        self
    }
    /// Called when the sender goes terminal, with (is_fatal, fatal_code).
    pub fn on_closed(mut self, f: impl Fn(bool, u64) + Send + Sync + 'static) -> Self {
        self.callbacks.on_closed = Some(Box::new(move |(fatal, code)| f(fatal, code)));
        self
    }
    /// Called when a track gains a subscriber, with the new count.
    pub fn on_subscriber_joined(
        mut self,
        f: impl Fn(MediaTrack, usize) + Send + Sync + 'static,
    ) -> Self {
        self.callbacks.on_subscriber_joined = Some(Box::new(move |(t, n)| f(t, n)));
        self
    }
    /// Called when a track loses a subscriber, with the new count.
    pub fn on_subscriber_left(
        mut self,
        f: impl Fn(MediaTrack, usize) + Send + Sync + 'static,
    ) -> Self {
        self.callbacks.on_subscriber_left = Some(Box::new(move |(t, n)| f(t, n)));
        self
    }
    /// Called when a track's publication closes.
    pub fn on_track_closed(mut self, f: impl Fn(MediaTrack) + Send + Sync + 'static) -> Self {
        self.callbacks.on_track_closed = Some(Box::new(f));
        self
    }
}

/// A sender cfg in libmoq's form, owning everything its pointers reach.
struct PreparedSender {
    raw: sys::moq_media_sender_cfg_t,
    _callbacks: Option<Box<CallbackCtx>>,
    _namespace: Namespace,
    _catalog_track: Option<String>,
    _cp_entries: Vec<ContentProtection>,
    _cps: Vec<sys::moq_cmsf_content_protection_t>,
    _kids: Vec<Vec<sys::moq_bytes_t>>,
    owner: u64,
}

// SAFETY: the raw cfg only points into buffers this struct owns, which are
// never mutated once prepared; the callbacks are Send + Sync.
unsafe impl Send for PreparedSender {}
unsafe impl Sync for PreparedSender {}

fn url(u: &Option<CmsfUrl>) -> sys::moq_cmsf_url_t {
    match u {
        Some(u) => sys::moq_cmsf_url_t {
            present: true,
            url: span(u.url.as_bytes()),
            has_type: u.url_type.is_some(),
            type_: opt_span(&u.url_type),
        },
        None => sys::moq_cmsf_url_t::default(),
    }
}

impl PreparedSender {
    fn new(cfg: SenderConfig) -> PreparedSender {
        let owner = next_owner_id();
        let mut raw = sys::moq_media_sender_cfg_t::default();
        unsafe {
            let size = std::mem::size_of_val(&raw);
            match cfg.preset {
                SenderPreset::Live => ffi::moq_media_sender_cfg_init_live_sized(&mut raw, size),
                SenderPreset::Lossless => {
                    ffi::moq_media_sender_cfg_init_lossless_sized(&mut raw, size)
                }
            }
        }
        raw.endpoint = std::ptr::null(); // attach: the endpoint is given
        raw.namespace_ = cfg.namespace.raw();
        raw.catalog_track = opt_span(&cfg.catalog_track);
        if let Some(bp) = cfg.backpressure {
            raw.backpressure = match bp {
                Backpressure::DropToKeyframe => sys::MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME,
                Backpressure::DropGroup => sys::MOQ_MEDIA_SEND_BP_DROP_GROUP,
                Backpressure::BlockTimeout => sys::MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT,
                Backpressure::ReturnWouldBlock => sys::MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK,
            };
        }
        if let Some(t) = cfg.block_timeout {
            raw.block_timeout_us = micros(t);
        }
        if let Some(v) = cfg.queue_max_objects {
            raw.queue_max_objects = v;
        }
        if let Some(v) = cfg.queue_max_bytes {
            raw.queue_max_bytes = v;
        }
        if let Some(v) = cfg.pre_ready_max_objects {
            raw.pre_ready_max_objects = v;
        }
        if let Some(v) = cfg.pre_ready_max_bytes {
            raw.pre_ready_max_bytes = v;
        }
        if let Some(v) = cfg.validate_cmaf {
            raw.validate_cmaf = v;
        }
        if let Some(v) = cfg.publish_tracks {
            raw.publish_tracks = v;
        }
        if let Some(v) = cfg.drop_without_demand {
            raw.drop_without_demand = v;
        }
        if let Some(v) = cfg.catalog_refresh_interval {
            raw.catalog_refresh_interval_us = micros(v);
        }

        let mut kids: Vec<Vec<sys::moq_bytes_t>> = cfg
            .content_protections
            .iter()
            .map(|e| e.default_kids.iter().map(|k| span(k.as_bytes())).collect())
            .collect();
        let cps: Vec<sys::moq_cmsf_content_protection_t> = cfg
            .content_protections
            .iter()
            .zip(kids.iter_mut())
            .map(|(e, kids)| {
                let d = &e.drm_system;
                sys::moq_cmsf_content_protection_t {
                    ref_id: span(e.ref_id.as_bytes()),
                    default_kids: if kids.is_empty() {
                        std::ptr::null_mut()
                    } else {
                        kids.as_mut_ptr()
                    },
                    default_kid_count: kids.len(),
                    scheme: span(e.scheme.as_bytes()),
                    drm_system: sys::moq_cmsf_drm_system_t {
                        system_id: span(d.system_id.as_bytes()),
                        la_url: url(&d.la_url),
                        cert_url: url(&d.cert_url),
                        auth_url: url(&d.auth_url),
                        has_pssh: d.pssh.is_some(),
                        pssh: opt_span(&d.pssh),
                        has_robustness: d.robustness.is_some(),
                        robustness: opt_span(&d.robustness),
                    },
                }
            })
            .collect();
        if !cps.is_empty() {
            raw.content_protections = cps.as_ptr();
            raw.content_protection_count = cps.len();
        }

        let callbacks = cfg.callbacks.any().then(|| {
            Box::new(CallbackCtx {
                callbacks: cfg.callbacks,
                owner,
            })
        });
        if let Some(ctx) = &callbacks {
            unsafe {
                ffi::moq_media_sender_callbacks_init_sized(
                    &mut raw.callbacks,
                    std::mem::size_of_val(&raw.callbacks),
                )
            };
            let c = &ctx.callbacks;
            let cb = &mut raw.callbacks;
            cb.ctx = &**ctx as *const CallbackCtx as *mut c_void;
            cb.on_ready = c.on_ready.as_ref().map(|_| cb_ready as _);
            cb.on_closed = c.on_closed.as_ref().map(|_| cb_closed as _);
            cb.on_subscriber_joined = c.on_subscriber_joined.as_ref().map(|_| cb_joined as _);
            cb.on_subscriber_left = c.on_subscriber_left.as_ref().map(|_| cb_left as _);
            cb.on_track_closed = c.on_track_closed.as_ref().map(|_| cb_track_closed as _);
        }

        PreparedSender {
            raw,
            _callbacks: callbacks,
            _namespace: cfg.namespace,
            _catalog_track: cfg.catalog_track,
            _cp_entries: cfg.content_protections,
            _cps: cps,
            _kids: kids,
            owner,
        }
    }
}

// -- track configuration ----------------------------------------------------------

/// One track to add to a [`MediaSender`]: enough to derive its catalog
/// entry. MSF requires a codec and a bitrate for every audio and video
/// track, and a sample rate and channel config for audio.
#[derive(Clone, Debug)]
pub struct TrackConfig {
    name: String,
    media_type: MediaType,
    packaging: Packaging,
    codec: String,
    timescale: u32,
    init_data: Vec<u8>,
    role: Option<String>,
    lang: Option<String>,
    is_live: bool,
    width: u32,
    height: u32,
    framerate_millis: u64,
    samplerate: u32,
    channel_config: Option<String>,
    bitrate: u64,
    max_grp_sap: Option<u32>,
    max_obj_sap: Option<u32>,
    content_protection_ref_ids: Vec<String>,
    sap_timeline: Option<u32>,
    track_duration_ms: Option<u64>,
    media_timeline: Option<u32>,
    alt_group: Option<i32>,
}

impl TrackConfig {
    /// A live track. `codec` is the catalog codec string, e.g. `avc1.64001f`.
    pub fn new(
        name: impl Into<String>,
        media_type: MediaType,
        packaging: Packaging,
        codec: impl Into<String>,
    ) -> TrackConfig {
        TrackConfig {
            name: name.into(),
            media_type,
            packaging,
            codec: codec.into(),
            timescale: 0,
            init_data: Vec::new(),
            role: None,
            lang: None,
            is_live: true,
            width: 0,
            height: 0,
            framerate_millis: 0,
            samplerate: 0,
            channel_config: None,
            bitrate: 0,
            max_grp_sap: None,
            max_obj_sap: None,
            content_protection_ref_ids: Vec::new(),
            sap_timeline: None,
            track_duration_ms: None,
            media_timeline: None,
            alt_group: None,
        }
    }

    /// Media timescale; 0 means microseconds.
    pub fn timescale(mut self, timescale: u32) -> Self {
        self.timescale = timescale;
        self
    }
    /// Decoder config carried in the catalog: a CMAF init segment, avcC,
    /// AudioSpecificConfig, ...
    pub fn init_data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.init_data = data.into();
        self
    }
    /// Catalog role; derived from the media type when unset.
    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
    /// Catalog language.
    pub fn lang(mut self, lang: impl Into<String>) -> Self {
        self.lang = Some(lang.into());
        self
    }
    /// Catalog isLive (true by default).
    pub fn live(mut self, is_live: bool) -> Self {
        self.is_live = is_live;
        self
    }
    /// Video dimensions.
    pub fn dimensions(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }
    /// Video frame rate, in frames per 1000 seconds (fps * 1000).
    pub fn framerate_millis(mut self, millis: u64) -> Self {
        self.framerate_millis = millis;
        self
    }
    /// Audio sample rate.
    pub fn samplerate(mut self, rate: u32) -> Self {
        self.samplerate = rate;
        self
    }
    /// Audio channelConfig, e.g. `"2"`.
    pub fn channel_config(mut self, config: impl Into<String>) -> Self {
        self.channel_config = Some(config.into());
        self
    }
    /// Maximum bitrate, bits/s.
    pub fn bitrate(mut self, bitrate: u64) -> Self {
        self.bitrate = bitrate;
        self
    }
    /// CMSF 3.5.2 maxGrpSapStartingType / maxObjSapStartingType hints.
    pub fn max_sap_types(mut self, group: Option<u32>, object: Option<u32>) -> Self {
        self.max_grp_sap = group;
        self.max_obj_sap = object;
        self
    }
    /// CMSF 4.1.2 contentProtectionRefIDs: declares the track encrypted.
    /// Each id must name a root entry of the sender config.
    pub fn content_protection_ref_ids(mut self, ids: Vec<String>) -> Self {
        self.content_protection_ref_ids = ids;
        self
    }
    /// Generate a CMSF SAP event timeline track `<name>.sap`, keeping
    /// `history_groups` groups of records (0 = default).
    pub fn sap_timeline(mut self, history_groups: u32) -> Self {
        self.sap_timeline = Some(history_groups);
        self
    }
    /// VOD track duration (requires `live(false)`).
    pub fn track_duration_ms(mut self, ms: u64) -> Self {
        self.track_duration_ms = Some(ms);
        self
    }
    /// Generate an MSF media timeline track `<name>.timeline`.
    pub fn media_timeline(mut self, history_groups: u32) -> Self {
        self.media_timeline = Some(history_groups);
        self
    }
    /// CMSF 3.2 altGroup shared by a switching set.
    pub fn alt_group(mut self, group: i32) -> Self {
        self.alt_group = Some(group);
        self
    }
}

/// A track cfg in libmoq's form, owning what its pointers reach.
struct PreparedTrack {
    raw: sys::moq_media_track_cfg_t,
    _cfg: TrackConfig,
    _refs: Vec<sys::moq_bytes_t>,
}

// SAFETY: as for PreparedSender.
unsafe impl Send for PreparedTrack {}
unsafe impl Sync for PreparedTrack {}

impl PreparedTrack {
    fn new(cfg: TrackConfig) -> PreparedTrack {
        let mut raw = sys::moq_media_track_cfg_t::default();
        unsafe { ffi::moq_media_track_cfg_init_sized(&mut raw, std::mem::size_of_val(&raw)) };
        raw.name = span(cfg.name.as_bytes());
        raw.media_type = cfg.media_type.raw();
        raw.packaging = cfg.packaging.raw();
        raw.codec = span(cfg.codec.as_bytes());
        raw.timescale = cfg.timescale;
        raw.init_data = span(&cfg.init_data);
        raw.role = opt_span(&cfg.role);
        raw.lang = opt_span(&cfg.lang);
        raw.is_live = cfg.is_live;
        raw.width = cfg.width;
        raw.height = cfg.height;
        raw.framerate_millis = cfg.framerate_millis;
        raw.samplerate = cfg.samplerate;
        raw.channel_config = opt_span(&cfg.channel_config);
        raw.bitrate = cfg.bitrate;
        raw.has_max_grp_sap = cfg.max_grp_sap.is_some();
        raw.max_grp_sap = cfg.max_grp_sap.unwrap_or(0);
        raw.has_max_obj_sap = cfg.max_obj_sap.is_some();
        raw.max_obj_sap = cfg.max_obj_sap.unwrap_or(0);
        let refs: Vec<sys::moq_bytes_t> = cfg
            .content_protection_ref_ids
            .iter()
            .map(|r| span(r.as_bytes()))
            .collect();
        if !refs.is_empty() {
            raw.content_protection_ref_ids = refs.as_ptr();
            raw.content_protection_ref_id_count = refs.len();
        }
        if let Some(h) = cfg.sap_timeline {
            raw.emit_sap_timeline = true;
            raw.sap_timeline_history_groups = h;
        }
        if let Some(ms) = cfg.track_duration_ms {
            raw.has_track_duration = true;
            raw.track_duration_ms = ms;
        }
        if let Some(h) = cfg.media_timeline {
            raw.emit_media_timeline = true;
            raw.media_timeline_history_groups = h;
        }
        if let Some(g) = cfg.alt_group {
            raw.has_alt_group = true;
            raw.alt_group = g;
        }
        PreparedTrack {
            raw,
            _cfg: cfg,
            _refs: refs,
        }
    }
}

// -- handles ----------------------------------------------------------------------

/// A track added to a [`MediaSender`]. A plain handle: copy it freely. Calls
/// with a handle from another sender fail with [`Error::INVAL`] instead of
/// reaching libmoq, and a handle outliving its sender is inert.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MediaTrack {
    raw: NonNull<sys::moq_media_track_t>,
    owner: u64,
}

// SAFETY: an opaque identifier; only its sender dereferences it, and only
// after checking the owner.
unsafe impl Send for MediaTrack {}
unsafe impl Sync for MediaTrack {}

impl MediaTrack {
    /// The underlying `moq_media_track_t`.
    pub fn as_ptr(&self) -> *mut sys::moq_media_track_t {
        self.raw.as_ptr()
    }
}

impl std::fmt::Debug for MediaTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MediaTrack").field(&self.raw).finish()
    }
}

/// One media object to [`write`](MediaSender::write).
#[derive(Debug)]
pub struct SendObject {
    /// The object payload. For CMAF, one moof+mdat chunk sequence.
    pub payload: RcBuf,
    /// CMAF only: the object's property block, passed through. Must be
    /// `None` for LOC, whose properties libmoq generates.
    pub properties: Option<RcBuf>,
    /// A random-access point.
    pub is_sync: bool,
    /// Opens a new group.
    pub starts_group: bool,
    /// Closes the group after this object.
    pub ends_group: bool,
    /// Decode time (advisory; write order is decode order).
    pub decode_time_us: u64,
    /// LOC: presentation time.
    pub presentation_time_us: u64,
    /// LOC: wall-clock capture time, microseconds since the Unix epoch.
    /// Falls back to the presentation time when unset.
    pub capture_time_us: Option<u64>,
    /// CMAF: the declared SAP type of the object's first sample.
    pub sap_type: Option<SapType>,
}

impl SendObject {
    /// A delta object with no timing.
    pub fn new(payload: RcBuf) -> SendObject {
        SendObject {
            payload,
            properties: None,
            is_sync: false,
            starts_group: false,
            ends_group: false,
            decode_time_us: 0,
            presentation_time_us: 0,
            capture_time_us: None,
            sap_type: None,
        }
    }
}

/// A failed [`write`](MediaSender::write): the error and the object, which
/// the caller still owns.
#[derive(Debug)]
pub struct WriteError {
    /// What libmoq reported.
    pub error: Error,
    /// The object, untouched.
    pub object: SendObject,
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "media write failed: {}", self.error)
    }
}

impl std::error::Error for WriteError {}

/// A [`MediaSender`] statistics snapshot. objects_written equals
/// objects_sent + objects_queued + objects_dropped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct SenderStats {
    pub objects_written: u64,
    pub objects_sent: u64,
    pub objects_queued: u64,
    pub bytes_queued: u64,
    pub objects_dropped: u64,
    pub groups_dropped: u64,
    pub keyframes_dropped: u64,
    pub groups_abandoned: u64,
    pub backpressure_stalls: u64,
    /// The last failed write's error.
    pub last_error: Option<Error>,
    pub sap_records_evicted: u64,
}

// -- the sender -------------------------------------------------------------------

/// Publishes a namespace: an MSF/CMSF catalog derived from its tracks, and
/// their media objects.
///
/// Calls must come from application threads, one at a time: the type is
/// `Send` but not `Sync` (share it behind a `Mutex`). It holds its
/// [`Endpoint`], which therefore outlives it.
pub struct MediaSender {
    raw: NonNull<sys::moq_media_sender_t>,
    owner: u64,
    /// Owns the cfg libmoq was given, callback context included.
    _cfg: Arc<dyn std::any::Any + Send + Sync>,
    endpoint: Endpoint,
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

// SAFETY: libmoq accepts sender calls from any application thread as long
// as they are serialized, which `!Sync` enforces.
unsafe impl Send for MediaSender {}

impl MediaSender {
    /// Attach a sender to `endpoint` from this thread. Tracks added after
    /// this can race the first catalog; use [`start`](MediaSender::start) to
    /// publish an initial track set atomically.
    pub fn attach(endpoint: &Endpoint, cfg: SenderConfig) -> Result<MediaSender> {
        let prepared = Arc::new(PreparedSender::new(cfg));
        let mut out = std::ptr::null_mut();
        check(unsafe { ffi::moq_media_sender_attach(endpoint.as_ptr(), &prepared.raw, &mut out) })?;
        let raw = NonNull::new(out).ok_or(Error::INTERNAL)?;
        Ok(MediaSender {
            raw,
            owner: prepared.owner,
            _cfg: prepared,
            endpoint: endpoint.clone(),
            _not_sync: std::marker::PhantomData,
        })
    }

    /// Attach a sender and add `tracks` in one turn of the endpoint's network
    /// thread, before its first pump, so the initial catalog carries all of
    /// them. Returns at once; collect the result with
    /// [`SenderStartup::wait`].
    pub fn start(
        endpoint: &Endpoint,
        cfg: SenderConfig,
        tracks: Vec<TrackConfig>,
    ) -> Result<SenderStartup> {
        let shared = Arc::new(Startup {
            state: Mutex::new(StartupState::default()),
            cond: Condvar::new(),
            sender: PreparedSender::new(cfg),
            tracks: tracks.into_iter().map(PreparedTrack::new).collect(),
        });
        let task = shared.clone();
        endpoint.post(move |ctx| task.run(ctx))?;
        Ok(SenderStartup {
            shared: Some(shared),
            endpoint: endpoint.clone(),
        })
    }

    /// The endpoint this sender is attached to.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The underlying `moq_media_sender_t`.
    pub fn as_ptr(&self) -> *mut sys::moq_media_sender_t {
        self.raw.as_ptr()
    }

    fn own(&self, track: MediaTrack) -> Result<*mut sys::moq_media_track_t> {
        if track.owner == self.owner {
            Ok(track.raw.as_ptr())
        } else {
            Err(Error::INVAL)
        }
    }

    /// Add a track. Before readiness it joins the initial catalog; after, it
    /// triggers a catalog republish.
    pub fn add_track(&self, cfg: TrackConfig) -> Result<MediaTrack> {
        let t = PreparedTrack::new(cfg);
        let mut out = std::ptr::null_mut();
        check(unsafe { ffi::moq_media_sender_add_track(self.as_ptr(), &t.raw, &mut out) })?;
        Ok(MediaTrack {
            raw: NonNull::new(out).ok_or(Error::INTERNAL)?,
            owner: self.owner,
        })
    }

    /// Remove a track from the catalog and end it.
    pub fn remove_track(&self, track: MediaTrack) -> Result<()> {
        let t = self.own(track)?;
        check(unsafe { ffi::moq_media_sender_remove_track(self.as_ptr(), t) })
    }

    /// Submit one object; per track, write order is decode order. On success
    /// libmoq owns the payload; on failure it comes back in the error.
    pub fn write(&self, track: MediaTrack, obj: SendObject) -> Result<(), WriteError> {
        let t = match self.own(track) {
            Ok(t) => t,
            Err(error) => return Err(WriteError { error, object: obj }),
        };
        let raw = sys::moq_media_send_object_t {
            struct_size: std::mem::size_of::<sys::moq_media_send_object_t>() as u32,
            payload: obj.payload.as_ptr(),
            properties: obj
                .properties
                .as_ref()
                .map_or(std::ptr::null_mut(), |p| p.as_ptr()),
            is_sync: obj.is_sync,
            starts_group: obj.starts_group,
            ends_group: obj.ends_group,
            decode_time_us: obj.decode_time_us,
            presentation_time_us: obj.presentation_time_us,
            has_capture_time: obj.capture_time_us.is_some(),
            capture_time_us: obj.capture_time_us.unwrap_or(0),
            has_sap_type: obj.sap_type.is_some(),
            sap_type: obj.sap_type.map_or(sys::MOQ_SAP_NONE, SapType::raw),
            metadata: std::ptr::null(),
        };
        match unsafe { ffi::moq_media_sender_write(self.as_ptr(), t, &raw) } {
            sys::MOQ_OK => {
                // Transferred: the references are libmoq's now.
                obj.payload.into_raw();
                if let Some(p) = obj.properties {
                    p.into_raw();
                }
                Ok(())
            }
            rc => Err(WriteError {
                error: Error::from_code(rc),
                object: obj,
            }),
        }
    }

    /// End a finite track once its queued objects drain. Idempotent. May
    /// fail with [`Error::WOULD_BLOCK`] while the queue is full: retry.
    pub fn end_track(&self, track: MediaTrack) -> Result<()> {
        let t = self.own(track)?;
        check(unsafe { ffi::moq_media_sender_end_track(self.as_ptr(), t) })
    }

    /// Terminate the broadcast (MSF 11.3): end every track and publish a
    /// final catalog with isComplete. Legal only after readiness.
    pub fn complete(&self) -> Result<()> {
        check(unsafe { ffi::moq_media_sender_complete(self.as_ptr()) })
    }

    /// Convert live tracks to VOD in place, each with its duration in ms.
    /// All or nothing.
    pub fn convert_to_vod(&self, tracks: &[(MediaTrack, u64)]) -> Result<()> {
        let items = tracks
            .iter()
            .map(|&(t, duration_ms)| {
                Ok(sys::moq_media_vod_track_t {
                    track: self.own(t)?,
                    duration_ms,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        check(unsafe {
            ffi::moq_media_sender_convert_to_vod(self.as_ptr(), items.as_ptr(), items.len())
        })
    }

    /// A statistics snapshot.
    pub fn stats(&self) -> Result<SenderStats> {
        let mut s = sys::moq_media_sender_stats_t::default();
        check(unsafe {
            ffi::moq_media_sender_get_stats(self.as_ptr(), &mut s, std::mem::size_of_val(&s))
        })?;
        Ok(SenderStats {
            objects_written: s.objects_written,
            objects_sent: s.objects_sent,
            objects_queued: s.objects_queued,
            bytes_queued: s.bytes_queued,
            objects_dropped: s.objects_dropped,
            groups_dropped: s.groups_dropped,
            keyframes_dropped: s.keyframes_dropped,
            groups_abandoned: s.groups_abandoned,
            backpressure_stalls: s.backpressure_stalls,
            last_error: (s.last_error != sys::MOQ_OK).then(|| Error::from_code(s.last_error)),
            sap_records_evicted: s.sap_records_evicted,
        })
    }

    /// The namespace is accepted and the catalog published. Says nothing
    /// about subscribers.
    pub fn is_ready(&self) -> bool {
        unsafe { ffi::moq_media_sender_is_ready(self.as_ptr()) }
    }

    /// Block until ready with send-queue headroom, the endpoint wakes, or
    /// `timeout` elapses. [`Wait::Ready`] is advice to try a write.
    pub fn wait(&self, timeout: Duration) -> Result<Wait> {
        wait_result(unsafe { ffi::moq_media_sender_wait(self.as_ptr(), micros(timeout)) })
    }

    /// Active subscriptions on `track`.
    pub fn track_subscriptions(&self, track: MediaTrack) -> usize {
        match self.own(track) {
            Ok(t) => unsafe { ffi::moq_media_sender_track_subscriptions(self.as_ptr(), t) },
            Err(_) => 0,
        }
    }

    /// `track` has a subscriber.
    pub fn track_has_subscriber(&self, track: MediaTrack) -> bool {
        match self.own(track) {
            Ok(t) => unsafe { ffi::moq_media_sender_track_has_subscriber(self.as_ptr(), t) },
            Err(_) => false,
        }
    }

    /// Any media track has a subscriber: worth encoding.
    pub fn has_media_subscriber(&self) -> bool {
        unsafe { ffi::moq_media_sender_has_media_subscriber(self.as_ptr()) }
    }

    /// Closed cleanly.
    pub fn is_closed(&self) -> bool {
        unsafe { ffi::moq_media_sender_is_closed(self.as_ptr()) }
    }

    /// Failed (connect, TLS, protocol or sender failure).
    pub fn is_fatal(&self) -> bool {
        unsafe { ffi::moq_media_sender_is_fatal(self.as_ptr()) }
    }

    /// The sender's fatal code, or the endpoint's.
    pub fn fatal_code(&self) -> u64 {
        unsafe { ffi::moq_media_sender_fatal_code(self.as_ptr()) }
    }
}

impl Drop for MediaSender {
    fn drop(&mut self) {
        // Settles callbacks before their context (in `_cfg`) goes, and
        // detaches before the endpoint handle is released.
        unsafe { ffi::moq_media_sender_destroy(self.raw.as_ptr()) }
    }
}

impl std::fmt::Debug for MediaSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MediaSender").field(&self.raw).finish()
    }
}

// -- atomic startup ---------------------------------------------------------------

#[derive(Default)]
struct StartupState {
    running: bool,
    done: bool,
    cancelled: bool,
    result: sys::moq_result_t,
    sender: Option<NonNull<sys::moq_media_sender_t>>,
    tracks: Vec<NonNull<sys::moq_media_track_t>>,
}

struct Startup {
    state: Mutex<StartupState>,
    cond: Condvar,
    sender: PreparedSender,
    tracks: Vec<PreparedTrack>,
}

// SAFETY: the raw pointers in the state are only used under its mutex, and
// the prepared cfgs are read-only.
unsafe impl Send for Startup {}
unsafe impl Sync for Startup {}

impl Startup {
    fn lock(&self) -> std::sync::MutexGuard<'_, StartupState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The posted task: attach, then add every track, with no other locks
    /// or waits. Run closed (no session) or after a cancel, it does nothing.
    fn run(&self, ctx: Option<&TaskContext>) -> Result<()> {
        let ctx = {
            let mut st = self.lock();
            st.running = true;
            ctx.filter(|_| !st.cancelled)
        };
        let mut sender = None;
        let mut tracks = Vec::with_capacity(self.tracks.len());
        let mut rc = match ctx {
            None => sys::MOQ_ERR_INTERRUPTED,
            Some(ctx) => {
                let mut out = std::ptr::null_mut();
                let rc = unsafe {
                    ffi::moq_media_sender_attach(ctx.endpoint_ptr(), &self.sender.raw, &mut out)
                };
                sender = NonNull::new(out);
                if rc == sys::MOQ_OK && sender.is_none() {
                    sys::MOQ_ERR_INTERNAL
                } else {
                    rc
                }
            }
        };
        for t in &self.tracks {
            if rc != sys::MOQ_OK {
                break;
            }
            let mut out = std::ptr::null_mut();
            let s = sender.map_or(std::ptr::null_mut(), |s| s.as_ptr());
            rc = unsafe { ffi::moq_media_sender_add_track(s, &t.raw, &mut out) };
            if rc == sys::MOQ_OK {
                match NonNull::new(out) {
                    Some(t) => tracks.push(t),
                    None => rc = sys::MOQ_ERR_INTERNAL,
                }
            }
        }
        let mut st = self.lock();
        st.sender = sender;
        st.tracks = tracks;
        st.result = rc;
        st.done = true;
        self.cond.notify_all();
        check(rc)
    }
}

/// A sender being started by [`MediaSender::start`]. Dropping it cancels the
/// startup: a task that has not run yet will do nothing, and a sender it
/// already attached is destroyed.
pub struct SenderStartup {
    shared: Option<Arc<Startup>>,
    endpoint: Endpoint,
}

/// What [`SenderStartup::wait`] found.
pub enum StartupStatus {
    /// Still pending: wait again or drop to cancel.
    Pending(SenderStartup),
    /// The task ran: the sender and its tracks, in the order given, or the
    /// first failure.
    Done(Result<(MediaSender, Vec<MediaTrack>)>),
}

impl SenderStartup {
    /// Wait up to `timeout` for the startup task to finish.
    pub fn wait(mut self, timeout: Duration) -> StartupStatus {
        let shared = self.shared.take().expect("startup already settled");
        let mut st = shared.lock();
        if !st.done {
            st = shared
                .cond
                .wait_timeout_while(st, timeout, |st| !st.done)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        if !st.done {
            drop(st);
            self.shared = Some(shared);
            return StartupStatus::Pending(self);
        }
        let sender = st.sender.take();
        let tracks = std::mem::take(&mut st.tracks);
        let result = st.result;
        drop(st);
        let owner = shared.sender.owner;
        let started = sender.map(|raw| MediaSender {
            raw,
            owner,
            _cfg: shared.clone(),
            endpoint: self.endpoint.clone(),
            _not_sync: std::marker::PhantomData,
        });
        StartupStatus::Done(match (result, started) {
            (sys::MOQ_OK, Some(sender)) => Ok((
                sender,
                tracks
                    .into_iter()
                    .map(|raw| MediaTrack { raw, owner })
                    .collect(),
            )),
            // A partial sender is destroyed as `started` drops.
            (rc, _) => Err(Error::from_code(if rc == sys::MOQ_OK {
                sys::MOQ_ERR_INTERNAL
            } else {
                rc
            })),
        })
    }

    /// Cancel; the same as dropping.
    pub fn cancel(self) {}
}

impl Drop for SenderStartup {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let mut st = shared.lock();
        st.cancelled = true;
        // A running task only performs finite attach/add calls: settle it
        // before destroying what it made. An unstarted one runs closed.
        while st.running && !st.done {
            st = shared.cond.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        if let Some(raw) = st.sender.take() {
            drop(st);
            unsafe { ffi::moq_media_sender_destroy(raw.as_ptr()) };
        }
    }
}
