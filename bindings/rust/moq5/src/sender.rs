use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fmt, mem, ptr};

use crate::buffer::Buffer;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::namespace::Namespace;
use crate::types::{CompanionProfile, MediaType, Packaging, SapType, WaitResult};
use crate::{ffi, sys};

/// Distinguishes the senders and receivers of this process, so a track
/// handle can be checked against the object it came from.
pub(crate) fn next_owner_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
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
    #[must_use]
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
/// `ref_id`. libmoq checks the syntax (UUIDs, scheme enum, unique refIDs)
/// and carries the DRM metadata opaquely; it is deep-copied at attach.
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

fn raw_url(url: Option<&CmsfUrl>) -> sys::moq_cmsf_url_t {
    match url {
        Some(url) => sys::moq_cmsf_url_t {
            present: true,
            url: ffi::span(url.url.as_bytes()),
            has_type: url.url_type.is_some(),
            type_: ffi::optional_span(url.url_type.as_deref()),
        },
        None => sys::moq_cmsf_url_t::default(),
    }
}

// -- sender configuration ---------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SenderPreset {
    Live,
    Lossless,
}

/// What to do when the network cannot keep up with the writer
/// (`moq_media_send_backpressure_t`).
#[non_exhaustive]
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

impl Backpressure {
    fn raw(self) -> sys::moq_media_send_backpressure_t {
        match self {
            Backpressure::DropToKeyframe => sys::MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME,
            Backpressure::DropGroup => sys::MOQ_MEDIA_SEND_BP_DROP_GROUP,
            Backpressure::BlockTimeout => sys::MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT,
            Backpressure::ReturnWouldBlock => sys::MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK,
        }
    }
}

/// How a [`Sender`] publishes. Start from a preset, then adjust; only the
/// fields set here override the preset's. The C configuration is built
/// with the preset's sized initializer at attach, so every field reaches
/// the service; demand callbacks are not offered (demand is observed through
/// the sender's queries).
#[derive(Clone, Debug)]
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
    publish_tracks: Option<bool>,
    drop_without_demand: Option<bool>,
    catalog_refresh_interval: Option<Duration>,
    companion_profile: Option<CompanionProfile>,
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
            publish_tracks: None,
            drop_without_demand: None,
            catalog_refresh_interval: None,
            companion_profile: None,
        }
    }

    /// Live: never block the encoder; drop to the newest keyframe under
    /// backpressure.
    #[must_use]
    pub fn live(namespace: Namespace) -> SenderConfig {
        SenderConfig::with_preset(namespace, SenderPreset::Live)
    }

    /// Lossless: block the writer under congestion, then surface
    /// [`Error::WOULD_BLOCK`] so the drop decision is the caller's.
    #[must_use]
    pub fn lossless(namespace: Namespace) -> SenderConfig {
        SenderConfig::with_preset(namespace, SenderPreset::Lossless)
    }

    /// Catalog track name; defaults to `catalog`.
    #[must_use]
    pub fn catalog_track(mut self, name: impl Into<String>) -> Self {
        self.catalog_track = Some(name.into());
        self
    }
    /// Override the preset's backpressure policy.
    #[must_use]
    pub fn backpressure(mut self, policy: Backpressure) -> Self {
        self.backpressure = Some(policy);
        self
    }
    /// How long [`Backpressure::BlockTimeout`] blocks, exact microseconds;
    /// zero selects the preset's default.
    #[must_use]
    pub fn block_timeout(mut self, timeout: Duration) -> Self {
        self.block_timeout = Some(timeout);
        self
    }
    /// Send-queue bounds (0 = the service's defaults).
    #[must_use]
    pub fn queue_limits(mut self, max_objects: u32, max_bytes: u32) -> Self {
        self.queue_max_objects = Some(max_objects);
        self.queue_max_bytes = Some(max_bytes);
        self
    }
    /// Pre-ready queue bounds (0 = the service's defaults).
    #[must_use]
    pub fn pre_ready_limits(mut self, max_objects: u32, max_bytes: u32) -> Self {
        self.pre_ready_max_objects = Some(max_objects);
        self.pre_ready_max_bytes = Some(max_bytes);
        self
    }
    /// Validate CMAF objects against CMSF 3.3/3.4 on write (on by default).
    #[must_use]
    pub fn validate_cmaf(mut self, validate: bool) -> Self {
        self.validate_cmaf = Some(validate);
        self
    }
    /// CMSF 4 root contentProtections, copied by the service at attach.
    #[must_use]
    pub fn content_protections(mut self, entries: Vec<ContentProtection>) -> Self {
        self.content_protections = entries;
        self
    }
    /// Also PUBLISH every track and the catalog (push), instead of only
    /// announcing and answering SUBSCRIBE.
    #[must_use]
    pub fn publish_tracks(mut self, publish: bool) -> Self {
        self.publish_tracks = Some(publish);
        self
    }
    /// Drop media written while its track has no subscriber, instead of
    /// queueing it until demand appears.
    #[must_use]
    pub fn drop_without_demand(mut self, drop: bool) -> Self {
        self.drop_without_demand = Some(drop);
        self
    }
    /// Republish the catalog this often while it has subscribers, exact
    /// microseconds; zero is off (the default). Meant for relays that serve
    /// late joiners from cache only.
    #[must_use]
    pub fn catalog_refresh_interval(mut self, interval: Duration) -> Self {
        self.catalog_refresh_interval = Some(interval);
        self
    }
    /// The media companion; [`CompanionProfile::Auto`] by default.
    #[must_use]
    pub fn companion_profile(mut self, profile: CompanionProfile) -> Self {
        self.companion_profile = Some(profile);
        self
    }
}

// -- track configuration ----------------------------------------------------------

/// One track to add to a [`Sender`]: enough to derive its catalog entry.
/// MSF requires a codec and a bitrate for every audio and video track, and
/// a sample rate and channel config for audio; the service refuses a track
/// missing them with [`Error::INVAL`].
///
/// The C configuration is built with the sized initializer, so every field
/// reaches the service. Not offered here and left at the initializer's
/// defaults: the packaging extension records and MPEG-TS settings, the
/// draft-22 track properties and dynamic groups.
#[derive(Clone, Debug)]
pub struct SendTrackConfig {
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
    max_group_sap: Option<u32>,
    max_object_sap: Option<u32>,
    content_protection_ref_ids: Vec<String>,
    sap_timeline: Option<u32>,
    track_duration_ms: Option<u64>,
    media_timeline: Option<u32>,
    alt_group: Option<i32>,
    publisher_priority: Option<u8>,
    render_group: Option<i32>,
    label: Option<String>,
}

impl SendTrackConfig {
    /// A live track. `codec` is the catalog codec string, e.g. `avc1.64001f`.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        media_type: MediaType,
        packaging: Packaging,
        codec: impl Into<String>,
    ) -> SendTrackConfig {
        SendTrackConfig {
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
            max_group_sap: None,
            max_object_sap: None,
            content_protection_ref_ids: Vec::new(),
            sap_timeline: None,
            track_duration_ms: None,
            media_timeline: None,
            alt_group: None,
            publisher_priority: None,
            render_group: None,
            label: None,
        }
    }

    /// Media timescale; 0 means microseconds.
    #[must_use]
    pub fn timescale(mut self, timescale: u32) -> Self {
        self.timescale = timescale;
        self
    }
    /// Decoder config carried in the catalog: a CMAF init segment, avcC,
    /// AudioSpecificConfig, ... Copied by the service at add.
    #[must_use]
    pub fn init_data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.init_data = data.into();
        self
    }
    /// Catalog role; derived from the media type when unset.
    #[must_use]
    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
    /// Catalog language.
    #[must_use]
    pub fn lang(mut self, lang: impl Into<String>) -> Self {
        self.lang = Some(lang.into());
        self
    }
    /// Catalog isLive (true by default).
    #[must_use]
    pub fn live(mut self, is_live: bool) -> Self {
        self.is_live = is_live;
        self
    }
    /// Video dimensions.
    #[must_use]
    pub fn dimensions(mut self, width: u32, height: u32) -> Self {
        self.width = width;
        self.height = height;
        self
    }
    /// Video frame rate, in frames per 1000 seconds (fps * 1000).
    #[must_use]
    pub fn framerate_millis(mut self, millis: u64) -> Self {
        self.framerate_millis = millis;
        self
    }
    /// Audio sample rate.
    #[must_use]
    pub fn samplerate(mut self, rate: u32) -> Self {
        self.samplerate = rate;
        self
    }
    /// Audio channelConfig, e.g. `"2"`.
    #[must_use]
    pub fn channel_config(mut self, config: impl Into<String>) -> Self {
        self.channel_config = Some(config.into());
        self
    }
    /// Maximum bitrate, bits/s.
    #[must_use]
    pub fn bitrate(mut self, bitrate: u64) -> Self {
        self.bitrate = bitrate;
        self
    }
    /// CMSF 3.5.2 maxGrpSapStartingType, an advisory catalog hint.
    #[must_use]
    pub fn max_group_sap(mut self, sap_type: u32) -> Self {
        self.max_group_sap = Some(sap_type);
        self
    }
    /// CMSF 3.5.2 maxObjSapStartingType, an advisory catalog hint.
    #[must_use]
    pub fn max_object_sap(mut self, sap_type: u32) -> Self {
        self.max_object_sap = Some(sap_type);
        self
    }
    /// CMSF 4.1.2 contentProtectionRefIDs: declares the track encrypted.
    /// Each id must name a root entry of the sender config.
    #[must_use]
    pub fn content_protection_ref_ids(mut self, ids: Vec<String>) -> Self {
        self.content_protection_ref_ids = ids;
        self
    }
    /// Generate a CMSF SAP event timeline track `<name>.sap`, keeping
    /// `history_groups` groups of records (0 = default). Every group-start
    /// object written to the track must then declare SAP type 1 or 2.
    #[must_use]
    pub fn sap_timeline(mut self, history_groups: u32) -> Self {
        self.sap_timeline = Some(history_groups);
        self
    }
    /// VOD track duration (requires `live(false)`).
    #[must_use]
    pub fn track_duration_ms(mut self, duration_ms: u64) -> Self {
        self.track_duration_ms = Some(duration_ms);
        self
    }
    /// Generate an MSF media timeline track `<name>.timeline`, keeping
    /// `history_groups` groups of records (0 = default).
    #[must_use]
    pub fn media_timeline(mut self, history_groups: u32) -> Self {
        self.media_timeline = Some(history_groups);
        self
    }
    /// CMSF 3.2 altGroup shared by a switching set.
    #[must_use]
    pub fn alt_group(mut self, group: i32) -> Self {
        self.alt_group = Some(group);
        self
    }
    /// Publisher scheduling preference, 0 highest to 255 lowest (default 128).
    #[must_use]
    pub fn publisher_priority(mut self, priority: u8) -> Self {
        self.publisher_priority = Some(priority);
        self
    }
    /// MSF renderGroup: tracks sharing a value are meant to render together.
    #[must_use]
    pub fn render_group(mut self, group: i32) -> Self {
        self.render_group = Some(group);
        self
    }
    /// MSF label: human-readable track-selection text (UTF-8).
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

// -- handles ----------------------------------------------------------------------

/// A track added to a [`Sender`]. A plain handle: copy it freely. Calls
/// with a handle from another sender fail with [`Error::INVAL`] instead of
/// reaching libmoq; after removal the service keeps the handle valid and
/// inert until the sender is destroyed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SendTrack {
    raw: NonNull<sys::moq_media_track_t>,
    owner: u64,
}

// SAFETY: an opaque identifier; only its sender dereferences it, and only
// after checking the owner.
unsafe impl Send for SendTrack {}
unsafe impl Sync for SendTrack {}

impl SendTrack {
    /// The underlying `moq_media_track_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_track_t {
        self.raw.as_ptr()
    }
}

/// One media object to [`write`](Sender::write). Build it with
/// [`new`](SendObject::new), then set the fields that apply, or spell the
/// ones that differ with a struct update over `new`. It is the
/// application's record, so it is not `#[non_exhaustive]`: a field added to
/// it is a visible change of what a write can say.
#[derive(Debug)]
pub struct SendObject {
    /// The object payload. For CMAF, one moof+mdat chunk sequence.
    pub payload: Buffer,
    /// CMAF only: the object's property block, passed through. Must be
    /// `None` for RAW/LOC, whose properties libmoq generates.
    pub properties: Option<Buffer>,
    /// A random-access point.
    pub is_sync: bool,
    /// Opens a new group.
    pub starts_group: bool,
    /// Closes the group after this object.
    pub ends_group: bool,
    /// Decode time (advisory; write order is decode order).
    pub decode_time_us: u64,
    /// RAW/LOC: presentation time; also drives the generated timelines.
    pub presentation_time_us: u64,
    /// RAW/LOC: wall-clock capture time, microseconds since the Unix epoch,
    /// emitted as the LOC timestamp; the presentation time is used when
    /// unset.
    pub capture_time_us: Option<u64>,
    /// CMAF: the declared SAP type of the object's first sample
    /// ([`SapType::Unknown`] is refused by the service).
    pub sap_type: Option<SapType>,
}

impl SendObject {
    /// A delta object with no timing.
    #[must_use]
    pub fn new(payload: Buffer) -> SendObject {
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

/// A failed [`write`](Sender::write): the error and the object, which the
/// caller still owns. Converting it into an [`Error`] drops the object.
/// The pair is the whole record (destructure it freely); it is not
/// `#[non_exhaustive]`.
#[derive(Debug)]
#[must_use]
pub struct WriteError {
    /// What libmoq reported.
    pub error: Error,
    /// The object, untouched.
    pub object: SendObject,
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "media write failed: {}", self.error)
    }
}

impl std::error::Error for WriteError {}

impl From<WriteError> for Error {
    fn from(failure: WriteError) -> Error {
        failure.error
    }
}

/// A [`Sender`] statistics snapshot. `objects_written` equals
/// `objects_sent + objects_queued + objects_dropped`; `objects_sent` means
/// handed to the session, not delivered.
#[non_exhaustive]
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
    /// The most recent failure the sender recorded; `None` for none.
    pub last_error: Option<Error>,
    /// Generated SAP timeline records aged out of the history; `None` when
    /// the library did not report the field.
    pub sap_records_evicted: Option<u64>,
    /// The first failure that stopped a graceful completion, sticky;
    /// `None` when none was recorded or the library did not report the
    /// field. It does not say that a completion succeeded.
    pub completion_error: Option<Error>,
}

// -- the sender -------------------------------------------------------------------

/// Publishes a namespace: an MSF/CMSF catalog derived from its tracks, and
/// their media objects.
///
/// Calls come from application threads, one at a time: the type is `Send`
/// but not `Sync` (share it behind a `Mutex`). It holds its [`Endpoint`],
/// which therefore outlives it; dropping it destroys the sender, discarding
/// what the service still had queued (drain the endpoint first for a
/// graceful finish).
#[derive(Debug)]
pub struct Sender {
    raw: NonNull<sys::moq_media_sender_t>,
    owner: u64,
    endpoint: Endpoint,
    _not_sync: PhantomData<Cell<()>>,
}

// SAFETY: libmoq accepts sender calls from any application thread as long
// as they are serialized, which `!Sync` enforces.
unsafe impl Send for Sender {}

const SENDER_STATS_V0: usize = ffi::field_end!(sys::moq_media_sender_stats_t, last_error, i32);

impl Sender {
    /// Attach a sender to `endpoint`. Add tracks after this; the catalog is
    /// published once the namespace is accepted, and a track added after
    /// readiness republishes it.
    pub fn attach(endpoint: &Endpoint, config: &SenderConfig) -> Result<Sender> {
        let block_timeout_us = match config.block_timeout {
            Some(timeout) => Some(ffi::micros(timeout)?),
            None => None,
        };
        let refresh_us = match config.catalog_refresh_interval {
            Some(interval) => Some(ffi::micros(interval)?),
            None => None,
        };

        let mut raw_config = sys::moq_media_sender_cfg_t::default();
        let size = mem::size_of_val(&raw_config);
        // SAFETY: a writable struct of exactly the size passed.
        unsafe {
            match config.preset {
                SenderPreset::Live => {
                    sys::moq_media_sender_cfg_init_live_sized(&mut raw_config, size)
                }
                SenderPreset::Lossless => {
                    sys::moq_media_sender_cfg_init_lossless_sized(&mut raw_config, size)
                }
            }
        }
        raw_config.endpoint = ptr::null(); // attach: the endpoint is given
        raw_config.namespace_ = config.namespace.raw();
        raw_config.catalog_track = ffi::optional_span(config.catalog_track.as_deref());
        if let Some(policy) = config.backpressure {
            raw_config.backpressure = policy.raw();
        }
        if let Some(timeout_us) = block_timeout_us {
            raw_config.block_timeout_us = timeout_us;
        }
        if let Some(max_objects) = config.queue_max_objects {
            raw_config.queue_max_objects = max_objects;
        }
        if let Some(max_bytes) = config.queue_max_bytes {
            raw_config.queue_max_bytes = max_bytes;
        }
        if let Some(max_objects) = config.pre_ready_max_objects {
            raw_config.pre_ready_max_objects = max_objects;
        }
        if let Some(max_bytes) = config.pre_ready_max_bytes {
            raw_config.pre_ready_max_bytes = max_bytes;
        }
        if let Some(validate) = config.validate_cmaf {
            raw_config.validate_cmaf = validate;
        }
        if let Some(publish) = config.publish_tracks {
            raw_config.publish_tracks = publish;
        }
        if let Some(drop) = config.drop_without_demand {
            raw_config.drop_without_demand = drop;
        }
        if let Some(interval_us) = refresh_us {
            raw_config.catalog_refresh_interval_us = interval_us;
        }
        if let Some(profile) = config.companion_profile {
            raw_config.companion_profile = profile.raw();
        }

        // The content-protection records, with each entry's defaultKID
        // spans in a buffer of its own; everything borrows `config` for the
        // call and the service deep-copies at attach.
        let mut kid_spans: Vec<Vec<sys::moq_bytes_t>> =
            Vec::with_capacity(config.content_protections.len());
        for entry in &config.content_protections {
            let mut spans = Vec::with_capacity(entry.default_kids.len());
            for kid in &entry.default_kids {
                spans.push(ffi::span(kid.as_bytes()));
            }
            kid_spans.push(spans);
        }
        let mut protections: Vec<sys::moq_cmsf_content_protection_t> =
            Vec::with_capacity(config.content_protections.len());
        let entries = config.content_protections.iter().zip(kid_spans.iter_mut());
        for (entry, spans) in entries {
            let drm = &entry.drm_system;
            protections.push(sys::moq_cmsf_content_protection_t {
                ref_id: ffi::span(entry.ref_id.as_bytes()),
                default_kids: if spans.is_empty() {
                    ptr::null_mut()
                } else {
                    spans.as_mut_ptr()
                },
                default_kid_count: spans.len(),
                scheme: ffi::span(entry.scheme.as_bytes()),
                drm_system: sys::moq_cmsf_drm_system_t {
                    system_id: ffi::span(drm.system_id.as_bytes()),
                    la_url: raw_url(drm.la_url.as_ref()),
                    cert_url: raw_url(drm.cert_url.as_ref()),
                    auth_url: raw_url(drm.auth_url.as_ref()),
                    has_pssh: drm.pssh.is_some(),
                    pssh: ffi::optional_span(drm.pssh.as_deref()),
                    has_robustness: drm.robustness.is_some(),
                    robustness: ffi::optional_span(drm.robustness.as_deref()),
                },
            });
        }
        if !protections.is_empty() {
            raw_config.content_protections = protections.as_ptr();
            raw_config.content_protection_count = protections.len();
        }

        let mut raw = ptr::null_mut();
        // SAFETY: every pointer in raw_config points into config, kid_spans
        // or protections, alive for the call; the service copies the
        // namespace, catalog name and content protections at attach.
        ffi::check(unsafe {
            sys::moq_media_sender_attach(endpoint.as_ptr(), &raw_config, &mut raw)
        })?;
        let raw = NonNull::new(raw).ok_or(Error::INTERNAL)?;
        Ok(Sender {
            raw,
            owner: next_owner_id(),
            endpoint: endpoint.clone(),
            _not_sync: PhantomData,
        })
    }

    /// The endpoint this sender is attached to.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The underlying `moq_media_sender_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_sender_t {
        self.raw.as_ptr()
    }

    fn own(&self, track: SendTrack) -> Result<*mut sys::moq_media_track_t> {
        if track.owner == self.owner {
            Ok(track.raw.as_ptr())
        } else {
            Err(Error::INVAL)
        }
    }

    /// Add a track. Before readiness it joins the initial catalog; after,
    /// it triggers a catalog republish. [`Error::WOULD_BLOCK`] means the
    /// name is still held by a just-removed track's teardown: retry.
    pub fn add_track(&self, config: &SendTrackConfig) -> Result<SendTrack> {
        let mut raw_config = sys::moq_media_track_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe {
            sys::moq_media_track_cfg_init_sized(&mut raw_config, mem::size_of_val(&raw_config))
        };
        raw_config.name = ffi::span(config.name.as_bytes());
        raw_config.media_type = config.media_type.raw();
        raw_config.packaging = config.packaging.raw();
        raw_config.codec = ffi::span(config.codec.as_bytes());
        raw_config.timescale = config.timescale;
        raw_config.init_data = ffi::span(&config.init_data);
        raw_config.role = ffi::optional_span(config.role.as_deref());
        raw_config.lang = ffi::optional_span(config.lang.as_deref());
        raw_config.is_live = config.is_live;
        raw_config.width = config.width;
        raw_config.height = config.height;
        raw_config.framerate_millis = config.framerate_millis;
        raw_config.samplerate = config.samplerate;
        raw_config.channel_config = ffi::optional_span(config.channel_config.as_deref());
        raw_config.bitrate = config.bitrate;
        raw_config.has_max_grp_sap = config.max_group_sap.is_some();
        raw_config.max_grp_sap = config.max_group_sap.unwrap_or(0);
        raw_config.has_max_obj_sap = config.max_object_sap.is_some();
        raw_config.max_obj_sap = config.max_object_sap.unwrap_or(0);
        let mut ref_ids: Vec<sys::moq_bytes_t> =
            Vec::with_capacity(config.content_protection_ref_ids.len());
        for ref_id in &config.content_protection_ref_ids {
            ref_ids.push(ffi::span(ref_id.as_bytes()));
        }
        if !ref_ids.is_empty() {
            raw_config.content_protection_ref_ids = ref_ids.as_ptr();
            raw_config.content_protection_ref_id_count = ref_ids.len();
        }
        if let Some(history_groups) = config.sap_timeline {
            raw_config.emit_sap_timeline = true;
            raw_config.sap_timeline_history_groups = history_groups;
        }
        if let Some(duration_ms) = config.track_duration_ms {
            raw_config.has_track_duration = true;
            raw_config.track_duration_ms = duration_ms;
        }
        if let Some(history_groups) = config.media_timeline {
            raw_config.emit_media_timeline = true;
            raw_config.media_timeline_history_groups = history_groups;
        }
        if let Some(group) = config.alt_group {
            raw_config.has_alt_group = true;
            raw_config.alt_group = group;
        }
        if let Some(priority) = config.publisher_priority {
            raw_config.has_publisher_priority = true;
            raw_config.publisher_priority = priority;
        }
        if let Some(group) = config.render_group {
            raw_config.has_render_group = true;
            raw_config.render_group = group;
        }
        if let Some(label) = &config.label {
            raw_config.has_label = true;
            raw_config.label = ffi::span(label.as_bytes());
        }
        let mut raw = ptr::null_mut();
        // SAFETY: every pointer in raw_config points into config or ref_ids,
        // alive for the call; the service copies what it keeps.
        ffi::check(unsafe {
            sys::moq_media_sender_add_track(self.as_ptr(), &raw_config, &mut raw)
        })?;
        Ok(SendTrack {
            raw: NonNull::new(raw).ok_or(Error::INTERNAL)?,
            owner: self.owner,
        })
    }

    /// Remove a track from the catalog and end its publication; its queued
    /// objects are discarded. The handle stays valid but inert.
    pub fn remove_track(&self, track: SendTrack) -> Result<()> {
        let track = self.own(track)?;
        // SAFETY: a live sender and one of its handles.
        ffi::check(unsafe { sys::moq_media_sender_remove_track(self.as_ptr(), track) })
    }

    /// Submit one object; per track, write order is decode order. On
    /// success libmoq owns the buffers; on failure they come back in the
    /// error, untouched. Accepted means accepted by the service, not
    /// delivered: under a drop policy an accepted object may still be
    /// evicted.
    pub fn write(
        &self,
        track: SendTrack,
        object: SendObject,
    ) -> std::result::Result<(), WriteError> {
        let track = match self.own(track) {
            Ok(track) => track,
            Err(error) => return Err(WriteError { error, object }),
        };
        let raw_object = sys::moq_media_send_object_t {
            struct_size: mem::size_of::<sys::moq_media_send_object_t>() as u32,
            payload: object.payload.as_ptr(),
            properties: object
                .properties
                .as_ref()
                .map_or(ptr::null_mut(), |properties| properties.as_ptr()),
            is_sync: object.is_sync,
            starts_group: object.starts_group,
            ends_group: object.ends_group,
            decode_time_us: object.decode_time_us,
            presentation_time_us: object.presentation_time_us,
            has_capture_time: object.capture_time_us.is_some(),
            capture_time_us: object.capture_time_us.unwrap_or(0),
            has_sap_type: object.sap_type.is_some(),
            sap_type: object.sap_type.map_or(sys::MOQ_SAP_NONE, SapType::raw),
            metadata: ptr::null(),
        };
        // SAFETY: a live sender and one of its handles; the buffers are
        // live references this object owns.
        let code = unsafe { sys::moq_media_sender_write(self.as_ptr(), track, &raw_object) };
        if code != sys::MOQ_OK {
            return Err(WriteError {
                error: Error::from_code(code),
                object,
            });
        }
        // Transferred: the references are libmoq's now.
        let _ = object.payload.into_raw();
        if let Some(properties) = object.properties {
            let _ = properties.into_raw();
        }
        Ok(())
    }

    /// End a finite track once its queued objects drain: a reliable
    /// END_OF_TRACK follows, which receivers see as the track ending.
    /// Idempotent. May fail with [`Error::WOULD_BLOCK`] while the queue is
    /// full: retry.
    pub fn end_track(&self, track: SendTrack) -> Result<()> {
        let track = self.own(track)?;
        // SAFETY: a live sender and one of its handles.
        ffi::check(unsafe { sys::moq_media_sender_end_track(self.as_ptr(), track) })
    }

    /// Terminate the broadcast (MSF 11.3): end every track, then publish a
    /// terminal catalog with isComplete. Legal only after readiness;
    /// asynchronous and idempotent. A failure that stops it is recorded in
    /// [`SenderStats::completion_error`].
    pub fn complete(&self) -> Result<()> {
        // SAFETY: a live sender.
        ffi::check(unsafe { sys::moq_media_sender_complete(self.as_ptr()) })
    }

    /// Convert live tracks to VOD in place, each with its duration in
    /// milliseconds, as one catalog generation. All or nothing.
    pub fn convert_to_vod(&self, tracks: impl IntoIterator<Item = (SendTrack, u64)>) -> Result<()> {
        let mut items: Vec<sys::moq_media_vod_track_t> = Vec::new();
        for (track, duration_ms) in tracks {
            items.push(sys::moq_media_vod_track_t {
                track: self.own(track)?,
                duration_ms,
            });
        }
        // SAFETY: a live sender; items holds its own handles for the call.
        ffi::check(unsafe {
            sys::moq_media_sender_convert_to_vod(self.as_ptr(), items.as_ptr(), items.len())
        })
    }

    /// A statistics snapshot.
    pub fn stats(&self) -> Result<SenderStats> {
        let mut stats = sys::moq_media_sender_stats_t::default();
        let size = mem::size_of_val(&stats);
        // SAFETY: a writable struct of exactly the size passed.
        ffi::check(unsafe { sys::moq_media_sender_get_stats(self.as_ptr(), &mut stats, size) })?;
        ffi::check_stamp(stats.struct_size, SENDER_STATS_V0, size)?;
        let covers = |end: usize| ffi::covers(stats.struct_size, end);
        let evicted_end = ffi::field_end!(sys::moq_media_sender_stats_t, sap_records_evicted, u64);
        let completion_end = ffi::field_end!(sys::moq_media_sender_stats_t, completion_error, i32);
        Ok(SenderStats {
            objects_written: stats.objects_written,
            objects_sent: stats.objects_sent,
            objects_queued: stats.objects_queued,
            bytes_queued: stats.bytes_queued,
            objects_dropped: stats.objects_dropped,
            groups_dropped: stats.groups_dropped,
            keyframes_dropped: stats.keyframes_dropped,
            groups_abandoned: stats.groups_abandoned,
            backpressure_stalls: stats.backpressure_stalls,
            last_error: if stats.last_error == sys::MOQ_OK {
                None
            } else {
                Some(Error::from_code(stats.last_error))
            },
            sap_records_evicted: if covers(evicted_end) {
                Some(stats.sap_records_evicted)
            } else {
                None
            },
            completion_error: if covers(completion_end) && stats.completion_error != sys::MOQ_OK {
                Some(Error::from_code(stats.completion_error))
            } else {
                None
            },
        })
    }

    /// The namespace is accepted and the catalog published, so the publish
    /// path can accept media. Says nothing about subscribers.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        // SAFETY: a live sender.
        unsafe { sys::moq_media_sender_is_ready(self.as_ptr()) }
    }

    /// Block until the write level holds (ready with send-queue headroom),
    /// the endpoint wakes, or `timeout` (exact microseconds; zero polls)
    /// elapses. [`WaitResult::Ready`] is advice to try a write, not a
    /// promise that it succeeds and not a sign of demand. The latch and
    /// terminal state win over the level: [`Error::INTERRUPTED`],
    /// [`Error::CLOSED`]; [`Error::UNSUPPORTED`] when the companion cannot
    /// serve the negotiated draft.
    pub fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        let timeout_us = ffi::micros(timeout)?;
        // SAFETY: a live sender, called from an application thread.
        ffi::wait_result(unsafe { sys::moq_media_sender_wait(self.as_ptr(), timeout_us) })
    }

    /// Active subscriptions on `track`, a point-in-time snapshot.
    pub fn track_subscriptions(&self, track: SendTrack) -> Result<usize> {
        let track = self.own(track)?;
        // SAFETY: a live sender and one of its handles; any thread.
        Ok(unsafe { sys::moq_media_sender_track_subscriptions(self.as_ptr(), track) })
    }

    /// `track` has a subscriber now.
    pub fn track_has_subscriber(&self, track: SendTrack) -> Result<bool> {
        let track = self.own(track)?;
        // SAFETY: a live sender and one of its handles; any thread.
        Ok(unsafe { sys::moq_media_sender_track_has_subscriber(self.as_ptr(), track) })
    }

    /// Any app-visible media track has a subscriber: worth encoding. A
    /// catalog-only subscription does not count.
    #[must_use]
    pub fn has_media_subscriber(&self) -> bool {
        // SAFETY: a live sender; any thread.
        unsafe { sys::moq_media_sender_has_media_subscriber(self.as_ptr()) }
    }

    /// Closed cleanly.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        // SAFETY: a live sender.
        unsafe { sys::moq_media_sender_is_closed(self.as_ptr()) }
    }

    /// Failed (connect, TLS, protocol or sender failure).
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        // SAFETY: a live sender.
        unsafe { sys::moq_media_sender_is_fatal(self.as_ptr()) }
    }

    /// The sender's own fatal code when sender-fatal, otherwise the
    /// endpoint's.
    #[must_use]
    pub fn fatal_code(&self) -> u64 {
        // SAFETY: a live sender.
        unsafe { sys::moq_media_sender_fatal_code(self.as_ptr()) }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        // SAFETY: the sole owner; destroy detaches before the endpoint
        // handle below is released.
        unsafe { sys::moq_media_sender_destroy(self.raw.as_ptr()) }
    }
}
