use std::marker::PhantomData;
use std::ptr::NonNull;
use std::time::Duration;

use crate::cmaf::{InitInfo, Sample};
use crate::endpoint::Endpoint;
use crate::error::{check, Error, Result};
use crate::ffi;
use crate::namespace::Namespace;
use crate::sender::next_owner_id;
use crate::sys;
use crate::types::{array, bytes, micros, span, wait_result, MediaType, Packaging, SapType, Wait};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiverPreset {
    Live,
    FlowControl,
}

/// How reported timestamps relate to the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeMode {
    /// Pass wire timestamps through.
    Raw,
    /// Rebase reported timestamps against one shared epoch (payloads are
    /// never modified).
    SharedEpoch,
}

/// How a [`MediaReceiver`] subscribes. Start from a preset, then adjust;
/// only the fields set here override the preset's.
pub struct ReceiverConfig {
    namespace: Namespace,
    preset: ReceiverPreset,
    catalog_track: Option<String>,
    auto_subscribe: Option<bool>,
    time_mode: Option<TimeMode>,
    max_objects: Option<u32>,
    max_bytes: Option<u64>,
    max_track_events: Option<u32>,
}

impl ReceiverConfig {
    fn with_preset(namespace: Namespace, preset: ReceiverPreset) -> ReceiverConfig {
        ReceiverConfig {
            namespace,
            preset,
            catalog_track: None,
            auto_subscribe: None,
            time_mode: None,
            max_objects: None,
            max_bytes: None,
            max_track_events: None,
        }
    }

    /// Live rendering: on overflow, drop to the newest keyframe.
    pub fn live(namespace: Namespace) -> ReceiverConfig {
        ReceiverConfig::with_preset(namespace, ReceiverPreset::Live)
    }

    /// Flow control: on overflow, pause the subscriptions upstream.
    pub fn flow_control(namespace: Namespace) -> ReceiverConfig {
        ReceiverConfig::with_preset(namespace, ReceiverPreset::FlowControl)
    }

    /// Catalog track name; defaults to `catalog`.
    pub fn catalog_track(mut self, name: impl Into<String>) -> Self {
        self.catalog_track = Some(name.into());
        self
    }
    /// Subscribe every eligible media track on discovery (`true`), or only
    /// the ones passed to [`MediaReceiver::subscribe_track`] (`false`).
    pub fn auto_subscribe(mut self, auto: bool) -> Self {
        self.auto_subscribe = Some(auto);
        self
    }
    /// Timestamp mode.
    pub fn time_mode(mut self, mode: TimeMode) -> Self {
        self.time_mode = Some(mode);
        self
    }
    /// Object-queue bounds (0 = libmoq's defaults).
    pub fn queue_limits(mut self, max_objects: u32, max_bytes: u64) -> Self {
        self.max_objects = Some(max_objects);
        self.max_bytes = Some(max_bytes);
        self
    }
    /// Track-event queue depth. Overflowing it is fatal.
    pub fn max_track_events(mut self, n: u32) -> Self {
        self.max_track_events = Some(n);
        self
    }
}

/// A track discovered by a [`MediaReceiver`]. A plain handle; calls with a
/// handle from another receiver fail with [`Error::INVAL`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiverTrack {
    raw: NonNull<sys::moq_media_track_t>,
    owner: u64,
}

// SAFETY: an opaque identifier, dereferenced only by its receiver after the
// owner check.
unsafe impl Send for ReceiverTrack {}
unsafe impl Sync for ReceiverTrack {}

impl ReceiverTrack {
    /// The underlying `moq_media_track_t`.
    pub fn as_ptr(&self) -> *mut sys::moq_media_track_t {
        self.raw.as_ptr()
    }
}

impl std::fmt::Debug for ReceiverTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ReceiverTrack").field(&self.raw).finish()
    }
}

/// A track's catalog description. Borrowed from the receiver, which keeps it
/// valid until it is dropped.
#[derive(Clone, Copy)]
pub struct TrackDesc<'r>(&'r sys::moq_media_track_desc_t);

impl<'r> TrackDesc<'r> {
    /// Catalog track name.
    pub fn name(&self) -> &'r [u8] {
        unsafe { bytes(self.0.name) }
    }
    /// Role; empty when absent.
    pub fn role(&self) -> &'r [u8] {
        unsafe { bytes(self.0.role) }
    }
    /// Codec string, e.g. `avc1.64001f`.
    pub fn codec(&self) -> &'r [u8] {
        unsafe { bytes(self.0.codec) }
    }
    /// Language; empty when absent.
    pub fn lang(&self) -> &'r [u8] {
        unsafe { bytes(self.0.lang) }
    }
    /// Label; empty when absent.
    pub fn label(&self) -> &'r [u8] {
        unsafe { bytes(self.0.label) }
    }
    /// Media type, when derived.
    pub fn media_type(&self) -> Option<MediaType> {
        MediaType::from_raw(self.0.info.media_type)
    }
    /// Media packaging, when derived. Non-media tracks (timelines) have
    /// none; see [`packaging_name`](TrackDesc::packaging_name).
    pub fn packaging(&self) -> Option<Packaging> {
        Packaging::from_raw(self.0.info.packaging)
    }
    /// The catalog packaging string, verbatim: `cmaf`, `loc`,
    /// `eventtimeline`, `mediatimeline`, ...
    pub fn packaging_name(&self) -> &'r [u8] {
        unsafe { bytes(self.0.packaging) }
    }
    /// Media timescale.
    pub fn timescale(&self) -> u32 {
        self.0.info.timescale
    }
    /// The init data could be parsed into [`init_info`](TrackDesc::init_info).
    pub fn has_init(&self) -> bool {
        self.0.has_init
    }
    /// The decoder config parsed from the init data; zeroed (unknown codec,
    /// empty config) when [`has_init`](TrackDesc::has_init) is false.
    pub fn init_info(&self) -> InitInfo<'r> {
        // SAFETY: the spans are handle-owned, valid while the receiver lives.
        unsafe { InitInfo::from_raw(&self.0.init) }
    }
    /// The full init data from the catalog: a CMAF init segment, or whatever
    /// a LOC sender supplied.
    pub fn init_data(&self) -> &'r [u8] {
        unsafe { bytes(self.0.init_data) }
    }
    /// Catalog width.
    pub fn width(&self) -> Option<u32> {
        self.0.has_width.then_some(self.0.width)
    }
    /// Catalog height.
    pub fn height(&self) -> Option<u32> {
        self.0.has_height.then_some(self.0.height)
    }
    /// Catalog sample rate.
    pub fn samplerate(&self) -> Option<u32> {
        self.0.has_samplerate.then_some(self.0.samplerate)
    }
    /// Catalog channelConfig, e.g. `2`; empty when absent.
    pub fn channel_config(&self) -> &'r [u8] {
        unsafe { bytes(self.0.channel_config) }
    }
    /// Frames per 1000 seconds.
    pub fn framerate_millis(&self) -> Option<u64> {
        self.0.has_framerate.then_some(self.0.framerate_millis)
    }
    /// Maximum bitrate.
    pub fn bitrate(&self) -> Option<u64> {
        self.0.has_bitrate.then_some(self.0.bitrate)
    }
    /// CMSF 3.5.2 maximum SAP starting types (group, object).
    pub fn max_sap_types(&self) -> (Option<u32>, Option<u32>) {
        (
            self.0.has_max_grp_sap.then_some(self.0.max_grp_sap),
            self.0.has_max_obj_sap.then_some(self.0.max_obj_sap),
        )
    }
    /// CMSF 4.1.2 contentProtectionRefIDs; resolve them with
    /// [`MediaReceiver::find_content_protection`].
    pub fn content_protection_ref_ids(&self) -> impl Iterator<Item = &'r [u8]> {
        let ids = unsafe {
            array(
                self.0.content_protection_ref_ids,
                self.0.content_protection_ref_id_count,
            )
        };
        ids.iter().map(|b| unsafe { bytes(*b) })
    }
    /// Timeline eventType.
    pub fn event_type(&self) -> &'r [u8] {
        unsafe { bytes(self.0.event_type) }
    }
    /// mimeType.
    pub fn mime_type(&self) -> &'r [u8] {
        unsafe { bytes(self.0.mime_type) }
    }
    /// The track names a timeline applies to.
    pub fn depends(&self) -> impl Iterator<Item = &'r [u8]> {
        let d = unsafe { array(self.0.depends, self.0.depends_count) };
        d.iter().map(|b| unsafe { bytes(*b) })
    }
    /// VOD track duration.
    pub fn track_duration_ms(&self) -> Option<u64> {
        self.0
            .has_track_duration
            .then_some(self.0.track_duration_ms)
    }
    /// Catalog isLive.
    pub fn is_live(&self) -> bool {
        self.0.is_live
    }
    /// The raw description.
    pub fn raw(&self) -> &'r sys::moq_media_track_desc_t {
        self.0
    }
}

impl std::fmt::Debug for TrackDesc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackDesc")
            .field("name", &String::from_utf8_lossy(self.name()))
            .field("codec", &String::from_utf8_lossy(self.codec()))
            .field("packaging", &String::from_utf8_lossy(self.packaging_name()))
            .finish_non_exhaustive()
    }
}

/// What a track event reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackEventKind {
    /// A track was discovered.
    Added,
    /// A track converted live to VOD; its description changed in place.
    Updated,
    /// A later catalog dropped the track.
    Removed,
    /// The peer ended or rejected the track.
    Ended,
    /// The first catalog is enumerated. Once per connection; no track.
    CatalogReady,
    /// The peer acknowledged an application pause/resume.
    UpdateOk,
    /// Media objects for the track failed to parse and were dropped.
    ParseDrop,
    /// The track's subscription metadata could not be retained locally; that
    /// track generation has failed (not a peer rejection).
    LocalFailure,
    /// One fill of the track ended; the track's own state is unchanged.
    FillTerminal,
    /// Local delivery capacity refused the track's objects: a live track
    /// fails, a fill fails only that fill.
    DeliveryLocalFailure,
    /// A kind this crate does not know yet.
    Other(u32),
}

/// A track-discovery event, borrowed from its receiver.
pub struct TrackEvent<'r> {
    raw: sys::moq_media_track_event_t,
    owner: u64,
    _r: PhantomData<&'r MediaReceiver>,
}

impl<'r> TrackEvent<'r> {
    /// What happened.
    pub fn kind(&self) -> TrackEventKind {
        match self.raw.kind {
            sys::MOQ_MEDIA_TRACK_ADDED => TrackEventKind::Added,
            sys::MOQ_MEDIA_TRACK_UPDATED => TrackEventKind::Updated,
            sys::MOQ_MEDIA_TRACK_REMOVED => TrackEventKind::Removed,
            sys::MOQ_MEDIA_TRACK_ENDED => TrackEventKind::Ended,
            sys::MOQ_MEDIA_CATALOG_READY => TrackEventKind::CatalogReady,
            sys::MOQ_MEDIA_TRACK_UPDATE_OK => TrackEventKind::UpdateOk,
            sys::MOQ_MEDIA_TRACK_PARSE_DROP => TrackEventKind::ParseDrop,
            sys::MOQ_MEDIA_TRACK_LOCAL_FAILURE => TrackEventKind::LocalFailure,
            sys::MOQ_MEDIA_TRACK_FILL_TERMINAL => TrackEventKind::FillTerminal,
            sys::MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE => TrackEventKind::DeliveryLocalFailure,
            k => TrackEventKind::Other(k),
        }
    }
    /// The track; `None` for [`TrackEventKind::CatalogReady`].
    pub fn track(&self) -> Option<ReceiverTrack> {
        NonNull::new(self.raw.track).map(|raw| ReceiverTrack {
            raw,
            owner: self.owner,
        })
    }
    /// The track's description; `None` for
    /// [`TrackEventKind::CatalogReady`].
    pub fn desc(&self) -> Option<TrackDesc<'r>> {
        // SAFETY: descriptions are handle-owned and live as long as the
        // receiver this event borrows.
        unsafe { self.raw.desc.as_ref() }.map(TrackDesc)
    }
    /// The raw event.
    pub fn raw(&self) -> &sys::moq_media_track_event_t {
        &self.raw
    }
}

impl std::fmt::Debug for TrackEvent<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackEvent")
            .field("kind", &self.kind())
            .field("track", &self.track())
            .field("desc", &self.desc())
            .finish()
    }
}

/// A received media object. It owns its buffers until dropped, and is
/// released on the polling thread: it is neither `Send` nor `Sync`. Copy the
/// bytes out to hand them to another thread.
pub struct MediaObject {
    raw: sys::moq_media_object_t,
    owner: u64,
    _not_send: PhantomData<*const ()>,
}

impl MediaObject {
    /// The track it belongs to.
    pub fn track(&self) -> Option<ReceiverTrack> {
        NonNull::new(self.raw.track).map(|raw| ReceiverTrack {
            raw,
            owner: self.owner,
        })
    }
    /// Its packaging.
    pub fn packaging(&self) -> Option<Packaging> {
        Packaging::from_raw(self.raw.packaging)
    }
    /// A random-access point.
    pub fn is_keyframe(&self) -> bool {
        self.raw.keyframe
    }
    /// The last object of its group.
    pub fn end_of_group(&self) -> bool {
        self.raw.end_of_group
    }
    /// Arrived as a datagram.
    pub fn is_datagram(&self) -> bool {
        self.raw.datagram
    }
    /// The MoQ object status.
    pub fn status(&self) -> u8 {
        self.raw.status
    }
    /// LOC capture time, microseconds since the Unix epoch.
    pub fn capture_time_us(&self) -> Option<u64> {
        self.raw
            .has_capture_time
            .then_some(self.raw.capture_time_us)
    }
    /// Decode time.
    pub fn decode_time_us(&self) -> u64 {
        self.raw.decode_time_us
    }
    /// Composition offset.
    pub fn composition_offset_us(&self) -> i64 {
        self.raw.composition_offset_us
    }
    /// Presentation time.
    pub fn presentation_time_us(&self) -> u64 {
        self.raw.presentation_time_us
    }
    /// LOC: the media bytes. Empty for CMAF.
    pub fn payload(&self) -> &[u8] {
        unsafe { bytes(self.raw.payload) }
    }
    /// CMAF: the full fragment, container framing included. Empty for LOC.
    pub fn fragment(&self) -> &[u8] {
        unsafe { bytes(self.raw.fragment) }
    }
    /// CMAF: the (first chunk's) mdat payload within the fragment.
    pub fn mdat(&self) -> &[u8] {
        let f = self.fragment();
        let start = self.raw.mdat_offset.min(f.len());
        let end = start.saturating_add(self.raw.mdat_len).min(f.len());
        &f[start..end]
    }
    /// CMAF: the sample table.
    pub fn samples(&self) -> Vec<Sample> {
        unsafe { array(self.raw.samples, self.raw.sample_count) }
            .iter()
            .map(|s| Sample {
                duration: s.duration,
                size: s.size,
                flags: s.flags,
                composition_offset: s.composition_offset,
            })
            .collect()
    }
    /// The raw object.
    pub fn raw(&self) -> &sys::moq_media_object_t {
        &self.raw
    }
}

impl Drop for MediaObject {
    fn drop(&mut self) {
        unsafe { ffi::moq_media_object_cleanup(&mut self.raw) }
    }
}

impl std::fmt::Debug for MediaObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaObject")
            .field("track", &self.track())
            .field("packaging", &self.packaging())
            .field("keyframe", &self.is_keyframe())
            .field("presentation_time_us", &self.presentation_time_us())
            .finish_non_exhaustive()
    }
}

/// A CMSF SAP timeline record (from a subscribed `eventtimeline` track).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct SapRecord {
    pub track: ReceiverTrack,
    pub group: u64,
    pub object: u64,
    pub sap_type: SapType,
    pub ept_ms: u64,
}

/// An MSF media timeline record (from a subscribed `mediatimeline` track).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct TimelineRecord {
    pub track: ReceiverTrack,
    pub media_time_ms: u64,
    pub group: u64,
    pub object: u64,
    /// 0 for VOD or unknown.
    pub wallclock_ms: u64,
}

/// Where a subscription starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StartMode {
    /// The live edge.
    #[default]
    Current,
    /// The next group boundary.
    NextGroup,
}

/// Options applied when a track's subscription is first issued.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscribeOptions {
    /// Where delivery starts.
    pub start: StartMode,
    /// Subscriber priority.
    pub priority: Option<u8>,
}

/// A track's delivery state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum TrackState {
    Discovered,
    Pending,
    Active,
    /// Paused by [`MediaReceiver::unsubscribe_track`].
    PausedApp,
    /// Paused by flow-control backpressure.
    PausedFlow,
    Ended,
    /// Failed locally; never rearmed.
    Failed,
    Other(u32),
}

/// A [`MediaReceiver`] statistics snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct ReceiverStats {
    pub objects_received: u64,
    pub objects_queued: u64,
    pub bytes_queued: u64,
    pub objects_dropped: u64,
    pub groups_dropped: u64,
    pub keyframes_dropped: u64,
    pub parse_drops: u64,
    pub overflow_events: u64,
    pub pause_transitions: u64,
    pub paused: bool,
    pub catalog_drops: u64,
    pub catalog_complete: bool,
}

/// A CMSF content-protection entry resolved from a receiver's catalog.
#[derive(Clone, Copy)]
pub struct ContentProtectionRef<'r>(&'r sys::moq_cmsf_content_protection_t);

impl<'r> ContentProtectionRef<'r> {
    /// refID.
    pub fn ref_id(&self) -> &'r [u8] {
        unsafe { bytes(self.0.ref_id) }
    }
    /// defaultKID strings.
    pub fn default_kids(&self) -> impl Iterator<Item = &'r [u8]> {
        unsafe { array(self.0.default_kids, self.0.default_kid_count) }
            .iter()
            .map(|b| unsafe { bytes(*b) })
    }
    /// Scheme.
    pub fn scheme(&self) -> &'r [u8] {
        unsafe { bytes(self.0.scheme) }
    }
    /// drmSystem.systemID.
    pub fn system_id(&self) -> &'r [u8] {
        unsafe { bytes(self.0.drm_system.system_id) }
    }
    /// laURL.
    pub fn la_url(&self) -> Option<&'r [u8]> {
        let u = &self.0.drm_system.la_url;
        u.present.then(|| unsafe { bytes(u.url) })
    }
    /// Base64 PSSH.
    pub fn pssh(&self) -> Option<&'r [u8]> {
        let d = &self.0.drm_system;
        d.has_pssh.then(|| unsafe { bytes(d.pssh) })
    }
    /// The raw entry.
    pub fn raw(&self) -> &'r sys::moq_cmsf_content_protection_t {
        self.0
    }
}

/// Receives a namespace: discovers its tracks from the MSF catalog and
/// delivers their objects.
///
/// Calls must come from one application thread at a time: the type is
/// `Send` but not `Sync`. It holds its [`Endpoint`], which therefore
/// outlives it.
pub struct MediaReceiver {
    raw: NonNull<sys::moq_media_receiver_t>,
    owner: u64,
    /// What the cfg pointed at, kept for the receiver's life.
    _keep: Option<(Namespace, Option<String>)>,
    endpoint: Endpoint,
    _not_sync: PhantomData<std::cell::Cell<()>>,
}

// SAFETY: as for MediaSender.
unsafe impl Send for MediaReceiver {}

impl MediaReceiver {
    /// Attach a receiver to `endpoint`.
    pub fn attach(endpoint: &Endpoint, cfg: ReceiverConfig) -> Result<MediaReceiver> {
        let mut raw = sys::moq_media_receiver_cfg_t::default();
        unsafe {
            match cfg.preset {
                ReceiverPreset::Live => ffi::moq_media_receiver_cfg_init_live(&mut raw),
                ReceiverPreset::FlowControl => {
                    ffi::moq_media_receiver_cfg_init_flow_control(&mut raw)
                }
            }
        }
        raw.endpoint = std::ptr::null(); // attach: the endpoint is given
        raw.namespace_ = cfg.namespace.raw();
        if let Some(c) = &cfg.catalog_track {
            raw.catalog_track = span(c.as_bytes());
        }
        if let Some(v) = cfg.auto_subscribe {
            raw.auto_subscribe = v;
        }
        if let Some(m) = cfg.time_mode {
            raw.time_mode = match m {
                TimeMode::Raw => sys::MOQ_MEDIA_TIME_RAW,
                TimeMode::SharedEpoch => sys::MOQ_MEDIA_TIME_SHARED_EPOCH,
            };
        }
        if let Some(v) = cfg.max_objects {
            raw.overflow.max_objects = v;
        }
        if let Some(v) = cfg.max_bytes {
            raw.overflow.max_bytes = v;
        }
        if let Some(v) = cfg.max_track_events {
            raw.max_track_events = v;
        }
        let mut out = std::ptr::null_mut();
        check(unsafe { ffi::moq_media_receiver_attach(endpoint.as_ptr(), &raw, &mut out) })?;
        Ok(MediaReceiver {
            raw: NonNull::new(out).ok_or(Error::INTERNAL)?,
            owner: next_owner_id(),
            _keep: Some((cfg.namespace, cfg.catalog_track)),
            endpoint: endpoint.clone(),
            _not_sync: PhantomData,
        })
    }

    /// Take ownership of a receiver attached elsewhere.
    ///
    /// # Safety
    /// `raw` must be a live receiver attached to `endpoint`, which nothing
    /// else will destroy, and whose namespace outlives it.
    pub unsafe fn from_raw(
        raw: *mut sys::moq_media_receiver_t,
        endpoint: Endpoint,
    ) -> MediaReceiver {
        MediaReceiver {
            raw: NonNull::new(raw).expect("null receiver"),
            owner: next_owner_id(),
            _keep: None,
            endpoint,
            _not_sync: PhantomData,
        }
    }

    /// Give up ownership without destroying the receiver; returns it and the
    /// endpoint handle it held.
    pub fn into_raw(self) -> (*mut sys::moq_media_receiver_t, Endpoint) {
        let me = std::mem::ManuallyDrop::new(self);
        // SAFETY: each field is moved out once and `me` is not used again.
        let (endpoint, keep) = unsafe { (std::ptr::read(&me.endpoint), std::ptr::read(&me._keep)) };
        // Whoever takes the receiver now keeps its namespace alive.
        std::mem::forget(keep);
        (me.raw.as_ptr(), endpoint)
    }

    /// The endpoint this receiver is attached to.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The underlying `moq_media_receiver_t`.
    pub fn as_ptr(&self) -> *mut sys::moq_media_receiver_t {
        self.raw.as_ptr()
    }

    /// A handle for a track of this receiver obtained through [`sys`].
    ///
    /// # Safety
    /// `raw` must be a track handle this receiver announced.
    pub unsafe fn track_from_raw(&self, raw: *mut sys::moq_media_track_t) -> ReceiverTrack {
        ReceiverTrack {
            raw: NonNull::new(raw).expect("null track"),
            owner: self.owner,
        }
    }

    fn own(&self, track: ReceiverTrack) -> Result<*mut sys::moq_media_track_t> {
        if track.owner == self.owner {
            Ok(track.raw.as_ptr())
        } else {
            Err(Error::INVAL)
        }
    }

    /// The next track event, or `None` when there is none now. Drain events
    /// before objects: a track's ADDED always precedes its first object.
    pub fn poll_track(&self) -> Result<Option<TrackEvent<'_>>> {
        let mut ev = sys::moq_media_track_event_t::default();
        match unsafe {
            ffi::moq_media_receiver_poll_track(self.as_ptr(), &mut ev, std::mem::size_of_val(&ev))
        } {
            sys::MOQ_OK => Ok(Some(TrackEvent {
                raw: ev,
                owner: self.owner,
                _r: PhantomData,
            })),
            sys::MOQ_DONE => Ok(None),
            rc => Err(Error::from_code(rc)),
        }
    }

    /// The next media object, or `None` when there is none now. Fails with
    /// [`Error::CLOSED`] once empty and terminal.
    pub fn poll_object(&self) -> Result<Option<MediaObject>> {
        let mut obj = sys::moq_media_object_t::default();
        match unsafe {
            ffi::moq_media_receiver_poll_object(
                self.as_ptr(),
                &mut obj,
                std::mem::size_of_val(&obj),
            )
        } {
            sys::MOQ_OK => Ok(Some(MediaObject {
                raw: obj,
                owner: self.owner,
                _not_send: PhantomData,
            })),
            sys::MOQ_DONE => Ok(None),
            rc => Err(Error::from_code(rc)),
        }
    }

    /// The next SAP timeline record.
    pub fn poll_sap(&self) -> Result<Option<SapRecord>> {
        let mut r = sys::moq_media_sap_record_t::default();
        match unsafe {
            ffi::moq_media_receiver_poll_sap(self.as_ptr(), &mut r, std::mem::size_of_val(&r))
        } {
            sys::MOQ_OK => Ok(NonNull::new(r.track).map(|raw| SapRecord {
                track: ReceiverTrack {
                    raw,
                    owner: self.owner,
                },
                group: r.group,
                object: r.object,
                sap_type: SapType::from_raw(r.sap_type),
                ept_ms: r.ept_ms,
            })),
            sys::MOQ_DONE => Ok(None),
            rc => Err(Error::from_code(rc)),
        }
    }

    /// The next media timeline record.
    pub fn poll_media_timeline(&self) -> Result<Option<TimelineRecord>> {
        let mut r = sys::moq_media_timeline_record_t::default();
        match unsafe {
            ffi::moq_media_receiver_poll_media_timeline(
                self.as_ptr(),
                &mut r,
                std::mem::size_of_val(&r),
            )
        } {
            sys::MOQ_OK => Ok(NonNull::new(r.track).map(|raw| TimelineRecord {
                track: ReceiverTrack {
                    raw,
                    owner: self.owner,
                },
                media_time_ms: r.media_time_ms,
                group: r.group,
                object: r.object,
                wallclock_ms: r.wallclock_ms,
            })),
            sys::MOQ_DONE => Ok(None),
            rc => Err(Error::from_code(rc)),
        }
    }

    /// Block until something is pollable, the receiver wakes or goes
    /// terminal, or `timeout` elapses.
    pub fn wait(&self, timeout: Duration) -> Result<Wait> {
        wait_result(unsafe { ffi::moq_media_receiver_wait(self.as_ptr(), micros(timeout)) })
    }

    /// Ask for a track's objects (or resume them). Asynchronous: success
    /// means the request is recorded, not that the peer accepted.
    pub fn subscribe_track(
        &self,
        track: ReceiverTrack,
        options: Option<SubscribeOptions>,
    ) -> Result<()> {
        let t = self.own(track)?;
        let cfg = options.map(|o| {
            let mut c = sys::moq_media_receiver_track_subscribe_cfg_t::default();
            unsafe { ffi::moq_media_receiver_track_subscribe_cfg_init(&mut c) };
            c.start = match o.start {
                StartMode::Current => sys::MOQ_MEDIA_START_CURRENT,
                StartMode::NextGroup => sys::MOQ_MEDIA_START_NEXT_GROUP,
            };
            c.has_priority = o.priority.is_some();
            c.priority = o.priority.unwrap_or(0);
            c
        });
        let cfg_ptr = cfg.as_ref().map_or(std::ptr::null(), |c| c as *const _);
        check(unsafe { ffi::moq_media_receiver_subscribe_track(self.as_ptr(), t, cfg_ptr) })
    }

    /// Pause a track's delivery and purge its queued objects.
    pub fn unsubscribe_track(&self, track: ReceiverTrack) -> Result<()> {
        let t = self.own(track)?;
        check(unsafe { ffi::moq_media_receiver_unsubscribe_track(self.as_ptr(), t) })
    }

    /// A track's current delivery state.
    pub fn track_state(&self, track: ReceiverTrack) -> Result<TrackState> {
        let t = self.own(track)?;
        let mut s = 0;
        check(unsafe { ffi::moq_media_receiver_track_state(self.as_ptr(), t, &mut s) })?;
        Ok(match s {
            sys::MOQ_MEDIA_TRACK_STATE_DISCOVERED => TrackState::Discovered,
            sys::MOQ_MEDIA_TRACK_STATE_PENDING => TrackState::Pending,
            sys::MOQ_MEDIA_TRACK_STATE_ACTIVE => TrackState::Active,
            sys::MOQ_MEDIA_TRACK_STATE_PAUSED_APP => TrackState::PausedApp,
            sys::MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW => TrackState::PausedFlow,
            sys::MOQ_MEDIA_TRACK_STATE_ENDED => TrackState::Ended,
            sys::MOQ_MEDIA_TRACK_STATE_FAILED => TrackState::Failed,
            s => TrackState::Other(s),
        })
    }

    /// A track's description.
    pub fn track_desc(&self, track: ReceiverTrack) -> Option<TrackDesc<'_>> {
        let t = self.own(track).ok()?;
        unsafe { ffi::moq_media_track_desc_get(t).as_ref() }.map(TrackDesc)
    }

    /// Resolve a contentProtectionRefID against the current catalog.
    pub fn find_content_protection(&self, ref_id: &[u8]) -> Option<ContentProtectionRef<'_>> {
        unsafe {
            ffi::moq_media_receiver_find_content_protection(self.as_ptr(), span(ref_id)).as_ref()
        }
        .map(ContentProtectionRef)
    }

    /// A statistics snapshot.
    pub fn stats(&self) -> Result<ReceiverStats> {
        let mut s = sys::moq_media_receiver_stats_t::default();
        check(unsafe {
            ffi::moq_media_receiver_get_stats(self.as_ptr(), &mut s, std::mem::size_of_val(&s))
        })?;
        Ok(ReceiverStats {
            objects_received: s.objects_received,
            objects_queued: s.objects_queued,
            bytes_queued: s.bytes_queued,
            objects_dropped: s.objects_dropped,
            groups_dropped: s.groups_dropped,
            keyframes_dropped: s.keyframes_dropped,
            parse_drops: s.parse_drops,
            overflow_events: s.overflow_events,
            pause_transitions: s.pause_transitions,
            paused: s.paused,
            catalog_drops: s.catalog_drops,
            catalog_complete: s.catalog_complete,
        })
    }

    /// Closed cleanly.
    pub fn is_closed(&self) -> bool {
        unsafe { ffi::moq_media_receiver_is_closed(self.as_ptr()) }
    }

    /// Failed.
    pub fn is_fatal(&self) -> bool {
        unsafe { ffi::moq_media_receiver_is_fatal(self.as_ptr()) }
    }

    /// The receiver's fatal code, or the endpoint's.
    pub fn fatal_code(&self) -> u64 {
        unsafe { ffi::moq_media_receiver_fatal_code(self.as_ptr()) }
    }
}

impl Drop for MediaReceiver {
    fn drop(&mut self) {
        // Every handle and description dies here, before the endpoint
        // handle is released.
        unsafe { ffi::moq_media_receiver_destroy(self.raw.as_ptr()) }
    }
}

impl std::fmt::Debug for MediaReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MediaReceiver").field(&self.raw).finish()
    }
}
