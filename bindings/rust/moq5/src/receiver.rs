use std::cell::Cell;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::time::Duration;
use std::{fmt, mem, ptr};

use crate::cmaf::{CmafSample, InitInfo};
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::metadata::ObjectMetadata;
use crate::mpeg2ts::{Mpeg2tsRecordRef, TrackExtension};
use crate::namespace::Namespace;
use crate::types::{
    CompanionProfile, DeliveryOrigin, GroupOrder, Location, LocationFilter, MediaType,
    ObjectStatus, Packaging, SapType, Version, WaitResult,
};
use crate::{ffi, sender, sys};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiverPreset {
    Live,
    FlowControl,
}

/// How reported timestamps relate to the wire (`moq_media_time_mode_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TimeMode {
    /// Pass wire timestamps through.
    #[default]
    Raw,
    /// Rebase reported timestamps against one shared epoch (payloads are
    /// never modified).
    SharedEpoch,
    /// A mode this crate does not know.
    Other(u32),
}

impl TimeMode {
    fn raw(self) -> sys::moq_media_time_mode_t {
        match self {
            TimeMode::Raw => sys::MOQ_MEDIA_TIME_RAW,
            TimeMode::SharedEpoch => sys::MOQ_MEDIA_TIME_SHARED_EPOCH,
            TimeMode::Other(code) => code,
        }
    }

    fn from_raw(code: u32) -> TimeMode {
        match code {
            sys::MOQ_MEDIA_TIME_RAW => TimeMode::Raw,
            sys::MOQ_MEDIA_TIME_SHARED_EPOCH => TimeMode::SharedEpoch,
            code => TimeMode::Other(code),
        }
    }

    /// The `moq_media_time_mode_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// What a receiver does when its object queue is full
/// (`moq_media_overflow_policy_t`), as [`ReceiverInfo`] reports it.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop to the newest keyframe (the live preset).
    DropToKeyframe,
    /// Drop whole groups.
    DropGroup,
    /// Pause the subscriptions upstream (the flow-control preset).
    FlowControl,
    /// A policy this crate does not know.
    Other(u32),
}

impl OverflowPolicy {
    fn from_raw(code: u32) -> OverflowPolicy {
        match code {
            sys::MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME => OverflowPolicy::DropToKeyframe,
            sys::MOQ_MEDIA_OVERFLOW_DROP_GROUP => OverflowPolicy::DropGroup,
            sys::MOQ_MEDIA_OVERFLOW_FLOW_CONTROL => OverflowPolicy::FlowControl,
            code => OverflowPolicy::Other(code),
        }
    }

    /// The `moq_media_overflow_policy_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            OverflowPolicy::DropToKeyframe => sys::MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME,
            OverflowPolicy::DropGroup => sys::MOQ_MEDIA_OVERFLOW_DROP_GROUP,
            OverflowPolicy::FlowControl => sys::MOQ_MEDIA_OVERFLOW_FLOW_CONTROL,
            OverflowPolicy::Other(code) => code,
        }
    }
}

/// How a [`Receiver`] subscribes. Start from a preset, then adjust; only
/// the fields set here override the preset's. The C configuration is built
/// with the preset's sized initializer at attach, so every field reaches
/// the service.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    namespace: Namespace,
    preset: ReceiverPreset,
    catalog_track: Option<String>,
    auto_subscribe: Option<bool>,
    time_mode: Option<TimeMode>,
    max_objects: Option<u32>,
    max_bytes: Option<u64>,
    max_track_events: Option<u32>,
    companion_profile: Option<CompanionProfile>,
    stream_terminals: Option<u32>,
    mpeg2ts_reorder: Option<(u32, Duration)>,
    mpeg2ts_strip_prefix: Option<bool>,
    mpeg2ts_join_psi: Option<bool>,
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
            companion_profile: None,
            stream_terminals: None,
            mpeg2ts_reorder: None,
            mpeg2ts_strip_prefix: None,
            mpeg2ts_join_psi: None,
        }
    }

    /// Live rendering: on overflow, drop to the newest keyframe.
    #[must_use]
    pub fn live(namespace: Namespace) -> ReceiverConfig {
        ReceiverConfig::with_preset(namespace, ReceiverPreset::Live)
    }

    /// Flow control: on overflow, pause the subscriptions upstream.
    #[must_use]
    pub fn flow_control(namespace: Namespace) -> ReceiverConfig {
        ReceiverConfig::with_preset(namespace, ReceiverPreset::FlowControl)
    }

    /// Catalog track name; defaults to `catalog`.
    #[must_use]
    pub fn catalog_track(mut self, name: impl Into<String>) -> Self {
        self.catalog_track = Some(name.into());
        self
    }
    /// Subscribe every media track with a delivery path on discovery
    /// (`true`), or only the ones passed to [`Receiver::subscribe_track`]
    /// (`false`). Timeline tracks are never auto-subscribed.
    #[must_use]
    pub fn auto_subscribe(mut self, auto: bool) -> Self {
        self.auto_subscribe = Some(auto);
        self
    }
    /// Timestamp mode.
    #[must_use]
    pub fn time_mode(mut self, mode: TimeMode) -> Self {
        self.time_mode = Some(mode);
        self
    }
    /// Object-queue bounds (0 = the service's defaults: 256 objects,
    /// 32 MiB).
    #[must_use]
    pub fn queue_limits(mut self, max_objects: u32, max_bytes: u64) -> Self {
        self.max_objects = Some(max_objects);
        self.max_bytes = Some(max_bytes);
        self
    }
    /// Track-event queue depth (0 = 64). Overflowing it is fatal to the
    /// receiver.
    #[must_use]
    pub fn max_track_events(mut self, depth: u32) -> Self {
        self.max_track_events = Some(depth);
        self
    }
    /// The media companion; [`CompanionProfile::Auto`] by default.
    #[must_use]
    pub fn companion_profile(mut self, profile: CompanionProfile) -> Self {
        self.companion_profile = Some(profile);
        self
    }
    /// Keep the stream-terminal lane, `max` records deep (0 = 64), and
    /// drain it with [`Receiver::poll_stream_terminal`]. A consumer that
    /// enables the lane polls it: while it is full, object intake stops
    /// under every overflow policy until records are polled.
    #[must_use]
    pub fn stream_terminals(mut self, max: u32) -> Self {
        self.stream_terminals = Some(max);
        self
    }
    /// MPEG-TS tracks release their objects in Location order through a
    /// reorder window: `slots` objects held per track (0 = 64), each waiting
    /// at most `window` (exact microseconds; zero = 300 ms) for the ones
    /// before it.
    #[must_use]
    pub fn mpeg2ts_reorder(mut self, slots: u32, window: Duration) -> Self {
        self.mpeg2ts_reorder = Some((slots, window));
        self
    }
    /// Remove the four-octet prefix of 192-octet source packets, so every
    /// delivered payload of such a track is 188-octet TS packets (the
    /// catalog's packet size on the description is unchanged).
    #[must_use]
    pub fn mpeg2ts_strip_prefix(mut self, strip: bool) -> Self {
        self.mpeg2ts_strip_prefix = Some(strip);
        self
    }
    /// Prefix the first object polled for each MPEG-TS subscription, live or
    /// from one of its fills, with the catalog's PAT/PMT packets (the
    /// track's init data), their continuity counters aligned to that object.
    /// An object dropped before it is polled never takes it, and no later
    /// object of the subscription carries it.
    #[must_use]
    pub fn mpeg2ts_join_psi(mut self, join: bool) -> Self {
        self.mpeg2ts_join_psi = Some(join);
        self
    }

    /// The configuration of the receiver a player owns: no auto-subscribe
    /// and the stream-terminal lane on (as deep as given, else the
    /// service's default), the time mode left as given for the player to
    /// judge.
    #[cfg(feature = "player")]
    pub(crate) fn for_player(&self) -> ReceiverConfig {
        let mut config = self.clone();
        config.auto_subscribe = Some(false);
        config.stream_terminals = Some(self.stream_terminals.unwrap_or(0));
        config
    }

    /// The requested time mode, or the preset's.
    #[cfg(feature = "player")]
    pub(crate) fn time_mode_requested(&self) -> TimeMode {
        self.time_mode.unwrap_or_default()
    }
}

/// A track discovered by a [`Receiver`]. A plain handle; calls with a
/// handle from another receiver fail with [`Error::INVAL`]. Handles are
/// valid until the receiver is destroyed and are never recycled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Track {
    raw: NonNull<sys::moq_media_track_t>,
    owner: u64,
}

// SAFETY: an opaque identifier, dereferenced only by its receiver after the
// owner check.
unsafe impl Send for Track {}
unsafe impl Sync for Track {}

impl Track {
    /// The handle `raw` of the receiver tagged `owner`.
    #[cfg(feature = "player")]
    pub(crate) fn new(raw: NonNull<sys::moq_media_track_t>, owner: u64) -> Track {
        Track { raw, owner }
    }

    /// The underlying `moq_media_track_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_track_t {
        self.raw.as_ptr()
    }
}

/// An MSF 7.4 inline media timeline template (catalog field 5.2.15).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct MediaTemplate {
    pub start_media_ms: u64,
    pub delta_media_ms: u64,
    pub start_group: u64,
    pub start_object: u64,
    pub delta_group: u64,
    pub delta_object: u64,
    pub start_wallclock_ms: u64,
    pub delta_wallclock_ms: u64,
}

/// A track's catalog description: the current state, copied under the
/// receiver's lock at the time of the query. Spans and arrays borrow the
/// track handle, which lives as long as the receiver.
///
/// This is a snapshot of the present, not the image a track event carried:
/// a live-to-VOD conversion rewrites `is_live` and the track duration on
/// the same handle, and a later query shows the new values.
pub struct TrackDescription<'receiver> {
    raw: sys::moq_media_track_desc_t,
    cmaf: Option<sys::moq_cmaf_init_info_t>,
    _receiver: PhantomData<&'receiver Receiver>,
}

/// Copy the receiver's parsed CMAF record into this crate's size of it.
/// The library's record may be smaller (an older library) or larger (a newer
/// one) than this crate's, so only the bytes both its stamp and this crate's
/// size cover are read, and the copy is stamped with that count. A record
/// stamped below its frozen floor is an ABI mismatch.
fn copy_cmaf_record(
    record: *const sys::moq_cmaf_init_info_t,
) -> Result<Option<sys::moq_cmaf_init_info_t>> {
    if record.is_null() {
        return Ok(None);
    }
    // SAFETY: a non-null record is handle-owned and immutable until the
    // receiver is destroyed, and begins with its struct_size.
    let stamp = unsafe { ptr::addr_of!((*record).struct_size).read() } as usize;
    if stamp < sys::MOQ_CMAF_INIT_INFO_V0_SIZE {
        return Err(Error::ABI_MISMATCH);
    }
    let mut copy = sys::moq_cmaf_init_info_t::default();
    let n = stamp.min(mem::size_of_val(&copy));
    // SAFETY: the record holds `stamp` bytes and copy holds its own size;
    // n is at most both, and the two never overlap.
    unsafe {
        ptr::copy_nonoverlapping(record.cast::<u8>(), ptr::addr_of_mut!(copy).cast::<u8>(), n);
    }
    copy.struct_size = n as u32;
    Ok(Some(copy))
}

impl<'receiver> TrackDescription<'receiver> {
    fn covers(&self, field_end: usize) -> bool {
        ffi::covers(self.raw.struct_size, field_end)
    }

    /// Catalog track name.
    #[must_use]
    pub fn name(&self) -> &'receiver [u8] {
        // SAFETY: handle-owned, valid for the receiver's life.
        unsafe { ffi::bytes(self.raw.name) }
    }
    /// Role; empty when absent.
    #[must_use]
    pub fn role(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.role) }
    }
    /// Codec string, e.g. `avc1.64001f`.
    #[must_use]
    pub fn codec(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.codec) }
    }
    /// Language; empty when absent.
    #[must_use]
    pub fn lang(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.lang) }
    }
    /// MSF label; `None` when the catalog carried none (an empty label is
    /// `Some(empty)`), or when the library did not report whether it did.
    #[must_use]
    pub fn label(&self) -> Option<&'receiver [u8]> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_desc_t,
            has_label,
            bool
        ));
        if !(reported && self.raw.has_label) {
            return None;
        }
        // SAFETY: as for name.
        Some(unsafe { ffi::bytes(self.raw.label) })
    }
    /// Media type, when derived from the catalog.
    #[must_use]
    pub fn media_type(&self) -> Option<MediaType> {
        MediaType::from_raw(self.raw.info.media_type)
    }
    /// Media packaging, when derived. Non-media tracks (timelines) have
    /// none; see [`packaging_name`](TrackDescription::packaging_name).
    #[must_use]
    pub fn packaging(&self) -> Option<Packaging> {
        Packaging::from_raw(self.raw.info.packaging)
    }
    /// The catalog packaging string, verbatim: `cmaf`, `loc`,
    /// `eventtimeline`, `mediatimeline`, ...
    #[must_use]
    pub fn packaging_name(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.packaging) }
    }
    /// Media timescale.
    #[must_use]
    pub fn timescale(&self) -> u32 {
        self.raw.info.timescale
    }
    /// The negotiated draft the track's objects are read under; `None`
    /// when the library did not report it.
    #[must_use]
    pub fn transport_version(&self) -> Option<Version> {
        let end = ffi::field_end!(sys::moq_media_track_info_t, transport_version, u32);
        let reported = ffi::covers(self.raw.info.struct_size, end);
        if reported && self.raw.info.transport_version != 0 {
            Some(Version::from_draft(self.raw.info.transport_version))
        } else {
            None
        }
    }
    /// The decoder extradata (SPS/PPS/VPS, AAC AudioSpecificConfig, ...),
    /// whatever the packaging: for CMAF the sample entry's configuration box
    /// (the same bytes as [`init_info`](TrackDescription::init_info)'s), for
    /// RAW/LOC the catalog's init data verbatim; empty for MPEG-2 TS or
    /// when the track carries none.
    #[must_use]
    pub fn codec_config(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.codec_config) }
    }
    /// The record parsed from the track's CMAF header: codec kind,
    /// dimensions, sample entry, CENC parameters. `None` for every track
    /// whose init data is not a CMAF header that parsed: RAW/LOC, MPEG-2 TS,
    /// a CMAF track without init data or with a malformed one. Its fields
    /// are header observations; the catalog's own declarations are
    /// [`width`](TrackDescription::width) and its siblings.
    #[must_use]
    pub fn init_info(&self) -> Option<InitInfo<'_>> {
        // SAFETY: the record's spans are handle-owned, valid while the
        // receiver lives, which outlives this description.
        self.cmaf
            .as_ref()
            .map(|raw| unsafe { InitInfo::from_raw(raw) })
    }
    /// The full init data from the catalog, resolved: a CMAF init segment,
    /// the MPEG-2 TS PSI source packets, or whatever a RAW sender supplied.
    #[must_use]
    pub fn init_data(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.init_data) }
    }
    /// Catalog width.
    #[must_use]
    pub fn width(&self) -> Option<u32> {
        self.raw.has_width.then_some(self.raw.width)
    }
    /// Catalog height.
    #[must_use]
    pub fn height(&self) -> Option<u32> {
        self.raw.has_height.then_some(self.raw.height)
    }
    /// Catalog sample rate.
    #[must_use]
    pub fn samplerate(&self) -> Option<u32> {
        self.raw.has_samplerate.then_some(self.raw.samplerate)
    }
    /// Catalog channelConfig, e.g. `2`; empty when absent.
    #[must_use]
    pub fn channel_config(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.channel_config) }
    }
    /// Frames per 1000 seconds.
    #[must_use]
    pub fn framerate_millis(&self) -> Option<u64> {
        self.raw.has_framerate.then_some(self.raw.framerate_millis)
    }
    /// Maximum bitrate.
    #[must_use]
    pub fn bitrate(&self) -> Option<u64> {
        self.raw.has_bitrate.then_some(self.raw.bitrate)
    }
    /// CMSF 3.5.2 maxGrpSapStartingType.
    #[must_use]
    pub fn max_group_sap(&self) -> Option<u32> {
        self.raw.has_max_grp_sap.then_some(self.raw.max_grp_sap)
    }
    /// CMSF 3.5.2 maxObjSapStartingType.
    #[must_use]
    pub fn max_object_sap(&self) -> Option<u32> {
        self.raw.has_max_obj_sap.then_some(self.raw.max_obj_sap)
    }
    /// CMSF 4.1.2 contentProtectionRefIDs; resolve them with
    /// [`Receiver::find_content_protection`].
    pub fn content_protection_ref_ids(&self) -> impl Iterator<Item = &'receiver [u8]> {
        // SAFETY: handle-owned array and spans, valid for the receiver's life.
        let ids = unsafe {
            ffi::array(
                self.raw.content_protection_ref_ids,
                self.raw.content_protection_ref_id_count,
            )
        };
        // SAFETY: as above.
        ids.iter().map(|id| unsafe { ffi::bytes(*id) })
    }
    /// Timeline eventType; empty when absent.
    #[must_use]
    pub fn event_type(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.event_type) }
    }
    /// mimeType; empty when absent.
    #[must_use]
    pub fn mime_type(&self) -> &'receiver [u8] {
        // SAFETY: as for name.
        unsafe { ffi::bytes(self.raw.mime_type) }
    }
    /// The track names a timeline applies to.
    pub fn depends(&self) -> impl Iterator<Item = &'receiver [u8]> {
        // SAFETY: as for content_protection_ref_ids.
        let names = unsafe { ffi::array(self.raw.depends, self.raw.depends_count) };
        // SAFETY: as above.
        names.iter().map(|name| unsafe { ffi::bytes(*name) })
    }
    /// The inline media timeline template, when the catalog carried one.
    #[must_use]
    pub fn template(&self) -> Option<MediaTemplate> {
        let template = &self.raw.template_;
        self.raw.has_template.then_some(MediaTemplate {
            start_media_ms: template.start_media_ms,
            delta_media_ms: template.delta_media_ms,
            start_group: template.start_group,
            start_object: template.start_object,
            delta_group: template.delta_group,
            delta_object: template.delta_object,
            start_wallclock_ms: template.start_wallclock_ms,
            delta_wallclock_ms: template.delta_wallclock_ms,
        })
    }
    /// VOD track duration, present on a non-live track.
    #[must_use]
    pub fn track_duration_ms(&self) -> Option<u64> {
        self.raw
            .has_track_duration
            .then_some(self.raw.track_duration_ms)
    }
    /// Catalog isLive, as of this snapshot.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.raw.is_live
    }
    /// MSF altGroup; `None` when absent or not reported by the library.
    #[must_use]
    pub fn alt_group(&self) -> Option<i32> {
        let reported = self.covers(ffi::field_end!(sys::moq_media_track_desc_t, alt_group, i32));
        (reported && self.raw.has_alt_group).then_some(self.raw.alt_group)
    }
    /// MSF renderGroup; `None` when absent or not reported by the library.
    #[must_use]
    pub fn render_group(&self) -> Option<i32> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_desc_t,
            render_group,
            i32
        ));
        (reported && self.raw.has_render_group).then_some(self.raw.render_group)
    }
    /// The catalog's packaging-extension records for the track, each read
    /// under its own stamp; empty when the track carries none or the
    /// library did not report the list.
    pub fn extensions(&self) -> impl Iterator<Item = TrackExtension<'receiver>> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_desc_t,
            extension_count,
            usize
        ));
        let entries = if reported {
            // SAFETY: the entries are owned by the track handle, valid for
            // the receiver's life.
            unsafe { ffi::array(self.raw.extensions, self.raw.extension_count) }
        } else {
            &[]
        };
        entries.iter().map(TrackExtension::from_raw)
    }
    /// The track's MPEG-TS record (`moq_msf_ext_mpeg2ts`): the catalog's
    /// mpeg2ts keys as parsed. `None` when the track carries none, or the
    /// library did not report the list or the record.
    #[must_use]
    pub fn mpeg2ts(&self) -> Option<Mpeg2tsRecordRef<'receiver>> {
        self.extensions().find_map(|extension| match extension {
            TrackExtension::Mpeg2ts(record) => Some(record),
            _ => None,
        })
    }
    /// The raw description; its spans borrow the handle.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_media_track_desc_t {
        &self.raw
    }
}

// The catalog identity, not the spans a derive would print.
impl fmt::Debug for TrackDescription<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackDescription")
            .field("name", &String::from_utf8_lossy(self.name()))
            .field("codec", &String::from_utf8_lossy(self.codec()))
            .field("packaging", &String::from_utf8_lossy(self.packaging_name()))
            .field("is_live", &self.is_live())
            .finish_non_exhaustive()
    }
}

/// What a track event reports (`moq_media_track_event_kind_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackEventKind {
    /// A track was discovered.
    Added,
    /// A track converted live to VOD; its description changed in place.
    Updated,
    /// A later catalog dropped the track.
    Removed,
    /// The track ended: END_OF_TRACK, a completed subscription, or a peer
    /// rejection. See [`TrackEvent::done_status`].
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
    /// A local UPDATE admission (pause/resume) failed permanently; that
    /// track generation has failed (not a peer rejection).
    UpdateLocalFailure,
    /// A kind this crate does not know.
    Other(u32),
}

impl TrackEventKind {
    fn from_raw(kind: sys::moq_media_track_event_kind_t) -> TrackEventKind {
        match kind {
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
            sys::MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE => TrackEventKind::UpdateLocalFailure,
            kind => TrackEventKind::Other(kind),
        }
    }

    /// The `moq_media_track_event_kind_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            TrackEventKind::Added => sys::MOQ_MEDIA_TRACK_ADDED,
            TrackEventKind::Updated => sys::MOQ_MEDIA_TRACK_UPDATED,
            TrackEventKind::Removed => sys::MOQ_MEDIA_TRACK_REMOVED,
            TrackEventKind::Ended => sys::MOQ_MEDIA_TRACK_ENDED,
            TrackEventKind::CatalogReady => sys::MOQ_MEDIA_CATALOG_READY,
            TrackEventKind::UpdateOk => sys::MOQ_MEDIA_TRACK_UPDATE_OK,
            TrackEventKind::ParseDrop => sys::MOQ_MEDIA_TRACK_PARSE_DROP,
            TrackEventKind::LocalFailure => sys::MOQ_MEDIA_TRACK_LOCAL_FAILURE,
            TrackEventKind::FillTerminal => sys::MOQ_MEDIA_TRACK_FILL_TERMINAL,
            TrackEventKind::DeliveryLocalFailure => sys::MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE,
            TrackEventKind::UpdateLocalFailure => sys::MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE,
            TrackEventKind::Other(kind) => kind,
        }
    }
}

/// Which receive path a coalesced parse-drop event came from
/// (`moq_media_parse_drop_class_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseDropClass {
    /// A media object (`poll_object`).
    Media,
    /// A CMSF SAP timeline object (`poll_sap`).
    Sap,
    /// An MSF media-timeline object (`poll_media_timeline`).
    MediaTimeline,
    /// A class this crate does not know.
    Other(u32),
}

impl ParseDropClass {
    fn from_raw(class: u32) -> ParseDropClass {
        match class {
            sys::MOQ_MEDIA_PARSE_DROP_MEDIA => ParseDropClass::Media,
            sys::MOQ_MEDIA_PARSE_DROP_SAP => ParseDropClass::Sap,
            sys::MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE => ParseDropClass::MediaTimeline,
            class => ParseDropClass::Other(class),
        }
    }

    /// The `moq_media_parse_drop_class_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            ParseDropClass::Media => sys::MOQ_MEDIA_PARSE_DROP_MEDIA,
            ParseDropClass::Sap => sys::MOQ_MEDIA_PARSE_DROP_SAP,
            ParseDropClass::MediaTimeline => sys::MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE,
            ParseDropClass::Other(class) => class,
        }
    }
}

/// A coalesced parse-drop observation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseDrop {
    /// The path of the last drop in this run.
    pub class: ParseDropClass,
    /// Cumulative parse drops for the track.
    pub total: u64,
    /// Drops since the previous parse-drop event for the track.
    pub delta: u64,
}

/// Why a fill ended (`MOQ_FILL_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FillReason {
    /// Its stream ended with the whole range delivered.
    Complete,
    /// The response opened no stream.
    NoStream,
    /// Cancelled locally ([`Receiver::cancel_fill`]).
    LocalCancelled,
    /// The peer cancelled or reset its stream.
    PeerCancelledOrReset,
    /// Aborted locally.
    LocalAborted,
    /// The request that carried it was rejected.
    ParentRejected,
    /// The subscription it belongs to ended first.
    ParentTerminated,
    /// A reason this crate does not know.
    Other(u32),
}

impl FillReason {
    fn from_raw(code: u32) -> FillReason {
        match code {
            sys::MOQ_FILL_COMPLETE => FillReason::Complete,
            sys::MOQ_FILL_NO_STREAM => FillReason::NoStream,
            sys::MOQ_FILL_LOCAL_CANCELLED => FillReason::LocalCancelled,
            sys::MOQ_FILL_PEER_CANCELLED_OR_RESET => FillReason::PeerCancelledOrReset,
            sys::MOQ_FILL_LOCAL_ABORTED => FillReason::LocalAborted,
            sys::MOQ_FILL_PARENT_REJECTED => FillReason::ParentRejected,
            sys::MOQ_FILL_PARENT_TERMINATED => FillReason::ParentTerminated,
            code => FillReason::Other(code),
        }
    }

    /// The `MOQ_FILL_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            FillReason::Complete => sys::MOQ_FILL_COMPLETE,
            FillReason::NoStream => sys::MOQ_FILL_NO_STREAM,
            FillReason::LocalCancelled => sys::MOQ_FILL_LOCAL_CANCELLED,
            FillReason::PeerCancelledOrReset => sys::MOQ_FILL_PEER_CANCELLED_OR_RESET,
            FillReason::LocalAborted => sys::MOQ_FILL_LOCAL_ABORTED,
            FillReason::ParentRejected => sys::MOQ_FILL_PARENT_REJECTED,
            FillReason::ParentTerminated => sys::MOQ_FILL_PARENT_TERMINATED,
            FillReason::Other(code) => code,
        }
    }
}

/// The error a fill ended with, in its domain (`MOQ_FILL_ERROR_*`). Zero
/// is a valid code in every domain but `None`: a stream code of zero is
/// still a failure, not completion.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FillError {
    /// No error.
    None,
    /// The fill stream's error code.
    Stream(u64),
    /// The parent request's error code.
    ParentRequest(u64),
    /// The parent's done status code.
    ParentDone(u64),
    /// A domain this crate does not know, with its code.
    Other {
        /// The `MOQ_FILL_ERROR_*` domain.
        domain: u32,
        /// The code in it.
        code: u64,
    },
}

impl FillError {
    fn from_raw(domain: u32, code: u64) -> FillError {
        match domain {
            sys::MOQ_FILL_ERROR_NONE => FillError::None,
            sys::MOQ_FILL_ERROR_STREAM => FillError::Stream(code),
            sys::MOQ_FILL_ERROR_PARENT_REQUEST => FillError::ParentRequest(code),
            sys::MOQ_FILL_ERROR_PARENT_DONE => FillError::ParentDone(code),
            domain => FillError::Other { domain, code },
        }
    }
}

/// One fill's end, for [`TrackEventKind::FillTerminal`]: exactly one per
/// fill, the winner when a local cancellation races a peer terminal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FillTerminal {
    /// The exact initiating SUBSCRIBE/UPDATE ID (zero is valid).
    pub initiating_request_id: u64,
    /// Why it ended.
    pub reason: FillReason,
    /// The error it ended with.
    pub error: FillError,
}

/// A fill's loss no delivered object reported, for
/// [`TrackEventKind::FillTerminal`] of an MPEG-TS media track.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FillLoss {
    /// The OR of `MOQ_MEDIA_FILL_LOSS_*` for every contribution; 0 for none.
    pub causes: u32,
    /// Where the earliest contribution in delivery order starts.
    pub start: Location,
}

/// A track-discovery event, borrowed from its receiver. Every appended
/// field is read under the library's stamp and is `None` when not
/// reported; each is meaningful for the kinds the C header names.
pub struct TrackEvent<'receiver> {
    raw: sys::moq_media_track_event_t,
    receiver: &'receiver Receiver,
}

impl<'receiver> TrackEvent<'receiver> {
    fn covers(&self, field_end: usize) -> bool {
        ffi::covers(self.raw.struct_size, field_end)
    }

    /// What happened.
    #[must_use]
    pub fn kind(&self) -> TrackEventKind {
        TrackEventKind::from_raw(self.raw.kind)
    }
    /// The track; `None` for [`TrackEventKind::CatalogReady`].
    #[must_use]
    pub fn track(&self) -> Option<Track> {
        NonNull::new(self.raw.track).map(|raw| Track {
            raw,
            owner: self.receiver.owner,
        })
    }
    /// The track's current description, copied now; `None` for an event
    /// without a track. This is the present state, not the image the event
    /// carried: see [`TrackDescription`].
    pub fn description(&self) -> Result<Option<TrackDescription<'receiver>>> {
        match self.track() {
            Some(track) => self.receiver.track_description(track).map(Some),
            None => Ok(None),
        }
    }
    /// Reserved for dynamic configuration; always 0 today.
    #[must_use]
    pub fn config_generation(&self) -> u32 {
        self.raw.config_generation
    }
    /// For [`TrackEventKind::UpdateOk`]: the largest Location known when
    /// delivery resumed, when the peer reported one.
    #[must_use]
    pub fn largest(&self) -> Option<Location> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            largest_object,
            u64
        ));
        (reported && self.raw.has_largest).then_some(Location {
            group_id: self.raw.largest_group,
            object_id: self.raw.largest_object,
        })
    }
    /// For [`TrackEventKind::UpdateOk`] from a draft-18 peer: the expiry.
    #[must_use]
    pub fn expires_ms(&self) -> Option<u64> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            expires_ms,
            u64
        ));
        (reported && self.raw.has_expires).then_some(self.raw.expires_ms)
    }
    /// For [`TrackEventKind::ParseDrop`]: the coalesced counters.
    #[must_use]
    pub fn parse_drop(&self) -> Option<ParseDrop> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            parse_drops_delta,
            u64
        ));
        reported.then_some(ParseDrop {
            class: ParseDropClass::from_raw(self.raw.parse_drop_class),
            total: self.raw.parse_drops_total,
            delta: self.raw.parse_drops_delta,
        })
    }
    /// For the local-failure kinds: the exact local result (never a wire
    /// code). `None` when zero or not reported.
    #[must_use]
    pub fn local_result(&self) -> Option<Error> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            local_result,
            i32
        ));
        if reported && self.raw.local_result != sys::MOQ_OK {
            Some(Error::from_code(self.raw.local_result))
        } else {
            None
        }
    }
    /// For [`TrackEventKind::FillTerminal`]: how the fill ended.
    #[must_use]
    pub fn fill_terminal(&self) -> Option<FillTerminal> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            fill_error_code,
            u64
        ));
        reported.then_some(FillTerminal {
            initiating_request_id: self.raw.initiating_request_id,
            reason: FillReason::from_raw(self.raw.fill_reason),
            error: FillError::from_raw(self.raw.fill_error_domain, self.raw.fill_error_code),
        })
    }
    /// For [`TrackEventKind::DeliveryLocalFailure`]: which delivery was
    /// refused.
    #[must_use]
    pub fn delivery_origin(&self) -> Option<DeliveryOrigin> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            delivery_origin,
            u32
        ));
        reported.then_some(DeliveryOrigin::from_raw(self.raw.delivery_origin))
    }
    /// For [`TrackEventKind::DeliveryLocalFailure`] of a fill: its
    /// initiating request ID (zero is valid; a live delivery leaves zero).
    #[must_use]
    pub fn delivery_initiating_request_id(&self) -> Option<u64> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            delivery_initiating_request_id,
            u64
        ));
        reported.then_some(self.raw.delivery_initiating_request_id)
    }
    /// For [`TrackEventKind::FillTerminal`] of an MPEG-TS media track: the
    /// fill's unreported loss.
    #[must_use]
    pub fn fill_unreported_loss(&self) -> Option<FillLoss> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            fill_unreported_loss_object,
            u64
        ));
        reported.then_some(FillLoss {
            causes: self.raw.fill_unreported_loss_causes,
            start: Location {
                group_id: self.raw.fill_unreported_loss_group,
                object_id: self.raw.fill_unreported_loss_object,
            },
        })
    }
    /// For [`TrackEventKind::Ended`]: the status code of the PUBLISH_DONE
    /// (SUBSCRIBE_DONE on draft 16) that completed the subscription;
    /// `None` when the track ended with no done status (its END_OF_TRACK, or
    /// a refused subscription) or the field was not reported. Whether the
    /// END_OF_TRACK arrived is a separate fact, on the stream-terminal lane.
    #[must_use]
    pub fn done_status(&self) -> Option<u64> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_track_event_t,
            done_status,
            u64
        ));
        (reported && self.raw.has_done_status).then_some(self.raw.done_status)
    }
    /// The raw event.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_media_track_event_t {
        &self.raw
    }
}

// The kind and the handle, not the raw record a derive would print.
impl fmt::Debug for TrackEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackEvent")
            .field("kind", &self.kind())
            .field("track", &self.track())
            .finish_non_exhaustive()
    }
}

/// An object's wire identity.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectIdentity {
    /// The object's Location in its track (zero is valid).
    pub location: Location,
    /// The subgroup the wire carried, when `has_subgroup`; zero is also a
    /// real subgroup ID, so only the flag says a datagram had none.
    pub subgroup_id: u64,
    /// The wire carried a subgroup (a subgroup stream, not a datagram).
    pub has_subgroup: bool,
    /// The effective publisher priority the session delivered.
    pub publisher_priority: u8,
}

/// Location-continuity loss preceding an object, for packagings whose
/// continuity the receiver tracks (MPEG-TS).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LossReport {
    /// A run of missing, rejected or dropped objects precedes this one.
    pub gap: bool,
    /// The run's first Location.
    pub gap_start: Location,
    /// The run is only the unconfirmed tail of the previous group: not a
    /// known loss.
    pub gap_tail_unknown: bool,
    /// The run's PCR span, when the packaging has an in-band clock.
    pub gap_duration_27mhz: Option<u64>,
    /// No random access point on the selected stream since the gap.
    pub discontinuous: bool,
}

const OBJECT_V0: usize = ffi::field_end!(
    sys::moq_media_object_t,
    samples_owned,
    *mut sys::moq_cmaf_sample_t
);

/// A received media object. It owns its buffers until dropped, and is
/// released on the polling thread: it is neither `Send` nor `Sync`. Copy
/// the bytes out to hand them to another thread.
pub struct MediaObject {
    raw: sys::moq_media_object_t,
    owner: u64,
    _not_send: PhantomData<*const ()>,
}

impl MediaObject {
    fn covers(&self, field_end: usize) -> bool {
        ffi::covers(self.raw.struct_size, field_end)
    }

    /// The track it belongs to.
    #[must_use]
    pub fn track(&self) -> Option<Track> {
        NonNull::new(self.raw.track).map(|raw| Track {
            raw,
            owner: self.owner,
        })
    }
    /// Its packaging.
    #[must_use]
    pub fn packaging(&self) -> Option<Packaging> {
        Packaging::from_raw(self.raw.packaging)
    }
    /// A random-access point.
    #[must_use]
    pub fn is_keyframe(&self) -> bool {
        self.raw.keyframe
    }
    /// The last object of its group.
    #[must_use]
    pub fn end_of_group(&self) -> bool {
        self.raw.end_of_group
    }
    /// Arrived as a datagram.
    #[must_use]
    pub fn is_datagram(&self) -> bool {
        self.raw.datagram
    }
    /// The MoQ object status; it, not the byte length, says whether the
    /// object carries media.
    #[must_use]
    pub fn status(&self) -> ObjectStatus {
        ObjectStatus::from_raw(self.raw.status)
    }
    /// Reserved; always 0 today.
    #[must_use]
    pub fn config_generation(&self) -> u32 {
        self.raw.config_generation
    }
    /// Capture time, microseconds since the Unix epoch, when the object
    /// carried one.
    #[must_use]
    pub fn capture_time_us(&self) -> Option<u64> {
        self.raw
            .has_capture_time
            .then_some(self.raw.capture_time_us)
    }
    /// Decode time.
    #[must_use]
    pub fn decode_time_us(&self) -> u64 {
        self.raw.decode_time_us
    }
    /// Composition offset.
    #[must_use]
    pub fn composition_offset_us(&self) -> i64 {
        self.raw.composition_offset_us
    }
    /// Presentation time.
    #[must_use]
    pub fn presentation_time_us(&self) -> u64 {
        self.raw.presentation_time_us
    }
    /// RAW/LOC: the media bytes. Empty for CMAF.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        // SAFETY: owned by the object until cleanup.
        unsafe { ffi::bytes(self.raw.payload) }
    }
    /// CMAF: the full fragment, container framing included. Empty for RAW.
    #[must_use]
    pub fn fragment(&self) -> &[u8] {
        // SAFETY: as for payload.
        unsafe { ffi::bytes(self.raw.fragment) }
    }
    /// CMAF: the (first chunk's) mdat payload within the fragment.
    #[must_use]
    pub fn mdat(&self) -> &[u8] {
        let fragment = self.fragment();
        let start = self.raw.mdat_offset.min(fragment.len());
        let end = start.saturating_add(self.raw.mdat_len).min(fragment.len());
        &fragment[start..end]
    }
    /// CMAF: the sample table, in the track's timescale.
    #[must_use]
    pub fn samples(&self) -> &[CmafSample] {
        // SAFETY: owned by the object until cleanup.
        unsafe { CmafSample::table(self.raw.samples, self.raw.sample_count) }
    }
    /// The object's wire identity; `None` when the library did not report
    /// it (never "absent").
    #[must_use]
    pub fn identity(&self) -> Option<ObjectIdentity> {
        let reported = self.covers(ffi::field_end!(sys::moq_media_object_t, has_subgroup, bool));
        reported.then_some(ObjectIdentity {
            location: Location {
                group_id: self.raw.group_id,
                object_id: self.raw.object_id,
            },
            subgroup_id: self.raw.subgroup_id,
            has_subgroup: self.raw.has_subgroup,
            publisher_priority: self.raw.publisher_priority,
        })
    }
    /// The loss report, when the receiver tracks continuity for the
    /// track's packaging and the library reported the block.
    #[must_use]
    pub fn loss_report(&self) -> Option<LossReport> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_object_t,
            discontinuous,
            bool
        ));
        (reported && self.raw.has_loss_report).then_some(LossReport {
            gap: self.raw.gap,
            gap_start: Location {
                group_id: self.raw.gap_group,
                object_id: self.raw.gap_object,
            },
            gap_tail_unknown: self.raw.gap_tail_unknown,
            gap_duration_27mhz: self
                .raw
                .gap_duration_known
                .then_some(self.raw.gap_duration_27mhz),
            discontinuous: self.raw.discontinuous,
        })
    }
    /// Where the object came from; `None` when not reported, never an
    /// inferred live delivery.
    #[must_use]
    pub fn delivery_origin(&self) -> Option<DeliveryOrigin> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_object_t,
            initiating_request_id,
            u64
        ));
        reported.then_some(DeliveryOrigin::from_raw(self.raw.delivery_origin))
    }
    /// For a fill delivery, its initiating request ID (zero is valid); a
    /// live delivery stores zero.
    #[must_use]
    pub fn initiating_request_id(&self) -> Option<u64> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_object_t,
            initiating_request_id,
            u64
        ));
        reported.then_some(self.raw.initiating_request_id)
    }
    /// The draft-22 effective metadata (LOC-04), borrowed from the object;
    /// `None` for a draft-16/18 object, which carries none, or when the
    /// library did not report the block.
    #[must_use]
    pub fn metadata(&self) -> Option<ObjectMetadata<'_>> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_object_t,
            metadata,
            sys::moq_media_metadata_t
        ));
        if !reported {
            return None;
        }
        ObjectMetadata::from_raw(&self.raw.metadata)
    }
    /// The track properties the subscription's acknowledgment carried
    /// (draft 22), the object's own copy, raw in the negotiated draft's
    /// encoding; `None` for an empty block, a draft-16/18 object, or when
    /// the library did not report the field.
    #[must_use]
    pub fn track_properties(&self) -> Option<&[u8]> {
        let reported = self.covers(ffi::field_end!(
            sys::moq_media_object_t,
            track_properties_ref,
            *mut sys::moq_rcbuf_t
        ));
        if !reported || self.raw.track_properties_ref.is_null() {
            return None;
        }
        let block = self.raw.track_properties_ref;
        // SAFETY: a reference the object owns until cleanup.
        Some(unsafe {
            ffi::bytes(sys::moq_bytes_t {
                data: sys::moq_rcbuf_data(block),
                len: sys::moq_rcbuf_len(block),
            })
        })
    }
    /// The bytes the object holds until it is dropped, as
    /// [`Receiver::poll_object_within`] charges them
    /// (`moq_media_object_charge`).
    #[must_use]
    pub fn charge(&self) -> u64 {
        // SAFETY: a polled object; the call reads only what its stamp covers.
        unsafe { sys::moq_media_object_charge(&self.raw) }
    }
    /// The raw object; its buffers stay owned by `self`.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_media_object_t {
        &self.raw
    }
}

impl Drop for MediaObject {
    fn drop(&mut self) {
        // SAFETY: the buffers a successful poll transferred, released once,
        // on this (the polling) thread.
        unsafe { sys::moq_media_object_cleanup(&mut self.raw) }
    }
}

// The identity and timing, not the raw record a derive would print.
impl fmt::Debug for MediaObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaObject")
            .field("track", &self.track())
            .field("packaging", &self.packaging())
            .field("status", &self.status())
            .field("keyframe", &self.is_keyframe())
            .field("presentation_time_us", &self.presentation_time_us())
            .finish_non_exhaustive()
    }
}

/// What a stream-terminal record reports (`MOQ_MEDIA_STREAM_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamTerminalKind {
    /// The stream carrying a subgroup ended with the group.
    FinEndOfGroup,
    /// The stream carrying a subgroup ended.
    Fin,
    /// The stream carrying a subgroup was reset by the peer.
    Reset,
    /// The receiver's overflow policy dropped objects of the track.
    ReceiverDrop,
    /// The track's END_OF_TRACK, at its Location.
    EndOfTrack,
    /// A kind this crate does not know.
    Other(u32),
}

impl StreamTerminalKind {
    pub(crate) fn from_raw(kind: sys::moq_media_stream_terminal_kind_t) -> StreamTerminalKind {
        match kind {
            sys::MOQ_MEDIA_STREAM_FIN_END_OF_GROUP => StreamTerminalKind::FinEndOfGroup,
            sys::MOQ_MEDIA_STREAM_FIN => StreamTerminalKind::Fin,
            sys::MOQ_MEDIA_STREAM_RESET => StreamTerminalKind::Reset,
            sys::MOQ_MEDIA_STREAM_RECEIVER_DROP => StreamTerminalKind::ReceiverDrop,
            sys::MOQ_MEDIA_STREAM_END_OF_TRACK => StreamTerminalKind::EndOfTrack,
            kind => StreamTerminalKind::Other(kind),
        }
    }

    /// The `MOQ_MEDIA_STREAM_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            StreamTerminalKind::FinEndOfGroup => sys::MOQ_MEDIA_STREAM_FIN_END_OF_GROUP,
            StreamTerminalKind::Fin => sys::MOQ_MEDIA_STREAM_FIN,
            StreamTerminalKind::Reset => sys::MOQ_MEDIA_STREAM_RESET,
            StreamTerminalKind::ReceiverDrop => sys::MOQ_MEDIA_STREAM_RECEIVER_DROP,
            StreamTerminalKind::EndOfTrack => sys::MOQ_MEDIA_STREAM_END_OF_TRACK,
            StreamTerminalKind::Other(kind) => kind,
        }
    }
}

/// One delivery fact from the stream-terminal lane: where a subgroup's
/// stream ended, what the receiver dropped, or where the track ends. Each
/// record is pollable once every object of its track taken in before it
/// has left the receiver, so applying records after the objects they
/// follow keeps them in order.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamTerminal {
    /// What happened.
    pub kind: StreamTerminalKind,
    /// The track.
    pub track: Track,
    /// FIN/RESET: the subgroup's group. END_OF_TRACK: its Location's group.
    /// RECEIVER_DROP: the first group of the range.
    pub group_id: u64,
    /// FIN/RESET: the subgroup.
    pub subgroup_id: u64,
    /// END_OF_TRACK: its Location's object.
    pub object_id: u64,
    /// RECEIVER_DROP: the last group of the range.
    pub last_group_id: u64,
    /// RESET: the peer's RESET_STREAM code.
    pub error_code: u64,
    /// FIN/RESET: the last object the stream carried completely.
    pub last_object_id: Option<u64>,
    /// RESET: the object the reset cut short.
    pub cut_object_id: Option<u64>,
    /// RECEIVER_DROP: whose objects were dropped.
    pub delivery_origin: DeliveryOrigin,
    /// RECEIVER_DROP of a fill: its initiating request ID (zero is valid).
    pub initiating_request_id: u64,
}

/// A CMSF SAP timeline record (from a subscribed `eventtimeline` track).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct SapRecord {
    pub track: Track,
    pub location: Location,
    pub sap_type: SapType,
    pub ept_ms: u64,
}

/// An MSF media timeline record (from a subscribed `mediatimeline` track).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct TimelineRecord {
    pub track: Track,
    pub media_time_ms: u64,
    pub location: Location,
    /// 0 for VOD or unknown.
    pub wallclock_ms: u64,
}

/// Where a subscription starts (`moq_media_start_mode_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StartMode {
    /// The live edge.
    #[default]
    Current,
    /// The next group boundary.
    NextGroup,
}

/// Options applied when a track's subscription is first issued; a later
/// subscribe is a resume and leaves them unchanged. It is the
/// application's record (start from `Default`); it is not
/// `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscribeOptions {
    /// Where delivery starts, unless `location` is given.
    pub start: StartMode,
    /// Subscriber priority.
    pub priority: Option<u8>,
    /// MPEG-TS: the program whose PMT names the random access and PCR PIDs
    /// the track's reorder window measures with (it matters on an
    /// unmodified-multiplex track); `None` selects the catalog's
    /// mpeg2tsProgramNumber, else the PAT's first program.
    pub mpeg2ts_program: Option<NonZeroU32>,
    /// The subscription's Location Filter. Given, it supersedes `start`, and
    /// it suppresses the implicit current-group fill, as any explicit
    /// option does. Drafts 16 and 18 take
    /// [`Unconstrained`](LocationFilter::Unconstrained),
    /// [`NextObject`](LocationFilter::NextObject),
    /// [`RelativeGroup`](LocationFilter::RelativeGroup) with offset 0,
    /// [`AbsoluteStart`](LocationFilter::AbsoluteStart) and
    /// [`GroupEnd`](LocationFilter::GroupEnd), within 62 bits on draft 16;
    /// the others need the draft-22 companion.
    pub location: Option<LocationFilter>,
    /// A fill requested with the subscription (draft 22's FILL_PARAMETERS;
    /// it needs the draft-22 companion). It suppresses the implicit
    /// current-group fill.
    pub fill: Option<FillOptions>,
}

/// A fill requested with a subscription (`moq_fill_cfg_t`): a range of
/// already-published objects, delivered on a stream of its own beside the
/// live ones. Its objects report [`DeliveryOrigin::Fill`] and the fill's
/// initiating request ID, and one [`TrackEventKind::FillTerminal`] ends it.
/// What it leaves out inherits the subscription's effective state when the
/// request is processed. It is the application's record (start from
/// `Default`); it is not `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FillOptions {
    /// Subscriber priority of the fill.
    pub subscriber_priority: Option<u8>,
    /// The order its groups come in.
    pub group_order: Option<GroupOrder>,
    /// How long upstream may take, in whole milliseconds (anything finer is
    /// [`Error::INVAL`]); zero takes only objects available at once.
    /// `None` leaves the wait to the implementation's policy.
    pub timeout: Option<Duration>,
    /// Which objects; `None` inherits the subscription's Location Filter.
    pub location: Option<LocationFilter>,
}

/// What [`Receiver::poll_object_within`] found at the head of the queue.
// The taken object is held by value, as poll_object returns it: a poll's
// result is consumed at once, and boxing it would allocate on every poll.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum PollWithin {
    /// The next object fit within the charge, and is the caller's now.
    Taken {
        /// The object.
        object: MediaObject,
        /// Its charge (as [`MediaObject::charge`]).
        charge: u64,
    },
    /// The next object needs `charge` bytes, more than allowed: nothing was
    /// dequeued and nothing changed.
    TooLarge {
        /// What the object would charge.
        charge: u64,
    },
    /// No object is queued now.
    Empty,
}

/// What a component built on a [`Receiver`] needs to know about it, as one
/// snapshot (`moq_media_receiver_info_t`). Configuration values are the
/// effective ones: a defaulted zero is reported as the value it selected.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiverInfo {
    /// The time mode.
    pub time_mode: TimeMode,
    /// The overflow policy.
    pub overflow_policy: OverflowPolicy,
    /// Object-queue depth.
    pub max_objects: u32,
    /// Object-queue byte budget.
    pub max_bytes: u64,
    /// Media tracks are subscribed on discovery.
    pub auto_subscribe: bool,
    /// The stream-terminal lane's depth; `None` with the lane off.
    pub stream_terminals: Option<u32>,
    /// The companion, [`CompanionProfile::Auto`] until the first session
    /// resolves it.
    pub companion_profile: CompanionProfile,
    /// The negotiated draft; `None` until a session has been observed.
    pub transport_version: Option<Version>,
    /// Track events [`Receiver::poll_track`] has returned so far.
    pub track_events_polled: u64,
}

/// A track's delivery state (`moq_media_track_state_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackState {
    /// In the catalog, never subscribed.
    Discovered,
    /// Subscribe issued, not yet active.
    Pending,
    /// Subscribed and delivering.
    Active,
    /// Paused by [`Receiver::unsubscribe_track`].
    PausedApp,
    /// Paused by flow-control backpressure.
    PausedFlow,
    /// Ended (peer rejection or end of track).
    Ended,
    /// Failed locally; never rearmed.
    Failed,
    /// A state this crate does not know.
    Other(u32),
}

impl TrackState {
    fn from_raw(state: u32) -> TrackState {
        match state {
            sys::MOQ_MEDIA_TRACK_STATE_DISCOVERED => TrackState::Discovered,
            sys::MOQ_MEDIA_TRACK_STATE_PENDING => TrackState::Pending,
            sys::MOQ_MEDIA_TRACK_STATE_ACTIVE => TrackState::Active,
            sys::MOQ_MEDIA_TRACK_STATE_PAUSED_APP => TrackState::PausedApp,
            sys::MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW => TrackState::PausedFlow,
            sys::MOQ_MEDIA_TRACK_STATE_ENDED => TrackState::Ended,
            sys::MOQ_MEDIA_TRACK_STATE_FAILED => TrackState::Failed,
            state => TrackState::Other(state),
        }
    }

    /// The `moq_media_track_state_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            TrackState::Discovered => sys::MOQ_MEDIA_TRACK_STATE_DISCOVERED,
            TrackState::Pending => sys::MOQ_MEDIA_TRACK_STATE_PENDING,
            TrackState::Active => sys::MOQ_MEDIA_TRACK_STATE_ACTIVE,
            TrackState::PausedApp => sys::MOQ_MEDIA_TRACK_STATE_PAUSED_APP,
            TrackState::PausedFlow => sys::MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW,
            TrackState::Ended => sys::MOQ_MEDIA_TRACK_STATE_ENDED,
            TrackState::Failed => sys::MOQ_MEDIA_TRACK_STATE_FAILED,
            TrackState::Other(state) => state,
        }
    }
}

/// A [`Receiver`] statistics snapshot. Appended counters are `None` when
/// the library did not report them.
#[non_exhaustive]
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
    pub catalog_drops: Option<u64>,
    /// A terminal catalog with isComplete was adopted.
    pub catalog_complete: Option<bool>,
    pub gaps_reported: Option<u64>,
    pub gap_tails_unknown: Option<u64>,
    pub reorder_refused: Option<u64>,
    pub tracks_malformed: Option<u64>,
}

/// A CMSF content-protection entry resolved from a receiver's catalog.
#[derive(Clone, Copy)]
pub struct ContentProtectionRef<'receiver>(&'receiver sys::moq_cmsf_content_protection_t);

impl<'receiver> ContentProtectionRef<'receiver> {
    /// refID.
    #[must_use]
    pub fn ref_id(&self) -> &'receiver [u8] {
        // SAFETY: retained by the receiver until destroyed.
        unsafe { ffi::bytes(self.0.ref_id) }
    }
    /// defaultKID strings.
    pub fn default_kids(&self) -> impl Iterator<Item = &'receiver [u8]> {
        // SAFETY: as for ref_id.
        let kids = unsafe { ffi::array(self.0.default_kids, self.0.default_kid_count) };
        // SAFETY: as for ref_id.
        kids.iter().map(|kid| unsafe { ffi::bytes(*kid) })
    }
    /// Scheme.
    #[must_use]
    pub fn scheme(&self) -> &'receiver [u8] {
        // SAFETY: as for ref_id.
        unsafe { ffi::bytes(self.0.scheme) }
    }
    /// drmSystem.systemID.
    #[must_use]
    pub fn system_id(&self) -> &'receiver [u8] {
        // SAFETY: as for ref_id.
        unsafe { ffi::bytes(self.0.drm_system.system_id) }
    }
    fn url(&self, url: &sys::moq_cmsf_url_t) -> Option<&'receiver [u8]> {
        if !url.present {
            return None;
        }
        // SAFETY: as for ref_id.
        Some(unsafe { ffi::bytes(url.url) })
    }
    /// laURL.
    #[must_use]
    pub fn la_url(&self) -> Option<&'receiver [u8]> {
        self.url(&self.0.drm_system.la_url)
    }
    /// certURL.
    #[must_use]
    pub fn cert_url(&self) -> Option<&'receiver [u8]> {
        self.url(&self.0.drm_system.cert_url)
    }
    /// authURL.
    #[must_use]
    pub fn auth_url(&self) -> Option<&'receiver [u8]> {
        self.url(&self.0.drm_system.auth_url)
    }
    /// Base64 PSSH.
    #[must_use]
    pub fn pssh(&self) -> Option<&'receiver [u8]> {
        let drm = &self.0.drm_system;
        if !drm.has_pssh {
            return None;
        }
        // SAFETY: as for ref_id.
        Some(unsafe { ffi::bytes(drm.pssh) })
    }
    /// Robustness level.
    #[must_use]
    pub fn robustness(&self) -> Option<&'receiver [u8]> {
        let drm = &self.0.drm_system;
        if !drm.has_robustness {
            return None;
        }
        // SAFETY: as for ref_id.
        Some(unsafe { ffi::bytes(drm.robustness) })
    }
    /// The raw entry.
    #[must_use]
    pub fn raw(&self) -> &'receiver sys::moq_cmsf_content_protection_t {
        self.0
    }
}

// The reference, not the raw spans a derive would print.
impl fmt::Debug for ContentProtectionRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContentProtectionRef")
            .field("ref_id", &String::from_utf8_lossy(self.ref_id()))
            .field("scheme", &String::from_utf8_lossy(self.scheme()))
            .finish_non_exhaustive()
    }
}

/// The C fill request for `fill`, sized and stamped by the library's
/// initializer, its location pointing at `location` (alive for the call).
fn raw_fill(
    fill: &FillOptions,
    location: Option<&sys::moq_location_filter_cfg_t>,
) -> Result<sys::moq_fill_cfg_t> {
    let mut raw = sys::moq_fill_cfg_t::default();
    // SAFETY: a writable struct of exactly the size passed.
    unsafe { sys::moq_fill_cfg_init_sized(&mut raw, mem::size_of_val(&raw)) };
    if let Some(priority) = fill.subscriber_priority {
        raw.has_subscriber_priority = true;
        raw.subscriber_priority = priority;
    }
    if let Some(order) = fill.group_order {
        raw.has_group_order = true;
        raw.group_order = order.code();
    }
    if let Some(timeout) = fill.timeout {
        raw.has_fill_timeout = true;
        raw.fill_timeout_ms = ffi::millis(timeout)?;
    }
    raw.location = location.map_or(ptr::null(), |location| location as *const _);
    Ok(raw)
}

const EVENT_V0: usize = ffi::field_end!(sys::moq_media_track_event_t, config_generation, u32);
const RECEIVER_STATS_V0: usize = ffi::field_end!(sys::moq_media_receiver_stats_t, paused, bool);

/// Receives a namespace: discovers its tracks from the MSF catalog and
/// delivers their objects, timeline records and stream terminals.
///
/// Calls come from one application thread at a time: the type is `Send`
/// but not `Sync` (share it behind a `Mutex`). It holds its [`Endpoint`],
/// which therefore outlives it. Dropping it destroys the receiver. A
/// borrowed description or span cannot outlive it: their lifetimes are tied
/// to the receiver. A [`Track`] is a plain `Copy` identifier and can: the
/// native handle it names is invalid once the receiver is destroyed, and
/// the owner tag refuses it at any other receiver with [`Error::INVAL`], so
/// a stale identifier is an error, never a dereference.
#[derive(Debug)]
pub struct Receiver {
    raw: NonNull<sys::moq_media_receiver_t>,
    owner: u64,
    endpoint: Endpoint,
    _not_sync: PhantomData<Cell<()>>,
}

// SAFETY: libmoq accepts receiver calls from any application thread as long
// as they are serialized, which `!Sync` enforces.
unsafe impl Send for Receiver {}

impl Receiver {
    /// Attach a receiver to `endpoint`.
    pub fn attach(endpoint: &Endpoint, config: &ReceiverConfig) -> Result<Receiver> {
        let mut raw_config = sys::moq_media_receiver_cfg_t::default();
        let size = mem::size_of_val(&raw_config);
        // SAFETY: a writable struct of exactly the size passed.
        unsafe {
            match config.preset {
                ReceiverPreset::Live => {
                    sys::moq_media_receiver_cfg_init_live_sized(&mut raw_config, size)
                }
                ReceiverPreset::FlowControl => {
                    sys::moq_media_receiver_cfg_init_flow_control_sized(&mut raw_config, size)
                }
            }
        }
        raw_config.endpoint = ptr::null(); // attach: the endpoint is given
        raw_config.namespace_ = config.namespace.raw();
        raw_config.catalog_track = ffi::optional_span(config.catalog_track.as_deref());
        if let Some(auto) = config.auto_subscribe {
            raw_config.auto_subscribe = auto;
        }
        if let Some(mode) = config.time_mode {
            raw_config.time_mode = mode.raw();
        }
        if let Some(max_objects) = config.max_objects {
            raw_config.overflow.max_objects = max_objects;
        }
        if let Some(max_bytes) = config.max_bytes {
            raw_config.overflow.max_bytes = max_bytes;
        }
        if let Some(depth) = config.max_track_events {
            raw_config.max_track_events = depth;
        }
        if let Some(profile) = config.companion_profile {
            raw_config.companion_profile = profile.raw();
        }
        if let Some(max) = config.stream_terminals {
            raw_config.surface_stream_terminals = true;
            raw_config.max_stream_terminals = max;
        }
        if let Some((slots, window)) = config.mpeg2ts_reorder {
            raw_config.mpeg2ts_reorder_slots = slots;
            raw_config.mpeg2ts_reorder_window_us = ffi::micros(window)?;
        }
        if let Some(strip) = config.mpeg2ts_strip_prefix {
            raw_config.mpeg2ts_strip_prefix = strip;
        }
        if let Some(join) = config.mpeg2ts_join_psi {
            raw_config.mpeg2ts_join_psi = join;
        }
        let mut raw = ptr::null_mut();
        // SAFETY: every pointer in raw_config points into config, alive for
        // the call; the service copies the namespace and catalog name at
        // attach.
        ffi::check(unsafe {
            sys::moq_media_receiver_attach(endpoint.as_ptr(), &raw_config, &mut raw)
        })?;
        Ok(Receiver {
            raw: NonNull::new(raw).ok_or(Error::INTERNAL)?,
            owner: sender::next_owner_id(),
            endpoint: endpoint.clone(),
            _not_sync: PhantomData,
        })
    }

    /// The endpoint this receiver is attached to.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The underlying `moq_media_receiver_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_media_receiver_t {
        self.raw.as_ptr()
    }

    /// The native handle of one of this receiver's tracks; a foreign track
    /// is [`Error::INVAL`].
    pub(crate) fn own(&self, track: Track) -> Result<*mut sys::moq_media_track_t> {
        if track.owner == self.owner {
            Ok(track.raw.as_ptr())
        } else {
            Err(Error::INVAL)
        }
    }

    /// The tag of this receiver's tracks.
    #[cfg(feature = "player")]
    pub(crate) fn owner(&self) -> u64 {
        self.owner
    }

    fn track_from_raw(&self, raw: *mut sys::moq_media_track_t) -> Result<Track> {
        let raw = NonNull::new(raw).ok_or(Error::INTERNAL)?;
        Ok(Track {
            raw,
            owner: self.owner,
        })
    }

    /// The next track event, or `None` when there is none now. Drain
    /// events before objects on each wakeup: a track's ADDED always
    /// precedes its first object. Fails with [`Error::CLOSED`] once empty
    /// and terminal.
    pub fn poll_track(&self) -> Result<Option<TrackEvent<'_>>> {
        let mut event = sys::moq_media_track_event_t::default();
        let size = mem::size_of_val(&event);
        // SAFETY: a live receiver and a writable struct of the size passed.
        let code = unsafe { sys::moq_media_receiver_poll_track(self.as_ptr(), &mut event, size) };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        ffi::check_stamp(event.struct_size, EVENT_V0, size)?;
        Ok(Some(TrackEvent {
            raw: event,
            receiver: self,
        }))
    }

    /// The next media object, or `None` when there is none now. Fails with
    /// [`Error::INTERRUPTED`] while the latch is set and [`Error::CLOSED`]
    /// once empty and terminal.
    pub fn poll_object(&self) -> Result<Option<MediaObject>> {
        let mut object = sys::moq_media_object_t::default();
        let size = mem::size_of_val(&object);
        // SAFETY: a live receiver and a writable struct of the size passed.
        let code = unsafe { sys::moq_media_receiver_poll_object(self.as_ptr(), &mut object, size) };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        self.take_object(object, size).map(Some)
    }

    /// The next media object, only when everything it carries fits in
    /// `max_charge` bytes (its buffers and sample table, as
    /// [`MediaObject::charge`] measures them). The check and the dequeue
    /// are one step: no other object can replace the head between them. A
    /// larger head stays queued and is reported with its charge. Every
    /// other outcome is [`poll_object`](Receiver::poll_object)'s.
    pub fn poll_object_within(&self, max_charge: u64) -> Result<PollWithin> {
        let mut object = sys::moq_media_object_t::default();
        let size = mem::size_of_val(&object);
        let mut charge = 0;
        // SAFETY: a live receiver, a writable charge and a writable struct
        // of the size passed.
        let code = unsafe {
            sys::moq_media_receiver_poll_object_within(
                self.as_ptr(),
                max_charge,
                &mut charge,
                &mut object,
                size,
            )
        };
        match code {
            sys::MOQ_DONE => Ok(PollWithin::Empty),
            sys::MOQ_ERR_BUFFER => Ok(PollWithin::TooLarge { charge }),
            code => {
                ffi::check(code)?;
                let object = self.take_object(object, size)?;
                Ok(PollWithin::Taken { object, charge })
            }
        }
    }

    /// Own a polled record, or refuse one stamped outside the contract
    /// after releasing what it transferred.
    fn take_object(&self, mut object: sys::moq_media_object_t, size: usize) -> Result<MediaObject> {
        if let Err(refusal) = ffi::check_stamp(object.struct_size, OBJECT_V0, size) {
            // The record owns transferred buffers: release them within this
            // struct before refusing it.
            object.struct_size = object.struct_size.min(size as u32);
            // SAFETY: cleanup bounds itself by the stamp, now at most this
            // struct's size.
            unsafe { sys::moq_media_object_cleanup(&mut object) };
            return Err(refusal);
        }
        Ok(MediaObject {
            raw: object,
            owner: self.owner,
            _not_send: PhantomData,
        })
    }

    /// The next stream-terminal record, or `None` when none is pollable
    /// now (none queued, the lane is off, or the head record still waits
    /// for its track's objects to leave the receiver).
    pub fn poll_stream_terminal(&self) -> Result<Option<StreamTerminal>> {
        let mut record = sys::moq_media_stream_terminal_t::default();
        let size = mem::size_of_val(&record);
        // SAFETY: a live receiver and a writable struct of the size passed.
        let code = unsafe {
            sys::moq_media_receiver_poll_stream_terminal(self.as_ptr(), &mut record, size)
        };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        ffi::check_stamp(
            record.struct_size,
            sys::MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE,
            size,
        )?;
        Ok(Some(StreamTerminal {
            kind: StreamTerminalKind::from_raw(record.kind),
            track: self.track_from_raw(record.track)?,
            group_id: record.group_id,
            subgroup_id: record.subgroup_id,
            object_id: record.object_id,
            last_group_id: record.last_group_id,
            error_code: record.error_code,
            last_object_id: record.has_last_object.then_some(record.last_object_id),
            cut_object_id: record.has_cut_object.then_some(record.cut_object_id),
            delivery_origin: DeliveryOrigin::from_raw(record.delivery_origin),
            initiating_request_id: record.initiating_request_id,
        }))
    }

    /// The next SAP timeline record.
    pub fn poll_sap(&self) -> Result<Option<SapRecord>> {
        let mut record = sys::moq_media_sap_record_t::default();
        let size = mem::size_of_val(&record);
        // SAFETY: a live receiver and a writable struct of the size passed.
        let code = unsafe { sys::moq_media_receiver_poll_sap(self.as_ptr(), &mut record, size) };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        ffi::check_stamp(record.struct_size, size, size)?;
        Ok(Some(SapRecord {
            track: self.track_from_raw(record.track)?,
            location: Location {
                group_id: record.group,
                object_id: record.object,
            },
            sap_type: SapType::from_raw(record.sap_type),
            ept_ms: record.ept_ms,
        }))
    }

    /// The next media timeline record.
    pub fn poll_media_timeline(&self) -> Result<Option<TimelineRecord>> {
        let mut record = sys::moq_media_timeline_record_t::default();
        let size = mem::size_of_val(&record);
        // SAFETY: a live receiver and a writable struct of the size passed.
        let code = unsafe {
            sys::moq_media_receiver_poll_media_timeline(self.as_ptr(), &mut record, size)
        };
        if code == sys::MOQ_DONE {
            return Ok(None);
        }
        ffi::check(code)?;
        ffi::check_stamp(record.struct_size, size, size)?;
        Ok(Some(TimelineRecord {
            track: self.track_from_raw(record.track)?,
            media_time_ms: record.media_time_ms,
            location: Location {
                group_id: record.group,
                object_id: record.object,
            },
            wallclock_ms: record.wallclock_ms,
        }))
    }

    /// Block until something is pollable on any of the receive queues, the
    /// receiver wakes or goes terminal, or `timeout` (exact microseconds;
    /// zero polls) elapses. [`WaitResult::Ready`] is "poll now".
    pub fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        let timeout_us = ffi::micros(timeout)?;
        // SAFETY: a live receiver, called from an application thread.
        ffi::wait_result(unsafe { sys::moq_media_receiver_wait(self.as_ptr(), timeout_us) })
    }

    /// Ask for a track's objects, or resume them. Asynchronous: success
    /// means the request is recorded, not that the peer accepted. The
    /// options apply only when the subscription is first issued; a later
    /// subscribe validates them and leaves the subscription as it is.
    /// [`Error::UNSUPPORTED`] when the track has no delivery path, or when
    /// a location or fill cannot be carried by the negotiated draft and
    /// companion; nothing is recorded then.
    pub fn subscribe_track(&self, track: Track, options: Option<SubscribeOptions>) -> Result<()> {
        let track = self.own(track)?;
        // The nested records the options point at, alive through the call.
        let location = options.and_then(|o| o.location).map(LocationFilter::raw);
        let fill_options = options.and_then(|o| o.fill);
        let fill_location = fill_options
            .and_then(|fill| fill.location)
            .map(LocationFilter::raw);
        let fill = match fill_options {
            Some(fill) => Some(raw_fill(&fill, fill_location.as_ref())?),
            None => None,
        };
        let raw_options = options.map(|options| {
            let mut raw_options = sys::moq_media_receiver_track_subscribe_cfg_t::default();
            // SAFETY: a writable struct of exactly the size passed.
            unsafe {
                sys::moq_media_receiver_track_subscribe_cfg_init_sized(
                    &mut raw_options,
                    mem::size_of_val(&raw_options),
                )
            };
            raw_options.start = match options.start {
                StartMode::Current => sys::MOQ_MEDIA_START_CURRENT,
                StartMode::NextGroup => sys::MOQ_MEDIA_START_NEXT_GROUP,
            };
            raw_options.has_priority = options.priority.is_some();
            raw_options.priority = options.priority.unwrap_or(0);
            raw_options.mpeg2ts_program_number = options.mpeg2ts_program.map_or(0, NonZeroU32::get);
            raw_options.location = location
                .as_ref()
                .map_or(ptr::null(), |location| location as *const _);
            raw_options.fill = fill.as_ref().map_or(ptr::null(), |fill| fill as *const _);
            raw_options
        });
        let options_ptr = raw_options
            .as_ref()
            .map_or(ptr::null(), |raw_options| raw_options as *const _);
        // SAFETY: a live receiver and one of its handles; the options and
        // the records they point at (when given) are borrowed for the call
        // and copied by the service.
        ffi::check(unsafe {
            sys::moq_media_receiver_subscribe_track(self.as_ptr(), track, options_ptr)
        })
    }

    /// Cancel one fill of `track`, named by its initiating request ID (zero
    /// is a valid ID). Asynchronous: success means the cancellation is
    /// scheduled (repeating it while pending is idempotent), not that it won
    /// a race with the peer's terminal. Exactly one
    /// [`TrackEventKind::FillTerminal`] reports how the fill ended, with
    /// [`FillReason::LocalCancelled`] or the terminal that won. The track
    /// and its other fills continue, and objects already polled stay valid.
    /// Accepted while the interrupt latch is set; interruption does not
    /// cancel recorded intent.
    ///
    /// [`Error::STALE_HANDLE`] for an unknown or completed fill,
    /// [`Error::UNSUPPORTED`] outside the draft-22 companion,
    /// [`Error::CLOSED`] for a terminal receiver or endpoint.
    pub fn cancel_fill(&self, track: Track, initiating_request_id: u64) -> Result<()> {
        let track = self.own(track)?;
        // SAFETY: a live receiver and one of its handles.
        ffi::check(unsafe {
            sys::moq_media_receiver_cancel_fill(self.as_ptr(), track, initiating_request_id)
        })
    }

    /// Pause a track's delivery and purge its queued objects. A later
    /// subscribe resumes it.
    pub fn unsubscribe_track(&self, track: Track) -> Result<()> {
        let track = self.own(track)?;
        // SAFETY: a live receiver and one of its handles.
        ffi::check(unsafe { sys::moq_media_receiver_unsubscribe_track(self.as_ptr(), track) })
    }

    /// A track's current delivery state, a point-in-time snapshot.
    pub fn track_state(&self, track: Track) -> Result<TrackState> {
        let track = self.own(track)?;
        let mut state = 0;
        // SAFETY: a live receiver and one of its handles; state is writable.
        ffi::check(unsafe {
            sys::moq_media_receiver_track_state(self.as_ptr(), track, &mut state)
        })?;
        Ok(TrackState::from_raw(state))
    }

    /// A track's current description, copied under the receiver's lock
    /// now. Removed, ended and failed tracks stay readable.
    pub fn track_description(&self, track: Track) -> Result<TrackDescription<'_>> {
        let track = self.own(track)?;
        let mut description = sys::moq_media_track_desc_t::default();
        let size = mem::size_of_val(&description);
        // SAFETY: a live receiver and one of its handles; description is a
        // writable struct of the size passed.
        ffi::check(unsafe {
            sys::moq_media_receiver_track_desc_copy(self.as_ptr(), track, &mut description, size)
        })?;
        ffi::check_stamp(
            description.struct_size,
            sys::MOQ_MEDIA_TRACK_DESC_V0_SIZE,
            size,
        )?;
        Ok(TrackDescription {
            cmaf: copy_cmaf_record(description.cmaf)?,
            raw: description,
            _receiver: PhantomData,
        })
    }

    /// Resolve a contentProtectionRefID against the current catalog. An
    /// entry stays valid until the receiver is destroyed.
    #[must_use]
    pub fn find_content_protection(&self, ref_id: &[u8]) -> Option<ContentProtectionRef<'_>> {
        // SAFETY: a live receiver; the span borrows ref_id for the call and
        // the returned entry is retained until destroy.
        let entry = unsafe {
            sys::moq_media_receiver_find_content_protection(self.as_ptr(), ffi::span(ref_id))
                .as_ref()
        };
        entry.map(ContentProtectionRef)
    }

    /// A statistics snapshot.
    pub fn stats(&self) -> Result<ReceiverStats> {
        let mut stats = sys::moq_media_receiver_stats_t::default();
        let size = mem::size_of_val(&stats);
        // SAFETY: a live receiver and a writable struct of the size passed.
        ffi::check(unsafe { sys::moq_media_receiver_get_stats(self.as_ptr(), &mut stats, size) })?;
        ffi::check_stamp(stats.struct_size, RECEIVER_STATS_V0, size)?;
        let reported = |end: usize| ffi::covers(stats.struct_size, end);
        Ok(ReceiverStats {
            objects_received: stats.objects_received,
            objects_queued: stats.objects_queued,
            bytes_queued: stats.bytes_queued,
            objects_dropped: stats.objects_dropped,
            groups_dropped: stats.groups_dropped,
            keyframes_dropped: stats.keyframes_dropped,
            parse_drops: stats.parse_drops,
            overflow_events: stats.overflow_events,
            pause_transitions: stats.pause_transitions,
            paused: stats.paused,
            catalog_drops: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                catalog_drops,
                u64
            ))
            .then_some(stats.catalog_drops),
            catalog_complete: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                catalog_complete,
                bool
            ))
            .then_some(stats.catalog_complete),
            gaps_reported: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                gaps_reported,
                u64
            ))
            .then_some(stats.gaps_reported),
            gap_tails_unknown: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                gap_tails_unknown,
                u64
            ))
            .then_some(stats.gap_tails_unknown),
            reorder_refused: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                reorder_refused,
                u64
            ))
            .then_some(stats.reorder_refused),
            tracks_malformed: reported(ffi::field_end!(
                sys::moq_media_receiver_stats_t,
                tracks_malformed,
                u64
            ))
            .then_some(stats.tracks_malformed),
        })
    }

    /// What the receiver is: its effective configuration, the resolved
    /// companion and draft, and the track events polled so far. Observes
    /// only.
    pub fn info(&self) -> Result<ReceiverInfo> {
        let mut info = sys::moq_media_receiver_info_t::default();
        let size = mem::size_of_val(&info);
        // SAFETY: a live receiver and a writable struct of the size passed.
        ffi::check(unsafe { sys::moq_media_receiver_get_info(self.as_ptr(), &mut info, size) })?;
        ffi::check_stamp(info.struct_size, sys::MOQ_MEDIA_RECEIVER_INFO_V0_SIZE, size)?;
        Ok(ReceiverInfo {
            time_mode: TimeMode::from_raw(info.time_mode),
            overflow_policy: OverflowPolicy::from_raw(info.overflow_policy),
            max_objects: info.max_objects,
            max_bytes: info.max_bytes,
            auto_subscribe: info.auto_subscribe,
            stream_terminals: info
                .surface_stream_terminals
                .then_some(info.max_stream_terminals),
            companion_profile: CompanionProfile::from_raw(info.companion_profile),
            transport_version: (info.transport_version != 0)
                .then(|| Version::from_draft(info.transport_version)),
            track_events_polled: info.track_events_polled,
        })
    }

    /// Closed cleanly or drained.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        // SAFETY: a live receiver.
        unsafe { sys::moq_media_receiver_is_closed(self.as_ptr()) }
    }

    /// Failed (connect, certificate, protocol, transport or receiver
    /// failure).
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        // SAFETY: a live receiver.
        unsafe { sys::moq_media_receiver_is_fatal(self.as_ptr()) }
    }

    /// The receiver's own fatal code when receiver-fatal, otherwise the
    /// endpoint's.
    #[must_use]
    pub fn fatal_code(&self) -> u64 {
        // SAFETY: a live receiver.
        unsafe { sys::moq_media_receiver_fatal_code(self.as_ptr()) }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // SAFETY: the sole owner. Every borrowed description and span is
        // gone (their lifetimes are tied to `self`); a Track identifier
        // that outlives this call names nothing the crate dereferences, as
        // the owner check precedes every use. Destroy detaches before the
        // endpoint handle below is released.
        unsafe { sys::moq_media_receiver_destroy(self.raw.as_ptr()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_drop_classes_round_trip() {
        let named = [
            (ParseDropClass::Media, sys::MOQ_MEDIA_PARSE_DROP_MEDIA),
            (ParseDropClass::Sap, sys::MOQ_MEDIA_PARSE_DROP_SAP),
            (
                ParseDropClass::MediaTimeline,
                sys::MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE,
            ),
        ];
        for (class, raw) in named {
            assert_eq!(class.code(), raw);
            assert_eq!(ParseDropClass::from_raw(raw), class);
        }
        assert_eq!(ParseDropClass::from_raw(99), ParseDropClass::Other(99));
        assert_eq!(ParseDropClass::Other(99).code(), 99);
    }

    #[test]
    fn track_states_round_trip() {
        let named = [
            (
                TrackState::Discovered,
                sys::MOQ_MEDIA_TRACK_STATE_DISCOVERED,
            ),
            (TrackState::Pending, sys::MOQ_MEDIA_TRACK_STATE_PENDING),
            (TrackState::Active, sys::MOQ_MEDIA_TRACK_STATE_ACTIVE),
            (TrackState::PausedApp, sys::MOQ_MEDIA_TRACK_STATE_PAUSED_APP),
            (
                TrackState::PausedFlow,
                sys::MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW,
            ),
            (TrackState::Ended, sys::MOQ_MEDIA_TRACK_STATE_ENDED),
            (TrackState::Failed, sys::MOQ_MEDIA_TRACK_STATE_FAILED),
        ];
        for (state, raw) in named {
            assert_eq!(state.code(), raw);
            assert_eq!(TrackState::from_raw(raw), state);
        }
        assert_eq!(TrackState::from_raw(99), TrackState::Other(99));
        assert_eq!(TrackState::Other(99).code(), 99);
    }
}
