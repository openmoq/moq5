//! Events: what the session tells the application.
//!
//! Every native event kind has a variant. Each record borrows the session
//! exclusively (its borrowed bytes point into the session's output scratch,
//! valid until the next advancing call), is neither `Send` nor `Sync`, and
//! holds its owned references as [`Payload`]s. A field after a kind's frozen
//! prefix is `None` when the library did not report it.

use std::marker::PhantomData;

use super::config::Perspective;
use super::handle::{
    Announcement, Fetch, Fill, NamespaceSubscription, OwnerId, Publication, Subgroup, Subscription,
    TrackStatusRequest, TrackSubscription,
};
use super::payload::Payload;
use super::raw::field_end;
use super::request::RedirectTarget;
use super::stream::StreamRef;
use super::time::Instant;
use super::view::{
    self, ClusterView, NamespaceView, Reader, Record, RequestRole, TerminalCause, Tokens,
};
use crate::{sys, Error, GroupOrder, Location, LocationFilter, ObjectStatus, Result};

/// An event kind (`moq_event_kind_t`, an open set). Every native kind has a
/// variant; a kind this crate does not know is kept as `Other`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EventKind {
    /// `MOQ_EVENT_SETUP_COMPLETE`.
    SetupComplete,
    /// `MOQ_EVENT_SESSION_CLOSED`.
    SessionClosed,
    /// `MOQ_EVENT_SUBSCRIBE_REQUEST`.
    SubscribeRequest,
    /// `MOQ_EVENT_SUBSCRIBE_OK`.
    SubscribeOk,
    /// `MOQ_EVENT_SUBSCRIBE_ERROR`.
    SubscribeError,
    /// `MOQ_EVENT_REQUEST_READY`.
    RequestReady,
    /// `MOQ_EVENT_OBJECT_RECEIVED`.
    ObjectReceived,
    /// `MOQ_EVENT_NAMESPACE_PUBLISHED`.
    NamespacePublished,
    /// `MOQ_EVENT_NAMESPACE_DONE`.
    NamespaceDone,
    /// `MOQ_EVENT_NAMESPACE_REJECTED`.
    NamespaceRejected,
    /// `MOQ_EVENT_NAMESPACE_ACCEPTED`.
    NamespaceAccepted,
    /// `MOQ_EVENT_NAMESPACE_CANCELLED`.
    NamespaceCancelled,
    /// `MOQ_EVENT_GOAWAY`.
    Goaway,
    /// `MOQ_EVENT_UNSUBSCRIBED`.
    Unsubscribed,
    /// `MOQ_EVENT_SUBSCRIBE_UPDATED`.
    SubscribeUpdated,
    /// `MOQ_EVENT_OBJECT_CHUNK`.
    ObjectChunk,
    /// `MOQ_EVENT_NS_SUB_REQUEST`.
    NamespaceSubscriptionRequest,
    /// `MOQ_EVENT_NS_SUB_OK`.
    NamespaceSubscriptionOk,
    /// `MOQ_EVENT_NS_SUB_ERROR`.
    NamespaceSubscriptionError,
    /// `MOQ_EVENT_NAMESPACE_FOUND`.
    NamespaceFound,
    /// `MOQ_EVENT_NAMESPACE_GONE`.
    NamespaceGone,
    /// `MOQ_EVENT_FETCH_REQUEST`.
    FetchRequest,
    /// `MOQ_EVENT_FETCH_ERROR`.
    FetchError,
    /// `MOQ_EVENT_FETCH_CANCELLED`.
    FetchCancelled,
    /// `MOQ_EVENT_FETCH_OK`.
    FetchOk,
    /// `MOQ_EVENT_FETCH_COMPLETE`.
    FetchComplete,
    /// `MOQ_EVENT_FETCH_OBJECT`.
    FetchObject,
    /// `MOQ_EVENT_FETCH_GAP`.
    FetchGap,
    /// `MOQ_EVENT_PUBLISH_REQUEST`.
    PublishRequest,
    /// `MOQ_EVENT_PUBLISH_OK`.
    PublishOk,
    /// `MOQ_EVENT_PUBLISH_ERROR`.
    PublishError,
    /// `MOQ_EVENT_PUBLISH_FINISHED`.
    PublishFinished,
    /// `MOQ_EVENT_TRACK_STATUS_REQUEST`.
    TrackStatusRequest,
    /// `MOQ_EVENT_TRACK_STATUS_OK`.
    TrackStatusOk,
    /// `MOQ_EVENT_TRACK_STATUS_ERROR`.
    TrackStatusError,
    /// `MOQ_EVENT_SUBSCRIBE_DONE`.
    SubscribeDone,
    /// `MOQ_EVENT_PUBLISH_UNSUBSCRIBED`.
    PublishUnsubscribed,
    /// `MOQ_EVENT_PUBLISH_UPDATED`.
    PublishUpdated,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST`.
    SubscribeTracksRequest,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_OK`.
    SubscribeTracksOk,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR`.
    SubscribeTracksError,
    /// `MOQ_EVENT_PUBLISH_BLOCKED`.
    PublishBlocked,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED`.
    SubscribeTracksCancelled,
    /// `MOQ_EVENT_REQUEST_REDIRECT`.
    RequestRedirect,
    /// `MOQ_EVENT_REQUEST_GOAWAY`.
    RequestGoaway,
    /// `MOQ_EVENT_SUBGROUP_FINISHED`.
    SubgroupFinished,
    /// `MOQ_EVENT_SUBSCRIPTION_UPDATE_OK`.
    SubscriptionUpdateOk,
    /// `MOQ_EVENT_PUBLICATION_UPDATE_OK`.
    PublicationUpdateOk,
    /// `MOQ_EVENT_SUBGROUP_RESET`.
    SubgroupReset,
    /// `MOQ_EVENT_FETCH_RESET`.
    FetchReset,
    /// `MOQ_EVENT_TRACK_STATUS_TERMINATED`.
    TrackStatusTerminated,
    /// `MOQ_EVENT_FILL_CREATED`.
    FillCreated,
    /// `MOQ_EVENT_FILL_REQUEST`.
    FillRequest,
    /// `MOQ_EVENT_FILL_OBJECT`.
    FillObject,
    /// `MOQ_EVENT_FILL_GAP`.
    FillGap,
    /// `MOQ_EVENT_FILL_TERMINAL`.
    FillTerminal,
    /// `MOQ_EVENT_NAMESPACE_TERMINATED`.
    NamespaceTerminated,
    /// `MOQ_EVENT_PUBLISH_STATE`.
    PublishState,
    /// `MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED`.
    NamespaceSubscriptionTerminated,
    /// `MOQ_EVENT_NAMESPACE_UPDATE_REQUEST`.
    NamespaceUpdateRequest,
    /// `MOQ_EVENT_NS_SUB_UPDATE_REQUEST`.
    NamespaceSubscriptionUpdateRequest,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST`.
    SubscribeTracksUpdateRequest,
    /// `MOQ_EVENT_NAMESPACE_UPDATE_RESULT`.
    NamespaceUpdateResult,
    /// `MOQ_EVENT_NS_SUB_UPDATE_RESULT`.
    NamespaceSubscriptionUpdateResult,
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT`.
    SubscribeTracksUpdateResult,
    /// `MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED`.
    OutgoingSubgroupTerminated,
    /// A kind this crate does not know.
    Other(u32),
}

/// The native kinds, in the order of their codes, paired with their variants.
const KINDS: [(u32, EventKind); 66] = [
    (sys::MOQ_EVENT_SETUP_COMPLETE, EventKind::SetupComplete),
    (sys::MOQ_EVENT_SESSION_CLOSED, EventKind::SessionClosed),
    (
        sys::MOQ_EVENT_SUBSCRIBE_REQUEST,
        EventKind::SubscribeRequest,
    ),
    (sys::MOQ_EVENT_SUBSCRIBE_OK, EventKind::SubscribeOk),
    (sys::MOQ_EVENT_SUBSCRIBE_ERROR, EventKind::SubscribeError),
    (sys::MOQ_EVENT_REQUEST_READY, EventKind::RequestReady),
    (sys::MOQ_EVENT_OBJECT_RECEIVED, EventKind::ObjectReceived),
    (
        sys::MOQ_EVENT_NAMESPACE_PUBLISHED,
        EventKind::NamespacePublished,
    ),
    (sys::MOQ_EVENT_NAMESPACE_DONE, EventKind::NamespaceDone),
    (
        sys::MOQ_EVENT_NAMESPACE_REJECTED,
        EventKind::NamespaceRejected,
    ),
    (
        sys::MOQ_EVENT_NAMESPACE_ACCEPTED,
        EventKind::NamespaceAccepted,
    ),
    (
        sys::MOQ_EVENT_NAMESPACE_CANCELLED,
        EventKind::NamespaceCancelled,
    ),
    (sys::MOQ_EVENT_GOAWAY, EventKind::Goaway),
    (sys::MOQ_EVENT_UNSUBSCRIBED, EventKind::Unsubscribed),
    (
        sys::MOQ_EVENT_SUBSCRIBE_UPDATED,
        EventKind::SubscribeUpdated,
    ),
    (sys::MOQ_EVENT_OBJECT_CHUNK, EventKind::ObjectChunk),
    (
        sys::MOQ_EVENT_NS_SUB_REQUEST,
        EventKind::NamespaceSubscriptionRequest,
    ),
    (sys::MOQ_EVENT_NS_SUB_OK, EventKind::NamespaceSubscriptionOk),
    (
        sys::MOQ_EVENT_NS_SUB_ERROR,
        EventKind::NamespaceSubscriptionError,
    ),
    (sys::MOQ_EVENT_NAMESPACE_FOUND, EventKind::NamespaceFound),
    (sys::MOQ_EVENT_NAMESPACE_GONE, EventKind::NamespaceGone),
    (sys::MOQ_EVENT_FETCH_REQUEST, EventKind::FetchRequest),
    (sys::MOQ_EVENT_FETCH_ERROR, EventKind::FetchError),
    (sys::MOQ_EVENT_FETCH_CANCELLED, EventKind::FetchCancelled),
    (sys::MOQ_EVENT_FETCH_OK, EventKind::FetchOk),
    (sys::MOQ_EVENT_FETCH_COMPLETE, EventKind::FetchComplete),
    (sys::MOQ_EVENT_FETCH_OBJECT, EventKind::FetchObject),
    (sys::MOQ_EVENT_FETCH_GAP, EventKind::FetchGap),
    (sys::MOQ_EVENT_PUBLISH_REQUEST, EventKind::PublishRequest),
    (sys::MOQ_EVENT_PUBLISH_OK, EventKind::PublishOk),
    (sys::MOQ_EVENT_PUBLISH_ERROR, EventKind::PublishError),
    (sys::MOQ_EVENT_PUBLISH_FINISHED, EventKind::PublishFinished),
    (
        sys::MOQ_EVENT_TRACK_STATUS_REQUEST,
        EventKind::TrackStatusRequest,
    ),
    (sys::MOQ_EVENT_TRACK_STATUS_OK, EventKind::TrackStatusOk),
    (
        sys::MOQ_EVENT_TRACK_STATUS_ERROR,
        EventKind::TrackStatusError,
    ),
    (sys::MOQ_EVENT_SUBSCRIBE_DONE, EventKind::SubscribeDone),
    (
        sys::MOQ_EVENT_PUBLISH_UNSUBSCRIBED,
        EventKind::PublishUnsubscribed,
    ),
    (sys::MOQ_EVENT_PUBLISH_UPDATED, EventKind::PublishUpdated),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST,
        EventKind::SubscribeTracksRequest,
    ),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_OK,
        EventKind::SubscribeTracksOk,
    ),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR,
        EventKind::SubscribeTracksError,
    ),
    (sys::MOQ_EVENT_PUBLISH_BLOCKED, EventKind::PublishBlocked),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED,
        EventKind::SubscribeTracksCancelled,
    ),
    (sys::MOQ_EVENT_REQUEST_REDIRECT, EventKind::RequestRedirect),
    (sys::MOQ_EVENT_REQUEST_GOAWAY, EventKind::RequestGoaway),
    (
        sys::MOQ_EVENT_SUBGROUP_FINISHED,
        EventKind::SubgroupFinished,
    ),
    (
        sys::MOQ_EVENT_SUBSCRIPTION_UPDATE_OK,
        EventKind::SubscriptionUpdateOk,
    ),
    (
        sys::MOQ_EVENT_PUBLICATION_UPDATE_OK,
        EventKind::PublicationUpdateOk,
    ),
    (sys::MOQ_EVENT_SUBGROUP_RESET, EventKind::SubgroupReset),
    (sys::MOQ_EVENT_FETCH_RESET, EventKind::FetchReset),
    (
        sys::MOQ_EVENT_TRACK_STATUS_TERMINATED,
        EventKind::TrackStatusTerminated,
    ),
    (sys::MOQ_EVENT_FILL_CREATED, EventKind::FillCreated),
    (sys::MOQ_EVENT_FILL_REQUEST, EventKind::FillRequest),
    (sys::MOQ_EVENT_FILL_OBJECT, EventKind::FillObject),
    (sys::MOQ_EVENT_FILL_GAP, EventKind::FillGap),
    (sys::MOQ_EVENT_FILL_TERMINAL, EventKind::FillTerminal),
    (
        sys::MOQ_EVENT_NAMESPACE_TERMINATED,
        EventKind::NamespaceTerminated,
    ),
    (sys::MOQ_EVENT_PUBLISH_STATE, EventKind::PublishState),
    (
        sys::MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED,
        EventKind::NamespaceSubscriptionTerminated,
    ),
    (
        sys::MOQ_EVENT_NAMESPACE_UPDATE_REQUEST,
        EventKind::NamespaceUpdateRequest,
    ),
    (
        sys::MOQ_EVENT_NS_SUB_UPDATE_REQUEST,
        EventKind::NamespaceSubscriptionUpdateRequest,
    ),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST,
        EventKind::SubscribeTracksUpdateRequest,
    ),
    (
        sys::MOQ_EVENT_NAMESPACE_UPDATE_RESULT,
        EventKind::NamespaceUpdateResult,
    ),
    (
        sys::MOQ_EVENT_NS_SUB_UPDATE_RESULT,
        EventKind::NamespaceSubscriptionUpdateResult,
    ),
    (
        sys::MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT,
        EventKind::SubscribeTracksUpdateResult,
    ),
    (
        sys::MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED,
        EventKind::OutgoingSubgroupTerminated,
    ),
];

impl EventKind {
    /// The kind of a native code.
    #[must_use]
    pub fn from_code(code: u32) -> EventKind {
        KINDS
            .iter()
            .find(|(native, _)| *native == code)
            .map_or(EventKind::Other(code), |(_, kind)| *kind)
    }

    /// The native code.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            EventKind::Other(code) => code,
            known => KINDS
                .iter()
                .find(|(_, kind)| *kind == known)
                .map_or(0, |(native, _)| *native),
        }
    }

    /// Every kind this crate knows, in the order of their codes.
    pub fn known() -> impl Iterator<Item = EventKind> {
        KINDS.iter().map(|(_, kind)| *kind)
    }
}

/// An identifying handle, selected by a request family (`moq_request_family_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequestHandle {
    /// A SUBSCRIBE.
    Subscription(Subscription),
    /// A FETCH.
    Fetch(Fetch),
    /// A TRACK_STATUS.
    TrackStatus(TrackStatusRequest),
    /// A namespace advertisement.
    Announcement(Announcement),
    /// A namespace subscription.
    NamespaceSubscription(NamespaceSubscription),
    /// A PUBLISH.
    Publication(Publication),
    /// A SUBSCRIBE_TRACKS.
    TrackSubscription(TrackSubscription),
    /// A family this crate does not know, with the handle's native value.
    Other {
        /// The native family.
        family: u32,
        /// The handle's native value.
        value: u64,
    },
}

fn request_handle(rd: &Reader<'_>, family: u32, value: u64) -> RequestHandle {
    match family {
        sys::MOQ_REQUEST_FAMILY_SUBSCRIBE => {
            RequestHandle::Subscription(rd.subscription(sys::moq_subscription_t { _opaque: value }))
        }
        sys::MOQ_REQUEST_FAMILY_FETCH => {
            RequestHandle::Fetch(rd.fetch(sys::moq_fetch_t { _opaque: value }))
        }
        sys::MOQ_REQUEST_FAMILY_TRACK_STATUS => RequestHandle::TrackStatus(
            rd.track_status(sys::moq_track_status_handle_t { _opaque: value }),
        ),
        sys::MOQ_REQUEST_FAMILY_ANNOUNCEMENT => {
            RequestHandle::Announcement(rd.announcement(sys::moq_announcement_t { _opaque: value }))
        }
        sys::MOQ_REQUEST_FAMILY_NS_SUB => RequestHandle::NamespaceSubscription(
            rd.ns_sub(sys::moq_ns_sub_handle_t { _opaque: value }),
        ),
        sys::MOQ_REQUEST_FAMILY_PUBLISH => {
            RequestHandle::Publication(rd.publication(sys::moq_publication_t { _opaque: value }))
        }
        sys::MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS => RequestHandle::TrackSubscription(
            rd.track_sub(sys::moq_track_sub_handle_t { _opaque: value }),
        ),
        family => RequestHandle::Other { family, value },
    }
}

/// A subscription filter in the form drafts 16 and 18 carry
/// (`moq_subscribe_filter_t`); draft 22 reports [`LocationFilter`]s.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SubscribeFilter {
    /// No filter.
    None,
    /// From the next group.
    NextGroup,
    /// From the largest object.
    LargestObject,
    /// From `start`, open-ended.
    AbsoluteStart {
        /// The first object.
        start: Location,
    },
    /// From `start` through `end_group`.
    AbsoluteRange {
        /// The first object.
        start: Location,
        /// The last group.
        end_group: u64,
    },
    /// A filter this crate does not know.
    Other(u32),
}

fn subscribe_filter(
    kind: u32,
    start_group: u64,
    start_object: u64,
    end_group: u64,
) -> SubscribeFilter {
    let start = view::location(start_group, start_object);
    match kind {
        sys::MOQ_SUBSCRIBE_FILTER_NONE => SubscribeFilter::None,
        sys::MOQ_SUBSCRIBE_FILTER_NEXT_GROUP => SubscribeFilter::NextGroup,
        sys::MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT => SubscribeFilter::LargestObject,
        sys::MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START => SubscribeFilter::AbsoluteStart { start },
        sys::MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE => {
            SubscribeFilter::AbsoluteRange { start, end_group }
        }
        other => SubscribeFilter::Other(other),
    }
}

fn optional<T>(present: bool, value: T) -> Option<T> {
    present.then_some(value)
}

// -- Records -----------------------------------------------------------

/// The session's setup completed (`MOQ_EVENT_SETUP_COMPLETE`).
#[derive(Debug)]
pub struct SetupComplete<'session> {
    /// This side's perspective.
    pub local_perspective: Perspective,
    /// The peer's perspective.
    pub peer_perspective: Perspective,
    /// The authorization tokens the peer's setup carried.
    pub tokens: Tokens<'session>,
    _record: Record<'session>,
}

/// The session closed (`MOQ_EVENT_SESSION_CLOSED`).
#[derive(Debug)]
pub struct SessionClosed<'session> {
    /// The session termination code.
    pub code: u64,
    /// The reason phrase.
    pub reason: &'session [u8],
    _record: Record<'session>,
}

/// What a peer's SUBSCRIBE selected beyond its legacy projections, draft 22
/// (`moq_subscription_admission_options_t`). Timeouts are exact milliseconds.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionOptions {
    /// The Location Filter, when the request carried one.
    pub location: Option<LocationFilter>,
    /// The request carried a fill request.
    pub fill_requested: bool,
    /// The object delivery timeout, when selected.
    pub object_delivery_timeout_ms: Option<u64>,
    /// The subgroup delivery timeout, when selected.
    pub subgroup_delivery_timeout_ms: Option<u64>,
    /// The rendezvous timeout, when selected and reported.
    pub rendezvous_timeout_ms: Option<u64>,
}

/// A peer asks to subscribe (`MOQ_EVENT_SUBSCRIBE_REQUEST`). Answer with
/// [`Session::accept_subscribe`](crate::Session::accept_subscribe) or
/// [`Session::reject_subscribe`](crate::Session::reject_subscribe).
#[derive(Debug)]
pub struct SubscribeRequest<'session> {
    /// The subscription.
    pub subscription: Subscription,
    /// The track namespace.
    pub namespace: NamespaceView<'session>,
    /// The track name.
    pub track_name: &'session [u8],
    /// The legacy projection of the filter (drafts 16 and 18).
    pub filter: SubscribeFilter,
    /// The subscriber priority.
    pub subscriber_priority: u8,
    /// The requested group order; `None` uses the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The forward state.
    pub forward: bool,
    /// The legacy delivery timeout projection, in microseconds.
    pub delivery_timeout_us: u64,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    /// A new-group request (0 is "no group info"), when carried.
    pub new_group_request: Option<u64>,
    /// The lossless parameters, draft 22.
    pub options: Option<AdmissionOptions>,
    _record: Record<'session>,
}

/// A subscription this side requested was accepted (`MOQ_EVENT_SUBSCRIBE_OK`).
#[derive(Debug)]
pub struct SubscribeOk<'session> {
    /// The subscription.
    pub subscription: Subscription,
    /// The track alias.
    pub track_alias: u64,
    /// The largest object, when the publisher reported one.
    pub largest: Option<Location>,
    /// EXPIRES, when carried.
    pub expires_ms: Option<u64>,
    /// The Track Properties, as received.
    pub track_properties: &'session [u8],
    /// The track supports dynamically started groups.
    pub dynamic_groups: bool,
    /// The publisher's preferred group order, when reported.
    pub publisher_group_order: Option<Option<GroupOrder>>,
    _record: Record<'session>,
}

/// A request this side made was refused, or (for namespace cancellation)
/// withdrawn by its responder.
#[derive(Debug)]
pub struct RequestRefused<'session, H> {
    /// The request.
    pub handle: H,
    /// The request error code.
    pub error_code: u64,
    /// The retry interval, when the refusal allows a retry.
    pub retry_after_ms: Option<u64>,
    /// The reason phrase.
    pub reason: &'session [u8],
    _record: Record<'session>,
}

/// Request capacity grew (`MOQ_EVENT_REQUEST_READY`).
#[derive(Debug)]
pub struct RequestReady<'session> {
    /// Requests this side may now make.
    pub available_requests: u64,
    _record: Record<'session>,
}

/// One whole object (`MOQ_EVENT_OBJECT_RECEIVED`).
#[derive(Debug)]
pub struct ObjectReceived<'session> {
    /// The subscription it arrived for, if a SUBSCRIBE began it.
    pub subscription: Option<Subscription>,
    /// The publication it arrived for, if a PUBLISH began it.
    pub publication: Option<Publication>,
    /// The group.
    pub group_id: u64,
    /// The subgroup.
    pub subgroup_id: u64,
    /// The object.
    pub object_id: u64,
    /// The publisher priority.
    pub publisher_priority: u8,
    /// The object status.
    pub status: ObjectStatus,
    /// The subgroup header's END_OF_GROUP bit.
    pub end_of_group: bool,
    /// Delivered by datagram.
    pub datagram: bool,
    /// The first object ever published in its subgroup.
    pub first_object: bool,
    /// The payload; `None` for a status object.
    pub payload: Option<Payload<'session>>,
    /// The object properties, if any.
    pub properties: Option<Payload<'session>>,
    /// The Prior Group ID Gap, when carried.
    pub prior_group_gap: Option<u64>,
    /// The Prior Object ID Gap, when carried.
    pub prior_object_gap: Option<u64>,
    /// The subgroup header set PROPERTIES.
    pub subgroup_properties: bool,
    _record: Record<'session>,
}

/// A request identified by its handle alone.
#[derive(Debug)]
pub struct HandleEvent<'session, H> {
    /// The request.
    pub handle: H,
    _record: Record<'session>,
}

/// A peer advertised a namespace (`MOQ_EVENT_NAMESPACE_PUBLISHED`).
#[derive(Debug)]
pub struct NamespacePublished<'session> {
    /// The advertisement.
    pub announcement: Announcement,
    /// The namespace.
    pub namespace: NamespaceView<'session>,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    /// The cluster metadata (cluster draft-01, draft 22), when reported.
    pub cluster: Option<ClusterView<'session>>,
    _record: Record<'session>,
}

/// The peer sent a session GOAWAY (`MOQ_EVENT_GOAWAY`).
#[derive(Debug)]
pub struct Goaway<'session> {
    /// The new session URI; empty reuses the current one.
    pub new_session_uri: &'session [u8],
    /// The sender's timeout, drafts 18 and 22 (zero: migrate at once).
    pub timeout_ms: Option<u64>,
    _record: Record<'session>,
}

/// A request ended without a message of its own, or by a stream signal.
#[derive(Debug)]
pub struct RequestEnded<'session, H> {
    /// The request; identifier only from here on.
    pub handle: H,
    /// How it ended, when reported.
    pub cause: Option<TerminalCause>,
    /// The stream error code of a stream-signal cause, when reported.
    pub stream_error_code: Option<u64>,
    _record: Record<'session>,
}

/// A replacement filter carried on an update (drafts 16 and 18).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdatedFilter {
    /// The filter.
    pub filter: SubscribeFilter,
}

/// A subscriber's update of a subscription this side publishes
/// (`MOQ_EVENT_SUBSCRIBE_UPDATED`, `MOQ_EVENT_PUBLISH_UPDATED`).
#[derive(Debug)]
pub struct TrackUpdated<'session, H> {
    /// The subscription or publication.
    pub handle: H,
    /// The new subscriber priority, when carried.
    pub subscriber_priority: Option<u8>,
    /// The new forward state, when carried.
    pub forward: Option<bool>,
    /// The legacy delivery timeout, when carried, in microseconds.
    pub delivery_timeout_us: Option<u64>,
    /// The authorization tokens (of the newest update covered).
    pub tokens: Tokens<'session>,
    /// A new-group request, when carried.
    pub new_group_request: Option<u64>,
    /// A replacement legacy filter, when carried and reported.
    pub filter: Option<UpdatedFilter>,
    /// The application owes the answer, when reported.
    pub answer_owed: Option<bool>,
    /// The update carries a fill request, when reported.
    pub fill_requested: Option<bool>,
    /// How many updates this event covers, when reported.
    pub update_count: Option<u32>,
    /// The Request ID the answer names, when reported.
    pub update_request_id: Option<u64>,
    /// The lossless selected parameters, on typed-options profiles (draft
    /// 22); the legacy projections above are then inactive.
    pub options: Option<UpdateOptions>,
    _record: Record<'session>,
}

/// The selected parameters of an update (`moq_subscription_update_options_t`):
/// the update's observation, not the later effective state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateOptions {
    /// The Request ID of the request the update belongs to.
    pub initiating_request_id: u64,
    /// The replacement Location Filter, when selected.
    pub location: Option<LocationFilter>,
    /// The object delivery timeout, when selected, exact milliseconds.
    pub object_delivery_timeout_ms: Option<u64>,
    /// The subgroup delivery timeout, when selected, exact milliseconds.
    pub subgroup_delivery_timeout_ms: Option<u64>,
    /// How the session processed it, when reported.
    pub resolution: Option<UpdateResolution>,
}

/// How a draft-22 update was processed (`moq_update_resolution_t`): the
/// Largest the response, parent filter and fill used, and the absolute
/// window a selected replacement filter resolved to.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UpdateResolution {
    /// The update's own Request ID.
    pub update_request_id: u64,
    /// The Largest Object used, when there was one.
    pub largest: Option<Location>,
    /// The replacement filter, resolved to NONE or an absolute window.
    pub location_replacement: Option<LocationFilter>,
}

impl UpdateResolution {
    pub(super) fn from_raw(raw: &sys::moq_update_resolution_t) -> Result<UpdateResolution> {
        super::raw::floor(raw.struct_size, sys::MOQ_UPDATE_RESOLUTION_V0_SIZE as usize)?;
        let replaced = raw.flags & sys::MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT != 0;
        Ok(UpdateResolution {
            update_request_id: raw.update_request_id,
            largest: optional(
                raw.flags & sys::MOQ_UPDATE_RESOLUTION_HAS_LARGEST != 0,
                view::location(raw.largest_group, raw.largest_object),
            ),
            location_replacement: if replaced {
                Some(view::location_filter(&raw.location)?)
            } else {
                None
            },
        })
    }
}

/// The frozen extent of `moq_subscription_update_options_t` before its
/// resolution was appended: through `subgroup_delivery_timeout_ms`.
fn update_options(o: &sys::moq_subscription_update_options_t) -> Result<UpdateOptions> {
    super::raw::floor(
        o.struct_size,
        field_end!(
            sys::moq_subscription_update_options_t,
            subgroup_delivery_timeout_ms,
            u64
        ),
    )?;
    let resolution = if super::raw::covers(
        o.struct_size,
        field_end!(
            sys::moq_subscription_update_options_t,
            resolution,
            sys::moq_update_resolution_t
        ),
    ) {
        Some(UpdateResolution::from_raw(&o.resolution)?)
    } else {
        None
    };
    Ok(UpdateOptions {
        initiating_request_id: o.initiating_request_id,
        location: if o.flags & sys::MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION != 0 {
            Some(view::location_filter(&o.location)?)
        } else {
            None
        },
        object_delivery_timeout_ms: optional(
            o.flags & sys::MOQ_SUBSCRIPTION_UPDATE_OPTIONS_OBJECT_DELIVERY_TIMEOUT != 0,
            o.object_delivery_timeout_ms,
        ),
        subgroup_delivery_timeout_ms: optional(
            o.flags & sys::MOQ_SUBSCRIPTION_UPDATE_OPTIONS_SUBGROUP_DELIVERY_TIMEOUT != 0,
            o.subgroup_delivery_timeout_ms,
        ),
        resolution,
    })
}

/// How a streamed object's chunk ends (`moq_object_terminal_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectTerminal {
    /// The object completed.
    Normal,
    /// The peer reset the stream while the object was in flight, with its
    /// code.
    Reset {
        /// The peer's RESET_STREAM code.
        error_code: u64,
    },
    /// The stream was stopped.
    Stop,
    /// A terminal this crate does not know.
    Other(u32),
}

/// What the first chunk of a streamed object carries.
#[derive(Debug)]
pub struct ChunkStart<'session> {
    /// The publisher priority.
    pub publisher_priority: u8,
    /// The object status.
    pub status: ObjectStatus,
    /// The subgroup header's END_OF_GROUP bit.
    pub end_of_group: bool,
    /// The first object ever published in its subgroup.
    pub first_object: bool,
    /// The declared payload length.
    pub payload_length: u64,
    /// The object properties, if any.
    pub properties: Option<Payload<'session>>,
    /// The Prior Group ID Gap, when carried.
    pub prior_group_gap: Option<u64>,
    /// The Prior Object ID Gap, when carried.
    pub prior_object_gap: Option<u64>,
    /// The subgroup header set PROPERTIES.
    pub subgroup_properties: bool,
}

/// One chunk of a streamed object (`MOQ_EVENT_OBJECT_CHUNK`).
#[derive(Debug)]
pub struct ObjectChunk<'session> {
    /// The subscription, if a SUBSCRIBE began it.
    pub subscription: Option<Subscription>,
    /// The publication, if a PUBLISH began it.
    pub publication: Option<Publication>,
    /// The group.
    pub group_id: u64,
    /// The subgroup.
    pub subgroup_id: u64,
    /// The object.
    pub object_id: u64,
    /// The chunk's bytes, if any.
    pub chunk: Option<Payload<'session>>,
    /// Present on the object's first chunk.
    pub start: Option<ChunkStart<'session>>,
    /// Present on its last chunk.
    pub end: Option<ObjectTerminal>,
    _record: Record<'session>,
}

/// A namespace interest (`moq_namespace_interest_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NamespaceInterest {
    /// Publisher state.
    PublisherState,
    /// Namespace state.
    NamespaceState,
    /// Both.
    Both,
    /// An interest this crate does not know.
    Other(u32),
}

impl NamespaceInterest {
    fn from_raw(raw: u32) -> NamespaceInterest {
        match raw {
            sys::MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE => NamespaceInterest::PublisherState,
            sys::MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE => NamespaceInterest::NamespaceState,
            sys::MOQ_NAMESPACE_INTEREST_BOTH => NamespaceInterest::Both,
            other => NamespaceInterest::Other(other),
        }
    }

    pub(super) fn raw(self) -> u32 {
        match self {
            NamespaceInterest::PublisherState => sys::MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE,
            NamespaceInterest::NamespaceState => sys::MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE,
            NamespaceInterest::Both => sys::MOQ_NAMESPACE_INTEREST_BOTH,
            NamespaceInterest::Other(code) => code,
        }
    }
}

/// A peer subscribes to namespaces under a prefix (`MOQ_EVENT_NS_SUB_REQUEST`).
#[derive(Debug)]
pub struct NamespaceSubscriptionRequest<'session> {
    /// The namespace subscription.
    pub subscription: NamespaceSubscription,
    /// The prefix.
    pub prefix: NamespaceView<'session>,
    /// What the subscriber is interested in.
    pub interest: NamespaceInterest,
    /// The forward state.
    pub forward: bool,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    _record: Record<'session>,
}

/// A namespace appeared or went under a namespace subscription
/// (`MOQ_EVENT_NAMESPACE_FOUND`, `MOQ_EVENT_NAMESPACE_GONE`).
#[derive(Debug)]
pub struct NamespaceSuffix<'session> {
    /// The namespace subscription.
    pub subscription: NamespaceSubscription,
    /// The suffix under the subscription's prefix.
    pub suffix: NamespaceView<'session>,
    /// The cluster metadata of a found namespace, when reported; a repeated
    /// FOUND is a metadata update.
    pub cluster: Option<ClusterView<'session>>,
    _record: Record<'session>,
}

/// The joined owner of a joining FETCH.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Joining {
    /// A SUBSCRIBE-initiated subscription.
    Subscription(Subscription),
    /// A PUBLISH-initiated one.
    Publication(Publication),
}

/// What a FETCH states beyond its range fields, on a profile whose FETCH
/// carries a Location Filter (`moq_fetch_request_options_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchRequestOptions {
    /// The semantic Location Filter (`Unconstrained` for none).
    pub location: LocationFilter,
    /// FILL_TIMEOUT, when carried.
    pub fill_timeout_ms: Option<u64>,
    /// INCLUDE_PROPERTIES, the effective value.
    pub include_properties: bool,
    /// The range is left to the application to resolve, when reported.
    pub range_unresolved: Option<bool>,
}

/// A peer asks to fetch (`MOQ_EVENT_FETCH_REQUEST`). Answer with
/// [`Session::accept_fetch`](crate::Session::accept_fetch) or
/// [`Session::reject_fetch`](crate::Session::reject_fetch).
#[derive(Debug)]
pub struct FetchRequest<'session> {
    /// The fetch.
    pub fetch: Fetch,
    /// The track namespace (empty for a joining fetch).
    pub namespace: NamespaceView<'session>,
    /// The track name (empty for a joining fetch).
    pub track_name: &'session [u8],
    /// The joined owner of a joining fetch.
    pub joining: Option<Joining>,
    /// The first object.
    pub start: Location,
    /// The end group.
    pub end_group: u64,
    /// The end object, exclusive; zero is the whole end group.
    pub end_object: u64,
    /// The subscriber priority.
    pub subscriber_priority: u8,
    /// The requested group order; `None` uses the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    /// The Location Filter form and options, where the profile carries them.
    pub options: Option<FetchRequestOptions>,
    _record: Record<'session>,
}

/// A FETCH this side requested was accepted (`MOQ_EVENT_FETCH_OK`).
#[derive(Debug)]
pub struct FetchOk<'session> {
    /// The fetch.
    pub fetch: Fetch,
    /// The range reaches the end of the track.
    pub end_of_track: bool,
    /// The end group served.
    pub end_group: u64,
    /// The end object served, exclusive; zero is the whole end group.
    pub end_object: u64,
    /// The Track Properties, as received.
    pub track_properties: &'session [u8],
    _record: Record<'session>,
}

/// One object of a FETCH (`MOQ_EVENT_FETCH_OBJECT`).
#[derive(Debug)]
pub struct FetchObject<'session> {
    /// The fetch.
    pub fetch: Fetch,
    /// The group.
    pub group_id: u64,
    /// The subgroup (zero for a datagram-preference object).
    pub subgroup_id: u64,
    /// The object.
    pub object_id: u64,
    /// The publisher priority.
    pub publisher_priority: u8,
    /// The original object's forwarding preference was Datagram.
    pub datagram: bool,
    /// The payload; `None` for zero length.
    pub payload: Option<Payload<'session>>,
    /// The object properties, if any.
    pub properties: Option<Payload<'session>>,
    /// The Prior Group ID Gap, when carried and reported.
    pub prior_group_gap: Option<u64>,
    /// The Prior Object ID Gap, when carried and reported.
    pub prior_object_gap: Option<u64>,
    _record: Record<'session>,
}

/// A range kind of a FETCH or fill gap (`moq_fetch_range_kind_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FetchRange {
    /// The objects do not exist.
    NonExistent,
    /// Unknown.
    Unknown,
    /// Timed out (draft 22).
    TimedOut,
    /// A kind this crate does not know.
    Other(u32),
}

impl FetchRange {
    fn from_raw(raw: u32) -> FetchRange {
        match raw {
            sys::MOQ_FETCH_RANGE_NON_EXISTENT => FetchRange::NonExistent,
            sys::MOQ_FETCH_RANGE_UNKNOWN => FetchRange::Unknown,
            sys::MOQ_FETCH_RANGE_TIMED_OUT => FetchRange::TimedOut,
            other => FetchRange::Other(other),
        }
    }

    pub(super) fn raw(self) -> u32 {
        match self {
            FetchRange::NonExistent => sys::MOQ_FETCH_RANGE_NON_EXISTENT,
            FetchRange::Unknown => sys::MOQ_FETCH_RANGE_UNKNOWN,
            FetchRange::TimedOut => sys::MOQ_FETCH_RANGE_TIMED_OUT,
            FetchRange::Other(code) => code,
        }
    }
}

/// An end-of-range marker of a FETCH (`MOQ_EVENT_FETCH_GAP`).
#[derive(Debug)]
pub struct FetchGap<'session> {
    /// The fetch.
    pub fetch: Fetch,
    /// What the range is.
    pub range: FetchRange,
    /// The marker's inclusive end location.
    pub end: Location,
    _record: Record<'session>,
}

/// A peer publishes a track to this side (`MOQ_EVENT_PUBLISH_REQUEST`).
#[derive(Debug)]
pub struct PublishRequest<'session> {
    /// The publication.
    pub publication: Publication,
    /// The track namespace.
    pub namespace: NamespaceView<'session>,
    /// The track name.
    pub track_name: &'session [u8],
    /// The track alias.
    pub track_alias: u64,
    /// The forward state.
    pub forward: bool,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    /// The Track Properties, as received.
    pub track_properties: &'session [u8],
    /// The track supports dynamically started groups.
    pub dynamic_groups: bool,
    /// The publisher's largest object, when advertised.
    pub largest: Option<Location>,
    /// EXPIRES, when advertised.
    pub expires_ms: Option<u64>,
    _record: Record<'session>,
}

/// A PUBLISH this side made was accepted (`MOQ_EVENT_PUBLISH_OK`).
#[derive(Debug)]
pub struct PublishOk<'session> {
    /// The publication.
    pub publication: Publication,
    /// The subscriber allows sending.
    pub send_allowed: bool,
    /// The subscriber priority.
    pub subscriber_priority: u8,
    /// The subscriber's group order; `None` uses the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The delivery timeout, when carried.
    pub delivery_timeout_ms: Option<u64>,
    /// EXPIRES, when carried.
    pub expires_ms: Option<u64>,
    /// A new-group request, when carried.
    pub new_group_request: Option<u64>,
    /// The subscriber's filter, raw, when carried and reported.
    pub filter: Option<SubscribeFilter>,
    _record: Record<'session>,
}

/// The publisher's terminal for a track (`MOQ_EVENT_SUBSCRIBE_DONE`,
/// `MOQ_EVENT_PUBLISH_FINISHED`).
#[derive(Debug)]
pub struct TrackDone<'session, H> {
    /// The subscription or publication; identifier only from here on.
    pub handle: H,
    /// The status code (meaningless when a stream signal ended it).
    pub status_code: u64,
    /// The data streams the publisher opened.
    pub stream_count: u64,
    /// The reason phrase.
    pub reason: &'session [u8],
    /// The stream signal that ended a publication, when reported.
    pub cause: Option<TerminalCause>,
    /// Its stream error code, when reported.
    pub stream_error_code: Option<u64>,
    _record: Record<'session>,
}

/// A peer asks for a track's status (`MOQ_EVENT_TRACK_STATUS_REQUEST`).
#[derive(Debug)]
pub struct TrackStatusRequestEvent<'session> {
    /// The request.
    pub request: TrackStatusRequest,
    /// The track namespace.
    pub namespace: NamespaceView<'session>,
    /// The track name.
    pub track_name: &'session [u8],
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    _record: Record<'session>,
}

/// A track status this side asked for (`MOQ_EVENT_TRACK_STATUS_OK`).
#[derive(Debug)]
pub struct TrackStatusOk<'session> {
    /// The request.
    pub request: TrackStatusRequest,
    /// The largest object, when reported.
    pub largest: Option<Location>,
    /// EXPIRES, when carried.
    pub expires_ms: Option<u64>,
    /// The Track Properties, as received.
    pub track_properties: &'session [u8],
    _record: Record<'session>,
}

/// The draft-22 SUBSCRIBE_TRACKS options (`moq_subscribe_tracks_options_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscribeTracksOptions {
    /// The effective group order; `None` is the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The request carried GROUP_ORDER.
    pub group_order_selected: bool,
    /// The effective INCLUDE_PROPERTIES.
    pub include_properties: bool,
    /// The request carried INCLUDE_PROPERTIES.
    pub include_properties_selected: bool,
}

/// A peer subscribes to the tracks under a prefix
/// (`MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST`).
#[derive(Debug)]
pub struct SubscribeTracksRequest<'session> {
    /// The request.
    pub request: TrackSubscription,
    /// The prefix.
    pub prefix: NamespaceView<'session>,
    /// The forward state.
    pub forward: bool,
    /// The authorization tokens.
    pub tokens: Tokens<'session>,
    /// The draft-22 options.
    pub options: Option<SubscribeTracksOptions>,
    _record: Record<'session>,
}

/// The publisher cannot PUBLISH a matching track (`MOQ_EVENT_PUBLISH_BLOCKED`).
#[derive(Debug)]
pub struct PublishBlocked<'session> {
    /// The request.
    pub request: TrackSubscription,
    /// The track's namespace suffix.
    pub suffix: NamespaceView<'session>,
    /// The track name.
    pub track_name: &'session [u8],
    _record: Record<'session>,
}

/// A request was refused with REDIRECT (`MOQ_EVENT_REQUEST_REDIRECT`).
#[derive(Debug)]
pub struct RequestRedirect<'session> {
    /// The request.
    pub request: RequestHandle,
    /// The request error code (REDIRECT).
    pub error_code: u64,
    /// The retry interval, when the refusal allows a retry.
    pub retry_after_ms: Option<u64>,
    /// Where to re-issue; empty is the current session.
    pub connect_uri: &'session [u8],
    /// The redirect's namespace, as received.
    pub namespace: NamespaceView<'session>,
    /// The redirect's track name, as received.
    pub track_name: &'session [u8],
    /// The reason phrase.
    pub reason: &'session [u8],
    _record: Record<'session>,
}

impl RequestRedirect<'_> {
    /// An owned copy of the target, exactly as received. On draft 18 an
    /// empty namespace and name mean the original request's; on draft 22
    /// they are the literal target.
    ///
    /// # Errors
    /// As for [`NamespaceView::to_namespace`].
    pub fn target(&self) -> Result<RedirectTarget> {
        Ok(RedirectTarget {
            connect_uri: self.connect_uri.to_vec(),
            namespace: self.namespace.to_namespace()?,
            track_name: self.track_name.to_vec(),
        })
    }
}

/// A peer sent GOAWAY on one request's stream (`MOQ_EVENT_REQUEST_GOAWAY`).
#[derive(Debug)]
pub struct RequestGoaway<'session> {
    /// The request.
    pub request: RequestHandle,
    /// Where to re-issue; empty is the current session.
    pub new_session_uri: &'session [u8],
    /// The sender's timeout hint (zero: none).
    pub timeout_ms: u64,
    _record: Record<'session>,
}

/// A subgroup data stream ended (`MOQ_EVENT_SUBGROUP_FINISHED` with its
/// FIN, `MOQ_EVENT_SUBGROUP_RESET` with a reset).
#[derive(Debug)]
pub struct SubgroupEnded<'session> {
    /// The subscription, if a SUBSCRIBE began it.
    pub subscription: Option<Subscription>,
    /// The publication, if a PUBLISH began it.
    pub publication: Option<Publication>,
    /// The group.
    pub group_id: u64,
    /// The subgroup.
    pub subgroup_id: u64,
    /// The subgroup header's END_OF_GROUP bit.
    pub end_of_group: bool,
    /// The reset's code (resets only).
    pub error_code: Option<u64>,
    /// The last object the stream carried (completely, for a reset): outer
    /// `None` when not reported, inner `None` when there was none.
    pub last_object_id: Option<Option<u64>>,
    _record: Record<'session>,
}

/// The peer accepted an update this side sent
/// (`MOQ_EVENT_SUBSCRIPTION_UPDATE_OK`, `MOQ_EVENT_PUBLICATION_UPDATE_OK`).
#[derive(Debug)]
pub struct UpdateAccepted<'session, H> {
    /// The subscription or publication.
    pub handle: H,
    /// The largest object, when the peer carried one.
    pub largest: Option<Location>,
    /// EXPIRES, when carried.
    pub expires_ms: Option<u64>,
    _record: Record<'session>,
}

/// A fetch's stream ended abruptly (`MOQ_EVENT_FETCH_RESET`).
#[derive(Debug)]
pub struct FetchReset<'session> {
    /// The fetch; retired.
    pub fetch: Fetch,
    /// The peer's stream error code, or the code this side sent for a local
    /// cancel.
    pub error_code: u64,
    /// The response data stream ended (else the request stream).
    pub data_stream: bool,
    /// The request stream got STOP_SENDING rather than a reset.
    pub stop_sending: bool,
    /// The cause, when reported.
    pub cause: Option<TerminalCause>,
    _record: Record<'session>,
}

/// A request ended abnormally by a stream signal (track status, namespace
/// advertisement, namespace subscription terminals). `C` is the cause set.
#[derive(Debug)]
pub struct StreamTerminated<'session, H, C> {
    /// The request; identifier only from here on.
    pub handle: H,
    /// The cause.
    pub cause: C,
    /// This side's role in the request.
    pub local_role: RequestRole,
    /// The peer's exact stream error for reset and stop causes; zero
    /// otherwise.
    pub stream_error_code: u64,
    _record: Record<'session>,
}

/// Why a TRACK_STATUS ended abnormally (`moq_track_status_terminal_cause_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrackStatusCause {
    /// The peer reset the stream.
    PeerResetStream,
    /// The peer sent STOP_SENDING.
    PeerStopSending,
    /// The peer ended the stream before a response.
    MissingResponse,
    /// The peer ended the stream inside a response.
    TruncatedResponse,
    /// A cause this crate does not know.
    Other(u32),
}

/// Why a draft-22 namespace advertisement ended (`moq_namespace_terminal_cause_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NamespaceTerminalCause {
    /// The peer's lawful FIN.
    PeerFinComplete,
    /// The peer reset the stream.
    PeerResetStream,
    /// The peer sent STOP_SENDING.
    PeerStopSending,
    /// The peer ended the stream before a response.
    MissingResponse,
    /// The peer ended the stream inside a message.
    TruncatedMessage,
    /// This side could not represent a required observation.
    LocalCapacity,
    /// A cause this crate does not know.
    Other(u32),
}

/// The parent of a fill, and the request that created it.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FillParent {
    /// The fill.
    pub fill: Fill,
    /// The parent subscription, if a SUBSCRIBE began it.
    pub subscription: Option<Subscription>,
    /// The parent publication, if a PUBLISH began it.
    pub publication: Option<Publication>,
    /// The Request ID that initiated the fill.
    pub initiating_request_id: u64,
}

/// A fill this side requested exists (`MOQ_EVENT_FILL_CREATED`).
#[derive(Debug)]
pub struct FillCreated<'session> {
    /// The fill and its parent.
    pub fill: FillParent,
    _record: Record<'session>,
}

/// This side owes a fill (`MOQ_EVENT_FILL_REQUEST`).
#[derive(Debug)]
pub struct FillRequest<'session> {
    /// The fill and its parent.
    pub fill: FillParent,
    /// The first object, inclusive.
    pub start: Location,
    /// The last object, inclusive.
    pub end: Location,
    /// When the session processed the request.
    pub processed_at: Instant,
    /// The fill timeout, when carried.
    pub fill_timeout_ms: Option<u64>,
    /// The group order; `None` is the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The subscriber priority.
    pub subscriber_priority: u8,
    _record: Record<'session>,
}

/// One fill-delivered object (`MOQ_EVENT_FILL_OBJECT`).
#[derive(Debug)]
pub struct FillObject<'session> {
    /// The fill and its parent.
    pub fill: FillParent,
    /// The group.
    pub group_id: u64,
    /// The subgroup.
    pub subgroup_id: u64,
    /// The object.
    pub object_id: u64,
    /// The publisher priority.
    pub publisher_priority: u8,
    /// Datagram forwarding preference.
    pub datagram: bool,
    /// The payload, if any.
    pub payload: Option<Payload<'session>>,
    /// The object properties, if any.
    pub properties: Option<Payload<'session>>,
    /// The Prior Group ID Gap, when carried and reported.
    pub prior_group_gap: Option<u64>,
    /// The Prior Object ID Gap, when carried and reported.
    pub prior_object_gap: Option<u64>,
    _record: Record<'session>,
}

/// A fill-delivered gap (`MOQ_EVENT_FILL_GAP`).
#[derive(Debug)]
pub struct FillGap<'session> {
    /// The fill and its parent.
    pub fill: FillParent,
    /// The marker's location.
    pub end: Location,
    /// What the range is.
    pub range: FetchRange,
    _record: Record<'session>,
}

/// Why a fill ended (`MOQ_FILL_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FillEnd {
    /// Completed.
    Complete,
    /// No stream was opened.
    NoStream,
    /// This side cancelled it.
    LocalCancelled,
    /// The peer cancelled or reset it.
    PeerCancelledOrReset,
    /// This side aborted it.
    LocalAborted,
    /// The parent was rejected.
    ParentRejected,
    /// The parent ended.
    ParentTerminated,
    /// A reason this crate does not know.
    Other(u32),
}

/// The error a fill ended with (`MOQ_FILL_ERROR_*` and its code). Zero is a
/// valid code in every domain but `None`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FillError {
    /// No error.
    None,
    /// A stream error code.
    Stream(u64),
    /// The parent request's error code.
    ParentRequest(u64),
    /// The parent's done status.
    ParentDone(u64),
    /// A domain this crate does not know, with its code.
    Other {
        /// The domain.
        domain: u32,
        /// The code.
        code: u64,
    },
}

/// A fill ended, exactly once (`MOQ_EVENT_FILL_TERMINAL`).
#[derive(Debug)]
pub struct FillTerminal<'session> {
    /// The fill and its parent.
    pub fill: FillParent,
    /// Why.
    pub reason: FillEnd,
    /// With which error.
    pub error: FillError,
    _record: Record<'session>,
}

/// The publisher's state notification (`MOQ_EVENT_PUBLISH_STATE`).
#[derive(Debug)]
pub struct PublishState<'session> {
    /// The subscription or publication it is about.
    pub request: RequestHandle,
    /// The forward state, when stated.
    pub forward: Option<bool>,
    /// The largest object, when stated.
    pub largest: Option<Location>,
    /// The filter, when stated.
    pub location: Option<LocationFilter>,
    _record: Record<'session>,
}

/// A draft-22 namespace update as observed (`moq_namespace_update_record_t`).
#[non_exhaustive]
#[derive(Debug)]
pub struct NamespaceUpdate<'session> {
    /// The update's own Request ID.
    pub update_request_id: u64,
    /// The selected prefix.
    pub prefix: Option<NamespaceView<'session>>,
    /// The selected forward state.
    pub forward: Option<bool>,
    /// The update's tokens resolved to at least one token.
    pub authorization: bool,
    /// The tokens, in wire order.
    pub tokens: Tokens<'session>,
    /// The selected partial cluster update, when reported.
    pub cluster: Option<ClusterView<'session>>,
}

/// The peer updated a namespace-scoped request this side answers
/// (`MOQ_EVENT_*_UPDATE_REQUEST`).
#[derive(Debug)]
pub struct NamespaceUpdateRequested<'session, H> {
    /// The request.
    pub handle: H,
    /// The update's Request ID.
    pub update_request_id: u64,
    /// The update.
    pub update: Option<NamespaceUpdate<'session>>,
    _record: Record<'session>,
}

/// How a namespace update concluded (`moq_namespace_update_outcome_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UpdateOutcome {
    /// Accepted.
    Ok,
    /// The responder refused it.
    Error,
    /// This side refused a peer update on its own.
    Refused,
    /// An outcome this crate does not know.
    Other(u32),
}

/// A namespace update concluded (`MOQ_EVENT_*_UPDATE_RESULT`).
#[derive(Debug)]
pub struct NamespaceUpdateConcluded<'session, H> {
    /// The request.
    pub handle: H,
    /// The update's Request ID.
    pub update_request_id: u64,
    /// How it concluded.
    pub outcome: UpdateOutcome,
    /// This side's role.
    pub local_role: RequestRole,
    /// The request error code (zero for OK).
    pub error_code: u64,
    /// The reason phrase.
    pub reason: &'session [u8],
    /// EXPIRES the acceptance carried.
    pub expires_ms: Option<u64>,
    /// The accepted partial cluster update (announcements only), when
    /// reported.
    pub cluster: Option<ClusterView<'session>>,
    _record: Record<'session>,
}

/// Why an outgoing subgroup was reset (`moq_outgoing_subgroup_terminal_cause_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SubgroupTerminalCause {
    /// No terminal event belongs to the reset.
    None,
    /// The peer sent STOP_SENDING.
    PeerStopSending,
    /// The delivery timeout expired.
    DeliveryTimeout,
    /// A cause this crate does not know.
    Other(u32),
}

impl SubgroupTerminalCause {
    pub(super) fn from_raw(raw: u32) -> SubgroupTerminalCause {
        match raw {
            sys::MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE => SubgroupTerminalCause::None,
            sys::MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING => {
                SubgroupTerminalCause::PeerStopSending
            }
            sys::MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT => {
                SubgroupTerminalCause::DeliveryTimeout
            }
            other => SubgroupTerminalCause::Other(other),
        }
    }
}

/// An outgoing subgroup was reset after a peer STOP or delivery expiry
/// (`MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED`). Local commitment, not peer
/// acknowledgement.
#[derive(Debug)]
pub struct OutgoingSubgroupTerminated<'session> {
    /// The subgroup; identifier only.
    pub subgroup: Subgroup,
    /// The subscription, if a SUBSCRIBE began it.
    pub subscription: Option<Subscription>,
    /// The publication, if a PUBLISH began it.
    pub publication: Option<Publication>,
    /// The group.
    pub group_id: u64,
    /// The subgroup ID.
    pub subgroup_id: u64,
    /// The stream.
    pub stream: StreamRef,
    /// The exact reset code; zero is valid.
    pub error_code: u64,
    /// The first recognized cause.
    pub cause: SubgroupTerminalCause,
    _record: Record<'session>,
}

/// An event of a kind this crate does not know, kept rather than dropped.
/// Its resources were released by the native cleanup.
#[derive(Debug)]
pub struct UnknownEvent<'session> {
    /// The native kind.
    pub kind: u32,
    /// The detail bytes the library reported.
    pub detail_size: u32,
    _record: Record<'session>,
}

/// One polled event. It borrows the session exclusively until it is
/// dropped; it is neither `Send` nor `Sync`.
#[non_exhaustive]
#[derive(Debug)]
pub enum Event<'session> {
    /// `MOQ_EVENT_SETUP_COMPLETE`.
    SetupComplete(SetupComplete<'session>),
    /// `MOQ_EVENT_SESSION_CLOSED`.
    SessionClosed(SessionClosed<'session>),
    /// `MOQ_EVENT_SUBSCRIBE_REQUEST`.
    SubscribeRequest(SubscribeRequest<'session>),
    /// `MOQ_EVENT_SUBSCRIBE_OK`.
    SubscribeOk(SubscribeOk<'session>),
    /// `MOQ_EVENT_SUBSCRIBE_ERROR`.
    SubscribeError(RequestRefused<'session, Subscription>),
    /// `MOQ_EVENT_REQUEST_READY`.
    RequestReady(RequestReady<'session>),
    /// `MOQ_EVENT_OBJECT_RECEIVED`.
    ObjectReceived(ObjectReceived<'session>),
    /// `MOQ_EVENT_NAMESPACE_PUBLISHED`.
    NamespacePublished(NamespacePublished<'session>),
    /// `MOQ_EVENT_NAMESPACE_DONE`.
    NamespaceDone(HandleEvent<'session, Announcement>),
    /// `MOQ_EVENT_NAMESPACE_REJECTED`.
    NamespaceRejected(RequestRefused<'session, Announcement>),
    /// `MOQ_EVENT_NAMESPACE_ACCEPTED`.
    NamespaceAccepted(HandleEvent<'session, Announcement>),
    /// `MOQ_EVENT_NAMESPACE_CANCELLED` (no retry interval).
    NamespaceCancelled(RequestRefused<'session, Announcement>),
    /// `MOQ_EVENT_GOAWAY`.
    Goaway(Goaway<'session>),
    /// `MOQ_EVENT_UNSUBSCRIBED`.
    Unsubscribed(RequestEnded<'session, Subscription>),
    /// `MOQ_EVENT_SUBSCRIBE_UPDATED`.
    SubscribeUpdated(TrackUpdated<'session, Subscription>),
    /// `MOQ_EVENT_OBJECT_CHUNK`.
    ObjectChunk(ObjectChunk<'session>),
    /// `MOQ_EVENT_NS_SUB_REQUEST`.
    NamespaceSubscriptionRequest(NamespaceSubscriptionRequest<'session>),
    /// `MOQ_EVENT_NS_SUB_OK`.
    NamespaceSubscriptionOk(HandleEvent<'session, NamespaceSubscription>),
    /// `MOQ_EVENT_NS_SUB_ERROR` (no retry interval).
    NamespaceSubscriptionError(RequestRefused<'session, NamespaceSubscription>),
    /// `MOQ_EVENT_NAMESPACE_FOUND`.
    NamespaceFound(NamespaceSuffix<'session>),
    /// `MOQ_EVENT_NAMESPACE_GONE`.
    NamespaceGone(NamespaceSuffix<'session>),
    /// `MOQ_EVENT_FETCH_REQUEST`.
    FetchRequest(FetchRequest<'session>),
    /// `MOQ_EVENT_FETCH_ERROR`.
    FetchError(RequestRefused<'session, Fetch>),
    /// `MOQ_EVENT_FETCH_CANCELLED`.
    FetchCancelled(RequestEnded<'session, Fetch>),
    /// `MOQ_EVENT_FETCH_OK`.
    FetchOk(FetchOk<'session>),
    /// `MOQ_EVENT_FETCH_COMPLETE`.
    FetchComplete(HandleEvent<'session, Fetch>),
    /// `MOQ_EVENT_FETCH_OBJECT`.
    FetchObject(FetchObject<'session>),
    /// `MOQ_EVENT_FETCH_GAP`.
    FetchGap(FetchGap<'session>),
    /// `MOQ_EVENT_PUBLISH_REQUEST`.
    PublishRequest(PublishRequest<'session>),
    /// `MOQ_EVENT_PUBLISH_OK`.
    PublishOk(PublishOk<'session>),
    /// `MOQ_EVENT_PUBLISH_ERROR`.
    PublishError(RequestRefused<'session, Publication>),
    /// `MOQ_EVENT_PUBLISH_FINISHED`.
    PublishFinished(TrackDone<'session, Publication>),
    /// `MOQ_EVENT_TRACK_STATUS_REQUEST`.
    TrackStatusRequest(TrackStatusRequestEvent<'session>),
    /// `MOQ_EVENT_TRACK_STATUS_OK`.
    TrackStatusOk(TrackStatusOk<'session>),
    /// `MOQ_EVENT_TRACK_STATUS_ERROR`.
    TrackStatusError(RequestRefused<'session, TrackStatusRequest>),
    /// `MOQ_EVENT_SUBSCRIBE_DONE`.
    SubscribeDone(TrackDone<'session, Subscription>),
    /// `MOQ_EVENT_PUBLISH_UNSUBSCRIBED`.
    PublishUnsubscribed(RequestEnded<'session, Publication>),
    /// `MOQ_EVENT_PUBLISH_UPDATED`.
    PublishUpdated(TrackUpdated<'session, Publication>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST`.
    SubscribeTracksRequest(SubscribeTracksRequest<'session>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_OK`.
    SubscribeTracksOk(HandleEvent<'session, TrackSubscription>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR`.
    SubscribeTracksError(RequestRefused<'session, TrackSubscription>),
    /// `MOQ_EVENT_PUBLISH_BLOCKED`.
    PublishBlocked(PublishBlocked<'session>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED`.
    SubscribeTracksCancelled(RequestEnded<'session, TrackSubscription>),
    /// `MOQ_EVENT_REQUEST_REDIRECT`.
    RequestRedirect(RequestRedirect<'session>),
    /// `MOQ_EVENT_REQUEST_GOAWAY`.
    RequestGoaway(RequestGoaway<'session>),
    /// `MOQ_EVENT_SUBGROUP_FINISHED`.
    SubgroupFinished(SubgroupEnded<'session>),
    /// `MOQ_EVENT_SUBSCRIPTION_UPDATE_OK`.
    SubscriptionUpdateOk(UpdateAccepted<'session, Subscription>),
    /// `MOQ_EVENT_PUBLICATION_UPDATE_OK`.
    PublicationUpdateOk(UpdateAccepted<'session, Publication>),
    /// `MOQ_EVENT_SUBGROUP_RESET`.
    SubgroupReset(SubgroupEnded<'session>),
    /// `MOQ_EVENT_FETCH_RESET`.
    FetchReset(FetchReset<'session>),
    /// `MOQ_EVENT_TRACK_STATUS_TERMINATED`.
    TrackStatusTerminated(StreamTerminated<'session, TrackStatusRequest, TrackStatusCause>),
    /// `MOQ_EVENT_FILL_CREATED`.
    FillCreated(FillCreated<'session>),
    /// `MOQ_EVENT_FILL_REQUEST`.
    FillRequest(FillRequest<'session>),
    /// `MOQ_EVENT_FILL_OBJECT`.
    FillObject(FillObject<'session>),
    /// `MOQ_EVENT_FILL_GAP`.
    FillGap(FillGap<'session>),
    /// `MOQ_EVENT_FILL_TERMINAL`.
    FillTerminal(FillTerminal<'session>),
    /// `MOQ_EVENT_NAMESPACE_TERMINATED`.
    NamespaceTerminated(StreamTerminated<'session, Announcement, NamespaceTerminalCause>),
    /// `MOQ_EVENT_PUBLISH_STATE`.
    PublishState(PublishState<'session>),
    /// `MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED`.
    NamespaceSubscriptionTerminated(
        StreamTerminated<'session, NamespaceSubscription, TerminalCause>,
    ),
    /// `MOQ_EVENT_NAMESPACE_UPDATE_REQUEST`.
    NamespaceUpdateRequest(NamespaceUpdateRequested<'session, Announcement>),
    /// `MOQ_EVENT_NS_SUB_UPDATE_REQUEST`.
    NamespaceSubscriptionUpdateRequest(NamespaceUpdateRequested<'session, NamespaceSubscription>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST`.
    SubscribeTracksUpdateRequest(NamespaceUpdateRequested<'session, TrackSubscription>),
    /// `MOQ_EVENT_NAMESPACE_UPDATE_RESULT`.
    NamespaceUpdateResult(NamespaceUpdateConcluded<'session, Announcement>),
    /// `MOQ_EVENT_NS_SUB_UPDATE_RESULT`.
    NamespaceSubscriptionUpdateResult(NamespaceUpdateConcluded<'session, NamespaceSubscription>),
    /// `MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT`.
    SubscribeTracksUpdateResult(NamespaceUpdateConcluded<'session, TrackSubscription>),
    /// `MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED`.
    OutgoingSubgroupTerminated(OutgoingSubgroupTerminated<'session>),
    /// A kind this crate does not know.
    Unknown(UnknownEvent<'session>),
}

impl Event<'_> {
    /// The kind.
    #[must_use]
    pub fn kind(&self) -> EventKind {
        match self {
            Event::SetupComplete(_) => EventKind::SetupComplete,
            Event::SessionClosed(_) => EventKind::SessionClosed,
            Event::SubscribeRequest(_) => EventKind::SubscribeRequest,
            Event::SubscribeOk(_) => EventKind::SubscribeOk,
            Event::SubscribeError(_) => EventKind::SubscribeError,
            Event::RequestReady(_) => EventKind::RequestReady,
            Event::ObjectReceived(_) => EventKind::ObjectReceived,
            Event::NamespacePublished(_) => EventKind::NamespacePublished,
            Event::NamespaceDone(_) => EventKind::NamespaceDone,
            Event::NamespaceRejected(_) => EventKind::NamespaceRejected,
            Event::NamespaceAccepted(_) => EventKind::NamespaceAccepted,
            Event::NamespaceCancelled(_) => EventKind::NamespaceCancelled,
            Event::Goaway(_) => EventKind::Goaway,
            Event::Unsubscribed(_) => EventKind::Unsubscribed,
            Event::SubscribeUpdated(_) => EventKind::SubscribeUpdated,
            Event::ObjectChunk(_) => EventKind::ObjectChunk,
            Event::NamespaceSubscriptionRequest(_) => EventKind::NamespaceSubscriptionRequest,
            Event::NamespaceSubscriptionOk(_) => EventKind::NamespaceSubscriptionOk,
            Event::NamespaceSubscriptionError(_) => EventKind::NamespaceSubscriptionError,
            Event::NamespaceFound(_) => EventKind::NamespaceFound,
            Event::NamespaceGone(_) => EventKind::NamespaceGone,
            Event::FetchRequest(_) => EventKind::FetchRequest,
            Event::FetchError(_) => EventKind::FetchError,
            Event::FetchCancelled(_) => EventKind::FetchCancelled,
            Event::FetchOk(_) => EventKind::FetchOk,
            Event::FetchComplete(_) => EventKind::FetchComplete,
            Event::FetchObject(_) => EventKind::FetchObject,
            Event::FetchGap(_) => EventKind::FetchGap,
            Event::PublishRequest(_) => EventKind::PublishRequest,
            Event::PublishOk(_) => EventKind::PublishOk,
            Event::PublishError(_) => EventKind::PublishError,
            Event::PublishFinished(_) => EventKind::PublishFinished,
            Event::TrackStatusRequest(_) => EventKind::TrackStatusRequest,
            Event::TrackStatusOk(_) => EventKind::TrackStatusOk,
            Event::TrackStatusError(_) => EventKind::TrackStatusError,
            Event::SubscribeDone(_) => EventKind::SubscribeDone,
            Event::PublishUnsubscribed(_) => EventKind::PublishUnsubscribed,
            Event::PublishUpdated(_) => EventKind::PublishUpdated,
            Event::SubscribeTracksRequest(_) => EventKind::SubscribeTracksRequest,
            Event::SubscribeTracksOk(_) => EventKind::SubscribeTracksOk,
            Event::SubscribeTracksError(_) => EventKind::SubscribeTracksError,
            Event::PublishBlocked(_) => EventKind::PublishBlocked,
            Event::SubscribeTracksCancelled(_) => EventKind::SubscribeTracksCancelled,
            Event::RequestRedirect(_) => EventKind::RequestRedirect,
            Event::RequestGoaway(_) => EventKind::RequestGoaway,
            Event::SubgroupFinished(_) => EventKind::SubgroupFinished,
            Event::SubscriptionUpdateOk(_) => EventKind::SubscriptionUpdateOk,
            Event::PublicationUpdateOk(_) => EventKind::PublicationUpdateOk,
            Event::SubgroupReset(_) => EventKind::SubgroupReset,
            Event::FetchReset(_) => EventKind::FetchReset,
            Event::TrackStatusTerminated(_) => EventKind::TrackStatusTerminated,
            Event::FillCreated(_) => EventKind::FillCreated,
            Event::FillRequest(_) => EventKind::FillRequest,
            Event::FillObject(_) => EventKind::FillObject,
            Event::FillGap(_) => EventKind::FillGap,
            Event::FillTerminal(_) => EventKind::FillTerminal,
            Event::NamespaceTerminated(_) => EventKind::NamespaceTerminated,
            Event::PublishState(_) => EventKind::PublishState,
            Event::NamespaceSubscriptionTerminated(_) => EventKind::NamespaceSubscriptionTerminated,
            Event::NamespaceUpdateRequest(_) => EventKind::NamespaceUpdateRequest,
            Event::NamespaceSubscriptionUpdateRequest(_) => {
                EventKind::NamespaceSubscriptionUpdateRequest
            }
            Event::SubscribeTracksUpdateRequest(_) => EventKind::SubscribeTracksUpdateRequest,
            Event::NamespaceUpdateResult(_) => EventKind::NamespaceUpdateResult,
            Event::NamespaceSubscriptionUpdateResult(_) => {
                EventKind::NamespaceSubscriptionUpdateResult
            }
            Event::SubscribeTracksUpdateResult(_) => EventKind::SubscribeTracksUpdateResult,
            Event::OutgoingSubgroupTerminated(_) => EventKind::OutgoingSubgroupTerminated,
            Event::Unknown(unknown) => EventKind::Other(unknown.kind),
        }
    }
}

// -- Decoding ------------------------------------------------------------

/// The polled record, released (`moq_event_cleanup`) after decoding. Owned
/// references are moved out as they are adopted, so each is released once:
/// by its payload, or by this cleanup when decoding stopped first.
pub(super) struct RawEvent(pub(super) sys::moq_event_t);

impl Drop for RawEvent {
    fn drop(&mut self) {
        // SAFETY: a record the session wrote; cleanup is idempotent and
        // releases only the references the record still holds.
        unsafe { sys::moq_event_cleanup(&mut self.0) }
    }
}

fn rec<'session>() -> Record<'session> {
    PhantomData
}

fn refused<'s, H>(
    rd: &Reader<'s>,
    handle: H,
    error_code: u64,
    can_retry: bool,
    retry_after_ms: u64,
    reason: sys::moq_bytes_t,
) -> Result<RequestRefused<'s, H>> {
    Ok(RequestRefused {
        handle,
        error_code,
        retry_after_ms: optional(can_retry, retry_after_ms),
        reason: rd.bytes(reason)?,
        _record: rec(),
    })
}

fn handled<'s, H>(handle: H) -> HandleEvent<'s, H> {
    HandleEvent {
        handle,
        _record: rec(),
    }
}

fn ended<'s, H>(handle: H, covered: bool, cause: u32, code: u64) -> RequestEnded<'s, H> {
    RequestEnded {
        handle,
        cause: covered.then(|| TerminalCause::from_raw(cause)),
        stream_error_code: covered.then_some(code),
        _record: rec(),
    }
}

/// The fields `moq_subscribe_updated_event_t` and
/// `moq_publish_updated_event_t` share, by name.
macro_rules! track_updated {
    ($rd:expr, $d:expr, $ty:ty, $handle:expr) => {{
        let rd = $rd;
        let d = $d;
        let filter = rd
            .covers(field_end!($ty, end_group, u64))
            .then(|| d.has_filter)
            .filter(|has| *has)
            .map(|_| UpdatedFilter {
                filter: subscribe_filter(d.filter, d.start_group, d.start_object, d.end_group),
            });
        TrackUpdated {
            handle: $handle,
            subscriber_priority: optional(d.has_subscriber_priority, d.subscriber_priority),
            forward: optional(d.has_forward, d.forward),
            delivery_timeout_us: optional(d.has_delivery_timeout, d.delivery_timeout_us),
            tokens: rd.tokens(d.tokens, d.token_count)?,
            new_group_request: optional(d.has_new_group_request, d.new_group_request),
            filter,
            answer_owed: rd
                .covers(field_end!($ty, answer_owed, bool))
                .then_some(d.answer_owed),
            fill_requested: rd
                .covers(field_end!($ty, fill_requested, bool))
                .then_some(d.fill_requested),
            update_count: rd
                .covers(field_end!($ty, update_count, u32))
                .then_some(d.update_count),
            update_request_id: rd
                .covers(field_end!($ty, update_request_id, u64))
                .then_some(d.update_request_id),
            options: if rd.covers(field_end!(
                $ty,
                options,
                *const sys::moq_subscription_update_options_t
            )) {
                match rd.nested(d.options)? {
                    Some(options) => Some(update_options(&options)?),
                    None => None,
                }
            } else {
                None
            },
            _record: rec(),
        }
    }};
}

/// The fields the three namespace update-result records share.
macro_rules! update_concluded {
    ($rd:expr, $d:expr, $handle:expr, $cluster:expr) => {{
        let d = $d;
        NamespaceUpdateConcluded {
            handle: $handle,
            update_request_id: d.update_request_id,
            outcome: match d.outcome {
                sys::MOQ_NAMESPACE_UPDATE_OUTCOME_OK => UpdateOutcome::Ok,
                sys::MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR => UpdateOutcome::Error,
                sys::MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED => UpdateOutcome::Refused,
                other => UpdateOutcome::Other(other),
            },
            local_role: RequestRole::from_raw(d.local_role),
            error_code: d.error_code,
            reason: $rd.bytes(d.reason)?,
            expires_ms: optional(d.has_expires, d.expires_ms),
            cluster: $cluster,
            _record: rec(),
        }
    }};
}

fn namespace_update<'s>(
    rd: &Reader<'s>,
    record: *const sys::moq_namespace_update_record_t,
) -> Result<Option<NamespaceUpdate<'s>>> {
    match rd.nested(record)? {
        Some(u) => namespace_update_record(rd, &u).map(Some),
        None => Ok(None),
    }
}

/// One namespace update record, from an event or an owed-update query; its
/// spans borrow what `rd` borrows.
pub(super) fn namespace_update_record<'s>(
    rd: &Reader<'s>,
    u: &sys::moq_namespace_update_record_t,
) -> Result<NamespaceUpdate<'s>> {
    super::raw::floor(u.struct_size, sys::MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE)?;
    Ok(NamespaceUpdate {
        update_request_id: u.update_request_id,
        prefix: if u.flags & sys::MOQ_NAMESPACE_UPDATE_PREFIX != 0 {
            Some(rd.namespace(u.track_namespace_prefix)?)
        } else {
            None
        },
        forward: optional(u.flags & sys::MOQ_NAMESPACE_UPDATE_FORWARD != 0, u.forward),
        authorization: u.flags & sys::MOQ_NAMESPACE_UPDATE_AUTHORIZATION != 0,
        tokens: rd.tokens(u.tokens, u.token_count)?,
        cluster: if super::raw::covers(
            u.struct_size,
            field_end!(
                sys::moq_namespace_update_record_t,
                cluster,
                sys::moq_cluster_metadata_t
            ),
        ) {
            Some(rd.cluster(&u.cluster)?)
        } else {
            None
        },
    })
}

fn fill_parent(
    rd: &Reader<'_>,
    fill: sys::moq_fill_t,
    sub: sys::moq_subscription_t,
    publication: sys::moq_publication_t,
    initiating_request_id: u64,
) -> FillParent {
    FillParent {
        fill: rd.fill(fill),
        subscription: rd.subscription_if_valid(sub),
        publication: rd.publication_if_valid(publication),
        initiating_request_id,
    }
}

/// Decode one polled record. Owned references move into [`Payload`]s as the
/// last step of each kind, after every check; the caller's [`RawEvent`]
/// releases whatever the record still holds.
///
/// # Safety
/// `ev` was written by a poll of a session that stays exclusively borrowed
/// for `'session`, and the caller owns its references; `owner` is that
/// session's owner.
pub(super) unsafe fn decode<'session>(
    ev: &mut sys::moq_event_t,
    owner: OwnerId,
) -> Result<Event<'session>> {
    // SAFETY: the caller's contract is the reader's.
    let rd: Reader<'session> = unsafe { Reader::new(ev.detail_size, owner) };
    let size = ev.detail_size;
    // SAFETY: the record's kind selects the union member C wrote; each arm
    // checks its kind's floor before it reads a field past it, and reads an
    // appended field only under the coverage check.
    unsafe {
        let u = &mut ev.u;
        Ok(match ev.kind {
            sys::MOQ_EVENT_SETUP_COMPLETE => {
                rd.floor(sys::MOQ_SETUP_COMPLETE_EVENT_V0_SIZE)?;
                let d = u.setup_complete;
                Event::SetupComplete(SetupComplete {
                    local_perspective: view::perspective(d.local_perspective)?,
                    peer_perspective: view::perspective(d.peer_perspective)?,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SESSION_CLOSED => {
                rd.floor(sys::MOQ_SESSION_CLOSED_EVENT_V0_SIZE)?;
                let d = u.closed;
                Event::SessionClosed(SessionClosed {
                    code: d.code,
                    reason: rd.bytes(d.reason)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_REQUEST => {
                rd.floor(sys::MOQ_SUBSCRIBE_REQUEST_EVENT_V0_SIZE)?;
                let d = u.subscribe_request;
                let options = if rd.covers(field_end!(
                    sys::moq_subscribe_request_event_t,
                    options,
                    *const sys::moq_subscription_admission_options_t
                )) {
                    match rd.nested(d.options)? {
                        Some(o) => Some(admission_options(&o)?),
                        None => None,
                    }
                } else {
                    None
                };
                Event::SubscribeRequest(SubscribeRequest {
                    subscription: rd.subscription(d.sub),
                    namespace: rd.namespace(d.track_namespace)?,
                    track_name: rd.bytes(d.track_name)?,
                    filter: subscribe_filter(d.filter, d.start_group, d.start_object, d.end_group),
                    subscriber_priority: d.subscriber_priority,
                    group_order: view::group_order(d.group_order)?,
                    forward: d.forward,
                    delivery_timeout_us: d.delivery_timeout_us,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    new_group_request: optional(d.has_new_group_request, d.new_group_request),
                    options,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_OK => {
                rd.floor(sys::MOQ_SUBSCRIBE_OK_EVENT_V0_SIZE)?;
                let d = u.subscribe_ok;
                let publisher_group_order = if rd.covers(field_end!(
                    sys::moq_subscribe_ok_event_t,
                    publisher_group_order,
                    u32
                )) {
                    Some(view::group_order(d.publisher_group_order)?)
                } else {
                    None
                };
                Event::SubscribeOk(SubscribeOk {
                    subscription: rd.subscription(d.sub),
                    track_alias: d.track_alias,
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    track_properties: rd.bytes(d.track_properties)?,
                    dynamic_groups: d.dynamic_groups,
                    publisher_group_order,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_ERROR => {
                rd.floor(sys::MOQ_SUBSCRIBE_ERROR_EVENT_V0_SIZE)?;
                let d = u.subscribe_error;
                Event::SubscribeError(refused(
                    &rd,
                    rd.subscription(d.sub),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_REQUEST_READY => {
                rd.floor(sys::MOQ_REQUEST_READY_EVENT_V0_SIZE)?;
                Event::RequestReady(RequestReady {
                    available_requests: u.request_ready.available_requests,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_OBJECT_RECEIVED => {
                rd.floor(sys::MOQ_OBJECT_RECEIVED_EVENT_V0_SIZE)?;
                let d = &mut u.object_received;
                let subscription = rd.subscription_if_valid(d.sub);
                let publication = rd.publication_if_valid(d.pub_);
                let payload = rd.take(&mut d.payload);
                let properties = rd.take(&mut d.properties);
                Event::ObjectReceived(ObjectReceived {
                    subscription,
                    publication,
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    object_id: d.object_id,
                    publisher_priority: d.publisher_priority,
                    status: ObjectStatus::from_code(d.status),
                    end_of_group: d.end_of_group,
                    datagram: d.datagram,
                    first_object: d.first_object,
                    payload,
                    properties,
                    prior_group_gap: optional(d.has_prior_group_gap, d.prior_group_gap),
                    prior_object_gap: optional(d.has_prior_object_gap, d.prior_object_gap),
                    subgroup_properties: d.subgroup_properties,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_PUBLISHED => {
                rd.floor(sys::MOQ_NAMESPACE_PUBLISHED_EVENT_V0_SIZE)?;
                let d = u.namespace_published;
                Event::NamespacePublished(NamespacePublished {
                    announcement: rd.announcement(d.ann),
                    namespace: rd.namespace(d.track_namespace)?,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    cluster: if rd.covers(field_end!(
                        sys::moq_namespace_published_event_t,
                        cluster,
                        sys::moq_cluster_metadata_t
                    )) {
                        Some(rd.cluster(&d.cluster)?)
                    } else {
                        None
                    },
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_DONE => {
                rd.floor(sys::MOQ_NAMESPACE_DONE_EVENT_V0_SIZE)?;
                Event::NamespaceDone(handled(rd.announcement(u.namespace_done.ann)))
            }
            sys::MOQ_EVENT_NAMESPACE_REJECTED => {
                rd.floor(sys::MOQ_NAMESPACE_REJECTED_EVENT_V0_SIZE)?;
                let d = u.namespace_rejected;
                Event::NamespaceRejected(refused(
                    &rd,
                    rd.announcement(d.ann),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_NAMESPACE_ACCEPTED => {
                rd.floor(sys::MOQ_NAMESPACE_ACCEPTED_EVENT_V0_SIZE)?;
                Event::NamespaceAccepted(handled(rd.announcement(u.namespace_accepted.ann)))
            }
            sys::MOQ_EVENT_NAMESPACE_CANCELLED => {
                rd.floor(sys::MOQ_NAMESPACE_CANCELLED_EVENT_V0_SIZE)?;
                let d = u.namespace_cancelled;
                Event::NamespaceCancelled(refused(
                    &rd,
                    rd.announcement(d.ann),
                    d.error_code,
                    false,
                    0,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_GOAWAY => {
                rd.floor(sys::MOQ_GOAWAY_EVENT_V0_SIZE)?;
                let d = u.goaway;
                let covered = rd.covers(field_end!(sys::moq_goaway_event_t, timeout_ms, u64));
                Event::Goaway(Goaway {
                    new_session_uri: rd.bytes(d.new_session_uri)?,
                    timeout_ms: optional(covered && d.has_timeout, d.timeout_ms),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_UNSUBSCRIBED => {
                rd.floor(sys::MOQ_UNSUBSCRIBED_EVENT_V0_SIZE)?;
                let d = u.unsubscribed;
                Event::Unsubscribed(ended(
                    rd.subscription(d.sub),
                    rd.covers(field_end!(
                        sys::moq_unsubscribed_event_t,
                        stream_error_code,
                        u64
                    )),
                    d.cause,
                    d.stream_error_code,
                ))
            }
            sys::MOQ_EVENT_SUBSCRIBE_UPDATED => {
                rd.floor(sys::MOQ_SUBSCRIBE_UPDATED_EVENT_V0_SIZE)?;
                let d = u.subscribe_updated;
                Event::SubscribeUpdated(track_updated!(
                    &rd,
                    d,
                    sys::moq_subscribe_updated_event_t,
                    rd.subscription(d.sub)
                ))
            }
            sys::MOQ_EVENT_OBJECT_CHUNK => {
                rd.floor(sys::MOQ_OBJECT_CHUNK_EVENT_V0_SIZE)?;
                let d = &mut u.object_chunk;
                let end = d.end.then_some(match d.terminal {
                    sys::MOQ_OBJECT_TERMINAL_NORMAL => ObjectTerminal::Normal,
                    sys::MOQ_OBJECT_TERMINAL_RESET => ObjectTerminal::Reset {
                        error_code: d.error_code,
                    },
                    sys::MOQ_OBJECT_TERMINAL_STOP => ObjectTerminal::Stop,
                    other => ObjectTerminal::Other(other),
                });
                let subscription = rd.subscription_if_valid(d.sub);
                let publication = rd.publication_if_valid(d.pub_);
                let chunk = rd.take(&mut d.chunk);
                let start = if d.begin {
                    Some(ChunkStart {
                        publisher_priority: d.publisher_priority,
                        status: ObjectStatus::from_code(d.status),
                        end_of_group: d.end_of_group,
                        first_object: d.first_object,
                        payload_length: d.payload_length,
                        properties: rd.take(&mut d.properties),
                        prior_group_gap: optional(d.has_prior_group_gap, d.prior_group_gap),
                        prior_object_gap: optional(d.has_prior_object_gap, d.prior_object_gap),
                        subgroup_properties: d.subgroup_properties,
                    })
                } else {
                    None
                };
                Event::ObjectChunk(ObjectChunk {
                    subscription,
                    publication,
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    object_id: d.object_id,
                    chunk,
                    start,
                    end,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NS_SUB_REQUEST => {
                rd.floor(sys::MOQ_NS_SUB_REQUEST_EVENT_V0_SIZE)?;
                let d = u.ns_sub_request;
                Event::NamespaceSubscriptionRequest(NamespaceSubscriptionRequest {
                    subscription: rd.ns_sub(d.handle),
                    prefix: rd.namespace(d.track_namespace_prefix)?,
                    interest: NamespaceInterest::from_raw(d.namespace_interest),
                    forward: d.forward,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NS_SUB_OK => {
                rd.floor(sys::MOQ_NS_SUB_OK_EVENT_V0_SIZE)?;
                Event::NamespaceSubscriptionOk(handled(rd.ns_sub(u.ns_sub_ok.handle)))
            }
            sys::MOQ_EVENT_NS_SUB_ERROR => {
                rd.floor(sys::MOQ_NS_SUB_ERROR_EVENT_V0_SIZE)?;
                let d = u.ns_sub_error;
                Event::NamespaceSubscriptionError(refused(
                    &rd,
                    rd.ns_sub(d.handle),
                    d.error_code,
                    false,
                    0,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_NAMESPACE_FOUND => {
                rd.floor(sys::MOQ_NAMESPACE_FOUND_EVENT_V0_SIZE)?;
                let d = u.namespace_found;
                Event::NamespaceFound(NamespaceSuffix {
                    subscription: rd.ns_sub(d.handle),
                    suffix: rd.namespace(d.track_namespace_suffix)?,
                    cluster: if rd.covers(field_end!(
                        sys::moq_namespace_found_event_t,
                        cluster,
                        sys::moq_cluster_metadata_t
                    )) {
                        Some(rd.cluster(&d.cluster)?)
                    } else {
                        None
                    },
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_GONE => {
                rd.floor(sys::MOQ_NAMESPACE_GONE_EVENT_V0_SIZE)?;
                let d = u.namespace_gone;
                Event::NamespaceGone(NamespaceSuffix {
                    subscription: rd.ns_sub(d.handle),
                    suffix: rd.namespace(d.track_namespace_suffix)?,
                    cluster: None,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FETCH_REQUEST => {
                rd.floor(sys::MOQ_FETCH_REQUEST_EVENT_V0_SIZE)?;
                let d = u.fetch_request;
                let joining_pub = if rd.covers(field_end!(
                    sys::moq_fetch_request_event_t,
                    joining_pub,
                    sys::moq_publication_t
                )) {
                    rd.publication_if_valid(d.joining_pub)
                } else {
                    None
                };
                let joining = rd
                    .subscription_if_valid(d.joining_sub)
                    .map(Joining::Subscription)
                    .or(joining_pub.map(Joining::Publication));
                let options = if rd.covers(field_end!(
                    sys::moq_fetch_request_event_t,
                    options,
                    *const sys::moq_fetch_request_options_t
                )) {
                    match rd.nested(d.options)? {
                        Some(o) => Some(fetch_options(&o)?),
                        None => None,
                    }
                } else {
                    None
                };
                Event::FetchRequest(FetchRequest {
                    fetch: rd.fetch(d.fetch),
                    namespace: rd.namespace(d.track_namespace)?,
                    track_name: rd.bytes(d.track_name)?,
                    joining,
                    start: view::location(d.start_group, d.start_object),
                    end_group: d.end_group,
                    end_object: d.end_object,
                    subscriber_priority: d.subscriber_priority,
                    group_order: view::group_order(d.group_order)?,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    options,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FETCH_ERROR => {
                rd.floor(sys::MOQ_FETCH_ERROR_EVENT_V0_SIZE)?;
                let d = u.fetch_error;
                Event::FetchError(refused(
                    &rd,
                    rd.fetch(d.fetch),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_FETCH_CANCELLED => {
                rd.floor(sys::MOQ_FETCH_CANCELLED_EVENT_V0_SIZE)?;
                let d = u.fetch_cancelled;
                Event::FetchCancelled(ended(
                    rd.fetch(d.fetch),
                    rd.covers(field_end!(
                        sys::moq_fetch_cancelled_event_t,
                        stream_error_code,
                        u64
                    )),
                    d.cause,
                    d.stream_error_code,
                ))
            }
            sys::MOQ_EVENT_FETCH_OK => {
                rd.floor(sys::MOQ_FETCH_OK_EVENT_V0_SIZE)?;
                let d = u.fetch_ok;
                Event::FetchOk(FetchOk {
                    fetch: rd.fetch(d.fetch),
                    end_of_track: d.end_of_track,
                    end_group: d.end_group,
                    end_object: d.end_object,
                    track_properties: rd.bytes(d.track_properties)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FETCH_COMPLETE => {
                rd.floor(sys::MOQ_FETCH_COMPLETE_EVENT_V0_SIZE)?;
                Event::FetchComplete(handled(rd.fetch(u.fetch_complete.fetch)))
            }
            sys::MOQ_EVENT_FETCH_OBJECT => {
                rd.floor(sys::MOQ_FETCH_OBJECT_EVENT_V0_SIZE)?;
                let d = &mut u.fetch_object;
                let gaps = rd.covers(field_end!(
                    sys::moq_fetch_object_event_t,
                    prior_object_gap,
                    u64
                ));
                let payload = rd.take(&mut d.payload);
                let properties = rd.take(&mut d.properties);
                Event::FetchObject(FetchObject {
                    fetch: rd.fetch(d.fetch),
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    object_id: d.object_id,
                    publisher_priority: d.publisher_priority,
                    datagram: d.datagram,
                    payload,
                    properties,
                    prior_group_gap: optional(gaps && d.has_prior_group_gap, d.prior_group_gap),
                    prior_object_gap: optional(gaps && d.has_prior_object_gap, d.prior_object_gap),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FETCH_GAP => {
                rd.floor(sys::MOQ_FETCH_GAP_EVENT_V0_SIZE)?;
                let d = u.fetch_gap;
                Event::FetchGap(FetchGap {
                    fetch: rd.fetch(d.fetch),
                    range: FetchRange::from_raw(d.range_kind),
                    end: view::location(d.group_id, d.object_id),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLISH_REQUEST => {
                rd.floor(sys::MOQ_PUBLISH_REQUEST_EVENT_V0_SIZE)?;
                let d = u.publish_request;
                Event::PublishRequest(PublishRequest {
                    publication: rd.publication(d.pub_),
                    namespace: rd.namespace(d.track_namespace)?,
                    track_name: rd.bytes(d.track_name)?,
                    track_alias: d.track_alias,
                    forward: d.forward,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    track_properties: rd.bytes(d.track_properties)?,
                    dynamic_groups: d.dynamic_groups,
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLISH_OK => {
                rd.floor(sys::MOQ_PUBLISH_OK_EVENT_V0_SIZE)?;
                let d = u.publish_ok;
                let filter = (rd.covers(field_end!(sys::moq_publish_ok_event_t, end_group, u64))
                    && d.has_filter)
                    .then(|| {
                        subscribe_filter(d.filter, d.start_group, d.start_object, d.end_group)
                    });
                Event::PublishOk(PublishOk {
                    publication: rd.publication(d.pub_),
                    send_allowed: d.send_allowed,
                    subscriber_priority: d.subscriber_priority,
                    group_order: view::group_order(d.group_order)?,
                    delivery_timeout_ms: optional(d.has_delivery_timeout, d.delivery_timeout_ms),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    new_group_request: optional(d.has_new_group_request, d.new_group_request),
                    filter,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLISH_ERROR => {
                rd.floor(sys::MOQ_PUBLISH_ERROR_EVENT_V0_SIZE)?;
                let d = u.publish_error;
                Event::PublishError(refused(
                    &rd,
                    rd.publication(d.pub_),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_PUBLISH_FINISHED => {
                rd.floor(sys::MOQ_PUBLISH_FINISHED_EVENT_V0_SIZE)?;
                let d = u.publish_finished;
                let covered = rd.covers(field_end!(
                    sys::moq_publish_finished_event_t,
                    stream_error_code,
                    u64
                ));
                Event::PublishFinished(TrackDone {
                    handle: rd.publication(d.pub_),
                    status_code: d.status_code,
                    stream_count: d.stream_count,
                    reason: rd.bytes(d.reason)?,
                    cause: covered.then(|| TerminalCause::from_raw(d.cause)),
                    stream_error_code: covered.then_some(d.stream_error_code),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_TRACK_STATUS_REQUEST => {
                rd.floor(sys::MOQ_TRACK_STATUS_REQUEST_EVENT_V0_SIZE)?;
                let d = u.track_status_request;
                Event::TrackStatusRequest(TrackStatusRequestEvent {
                    request: rd.track_status(d.handle),
                    namespace: rd.namespace(d.track_namespace)?,
                    track_name: rd.bytes(d.track_name)?,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_TRACK_STATUS_OK => {
                rd.floor(sys::MOQ_TRACK_STATUS_OK_EVENT_V0_SIZE)?;
                let d = u.track_status_ok;
                Event::TrackStatusOk(TrackStatusOk {
                    request: rd.track_status(d.handle),
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    track_properties: rd.bytes(d.track_properties)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_TRACK_STATUS_ERROR => {
                rd.floor(sys::MOQ_TRACK_STATUS_ERROR_EVENT_V0_SIZE)?;
                let d = u.track_status_error;
                Event::TrackStatusError(refused(
                    &rd,
                    rd.track_status(d.handle),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_SUBSCRIBE_DONE => {
                rd.floor(sys::MOQ_SUBSCRIBE_DONE_EVENT_V0_SIZE)?;
                let d = u.subscribe_done;
                Event::SubscribeDone(TrackDone {
                    handle: rd.subscription(d.sub),
                    status_code: d.status_code,
                    stream_count: d.stream_count,
                    reason: rd.bytes(d.reason)?,
                    cause: None,
                    stream_error_code: None,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLISH_UNSUBSCRIBED => {
                rd.floor(sys::MOQ_PUBLISH_UNSUBSCRIBED_EVENT_V0_SIZE)?;
                let d = u.publish_unsubscribed;
                Event::PublishUnsubscribed(ended(
                    rd.publication(d.pub_),
                    rd.covers(field_end!(
                        sys::moq_publish_unsubscribed_event_t,
                        stream_error_code,
                        u64
                    )),
                    d.cause,
                    d.stream_error_code,
                ))
            }
            sys::MOQ_EVENT_PUBLISH_UPDATED => {
                rd.floor(sys::MOQ_PUBLISH_UPDATED_EVENT_V0_SIZE)?;
                let d = u.publish_updated;
                Event::PublishUpdated(track_updated!(
                    &rd,
                    d,
                    sys::moq_publish_updated_event_t,
                    rd.publication(d.pub_)
                ))
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_REQUEST_EVENT_V0_SIZE)?;
                let d = u.subscribe_tracks_request;
                let options = if rd.covers(field_end!(
                    sys::moq_subscribe_tracks_request_event_t,
                    options,
                    *const sys::moq_subscribe_tracks_options_t
                )) {
                    match rd.nested(d.options)? {
                        Some(o) => {
                            super::raw::floor(
                                o.struct_size,
                                sys::MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE as usize,
                            )?;
                            Some(SubscribeTracksOptions {
                                group_order: view::group_order(o.group_order)?,
                                group_order_selected: o.flags
                                    & sys::MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER
                                    != 0,
                                include_properties: o.include_properties,
                                include_properties_selected: o.flags
                                    & sys::MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES
                                    != 0,
                            })
                        }
                        None => None,
                    }
                } else {
                    None
                };
                Event::SubscribeTracksRequest(SubscribeTracksRequest {
                    request: rd.track_sub(d.handle),
                    prefix: rd.namespace(d.track_namespace_prefix)?,
                    forward: d.forward,
                    tokens: rd.tokens(d.tokens, d.token_count)?,
                    options,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_OK => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_OK_EVENT_V0_SIZE)?;
                Event::SubscribeTracksOk(handled(rd.track_sub(u.subscribe_tracks_ok.handle)))
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_ERROR_EVENT_V0_SIZE)?;
                let d = u.subscribe_tracks_error;
                Event::SubscribeTracksError(refused(
                    &rd,
                    rd.track_sub(d.handle),
                    d.error_code,
                    d.can_retry,
                    d.retry_after_ms,
                    d.reason,
                )?)
            }
            sys::MOQ_EVENT_PUBLISH_BLOCKED => {
                rd.floor(sys::MOQ_PUBLISH_BLOCKED_EVENT_V0_SIZE)?;
                let d = u.publish_blocked;
                Event::PublishBlocked(PublishBlocked {
                    request: rd.track_sub(d.handle),
                    suffix: rd.namespace(d.track_namespace_suffix)?,
                    track_name: rd.bytes(d.track_name)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_CANCELLED_EVENT_V0_SIZE)?;
                let d = u.subscribe_tracks_cancelled;
                Event::SubscribeTracksCancelled(ended(
                    rd.track_sub(d.handle),
                    rd.covers(field_end!(
                        sys::moq_subscribe_tracks_cancelled_event_t,
                        stream_error_code,
                        u64
                    )),
                    d.cause,
                    d.stream_error_code,
                ))
            }
            sys::MOQ_EVENT_REQUEST_REDIRECT => {
                rd.floor(sys::MOQ_REQUEST_REDIRECT_EVENT_V0_SIZE)?;
                let d = u.request_redirect;
                Event::RequestRedirect(RequestRedirect {
                    request: request_handle(&rd, d.family, d.handle.raw),
                    error_code: d.error_code,
                    retry_after_ms: optional(d.can_retry, d.retry_after_ms),
                    connect_uri: rd.bytes(d.connect_uri)?,
                    namespace: rd.namespace(d.track_namespace)?,
                    track_name: rd.bytes(d.track_name)?,
                    reason: rd.bytes(d.reason)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_REQUEST_GOAWAY => {
                rd.floor(sys::MOQ_REQUEST_GOAWAY_EVENT_V0_SIZE)?;
                let d = u.request_goaway;
                Event::RequestGoaway(RequestGoaway {
                    request: request_handle(&rd, d.family, d.handle.raw),
                    new_session_uri: rd.bytes(d.new_session_uri)?,
                    timeout_ms: d.timeout_ms,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBGROUP_FINISHED => {
                rd.floor(sys::MOQ_SUBGROUP_FINISHED_EVENT_V0_SIZE)?;
                let d = u.subgroup_finished;
                let covered = rd.covers(field_end!(
                    sys::moq_subgroup_finished_event_t,
                    last_object_id,
                    u64
                ));
                Event::SubgroupFinished(SubgroupEnded {
                    subscription: rd.subscription_if_valid(d.sub),
                    publication: rd.publication_if_valid(d.pub_),
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    end_of_group: d.end_of_group,
                    error_code: None,
                    last_object_id: covered.then(|| optional(d.has_last_object, d.last_object_id)),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIPTION_UPDATE_OK => {
                rd.floor(sys::MOQ_SUBSCRIPTION_UPDATE_OK_EVENT_V0_SIZE)?;
                let d = u.subscription_update_ok;
                Event::SubscriptionUpdateOk(UpdateAccepted {
                    handle: rd.subscription(d.sub),
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLICATION_UPDATE_OK => {
                rd.floor(sys::MOQ_PUBLICATION_UPDATE_OK_EVENT_V0_SIZE)?;
                let d = u.publication_update_ok;
                Event::PublicationUpdateOk(UpdateAccepted {
                    handle: rd.publication(d.pub_),
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    expires_ms: optional(d.has_expires, d.expires_ms),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBGROUP_RESET => {
                rd.floor(sys::MOQ_SUBGROUP_RESET_EVENT_V0_SIZE)?;
                let d = u.subgroup_reset;
                let covered = rd.covers(field_end!(
                    sys::moq_subgroup_reset_event_t,
                    last_object_id,
                    u64
                ));
                Event::SubgroupReset(SubgroupEnded {
                    subscription: rd.subscription_if_valid(d.sub),
                    publication: rd.publication_if_valid(d.pub_),
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    end_of_group: d.end_of_group,
                    error_code: Some(d.error_code),
                    last_object_id: covered.then(|| optional(d.has_last_object, d.last_object_id)),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FETCH_RESET => {
                rd.floor(sys::MOQ_FETCH_RESET_EVENT_V0_SIZE)?;
                let d = u.fetch_reset;
                let covered = rd.covers(field_end!(sys::moq_fetch_reset_event_t, cause, u32));
                Event::FetchReset(FetchReset {
                    fetch: rd.fetch(d.fetch),
                    error_code: d.error_code,
                    data_stream: d.data_stream,
                    stop_sending: d.stop_sending,
                    cause: covered.then(|| TerminalCause::from_raw(d.cause)),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_TRACK_STATUS_TERMINATED => {
                rd.floor(sys::MOQ_TRACK_STATUS_TERMINATED_EVENT_V0_SIZE)?;
                let d = u.track_status_terminated;
                Event::TrackStatusTerminated(StreamTerminated {
                    handle: rd.track_status(d.handle),
                    cause: match d.cause {
                        sys::MOQ_TRACK_STATUS_PEER_RESET_STREAM => {
                            TrackStatusCause::PeerResetStream
                        }
                        sys::MOQ_TRACK_STATUS_PEER_STOP_SENDING => {
                            TrackStatusCause::PeerStopSending
                        }
                        sys::MOQ_TRACK_STATUS_MISSING_RESPONSE => TrackStatusCause::MissingResponse,
                        sys::MOQ_TRACK_STATUS_TRUNCATED_RESPONSE => {
                            TrackStatusCause::TruncatedResponse
                        }
                        other => TrackStatusCause::Other(other),
                    },
                    local_role: RequestRole::from_raw(d.local_role),
                    stream_error_code: d.stream_error_code,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FILL_CREATED => {
                rd.floor(sys::MOQ_FILL_CREATED_EVENT_V0_SIZE as usize)?;
                let d = u.fill_created;
                Event::FillCreated(FillCreated {
                    fill: fill_parent(&rd, d.fill, d.sub, d.pub_, d.initiating_request_id),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FILL_REQUEST => {
                rd.floor(sys::MOQ_FILL_REQUEST_EVENT_V0_SIZE as usize)?;
                let d = u.fill_request;
                Event::FillRequest(FillRequest {
                    fill: fill_parent(&rd, d.fill, d.sub, d.pub_, d.initiating_request_id),
                    start: view::location(d.start_group, d.start_object),
                    end: view::location(d.end_group, d.end_object),
                    processed_at: Instant::from_micros(d.processed_at_us),
                    fill_timeout_ms: optional(d.has_fill_timeout, d.fill_timeout_ms),
                    group_order: view::group_order(d.group_order)?,
                    subscriber_priority: d.subscriber_priority,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FILL_OBJECT => {
                rd.floor(sys::MOQ_FILL_OBJECT_EVENT_V0_SIZE)?;
                let d = &mut u.fill_object;
                let gaps = rd.covers(field_end!(
                    sys::moq_fill_object_event_t,
                    prior_object_gap,
                    u64
                ));
                let fill = fill_parent(&rd, d.fill, d.sub, d.pub_, d.initiating_request_id);
                let payload = rd.take(&mut d.payload);
                let properties = rd.take(&mut d.properties);
                Event::FillObject(FillObject {
                    fill,
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    object_id: d.object_id,
                    publisher_priority: d.publisher_priority,
                    datagram: d.datagram,
                    payload,
                    properties,
                    prior_group_gap: optional(gaps && d.has_prior_group_gap, d.prior_group_gap),
                    prior_object_gap: optional(gaps && d.has_prior_object_gap, d.prior_object_gap),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FILL_GAP => {
                rd.floor(sys::MOQ_FILL_GAP_EVENT_V0_SIZE)?;
                let d = u.fill_gap;
                Event::FillGap(FillGap {
                    fill: fill_parent(&rd, d.fill, d.sub, d.pub_, d.initiating_request_id),
                    end: view::location(d.group_id, d.object_id),
                    range: FetchRange::from_raw(d.kind),
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_FILL_TERMINAL => {
                rd.floor(sys::MOQ_FILL_TERMINAL_EVENT_V0_SIZE as usize)?;
                let d = u.fill_terminal;
                Event::FillTerminal(FillTerminal {
                    fill: fill_parent(&rd, d.fill, d.sub, d.pub_, d.initiating_request_id),
                    reason: match d.reason {
                        sys::MOQ_FILL_COMPLETE => FillEnd::Complete,
                        sys::MOQ_FILL_NO_STREAM => FillEnd::NoStream,
                        sys::MOQ_FILL_LOCAL_CANCELLED => FillEnd::LocalCancelled,
                        sys::MOQ_FILL_PEER_CANCELLED_OR_RESET => FillEnd::PeerCancelledOrReset,
                        sys::MOQ_FILL_LOCAL_ABORTED => FillEnd::LocalAborted,
                        sys::MOQ_FILL_PARENT_REJECTED => FillEnd::ParentRejected,
                        sys::MOQ_FILL_PARENT_TERMINATED => FillEnd::ParentTerminated,
                        other => FillEnd::Other(other),
                    },
                    error: match d.error_domain {
                        sys::MOQ_FILL_ERROR_NONE => FillError::None,
                        sys::MOQ_FILL_ERROR_STREAM => FillError::Stream(d.error_code),
                        sys::MOQ_FILL_ERROR_PARENT_REQUEST => {
                            FillError::ParentRequest(d.error_code)
                        }
                        sys::MOQ_FILL_ERROR_PARENT_DONE => FillError::ParentDone(d.error_code),
                        domain => FillError::Other {
                            domain,
                            code: d.error_code,
                        },
                    },
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_TERMINATED => {
                rd.floor(sys::MOQ_NAMESPACE_TERMINATED_EVENT_V0_SIZE)?;
                let d = u.namespace_terminated;
                Event::NamespaceTerminated(StreamTerminated {
                    handle: rd.announcement(d.ann),
                    cause: match d.cause {
                        sys::MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE => {
                            NamespaceTerminalCause::PeerFinComplete
                        }
                        sys::MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM => {
                            NamespaceTerminalCause::PeerResetStream
                        }
                        sys::MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING => {
                            NamespaceTerminalCause::PeerStopSending
                        }
                        sys::MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE => {
                            NamespaceTerminalCause::MissingResponse
                        }
                        sys::MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE => {
                            NamespaceTerminalCause::TruncatedMessage
                        }
                        sys::MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY => {
                            NamespaceTerminalCause::LocalCapacity
                        }
                        other => NamespaceTerminalCause::Other(other),
                    },
                    local_role: RequestRole::from_raw(d.local_role),
                    stream_error_code: d.stream_error_code,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_PUBLISH_STATE => {
                rd.floor(sys::MOQ_PUBLISH_STATE_EVENT_V0_SIZE)?;
                let d = u.publish_state;
                let location = if d.has_filter {
                    match rd.nested(d.location)? {
                        Some(cfg) => Some(view::location_filter(&cfg)?),
                        None => return Err(Error::ABI_MISMATCH),
                    }
                } else {
                    None
                };
                Event::PublishState(PublishState {
                    request: request_handle(&rd, d.family, d.handle.raw),
                    forward: optional(d.has_forward, d.forward),
                    largest: optional(
                        d.has_largest,
                        view::location(d.largest_group, d.largest_object),
                    ),
                    location,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED => {
                rd.floor(sys::MOQ_NS_SUBSCRIPTION_TERMINATED_EVENT_V0_SIZE)?;
                let d = u.ns_subscription_terminated;
                Event::NamespaceSubscriptionTerminated(StreamTerminated {
                    handle: rd.ns_sub(d.handle),
                    cause: TerminalCause::from_raw(d.cause),
                    local_role: RequestRole::from_raw(d.local_role),
                    stream_error_code: d.stream_error_code,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_UPDATE_REQUEST => {
                rd.floor(sys::MOQ_NAMESPACE_UPDATE_REQUEST_EVENT_V0_SIZE)?;
                let d = u.namespace_update_request;
                Event::NamespaceUpdateRequest(NamespaceUpdateRequested {
                    handle: rd.announcement(d.ann),
                    update_request_id: d.update_request_id,
                    update: namespace_update(&rd, d.update)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NS_SUB_UPDATE_REQUEST => {
                rd.floor(sys::MOQ_NS_SUB_UPDATE_REQUEST_EVENT_V0_SIZE)?;
                let d = u.ns_sub_update_request;
                Event::NamespaceSubscriptionUpdateRequest(NamespaceUpdateRequested {
                    handle: rd.ns_sub(d.handle),
                    update_request_id: d.update_request_id,
                    update: namespace_update(&rd, d.update)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_UPDATE_REQUEST_EVENT_V0_SIZE)?;
                let d = u.subscribe_tracks_update_request;
                Event::SubscribeTracksUpdateRequest(NamespaceUpdateRequested {
                    handle: rd.track_sub(d.handle),
                    update_request_id: d.update_request_id,
                    update: namespace_update(&rd, d.update)?,
                    _record: rec(),
                })
            }
            sys::MOQ_EVENT_NAMESPACE_UPDATE_RESULT => {
                rd.floor(sys::MOQ_NAMESPACE_UPDATE_RESULT_EVENT_V0_SIZE)?;
                let d = u.namespace_update_result;
                let cluster = if rd.covers(field_end!(
                    sys::moq_namespace_update_result_event_t,
                    cluster,
                    sys::moq_cluster_metadata_t
                )) {
                    Some(rd.cluster(&d.cluster)?)
                } else {
                    None
                };
                Event::NamespaceUpdateResult(update_concluded!(
                    &rd,
                    d,
                    rd.announcement(d.ann),
                    cluster
                ))
            }
            sys::MOQ_EVENT_NS_SUB_UPDATE_RESULT => {
                rd.floor(sys::MOQ_NS_SUB_UPDATE_RESULT_EVENT_V0_SIZE)?;
                let d = u.ns_sub_update_result;
                Event::NamespaceSubscriptionUpdateResult(update_concluded!(
                    &rd,
                    d,
                    rd.ns_sub(d.handle),
                    None
                ))
            }
            sys::MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT => {
                rd.floor(sys::MOQ_SUBSCRIBE_TRACKS_UPDATE_RESULT_EVENT_V0_SIZE)?;
                let d = u.subscribe_tracks_update_result;
                Event::SubscribeTracksUpdateResult(update_concluded!(
                    &rd,
                    d,
                    rd.track_sub(d.handle),
                    None
                ))
            }
            sys::MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED => {
                rd.floor(sys::MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE as usize)?;
                let d = u.outgoing_subgroup_terminated;
                Event::OutgoingSubgroupTerminated(OutgoingSubgroupTerminated {
                    subgroup: rd.subgroup(d.subgroup),
                    subscription: rd.subscription_if_valid(d.sub),
                    publication: rd.publication_if_valid(d.pub_),
                    group_id: d.group_id,
                    subgroup_id: d.subgroup_id,
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                    cause: SubgroupTerminalCause::from_raw(d.cause),
                    _record: rec(),
                })
            }
            kind => Event::Unknown(UnknownEvent {
                kind,
                detail_size: size,
                _record: rec(),
            }),
        })
    }
}

fn admission_options(o: &sys::moq_subscription_admission_options_t) -> Result<AdmissionOptions> {
    super::raw::floor(
        o.struct_size,
        sys::MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE as usize,
    )?;
    let rendezvous = super::raw::covers(
        o.struct_size,
        field_end!(
            sys::moq_subscription_admission_options_t,
            rendezvous_timeout_ms,
            u64
        ),
    );
    Ok(AdmissionOptions {
        location: if o.flags & sys::MOQ_SUBSCRIPTION_ADMISSION_LOCATION != 0 {
            Some(view::location_filter(&o.location)?)
        } else {
            None
        },
        fill_requested: o.flags & sys::MOQ_SUBSCRIPTION_ADMISSION_FILL != 0,
        object_delivery_timeout_ms: optional(
            o.flags & sys::MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT != 0,
            o.object_delivery_timeout_ms,
        ),
        subgroup_delivery_timeout_ms: optional(
            o.flags & sys::MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT != 0,
            o.subgroup_delivery_timeout_ms,
        ),
        rendezvous_timeout_ms: optional(
            rendezvous && o.flags & sys::MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT != 0,
            o.rendezvous_timeout_ms,
        ),
    })
}

fn fetch_options(o: &sys::moq_fetch_request_options_t) -> Result<FetchRequestOptions> {
    super::raw::floor(
        o.struct_size,
        sys::MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE as usize,
    )?;
    let unresolved = super::raw::covers(
        o.struct_size,
        field_end!(sys::moq_fetch_request_options_t, range_unresolved, bool),
    );
    Ok(FetchRequestOptions {
        location: view::location_filter(&o.location)?,
        fill_timeout_ms: optional(
            o.flags & sys::MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT != 0,
            o.fill_timeout_ms,
        ),
        include_properties: o.flags & sys::MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES != 0,
        range_unresolved: unresolved.then_some(o.range_unresolved),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_codes_round_trip() {
        for (index, kind) in EventKind::known().enumerate() {
            let code = u32::try_from(index + 1).expect("66 kinds");
            assert_eq!(kind.code(), code, "kinds are listed in code order");
            assert_eq!(EventKind::from_code(code), kind);
        }
        assert_eq!(EventKind::from_code(999), EventKind::Other(999));
        assert_eq!(EventKind::Other(999).code(), 999);
    }

    #[test]
    fn an_unknown_kind_is_kept() {
        // SAFETY: zero is a valid record; an unknown kind owns nothing.
        let mut raw: sys::moq_event_t = unsafe { std::mem::zeroed() };
        raw.kind = 4242;
        raw.detail_size = 12;
        let owner = OwnerId::next();
        // SAFETY: a record with no borrowed span or owned reference.
        let event = unsafe { decode(&mut raw, owner) }.expect("an unknown kind decodes");
        assert_eq!(event.kind(), EventKind::Other(4242));
        match event {
            Event::Unknown(unknown) => assert_eq!(unknown.detail_size, 12),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    /// A SUBSCRIBE_REQUEST whose admission options a producer of `stamp`
    /// bytes wrote: followed in memory by bytes a too-long read would take,
    /// or (`exact`) in an allocation of exactly `stamp` bytes.
    fn subscribe_request(
        stamp: usize,
        flags: u32,
        exact: bool,
    ) -> Result<Option<AdmissionOptions>> {
        // SAFETY: zero is a valid record.
        let mut options: sys::moq_subscription_admission_options_t = unsafe { std::mem::zeroed() };
        options.flags = flags;
        options.object_delivery_timeout_ms = 5;
        options.rendezvous_timeout_ms = 7;
        let words = (std::mem::size_of_val(&options) + 64) / 8;
        let mut padded = vec![u64::MAX; words];
        let take = stamp.min(std::mem::size_of_val(&options));
        // SAFETY: `padded` holds more than the record; they do not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(&options).cast::<u8>(),
                padded.as_mut_ptr().cast::<u8>(),
                take,
            );
            padded
                .as_mut_ptr()
                .cast::<u32>()
                .write(u32::try_from(stamp).expect("small"));
        }
        let sized = crate::session::view::Exact::record(&options, stamp, stamp);
        // SAFETY: zero is a valid record.
        let mut raw: sys::moq_event_t = unsafe { std::mem::zeroed() };
        raw.kind = sys::MOQ_EVENT_SUBSCRIBE_REQUEST;
        raw.detail_size = u32::try_from(std::mem::size_of::<sys::moq_subscribe_request_event_t>())
            .expect("a small record");
        raw.u.subscribe_request.options = if exact {
            sized.as_ptr()
        } else {
            padded.as_ptr().cast()
        };
        // SAFETY: the record borrows `padded` or `sized`, both alive past
        // the decode, and no session span; it owns no reference.
        let event = unsafe { decode(&mut raw, OwnerId::next()) }?;
        let Event::SubscribeRequest(request) = event else {
            panic!("expected SubscribeRequest");
        };
        Ok(request.options)
    }

    #[test]
    fn nested_options_of_an_older_producer() {
        let frozen = sys::MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE as usize;
        let full = std::mem::size_of::<sys::moq_subscription_admission_options_t>();
        let flags = sys::MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT
            | sys::MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT;
        for exact in [false, true] {
            // The frozen prefix: the appended rendezvous timeout is absent
            // even though its flag is set; nothing past the record is read.
            let old = subscribe_request(frozen, flags, exact)
                .expect("decodes")
                .expect("options");
            assert_eq!(old.object_delivery_timeout_ms, Some(5), "exact {exact}");
            assert_eq!(old.rendezvous_timeout_ms, None, "exact {exact}");
            // A stamp ending inside the appended field: still absent.
            let torn = subscribe_request(full - 1, flags, exact)
                .expect("decodes")
                .expect("options");
            assert_eq!(torn.rendezvous_timeout_ms, None, "exact {exact}");
            // The current record, and a longer future one.
            for stamp in [full, full + 16] {
                let now = subscribe_request(stamp, flags, exact)
                    .expect("decodes")
                    .expect("options");
                assert_eq!(
                    now.rendezvous_timeout_ms,
                    Some(7),
                    "exact {exact} stamp {stamp}"
                );
            }
            // Below the frozen prefix the record is refused whole.
            assert_eq!(
                subscribe_request(frozen - 8, flags, exact).err(),
                Some(Error::ABI_MISMATCH),
                "exact {exact}"
            );
            // An unknown flag is ignored, as for the current record, and in
            // the frozen prefix too.
            for stamp in [frozen, full] {
                let unknown =
                    subscribe_request(stamp, flags | 0x8000_0000, exact).expect("decodes");
                assert!(unknown.is_some(), "exact {exact} stamp {stamp}");
            }
        }
    }

    #[test]
    fn a_short_record_is_refused() {
        // SAFETY: zero is a valid record.
        let mut raw: sys::moq_event_t = unsafe { std::mem::zeroed() };
        raw.kind = sys::MOQ_EVENT_SUBSCRIBE_OK;
        raw.detail_size = 8;
        // SAFETY: a record with no borrowed span or owned reference.
        let result = unsafe { decode(&mut raw, OwnerId::next()) };
        assert_eq!(
            result.err().map(|e| e.code()),
            Some(Error::ABI_MISMATCH.code())
        );
    }
}
