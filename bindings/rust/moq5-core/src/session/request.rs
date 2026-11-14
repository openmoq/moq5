//! Requests: subscribe, publish, fetch, namespaces, GOAWAY, and their
//! answers. Every configuration is built with its sized initializer and
//! borrowed by C for the call only.

use std::ptr;
use std::time::Duration;

use super::custody::WriteError;
use super::event::{FetchRange, Joining, NamespaceInterest, SubscribeFilter};
use super::handle::{
    checked, Announcement, Fetch, NamespaceSubscription, Publication, Subgroup, Subscription,
};
use super::owner::Session;
use super::raw;
use super::time::Instant;
use crate::{
    sys, Buffer, Error, GroupOrder, Location, LocationFilter, Namespace, ObjectStatus, Result,
};

/// An authorization token sent by value with a request.
///
/// Closed: a token by value is its type and its value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthToken {
    /// The token type; draft 16 carries at most `2^62-1`.
    pub token_type: u64,
    /// The token value.
    pub value: Vec<u8>,
}

pub(super) fn tokens_raw(tokens: &[AuthToken]) -> Vec<sys::moq_auth_token_t> {
    tokens
        .iter()
        .map(|token| sys::moq_auth_token_t {
            token_type: token.token_type,
            token_value: raw::span(&token.value),
        })
        .collect()
}

pub(super) fn group_order_raw(order: Option<GroupOrder>) -> u32 {
    match order {
        None => sys::MOQ_GROUP_ORDER_DEFAULT,
        Some(GroupOrder::Ascending) => sys::MOQ_GROUP_ORDER_ASCENDING,
        Some(GroupOrder::Descending) => sys::MOQ_GROUP_ORDER_DESCENDING,
    }
}

/// Initialize a native configuration with its sized initializer.
macro_rules! sized {
    ($init:ident, $ty:ty) => {{
        // SAFETY: zero is a valid bit pattern for this plain C record (its
        // pointers become null); the initializer clears and stamps it.
        let mut cfg: $ty = unsafe { ::std::mem::zeroed() };
        // SAFETY: a writable record of exactly the size passed.
        unsafe { $crate::sys::$init(&mut cfg, ::std::mem::size_of::<$ty>()) };
        cfg
    }};
}
pub(super) use sized;

/// The pointer of a token list as C takes it: null for none.
pub(super) fn tokens_ptr(tokens: &[sys::moq_auth_token_t]) -> *const sys::moq_auth_token_t {
    if tokens.is_empty() {
        ptr::null()
    } else {
        tokens.as_ptr()
    }
}

/// Where a REDIRECT refusal sends the requester (`moq_redirect_target_t`).
/// The fields go on the wire exactly as given; the session never fills one
/// in. An empty `connect_uri` is the current session's URI on both drafts.
/// The namespace and name differ by draft:
///
/// - Draft 22 (§9.4.1): both are literal. An empty namespace is the root
///   and an empty name is the empty name, whatever the original request
///   named.
/// - Draft 18 (§10.6.1): when both are empty, the redirected request uses
///   the original request's namespace and name; otherwise both are the
///   literal values.
///
/// A client leaves `connect_uri` empty; namespace-scoped requests leave
/// `track_name` empty. A received target is reported as received
/// ([`RequestRedirect::target`](crate::RequestRedirect::target)).
///
/// Closed: the Redirect structure has exactly these three fields (draft 18
/// §10.6.1, draft 22 §9.4.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedirectTarget {
    /// The session URI to re-issue the request at; empty is the current one.
    pub connect_uri: Vec<u8>,
    /// The namespace to use, as given.
    pub namespace: Namespace,
    /// The track name to use, as given.
    pub track_name: Vec<u8>,
}

impl RedirectTarget {
    /// The all-empty target: the current session's URI, the root namespace
    /// and the empty name, sent literally. On draft 18 it means the
    /// original request's namespace and name; on draft 22 it names the root
    /// and the empty name. To send a request to its own track on draft 22,
    /// name that track.
    #[must_use]
    pub fn empty() -> RedirectTarget {
        RedirectTarget {
            connect_uri: Vec::new(),
            namespace: Namespace::root(),
            track_name: Vec::new(),
        }
    }

    /// Whether every field is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.connect_uri.is_empty()
            && self.namespace == Namespace::root()
            && self.track_name.is_empty()
    }
}

/// The native target, borrowing `target` for the call.
pub(super) fn redirect_raw(target: &RedirectTarget) -> sys::moq_redirect_target_t {
    sys::moq_redirect_target_t {
        connect_uri: raw::span(&target.connect_uri),
        track_namespace: target.namespace.as_raw(),
        track_name: raw::span(&target.track_name),
    }
}

/// Cluster draft-01 namespace metadata this side sends
/// (`moq_cluster_metadata_t`): the Hop IDs from the original publisher to
/// this side, and the route cost when present. In an update, an empty path
/// and an absent cost leave the prior values.
///
/// Closed: cluster draft-01 metadata is a Hop ID path and a route cost.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterPath {
    /// The Hop IDs, in path order.
    pub hop_ids: Vec<u64>,
    /// The route cost, when present.
    pub route_cost: Option<u64>,
}

/// The native metadata, borrowing `path` for the call.
pub(super) fn cluster_raw(path: &ClusterPath) -> sys::moq_cluster_metadata_t {
    sys::moq_cluster_metadata_t {
        hop_ids: if path.hop_ids.is_empty() {
            ptr::null()
        } else {
            path.hop_ids.as_ptr()
        },
        hop_count: path.hop_ids.len(),
        has_route_cost: path.route_cost.is_some(),
        route_cost: path.route_cost.unwrap_or(0),
    }
}

/// One fill request carried on a SUBSCRIBE or an update (`moq_fill_cfg_t`,
/// draft 22). Every field left unset inherits the subscription's effective
/// state when the request is processed.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FillConfig {
    /// The fill's subscriber priority.
    pub subscriber_priority: Option<u8>,
    /// The fill's group order.
    pub group_order: Option<GroupOrder>,
    /// The most to wait for objects not immediately available, in
    /// milliseconds; zero takes only what is available at once. Unset uses
    /// the implementation's own wait.
    pub fill_timeout_ms: Option<u64>,
    /// The fill's Location Filter (`Unconstrained` removes the constraint).
    pub location: Option<LocationFilter>,
}

/// The native fill request, borrowing `location` (the caller's copy of
/// `fill.location` as C takes it) for the call.
pub(super) fn fill_raw(
    fill: &FillConfig,
    location: Option<&sys::moq_location_filter_cfg_t>,
) -> sys::moq_fill_cfg_t {
    let mut cfg = sized!(moq_fill_cfg_init_sized, sys::moq_fill_cfg_t);
    if let Some(priority) = fill.subscriber_priority {
        cfg.has_subscriber_priority = true;
        cfg.subscriber_priority = priority;
    }
    if let Some(order) = fill.group_order {
        cfg.has_group_order = true;
        cfg.group_order = group_order_raw(Some(order));
    }
    if let Some(timeout) = fill.fill_timeout_ms {
        cfg.has_fill_timeout = true;
        cfg.fill_timeout_ms = timeout;
    }
    cfg.location = location.map_or(ptr::null(), |l| l as *const _);
    cfg
}

/// A refusal whose native configuration has no redirect target field: a
/// nonempty target is refused, not dropped.
pub(super) fn targetless(rejection: &Rejection) -> Result<()> {
    match &rejection.redirect {
        Some(target) if !target.is_empty() => Err(Error::INVAL),
        _ => Ok(()),
    }
}

/// A refusal whose native configuration has only a code and a reason: a
/// retry interval or a nonempty redirect target is refused, not dropped.
pub(super) fn plain_refusal(rejection: &Rejection) -> Result<()> {
    targetless(rejection)?;
    if rejection.retry_after_ms.is_some() {
        return Err(Error::INVAL);
    }
    Ok(())
}

/// The legacy filter fields of drafts 16 and 18: (kind, start, end group).
pub(super) fn legacy_filter_raw(filter: SubscribeFilter) -> (u32, Location, u64) {
    let zero = Location::default();
    match filter {
        SubscribeFilter::None => (sys::MOQ_SUBSCRIBE_FILTER_NONE, zero, 0),
        SubscribeFilter::NextGroup => (sys::MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, zero, 0),
        SubscribeFilter::LargestObject => (sys::MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, zero, 0),
        SubscribeFilter::AbsoluteStart { start } => {
            (sys::MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, start, 0)
        }
        SubscribeFilter::AbsoluteRange { start, end_group } => {
            (sys::MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, start, end_group)
        }
        SubscribeFilter::Other(code) => (code, zero, 0),
    }
}

/// A SUBSCRIBE this side makes.
#[derive(Clone, Debug)]
pub struct SubscribeConfig {
    namespace: Namespace,
    track_name: Vec<u8>,
    filter: LocationFilter,
    subscriber_priority: Option<u8>,
    group_order: Option<GroupOrder>,
    forward: Option<bool>,
    tokens: Vec<AuthToken>,
    new_group_request: Option<u64>,
    object_delivery_timeout: Option<Duration>,
    subgroup_delivery_timeout: Option<Duration>,
    fill: Option<FillConfig>,
    rendezvous_timeout: Option<Duration>,
    include_properties: Option<bool>,
}

impl SubscribeConfig {
    /// A subscription to the track `track_name` in `namespace`, covering
    /// what `filter` selects. A form the session's draft cannot carry is
    /// refused by [`Session::subscribe`] with [`Error::UNSUPPORTED`].
    #[must_use]
    pub fn new(
        namespace: Namespace,
        track_name: impl Into<Vec<u8>>,
        filter: LocationFilter,
    ) -> SubscribeConfig {
        SubscribeConfig {
            namespace,
            track_name: track_name.into(),
            filter,
            subscriber_priority: None,
            group_order: None,
            forward: None,
            tokens: Vec::new(),
            new_group_request: None,
            object_delivery_timeout: None,
            subgroup_delivery_timeout: None,
            fill: None,
            rendezvous_timeout: None,
            include_properties: None,
        }
    }

    /// The subscriber priority (default 128).
    #[must_use]
    pub fn subscriber_priority(mut self, priority: u8) -> SubscribeConfig {
        self.subscriber_priority = Some(priority);
        self
    }

    /// The group order; unset uses the publisher's.
    #[must_use]
    pub fn group_order(mut self, order: GroupOrder) -> SubscribeConfig {
        self.group_order = Some(order);
        self
    }

    /// The forward state (default true).
    #[must_use]
    pub fn forward(mut self, forward: bool) -> SubscribeConfig {
        self.forward = Some(forward);
        self
    }

    /// Add an authorization token.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> SubscribeConfig {
        self.tokens.push(token);
        self
    }

    /// Ask the original publisher to start a new group (0 is "no group
    /// info").
    #[must_use]
    pub fn new_group_request(mut self, group: u64) -> SubscribeConfig {
        self.new_group_request = Some(group);
        self
    }

    /// The object delivery timeout, a whole number of milliseconds. Draft
    /// 16 has one timeout: set both to the same nonzero value there.
    #[must_use]
    pub fn object_delivery_timeout(mut self, timeout: Duration) -> SubscribeConfig {
        self.object_delivery_timeout = Some(timeout);
        self
    }

    /// The subgroup delivery timeout, a whole number of milliseconds.
    #[must_use]
    pub fn subgroup_delivery_timeout(mut self, timeout: Duration) -> SubscribeConfig {
        self.subgroup_delivery_timeout = Some(timeout);
        self
    }

    /// Carry one fill request on the SUBSCRIBE (draft 22, see
    /// [`Session::supports_fill`]); elsewhere [`Session::subscribe`] refuses
    /// it with [`Error::UNSUPPORTED`].
    #[must_use]
    pub fn fill(mut self, fill: FillConfig) -> SubscribeConfig {
        self.fill = Some(fill);
        self
    }

    /// The rendezvous timeout, a whole number of milliseconds (draft 22;
    /// [`Error::UNSUPPORTED`] elsewhere, zero included).
    #[must_use]
    pub fn rendezvous_timeout(mut self, timeout: Duration) -> SubscribeConfig {
        self.rendezvous_timeout = Some(timeout);
        self
    }

    /// INCLUDE_PROPERTIES (draft 22): `false` asks for an answer with empty
    /// Track Properties. [`Error::UNSUPPORTED`] on drafts 16 and 18.
    #[must_use]
    pub fn include_properties(mut self, include: bool) -> SubscribeConfig {
        self.include_properties = Some(include);
        self
    }
}

/// How this side accepts a subscription. Every field left unset keeps its
/// native meaning: the session chooses the alias, no Largest, no EXPIRES, no
/// Track Properties.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct AcceptSubscribe {
    /// The track alias; unset, the session chooses one.
    pub track_alias: Option<u64>,
    /// The largest object published.
    pub largest: Option<Location>,
    /// EXPIRES, in milliseconds.
    pub expires_ms: Option<u64>,
    /// Track Properties, encoded for the session's draft.
    pub track_properties: Vec<u8>,
}

/// A refusal of a peer's request: an error code, a reason and an optional
/// retry interval.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Rejection {
    /// The request error code (`MOQ_REQUEST_ERROR_*`, as the draft
    /// registers them).
    pub error_code: u64,
    /// The reason phrase.
    pub reason: Vec<u8>,
    /// The retry interval, in milliseconds, when a retry is allowed.
    pub retry_after_ms: Option<u64>,
    /// With `MOQ_REQUEST_ERROR_REDIRECT` (drafts 18 and 22): where the
    /// requester re-issues the request; any other code with a nonempty
    /// target is [`Error::INVAL`]. The native refusals of SUBSCRIBE, FETCH,
    /// TRACK_STATUS and the two namespace requests have a target field. The
    /// native PUBLISH and SUBSCRIBE_TRACKS refusals have none yet, though
    /// the wire can carry one: on draft 22, where they may be REDIRECT,
    /// they send the empty target, and a nonempty one is [`Error::INVAL`]
    /// before anything is sent.
    pub redirect: Option<RedirectTarget>,
}

impl Rejection {
    /// A refusal with `error_code` and no reason.
    #[must_use]
    pub fn new(error_code: u64) -> Rejection {
        Rejection {
            error_code,
            reason: Vec::new(),
            retry_after_ms: None,
            redirect: None,
        }
    }

    /// A REDIRECT refusal (`MOQ_REQUEST_ERROR_REDIRECT`) to `target`.
    #[must_use]
    pub fn redirect(target: RedirectTarget) -> Rejection {
        Rejection {
            redirect: Some(target),
            ..Rejection::new(sys::MOQ_REQUEST_ERROR_REDIRECT as u64)
        }
    }

    /// With a retry interval, in milliseconds.
    #[must_use]
    pub fn retry_after_ms(mut self, retry_after_ms: u64) -> Rejection {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }

    /// With a reason phrase.
    #[must_use]
    pub fn reason(mut self, reason: impl Into<Vec<u8>>) -> Rejection {
        self.reason = reason.into();
        self
    }
}

/// The publisher's end of a track: SUBSCRIBE_DONE or PUBLISH_DONE.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TrackDoneConfig {
    /// The status code.
    pub status_code: u64,
    /// The data streams the application opened for the track. Ignored in
    /// the tagged mode, where the session counts the opens the host
    /// reported.
    pub stream_count: u64,
    /// The reason phrase.
    pub reason: Vec<u8>,
}

impl TrackDoneConfig {
    /// A done with `status_code`, no reason and no stream count.
    #[must_use]
    pub fn new(status_code: u64) -> TrackDoneConfig {
        TrackDoneConfig {
            status_code,
            stream_count: 0,
            reason: Vec::new(),
        }
    }

    /// With the data streams the application opened for the track.
    #[must_use]
    pub fn stream_count(mut self, count: u64) -> TrackDoneConfig {
        self.stream_count = count;
        self
    }

    /// With a reason phrase.
    #[must_use]
    pub fn reason(mut self, reason: impl Into<Vec<u8>>) -> TrackDoneConfig {
        self.reason = reason.into();
        self
    }
}

/// A subscriber's update of a subscription (or, with
/// [`Session::update_publication`], of a publication) this side accepted or
/// requested. Every field left unset is not carried.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SubscriptionUpdate {
    /// A new subscriber priority.
    pub subscriber_priority: Option<u8>,
    /// A new forward state.
    pub forward: Option<bool>,
    /// A new-group request (0 is "no group info").
    pub new_group_request: Option<u64>,
    /// Authorization tokens carried on the update.
    pub tokens: Vec<AuthToken>,
    /// The single delivery timeout of drafts 16 and 18, in microseconds;
    /// [`Error::UNSUPPORTED`] on draft 22, which carries the two below.
    pub delivery_timeout_us: Option<u64>,
    /// A replacement filter in the form drafts 16 and 18 carry (subscriptions
    /// only; an update of a publication with one is [`Error::INVAL`]).
    pub filter: Option<SubscribeFilter>,
    /// A replacement Location Filter (draft 22; `Unconstrained` removes
    /// the restriction).
    pub location: Option<LocationFilter>,
    /// One fill request (draft 22).
    pub fill: Option<FillConfig>,
    /// The object delivery timeout, exact milliseconds (zero included).
    /// Draft 16 has one timeout: set both, equal and nonzero, there.
    pub object_delivery_timeout_ms: Option<u64>,
    /// The subgroup delivery timeout, exact milliseconds.
    pub subgroup_delivery_timeout_ms: Option<u64>,
}

/// An outgoing subgroup: one data stream.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub struct SubgroupConfig {
    /// The group.
    pub group_id: u64,
    /// The subgroup within it.
    pub subgroup_id: u64,
    /// The publisher priority.
    pub publisher_priority: u8,
    /// Every object carries a Properties field.
    pub object_properties: bool,
    /// The stream carries the group's last object.
    pub end_of_group: bool,
    /// The first object written is the first ever published in the
    /// subgroup (drafts 18 and 22; an original publisher sets it).
    pub first_object: bool,
}

impl SubgroupConfig {
    /// Subgroup 0 of `group_id`, priority 0, an original publisher's first
    /// object.
    #[must_use]
    pub fn new(group_id: u64) -> SubgroupConfig {
        SubgroupConfig {
            group_id,
            subgroup_id: 0,
            publisher_priority: 0,
            object_properties: false,
            end_of_group: false,
            first_object: true,
        }
    }
}

/// A PUBLISH this side makes (`moq_publish_cfg_t`). The draft-22
/// selections ([`expires_ms`](PublishConfig::expires_ms) and after) are
/// refused with [`Error::UNSUPPORTED`] on drafts 16 and 18, before anything
/// is sent.
#[derive(Clone, Debug)]
pub struct PublishConfig {
    namespace: Namespace,
    track_name: Vec<u8>,
    track_alias: Option<u64>,
    forward: Option<bool>,
    track_properties: Vec<u8>,
    tokens: Vec<AuthToken>,
    expires_ms: Option<u64>,
    subscriber_priority: Option<u8>,
    group_order: Option<GroupOrder>,
    location: Option<LocationFilter>,
    object_delivery_timeout_ms: Option<u64>,
    subgroup_delivery_timeout_ms: Option<u64>,
}

impl PublishConfig {
    /// A publication of the track `track_name` in `namespace`.
    #[must_use]
    pub fn new(namespace: Namespace, track_name: impl Into<Vec<u8>>) -> PublishConfig {
        PublishConfig {
            namespace,
            track_name: track_name.into(),
            track_alias: None,
            forward: None,
            track_properties: Vec::new(),
            tokens: Vec::new(),
            expires_ms: None,
            subscriber_priority: None,
            group_order: None,
            location: None,
            object_delivery_timeout_ms: None,
            subgroup_delivery_timeout_ms: None,
        }
    }

    /// EXPIRES, in milliseconds (draft 22; zero is a value, not omission).
    #[must_use]
    pub fn expires_ms(mut self, expires: u64) -> PublishConfig {
        self.expires_ms = Some(expires);
        self
    }

    /// The initial subscriber priority (draft 22; unset, the session sends
    /// its default, 128).
    #[must_use]
    pub fn subscriber_priority(mut self, priority: u8) -> PublishConfig {
        self.subscriber_priority = Some(priority);
        self
    }

    /// The initial group order (draft 22; unset, the session sends the
    /// publisher's preference).
    #[must_use]
    pub fn group_order(mut self, order: GroupOrder) -> PublishConfig {
        self.group_order = Some(order);
        self
    }

    /// The initial Location Filter (draft 22; unset, none is sent;
    /// `Unconstrained` is sent as an explicit NONE).
    #[must_use]
    pub fn location(mut self, filter: LocationFilter) -> PublishConfig {
        self.location = Some(filter);
        self
    }

    /// The object delivery timeout, exact milliseconds (draft 22;
    /// independent of any Track Property timeout).
    #[must_use]
    pub fn object_delivery_timeout_ms(mut self, timeout: u64) -> PublishConfig {
        self.object_delivery_timeout_ms = Some(timeout);
        self
    }

    /// The subgroup delivery timeout, exact milliseconds (draft 22).
    #[must_use]
    pub fn subgroup_delivery_timeout_ms(mut self, timeout: u64) -> PublishConfig {
        self.subgroup_delivery_timeout_ms = Some(timeout);
        self
    }

    /// The track alias; unset, the session chooses one.
    #[must_use]
    pub fn track_alias(mut self, alias: u64) -> PublishConfig {
        self.track_alias = Some(alias);
        self
    }

    /// The initial forward state.
    #[must_use]
    pub fn forward(mut self, forward: bool) -> PublishConfig {
        self.forward = Some(forward);
        self
    }

    /// Track Properties, encoded for the session's draft.
    #[must_use]
    pub fn track_properties(mut self, properties: impl Into<Vec<u8>>) -> PublishConfig {
        self.track_properties = properties.into();
        self
    }

    /// Add an authorization token.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> PublishConfig {
        self.tokens.push(token);
        self
    }
}

/// A namespace subscription this side makes (`moq_subscribe_namespace_cfg_t`).
#[derive(Clone, Debug)]
pub struct SubscribeNamespaceConfig {
    prefix: Namespace,
    interest: NamespaceInterest,
    tokens: Vec<AuthToken>,
}

impl SubscribeNamespaceConfig {
    /// Namespaces under `prefix`, with `interest`.
    #[must_use]
    pub fn new(prefix: Namespace, interest: NamespaceInterest) -> SubscribeNamespaceConfig {
        SubscribeNamespaceConfig {
            prefix,
            interest,
            tokens: Vec::new(),
        }
    }

    /// Add an authorization token, sent by value with the request.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> SubscribeNamespaceConfig {
        self.tokens.push(token);
        self
    }
}

/// How this side accepts a peer's PUBLISH (`moq_accept_publish_cfg_t`).
/// Draft 22's answer carries only EXPIRES: the draft-16/18 overrides (priority,
/// group order, forward, filter, new-group request, delivery timeouts) are
/// [`Error::INVAL`] there, and EXPIRES is [`Error::UNSUPPORTED`] on drafts
/// 16 and 18.
///
/// A selection left unset sends nothing of its own. What takes effect
/// then depends on the selection and the draft:
///
/// - Forward: on drafts 16 and 18 an answer without Forward means
///   forwarding, so an acceptance without [`forward`](AcceptPublish::forward)
///   forwards even a PUBLISH that asked to be paused; set `forward(false)`
///   to keep it paused. On draft 22 the PUBLISH's own forward state stands.
/// - Subscriber priority: 128 on drafts 16 and 18.
/// - Group order, filter and delivery timeouts: the PUBLISH's own.
/// - New-group request and EXPIRES: none.
#[derive(Clone, Copy, Debug, Default)]
pub struct AcceptPublish {
    subscriber_priority: Option<u8>,
    group_order: Option<GroupOrder>,
    forward: Option<bool>,
    new_group_request: Option<u64>,
    filter: Option<SubscribeFilter>,
    expires_ms: Option<u64>,
    object_delivery_timeout_ms: Option<u64>,
    subgroup_delivery_timeout_ms: Option<u64>,
}

impl AcceptPublish {
    /// An acceptance with no selection of its own: see the type for what
    /// takes effect (on drafts 16 and 18 it forwards, whatever the
    /// PUBLISH asked).
    #[must_use]
    pub fn new() -> AcceptPublish {
        AcceptPublish::default()
    }

    /// A subscriber priority (drafts 16 and 18).
    #[must_use]
    pub fn subscriber_priority(mut self, priority: u8) -> AcceptPublish {
        self.subscriber_priority = Some(priority);
        self
    }

    /// A group order (drafts 16 and 18).
    #[must_use]
    pub fn group_order(mut self, order: GroupOrder) -> AcceptPublish {
        self.group_order = Some(order);
        self
    }

    /// A forward state (drafts 16 and 18).
    #[must_use]
    pub fn forward(mut self, forward: bool) -> AcceptPublish {
        self.forward = Some(forward);
        self
    }

    /// Ask the original publisher to start a new group (0 is "no group
    /// info"; drafts 16 and 18, and only when the PUBLISH carried dynamic
    /// groups).
    #[must_use]
    pub fn new_group_request(mut self, group: u64) -> AcceptPublish {
        self.new_group_request = Some(group);
        self
    }

    /// The subscription filter this side chooses, in the form drafts 16 and
    /// 18 carry.
    #[must_use]
    pub fn filter(mut self, filter: SubscribeFilter) -> AcceptPublish {
        self.filter = Some(filter);
        self
    }

    /// EXPIRES, in milliseconds (draft 22).
    #[must_use]
    pub fn expires_ms(mut self, expires: u64) -> AcceptPublish {
        self.expires_ms = Some(expires);
        self
    }

    /// The object delivery timeout, exact milliseconds (drafts 16 and 18;
    /// draft 16 has one timeout: set both, equal and nonzero).
    #[must_use]
    pub fn object_delivery_timeout_ms(mut self, timeout: u64) -> AcceptPublish {
        self.object_delivery_timeout_ms = Some(timeout);
        self
    }

    /// The subgroup delivery timeout, exact milliseconds (drafts 16 and 18).
    #[must_use]
    pub fn subgroup_delivery_timeout_ms(mut self, timeout: u64) -> AcceptPublish {
        self.subgroup_delivery_timeout_ms = Some(timeout);
        self
    }
}

/// A standalone FETCH this side makes. The range is
/// `[start, (end_group, end_object))`; `end_object` 0 is the whole
/// `end_group`.
#[derive(Clone, Debug)]
pub struct FetchConfig {
    namespace: Namespace,
    track_name: Vec<u8>,
    start: Location,
    end_group: u64,
    end_object: u64,
    subscriber_priority: Option<u8>,
    group_order: Option<GroupOrder>,
    tokens: Vec<AuthToken>,
    location: Option<LocationFilter>,
    joining: Option<(Joining, JoiningStart)>,
    fill_timeout_ms: Option<u64>,
    include_properties: Option<bool>,
}

/// Where a joining FETCH starts (drafts 16 and 18; see
/// [`Session::supports_joining_fetch`]). A relative start before group zero
/// is group zero.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoiningStart {
    /// This many groups before the joined subscription's current group.
    Relative(u64),
    /// At this group.
    Absolute(u64),
}

impl FetchConfig {
    /// A fetch of the track `track_name` in `namespace` over the range.
    #[must_use]
    pub fn new(
        namespace: Namespace,
        track_name: impl Into<Vec<u8>>,
        start: Location,
        end_group: u64,
        end_object: u64,
    ) -> FetchConfig {
        FetchConfig {
            namespace,
            track_name: track_name.into(),
            start,
            end_group,
            end_object,
            subscriber_priority: None,
            group_order: None,
            tokens: Vec::new(),
            location: None,
            joining: None,
            fill_timeout_ms: None,
            include_properties: None,
        }
    }

    /// A joining FETCH of the track `joined` carries, from `start` up to
    /// the joined subscription's starting point. A SUBSCRIBE-initiated
    /// subscription must use the Largest Object filter
    /// ([`LocationFilter::NextObject`]) and hold a Largest; a
    /// PUBLISH-initiated one must be established and eligible under the
    /// draft's rule.
    #[must_use]
    pub fn joining(joined: Joining, start: JoiningStart) -> FetchConfig {
        FetchConfig {
            joining: Some((joined, start)),
            ..FetchConfig::new(Namespace::root(), Vec::new(), Location::default(), 0, 0)
        }
    }

    /// The subscriber priority.
    #[must_use]
    pub fn subscriber_priority(mut self, priority: u8) -> FetchConfig {
        self.subscriber_priority = Some(priority);
        self
    }

    /// The group order; unset uses the publisher's.
    #[must_use]
    pub fn group_order(mut self, order: GroupOrder) -> FetchConfig {
        self.group_order = Some(order);
        self
    }

    /// Add an authorization token.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> FetchConfig {
        self.tokens.push(token);
        self
    }

    /// The range in its semantic form instead of the explicit one, on a
    /// draft whose FETCH carries a Location Filter (draft 22); elsewhere
    /// [`Session::fetch`] refuses it with [`Error::UNSUPPORTED`].
    #[must_use]
    pub fn location(mut self, filter: LocationFilter) -> FetchConfig {
        self.location = Some(filter);
        self
    }

    /// FILL_TIMEOUT, in milliseconds: the most a relay waits for objects
    /// not immediately available (zero: only what is available at once).
    /// Draft 22 only, standalone fetches only.
    #[must_use]
    pub fn fill_timeout_ms(mut self, timeout: u64) -> FetchConfig {
        self.fill_timeout_ms = Some(timeout);
        self
    }

    /// INCLUDE_PROPERTIES (draft 22, standalone fetches only).
    #[must_use]
    pub fn include_properties(mut self, include: bool) -> FetchConfig {
        self.include_properties = Some(include);
        self
    }
}

/// How this side accepts a FETCH: the end of the range served, in the
/// FETCH's own form (`end_object` exclusive, 0 the whole `end_group`).
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct AcceptFetch {
    /// The end group served.
    pub end_group: u64,
    /// The end object served, exclusive.
    pub end_object: u64,
    /// The range reaches the end of the track.
    pub end_of_track: bool,
    /// Accept with no objects: the response stream is opened with its FIN
    /// and the fetch is finished.
    pub empty: bool,
    /// Track Properties, encoded for the session's draft.
    pub track_properties: Vec<u8>,
}

impl AcceptFetch {
    /// An acceptance serving through `(end_group, end_object)`.
    #[must_use]
    pub fn new(end_group: u64, end_object: u64) -> AcceptFetch {
        AcceptFetch {
            end_group,
            end_object,
            end_of_track: false,
            empty: false,
            track_properties: Vec::new(),
        }
    }
}

/// One object written on an accepted FETCH's response stream.
#[non_exhaustive]
#[derive(Debug)]
pub struct FetchObjectWrite {
    /// The group.
    pub group_id: u64,
    /// The subgroup (ignored with `datagram`).
    pub subgroup_id: u64,
    /// The object.
    pub object_id: u64,
    /// The publisher priority.
    pub publisher_priority: u8,
    /// Datagram forwarding preference (drafts 18 and 22).
    pub datagram: bool,
    /// The payload.
    pub payload: Buffer,
    /// The object properties, encoded for the draft.
    pub properties: Option<Buffer>,
}

impl FetchObjectWrite {
    /// Object `object_id` of group `group_id` (subgroup zero, publisher
    /// priority 128, no properties) with `payload`.
    #[must_use]
    pub fn new(group_id: u64, object_id: u64, payload: Buffer) -> FetchObjectWrite {
        FetchObjectWrite {
            group_id,
            subgroup_id: 0,
            object_id,
            publisher_priority: 128,
            datagram: false,
            payload,
            properties: None,
        }
    }
}

/// One whole object with what [`Session::write_object`] leaves out
/// (`moq_object_cfg_t`).
#[non_exhaustive]
#[derive(Debug)]
pub struct ObjectWrite {
    /// The object.
    pub object_id: u64,
    /// The payload.
    pub payload: Buffer,
    /// The object properties, encoded for the draft (the subgroup must be
    /// opened with [`SubgroupConfig::object_properties`]).
    pub properties: Option<Buffer>,
    /// Prior Group ID Gap (drafts 18 and 22).
    pub prior_group_gap: Option<u64>,
    /// Prior Object ID Gap (drafts 18 and 22).
    pub prior_object_gap: Option<u64>,
}

impl ObjectWrite {
    /// Object `object_id` with `payload` and nothing else.
    #[must_use]
    pub fn new(object_id: u64, payload: Buffer) -> ObjectWrite {
        ObjectWrite {
            object_id,
            payload,
            properties: None,
            prior_group_gap: None,
            prior_object_gap: None,
        }
    }

    /// With object properties.
    #[must_use]
    pub fn properties(mut self, properties: Buffer) -> ObjectWrite {
        self.properties = Some(properties);
        self
    }
}

/// Which refusals of a write the native call states retain no reference
/// to the caller's buffers.
#[derive(Clone, Copy, Debug)]
pub(super) enum Kept {
    /// Every refusal.
    EveryRefusal,
    /// `MOQ_ERR_WOULD_BLOCK` only.
    WouldBlock,
}

/// Whether `buffer` is still the caller's alone.
fn exclusive(buffer: &Buffer) -> bool {
    // SAFETY: a live rcbuf the caller holds.
    unsafe { sys::moq_rcbuf_refcount(buffer.as_ptr()) == 1 }
}

impl Session {
    pub(super) fn check<H: super::handle::Handle>(&self, handle: H) -> Result<u64> {
        checked(handle, self.owner)
    }

    // -- Subscribe --------------------------------------------------------

    /// Subscribe (`moq_session_subscribe`).
    ///
    /// # Errors
    /// [`Error::REQUEST_BLOCKED`] without request capacity; [`Error::INVAL`]
    /// for a request the draft cannot carry (draft 16: a root namespace, a
    /// second subscription to a track); [`Error::UNSUPPORTED`] for a filter
    /// form the draft has no encoding for; [`Error::WOULD_BLOCK`].
    pub fn subscribe(&mut self, config: &SubscribeConfig, now: Instant) -> Result<Subscription> {
        let tokens = tokens_raw(&config.tokens);
        let location = config.filter.as_raw();
        let mut cfg = sized!(moq_subscribe_cfg_init_sized, sys::moq_subscribe_cfg_t);
        cfg.track_namespace = config.namespace.as_raw();
        cfg.track_name = raw::span(&config.track_name);
        // The semantic form replaces the legacy filter fields entirely.
        cfg.location = &location;
        if let Some(priority) = config.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        cfg.group_order = group_order_raw(config.group_order);
        if let Some(forward) = config.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        cfg.auth_tokens = if tokens.is_empty() {
            ptr::null()
        } else {
            tokens.as_ptr()
        };
        cfg.auth_token_count = tokens.len();
        if let Some(group) = config.new_group_request {
            cfg.has_new_group_request = true;
            cfg.new_group_request = group;
        }
        if let Some(timeout) = config.object_delivery_timeout {
            cfg.delivery_timeout_flags |= sys::MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
            cfg.object_delivery_timeout_ms = raw::millis(timeout)?;
        }
        if let Some(timeout) = config.subgroup_delivery_timeout {
            cfg.delivery_timeout_flags |= sys::MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
            cfg.subgroup_delivery_timeout_ms = raw::millis(timeout)?;
        }
        let fill_location = config
            .fill
            .and_then(|f| f.location)
            .map(LocationFilter::as_raw);
        let fill = config.fill.map(|f| fill_raw(&f, fill_location.as_ref()));
        cfg.fill = fill.as_ref().map_or(ptr::null(), |f| f as *const _);
        if let Some(timeout) = config.rendezvous_timeout {
            cfg.has_rendezvous_timeout = true;
            cfg.rendezvous_timeout_ms = raw::millis(timeout)?;
        }
        if let Some(include) = config.include_properties {
            cfg.has_include_properties = true;
            cfg.include_properties = include;
        }
        let mut out = sys::moq_subscription_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and everything it
        // points at (namespace, name, tokens, filter, fill) live through it.
        raw::check(unsafe {
            sys::moq_session_subscribe(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(Subscription::new(out, self.owner))
    }

    /// Accept a peer's subscription (`moq_session_accept_subscribe`).
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`] for a handle of another session; the native
    /// code otherwise ([`Error::STALE_HANDLE`], [`Error::INVAL`],
    /// [`Error::WOULD_BLOCK`]).
    pub fn accept_subscribe(
        &mut self,
        subscription: Subscription,
        config: &AcceptSubscribe,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let mut cfg = sized!(
            moq_accept_subscribe_cfg_init_sized,
            sys::moq_accept_subscribe_cfg_t
        );
        if let Some(alias) = config.track_alias {
            cfg.has_track_alias = true;
            cfg.track_alias = alias;
        }
        if let Some(largest) = config.largest {
            cfg.has_largest = true;
            cfg.largest_group = largest.group_id;
            cfg.largest_object = largest.object_id;
        }
        if let Some(expires) = config.expires_ms {
            cfg.has_expires = true;
            cfg.expires_ms = expires;
        }
        cfg.track_properties = raw::span(&config.track_properties);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_subscribe(self.ptr(), subscription.raw(), &cfg, now.as_micros())
        })
    }

    /// Reject a peer's subscription (`moq_session_reject_subscribe`).
    ///
    /// # Errors
    /// As for [`accept_subscribe`](Session::accept_subscribe); a code the
    /// draft cannot carry is [`Error::INVAL`].
    pub fn reject_subscribe(
        &mut self,
        subscription: Subscription,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let mut cfg = sized!(
            moq_reject_subscribe_cfg_init_sized,
            sys::moq_reject_subscribe_cfg_t
        );
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        if let Some(retry) = rejection.retry_after_ms {
            cfg.can_retry = true;
            cfg.retry_after_ms = retry;
        }
        let redirect = rejection.redirect.as_ref().map(redirect_raw);
        if let Some(target) = redirect {
            cfg.redirect = target;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_subscribe(self.ptr(), subscription.raw(), &cfg, now.as_micros())
        })
    }

    /// Cancel a subscription this side requested (`moq_session_unsubscribe`).
    /// Its open data streams are stopped.
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] (the handle stays valid for a retry),
    /// [`Error::STALE_HANDLE`], [`Error::WRONG_STATE`].
    pub fn unsubscribe(&mut self, subscription: Subscription, now: Instant) -> Result<()> {
        self.check(subscription)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_unsubscribe(self.ptr(), subscription.raw(), now.as_micros())
        })
    }

    /// End a subscription this side publishes (`moq_session_done_subscribe`).
    /// Its subgroups must be closed or reset first.
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] while a subgroup terminal or an open report is
    /// owed; [`Error::WRONG_STATE`] while subgroups are open or an update
    /// answer is owed; [`Error::STALE_HANDLE`].
    pub fn done_subscribe(
        &mut self,
        subscription: Subscription,
        done: &TrackDoneConfig,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let mut cfg = sized!(
            moq_done_subscribe_cfg_init_sized,
            sys::moq_done_subscribe_cfg_t
        );
        cfg.status_code = done.status_code;
        cfg.stream_count = done.stream_count;
        cfg.reason = raw::span(&done.reason);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_done_subscribe(self.ptr(), subscription.raw(), &cfg, now.as_micros())
        })
    }

    /// Update a subscription this side requested
    /// (`moq_session_update_subscription`).
    ///
    /// # Errors
    /// [`Error::REQUEST_BLOCKED`], [`Error::WOULD_BLOCK`],
    /// [`Error::WRONG_STATE`], [`Error::INVAL`] (an empty update on drafts
    /// 16 and 18), [`Error::UNSUPPORTED`] for a field the draft cannot
    /// carry.
    pub fn update_subscription(
        &mut self,
        subscription: Subscription,
        update: &SubscriptionUpdate,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let tokens = tokens_raw(&update.tokens);
        let location = update.location.map(LocationFilter::as_raw);
        let fill_location = update
            .fill
            .and_then(|f| f.location)
            .map(LocationFilter::as_raw);
        let fill = update.fill.map(|f| fill_raw(&f, fill_location.as_ref()));
        let mut cfg = sized!(
            moq_subscription_update_cfg_init_sized,
            sys::moq_subscription_update_cfg_t
        );
        if let Some(priority) = update.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        if let Some(forward) = update.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        if let Some(timeout) = update.delivery_timeout_us {
            cfg.has_delivery_timeout = true;
            cfg.delivery_timeout_us = timeout;
        }
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(group) = update.new_group_request {
            cfg.has_new_group_request = true;
            cfg.new_group_request = group;
        }
        if let Some(filter) = update.filter {
            let (kind, start, end_group) = legacy_filter_raw(filter);
            cfg.has_filter = true;
            cfg.filter = kind;
            cfg.start_group = start.group_id;
            cfg.start_object = start.object_id;
            cfg.end_group = end_group;
        }
        cfg.location = location.as_ref().map_or(ptr::null(), |l| l as *const _);
        cfg.fill = fill.as_ref().map_or(ptr::null(), |f| f as *const _);
        if let Some(timeout) = update.object_delivery_timeout_ms {
            cfg.delivery_timeout_flags |= sys::MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
            cfg.object_delivery_timeout_ms = timeout;
        }
        if let Some(timeout) = update.subgroup_delivery_timeout_ms {
            cfg.delivery_timeout_flags |= sys::MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
            cfg.subgroup_delivery_timeout_ms = timeout;
        }
        // SAFETY: an advancing call; the configuration and everything it
        // points at (tokens, filter, fill) live through it.
        raw::check(unsafe {
            sys::moq_session_update_subscription(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    // -- Publishing objects ----------------------------------------------

    fn subgroup_raw(config: &SubgroupConfig) -> sys::moq_subgroup_cfg_t {
        let mut cfg = sized!(moq_subgroup_cfg_init_sized, sys::moq_subgroup_cfg_t);
        cfg.group_id = config.group_id;
        cfg.subgroup_id = config.subgroup_id;
        cfg.publisher_priority = config.publisher_priority;
        cfg.object_properties = config.object_properties;
        cfg.end_of_group = config.end_of_group;
        cfg.first_object = config.first_object;
        cfg
    }

    /// Open a subgroup of a subscription this side publishes
    /// (`moq_session_open_subgroup`).
    ///
    /// # Errors
    /// The native code ([`Error::WRONG_STATE`] before the subscription is
    /// accepted, [`Error::WOULD_BLOCK`] while every place in the subgroup
    /// pool is taken or the action queue has no room).
    pub fn open_subgroup(
        &mut self,
        subscription: Subscription,
        config: &SubgroupConfig,
        now: Instant,
    ) -> Result<Subgroup> {
        self.check(subscription)?;
        let cfg = Session::subgroup_raw(config);
        let mut out = sys::moq_subgroup_handle_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_open_subgroup(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
                &mut out,
            )
        })?;
        Ok(Subgroup::new(out, self.owner))
    }

    /// Open a subgroup of a publication (`moq_session_open_pub_subgroup`).
    ///
    /// # Errors
    /// As for [`open_subgroup`](Session::open_subgroup).
    pub fn open_publication_subgroup(
        &mut self,
        publication: Publication,
        config: &SubgroupConfig,
        now: Instant,
    ) -> Result<Subgroup> {
        self.check(publication)?;
        let cfg = Session::subgroup_raw(config);
        let mut out = sys::moq_subgroup_handle_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_open_pub_subgroup(
                self.ptr(),
                publication.raw(),
                &cfg,
                now.as_micros(),
                &mut out,
            )
        })?;
        Ok(Subgroup::new(out, self.owner))
    }

    /// The result of a write that took owned buffers: on success the
    /// session holds its own references and the caller's are released; a
    /// refusal the call states retains no reference gives them back, when
    /// they are still exclusively the caller's.
    pub(super) fn written(
        rc: sys::moq_result_t,
        payload: Option<Buffer>,
        properties: Option<Buffer>,
        kept: Kept,
    ) -> std::result::Result<(), WriteError> {
        if rc == sys::MOQ_OK {
            return Ok(());
        }
        let stated = match kept {
            Kept::EveryRefusal => true,
            Kept::WouldBlock => rc == sys::MOQ_ERR_WOULD_BLOCK,
        };
        // A buffer still carrying another reference is not returned: it
        // would no longer be exclusively the caller's.
        let back = |buffer: Buffer| (stated && exclusive(&buffer)).then_some(buffer);
        Err(WriteError {
            error: Error::from_code(rc),
            payload: payload.and_then(back),
            properties: properties.and_then(back),
        })
    }

    /// A refusal before the call reached C: every buffer comes back.
    pub(super) fn not_written(
        error: Error,
        payload: Option<Buffer>,
        properties: Option<Buffer>,
    ) -> std::result::Result<(), WriteError> {
        Err(WriteError {
            error,
            payload,
            properties,
        })
    }

    /// Write one whole object with no properties on a subgroup
    /// (`moq_session_write_object`). The payload is consumed: the session
    /// takes its own reference on success and none on any refusal.
    ///
    /// # Errors
    /// The native code, with the payload back ([`Error::INVAL`] for an
    /// object ID not above the last, [`Error::WOULD_BLOCK`],
    /// [`Error::WRONG_STATE`], [`Error::STALE_HANDLE`], ...).
    pub fn write_object(
        &mut self,
        subgroup: Subgroup,
        object_id: u64,
        payload: Buffer,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(subgroup) {
            return Session::not_written(error, Some(payload), None);
        }
        // SAFETY: an advancing call; the payload is borrowed for it, and C
        // adds a reference of its own only when it queues the object.
        let rc = unsafe {
            sys::moq_session_write_object(
                self.ptr(),
                subgroup.raw(),
                object_id,
                payload.as_ptr(),
                now.as_micros(),
            )
        };
        Session::written(rc, Some(payload), None, Kept::EveryRefusal)
    }

    /// Write one whole object with its properties or prior gaps on a
    /// subgroup (`moq_session_write_object_ex`); properties need a subgroup
    /// opened with [`SubgroupConfig::object_properties`]. The buffers are
    /// consumed.
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] with both buffers back (no queue room, or the
    /// subgroup owes a terminal; C retains no reference then);
    /// [`Error::UNSUPPORTED`] for prior gaps on draft 16; other refusals
    /// release them.
    pub fn write_object_with(
        &mut self,
        subgroup: Subgroup,
        object: ObjectWrite,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(subgroup) {
            return Session::not_written(error, Some(object.payload), object.properties);
        }
        let mut cfg = sized!(moq_object_cfg_init_sized, sys::moq_object_cfg_t);
        cfg.object_id = object.object_id;
        cfg.payload = object.payload.as_ptr();
        cfg.properties = object
            .properties
            .as_ref()
            .map_or(ptr::null_mut(), Buffer::as_ptr);
        if let Some(gap) = object.prior_group_gap {
            cfg.has_prior_group_gap = true;
            cfg.prior_group_gap = gap;
        }
        if let Some(gap) = object.prior_object_gap {
            cfg.has_prior_object_gap = true;
            cfg.prior_object_gap = gap;
        }
        // SAFETY: an advancing call; the buffers are live references this
        // call holds, to which C adds its own when it queues the object.
        let rc = unsafe {
            sys::moq_session_write_object_ex(self.ptr(), subgroup.raw(), &cfg, now.as_micros())
        };
        Session::written(
            rc,
            Some(object.payload),
            object.properties,
            Kept::WouldBlock,
        )
    }

    /// Write a zero-length status object (END_OF_GROUP, END_OF_TRACK) on a
    /// subgroup with no object properties (`moq_session_write_status_object`).
    ///
    /// # Errors
    /// The native code.
    pub fn write_status_object(
        &mut self,
        subgroup: Subgroup,
        object_id: u64,
        status: ObjectStatus,
        now: Instant,
    ) -> Result<()> {
        self.check(subgroup)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_write_status_object(
                self.ptr(),
                subgroup.raw(),
                object_id,
                status.code(),
                now.as_micros(),
            )
        })
    }

    /// Begin a streamed object of `payload_length` bytes
    /// (`moq_session_begin_object`); write it with
    /// [`write_object_data`](Session::write_object_data), finish it with
    /// [`end_object`](Session::end_object).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] while another streamed object is active;
    /// [`Error::WOULD_BLOCK`].
    pub fn begin_object(
        &mut self,
        subgroup: Subgroup,
        object_id: u64,
        payload_length: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(subgroup)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_begin_object(
                self.ptr(),
                subgroup.raw(),
                object_id,
                payload_length,
                now.as_micros(),
            )
        })
    }

    /// Begin a streamed object with its properties
    /// (`moq_session_begin_object_ex`), on a subgroup opened with
    /// [`SubgroupConfig::object_properties`]; then as for
    /// [`begin_object`](Session::begin_object). The properties are
    /// consumed.
    ///
    /// # Errors
    /// As for [`begin_object`](Session::begin_object). On
    /// [`Error::WOULD_BLOCK`] (the subgroup's terminal event is owed, or the
    /// action queue cannot take the header and the properties) nothing
    /// begins and C takes no reference to the properties: they come back in
    /// [`WriteError::properties`], the very buffer, to pass again. Other
    /// refusals release them.
    pub fn begin_object_with_properties(
        &mut self,
        subgroup: Subgroup,
        object_id: u64,
        payload_length: u64,
        properties: Buffer,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(subgroup) {
            return Session::not_written(error, None, Some(properties));
        }
        let mut cfg = sized!(moq_begin_object_cfg_init_sized, sys::moq_begin_object_cfg_t);
        cfg.object_id = object_id;
        cfg.payload_length = payload_length;
        cfg.properties = properties.as_ptr();
        // SAFETY: an advancing call; the properties are a live reference
        // this call holds, to which C adds its own when it queues them.
        let rc = unsafe {
            sys::moq_session_begin_object_ex(self.ptr(), subgroup.raw(), &cfg, now.as_micros())
        };
        Session::written(rc, None, Some(properties), Kept::WouldBlock)
    }

    /// Push payload bytes of the active streamed object
    /// (`moq_session_write_object_data`). The data is consumed.
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] with the data back (C retains no reference);
    /// [`Error::INVAL`] past the declared length and other refusals release
    /// it.
    pub fn write_object_data(
        &mut self,
        subgroup: Subgroup,
        data: Buffer,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(subgroup) {
            return Session::not_written(error, Some(data), None);
        }
        // SAFETY: an advancing call; C adds its own reference when it queues
        // the bytes.
        let rc = unsafe {
            sys::moq_session_write_object_data(
                self.ptr(),
                subgroup.raw(),
                data.as_ptr(),
                now.as_micros(),
            )
        };
        Session::written(rc, Some(data), None, Kept::WouldBlock)
    }

    /// Finish the active streamed object (`moq_session_end_object`).
    ///
    /// # Errors
    /// [`Error::INVAL`] when fewer bytes than declared were written.
    pub fn end_object(&mut self, subgroup: Subgroup, now: Instant) -> Result<()> {
        self.check(subgroup)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_end_object(self.ptr(), subgroup.raw(), now.as_micros())
        })
    }

    /// End a subgroup with its FIN (`moq_session_close_subgroup`).
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] while the subgroup owes a terminal.
    pub fn close_subgroup(&mut self, subgroup: Subgroup, now: Instant) -> Result<()> {
        self.check(subgroup)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_close_subgroup(self.ptr(), subgroup.raw(), now.as_micros())
        })
    }

    /// Reset a subgroup's stream (`moq_session_reset_subgroup`).
    ///
    /// # Errors
    /// As for [`close_subgroup`](Session::close_subgroup).
    pub fn reset_subgroup(
        &mut self,
        subgroup: Subgroup,
        error_code: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(subgroup)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_reset_subgroup(self.ptr(), subgroup.raw(), error_code, now.as_micros())
        })
    }

    /// Send an object as a datagram on a subscription this side publishes
    /// (`moq_session_send_object_datagram`), with its object properties
    /// (`properties`, encoded for the draft; empty for none). The payload
    /// must not be empty. Its bytes are copied: the session never takes a
    /// reference, so it is consumed on success and comes back on any
    /// refusal.
    ///
    /// # Errors
    /// The native code, with the payload back.
    #[allow(
        clippy::too_many_arguments,
        reason = "the native call's own parameters"
    )]
    pub fn send_object_datagram(
        &mut self,
        subscription: Subscription,
        location: Location,
        publisher_priority: u8,
        end_of_group: bool,
        payload: Buffer,
        properties: &[u8],
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(subscription) {
            return Session::not_written(error, Some(payload), None);
        }
        // SAFETY: an advancing call; the payload and the property bytes are
        // borrowed for it, and C copies what it keeps.
        let rc = unsafe {
            sys::moq_session_send_object_datagram(
                self.ptr(),
                subscription.raw(),
                location.group_id,
                location.object_id,
                publisher_priority,
                end_of_group,
                payload.as_ptr(),
                properties.as_ptr(),
                properties.len(),
                now.as_micros(),
            )
        };
        Session::written(rc, Some(payload), None, Kept::EveryRefusal)
    }

    // -- PUBLISH ----------------------------------------------------------

    /// Publish a track to the peer (`moq_session_publish`).
    ///
    /// # Errors
    /// [`Error::REQUEST_BLOCKED`], [`Error::INVAL`], [`Error::WOULD_BLOCK`].
    pub fn publish(&mut self, config: &PublishConfig, now: Instant) -> Result<Publication> {
        let tokens = tokens_raw(&config.tokens);
        let mut cfg = sized!(moq_publish_cfg_init_sized, sys::moq_publish_cfg_t);
        cfg.track_namespace = config.namespace.as_raw();
        cfg.track_name = raw::span(&config.track_name);
        if let Some(alias) = config.track_alias {
            cfg.has_track_alias = true;
            cfg.track_alias = alias;
        }
        if let Some(forward) = config.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        cfg.track_properties = raw::span(&config.track_properties);
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(expires) = config.expires_ms {
            cfg.has_expires = true;
            cfg.expires_ms = expires;
        }
        if let Some(priority) = config.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        if let Some(order) = config.group_order {
            cfg.has_group_order = true;
            cfg.group_order = group_order_raw(Some(order));
        }
        let location = config.location.map(LocationFilter::as_raw);
        cfg.location = location.as_ref().map_or(ptr::null(), |l| l as *const _);
        if let Some(timeout) = config.object_delivery_timeout_ms {
            cfg.has_object_delivery_timeout = true;
            cfg.object_delivery_timeout_ms = timeout;
        }
        if let Some(timeout) = config.subgroup_delivery_timeout_ms {
            cfg.has_subgroup_delivery_timeout = true;
            cfg.subgroup_delivery_timeout_ms = timeout;
        }
        let mut out = sys::moq_publication_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // live through it.
        raw::check(unsafe {
            sys::moq_session_publish(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(Publication::new(out, self.owner))
    }

    /// Accept a peer's PUBLISH (`moq_session_accept_publish`).
    ///
    /// # Errors
    /// [`Error::INVAL`] for an override draft 22 refuses; the native code.
    pub fn accept_publish(
        &mut self,
        publication: Publication,
        config: &AcceptPublish,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        let mut cfg = sized!(
            moq_accept_publish_cfg_init_sized,
            sys::moq_accept_publish_cfg_t
        );
        if let Some(priority) = config.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        cfg.group_order = group_order_raw(config.group_order);
        if let Some(forward) = config.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        if let Some(group) = config.new_group_request {
            cfg.has_new_group_request = true;
            cfg.new_group_request = group;
        }
        if let Some(filter) = config.filter {
            let (kind, start, end_group) = legacy_filter_raw(filter);
            cfg.filter = kind;
            cfg.start_group = start.group_id;
            cfg.start_object = start.object_id;
            cfg.end_group = end_group;
        }
        if let Some(expires) = config.expires_ms {
            cfg.has_expires = true;
            cfg.expires_ms = expires;
        }
        if let Some(timeout) = config.object_delivery_timeout_ms {
            cfg.delivery_timeout_flags |= sys::MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT;
            cfg.object_delivery_timeout_ms = timeout;
        }
        if let Some(timeout) = config.subgroup_delivery_timeout_ms {
            cfg.delivery_timeout_flags |= sys::MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT;
            cfg.subgroup_delivery_timeout_ms = timeout;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_publish(self.ptr(), publication.raw(), &cfg, now.as_micros())
        })
    }

    /// Reject a peer's PUBLISH (`moq_session_reject_publish`).
    ///
    /// # Errors
    /// [`Error::INVAL`] for a nonempty redirect target, which the native
    /// PUBLISH refusal has no field for, and for REDIRECT before draft 22;
    /// the native code otherwise.
    pub fn reject_publish(
        &mut self,
        publication: Publication,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        targetless(rejection)?;
        let mut cfg = sized!(
            moq_reject_publish_cfg_init_sized,
            sys::moq_reject_publish_cfg_t
        );
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        if let Some(retry) = rejection.retry_after_ms {
            cfg.can_retry = true;
            cfg.retry_after_ms = retry;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_publish(self.ptr(), publication.raw(), &cfg, now.as_micros())
        })
    }

    /// End a publication this side made (`moq_session_finish_publish`).
    ///
    /// # Errors
    /// As for [`done_subscribe`](Session::done_subscribe).
    pub fn finish_publish(
        &mut self,
        publication: Publication,
        done: &TrackDoneConfig,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        let mut cfg = sized!(
            moq_finish_publish_cfg_init_sized,
            sys::moq_finish_publish_cfg_t
        );
        cfg.status_code = done.status_code;
        cfg.stream_count = done.stream_count;
        cfg.reason = raw::span(&done.reason);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_finish_publish(self.ptr(), publication.raw(), &cfg, now.as_micros())
        })
    }

    // -- FETCH --------------------------------------------------------------

    /// Fetch a range of a track (`moq_session_fetch`).
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`] for a joined handle of another session;
    /// [`Error::REQUEST_BLOCKED`]; [`Error::UNSUPPORTED`] for a joining
    /// fetch on draft 22, and for a semantic range, FILL_TIMEOUT or
    /// INCLUDE_PROPERTIES on a draft without them; [`Error::INVAL`] (a joined
    /// subscription without the Largest Object filter or a current Largest,
    /// those fields on a joining fetch); [`Error::WRONG_STATE`] for an
    /// ineligible joined publication.
    pub fn fetch(&mut self, config: &FetchConfig, now: Instant) -> Result<Fetch> {
        if let Some((joined, _)) = config.joining {
            match joined {
                Joining::Subscription(sub) => self.check(sub)?,
                Joining::Publication(publication) => self.check(publication)?,
            };
        }
        let tokens = tokens_raw(&config.tokens);
        let location = config.location.map(LocationFilter::as_raw);
        let mut cfg = sized!(moq_fetch_cfg_init_sized, sys::moq_fetch_cfg_t);
        cfg.track_namespace = config.namespace.as_raw();
        cfg.track_name = raw::span(&config.track_name);
        cfg.start_group = config.start.group_id;
        cfg.start_object = config.start.object_id;
        cfg.end_group = config.end_group;
        cfg.end_object = config.end_object;
        cfg.group_order = group_order_raw(config.group_order);
        if let Some(priority) = config.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        cfg.auth_tokens = if tokens.is_empty() {
            ptr::null()
        } else {
            tokens.as_ptr()
        };
        cfg.auth_token_count = tokens.len();
        cfg.location = location.as_ref().map_or(ptr::null(), |l| l as *const _);
        if let Some((joined, start)) = config.joining {
            cfg.is_joining = true;
            match joined {
                Joining::Subscription(sub) => cfg.joining_sub = sub.raw(),
                Joining::Publication(publication) => cfg.joining_pub = publication.raw(),
            }
            (cfg.joining_relative, cfg.joining_start) = match start {
                JoiningStart::Relative(groups) => (true, groups),
                JoiningStart::Absolute(group) => (false, group),
            };
        }
        if let Some(timeout) = config.fill_timeout_ms {
            cfg.has_fill_timeout = true;
            cfg.fill_timeout_ms = timeout;
        }
        if let Some(include) = config.include_properties {
            cfg.has_include_properties = true;
            cfg.include_properties = include;
        }
        let mut out = sys::moq_fetch_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // live through it.
        raw::check(unsafe { sys::moq_session_fetch(self.ptr(), &cfg, now.as_micros(), &mut out) })?;
        Ok(Fetch::new(out, self.owner))
    }

    /// Accept a peer's FETCH (`moq_session_accept_fetch`): queues the
    /// response and opens the response stream.
    ///
    /// # Errors
    /// [`Error::INVAL`] for an end that contradicts the request;
    /// [`Error::WOULD_BLOCK`]; [`Error::STALE_HANDLE`].
    pub fn accept_fetch(&mut self, fetch: Fetch, config: &AcceptFetch, now: Instant) -> Result<()> {
        self.check(fetch)?;
        let mut cfg = sized!(moq_accept_fetch_cfg_init_sized, sys::moq_accept_fetch_cfg_t);
        cfg.end_of_track = config.end_of_track;
        cfg.end_group = config.end_group;
        cfg.end_object = config.end_object;
        cfg.empty = config.empty;
        cfg.track_properties = raw::span(&config.track_properties);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_fetch(self.ptr(), fetch.raw(), &cfg, now.as_micros())
        })
    }

    /// Reject a peer's FETCH (`moq_session_reject_fetch`).
    ///
    /// # Errors
    /// The native code.
    pub fn reject_fetch(
        &mut self,
        fetch: Fetch,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(fetch)?;
        let mut cfg = sized!(moq_reject_fetch_cfg_init_sized, sys::moq_reject_fetch_cfg_t);
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        if let Some(retry) = rejection.retry_after_ms {
            cfg.can_retry = true;
            cfg.retry_after_ms = retry;
        }
        let redirect = rejection.redirect.as_ref().map(redirect_raw);
        if let Some(target) = redirect {
            cfg.redirect = target;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_fetch(self.ptr(), fetch.raw(), &cfg, now.as_micros())
        })
    }

    /// Write one object on an accepted FETCH (`moq_session_write_fetch_object`).
    /// The buffers are borrowed for the call: the session takes references
    /// of its own on success and none on any refusal.
    ///
    /// # Errors
    /// The native code, with the payload and properties back.
    pub fn write_fetch_object(
        &mut self,
        fetch: Fetch,
        object: FetchObjectWrite,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(fetch) {
            return Session::not_written(error, Some(object.payload), object.properties);
        }
        let mut cfg = sized!(moq_fetch_object_cfg_init_sized, sys::moq_fetch_object_cfg_t);
        cfg.group_id = object.group_id;
        cfg.subgroup_id = object.subgroup_id;
        cfg.object_id = object.object_id;
        cfg.publisher_priority = object.publisher_priority;
        cfg.datagram = object.datagram;
        cfg.payload = object.payload.as_ptr();
        cfg.properties = object
            .properties
            .as_ref()
            .map_or(ptr::null_mut(), Buffer::as_ptr);
        // SAFETY: an advancing call; the buffers are borrowed for it, and C
        // adds references of its own only when it queues the object.
        let rc = unsafe {
            sys::moq_session_write_fetch_object(self.ptr(), fetch.raw(), &cfg, now.as_micros())
        };
        Session::written(
            rc,
            Some(object.payload),
            object.properties,
            Kept::EveryRefusal,
        )
    }

    /// Write an end-of-range marker on an accepted FETCH
    /// (`moq_session_write_fetch_range`).
    ///
    /// # Errors
    /// [`Error::INVAL`] for a kind the draft has no encoding for.
    pub fn write_fetch_range(
        &mut self,
        fetch: Fetch,
        range: FetchRange,
        end: Location,
        now: Instant,
    ) -> Result<()> {
        self.check(fetch)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_write_fetch_range(
                self.ptr(),
                fetch.raw(),
                range.raw(),
                end.group_id,
                end.object_id,
                now.as_micros(),
            )
        })
    }

    /// Finish an accepted FETCH with its FIN (`moq_session_end_fetch`).
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`], with nothing changed.
    pub fn end_fetch(&mut self, fetch: Fetch, now: Instant) -> Result<()> {
        self.check(fetch)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe { sys::moq_session_end_fetch(self.ptr(), fetch.raw(), now.as_micros()) })
    }

    /// Abort an accepted FETCH: reset its response stream with
    /// `error_code` (`moq_session_abort_fetch`).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] before acceptance; [`Error::WOULD_BLOCK`].
    pub fn abort_fetch(&mut self, fetch: Fetch, error_code: u64, now: Instant) -> Result<()> {
        self.check(fetch)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_abort_fetch(self.ptr(), fetch.raw(), error_code, now.as_micros())
        })
    }

    /// Cancel a FETCH this side requested (`moq_session_fetch_cancel`).
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`], with nothing changed.
    pub fn cancel_fetch(&mut self, fetch: Fetch, now: Instant) -> Result<()> {
        self.check(fetch)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_fetch_cancel(self.ptr(), fetch.raw(), now.as_micros())
        })
    }

    // -- Namespaces ---------------------------------------------------------

    /// Advertise a namespace (`moq_session_publish_namespace`).
    ///
    /// # Errors
    /// The native code.
    pub fn publish_namespace(
        &mut self,
        namespace: &Namespace,
        now: Instant,
    ) -> Result<Announcement> {
        let mut cfg = sized!(
            moq_publish_namespace_cfg_init_sized,
            sys::moq_publish_namespace_cfg_t
        );
        cfg.track_namespace = namespace.as_raw();
        let mut out = sys::moq_announcement_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_publish_namespace(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(Announcement::new(out, self.owner))
    }

    /// Withdraw an advertisement this side made
    /// (`moq_session_publish_namespace_done`).
    ///
    /// # Errors
    /// The native code.
    pub fn publish_namespace_done(
        &mut self,
        announcement: Announcement,
        now: Instant,
    ) -> Result<()> {
        self.check(announcement)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_publish_namespace_done(self.ptr(), announcement.raw(), now.as_micros())
        })
    }

    /// Accept a peer's advertisement (`moq_session_accept_namespace`).
    ///
    /// # Errors
    /// The native code.
    pub fn accept_namespace(&mut self, announcement: Announcement, now: Instant) -> Result<()> {
        self.check(announcement)?;
        let cfg = sized!(
            moq_accept_namespace_cfg_init_sized,
            sys::moq_accept_namespace_cfg_t
        );
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_namespace(self.ptr(), announcement.raw(), &cfg, now.as_micros())
        })
    }

    /// Reject a peer's pending advertisement (`moq_session_reject_namespace`).
    ///
    /// # Errors
    /// The native code.
    pub fn reject_namespace(
        &mut self,
        announcement: Announcement,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(announcement)?;
        let mut cfg = sized!(
            moq_reject_namespace_cfg_init_sized,
            sys::moq_reject_namespace_cfg_t
        );
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        if let Some(retry) = rejection.retry_after_ms {
            cfg.can_retry = true;
            cfg.retry_after_ms = retry;
        }
        let redirect = rejection.redirect.as_ref().map(redirect_raw);
        if let Some(target) = redirect {
            cfg.redirect = target;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_namespace(self.ptr(), announcement.raw(), &cfg, now.as_micros())
        })
    }

    /// Subscribe to namespaces under `prefix` (`moq_session_subscribe_namespace`).
    ///
    /// # Errors
    /// [`Error::REQUEST_BLOCKED`], [`Error::WOULD_BLOCK`].
    pub fn subscribe_namespace(
        &mut self,
        prefix: &Namespace,
        interest: NamespaceInterest,
        now: Instant,
    ) -> Result<NamespaceSubscription> {
        self.subscribe_namespace_with(
            &SubscribeNamespaceConfig::new(prefix.clone(), interest),
            now,
        )
    }

    /// Subscribe to namespaces with the request's own authorization tokens
    /// (`moq_session_subscribe_namespace`). The tokens authorize the initial
    /// request; a later update's tokens do not stand in for them.
    ///
    /// # Errors
    /// As for [`subscribe_namespace`](Session::subscribe_namespace).
    pub fn subscribe_namespace_with(
        &mut self,
        config: &SubscribeNamespaceConfig,
        now: Instant,
    ) -> Result<NamespaceSubscription> {
        let tokens = tokens_raw(&config.tokens);
        let mut cfg = sized!(
            moq_subscribe_namespace_cfg_init_sized,
            sys::moq_subscribe_namespace_cfg_t
        );
        cfg.track_namespace_prefix = config.prefix.as_raw();
        cfg.namespace_interest = config.interest.raw();
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        let mut out = sys::moq_ns_sub_handle_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // (prefix, tokens) live through it.
        raw::check(unsafe {
            sys::moq_session_subscribe_namespace(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(NamespaceSubscription::new(out, self.owner))
    }

    /// Accept a peer's namespace subscription (`moq_session_accept_ns_sub`).
    ///
    /// # Errors
    /// The native code.
    pub fn accept_namespace_subscription(
        &mut self,
        subscription: NamespaceSubscription,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let cfg = sized!(
            moq_accept_ns_sub_cfg_init_sized,
            sys::moq_accept_ns_sub_cfg_t
        );
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_ns_sub(self.ptr(), subscription.raw(), &cfg, now.as_micros())
        })
    }

    /// Reject a peer's namespace subscription (`moq_session_reject_ns_sub`).
    ///
    /// # Errors
    /// The native code.
    pub fn reject_namespace_subscription(
        &mut self,
        subscription: NamespaceSubscription,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let mut cfg = sized!(
            moq_reject_ns_sub_cfg_init_sized,
            sys::moq_reject_ns_sub_cfg_t
        );
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        if let Some(retry) = rejection.retry_after_ms {
            cfg.can_retry = true;
            cfg.retry_after_ms = retry;
        }
        let redirect = rejection.redirect.as_ref().map(redirect_raw);
        if let Some(target) = redirect {
            cfg.redirect = target;
        }
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_ns_sub(self.ptr(), subscription.raw(), &cfg, now.as_micros())
        })
    }

    /// Announce a namespace suffix on an accepted namespace subscription
    /// (`moq_session_send_namespace`).
    ///
    /// # Errors
    /// The native code.
    pub fn send_namespace(
        &mut self,
        subscription: NamespaceSubscription,
        suffix: &Namespace,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let suffix = suffix.as_raw();
        // SAFETY: an advancing call; the suffix lives through it.
        raw::check(unsafe {
            sys::moq_session_send_namespace(
                self.ptr(),
                subscription.raw(),
                &suffix,
                now.as_micros(),
            )
        })
    }

    /// Withdraw a namespace suffix (`moq_session_send_namespace_done`).
    ///
    /// # Errors
    /// The native code.
    pub fn send_namespace_done(
        &mut self,
        subscription: NamespaceSubscription,
        suffix: &Namespace,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let suffix = suffix.as_raw();
        // SAFETY: an advancing call; the suffix lives through it.
        raw::check(unsafe {
            sys::moq_session_send_namespace_done(
                self.ptr(),
                subscription.raw(),
                &suffix,
                now.as_micros(),
            )
        })
    }

    /// Cancel a namespace subscription this side made
    /// (`moq_session_cancel_namespace_sub`).
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`], with nothing changed.
    pub fn cancel_namespace_subscription(
        &mut self,
        subscription: NamespaceSubscription,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_cancel_namespace_sub(self.ptr(), subscription.raw(), now.as_micros())
        })
    }

    // -- GOAWAY ---------------------------------------------------------------

    /// Send a session GOAWAY (`moq_session_goaway`); the session drains.
    /// `new_session_uri` is a server's (at most 8192 bytes); a client passes
    /// an empty one.
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] when already sent or not established;
    /// [`Error::INVAL`] for a client's URI.
    pub fn goaway(&mut self, new_session_uri: &[u8], now: Instant) -> Result<()> {
        // SAFETY: an advancing call; the URI is borrowed for it.
        raw::check(unsafe {
            sys::moq_session_goaway(
                self.ptr(),
                if new_session_uri.is_empty() {
                    ptr::null()
                } else {
                    new_session_uri.as_ptr()
                },
                new_session_uri.len(),
                now.as_micros(),
            )
        })
    }
}
