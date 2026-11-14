//! The request families beyond SUBSCRIBE, PUBLISH, FETCH and namespaces:
//! TRACK_STATUS, SUBSCRIBE_TRACKS, namespace-request updates, per-request
//! GOAWAY, fills, publication updates, update answers, FETCH updates and
//! publisher state notifications. Every configuration is built with its
//! sized initializer and borrowed by C for the call only.

use std::ptr;

use super::custody::WriteError;
use super::event::{FetchRange, RequestHandle};
use super::handle::{
    Announcement, Fetch, Fill, NamespaceSubscription, Publication, Subscription,
    TrackStatusRequest, TrackSubscription,
};
use super::owner::Session;
use super::raw;
use super::request::{
    cluster_raw, fill_raw, group_order_raw, plain_refusal, redirect_raw, sized, tokens_ptr,
    tokens_raw, AuthToken, ClusterPath, FetchObjectWrite, Kept, Rejection, SubscriptionUpdate,
};
use super::time::Instant;
use crate::{
    sys, Buffer, Error, GroupOrder, Location, LocationFilter, Namespace, ObjectStatus, Result,
};

/// A TRACK_STATUS this side asks (`moq_track_status_cfg_t`).
#[derive(Clone, Debug)]
pub struct TrackStatusConfig {
    namespace: Namespace,
    track_name: Vec<u8>,
    tokens: Vec<AuthToken>,
    include_properties: Option<bool>,
}

impl TrackStatusConfig {
    /// The status of the track `track_name` in `namespace`.
    #[must_use]
    pub fn new(namespace: Namespace, track_name: impl Into<Vec<u8>>) -> TrackStatusConfig {
        TrackStatusConfig {
            namespace,
            track_name: track_name.into(),
            tokens: Vec::new(),
            include_properties: None,
        }
    }

    /// Add an authorization token.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> TrackStatusConfig {
        self.tokens.push(token);
        self
    }

    /// INCLUDE_PROPERTIES (draft 22): `false` asks for an answer with empty
    /// Track Properties. [`Error::UNSUPPORTED`] on drafts 16 and 18.
    #[must_use]
    pub fn include_properties(mut self, include: bool) -> TrackStatusConfig {
        self.include_properties = Some(include);
        self
    }
}

/// How this side answers a TRACK_STATUS (`moq_accept_track_status_cfg_t`).
/// Every field left unset is sent absent.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct AcceptTrackStatus {
    /// The largest object; the session reports the larger of this and the
    /// one it has observed for the track.
    pub largest: Option<Location>,
    /// EXPIRES, in milliseconds.
    pub expires_ms: Option<u64>,
    /// Track Properties, encoded for the session's draft (validated and not
    /// sent on draft 16, whose answer has none).
    pub track_properties: Vec<u8>,
}

/// A SUBSCRIBE_TRACKS this side makes (`moq_subscribe_tracks_cfg_t`,
/// drafts 18 and 22).
#[derive(Clone, Debug)]
pub struct SubscribeTracksConfig {
    prefix: Namespace,
    forward: Option<bool>,
    tokens: Vec<AuthToken>,
    group_order: Option<GroupOrder>,
    include_properties: Option<bool>,
}

impl SubscribeTracksConfig {
    /// Track discovery under `prefix`.
    #[must_use]
    pub fn new(prefix: Namespace) -> SubscribeTracksConfig {
        SubscribeTracksConfig {
            prefix,
            forward: None,
            tokens: Vec::new(),
            group_order: None,
            include_properties: None,
        }
    }

    /// The forward state of the PUBLISHes the request leads to (default
    /// true).
    #[must_use]
    pub fn forward(mut self, forward: bool) -> SubscribeTracksConfig {
        self.forward = Some(forward);
        self
    }

    /// Add an authorization token.
    #[must_use]
    pub fn auth_token(mut self, token: AuthToken) -> SubscribeTracksConfig {
        self.tokens.push(token);
        self
    }

    /// GROUP_ORDER (draft 22; [`Error::UNSUPPORTED`] on draft 18).
    #[must_use]
    pub fn group_order(mut self, order: GroupOrder) -> SubscribeTracksConfig {
        self.group_order = Some(order);
        self
    }

    /// INCLUDE_PROPERTIES (draft 22; [`Error::UNSUPPORTED`] on draft 18).
    #[must_use]
    pub fn include_properties(mut self, include: bool) -> SubscribeTracksConfig {
        self.include_properties = Some(include);
        self
    }
}

/// An update of a namespace-scoped request this side made
/// (`moq_namespace_update_cfg_t`, draft 22). Which fields a family carries:
/// an advertisement only tokens; a namespace subscription tokens and a
/// prefix; SUBSCRIBE_TRACKS tokens, a prefix and a forward state. Another
/// field is [`Error::INVAL`].
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct NamespaceUpdateConfig {
    /// A new prefix (the root selects the empty prefix).
    pub prefix: Option<Namespace>,
    /// A new forward state, for the subscriptions created from now on.
    pub forward: Option<bool>,
    /// Authorization tokens.
    pub tokens: Vec<AuthToken>,
    /// A partial cluster update of an advertisement: an empty path and an
    /// absent cost keep the prior values.
    pub cluster: Option<ClusterPath>,
}

/// A namespace advertisement this side makes, with what
/// [`Session::publish_namespace`] leaves out (`moq_publish_namespace_cfg_t`).
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PublishNamespaceConfig {
    /// The namespace.
    pub namespace: Namespace,
    /// Authorization tokens.
    pub tokens: Vec<AuthToken>,
    /// The cluster metadata the advertisement carries, on a session that
    /// negotiated the extension.
    pub cluster: Option<ClusterPath>,
}

impl PublishNamespaceConfig {
    /// An advertisement of `namespace` with no tokens and no cluster path.
    #[must_use]
    pub fn new(namespace: Namespace) -> PublishNamespaceConfig {
        PublishNamespaceConfig {
            namespace,
            tokens: Vec::new(),
            cluster: None,
        }
    }
}

/// A fetcher's update of a FETCH (`moq_fetch_update_cfg_t`, draft 22). At
/// least one field is required.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct FetchUpdate {
    /// The new subscriber priority of the fetch's data stream.
    pub subscriber_priority: Option<u8>,
    /// Authorization tokens.
    pub tokens: Vec<AuthToken>,
}

/// A publisher's state notification (`moq_publish_state_cfg_t`, draft 22):
/// the forward state and/or the filter in effect, each matching what the
/// subscriber authorized. At least one is required.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default)]
pub struct PublishStateConfig {
    /// The forward state in effect.
    pub forward: Option<bool>,
    /// The filter in effect.
    pub filter: Option<LocationFilter>,
}

/// A refusal of a peer's update: a code and a reason, never a retry or a
/// redirect target.
fn update_refusal(
    rejection: &Rejection,
    update_request_id: u64,
) -> Result<sys::moq_reject_subscription_update_cfg_t> {
    plain_refusal(rejection)?;
    let mut cfg = sized!(
        moq_reject_subscription_update_cfg_init_sized,
        sys::moq_reject_subscription_update_cfg_t
    );
    cfg.update_request_id = update_request_id;
    cfg.error_code = rejection.error_code;
    cfg.reason = raw::span(&rejection.reason);
    Ok(cfg)
}

fn update_acceptance(update_request_id: u64) -> sys::moq_accept_subscription_update_cfg_t {
    let mut cfg = sized!(
        moq_accept_subscription_update_cfg_init_sized,
        sys::moq_accept_subscription_update_cfg_t
    );
    cfg.update_request_id = update_request_id;
    cfg
}

impl Session {
    // -- TRACK_STATUS -------------------------------------------------------

    /// Ask for a track's status (`moq_session_track_status`). No
    /// subscription is created.
    ///
    /// # Errors
    /// [`Error::REQUEST_BLOCKED`] without request capacity;
    /// [`Error::UNSUPPORTED`] for INCLUDE_PROPERTIES before draft 22;
    /// [`Error::WOULD_BLOCK`].
    pub fn track_status(
        &mut self,
        config: &TrackStatusConfig,
        now: Instant,
    ) -> Result<TrackStatusRequest> {
        let tokens = tokens_raw(&config.tokens);
        let mut cfg = sized!(moq_track_status_cfg_init_sized, sys::moq_track_status_cfg_t);
        cfg.track_namespace = config.namespace.as_raw();
        cfg.track_name = raw::span(&config.track_name);
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(include) = config.include_properties {
            cfg.has_include_properties = true;
            cfg.include_properties = include;
        }
        let mut out = sys::moq_track_status_handle_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // (namespace, name, tokens) live through it.
        raw::check(unsafe {
            sys::moq_session_track_status(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(TrackStatusRequest::new(out, self.owner))
    }

    /// Answer a peer's TRACK_STATUS (`moq_session_accept_track_status`).
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`] for a handle of another session; the native
    /// code otherwise ([`Error::INVAL`] for invalid Track Properties).
    pub fn accept_track_status(
        &mut self,
        request: TrackStatusRequest,
        config: &AcceptTrackStatus,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let mut cfg = sized!(
            moq_accept_track_status_cfg_init_sized,
            sys::moq_accept_track_status_cfg_t
        );
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
            sys::moq_session_accept_track_status(self.ptr(), request.raw(), &cfg, now.as_micros())
        })
    }

    /// Refuse a peer's TRACK_STATUS (`moq_session_reject_track_status`),
    /// with a redirect target when the code is REDIRECT.
    ///
    /// # Errors
    /// As for [`accept_track_status`](Session::accept_track_status).
    pub fn reject_track_status(
        &mut self,
        request: TrackStatusRequest,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let mut cfg = sized!(
            moq_reject_track_status_cfg_init_sized,
            sys::moq_reject_track_status_cfg_t
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
        // SAFETY: an advancing call; the configuration and the target it
        // borrows live through it.
        raw::check(unsafe {
            sys::moq_session_reject_track_status(self.ptr(), request.raw(), &cfg, now.as_micros())
        })
    }

    // -- SUBSCRIBE_TRACKS ---------------------------------------------------

    /// Ask for PUBLISHes of every track under a prefix
    /// (`moq_session_subscribe_tracks`, drafts 18 and 22).
    ///
    /// # Errors
    /// [`Error::INVAL`] on draft 16 or for an unrepresentable prefix;
    /// [`Error::UNSUPPORTED`] for an option before draft 22;
    /// [`Error::REQUEST_BLOCKED`]; [`Error::WOULD_BLOCK`].
    pub fn subscribe_tracks(
        &mut self,
        config: &SubscribeTracksConfig,
        now: Instant,
    ) -> Result<TrackSubscription> {
        let tokens = tokens_raw(&config.tokens);
        let mut cfg = sized!(
            moq_subscribe_tracks_cfg_init_sized,
            sys::moq_subscribe_tracks_cfg_t
        );
        cfg.track_namespace_prefix = config.prefix.as_raw();
        if let Some(forward) = config.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(order) = config.group_order {
            cfg.options_flags |= sys::MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER;
            cfg.group_order = group_order_raw(Some(order));
        }
        if let Some(include) = config.include_properties {
            cfg.options_flags |= sys::MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES;
            cfg.include_properties = include;
        }
        let mut out = sys::moq_track_sub_handle_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // (prefix, tokens) live through it.
        raw::check(unsafe {
            sys::moq_session_subscribe_tracks(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(TrackSubscription::new(out, self.owner))
    }

    /// Accept a peer's SUBSCRIBE_TRACKS (`moq_session_accept_subscribe_tracks`).
    /// The request stays open for PUBLISH_BLOCKED and later tracks.
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`]; the native code otherwise.
    pub fn accept_subscribe_tracks(
        &mut self,
        request: TrackSubscription,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let cfg = sized!(
            moq_accept_subscribe_tracks_cfg_init_sized,
            sys::moq_accept_subscribe_tracks_cfg_t
        );
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_subscribe_tracks(
                self.ptr(),
                request.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse a peer's SUBSCRIBE_TRACKS (`moq_session_reject_subscribe_tracks`).
    ///
    /// # Errors
    /// [`Error::INVAL`] for a retry interval or a nonempty redirect target,
    /// which the native refusal has no field for, and for REDIRECT before
    /// draft 22; [`Error::WRONG_SESSION`]; the native code.
    pub fn reject_subscribe_tracks(
        &mut self,
        request: TrackSubscription,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        plain_refusal(rejection)?;
        let mut cfg = sized!(
            moq_reject_subscribe_tracks_cfg_init_sized,
            sys::moq_reject_subscribe_tracks_cfg_t
        );
        cfg.error_code = rejection.error_code;
        cfg.reason = raw::span(&rejection.reason);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_subscribe_tracks(
                self.ptr(),
                request.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Tell the subscriber a matching track cannot be PUBLISHed
    /// (`moq_session_send_publish_blocked`); `suffix` is relative to the
    /// request's prefix.
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] before acceptance; [`Error::WOULD_BLOCK`].
    pub fn send_publish_blocked(
        &mut self,
        request: TrackSubscription,
        suffix: &Namespace,
        track_name: &[u8],
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let suffix = suffix.as_raw();
        // SAFETY: an advancing call; the suffix and name live through it.
        raw::check(unsafe {
            sys::moq_session_send_publish_blocked(
                self.ptr(),
                request.raw(),
                &suffix,
                raw::span(track_name),
                now.as_micros(),
            )
        })
    }

    // -- Namespace advertisements with tokens or a cluster path -------------

    /// Advertise a namespace with tokens and cluster metadata
    /// (`moq_session_publish_namespace`).
    ///
    /// # Errors
    /// The native code.
    pub fn publish_namespace_with(
        &mut self,
        config: &PublishNamespaceConfig,
        now: Instant,
    ) -> Result<Announcement> {
        let tokens = tokens_raw(&config.tokens);
        let mut cfg = sized!(
            moq_publish_namespace_cfg_init_sized,
            sys::moq_publish_namespace_cfg_t
        );
        cfg.track_namespace = config.namespace.as_raw();
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(path) = &config.cluster {
            cfg.cluster = cluster_raw(path);
        }
        let mut out = sys::moq_announcement_t { _opaque: 0 };
        // SAFETY: an advancing call; the configuration and what it points at
        // (namespace, tokens, Hop IDs) live through it.
        raw::check(unsafe {
            sys::moq_session_publish_namespace(self.ptr(), &cfg, now.as_micros(), &mut out)
        })?;
        Ok(Announcement::new(out, self.owner))
    }

    /// Announce a namespace suffix with its cluster path on an accepted
    /// namespace subscription (`moq_session_send_namespace_cluster`). A
    /// negotiated session requires a nonempty path; a repeated send updates
    /// the same advertisement and keeps its first Hop ID.
    ///
    /// # Errors
    /// The native code.
    pub fn send_namespace_cluster(
        &mut self,
        subscription: NamespaceSubscription,
        suffix: &Namespace,
        path: &ClusterPath,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let suffix = suffix.as_raw();
        let cluster = cluster_raw(path);
        // SAFETY: an advancing call; the suffix and the metadata (whose Hop
        // IDs C copies before it returns) live through it.
        raw::check(unsafe {
            sys::moq_session_send_namespace_cluster(
                self.ptr(),
                subscription.raw(),
                &suffix,
                &cluster,
                std::mem::size_of::<sys::moq_cluster_metadata_t>(),
                now.as_micros(),
            )
        })
    }

    /// Cancel a peer's accepted advertisement (`moq_session_cancel_namespace`).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] unless established; the native code.
    pub fn cancel_namespace(
        &mut self,
        announcement: Announcement,
        error_code: u64,
        reason: &[u8],
        now: Instant,
    ) -> Result<()> {
        self.check(announcement)?;
        let mut cfg = sized!(
            moq_cancel_namespace_cfg_init_sized,
            sys::moq_cancel_namespace_cfg_t
        );
        cfg.error_code = error_code;
        cfg.reason = raw::span(reason);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_cancel_namespace(self.ptr(), announcement.raw(), &cfg, now.as_micros())
        })
    }

    // -- Namespace-request updates (draft 22) -------------------------------

    fn namespace_update_raw(
        config: &NamespaceUpdateConfig,
        tokens: &[sys::moq_auth_token_t],
    ) -> sys::moq_namespace_update_cfg_t {
        let mut cfg = sized!(
            moq_namespace_update_cfg_init_sized,
            sys::moq_namespace_update_cfg_t
        );
        if let Some(prefix) = &config.prefix {
            cfg.flags |= sys::MOQ_NAMESPACE_UPDATE_PREFIX;
            cfg.track_namespace_prefix = prefix.as_raw();
        }
        if let Some(forward) = config.forward {
            cfg.flags |= sys::MOQ_NAMESPACE_UPDATE_FORWARD;
            cfg.forward = forward;
        }
        cfg.auth_tokens = tokens_ptr(tokens);
        cfg.auth_token_count = tokens.len();
        if let Some(path) = &config.cluster {
            cfg.cluster = cluster_raw(path);
        }
        cfg
    }

    /// Update an advertisement this side made (`moq_session_update_namespace`).
    /// Returns the update's Request ID; the answer arrives as
    /// [`Event::NamespaceUpdateResult`](crate::Event::NamespaceUpdateResult)
    /// naming it.
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::REQUEST_BLOCKED`]
    /// while an update awaits its answer; [`Error::INVAL`] for a field the
    /// family does not carry; [`Error::WRONG_STATE`]; [`Error::WOULD_BLOCK`].
    pub fn update_namespace(
        &mut self,
        announcement: Announcement,
        update: &NamespaceUpdateConfig,
        now: Instant,
    ) -> Result<u64> {
        self.check(announcement)?;
        let tokens = tokens_raw(&update.tokens);
        let cfg = Session::namespace_update_raw(update, &tokens);
        let mut id = 0;
        // SAFETY: an advancing call; the configuration and what it points at
        // live through it.
        raw::check(unsafe {
            sys::moq_session_update_namespace(
                self.ptr(),
                announcement.raw(),
                &cfg,
                now.as_micros(),
                &mut id,
            )
        })?;
        Ok(id)
    }

    /// Update a namespace subscription this side made
    /// (`moq_session_update_ns_sub`).
    ///
    /// # Errors
    /// As for [`update_namespace`](Session::update_namespace).
    pub fn update_namespace_subscription(
        &mut self,
        subscription: NamespaceSubscription,
        update: &NamespaceUpdateConfig,
        now: Instant,
    ) -> Result<u64> {
        self.check(subscription)?;
        let tokens = tokens_raw(&update.tokens);
        let cfg = Session::namespace_update_raw(update, &tokens);
        let mut id = 0;
        // SAFETY: an advancing call; the configuration and what it points at
        // live through it.
        raw::check(unsafe {
            sys::moq_session_update_ns_sub(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
                &mut id,
            )
        })?;
        Ok(id)
    }

    /// Update a SUBSCRIBE_TRACKS this side made
    /// (`moq_session_update_subscribe_tracks`).
    ///
    /// # Errors
    /// As for [`update_namespace`](Session::update_namespace).
    pub fn update_subscribe_tracks(
        &mut self,
        request: TrackSubscription,
        update: &NamespaceUpdateConfig,
        now: Instant,
    ) -> Result<u64> {
        self.check(request)?;
        let tokens = tokens_raw(&update.tokens);
        let cfg = Session::namespace_update_raw(update, &tokens);
        let mut id = 0;
        // SAFETY: an advancing call; the configuration and what it points at
        // live through it.
        raw::check(unsafe {
            sys::moq_session_update_subscribe_tracks(
                self.ptr(),
                request.raw(),
                &cfg,
                now.as_micros(),
                &mut id,
            )
        })?;
        Ok(id)
    }

    /// Surface peer namespace-request updates for the application to answer
    /// (`moq_session_set_defer_namespace_updates`). Off, this side refuses
    /// every one with NOT_SUPPORTED. A no-op before draft 22.
    ///
    /// # Errors
    /// The native code.
    pub fn set_defer_namespace_updates(&mut self, defer: bool) -> Result<()> {
        // SAFETY: a non-advancing call on a live session.
        raw::check(unsafe { sys::moq_session_set_defer_namespace_updates(self.ptr(), defer) })
    }

    /// Accept the owed update of a peer's advertisement
    /// (`moq_session_accept_namespace_update`).
    ///
    /// # Errors
    /// [`Error::STALE_HANDLE`] for an ID that is not the owed one;
    /// [`Error::WRONG_STATE`] when none is owed; [`Error::WOULD_BLOCK`] and
    /// [`Error::NOMEM`] with nothing applied.
    pub fn accept_namespace_update(
        &mut self,
        announcement: Announcement,
        update_request_id: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(announcement)?;
        let cfg = update_acceptance(update_request_id);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_namespace_update(
                self.ptr(),
                announcement.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse the owed update of a peer's advertisement; the request ends
    /// (`moq_session_reject_namespace_update`).
    ///
    /// # Errors
    /// [`Error::INVAL`] for a retry interval or a nonempty redirect target
    /// (which an update refusal cannot carry), REDIRECT or a reason over
    /// 1024 bytes;
    /// as for [`accept_namespace_update`](Session::accept_namespace_update).
    pub fn reject_namespace_update(
        &mut self,
        announcement: Announcement,
        update_request_id: u64,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(announcement)?;
        let cfg = update_refusal(rejection, update_request_id)?;
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_namespace_update(
                self.ptr(),
                announcement.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Accept the owed update of a peer's namespace subscription
    /// (`moq_session_accept_ns_sub_update`).
    ///
    /// # Errors
    /// As for [`accept_namespace_update`](Session::accept_namespace_update).
    pub fn accept_namespace_subscription_update(
        &mut self,
        subscription: NamespaceSubscription,
        update_request_id: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let cfg = update_acceptance(update_request_id);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_ns_sub_update(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse the owed update of a peer's namespace subscription
    /// (`moq_session_reject_ns_sub_update`).
    ///
    /// # Errors
    /// As for [`reject_namespace_update`](Session::reject_namespace_update).
    pub fn reject_namespace_subscription_update(
        &mut self,
        subscription: NamespaceSubscription,
        update_request_id: u64,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let cfg = update_refusal(rejection, update_request_id)?;
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_ns_sub_update(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Accept the owed update of a peer's SUBSCRIBE_TRACKS
    /// (`moq_session_accept_subscribe_tracks_update`).
    ///
    /// # Errors
    /// As for [`accept_namespace_update`](Session::accept_namespace_update).
    pub fn accept_subscribe_tracks_update(
        &mut self,
        request: TrackSubscription,
        update_request_id: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let cfg = update_acceptance(update_request_id);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_subscribe_tracks_update(
                self.ptr(),
                request.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse the owed update of a peer's SUBSCRIBE_TRACKS
    /// (`moq_session_reject_subscribe_tracks_update`).
    ///
    /// # Errors
    /// As for [`reject_namespace_update`](Session::reject_namespace_update).
    pub fn reject_subscribe_tracks_update(
        &mut self,
        request: TrackSubscription,
        update_request_id: u64,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(request)?;
        let cfg = update_refusal(rejection, update_request_id)?;
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_subscribe_tracks_update(
                self.ptr(),
                request.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    // -- Subscription and publication updates -------------------------------

    /// Update a publication this side accepted
    /// (`moq_session_update_publication`). A PUBLISH-initiated subscription
    /// has no legacy filter: `update.filter` set is [`Error::INVAL`].
    ///
    /// # Errors
    /// As for [`update_subscription`](Session::update_subscription).
    pub fn update_publication(
        &mut self,
        publication: Publication,
        update: &SubscriptionUpdate,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        if update.filter.is_some() {
            return Err(Error::INVAL);
        }
        let tokens = tokens_raw(&update.tokens);
        let location = update.location.map(LocationFilter::as_raw);
        let fill_location = update
            .fill
            .and_then(|f| f.location)
            .map(LocationFilter::as_raw);
        let fill = update.fill.map(|f| fill_raw(&f, fill_location.as_ref()));
        let mut cfg = sized!(
            moq_publication_update_cfg_init_sized,
            sys::moq_publication_update_cfg_t
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
            sys::moq_session_update_publication(
                self.ptr(),
                publication.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Cancel a publication this side accepted (`moq_session_cancel_publication`):
    /// its request stream is reset and stopped with `error_code` (draft 16
    /// sends no code) and its data streams are stopped. No event follows.
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] unless established and not ended by its
    /// publisher; [`Error::WOULD_BLOCK`] with nothing changed.
    pub fn cancel_publication(
        &mut self,
        publication: Publication,
        error_code: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_cancel_publication(
                self.ptr(),
                publication.raw(),
                error_code,
                now.as_micros(),
            )
        })
    }

    /// Accept the owed update of a subscription this side publishes
    /// (`moq_session_accept_subscription_update`), on a session that defers
    /// them (see [`set_defer_subscription_updates`](Session::set_defer_subscription_updates)).
    ///
    /// # Errors
    /// [`Error::STALE_HANDLE`] for a superseded ID; [`Error::WRONG_STATE`]
    /// when none is owed; [`Error::WOULD_BLOCK`] with nothing applied.
    pub fn accept_subscription_update(
        &mut self,
        subscription: Subscription,
        update_request_id: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let cfg = update_acceptance(update_request_id);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_subscription_update(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse the owed update of a subscription this side publishes; the
    /// subscription ends with UPDATE_FAILED and the handle is stale
    /// (`moq_session_reject_subscription_update`).
    ///
    /// # Errors
    /// As for [`reject_namespace_update`](Session::reject_namespace_update).
    pub fn reject_subscription_update(
        &mut self,
        subscription: Subscription,
        update_request_id: u64,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let cfg = update_refusal(rejection, update_request_id)?;
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_subscription_update(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Accept the owed update of a publication this side made
    /// (`moq_session_accept_publication_update`).
    ///
    /// # Errors
    /// As for [`accept_subscription_update`](Session::accept_subscription_update).
    pub fn accept_publication_update(
        &mut self,
        publication: Publication,
        update_request_id: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        let cfg = update_acceptance(update_request_id);
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_accept_publication_update(
                self.ptr(),
                publication.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Refuse the owed update of a publication this side made; the
    /// publication ends with UPDATE_FAILED
    /// (`moq_session_reject_publication_update`).
    ///
    /// # Errors
    /// As for [`reject_namespace_update`](Session::reject_namespace_update).
    pub fn reject_publication_update(
        &mut self,
        publication: Publication,
        update_request_id: u64,
        rejection: &Rejection,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        let cfg = update_refusal(rejection, update_request_id)?;
        // SAFETY: an advancing call; the configuration lives through it.
        raw::check(unsafe {
            sys::moq_session_reject_publication_update(
                self.ptr(),
                publication.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// Switch deferred subscription-update answers after creation
    /// (`moq_session_set_defer_subscription_updates`); an answer already
    /// owed stays owed.
    ///
    /// # Errors
    /// The native code.
    pub fn set_defer_subscription_updates(&mut self, defer: bool) -> Result<()> {
        // SAFETY: a non-advancing call on a live session.
        raw::check(unsafe { sys::moq_session_set_defer_subscription_updates(self.ptr(), defer) })
    }

    /// Switch deferred FETCH ranges after creation
    /// (`moq_session_set_defer_fetch_ranges`), for FETCHes that arrive
    /// afterwards.
    ///
    /// # Errors
    /// The native code.
    pub fn set_defer_fetch_ranges(&mut self, defer: bool) -> Result<()> {
        // SAFETY: a non-advancing call on a live session.
        raw::check(unsafe { sys::moq_session_set_defer_fetch_ranges(self.ptr(), defer) })
    }

    fn publish_state_raw(
        config: &PublishStateConfig,
        filter: Option<&sys::moq_location_filter_cfg_t>,
    ) -> sys::moq_publish_state_cfg_t {
        let mut cfg = sized!(
            moq_publish_state_cfg_init_sized,
            sys::moq_publish_state_cfg_t
        );
        if let Some(forward) = config.forward {
            cfg.has_forward = true;
            cfg.forward = forward;
        }
        cfg.filter = filter.map_or(ptr::null(), |f| f as *const _);
        cfg
    }

    /// State the forward state or filter in effect on a subscription this
    /// side publishes (`moq_session_notify_subscription_state`, draft 22).
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::INVAL`] for a value
    /// the subscriber has not authorized.
    pub fn notify_subscription_state(
        &mut self,
        subscription: Subscription,
        config: &PublishStateConfig,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        let filter = config.filter.map(LocationFilter::as_raw);
        let cfg = Session::publish_state_raw(config, filter.as_ref());
        // SAFETY: an advancing call; the configuration and its filter live
        // through it.
        raw::check(unsafe {
            sys::moq_session_notify_subscription_state(
                self.ptr(),
                subscription.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    /// The same notification for a publication this side made
    /// (`moq_session_notify_publication_state`).
    ///
    /// # Errors
    /// As for [`notify_subscription_state`](Session::notify_subscription_state).
    pub fn notify_publication_state(
        &mut self,
        publication: Publication,
        config: &PublishStateConfig,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        let filter = config.filter.map(LocationFilter::as_raw);
        let cfg = Session::publish_state_raw(config, filter.as_ref());
        // SAFETY: an advancing call; the configuration and its filter live
        // through it.
        raw::check(unsafe {
            sys::moq_session_notify_publication_state(
                self.ptr(),
                publication.raw(),
                &cfg,
                now.as_micros(),
            )
        })
    }

    // -- FETCH updates (draft 22) -------------------------------------------

    /// Update a FETCH this side requested (`moq_session_update_fetch`). The
    /// acknowledgement completes it without an event.
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::INVAL`] for an empty
    /// update; [`Error::WRONG_STATE`] while one is outstanding;
    /// [`Error::REQUEST_BLOCKED`]; [`Error::WOULD_BLOCK`].
    pub fn update_fetch(&mut self, fetch: Fetch, update: &FetchUpdate, now: Instant) -> Result<()> {
        self.check(fetch)?;
        let tokens = tokens_raw(&update.tokens);
        let mut cfg = sized!(moq_fetch_update_cfg_init_sized, sys::moq_fetch_update_cfg_t);
        if let Some(priority) = update.subscriber_priority {
            cfg.has_subscriber_priority = true;
            cfg.subscriber_priority = priority;
        }
        cfg.auth_tokens = tokens_ptr(&tokens);
        cfg.auth_token_count = tokens.len();
        // SAFETY: an advancing call; the configuration and its tokens live
        // through it.
        raw::check(unsafe {
            sys::moq_session_update_fetch(self.ptr(), fetch.raw(), &cfg, now.as_micros())
        })
    }

    // -- Per-request GOAWAY (drafts 18 and 22) ------------------------------

    /// Migrate one request: a GOAWAY on its own stream
    /// (`moq_session_request_goaway_*`). The peer re-issues it at
    /// `new_session_uri` (empty: this session; a client must leave it
    /// empty); `timeout_ms` is the migration hint (zero: none).
    ///
    /// # Errors
    /// [`Error::INVAL`] on draft 16, for a client's URI or for a family this
    /// crate does not know; [`Error::WRONG_STATE`] for an ineligible request
    /// or a second GOAWAY on one; [`Error::WRONG_SESSION`].
    pub fn request_goaway(
        &mut self,
        request: RequestHandle,
        new_session_uri: &[u8],
        timeout_ms: u64,
        now: Instant,
    ) -> Result<()> {
        let mut cfg = sized!(
            moq_request_goaway_cfg_init_sized,
            sys::moq_request_goaway_cfg_t
        );
        cfg.new_session_uri = raw::span(new_session_uri);
        cfg.timeout_ms = timeout_ms;
        let (s, t) = (self.ptr(), now.as_micros());
        let rc = match request {
            RequestHandle::Subscription(h) => {
                self.check(h)?;
                // SAFETY: an advancing call; the configuration and its URI
                // live through it.
                unsafe { sys::moq_session_request_goaway_subscribe(s, h.raw(), &cfg, t) }
            }
            RequestHandle::Fetch(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_fetch(s, h.raw(), &cfg, t) }
            }
            RequestHandle::TrackStatus(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_track_status(s, h.raw(), &cfg, t) }
            }
            RequestHandle::Announcement(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_namespace(s, h.raw(), &cfg, t) }
            }
            RequestHandle::NamespaceSubscription(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_ns_sub(s, h.raw(), &cfg, t) }
            }
            RequestHandle::Publication(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_publish(s, h.raw(), &cfg, t) }
            }
            RequestHandle::TrackSubscription(h) => {
                self.check(h)?;
                // SAFETY: as above.
                unsafe { sys::moq_session_request_goaway_subscribe_tracks(s, h.raw(), &cfg, t) }
            }
            RequestHandle::Other { .. } => return Err(Error::INVAL),
        };
        raw::check(rc)
    }

    // -- Fills (draft 22) ---------------------------------------------------

    /// Open the stream of a fill this side owes ([`Event::FillRequest`](crate::Event::FillRequest))
    /// with its header (`moq_session_open_fill`).
    ///
    /// # Errors
    /// [`Error::STALE_HANDLE`] after the fill's terminal;
    /// [`Error::WOULD_BLOCK`] with nothing committed.
    pub fn open_fill(&mut self, fill: Fill, now: Instant) -> Result<()> {
        self.check(fill)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe { sys::moq_session_open_fill(self.ptr(), fill.raw(), now.as_micros()) })
    }

    /// Write one object on a fill's stream (`moq_session_write_fill_object`).
    /// The session takes its own references to the buffers; on a refusal it
    /// keeps none and they come back while they are still the caller's
    /// alone.
    ///
    /// # Errors
    /// The native code, with the payload and properties.
    pub fn write_fill_object(
        &mut self,
        fill: Fill,
        object: FetchObjectWrite,
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(fill) {
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
        // SAFETY: an advancing call; the buffers are live references this
        // call holds; C keeps the caller's ownership and queued actions take
        // their own references.
        let rc = unsafe {
            sys::moq_session_write_fill_object(self.ptr(), fill.raw(), &cfg, now.as_micros())
        };
        Session::written(
            rc,
            Some(object.payload),
            object.properties,
            Kept::EveryRefusal,
        )
    }

    /// Write an end-of-range marker on a fill's stream
    /// (`moq_session_write_fill_range`).
    ///
    /// # Errors
    /// The native code.
    pub fn write_fill_range(
        &mut self,
        fill: Fill,
        range: FetchRange,
        end: Location,
        now: Instant,
    ) -> Result<()> {
        self.check(fill)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_write_fill_range(
                self.ptr(),
                fill.raw(),
                range.raw(),
                end.group_id,
                end.object_id,
                now.as_micros(),
            )
        })
    }

    /// Finish a fill's stream with its FIN (`moq_session_end_fill`).
    ///
    /// # Errors
    /// The native code.
    pub fn end_fill(&mut self, fill: Fill, now: Instant) -> Result<()> {
        self.check(fill)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe { sys::moq_session_end_fill(self.ptr(), fill.raw(), now.as_micros()) })
    }

    /// Reset a fill's stream with `error_code` (`moq_session_abort_fill`);
    /// an unopened fill is opened with its header first.
    ///
    /// # Errors
    /// The native code.
    pub fn abort_fill(&mut self, fill: Fill, error_code: u64, now: Instant) -> Result<()> {
        self.check(fill)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_abort_fill(self.ptr(), fill.raw(), error_code, now.as_micros())
        })
    }

    /// Stop one fill this side requested, leaving its subscription
    /// (`moq_session_cancel_fill`).
    ///
    /// # Errors
    /// [`Error::WOULD_BLOCK`] with the obligation retained.
    pub fn cancel_fill(&mut self, fill: Fill, now: Instant) -> Result<()> {
        self.check(fill)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe { sys::moq_session_cancel_fill(self.ptr(), fill.raw(), now.as_micros()) })
    }

    // -- Request capacity ---------------------------------------------------

    /// Raise this side's request capacity and tell the peer
    /// (`moq_session_grant_request_capacity`, draft 16).
    ///
    /// # Errors
    /// [`Error::INVAL`] unless above the current capacity, and on drafts 18
    /// and 22, which have none, with nothing sent or changed;
    /// [`Error::WRONG_STATE`] on draft 16 before the session is established
    /// or once it is closed; [`Error::WOULD_BLOCK`] with the capacity
    /// unchanged.
    pub fn grant_request_capacity(&mut self, capacity: u64, now: Instant) -> Result<()> {
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_grant_request_capacity(self.ptr(), capacity, now.as_micros())
        })
    }

    // -- Datagrams, range markers and Track Properties ----------------------

    /// Send a status-only object datagram on a subscription this side
    /// publishes (`moq_session_send_status_datagram`).
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`]; the native code ([`Error::WRONG_STATE`]
    /// unless established, [`Error::WOULD_BLOCK`]).
    pub fn send_status_datagram(
        &mut self,
        subscription: Subscription,
        location: Location,
        publisher_priority: u8,
        status: ObjectStatus,
        now: Instant,
    ) -> Result<()> {
        self.check(subscription)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_send_status_datagram(
                self.ptr(),
                subscription.raw(),
                location.group_id,
                location.object_id,
                publisher_priority,
                status.code(),
                now.as_micros(),
            )
        })
    }

    /// Send an object datagram on a publication this side made
    /// (`moq_session_send_pub_object_datagram`), with the custody of
    /// [`send_object_datagram`](Session::send_object_datagram): the bytes
    /// are copied, the payload is consumed on success and comes back on any
    /// refusal.
    ///
    /// # Errors
    /// The native code, with the payload back.
    #[allow(
        clippy::too_many_arguments,
        reason = "the native call's own parameters"
    )]
    pub fn send_publication_datagram(
        &mut self,
        publication: Publication,
        location: Location,
        publisher_priority: u8,
        end_of_group: bool,
        payload: Buffer,
        properties: &[u8],
        now: Instant,
    ) -> std::result::Result<(), WriteError> {
        if let Err(error) = self.check(publication) {
            return Session::not_written(error, Some(payload), None);
        }
        // SAFETY: an advancing call; the payload and the property bytes are
        // borrowed for it, and C copies what it keeps.
        let rc = unsafe {
            sys::moq_session_send_pub_object_datagram(
                self.ptr(),
                publication.raw(),
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

    /// Send a status-only object datagram on a publication this side made
    /// (`moq_session_send_pub_status_datagram`).
    ///
    /// # Errors
    /// As for [`send_status_datagram`](Session::send_status_datagram).
    pub fn send_publication_status_datagram(
        &mut self,
        publication: Publication,
        location: Location,
        publisher_priority: u8,
        status: ObjectStatus,
        now: Instant,
    ) -> Result<()> {
        self.check(publication)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_send_pub_status_datagram(
                self.ptr(),
                publication.raw(),
                location.group_id,
                location.object_id,
                publisher_priority,
                status.code(),
                now.as_micros(),
            )
        })
    }

    /// Mark a leading gap on an accepted FETCH: every Location before
    /// `first_group` (`moq_session_write_fetch_range_before_group`). The
    /// session supplies the draft's highest object ID.
    ///
    /// # Errors
    /// [`Error::INVAL`] for group zero or a kind the draft has no encoding
    /// for, with nothing queued.
    pub fn write_fetch_range_before_group(
        &mut self,
        fetch: Fetch,
        range: FetchRange,
        first_group: u64,
        now: Instant,
    ) -> Result<()> {
        self.check(fetch)?;
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_write_fetch_range_before_group(
                self.ptr(),
                fetch.raw(),
                range.raw(),
                first_group,
                now.as_micros(),
            )
        })
    }

    /// `properties` with the publisher's preferred group order added, for
    /// an accept or a PUBLISH, encoded for this session's draft
    /// (`moq_session_track_properties_add_group_order`).
    ///
    /// # Errors
    /// [`Error::INVAL`] when `properties` carries another order or does not
    /// parse.
    pub fn track_properties_with_group_order(
        &self,
        properties: &[u8],
        order: GroupOrder,
    ) -> Result<Vec<u8>> {
        // The preference adds at most 16 bytes.
        let mut out = vec![0; properties.len().checked_add(16).ok_or(Error::INVAL)?];
        let mut len = 0;
        // SAFETY: an observing call; `properties` is readable and `out`
        // writable for their lengths, and they do not overlap.
        raw::check(unsafe {
            sys::moq_session_track_properties_add_group_order(
                self.ptr(),
                properties.as_ptr(),
                properties.len(),
                order.code(),
                out.as_mut_ptr(),
                out.len(),
                &mut len,
            )
        })?;
        built(out, len)
    }

    /// `properties` with the publisher's delivery timeouts added, exact
    /// milliseconds, for an accept or a PUBLISH
    /// (`moq_session_track_properties_add_delivery_timeouts`). Draft 16 has
    /// one timeout: both, equal and nonzero.
    ///
    /// # Errors
    /// [`Error::INVAL`] for no selection, a value the draft cannot carry, a
    /// conflicting existing value or input that does not parse.
    pub fn track_properties_with_delivery_timeouts(
        &self,
        properties: &[u8],
        object_ms: Option<u64>,
        subgroup_ms: Option<u64>,
    ) -> Result<Vec<u8>> {
        let mut cfg = sized!(
            moq_track_delivery_timeouts_cfg_init_sized,
            sys::moq_track_delivery_timeouts_cfg_t
        );
        if let Some(ms) = object_ms {
            cfg.has_object_delivery_timeout = true;
            cfg.object_delivery_timeout_ms = ms;
        }
        if let Some(ms) = subgroup_ms {
            cfg.has_subgroup_delivery_timeout = true;
            cfg.subgroup_delivery_timeout_ms = ms;
        }
        // The two timeouts add at most 32 bytes.
        let mut out = vec![0; properties.len().checked_add(32).ok_or(Error::INVAL)?];
        let mut len = 0;
        // SAFETY: an observing call; the configuration is borrowed for it,
        // `properties` is readable and `out` writable for their lengths, and
        // they do not overlap.
        raw::check(unsafe {
            sys::moq_session_track_properties_add_delivery_timeouts(
                self.ptr(),
                properties.as_ptr(),
                properties.len(),
                &cfg,
                out.as_mut_ptr(),
                out.len(),
                &mut len,
            )
        })?;
        built(out, len)
    }
}

/// A built output of `len` bytes, which the library never states beyond
/// what it was given.
fn built(mut out: Vec<u8>, len: usize) -> Result<Vec<u8>> {
    if len > out.len() {
        return Err(Error::ABI_MISMATCH);
    }
    out.truncate(len);
    Ok(out)
}
