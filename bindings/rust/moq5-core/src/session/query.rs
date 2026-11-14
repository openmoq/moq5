//! Observations: the filter and window in effect, owed updates and how they
//! resolved, a PUBLISH's initial parameters, cluster metadata, request
//! capacity and what the draft can carry. None advances the session; a
//! borrowed answer holds the session shared, so no advancing call can run
//! while it lives.

use std::mem;

use super::event::{namespace_update_record, NamespaceUpdate, UpdateResolution};
use super::handle::{
    Announcement, NamespaceSubscription, Publication, Subscription, TrackSubscription,
};
use super::owner::Session;
use super::raw::{self, field_end};
use super::view::{self, ClusterView, Reader};
use crate::{sys, Error, GroupOrder, Location, LocationFilter, Namespace, Result};

/// The Location window the filter in effect resolved to
/// (`moq_filter_window_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterWindow {
    /// No window is resolved yet (before the answer): every Location is
    /// inside.
    Unresolved,
    /// Resolved, and no Location is inside.
    Empty,
    /// The Locations from `start`, through `end` when bounded.
    Range {
        /// The first Location inside.
        start: Location,
        /// The bound, `None` when open-ended.
        end: Option<WindowEnd>,
    },
}

/// The bound of a [`FilterWindow`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowEnd {
    /// The whole of this group is inside.
    Group(u64),
    /// Through this Location, inclusive.
    Object(Location),
}

/// The update whose answer is owed on a subscription or publication this
/// side publishes.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwedUpdate {
    /// The Request ID the answer names (the newest covered update).
    pub update_request_id: u64,
    /// The replacement filter the cumulative update selects, when it
    /// selects one; the filter in effect stays the old one until accepted.
    pub filter: Option<LocationFilter>,
}

/// A peer PUBLISH's immutable initial parameters
/// (`moq_publish_initial_parameters_t`, draft 22).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishInitialParameters {
    /// The effective initial subscriber priority.
    pub subscriber_priority: u8,
    /// The PUBLISH carried SUBSCRIBER_PRIORITY.
    pub subscriber_priority_carried: bool,
    /// The effective initial group order; `None` is the publisher's.
    pub group_order: Option<GroupOrder>,
    /// The PUBLISH carried GROUP_ORDER.
    pub group_order_carried: bool,
    /// The Location Filter, when carried.
    pub location: Option<LocationFilter>,
    /// The object delivery timeout, when carried, exact milliseconds.
    pub object_delivery_timeout_ms: Option<u64>,
    /// The subgroup delivery timeout, when carried, exact milliseconds.
    pub subgroup_delivery_timeout_ms: Option<u64>,
}

/// The negotiated cluster metadata of a session (`moq_cluster_info_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterInfo {
    /// This side enabled the extension.
    pub enabled: bool,
    /// Both sides negotiated it.
    pub negotiated: bool,
    /// This side's Hop ID.
    pub local_hop_id: u64,
    /// The peer's Hop ID.
    pub peer_hop_id: u64,
    /// The peer's relay cost (1 when it stated none).
    pub peer_relay_cost: u64,
    /// The peer stated its relay cost.
    pub peer_relay_cost_stated: bool,
}

/// A native output the library stamped, checked against its floor and the
/// size passed.
fn stamped<T>(stamp: u32, floor: usize) -> Result<()> {
    raw::stamp(stamp, floor, mem::size_of::<T>())
}

fn window(out: &sys::moq_filter_window_t) -> Result<FilterWindow> {
    stamped::<sys::moq_filter_window_t>(
        out.struct_size,
        field_end!(sys::moq_filter_window_t, end_object, u64),
    )?;
    let start = view::location(out.start_group, out.start_object);
    Ok(if !out.resolved {
        FilterWindow::Unresolved
    } else if out.empty {
        FilterWindow::Empty
    } else {
        FilterWindow::Range {
            start,
            end: match (out.has_end, out.has_end_object) {
                (false, _) => None,
                (true, false) => Some(WindowEnd::Group(out.end_group)),
                (true, true) => Some(WindowEnd::Object(view::location(
                    out.end_group,
                    out.end_object,
                ))),
            },
        }
    })
}

fn filter(out: &sys::moq_location_filter_cfg_t) -> Result<LocationFilter> {
    stamped::<sys::moq_location_filter_cfg_t>(
        out.struct_size,
        sys::MOQ_LOCATION_FILTER_CFG_V0_SIZE as usize,
    )?;
    view::location_filter(out)
}

fn owed(id: u64, has_filter: bool, out: &sys::moq_location_filter_cfg_t) -> Result<OwedUpdate> {
    Ok(OwedUpdate {
        update_request_id: id,
        filter: if has_filter { Some(filter(out)?) } else { None },
    })
}

fn resolution(out: &sys::moq_update_resolution_t) -> Result<UpdateResolution> {
    stamped::<sys::moq_update_resolution_t>(
        out.struct_size,
        sys::MOQ_UPDATE_RESOLUTION_V0_SIZE as usize,
    )?;
    UpdateResolution::from_raw(out)
}

fn group_order(raw: u32) -> Result<GroupOrder> {
    // C reports DEFAULT only for a stale handle.
    view::group_order(raw)?.ok_or(Error::STALE_HANDLE)
}

impl Session {
    /// The filter in effect on a subscription as this side knows it
    /// (`moq_session_subscription_filter_sized`); `Unconstrained` when
    /// unfiltered.
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`]; [`Error::STALE_HANDLE`] for a retired
    /// handle.
    pub fn subscription_filter(&self, subscription: Subscription) -> Result<LocationFilter> {
        self.check(subscription)?;
        let mut out = sys::moq_location_filter_cfg_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_subscription_filter_sized(
                self.ptr(),
                subscription.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        filter(&out)
    }

    /// The filter in effect on a publication
    /// (`moq_session_publication_filter_sized`).
    ///
    /// # Errors
    /// As for [`subscription_filter`](Session::subscription_filter).
    pub fn publication_filter(&self, publication: Publication) -> Result<LocationFilter> {
        self.check(publication)?;
        let mut out = sys::moq_location_filter_cfg_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_publication_filter_sized(
                self.ptr(),
                publication.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        filter(&out)
    }

    /// The window the filter in effect on a subscription resolved to
    /// (`moq_session_subscription_window`). A publisher writing to the raw
    /// session checks its objects against it; the session does not.
    ///
    /// # Errors
    /// As for [`subscription_filter`](Session::subscription_filter).
    pub fn subscription_window(&self, subscription: Subscription) -> Result<FilterWindow> {
        self.check(subscription)?;
        let mut out = sys::moq_filter_window_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_subscription_window(
                self.ptr(),
                subscription.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        window(&out)
    }

    /// The window of a publication (`moq_session_publication_window`).
    ///
    /// # Errors
    /// As for [`subscription_filter`](Session::subscription_filter).
    pub fn publication_window(&self, publication: Publication) -> Result<FilterWindow> {
        self.check(publication)?;
        let mut out = sys::moq_filter_window_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_publication_window(
                self.ptr(),
                publication.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        window(&out)
    }

    /// The group order in effect for a subscription: the subscriber's,
    /// else the publisher's, else ascending
    /// (`moq_session_subscription_group_order`).
    ///
    /// # Errors
    /// [`Error::WRONG_SESSION`]; [`Error::STALE_HANDLE`].
    pub fn subscription_group_order(&self, subscription: Subscription) -> Result<GroupOrder> {
        self.check(subscription)?;
        // SAFETY: an observing call on a live session.
        group_order(unsafe {
            sys::moq_session_subscription_group_order(self.ptr(), subscription.raw())
        })
    }

    /// The group order in effect for a publication
    /// (`moq_session_publication_group_order`).
    ///
    /// # Errors
    /// As for [`subscription_group_order`](Session::subscription_group_order).
    pub fn publication_group_order(&self, publication: Publication) -> Result<GroupOrder> {
        self.check(publication)?;
        // SAFETY: an observing call on a live session.
        group_order(unsafe {
            sys::moq_session_publication_group_order(self.ptr(), publication.raw())
        })
    }

    /// The owed update of a subscription this side publishes
    /// (`moq_session_subscription_owed_update_sized`).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] when none is owed; [`Error::STALE_HANDLE`]
    /// (also for one the session ended itself); [`Error::WRONG_SESSION`].
    pub fn subscription_owed_update(&self, subscription: Subscription) -> Result<OwedUpdate> {
        self.check(subscription)?;
        let (mut id, mut has_filter) = (0, false);
        let mut out = sys::moq_location_filter_cfg_t::default();
        // SAFETY: an observing call; every output is writable, the filter
        // for the size passed.
        raw::check(unsafe {
            sys::moq_session_subscription_owed_update_sized(
                self.ptr(),
                subscription.raw(),
                &mut id,
                &mut has_filter,
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        owed(id, has_filter, &out)
    }

    /// The owed update of a publication this side made
    /// (`moq_session_publication_owed_update_sized`), observable before its
    /// event is polled.
    ///
    /// # Errors
    /// As for [`subscription_owed_update`](Session::subscription_owed_update).
    pub fn publication_owed_update(&self, publication: Publication) -> Result<OwedUpdate> {
        self.check(publication)?;
        let (mut id, mut has_filter) = (0, false);
        let mut out = sys::moq_location_filter_cfg_t::default();
        // SAFETY: an observing call; every output is writable, the filter
        // for the size passed.
        raw::check(unsafe {
            sys::moq_session_publication_owed_update_sized(
                self.ptr(),
                publication.raw(),
                &mut id,
                &mut has_filter,
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        owed(id, has_filter, &out)
    }

    /// How the owed update named `update_request_id` resolved
    /// (`moq_session_get_subscription_update_resolution`, draft 22).
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::WRONG_STATE`] when
    /// none is owed; [`Error::STALE_HANDLE`] for a superseded ID.
    pub fn subscription_update_resolution(
        &self,
        subscription: Subscription,
        update_request_id: u64,
    ) -> Result<UpdateResolution> {
        self.check(subscription)?;
        let mut out = sys::moq_update_resolution_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_get_subscription_update_resolution(
                self.ptr(),
                subscription.raw(),
                update_request_id,
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        resolution(&out)
    }

    /// The same for a publication
    /// (`moq_session_get_publication_update_resolution`).
    ///
    /// # Errors
    /// As for [`subscription_update_resolution`](Session::subscription_update_resolution).
    pub fn publication_update_resolution(
        &self,
        publication: Publication,
        update_request_id: u64,
    ) -> Result<UpdateResolution> {
        self.check(publication)?;
        let mut out = sys::moq_update_resolution_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_get_publication_update_resolution(
                self.ptr(),
                publication.raw(),
                update_request_id,
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        resolution(&out)
    }

    /// The owed update of a peer's advertisement, before or after its event
    /// (`moq_session_namespace_owed_update`, draft 22).
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::WRONG_STATE`] when
    /// none is owed; [`Error::STALE_HANDLE`]; [`Error::WRONG_SESSION`].
    pub fn namespace_owed_update(&self, announcement: Announcement) -> Result<NamespaceUpdate<'_>> {
        self.check(announcement)?;
        let mut out = sys::moq_namespace_update_record_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_namespace_owed_update(
                self.ptr(),
                announcement.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        self.owed_namespace_update(&out)
    }

    /// The owed update of a peer's namespace subscription
    /// (`moq_session_ns_sub_owed_update`).
    ///
    /// # Errors
    /// As for [`namespace_owed_update`](Session::namespace_owed_update).
    pub fn namespace_subscription_owed_update(
        &self,
        subscription: NamespaceSubscription,
    ) -> Result<NamespaceUpdate<'_>> {
        self.check(subscription)?;
        let mut out = sys::moq_namespace_update_record_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_ns_sub_owed_update(
                self.ptr(),
                subscription.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        self.owed_namespace_update(&out)
    }

    /// The owed update of a peer's SUBSCRIBE_TRACKS
    /// (`moq_session_subscribe_tracks_owed_update`).
    ///
    /// # Errors
    /// As for [`namespace_owed_update`](Session::namespace_owed_update).
    pub fn subscribe_tracks_owed_update(
        &self,
        request: TrackSubscription,
    ) -> Result<NamespaceUpdate<'_>> {
        self.check(request)?;
        let mut out = sys::moq_namespace_update_record_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_subscribe_tracks_owed_update(
                self.ptr(),
                request.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        self.owed_namespace_update(&out)
    }

    fn owed_namespace_update(
        &self,
        out: &sys::moq_namespace_update_record_t,
    ) -> Result<NamespaceUpdate<'_>> {
        stamped::<sys::moq_namespace_update_record_t>(
            out.struct_size,
            sys::MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE,
        )?;
        // SAFETY: the record's pointers borrow session storage valid until
        // the next advancing call, and the reader's lifetime is the shared
        // borrow of `self`, during which no advancing call (each takes
        // `&mut self`) can run. The record holds no owned reference.
        let rd = unsafe { Reader::new(out.struct_size, self.owner) };
        namespace_update_record(&rd, out)
    }

    /// The accepted cluster metadata of an advertisement
    /// (`moq_session_namespace_cluster`).
    ///
    /// # Errors
    /// The native code; [`Error::WRONG_SESSION`].
    pub fn namespace_cluster(&self, announcement: Announcement) -> Result<ClusterView<'_>> {
        self.check(announcement)?;
        let mut out = sys::moq_cluster_metadata_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_namespace_cluster(
                self.ptr(),
                announcement.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        // SAFETY: the Hop IDs borrow session storage valid until the next
        // advancing call, which cannot run while `self` is borrowed.
        let rd = unsafe { Reader::new(0, self.owner) };
        rd.cluster(&out)
    }

    /// The session's negotiated cluster metadata (`moq_session_cluster_info`).
    ///
    /// # Errors
    /// The native code.
    pub fn cluster_info(&self) -> Result<ClusterInfo> {
        let mut out = sys::moq_cluster_info_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_cluster_info(self.ptr(), &mut out, mem::size_of_val(&out))
        })?;
        stamped::<sys::moq_cluster_info_t>(out.struct_size, sys::MOQ_CLUSTER_INFO_V0_SIZE)?;
        Ok(ClusterInfo {
            enabled: out.enabled,
            negotiated: out.negotiated,
            local_hop_id: out.local_hop_id,
            peer_hop_id: out.peer_hop_id,
            peer_relay_cost: out.peer_relay_cost,
            peer_relay_cost_stated: out.peer_has_relay_cost,
        })
    }

    /// A peer PUBLISH's initial parameters
    /// (`moq_session_get_publish_initial_parameters`, draft 22).
    ///
    /// # Errors
    /// [`Error::UNSUPPORTED`] before draft 22; [`Error::WRONG_STATE`] for
    /// this side's own PUBLISH; [`Error::STALE_HANDLE`].
    pub fn publish_initial_parameters(
        &self,
        publication: Publication,
    ) -> Result<PublishInitialParameters> {
        self.check(publication)?;
        let mut out = sys::moq_publish_initial_parameters_t::default();
        // SAFETY: an observing call; `out` is writable for the size passed.
        raw::check(unsafe {
            sys::moq_session_get_publish_initial_parameters(
                self.ptr(),
                publication.raw(),
                &mut out,
                mem::size_of_val(&out),
            )
        })?;
        stamped::<sys::moq_publish_initial_parameters_t>(
            out.struct_size,
            sys::MOQ_PUBLISH_INITIAL_PARAMETERS_V0_SIZE as usize,
        )?;
        let has = |flag: u32| out.flags & flag != 0;
        Ok(PublishInitialParameters {
            subscriber_priority: out.subscriber_priority,
            subscriber_priority_carried: has(sys::MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY),
            group_order: view::group_order(out.group_order)?,
            group_order_carried: has(sys::MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER),
            location: if has(sys::MOQ_PUBLISH_INITIAL_HAS_LOCATION) {
                Some(view::location_filter(&out.location)?)
            } else {
                None
            },
            object_delivery_timeout_ms: has(sys::MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT)
                .then_some(out.object_delivery_timeout_ms),
            subgroup_delivery_timeout_ms: has(sys::MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT)
                .then_some(out.subgroup_delivery_timeout_ms),
        })
    }

    /// The peer's request capacity: what its setup stated, raised by its
    /// later grants (draft 16; zero when it stated none, and on drafts 18
    /// and 22).
    #[must_use]
    pub fn peer_request_capacity(&self) -> u64 {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_peer_request_capacity(self.ptr()) }
    }

    /// This side's request capacity as advertised (draft 16; zero on drafts
    /// 18 and 22).
    #[must_use]
    pub fn local_request_capacity(&self) -> u64 {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_local_request_capacity(self.ptr()) }
    }

    /// The peer's MAX_AUTH_TOKEN_CACHE_SIZE (zero when it sent none).
    #[must_use]
    pub fn peer_auth_token_cache_size(&self) -> u64 {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_peer_auth_token_cache_size(self.ptr()) }
    }

    /// The draft's FETCH objects can carry the datagram preference (drafts
    /// 18 and 22).
    #[must_use]
    pub fn supports_fetch_datagram(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_supports_fetch_datagram(self.ptr()) }
    }

    /// The draft has joining FETCHes (drafts 16 and 18; draft 22 joins with
    /// a fill instead).
    #[must_use]
    pub fn supports_joining_fetch(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_supports_joining_fetch(self.ptr()) }
    }

    /// The draft allows several subscriptions to one track (draft 22).
    #[must_use]
    pub fn supports_concurrent_subscriptions(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_supports_concurrent_subscriptions(self.ptr()) }
    }

    /// Record that an object of a track was published or received,
    /// advancing its Largest Object (`moq_session_note_object_published`);
    /// a location not past the current one is a no-op.
    ///
    /// # Errors
    /// [`Error::INVAL`] for a name or location the draft cannot hold;
    /// [`Error::NOMEM`] at the history limit; [`Error::WRONG_STATE`] when
    /// the session is not active.
    pub fn note_object_published(
        &mut self,
        namespace: &Namespace,
        track_name: &[u8],
        location: Location,
    ) -> Result<()> {
        let ns = namespace.as_raw();
        // SAFETY: a non-advancing call; the namespace and name are borrowed
        // for it.
        raw::check(unsafe {
            sys::moq_session_note_object_published(
                self.ptr(),
                &ns,
                raw::span(track_name),
                location.group_id,
                location.object_id,
            )
        })
    }

    /// Opt in to [`ActionDetail::SetDataPriority`](crate::ActionDetail::SetDataPriority)
    /// (`moq_session_set_data_priority_updates`), before any data stream
    /// opens.
    ///
    /// # Errors
    /// [`Error::INVAL`] when the action queue holds fewer than four slots.
    pub fn set_data_priority_updates(&mut self, enabled: bool) -> Result<()> {
        // SAFETY: a non-advancing call on a live session.
        raw::check(unsafe { sys::moq_session_set_data_priority_updates(self.ptr(), enabled) })
    }
}
