//! Borrowed views of what a polled event points into, and the reader that
//! decodes a record under its stamped size.

use std::fmt;
use std::marker::PhantomData;

use super::config::Perspective;
use super::handle::{
    Announcement, Fetch, Fill, NamespaceSubscription, OwnerId, Publication, Subgroup, Subscription,
    TrackStatusRequest, TrackSubscription,
};
use super::payload::Payload;
use super::raw;
use crate::{sys, Error, GroupOrder, Location, LocationFilter, Namespace, Result};

/// A track namespace borrowed from a polled event: its parts, each a byte
/// string, in order. A root namespace (drafts 18 and 22) has no parts.
#[derive(Clone, Copy)]
pub struct NamespaceView<'session> {
    parts: &'session [sys::moq_bytes_t],
}

impl<'session> NamespaceView<'session> {
    /// The number of parts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    /// No parts: the root namespace, or an empty prefix.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// The parts, in order.
    pub fn parts(&self) -> impl Iterator<Item = &'session [u8]> + 'session {
        self.parts.iter().map(|part| {
            if part.len == 0 {
                &[][..]
            } else {
                // SAFETY: every part was checked non-null with its length
                // when the view was built, and the record's borrow keeps it
                // valid for 'session.
                unsafe { std::slice::from_raw_parts(part.data, part.len) }
            }
        })
    }

    /// An owned copy.
    ///
    /// # Errors
    /// [`Error::INVAL`] when a part is empty, which no namespace C reports
    /// may contain.
    pub fn to_namespace(&self) -> Result<Namespace> {
        Namespace::try_new(self.parts().map(<[u8]>::to_vec))
    }
}

impl fmt::Debug for NamespaceView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NamespaceView(")?;
        for (index, part) in self.parts().enumerate() {
            if index > 0 {
                f.write_str("/")?;
            }
            f.write_str(&String::from_utf8_lossy(part))?;
        }
        f.write_str(")")
    }
}

/// One authorization token an event carried, resolved against the session's
/// token cache.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedToken<'session> {
    /// The token type.
    pub token_type: u64,
    /// The token value.
    pub value: &'session [u8],
}

/// The authorization tokens an event carried, in wire order.
#[derive(Clone, Copy, Default)]
pub struct Tokens<'session> {
    tokens: &'session [sys::moq_resolved_token_t],
}

impl<'session> Tokens<'session> {
    /// The number of tokens.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// No tokens.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// The tokens, in order.
    pub fn iter(&self) -> impl Iterator<Item = ResolvedToken<'session>> + 'session {
        self.tokens.iter().map(|token| ResolvedToken {
            token_type: token.token_type,
            value: if token.token_value.len == 0 {
                &[][..]
            } else {
                // SAFETY: checked non-null with its length when the view was
                // built; valid for the record's borrow.
                unsafe { std::slice::from_raw_parts(token.token_value.data, token.token_value.len) }
            },
        })
    }
}

impl fmt::Debug for Tokens<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Cluster draft-01 namespace metadata borrowed from a record
/// (`moq_cluster_metadata_t`): the Hop IDs from the original publisher to
/// the sender, and the route cost when present. No hop is an absent
/// HOP_PATH (a partial update, or a session that did not negotiate the
/// extension). Zero is a valid Hop ID and a valid cost.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterView<'session> {
    /// The Hop IDs, in path order.
    pub hop_ids: &'session [u64],
    /// The route cost, when present.
    pub route_cost: Option<u64>,
}

impl ClusterView<'_> {
    /// An owned copy.
    #[must_use]
    pub fn to_path(&self) -> crate::ClusterPath {
        crate::ClusterPath {
            hop_ids: self.hop_ids.to_vec(),
            route_cost: self.route_cost,
        }
    }
}

/// Why a request ended by a stream signal rather than by a message
/// (`moq_request_terminal_cause_t`, an open set).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TerminalCause {
    /// A message ended it (`MOQ_REQUEST_TERMINAL_NONE`).
    Message,
    /// The peer reset the stream; the code is the peer's.
    PeerResetStream,
    /// The peer sent STOP_SENDING; the code is the peer's.
    PeerStopSending,
    /// The peer ended the stream before any required message.
    MissingResponse,
    /// The peer ended the stream inside a partial message.
    TruncatedResponse,
    /// The responder's lawful FIN after an established namespace
    /// subscription's required messages.
    PeerFinComplete,
    /// This side cancelled it: the response carried what it cannot take.
    LocalCancel,
    /// This side cancelled it: its track is malformed.
    MalformedTrack,
    /// The peer stopped the FETCH response stream.
    PeerStopData,
    /// A cause this crate does not know.
    Other(u32),
}

impl TerminalCause {
    pub(super) fn from_raw(raw: u32) -> TerminalCause {
        match raw {
            sys::MOQ_REQUEST_TERMINAL_NONE => TerminalCause::Message,
            sys::MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM => TerminalCause::PeerResetStream,
            sys::MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING => TerminalCause::PeerStopSending,
            sys::MOQ_REQUEST_TERMINAL_MISSING_RESPONSE => TerminalCause::MissingResponse,
            sys::MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE => TerminalCause::TruncatedResponse,
            sys::MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE => TerminalCause::PeerFinComplete,
            sys::MOQ_REQUEST_TERMINAL_LOCAL_CANCEL => TerminalCause::LocalCancel,
            sys::MOQ_REQUEST_TERMINAL_MALFORMED_TRACK => TerminalCause::MalformedTrack,
            sys::MOQ_REQUEST_TERMINAL_PEER_STOP_DATA => TerminalCause::PeerStopData,
            other => TerminalCause::Other(other),
        }
    }
}

/// This endpoint's side of a request (`moq_request_role_t` and the
/// namespace and track-status role sets, which share its values).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequestRole {
    /// This side made the request.
    Requester,
    /// This side answers it.
    Responder,
    /// A role this crate does not know.
    Other(u32),
}

impl RequestRole {
    pub(super) fn from_raw(raw: u32) -> RequestRole {
        match raw {
            sys::MOQ_REQUEST_ROLE_REQUESTER => RequestRole::Requester,
            sys::MOQ_REQUEST_ROLE_RESPONDER => RequestRole::Responder,
            other => RequestRole::Other(other),
        }
    }
}

/// The record's lifetime marker: the exclusive borrow of the session, and
/// `*const ()` to keep every event record off other threads.
pub(super) type Record<'session> = PhantomData<(&'session (), *const ())>;

/// Decodes one polled event record under its stamped detail size.
///
/// Its safe methods dereference spans and pointers read from the record;
/// that is sound because a reader exists only inside the decoder of a record
/// the session wrote, whose borrowed memory stays valid for `'session`
/// (see [`Reader::new`]).
/// A plain C record a session nests in another by pointer: it begins with
/// its `uint32_t struct_size` stamp, and all-zero bytes are a valid value.
///
/// # Safety
/// An implementing type must be exactly that: `struct_size: u32` at offset
/// zero, and every other field an integer, a `bool`, a raw pointer, a
/// fixed array of those or a nested such record.
pub(super) unsafe trait SizedRecord: Copy {}

macro_rules! sized_record {
    ($($ty:ty),* $(,)?) => {$(
        // SAFETY: a bindgen record of the native header whose first field
        // is `struct_size: u32` (asserted below) and whose other fields are
        // integers, bools, raw pointers and nested plain records, for which
        // zero is valid.
        unsafe impl SizedRecord for $ty {}
        const _: () = assert!(std::mem::offset_of!($ty, struct_size) == 0);
    )*};
}

sized_record!(
    sys::moq_subscription_admission_options_t,
    sys::moq_subscription_update_options_t,
    sys::moq_fetch_request_options_t,
    sys::moq_subscribe_tracks_options_t,
    sys::moq_namespace_update_record_t,
    sys::moq_location_filter_cfg_t,
);

pub(super) struct Reader<'session> {
    size: u32,
    owner: OwnerId,
    _session: PhantomData<&'session ()>,
}

impl<'session> Reader<'session> {
    /// # Safety
    /// Every span, array and pointer later passed to the reader's methods was
    /// read from one record a session wrote, and that session stays borrowed
    /// for `'session` so that no advancing call can run; every rcbuf field
    /// passed to [`take`](Reader::take) holds that record's own reference. A
    /// nested record pointer passed to [`nested`](Reader::nested) points at
    /// a record valid for at least the size it stamps, whatever its type's
    /// current size.
    pub(super) unsafe fn new(size: u32, owner: OwnerId) -> Reader<'session> {
        Reader {
            size,
            owner,
            _session: PhantomData,
        }
    }

    pub(super) fn floor(&self, floor: usize) -> Result<()> {
        raw::floor(self.size, floor)
    }

    pub(super) fn covers(&self, field_end: usize) -> bool {
        raw::covers(self.size, field_end)
    }

    pub(super) fn bytes(&self, span: sys::moq_bytes_t) -> Result<&'session [u8]> {
        // SAFETY: the reader's contract.
        unsafe { raw::record_span(span) }
    }

    pub(super) fn namespace(&self, ns: sys::moq_namespace_t) -> Result<NamespaceView<'session>> {
        // SAFETY: the reader's contract.
        let parts = unsafe { raw::record_array(ns.parts, ns.count) }?;
        if parts
            .iter()
            .any(|part| part.len != 0 && part.data.is_null())
        {
            return Err(Error::ABI_MISMATCH);
        }
        Ok(NamespaceView { parts })
    }

    pub(super) fn tokens(
        &self,
        tokens: *const sys::moq_resolved_token_t,
        count: usize,
    ) -> Result<Tokens<'session>> {
        // SAFETY: the reader's contract.
        let tokens = unsafe { raw::record_array(tokens, count) }?;
        if tokens
            .iter()
            .any(|token| token.token_value.len != 0 && token.token_value.data.is_null())
        {
            return Err(Error::ABI_MISMATCH);
        }
        Ok(Tokens { tokens })
    }

    pub(super) fn cluster(
        &self,
        raw: &sys::moq_cluster_metadata_t,
    ) -> Result<ClusterView<'session>> {
        // SAFETY: the reader's contract.
        let hop_ids = unsafe { raw::record_array(raw.hop_ids, raw.hop_count) }?;
        Ok(ClusterView {
            hop_ids,
            route_cost: raw.has_route_cost.then_some(raw.route_cost),
        })
    }

    /// A nested sized record, copied; `None` for null. Its size stamp is
    /// read through raw memory, then only the bytes the stamp covers, at
    /// most the current layout, are copied into a zeroed current record.
    /// No reference to the producer's record is formed, so a smaller
    /// record an older library wrote is never read past its end. The copy
    /// keeps the producer's stamp: the caller checks its floor and each
    /// appended field's coverage against it, as for any sized record. Its
    /// pointers still borrow the session's storage for 'session.
    ///
    /// # Errors
    /// [`Error::ABI_MISMATCH`] for a stamp smaller than the stamp itself.
    pub(super) fn nested<T: SizedRecord>(&self, ptr: *const T) -> Result<Option<T>> {
        if ptr.is_null() {
            return Ok(None);
        }
        // SAFETY: the reader's contract: a non-null nested pointer read from
        // the record points at a record the session wrote, valid for
        // 'session for at least the size it stamped, which begins it.
        let stamp = unsafe { ptr.cast::<u32>().read_unaligned() } as usize;
        if stamp < std::mem::size_of::<u32>() {
            return Err(Error::ABI_MISMATCH);
        }
        let take = stamp.min(std::mem::size_of::<T>());
        // SAFETY: SizedRecord: all-zero bytes are a valid T.
        let mut copy: T = unsafe { std::mem::zeroed() };
        // SAFETY: the producer's record is valid for its stamp, at least
        // `take` bytes; the destination is a whole T, at least `take` bytes;
        // the two do not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(
                ptr.cast::<u8>(),
                std::ptr::addr_of_mut!(copy).cast::<u8>(),
                take,
            );
        }
        Ok(Some(copy))
    }

    /// Adopt a record's owned reference, clearing its field so the record no
    /// longer releases it.
    pub(super) fn take(&self, field: &mut *mut sys::moq_rcbuf_t) -> Option<Payload<'session>> {
        let raw = std::mem::replace(field, std::ptr::null_mut());
        // SAFETY: the reader's contract: the field held the record's own
        // reference, now owned here alone.
        unsafe { Payload::adopt(raw) }
    }

    pub(super) fn subscription(&self, raw: sys::moq_subscription_t) -> Subscription {
        Subscription::new(raw, self.owner)
    }

    /// The handle when it is structurally valid (one of a pair is unset).
    pub(super) fn subscription_if_valid(
        &self,
        raw: sys::moq_subscription_t,
    ) -> Option<Subscription> {
        // SAFETY: a pure function of the value.
        unsafe { sys::moq_subscription_is_valid(raw) }.then(|| self.subscription(raw))
    }

    pub(super) fn publication(&self, raw: sys::moq_publication_t) -> Publication {
        Publication::new(raw, self.owner)
    }

    pub(super) fn publication_if_valid(&self, raw: sys::moq_publication_t) -> Option<Publication> {
        // SAFETY: a pure function of the value.
        unsafe { sys::moq_publication_is_valid(raw) }.then(|| self.publication(raw))
    }

    pub(super) fn fetch(&self, raw: sys::moq_fetch_t) -> Fetch {
        Fetch::new(raw, self.owner)
    }

    pub(super) fn announcement(&self, raw: sys::moq_announcement_t) -> Announcement {
        Announcement::new(raw, self.owner)
    }

    pub(super) fn ns_sub(&self, raw: sys::moq_ns_sub_handle_t) -> NamespaceSubscription {
        NamespaceSubscription::new(raw, self.owner)
    }

    pub(super) fn fill(&self, raw: sys::moq_fill_t) -> Fill {
        Fill::new(raw, self.owner)
    }

    pub(super) fn track_status(&self, raw: sys::moq_track_status_handle_t) -> TrackStatusRequest {
        TrackStatusRequest::new(raw, self.owner)
    }

    pub(super) fn track_sub(&self, raw: sys::moq_track_sub_handle_t) -> TrackSubscription {
        TrackSubscription::new(raw, self.owner)
    }

    pub(super) fn subgroup(&self, raw: sys::moq_subgroup_handle_t) -> Subgroup {
        Subgroup::new(raw, self.owner)
    }
}

pub(super) fn perspective(raw: sys::moq_perspective_t) -> Result<Perspective> {
    match raw {
        sys::MOQ_PERSPECTIVE_CLIENT => Ok(Perspective::Client),
        sys::MOQ_PERSPECTIVE_SERVER => Ok(Perspective::Server),
        _ => Err(Error::ABI_MISMATCH),
    }
}

/// `MOQ_GROUP_ORDER_DEFAULT` is `None`: no order stated.
pub(super) fn group_order(raw: u32) -> Result<Option<GroupOrder>> {
    match raw {
        sys::MOQ_GROUP_ORDER_DEFAULT => Ok(None),
        sys::MOQ_GROUP_ORDER_ASCENDING => Ok(Some(GroupOrder::Ascending)),
        sys::MOQ_GROUP_ORDER_DESCENDING => Ok(Some(GroupOrder::Descending)),
        _ => Err(Error::ABI_MISMATCH),
    }
}

pub(super) fn location(group_id: u64, object_id: u64) -> Location {
    Location {
        group_id,
        object_id,
    }
}

/// A Location Filter as C stated it. A filter kind this crate does not know
/// cannot be represented and refuses the record.
pub(super) fn location_filter(raw: &sys::moq_location_filter_cfg_t) -> Result<LocationFilter> {
    raw::floor(
        raw.struct_size,
        sys::MOQ_LOCATION_FILTER_CFG_V0_SIZE as usize,
    )?;
    let start = location(raw.start_group, raw.start_object);
    Ok(match raw.kind {
        sys::MOQ_LOCATION_FILTER_NONE => LocationFilter::Unconstrained,
        sys::MOQ_LOCATION_FILTER_NEXT_OBJECT => LocationFilter::NextObject,
        sys::MOQ_LOCATION_FILTER_RELATIVE_GROUP => LocationFilter::RelativeGroup {
            offset: raw.relative_group_offset,
        },
        sys::MOQ_LOCATION_FILTER_ABSOLUTE_START => LocationFilter::AbsoluteStart(start),
        sys::MOQ_LOCATION_FILTER_GROUP_END => LocationFilter::GroupEnd {
            start,
            end_group: raw.end_group,
        },
        sys::MOQ_LOCATION_FILTER_OBJECT_END => LocationFilter::ObjectEnd {
            start,
            end: location(raw.end_group, raw.end_object),
        },
        _ => return Err(Error::ABI_MISMATCH),
    })
}

/// Test storage of exactly a frozen record size, aligned for its
/// type, the way a producer built against that header allocates it.
#[cfg(test)]
pub(super) struct Exact {
    ptr: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

#[cfg(test)]
impl Exact {
    /// The bytes a producer of `stamp` bytes wrote: the first
    /// `min(stamp, size_of::<T>())` bytes of `current`, `stamp` in its
    /// leading `struct_size`, and `0xA5` in any bytes past the current
    /// record (a later producer's fields), in an allocation of exactly
    /// `len` bytes (at least four: every record starts with its stamp).
    pub(super) fn record<T: SizedRecord>(current: &T, stamp: usize, len: usize) -> Exact {
        let size = len.max(std::mem::size_of::<u32>());
        let layout = std::alloc::Layout::from_size_align(size, std::mem::align_of::<T>())
            .expect("a small layout");
        // SAFETY: the layout has a nonzero size.
        let raw = unsafe { std::alloc::alloc(layout) };
        let ptr =
            std::ptr::NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        let take = stamp.min(std::mem::size_of::<T>()).min(size);
        // SAFETY: the allocation holds `size` bytes, at least `take` and at
        // least the four of the stamp; `current` is a whole T; the two do
        // not overlap.
        unsafe {
            ptr.as_ptr().write_bytes(0xA5, size);
            std::ptr::copy_nonoverlapping(
                std::ptr::from_ref(current).cast::<u8>(),
                ptr.as_ptr(),
                take,
            );
            ptr.as_ptr()
                .cast::<u32>()
                .write_unaligned(u32::try_from(stamp).expect("a small stamp"));
        }
        Exact { ptr, layout }
    }

    pub(super) fn as_ptr<T>(&self) -> *const T {
        self.ptr.as_ptr().cast_const().cast()
    }
}

#[cfg(test)]
impl Drop for Exact {
    fn drop(&mut self) {
        // SAFETY: allocated in `record` with this layout, freed once.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{offset_of, size_of};

    use super::*;
    use crate::session::raw::field_end;

    type Admission = sys::moq_subscription_admission_options_t;
    type UpdateOptions = sys::moq_subscription_update_options_t;

    const SENTINEL: u8 = 0xA5;

    /// A record of `stamp` bytes the way a producer of that size wrote it,
    /// laid at the start of a larger buffer whose remaining bytes hold a
    /// sentinel a too-long read would copy.
    fn produced<T: SizedRecord>(current: &T, stamp: usize) -> Vec<u64> {
        let words = (size_of::<T>() + 64).div_ceil(8);
        let mut storage = vec![u64::from_ne_bytes([SENTINEL; 8]); words];
        let bytes = storage.as_mut_ptr().cast::<u8>();
        let take = stamp.min(size_of::<T>());
        // SAFETY: `storage` holds more than size_of::<T>() bytes; `current`
        // is a whole T; the two do not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(std::ptr::from_ref(current).cast::<u8>(), bytes, take);
            bytes
                .cast::<u32>()
                .write_unaligned(u32::try_from(stamp).expect("a small stamp"));
        }
        storage
    }

    /// The two storages of one producer's record: followed by sentinel
    /// bytes a too-long read would copy, and allocated at exactly its size.
    enum Storage {
        Padded(Vec<u64>),
        Exact(Exact),
    }

    impl Storage {
        fn ptr<T>(&self) -> *const T {
            match self {
                Storage::Padded(words) => words.as_ptr().cast(),
                Storage::Exact(exact) => exact.as_ptr(),
            }
        }
    }

    fn storages<T: SizedRecord>(current: &T, stamp: usize) -> [Storage; 2] {
        [
            Storage::Padded(produced(current, stamp)),
            Storage::Exact(Exact::record(current, stamp, stamp)),
        ]
    }

    fn read<T: SizedRecord>(storage: &Storage) -> Result<Option<T>> {
        // SAFETY: the pointer is into `storage`, which outlives the reader,
        // and is valid for its stamp.
        let reader = unsafe { Reader::new(0, OwnerId::next()) };
        reader.nested(storage.ptr::<T>())
    }

    fn admission_full() -> Admission {
        // SAFETY: SizedRecord: zero is a valid Admission.
        let mut full: Admission = unsafe { std::mem::zeroed() };
        full.flags = 0x7;
        full.object_delivery_timeout_ms = 11;
        full.subgroup_delivery_timeout_ms = 22;
        full.rendezvous_timeout_ms = 33;
        full
    }

    #[test]
    fn a_historical_prefix_is_copied_and_nothing_past_it() {
        let full = admission_full();
        let frozen = sys::MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE as usize;
        assert!(frozen < size_of::<Admission>(), "a frozen size");
        for storage in storages(&full, frozen) {
            let copy: Admission = read(&storage).expect("a stamp").expect("a record");
            assert_eq!(copy.struct_size as usize, frozen);
            assert_eq!(copy.object_delivery_timeout_ms, 11);
            assert_eq!(copy.subgroup_delivery_timeout_ms, 22);
            // The appended field is outside the stamp: zero, never the
            // sentinel beyond the producer's record.
            assert_eq!(copy.rendezvous_timeout_ms, 0);
            assert!(!raw::covers(
                copy.struct_size,
                field_end!(Admission, rendezvous_timeout_ms, u64)
            ));
        }
    }

    #[test]
    fn every_cutoff_copies_whole_fields_only_up_to_the_stamp() {
        let full = admission_full();
        let end = field_end!(Admission, rendezvous_timeout_ms, u64);
        for stamp in sys::MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE as usize..=end {
            for storage in storages(&full, stamp) {
                let copy: Admission = read(&storage).expect("a stamp").expect("a record");
                assert_eq!(copy.struct_size as usize, stamp);
                let bytes = std::ptr::from_ref(&copy).cast::<u8>();
                for at in stamp..size_of::<Admission>() {
                    // SAFETY: within the local copy.
                    let byte = unsafe { *bytes.add(at) };
                    assert_eq!(byte, 0, "stamp {stamp}: byte {at} copied");
                }
                let covered = raw::covers(copy.struct_size, end);
                assert_eq!(covered, stamp == end, "stamp {stamp}");
            }
        }
    }

    #[test]
    fn the_whole_current_record_and_a_larger_future_one() {
        let full = admission_full();
        for stamp in [size_of::<Admission>(), size_of::<Admission>() + 24] {
            for storage in storages(&full, stamp) {
                let copy: Admission = read(&storage).expect("a stamp").expect("a record");
                assert_eq!(
                    copy.struct_size as usize, stamp,
                    "the producer's stamp, kept"
                );
                assert_eq!(copy.rendezvous_timeout_ms, 33);
            }
        }
    }

    #[test]
    fn short_stamps_and_null() {
        let full = admission_full();
        // A record of four bytes, its stamp too short to be one.
        let short = Storage::Exact(Exact::record(&full, 3, 4));
        assert_eq!(read::<Admission>(&short).err(), Some(Error::ABI_MISMATCH));
        assert_eq!(
            read::<Admission>(&Storage::Padded(vec![0])).err(),
            Some(Error::ABI_MISMATCH)
        );
        let mut words = vec![0u64; 16];
        words[0] = 3;
        assert_eq!(
            read::<Admission>(&Storage::Padded(words.clone())).err(),
            Some(Error::ABI_MISMATCH)
        );
        // A stamp below the frozen prefix is copied as far as it goes; the
        // caller's floor refuses it.
        words[0] = 8;
        for storage in [
            Storage::Padded(words),
            Storage::Exact(Exact::record(&full, 8, 8)),
        ] {
            let copy: Admission = read(&storage).expect("a stamp").expect("a record");
            assert_eq!(copy.struct_size, 8);
            assert_eq!(copy.object_delivery_timeout_ms, 0, "past the stamp");
            assert!(raw::floor(
                copy.struct_size,
                sys::MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE as usize
            )
            .is_err());
        }
        // SAFETY: a null pointer is never read.
        let reader = unsafe { Reader::new(0, OwnerId::next()) };
        assert!(matches!(
            reader.nested::<Admission>(std::ptr::null()),
            Ok(None)
        ));
    }

    #[test]
    fn update_options_before_and_with_their_resolution() {
        // SAFETY: SizedRecord: zero is a valid UpdateOptions.
        let mut full: UpdateOptions = unsafe { std::mem::zeroed() };
        full.initiating_request_id = 9;
        full.resolution.struct_size = sys::MOQ_UPDATE_RESOLUTION_V0_SIZE;
        full.resolution.update_request_id = 12;
        let through_delivery_timeout = field_end!(UpdateOptions, subgroup_delivery_timeout_ms, u64);
        let with = field_end!(UpdateOptions, resolution, sys::moq_update_resolution_t);
        assert_eq!(
            through_delivery_timeout, 80,
            "the extent through subgroup_delivery_timeout_ms"
        );
        for storage in storages(&full, through_delivery_timeout) {
            let copy: UpdateOptions = read(&storage).expect("a stamp").expect("a record");
            assert_eq!(copy.initiating_request_id, 9);
            assert_eq!(
                copy.resolution.struct_size, 0,
                "the resolution is past the stamp"
            );
            assert!(!raw::covers(copy.struct_size, with));
        }
        for storage in storages(&full, with) {
            let copy: UpdateOptions = read(&storage).expect("a stamp").expect("a record");
            assert_eq!(copy.resolution.update_request_id, 12);
            assert!(raw::covers(copy.struct_size, with));
        }
        assert_eq!(offset_of!(UpdateOptions, struct_size), 0);
    }
}
