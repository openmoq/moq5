//! LOCMAF: Low Overhead CMAF for Media over QUIC
//! (draft-einarsson-moq-locmaf-01; Object encoding version "0.3").
//!
//! A LOCMAF Object carries one CMAF chunk: the boxes before its moof as
//! generic box elements, the moof as tagged fields (absolute in a full
//! header, differences against the previous chunk of the same MoQT group in
//! a delta header) and the mdat payload unchanged. An [`Encoder`] turns
//! CMAF chunks into Objects; a [`Decoder`] turns Objects into the chunk's
//! effective values ([`Chunk`]), and [`write_cmaf`] writes the canonical
//! CMAF chunk those values define. Both are relative to the facts of the
//! track's CMAF Header ([`Track`]).
//!
//! An application on the service tier does not need this module: a
//! receiver's LOCMAF track delivers the canonical CMAF chunks, and a
//! sender's LOCMAF track takes CMAF chunks. It is for a host that owns its
//! transport, or that relays or inspects the Objects themselves.
//!
//! # Ownership
//!
//! A decoder or an encoder owns two allocations: its state, boxed so it
//! never moves, and the raw storage the library carves its tables from,
//! sized by the library for the [`Limits`] and allocated from the global
//! allocator with the library's storage alignment. The storage holds no
//! Rust value and is never read or written from Rust: only its address
//! crosses to the library, which keeps pointers into it in the state. Both
//! are freed when their owner drops, the state first. Moving the owner
//! moves the two pointers, so a chain in progress survives a move into a
//! container or to another thread. Neither is `Clone`: the state's pointers
//! into its own storage cannot be shared, and [`Decoder::copy_from`]
//! continues a chain in another decoder's own storage instead.
//!
//! # Threads
//!
//! A decoder or an encoder has no thread affinity, but calls on one must
//! never overlap: each is `Send` and not `Sync`. A [`Chunk`] borrows its
//! decoder exclusively and the Object it was decoded from, so the borrow
//! checker refuses another push, a reset, a move or a drop of the decoder,
//! and the end of the Object's bytes, while the chunk lives.
//!
//! Requires the `locmaf` feature and an SDK built with
//! `MOQ_BUILD_LOCMAF=ON` (`libmoq-locmaf`).

use std::alloc::{self, Layout};
use std::ffi::{c_void, CStr};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr::{self, NonNull};

use moq5_core::{Error, Result};

use crate::cmaf::{InitInfo, Trex};
use crate::{ffi, sys};

/// The alignment the library requires of decoder and encoder storage.
const STORAGE_ALIGN: usize = sys::MOQ_LOCMAF_STORAGE_ALIGN as usize;

// -- reasons -----------------------------------------------------------------

/// Why an Object or a CMAF chunk was refused (`moq_locmaf_reason_t`, an
/// open set): the section of the draft it breaks, or the limit or model it
/// exceeds. A reason this crate does not name keeps its code.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reason(sys::moq_locmaf_reason_t);

impl Reason {
    /// A reason from its `MOQ_LOCMAF_REASON_*` value, known or not.
    #[must_use]
    pub const fn from_code(code: sys::moq_locmaf_reason_t) -> Reason {
        Reason(code)
    }

    /// The `MOQ_LOCMAF_REASON_*` value.
    #[must_use]
    pub const fn code(self) -> sys::moq_locmaf_reason_t {
        self.0
    }

    /// The library's stable name for it (`"no_reference"`, `"limit"`, ...);
    /// empty for a value the library does not know.
    #[must_use]
    pub fn name(self) -> &'static str {
        // SAFETY: a pure function returning a static string, never freed.
        let name = unsafe { sys::moq_locmaf_reason_name(self.0) };
        if name.is_null() {
            return "";
        }
        // SAFETY: a non-null result is a NUL-terminated static string.
        unsafe { CStr::from_ptr(name) }.to_str().unwrap_or("")
    }

    /// A counter record's last reason: zero is "none".
    fn last(code: sys::moq_locmaf_reason_t) -> Option<Reason> {
        (code != sys::MOQ_LOCMAF_REASON_NONE).then_some(Reason(code))
    }
}

// The name, or the code when the library does not know it.
impl fmt::Debug for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            "" => write!(f, "Reason({})", self.0),
            name => write!(f, "Reason({name})"),
        }
    }
}

// -- track facts ---------------------------------------------------------------

/// The CMAF Header facts a track's encoding is relative to
/// (`moq_locmaf_track_t`, section 6): the single trak's track_ID, its trex
/// defaults and its encryption defaults.
#[derive(Clone, Copy)]
pub struct Track(sys::moq_locmaf_track_t);

impl Track {
    /// The facts of a parsed CMAF Header. Reads `init` only; nothing is
    /// retained.
    ///
    /// # Errors
    /// [`Error::PROTO`] for a header LOCMAF cannot use: not exactly one
    /// trak (sections 4.1, 6), no trex for the track, track_ID 0, or a
    /// protected track whose default per-sample IV size is not 0, 8 or 16.
    /// [`Error::INVAL`] for a record without the header facts (one an older
    /// library stamped), and [`Error::ABI_MISMATCH`] when the library
    /// stamps the facts outside their size contract.
    pub fn from_init(init: InitInfo<'_>) -> Result<Track> {
        let mut raw = sys::moq_locmaf_track_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: the init record is valid for the call (InitInfo's
        // contract) and only read; raw is writable for the size passed.
        ffi::check(unsafe { sys::moq_locmaf_track_from_cmaf(init.raw(), &mut raw, size) })?;
        ffi::check_stamp(raw.struct_size, sys::MOQ_LOCMAF_TRACK_V0_SIZE, size)?;
        Ok(Track(raw))
    }

    /// tkhd track_ID, nonzero.
    #[must_use]
    pub fn track_id(&self) -> u32 {
        self.0.track_id
    }

    /// The trex defaults the fields are relative to.
    #[must_use]
    pub fn trex(&self) -> Trex {
        Trex {
            default_sample_description_index: self.0.trex_default_sample_description_index,
            default_sample_duration: self.0.trex_default_sample_duration,
            default_sample_size: self.0.trex_default_sample_size,
            default_sample_flags: self.0.trex_default_sample_flags,
        }
    }

    /// tenc default_isProtected is 1.
    #[must_use]
    pub fn is_protected(&self) -> bool {
        self.0.is_protected
    }

    /// tenc default_Per_Sample_IV_Size: 0, 8 or 16.
    #[must_use]
    pub fn default_per_sample_iv_size(&self) -> u8 {
        self.0.default_per_sample_iv_size
    }

    /// The raw record.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_locmaf_track_t {
        &self.0
    }
}

// The facts, not the raw record's stamp and reserve.
impl fmt::Debug for Track {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Track")
            .field("track_id", &self.track_id())
            .field("trex", &self.trex())
            .field("is_protected", &self.is_protected())
            .field(
                "default_per_sample_iv_size",
                &self.default_per_sample_iv_size(),
            )
            .finish()
    }
}

/// The table sizes a decoder or an encoder holds; zero selects the
/// library's default. Its storage grows linearly with them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Limits {
    /// Samples per chunk: at most 65536 (default 1024).
    pub max_samples: u32,
    /// senc subsample entries per chunk: at most 1048576 (default 4096).
    pub max_subsamples: u32,
    /// genBox elements per Object: at most 256 (default 16).
    pub max_gen_boxes: u32,
}

// -- storage -------------------------------------------------------------------

/// Raw storage from the global allocator: never viewed as a Rust slice and
/// never holding a Rust value; only its address crosses to the library.
struct RawStorage {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl RawStorage {
    /// `bytes` of storage (at least one) at the library's alignment.
    fn new(bytes: usize) -> Result<RawStorage> {
        let layout =
            Layout::from_size_align(bytes.max(1), STORAGE_ALIGN).map_err(|_| Error::INVAL)?;
        // SAFETY: the layout has a nonzero size.
        let ptr = NonNull::new(unsafe { alloc::alloc(layout) }).ok_or(Error::NOMEM)?;
        Ok(RawStorage { ptr, layout })
    }

    fn as_ptr(&self) -> *mut c_void {
        self.ptr.as_ptr().cast()
    }

    fn size(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for RawStorage {
    fn drop(&mut self) {
        // SAFETY: allocated in new() with exactly this layout, and freed
        // only here, once.
        unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

// -- decoder -------------------------------------------------------------------

/// A LOCMAF decoder for one track: the position and in-group reference of
/// the Objects pushed so far (`moq_locmaf_decoder_t`).
pub struct Decoder {
    // Fields drop in declaration order: the state, which points into the
    // storage, goes before it.
    state: Box<sys::moq_locmaf_decoder_t>,
    storage: RawStorage,
}

// SAFETY: a decoder has no thread affinity: the library lets one be used
// from one thread and then another, provided calls on it never overlap
// (locmaf.h, the storage contract above MOQ_LOCMAF_STORAGE_ALIGN). This
// value owns the state and the storage it points into, and nothing else
// refers to either: copy_from copies into the destination's own storage,
// and every chunk borrows the decoder, so none outlives a move. Moving the
// value moves every reference to both, and each call that changes them
// takes `&mut self`. It is not Sync: the library allows no overlapping
// calls at all, reads included.
unsafe impl Send for Decoder {}

impl Decoder {
    /// A decoder for `track` with `limits`, with no position and no
    /// reference.
    ///
    /// # Errors
    /// [`Error::INVAL`] for a limit above its maximum, and
    /// [`Error::NOMEM`] when the storage cannot be allocated.
    pub fn new(track: &Track, limits: Limits) -> Result<Decoder> {
        let mut cfg = sys::moq_locmaf_decoder_cfg_t::default();
        // SAFETY: a writable cfg of exactly the size passed.
        unsafe { sys::moq_locmaf_decoder_cfg_init_sized(&mut cfg, mem::size_of_val(&cfg)) };
        cfg.max_samples = limits.max_samples;
        cfg.max_subsamples = limits.max_subsamples;
        cfg.max_gen_boxes = limits.max_gen_boxes;
        cfg.track = &track.0;
        let mut bytes = 0usize;
        // SAFETY: an initialized cfg whose track is borrowed for the call;
        // bytes is writable.
        ffi::check(unsafe { sys::moq_locmaf_decoder_storage_size(&cfg, &mut bytes) })?;
        let storage = RawStorage::new(bytes)?;
        let mut state = Box::<sys::moq_locmaf_decoder_t>::default();
        cfg.storage = storage.as_ptr();
        cfg.storage_size = storage.size();
        // SAFETY: the state is a writable record of the size passed, at a
        // heap address it keeps for its life. The storage is raw memory of
        // at least the size the library asked for, at its alignment, which
        // the returned decoder owns and nothing in Rust accesses. The cfg
        // and its track are borrowed for the call.
        ffi::check(unsafe {
            sys::moq_locmaf_decoder_init(
                &mut *state,
                mem::size_of::<sys::moq_locmaf_decoder_t>(),
                &cfg,
            )
        })?;
        Ok(Decoder { state, storage })
    }

    /// Decode the LOCMAF Object at (`group_id`, `object_id`). Objects of one
    /// group are pushed in ascending Object ID order; another group starts
    /// afresh, and a gap in Object IDs discards the reference first
    /// (section 3). Report a lost Object or a reset stream with
    /// [`reset`](Decoder::reset).
    ///
    /// The chunk's sample, subsample, IV and genBox tables live in this
    /// decoder's storage, and its mdat payload, rawBoxes and genBox
    /// payloads in `object`: it borrows both, so it cannot outlive either,
    /// and nothing else can change the decoder while it lives.
    ///
    /// # Errors
    /// The refusal's reason is the decoder's
    /// [`last_reason`](DecoderStats::last_reason).
    /// - [`Error::WRONG_STATE`]: a delta header with no reference (joined
    ///   mid-group, or after a loss). Not malformed; the position advances.
    /// - [`Error::PROTO`]: malformed. The reference is discarded and the
    ///   position advances.
    /// - [`Error::UNSUPPORTED`]: a box this library cannot validate
    ///   (section 18); as for PROTO.
    /// - [`Error::LIMIT`]: beyond the decoder's [`Limits`]; as for PROTO.
    /// - [`Error::INVAL`]: an Object ID below the next one expected in the
    ///   same group (a duplicate, or out of order). Nothing changes.
    /// - [`Error::ABI_MISMATCH`]: the library stamped the chunk outside its
    ///   size contract.
    pub fn push<'object>(
        &mut self,
        group_id: u64,
        object_id: u64,
        object: &'object [u8],
    ) -> Result<Chunk<'_, 'object>> {
        let mut raw = sys::moq_locmaf_chunk_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: an initialized decoder whose storage this value owns; the
        // span borrows `object` for the call; raw is writable for the size
        // passed; a null reason is allowed.
        ffi::check(unsafe {
            sys::moq_locmaf_decoder_push(
                &mut *self.state,
                group_id,
                object_id,
                ffi::span(object),
                &mut raw,
                size,
                ptr::null_mut(),
            )
        })?;
        ffi::check_stamp(raw.struct_size, sys::MOQ_LOCMAF_CHUNK_V0_SIZE, size)?;
        Ok(Chunk {
            raw,
            _decoder: PhantomData,
            _object: PhantomData,
        })
    }

    /// Forget the position and the reference: the subgroup's stream was
    /// reset, or Objects were lost (section 3).
    pub fn reset(&mut self) {
        // SAFETY: an initialized decoder.
        unsafe { sys::moq_locmaf_decoder_reset(&mut *self.state) }
    }

    /// Whether a delta header at (`group_id`, `object_id`) would apply: the
    /// decoder holds a reference and expects exactly that Location next.
    #[must_use]
    pub fn expects(&self, group_id: u64, object_id: u64) -> bool {
        // SAFETY: an initialized decoder, only read.
        unsafe { sys::moq_locmaf_decoder_expects(&*self.state, group_id, object_id) }
    }

    /// Continue where `source` stands: its position, reference and derived
    /// decode time, copied into this decoder's own storage. For two
    /// delivery paths of one group (a fill and the live tail): the decoder
    /// that saw the earlier Objects hands the chain on. `source` is
    /// unchanged, and the two stay independent.
    ///
    /// # Errors
    /// [`Error::INVAL`], leaving this decoder unchanged, when the track
    /// facts differ or this decoder's limits are smaller than the lists
    /// `source`'s reference holds.
    pub fn copy_from(&mut self, source: &Decoder) -> Result<()> {
        // SAFETY: two initialized decoders; the borrows make them distinct.
        // Only this decoder's own state and storage are written.
        ffi::check(unsafe { sys::moq_locmaf_decoder_copy(&mut *self.state, &*source.state) })
    }

    /// The decoder's counters since it was created; a reset does not clear
    /// them.
    ///
    /// # Errors
    /// [`Error::ABI_MISMATCH`] when the library stamps the counters outside
    /// their size contract.
    pub fn stats(&self) -> Result<DecoderStats> {
        let mut raw = sys::moq_locmaf_decoder_stats_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: an initialized decoder, only read; raw is writable for the
        // size passed.
        ffi::check(unsafe { sys::moq_locmaf_decoder_get_stats(&*self.state, &mut raw, size) })?;
        ffi::check_stamp(raw.struct_size, sys::MOQ_LOCMAF_DECODER_STATS_V0_SIZE, size)?;
        Ok(DecoderStats {
            last_reason: Reason::last(raw.last_reason),
            objects_decoded: raw.objects_decoded,
            full_headers_decoded: raw.full_headers_decoded,
            delta_headers_decoded: raw.delta_headers_decoded,
            raw_boxes_decoded: raw.raw_boxes_decoded,
            objects_unreferenced: raw.objects_unreferenced,
            objects_malformed: raw.objects_malformed,
            objects_limited: raw.objects_limited,
            references_discarded: raw.references_discarded,
            unknown_fields_skipped: raw.unknown_fields_skipped,
            objects_unsupported: raw.objects_unsupported,
        })
    }
}

// The storage size, not the private state.
impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("storage_bytes", &self.storage.size())
            .finish_non_exhaustive()
    }
}

/// A [`Decoder`]'s counters (`moq_locmaf_decoder_stats_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecoderStats {
    /// The reason of the last refused Object; `None` if none was refused.
    pub last_reason: Option<Reason>,
    /// Objects decoded, of any kind.
    pub objects_decoded: u64,
    /// Full headers decoded.
    pub full_headers_decoded: u64,
    /// Delta headers decoded.
    pub delta_headers_decoded: u64,
    /// rawBoxes Objects decoded.
    pub raw_boxes_decoded: u64,
    /// Delta headers refused for want of a reference.
    pub objects_unreferenced: u64,
    /// Objects refused as malformed.
    pub objects_malformed: u64,
    /// Objects refused for exceeding the limits.
    pub objects_limited: u64,
    /// Held references dropped: a new group, a gap, a reset, rawBoxes or a
    /// refusal.
    pub references_discarded: u64,
    /// Unknown header fields skipped (section 7.3).
    pub unknown_fields_skipped: u64,
    /// Objects refused for a box this library cannot validate.
    pub objects_unsupported: u64,
}

// -- decoded chunks --------------------------------------------------------------

/// The kind of a decoded Object (`moq_locmaf_object_kind_t`, an open set).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// genBoxes, a full header and the mdat payload.
    Full,
    /// genBoxes, a delta header and the mdat payload.
    Delta,
    /// One rawBoxes element: the chunk's boxes verbatim (section 9).
    RawBoxes,
    /// A kind this crate does not know.
    Other(u32),
}

impl ObjectKind {
    fn from_raw(code: sys::moq_locmaf_object_kind_t) -> ObjectKind {
        match code {
            sys::MOQ_LOCMAF_OBJECT_FULL => ObjectKind::Full,
            sys::MOQ_LOCMAF_OBJECT_DELTA => ObjectKind::Delta,
            sys::MOQ_LOCMAF_OBJECT_RAW_BOXES => ObjectKind::RawBoxes,
            code => ObjectKind::Other(code),
        }
    }

    /// The `MOQ_LOCMAF_OBJECT_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            ObjectKind::Full => sys::MOQ_LOCMAF_OBJECT_FULL,
            ObjectKind::Delta => sys::MOQ_LOCMAF_OBJECT_DELTA,
            ObjectKind::RawBoxes => sys::MOQ_LOCMAF_OBJECT_RAW_BOXES,
            ObjectKind::Other(code) => code,
        }
    }
}

/// One decoded chunk (`moq_locmaf_chunk_t`): the effective values of
/// section 15.1, the genBoxes in Object order and the mdat payload, which
/// with the track facts define the canonical CMAF chunk
/// ([`write_cmaf`]). For a rawBoxes Object only the kind and the boxes are
/// set.
///
/// It borrows its decoder exclusively for `'decoder`, whose storage holds
/// its tables, and the pushed Object for `'object`, which holds its
/// payloads.
pub struct Chunk<'decoder, 'object> {
    raw: sys::moq_locmaf_chunk_t,
    _decoder: PhantomData<&'decoder mut Decoder>,
    _object: PhantomData<&'object [u8]>,
}

impl<'object> Chunk<'_, 'object> {
    /// What the Object was.
    #[must_use]
    pub fn kind(&self) -> ObjectKind {
        ObjectKind::from_raw(self.raw.kind)
    }

    /// tfdt baseMediaDecodeTime, in timescale units.
    #[must_use]
    pub fn base_media_decode_time(&self) -> u64 {
        self.raw.base_media_decode_time
    }

    /// The effective sample description index.
    #[must_use]
    pub fn sample_description_index(&self) -> u32 {
        self.raw.sample_description_index
    }

    /// The samples' effective values, in decode order.
    #[must_use]
    pub fn samples(&self) -> &[Sample] {
        // SAFETY: the table is in the decoder's storage, written by the push
        // that made this chunk and unchanged while the chunk borrows the
        // decoder; Sample is a transparent wrapper of the C record.
        unsafe {
            ffi::array(
                self.raw.samples.cast::<Sample>(),
                self.raw.sample_count as usize,
            )
        }
    }

    /// The effective per-sample IV size: 0, 8 or 16.
    #[must_use]
    pub fn per_sample_iv_size(&self) -> u8 {
        self.raw.per_sample_iv_size
    }

    /// senc, saiz and saio are rebuilt: the chunk carries CENC auxiliary
    /// information.
    #[must_use]
    pub fn has_aux_info(&self) -> bool {
        self.raw.has_aux_info
    }

    /// The senc carries subsample maps (its flags 0x000002).
    #[must_use]
    pub fn has_subsamples(&self) -> bool {
        self.raw.has_subsamples
    }

    /// The samples' IVs, `per_sample_iv_size` bytes each, when the chunk
    /// has auxiliary information.
    #[must_use]
    pub fn ivs(&self) -> &[u8] {
        // SAFETY: as for samples.
        unsafe { ffi::bytes(self.raw.ivs) }
    }

    /// Every sample's subsample entries, in sample order: a sample's own
    /// start at its [`subsample_index`](Sample::subsample_index).
    #[must_use]
    pub fn subsamples(&self) -> &[Subsample] {
        // SAFETY: as for samples; Subsample wraps the C record transparently.
        unsafe {
            ffi::array(
                self.raw.subsamples.cast::<Subsample>(),
                self.raw.subsample_total,
            )
        }
    }

    /// The boxes before the moof, in Object order.
    pub fn gen_boxes(&self) -> impl ExactSizeIterator<Item = GenBox<'object>> + '_ {
        // SAFETY: as for samples; each entry's payload borrows the Object,
        // valid for 'object.
        let boxes = unsafe { ffi::array(self.raw.gen_boxes, self.raw.gen_box_count) };
        boxes.iter().map(|entry| GenBox {
            box_type: entry.type_.to_be_bytes(),
            // SAFETY: the payload is a span of the Object, valid for
            // 'object.
            payload: unsafe { ffi::bytes(entry.payload) },
        })
    }

    /// The mdat payload, borrowed from the Object.
    #[must_use]
    pub fn mdat_payload(&self) -> &'object [u8] {
        // SAFETY: a span of the Object, valid for 'object.
        unsafe { ffi::bytes(self.raw.mdat_payload) }
    }

    /// A rawBoxes Object's boxes, verbatim; empty for any other kind.
    #[must_use]
    pub fn raw_boxes(&self) -> &'object [u8] {
        // SAFETY: as for mdat_payload.
        unsafe { ffi::bytes(self.raw.raw_boxes) }
    }

    /// The raw record; its pointers are valid while this chunk lives.
    #[must_use]
    pub fn raw(&self) -> &sys::moq_locmaf_chunk_t {
        &self.raw
    }
}

// The decoded values, not the raw pointers a derive would print.
impl fmt::Debug for Chunk<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunk")
            .field("kind", &self.kind())
            .field("base_media_decode_time", &self.base_media_decode_time())
            .field("samples", &self.samples())
            .field("gen_boxes", &self.gen_boxes().count())
            .field("mdat_payload_len", &self.mdat_payload().len())
            .field("raw_boxes_len", &self.raw_boxes().len())
            .finish_non_exhaustive()
    }
}

/// One sample's effective values (`moq_locmaf_sample_t`, a frozen record),
/// viewed in place.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Sample(sys::moq_locmaf_sample_t);

impl Sample {
    /// Duration, in timescale units.
    #[must_use]
    pub fn duration(&self) -> u32 {
        self.0.duration
    }
    /// Size, in bytes.
    #[must_use]
    pub fn size(&self) -> u32 {
        self.0.size
    }
    /// ISO/IEC 14496-12 sample_flags.
    #[must_use]
    pub fn flags(&self) -> u32 {
        self.0.flags
    }
    /// `sample_is_non_sync_sample` is clear.
    #[must_use]
    pub fn is_sync(&self) -> bool {
        self.0.flags & 0x0001_0000 == 0
    }
    /// The composition time offset, in timescale units.
    #[must_use]
    pub fn composition_offset(&self) -> i64 {
        self.0.composition_offset
    }
    /// The sample's first byte in the mdat payload.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.0.offset
    }
    /// senc subsample_count; 0 without a map.
    #[must_use]
    pub fn subsample_count(&self) -> u32 {
        self.0.subsample_count
    }
    /// The index of its first entry in the chunk's
    /// [`subsamples`](Chunk::subsamples).
    #[must_use]
    pub fn subsample_index(&self) -> u32 {
        self.0.subsample_index
    }
}

// The values, not the raw record's reserve.
impl fmt::Debug for Sample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sample")
            .field("duration", &self.duration())
            .field("size", &self.size())
            .field("flags", &format_args!("{:#010x}", self.flags()))
            .field("composition_offset", &self.composition_offset())
            .field("offset", &self.offset())
            .field("subsample_count", &self.subsample_count())
            .finish()
    }
}

/// One senc subsample entry (`moq_locmaf_subsample_t`, a frozen record),
/// viewed in place.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Subsample(sys::moq_locmaf_subsample_t);

impl Subsample {
    /// BytesOfClearData, 0 to 65535.
    #[must_use]
    pub fn clear_bytes(&self) -> u32 {
        self.0.clear_bytes
    }
    /// BytesOfProtectedData.
    #[must_use]
    pub fn protected_bytes(&self) -> u32 {
        self.0.protected_bytes
    }
}

impl fmt::Debug for Subsample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subsample")
            .field("clear_bytes", &self.clear_bytes())
            .field("protected_bytes", &self.protected_bytes())
            .finish()
    }
}

/// One generic box element (section 8): a box before the moof. Its type
/// and contents are the whole of the box, so it is not
/// `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenBox<'object> {
    /// The box FourCC.
    pub box_type: [u8; 4],
    /// The box contents after its 8-byte header (a uuid box's usertype
    /// first), borrowed from the Object.
    pub payload: &'object [u8],
}

/// Write the canonical CMAF chunk of `chunk` for `track` (section 15): its
/// genBoxes in Object order, then moof and mdat in the pinned layout; for a
/// rawBoxes Object, the boxes verbatim. Every conformant writer produces the
/// same bytes for the same facts and chunk.
///
/// # Errors
/// [`Error::INVAL`] for a chunk that is not consistent with `track` (a
/// chunk decoded for another track's facts); one decoded with the same
/// facts always is.
pub fn write_cmaf(track: &Track, chunk: &Chunk<'_, '_>) -> Result<Vec<u8>> {
    sized_output(|out, cap, len| {
        // SAFETY: the track and the chunk are valid for the call, and the
        // chunk's pointers are valid while it lives (it borrows their
        // owners); out is null with cap 0, or writable for cap bytes.
        unsafe { sys::moq_locmaf_write_cmaf(&track.0, &chunk.raw, out, cap, len) }
    })
}

/// Run a writer that reports its length on `MOQ_ERR_BUFFER`: once with no
/// buffer to learn the length, then into a buffer of exactly that length.
fn sized_output(
    mut write: impl FnMut(*mut u8, usize, &mut usize) -> sys::moq_result_t,
) -> Result<Vec<u8>> {
    let mut len = 0usize;
    match write(ptr::null_mut(), 0, &mut len) {
        sys::MOQ_ERR_BUFFER => {}
        sys::MOQ_OK if len == 0 => return Ok(Vec::new()),
        sys::MOQ_OK => return Err(Error::INTERNAL),
        code => return Err(Error::from_code(code)),
    }
    let mut out = vec![0u8; len];
    let mut written = 0usize;
    ffi::check(write(out.as_mut_ptr(), out.len(), &mut written))?;
    if written > out.len() {
        return Err(Error::INTERNAL);
    }
    out.truncate(written);
    Ok(out)
}

// -- encoder ---------------------------------------------------------------------

/// A LOCMAF encoder for one track: the committed position and reference,
/// and the one staged by the last encode (`moq_locmaf_encoder_t`).
///
/// An encode stages the reference it leaves; nothing becomes the reference
/// until [`commit`](Encoder::commit), so an Object whose send was refused
/// can be encoded again at the same Location, to the same bytes.
pub struct Encoder {
    // Fields drop in declaration order: the state, which points into the
    // storage, goes before it.
    state: Box<sys::moq_locmaf_encoder_t>,
    storage: RawStorage,
}

// SAFETY: as for Decoder: the library gives an encoder no thread affinity
// provided calls never overlap; this value owns the state and its storage
// alone (the encoder has no copy, and its outputs are owned byte vectors),
// every call that changes them takes `&mut self`, and a move carries every
// reference to both. It is not Sync: the library allows no overlapping
// calls at all.
unsafe impl Send for Encoder {}

impl Encoder {
    /// An encoder for `track` with `limits`, with no position and no
    /// reference: its first chunk is a full header.
    ///
    /// # Errors
    /// [`Error::INVAL`] for a limit above its maximum, and
    /// [`Error::NOMEM`] when the storage cannot be allocated.
    pub fn new(track: &Track, limits: Limits) -> Result<Encoder> {
        let mut cfg = sys::moq_locmaf_encoder_cfg_t::default();
        // SAFETY: a writable cfg of exactly the size passed.
        unsafe { sys::moq_locmaf_encoder_cfg_init_sized(&mut cfg, mem::size_of_val(&cfg)) };
        cfg.max_samples = limits.max_samples;
        cfg.max_subsamples = limits.max_subsamples;
        cfg.max_gen_boxes = limits.max_gen_boxes;
        cfg.track = &track.0;
        let mut bytes = 0usize;
        // SAFETY: an initialized cfg whose track is borrowed for the call;
        // bytes is writable.
        ffi::check(unsafe { sys::moq_locmaf_encoder_storage_size(&cfg, &mut bytes) })?;
        let storage = RawStorage::new(bytes)?;
        let mut state = Box::<sys::moq_locmaf_encoder_t>::default();
        cfg.storage = storage.as_ptr();
        cfg.storage_size = storage.size();
        // SAFETY: as for Decoder::new, with the encoder's state and
        // storage.
        ffi::check(unsafe {
            sys::moq_locmaf_encoder_init(
                &mut *state,
                mem::size_of::<sys::moq_locmaf_encoder_t>(),
                &cfg,
            )
        })?;
        Ok(Encoder { state, storage })
    }

    /// Check that a CMAF chunk can be encoded, without encoding it: one
    /// chunk (genBox-able boxes, then one moof and one mdat) inside the
    /// field model of section 10, consistent with the track facts and
    /// within the limits. A staged encode is not disturbed. Returns a length
    /// no encoding of this chunk exceeds, at any Location.
    ///
    /// # Errors
    /// [`Error::PROTO`] for a malformed chunk, [`Error::UNSUPPORTED`] for one
    /// outside the field model, [`Error::LIMIT`] for one beyond the limits;
    /// the reason is the encoder's
    /// [`last_reason`](EncoderStats::last_reason). [`Error::INVAL`] when the
    /// bound does not fit `usize`.
    pub fn check(&mut self, cmaf_chunk: &[u8]) -> Result<usize> {
        let mut bound = 0usize;
        // SAFETY: an initialized encoder whose storage this value owns; the
        // span borrows `cmaf_chunk` for the call; bound is writable; a null
        // reason is allowed.
        ffi::check(unsafe {
            sys::moq_locmaf_encoder_check(
                &mut *self.state,
                ffi::span(cmaf_chunk),
                &mut bound,
                ptr::null_mut(),
            )
        })?;
        Ok(bound)
    }

    /// Encode one CMAF chunk as the LOCMAF Object at (`group_id`,
    /// `object_id`) and stage the reference it leaves. A full header is
    /// written at a new group, a gap in Object IDs, after a rawBoxes Object,
    /// with no reference, and where the chunk's decode time does not
    /// continue the previous chunk's (section 12.2); otherwise a delta
    /// header. The output is canonical (section 15.9). A later encode before
    /// a commit replaces the stage.
    ///
    /// # Errors
    /// Nothing is staged on any error. [`Error::PROTO`],
    /// [`Error::UNSUPPORTED`] and [`Error::LIMIT`] as for
    /// [`check`](Encoder::check); [`Error::INVAL`] for an Object ID at or
    /// below the committed position in the same group.
    pub fn encode_chunk(
        &mut self,
        group_id: u64,
        object_id: u64,
        cmaf_chunk: &[u8],
    ) -> Result<Vec<u8>> {
        let state = &mut *self.state;
        sized_output(|out, cap, len| {
            // SAFETY: an initialized encoder whose storage the encoder owns;
            // the span borrows `cmaf_chunk` for the call; out is null with
            // cap 0, or writable for cap bytes; a null reason is allowed.
            unsafe {
                sys::moq_locmaf_encode_chunk(
                    &mut *state,
                    group_id,
                    object_id,
                    ffi::span(cmaf_chunk),
                    out,
                    cap,
                    len,
                    ptr::null_mut(),
                )
            }
        })
    }

    /// Encode complete ISO BMFF boxes verbatim as a rawBoxes Object (section
    /// 9): a chunk outside the field model, or a CMAF Header carried in
    /// band. Stages a reference reset: after its commit, the next chunk of
    /// the group is a full header.
    ///
    /// # Errors
    /// As for [`encode_chunk`](Encoder::encode_chunk); [`Error::PROTO`] for
    /// boxes that break section 9.1.
    pub fn encode_raw_boxes(
        &mut self,
        group_id: u64,
        object_id: u64,
        boxes: &[u8],
    ) -> Result<Vec<u8>> {
        let state = &mut *self.state;
        sized_output(|out, cap, len| {
            // SAFETY: as for encode_chunk, with `boxes`.
            unsafe {
                sys::moq_locmaf_encode_raw_boxes(
                    &mut *state,
                    group_id,
                    object_id,
                    ffi::span(boxes),
                    out,
                    cap,
                    len,
                    ptr::null_mut(),
                )
            }
        })
    }

    /// Adopt what the last successful encode staged: its Object was sent.
    /// Nothing happens when nothing is staged.
    pub fn commit(&mut self) {
        // SAFETY: an initialized encoder.
        unsafe { sys::moq_locmaf_encoder_commit(&mut *self.state) }
    }

    /// Forget the committed and staged position and reference: the next
    /// encode writes a full header.
    pub fn reset(&mut self) {
        // SAFETY: an initialized encoder.
        unsafe { sys::moq_locmaf_encoder_reset(&mut *self.state) }
    }

    /// The encoder's counters since it was created.
    ///
    /// # Errors
    /// [`Error::ABI_MISMATCH`] when the library stamps the counters outside
    /// their size contract.
    pub fn stats(&self) -> Result<EncoderStats> {
        let mut raw = sys::moq_locmaf_encoder_stats_t::default();
        let size = mem::size_of_val(&raw);
        // SAFETY: an initialized encoder, only read; raw is writable for the
        // size passed.
        ffi::check(unsafe { sys::moq_locmaf_encoder_get_stats(&*self.state, &mut raw, size) })?;
        ffi::check_stamp(raw.struct_size, sys::MOQ_LOCMAF_ENCODER_STATS_V0_SIZE, size)?;
        Ok(EncoderStats {
            last_reason: Reason::last(raw.last_reason),
            chunks_encoded: raw.chunks_encoded,
            full_headers_written: raw.full_headers_written,
            delta_headers_written: raw.delta_headers_written,
            raw_boxes_written: raw.raw_boxes_written,
            chunks_refused: raw.chunks_refused,
            stages_committed: raw.stages_committed,
            stages_replaced: raw.stages_replaced,
        })
    }
}

// The storage size, not the private state.
impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Encoder")
            .field("storage_bytes", &self.storage.size())
            .finish_non_exhaustive()
    }
}

/// An [`Encoder`]'s counters (`moq_locmaf_encoder_stats_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EncoderStats {
    /// The reason of the last refused chunk; `None` if none was refused.
    pub last_reason: Option<Reason>,
    /// Chunks staged by an encode.
    pub chunks_encoded: u64,
    /// Full headers written.
    pub full_headers_written: u64,
    /// Delta headers written.
    pub delta_headers_written: u64,
    /// rawBoxes Objects written.
    pub raw_boxes_written: u64,
    /// Chunks refused (malformed, unsupported or beyond the limits), by an
    /// encode or a check.
    pub chunks_refused: u64,
    /// Stages committed.
    pub stages_committed: u64,
    /// Uncommitted stages a later encode replaced.
    pub stages_replaced: u64,
}
