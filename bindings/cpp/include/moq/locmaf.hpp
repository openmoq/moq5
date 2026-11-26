#ifndef MOQ_LOCMAF_HPP
#define MOQ_LOCMAF_HPP

// Header-only C++ view of <moq/locmaf.h>: LOCMAF (Low Overhead CMAF,
// draft-einarsson-moq-locmaf-01) decoding into the effective values of a
// CMAF chunk, the canonical CMAF chunk they define, and encoding CMAF chunks
// into LOCMAF Objects.
//
// A decoder or encoder owns two allocations: its C state, and the storage
// the C library carves into its arrays. Moving a wrapper moves the two
// owners and relocates neither, so the C state's pointers into its storage
// stay valid. The wrappers are movable, not copyable; a moved-from wrapper
// owns nothing and refuses every call with errc::wrong_state. A decoder or
// encoder has no thread affinity: it may be moved to another thread between
// calls, but calls on one must not overlap.
//
// Allocation failure propagates std::bad_alloc, as every allocation in the
// binding does. Aligned allocation requires macOS 10.13 or iOS 11 on Apple
// platforms.

#include <moq/locmaf.h>
#include <moq/cmaf.h>
#include <moq/result.hpp>
#include <moq/types.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <span>
#include <utility>
#include <vector>

namespace moq::locmaf {

using object_kind   = moq_locmaf_object_kind_t;
using reason        = moq_locmaf_reason_t;
using sample        = moq_locmaf_sample_t;
using subsample     = moq_locmaf_subsample_t;
using gen_box       = moq_locmaf_gen_box_t;
using decoder_stats = moq_locmaf_decoder_stats_t;
using encoder_stats = moq_locmaf_encoder_stats_t;

/// A stable name for a reason ("" for an unknown value).
inline const char *reason_name(reason r) noexcept { return moq_locmaf_reason_name(r); }

/// The CMAF Header facts the encoding is relative to (section 6,
/// moq_locmaf_track_t): the single trak's track_ID, the trex defaults and
/// the track encryption defaults.
struct track {
    uint32_t track_id = 0;
    uint32_t trex_default_sample_description_index = 0;
    uint32_t trex_default_sample_duration = 0;   ///< timescale units
    uint32_t trex_default_sample_size = 0;       ///< bytes
    uint32_t trex_default_sample_flags = 0;      ///< sample_flags
    bool     is_protected = false;               ///< tenc default_isProtected == 1
    uint8_t  default_per_sample_iv_size = 0;     ///< tenc: 0, 8 or 16

    bool operator==(const track &) const = default;

    /// The facts of a parsed CMAF Header record (a moq_cmaf_parse_init_sized
    /// output, or a receiver description's cmaf record). errc::invalid for a
    /// record whose stamp does not cover the header facts block;
    /// errc::protocol for a header LOCMAF cannot use (not one trak, no trex
    /// for the track, track_ID 0, an IV size other than 0, 8 or 16).
    static result<track> from_cmaf(const moq_cmaf_init_info_t &init);

    /// Parse a CMAF Header (ftyp + moov) and take its facts, as from_cmaf.
    /// errc::protocol for a header that does not parse.
    static result<track> from_init(bytes_view init_segment);
};

/// Per-chunk limits; 0 selects the C default. Storage grows linearly with
/// them; a limit above its maximum is errc::invalid.
struct limits {
    uint32_t max_samples = 0;      ///< default MOQ_LOCMAF_DEFAULT_MAX_SAMPLES
    uint32_t max_subsamples = 0;   ///< default MOQ_LOCMAF_DEFAULT_MAX_SUBSAMPLES
    uint32_t max_gen_boxes = 0;    ///< default MOQ_LOCMAF_DEFAULT_MAX_GEN_BOXES
};

namespace detail {

inline moq_locmaf_track_t to_c(const track &t) noexcept
{
    moq_locmaf_track_t c;
    moq_locmaf_track_init(&c);
    c.track_id = t.track_id;
    c.trex_default_sample_description_index = t.trex_default_sample_description_index;
    c.trex_default_sample_duration = t.trex_default_sample_duration;
    c.trex_default_sample_size = t.trex_default_sample_size;
    c.trex_default_sample_flags = t.trex_default_sample_flags;
    c.is_protected = t.is_protected;
    c.default_per_sample_iv_size = t.default_per_sample_iv_size;
    return c;
}

inline track from_c(const moq_locmaf_track_t &c) noexcept
{
    track t;
    t.track_id = c.track_id;
    t.trex_default_sample_description_index = c.trex_default_sample_description_index;
    t.trex_default_sample_duration = c.trex_default_sample_duration;
    t.trex_default_sample_size = c.trex_default_sample_size;
    t.trex_default_sample_flags = c.trex_default_sample_flags;
    t.is_protected = c.is_protected;
    t.default_per_sample_iv_size = c.default_per_sample_iv_size;
    return t;
}

// Raw storage from an allocation function, as the C storage contract asks
// (see MOQ_LOCMAF_STORAGE_ALIGN): it holds no C++ object, and the binding
// never creates one in it. Only the C library stores into it, through its
// own types. It is released by the aligned deallocator matching the
// allocation; the unsized form is the one every supported C++ toolchain
// declares under its default flags (a sized one needs sized deallocation,
// which some compilers leave off).
class raw_storage {
    void       *p_ = nullptr;
    std::size_t n_ = 0;
    static constexpr std::align_val_t al{MOQ_LOCMAF_STORAGE_ALIGN};

    void release() noexcept
    {
        if (p_) ::operator delete(p_, al);
        p_ = nullptr;
        n_ = 0;
    }

public:
    raw_storage() = default;
    explicit raw_storage(std::size_t n)   // may throw std::bad_alloc
        : p_(::operator new(n ? n : 1, al)), n_(n ? n : 1) {}
    ~raw_storage() { release(); }
    raw_storage(raw_storage &&o) noexcept
        : p_(std::exchange(o.p_, nullptr)), n_(std::exchange(o.n_, 0)) {}
    raw_storage &operator=(raw_storage &&o) noexcept
    {
        if (this != &o) {
            release();
            p_ = std::exchange(o.p_, nullptr);
            n_ = std::exchange(o.n_, 0);
        }
        return *this;
    }
    raw_storage(const raw_storage &) = delete;
    raw_storage &operator=(const raw_storage &) = delete;

    void       *get() const noexcept { return p_; }
    std::size_t size() const noexcept { return n_; }
};

} // namespace detail

inline result<track> track::from_cmaf(const moq_cmaf_init_info_t &init)
{
    moq_locmaf_track_t c;
    moq_locmaf_track_init(&c);
    moq_result_t rc = moq_locmaf_track_from_cmaf(&init, &c, sizeof(c));
    if (rc < 0) return errc_from_result(rc);
    return detail::from_c(c);
}

inline result<track> track::from_init(bytes_view init_segment)
{
    moq_cmaf_init_info_t ci;
    moq_cmaf_init_info_init(&ci);
    moq_result_t rc = moq_cmaf_parse_init_sized(init_segment.raw(), &ci, sizeof(ci));
    if (rc < 0) return errc_from_result(rc);
    return from_cmaf(ci);
}

/// One decoded chunk (moq_locmaf_chunk_t): the effective values of section
/// 15.1, the genBoxes in Object order and the mdat payload. For a rawBoxes
/// Object only kind() and raw_boxes() are set.
///
/// A non-owning view. A chunk from decoder::push() borrows twice:
///   - samples(), subsamples(), gen_boxes() and ivs() borrow the decoder's
///     storage, valid until the next push(), reset() or copy_from() on the
///     decoder that holds that state, or its destruction; moving the
///     decoder ends neither, because nothing relocates;
///   - mdat_payload(), raw_boxes() and every genBox payload borrow the
///     pushed Object bytes, valid while those bytes are.
/// Whichever ends first ends the view.
class chunk_view {
    moq_locmaf_chunk_t c_{};

    template <class T>
    static std::span<const T> span_of(const T *p, std::size_t n) noexcept
    {
        return p ? std::span<const T>(p, n) : std::span<const T>();
    }

public:
    chunk_view() = default;
    /// View a C chunk record; it borrows whatever the record borrows.
    explicit chunk_view(const moq_locmaf_chunk_t &c) noexcept : c_(c) {}

    object_kind kind() const noexcept { return c_.kind; }
    uint64_t base_media_decode_time() const noexcept { return c_.base_media_decode_time; }
    uint32_t sample_description_index() const noexcept { return c_.sample_description_index; }
    std::span<const sample> samples() const noexcept
    {
        return span_of(c_.samples, c_.sample_count);
    }
    /// The effective per-sample IV size: 0, 8 or 16.
    uint8_t per_sample_iv_size() const noexcept { return c_.per_sample_iv_size; }
    /// senc, saiz and saio are rebuilt.
    bool has_aux_info() const noexcept { return c_.has_aux_info; }
    /// senc flags 0x000002.
    bool has_subsamples() const noexcept { return c_.has_subsamples; }
    /// sample count * per_sample_iv_size() bytes when has_aux_info().
    bytes_view ivs() const noexcept { return bytes_view(c_.ivs); }
    std::span<const subsample> subsamples() const noexcept
    {
        return span_of(c_.subsamples, c_.subsample_total);
    }
    std::span<const gen_box> gen_boxes() const noexcept
    {
        return span_of(c_.gen_boxes, c_.gen_box_count);
    }
    bytes_view mdat_payload() const noexcept { return bytes_view(c_.mdat_payload); }
    bytes_view raw_boxes() const noexcept { return bytes_view(c_.raw_boxes); }

    const moq_locmaf_chunk_t &raw() const noexcept { return c_; }
};

/// Write the canonical CMAF chunk a decoded chunk defines (section 15): its
/// genBoxes, then moof and mdat in the pinned layout; for rawBoxes, the
/// boxes verbatim. Only the output bytes are allocated. errc::invalid for a
/// chunk that is not self-consistent (a chunk from decoder::push() always
/// is).
inline result<std::vector<uint8_t>> write_cmaf(const track &t, const chunk_view &chunk)
{
    const moq_locmaf_track_t ct = detail::to_c(t);
    std::size_t len = 0;
    moq_result_t rc = moq_locmaf_write_cmaf(&ct, &chunk.raw(), nullptr, 0, &len);
    if (rc < 0 && rc != MOQ_ERR_BUFFER) return errc_from_result(rc);
    std::vector<uint8_t> out(len);
    rc = moq_locmaf_write_cmaf(&ct, &chunk.raw(), out.data(), out.size(), &len);
    if (rc < 0) return errc_from_result(rc);
    out.resize(len);
    return out;
}

/// One track's decoder state (moq_locmaf_decoder_t) over its own storage.
class decoder {
    // Members are destroyed in reverse order of declaration: the state
    // first, then the storage it points into.
    detail::raw_storage                   mem_;
    std::unique_ptr<moq_locmaf_decoder_t> st_;

public:
    /// An empty decoder: every call is errc::wrong_state.
    decoder() = default;
    decoder(decoder &&o) noexcept = default;
    decoder &operator=(decoder &&o) noexcept
    {
        if (this != &o) {
            st_.reset();   // the target's state goes before its storage
            mem_ = std::move(o.mem_);
            st_ = std::move(o.st_);
        }
        return *this;
    }
    decoder(const decoder &) = delete;
    decoder &operator=(const decoder &) = delete;

    /// A decoder with no position and no reference. errc::invalid for a
    /// track or limits the C refuses.
    static result<decoder> make(const track &t, const limits &l = {})
    {
        const moq_locmaf_track_t ct = detail::to_c(t);
        moq_locmaf_decoder_cfg_t cfg;
        moq_locmaf_decoder_cfg_init(&cfg);
        cfg.max_samples = l.max_samples;
        cfg.max_subsamples = l.max_subsamples;
        cfg.max_gen_boxes = l.max_gen_boxes;
        cfg.track = &ct;
        std::size_t bytes = 0;
        moq_result_t rc = moq_locmaf_decoder_storage_size(&cfg, &bytes);
        if (rc < 0) return errc_from_result(rc);
        decoder d;
        d.st_.reset(new moq_locmaf_decoder_t{});
        d.mem_ = detail::raw_storage(bytes);
        cfg.storage = d.mem_.get();
        cfg.storage_size = d.mem_.size();
        rc = moq_locmaf_decoder_init(d.st_.get(), sizeof(moq_locmaf_decoder_t), &cfg);
        if (rc < 0) return errc_from_result(rc);
        return d;
    }

    /// Decode the LOCMAF Object at (group, object); see
    /// moq_locmaf_decoder_push() for the order rules. errc::wrong_state for a
    /// delta with no reference (reason NO_REFERENCE), errc::protocol,
    /// errc::unsupported or errc::limit for an Object that is refused
    /// (reason says why), errc::invalid for an Object ID out of order.
    /// `why`, when given, is set on every return.
    result<chunk_view> push(uint64_t group, uint64_t object, bytes_view payload,
                            reason *why = nullptr)
    {
        if (why) *why = MOQ_LOCMAF_REASON_NONE;
        if (!st_) return errc::wrong_state;
        moq_locmaf_chunk_t c{};
        moq_result_t rc = moq_locmaf_decoder_push(st_.get(), group, object, payload.raw(),
                                                  &c, sizeof(c), why);
        if (rc < 0) return errc_from_result(rc);
        return chunk_view(c);
    }

    /// Forget the position and the reference (a stream reset, or loss).
    result<void> reset() noexcept
    {
        if (!st_) return errc::wrong_state;
        moq_locmaf_decoder_reset(st_.get());
        return {};
    }

    /// Whether a delta header at (group, object) would apply.
    result<bool> expects(uint64_t group, uint64_t object) const noexcept
    {
        if (!st_) return errc::wrong_state;
        return moq_locmaf_decoder_expects(st_.get(), group, object);
    }

    /// Continue where src stands (moq_locmaf_decoder_copy): its position,
    /// reference and derived BMDT, copied into this decoder's own storage.
    /// errc::invalid for src == *this, differing track facts, or limits
    /// smaller than src's reference needs.
    result<void> copy_from(const decoder &src) noexcept
    {
        if (!st_ || !src.st_) return errc::wrong_state;
        return result_from_rc(moq_locmaf_decoder_copy(st_.get(), src.st_.get()));
    }

    /// The decoder's counters since make().
    result<decoder_stats> stats() const noexcept
    {
        if (!st_) return errc::wrong_state;
        decoder_stats s{};
        moq_result_t rc = moq_locmaf_decoder_get_stats(st_.get(), &s, sizeof(s));
        if (rc < 0) return errc_from_result(rc);
        return s;
    }
};

/// One track's encoder state (moq_locmaf_encoder_t) over its own storage.
/// An encode stages the reference it leaves; commit() adopts it.
class encoder {
    // As for the decoder: the state is destroyed before its storage.
    detail::raw_storage                   mem_;
    std::unique_ptr<moq_locmaf_encoder_t> st_;

    template <class Encode>
    result<std::vector<uint8_t>> encode_into(Encode &&encode, reason *why)
    {
        if (why) *why = MOQ_LOCMAF_REASON_NONE;
        if (!st_) return errc::wrong_state;
        std::size_t len = 0;
        moq_result_t rc = encode(nullptr, 0, &len, why);
        if (rc < 0 && rc != MOQ_ERR_BUFFER) return errc_from_result(rc);
        std::vector<uint8_t> out(len);
        rc = encode(out.data(), out.size(), &len, why);
        if (rc < 0) return errc_from_result(rc);
        out.resize(len);
        return out;
    }

public:
    /// An empty encoder: every call is errc::wrong_state.
    encoder() = default;
    encoder(encoder &&o) noexcept = default;
    encoder &operator=(encoder &&o) noexcept
    {
        if (this != &o) {
            st_.reset();   // the target's state goes before its storage
            mem_ = std::move(o.mem_);
            st_ = std::move(o.st_);
        }
        return *this;
    }
    encoder(const encoder &) = delete;
    encoder &operator=(const encoder &) = delete;

    /// An encoder with nothing committed. errc::invalid for a track or
    /// limits the C refuses.
    static result<encoder> make(const track &t, const limits &l = {})
    {
        const moq_locmaf_track_t ct = detail::to_c(t);
        moq_locmaf_encoder_cfg_t cfg;
        moq_locmaf_encoder_cfg_init(&cfg);
        cfg.max_samples = l.max_samples;
        cfg.max_subsamples = l.max_subsamples;
        cfg.max_gen_boxes = l.max_gen_boxes;
        cfg.track = &ct;
        std::size_t bytes = 0;
        moq_result_t rc = moq_locmaf_encoder_storage_size(&cfg, &bytes);
        if (rc < 0) return errc_from_result(rc);
        encoder e;
        e.st_.reset(new moq_locmaf_encoder_t{});
        e.mem_ = detail::raw_storage(bytes);
        cfg.storage = e.mem_.get();
        cfg.storage_size = e.mem_.size();
        rc = moq_locmaf_encoder_init(e.st_.get(), sizeof(moq_locmaf_encoder_t), &cfg);
        if (rc < 0) return errc_from_result(rc);
        return e;
    }

    /// Check that a CMAF chunk can be encoded, without encoding it or
    /// disturbing a stage: the length no encoding of it exceeds.
    /// errc::protocol, errc::unsupported or errc::limit with `why` set.
    result<std::size_t> check(bytes_view cmaf_chunk, reason *why = nullptr)
    {
        if (why) *why = MOQ_LOCMAF_REASON_NONE;
        if (!st_) return errc::wrong_state;
        std::size_t bound = 0;
        moq_result_t rc = moq_locmaf_encoder_check(st_.get(), cmaf_chunk.raw(), &bound, why);
        if (rc < 0) return errc_from_result(rc);
        return bound;
    }

    /// Encode one CMAF chunk as the LOCMAF Object at (group, object) and
    /// stage its reference (moq_locmaf_encode_chunk). Encoding the same
    /// chunk at the same Location again before commit() gives the same
    /// bytes. errc::protocol, errc::unsupported or errc::limit with `why`
    /// set; errc::invalid for an Object ID at or below the committed one in
    /// the same group. Nothing is staged on an error.
    result<std::vector<uint8_t>> encode_chunk(uint64_t group, uint64_t object,
                                              bytes_view cmaf_chunk, reason *why = nullptr)
    {
        return encode_into(
            [&](uint8_t *out, std::size_t cap, std::size_t *len, reason *r) {
                return moq_locmaf_encode_chunk(st_.get(), group, object, cmaf_chunk.raw(),
                                               out, cap, len, r);
            },
            why);
    }

    /// Encode complete ISO BMFF boxes verbatim as a rawBoxes Object
    /// (section 9) and stage a reference reset. Errors as encode_chunk().
    result<std::vector<uint8_t>> encode_raw_boxes(uint64_t group, uint64_t object,
                                                  bytes_view boxes, reason *why = nullptr)
    {
        return encode_into(
            [&](uint8_t *out, std::size_t cap, std::size_t *len, reason *r) {
                return moq_locmaf_encode_raw_boxes(st_.get(), group, object, boxes.raw(),
                                                   out, cap, len, r);
            },
            why);
    }

    /// Adopt what the last successful encode staged (nothing when nothing is
    /// staged).
    result<void> commit() noexcept
    {
        if (!st_) return errc::wrong_state;
        moq_locmaf_encoder_commit(st_.get());
        return {};
    }

    /// Forget the committed and staged state: the next encode is a full
    /// header.
    result<void> reset() noexcept
    {
        if (!st_) return errc::wrong_state;
        moq_locmaf_encoder_reset(st_.get());
        return {};
    }

    /// The encoder's counters since make().
    result<encoder_stats> stats() const noexcept
    {
        if (!st_) return errc::wrong_state;
        encoder_stats s{};
        moq_result_t rc = moq_locmaf_encoder_get_stats(st_.get(), &s, sizeof(s));
        if (rc < 0) return errc_from_result(rc);
        return s;
    }
};

} // namespace moq::locmaf

#endif // MOQ_LOCMAF_HPP
