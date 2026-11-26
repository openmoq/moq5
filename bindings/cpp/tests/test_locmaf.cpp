#include <moq/locmaf.hpp>
#include "test_support.hpp"

extern "C" {
#include "media_builders.h"
}

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

// -- Counting replacements of the global allocation functions -------------
//
// While `tracking` is set, every allocation and deallocation through the
// replaceable global functions is recorded in fixed tables (an allocation
// function must not allocate). A deallocation of a block allocated before
// tracking began is passed through unrecorded. `fail_next_aligned` makes the
// next aligned allocation throw std::bad_alloc.

namespace {

enum class ev { plain_alloc, plain_free, aligned_alloc, aligned_free };

struct event {
    ev          kind;
    void       *p;
    std::size_t n;      // the block's size (a free reports its block's)
    std::size_t align;  // 0 for the plain functions
};

struct block {
    void       *p;
    std::size_t n;
    std::size_t align;
    bool        live;
};

constexpr std::size_t max_records = 256;
event       events[max_records];
std::size_t event_count = 0;
block       blocks[max_records];
std::size_t block_count = 0;
bool        tracking = false;
bool        overflowed = false;
bool        fail_next_aligned = false;
int         bad_frees = 0;   // a tracked block freed by the other kind, or with another alignment

void note(ev kind, void *p, std::size_t n, std::size_t align)
{
    if (event_count == max_records) {
        overflowed = true;
        return;
    }
    events[event_count++] = event{kind, p, n, align};
}

void note_alloc(ev kind, void *p, std::size_t n, std::size_t align)
{
    if (!tracking) return;
    if (block_count == max_records) {
        overflowed = true;
        return;
    }
    blocks[block_count++] = block{p, n, align, true};
    note(kind, p, n, align);
}

void note_free(ev kind, void *p, std::size_t align)
{
    if (!tracking) return;
    for (std::size_t i = block_count; i-- > 0;) {
        block &b = blocks[i];
        if (b.p != p || !b.live) continue;
        b.live = false;
        if (b.align != align) bad_frees++;
        note(kind, p, b.n, align);
        return;
    }
}

void *aligned_block(std::size_t n, std::size_t align)
{
    if (align < sizeof(void *)) align = sizeof(void *);
#ifdef _WIN32
    return _aligned_malloc(n ? n : 1, align);
#else
    void *p = nullptr;
    return posix_memalign(&p, align, n ? n : 1) == 0 ? p : nullptr;
#endif
}

void free_aligned_block(void *p)
{
#ifdef _WIN32
    _aligned_free(p);
#else
    std::free(p);
#endif
}

void begin_tracking()
{
    event_count = 0;
    block_count = 0;
    overflowed = false;
    bad_frees = 0;
    fail_next_aligned = false;
    tracking = true;
}

void end_tracking() { tracking = false; }

std::size_t count(ev kind, std::size_t n = 0)
{
    std::size_t c = 0;
    for (std::size_t i = 0; i < event_count; i++)
        if (events[i].kind == kind && (n == 0 || events[i].n == n)) c++;
    return c;
}

std::size_t live_blocks()
{
    std::size_t c = 0;
    for (std::size_t i = 0; i < block_count; i++)
        if (blocks[i].live) c++;
    return c;
}

// The position in the event log of the free of block p, or max_records.
std::size_t free_position(const void *p)
{
    for (std::size_t i = 0; i < event_count; i++)
        if ((events[i].kind == ev::plain_free || events[i].kind == ev::aligned_free) &&
            events[i].p == p)
            return i;
    return max_records;
}

const void *alloc_of(ev kind, std::size_t n)
{
    for (std::size_t i = 0; i < event_count; i++)
        if (events[i].kind == kind && events[i].n == n) return events[i].p;
    return nullptr;
}

} // namespace

void *operator new(std::size_t n)
{
    void *p = std::malloc(n ? n : 1);
    if (!p) throw std::bad_alloc();
    note_alloc(ev::plain_alloc, p, n, 0);
    return p;
}

void operator delete(void *p) noexcept
{
    if (!p) return;
    note_free(ev::plain_free, p, 0);
    std::free(p);
}

void operator delete(void *p, std::size_t) noexcept { ::operator delete(p); }

void *operator new(std::size_t n, std::align_val_t al)
{
    if (fail_next_aligned) {
        fail_next_aligned = false;
        throw std::bad_alloc();
    }
    void *p = aligned_block(n, static_cast<std::size_t>(al));
    if (!p) throw std::bad_alloc();
    note_alloc(ev::aligned_alloc, p, n, static_cast<std::size_t>(al));
    return p;
}

void operator delete(void *p, std::align_val_t al) noexcept
{
    if (!p) return;
    note_free(ev::aligned_free, p, static_cast<std::size_t>(al));
    free_aligned_block(p);
}

void operator delete(void *p, std::size_t, std::align_val_t al) noexcept
{
    ::operator delete(p, al);
}

// -- Fixtures (draft-einarsson-moq-locmaf-01, hand-built) -----------------
//
// Track A: track_ID 1, trex sample description index 1, duration 1024,
// size 0, flags 0x02000000; clear. F1 is a full header for one 4-byte
// sample at BMDT 0 (Object 0); F2 an empty delta, the next chunk of the
// same group (Object 1, BMDT 1024). Each canonical chunk is moof + mdat.

namespace {

std::vector<uint8_t> hex(std::string_view s)
{
    auto val = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        return -1;
    };
    std::vector<uint8_t> out;
    int hi = -1;
    for (char c : s) {
        int v = val(c);
        if (v < 0) continue;
        if (hi < 0) {
            hi = v;
        } else {
            out.push_back(static_cast<uint8_t>(hi << 4 | v));
            hi = -1;
        }
    }
    return out;
}

constexpr std::string_view moof_head =
    "0000005C 6D6F6F66 00000010 6D666864 00000000 00000000 00000044 74726166"
    "00000014 74666864 00020010 00000001 00000004"
    "00000014 74666474 01000000 00000000";
constexpr std::string_view moof_tail =
    "00000014 7472756E 00000001 00000001 00000064"
    "0000000C 6D646174";

std::vector<uint8_t> canonical(std::string_view bmdt, std::string_view payload)
{
    std::string s(moof_head);
    s += bmdt;
    s += moof_tail;
    s += payload;
    return hex(s);
}

const std::vector<uint8_t> f1_object = hex("02 04 0A 00 0E 01 DEADBEEF");
const std::vector<uint8_t> f2_object = hex("03 00 CAFEBABE");
const std::vector<uint8_t> f3_object = hex("03 00 01020304");
const std::vector<uint8_t> f1_chunk = canonical("00000000", "DEADBEEF");
const std::vector<uint8_t> f2_chunk = canonical("00000400", "CAFEBABE");
const std::vector<uint8_t> f3_chunk = canonical("00000800", "01020304");

moq::locmaf::track track_a()
{
    moq::locmaf::track t;
    t.track_id = 1;
    t.trex_default_sample_description_index = 1;
    t.trex_default_sample_duration = 1024;
    t.trex_default_sample_size = 0;
    t.trex_default_sample_flags = 0x02000000u;
    return t;
}

moq::bytes_view view(const std::vector<uint8_t> &v) { return {v.data(), v.size()}; }

bool same(const std::vector<uint8_t> &a, const std::vector<uint8_t> &b) { return a == b; }

std::size_t decoder_storage_size()
{
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    std::size_t n = 0;
    return moq_locmaf_decoder_storage_size(&cfg, &n) == MOQ_OK ? n : 0;
}

std::size_t encoder_storage_size()
{
    moq_locmaf_encoder_cfg_t cfg;
    moq_locmaf_encoder_cfg_init(&cfg);
    std::size_t n = 0;
    return moq_locmaf_encoder_storage_size(&cfg, &n) == MOQ_OK ? n : 0;
}

} // namespace

static_assert(!std::is_copy_constructible_v<moq::locmaf::decoder>);
static_assert(!std::is_copy_assignable_v<moq::locmaf::decoder>);
static_assert(std::is_nothrow_move_constructible_v<moq::locmaf::decoder>);
static_assert(std::is_nothrow_move_assignable_v<moq::locmaf::decoder>);
static_assert(!std::is_copy_constructible_v<moq::locmaf::encoder>);
static_assert(!std::is_copy_assignable_v<moq::locmaf::encoder>);
static_assert(std::is_nothrow_move_constructible_v<moq::locmaf::encoder>);
static_assert(std::is_nothrow_move_assignable_v<moq::locmaf::encoder>);

int main()
{
    int failures = 0;
    const std::size_t dec_bytes = decoder_storage_size();
    const std::size_t enc_bytes = encoder_storage_size();
    MOQ_CHECK(dec_bytes > 0 && enc_bytes > 0);

    // track: the header facts from a CMAF Header, or the reasons it has none.
    {
        static const uint32_t trex[4] = {1, 1024, 0, 0x02000000u};
        uint8_t init[512];
        size_t len = moq_test_build_cmaf_init_trex(init, 1, 48000, trex, 1, true);
        auto t = moq::locmaf::track::from_init(moq::bytes_view(init, len));
        MOQ_CHECK(t.ok());
        if (t.ok()) MOQ_CHECK(*t == track_a());

        moq_cmaf_init_info_t ci;
        moq_cmaf_init_info_init(&ci);
        MOQ_CHECK(moq_cmaf_parse_init_sized(moq::bytes_view(init, len).raw(), &ci,
                                            sizeof(ci)) == MOQ_OK);
        auto c = moq::locmaf::track::from_cmaf(ci);
        MOQ_CHECK(c.ok() && *c == track_a());
        // A record an older library stamped carries no header facts.
        ci.struct_size = MOQ_CMAF_INIT_INFO_SIZE_THROUGH_RESERVED;
        auto old = moq::locmaf::track::from_cmaf(ci);
        MOQ_CHECK(!old.ok() && old.error().code() == moq::errc::invalid);

        len = moq_test_build_cmaf_init_trex(init, 1, 48000, trex, 1, false);
        auto no_trex = moq::locmaf::track::from_init(moq::bytes_view(init, len));
        MOQ_CHECK(!no_trex.ok() && no_trex.error().code() == moq::errc::protocol);
        len = moq_test_build_cmaf_init_trex(init, 1, 48000, trex, 2, true);
        auto two = moq::locmaf::track::from_init(moq::bytes_view(init, len));
        MOQ_CHECK(!two.ok() && two.error().code() == moq::errc::protocol);
        auto bad = moq::locmaf::track::from_init(moq::bytes_view(init, 4));
        MOQ_CHECK(!bad.ok() && bad.error().code() == moq::errc::protocol);
    }

    // decode: a full header then a delta, their effective values, and the
    // canonical chunks they define.
    {
        auto d = moq::locmaf::decoder::make(track_a());
        MOQ_CHECK(d.ok());
        if (d.ok()) {
            moq_locmaf_reason_t why = 99;
            auto c1 = d->push(0, 0, view(f1_object), &why);
            MOQ_CHECK(c1.ok() && why == MOQ_LOCMAF_REASON_NONE);
            if (c1.ok()) {
                MOQ_CHECK(c1->kind() == MOQ_LOCMAF_OBJECT_FULL);
                MOQ_CHECK(c1->base_media_decode_time() == 0);
                MOQ_CHECK(c1->sample_description_index() == 1);
                MOQ_CHECK(c1->samples().size() == 1);
                if (c1->samples().size() == 1) {
                    MOQ_CHECK(c1->samples()[0].size == 4);
                    MOQ_CHECK(c1->samples()[0].duration == 1024);
                    MOQ_CHECK(c1->samples()[0].flags == 0x02000000u);
                }
                MOQ_CHECK(!c1->has_aux_info() && !c1->has_subsamples());
                MOQ_CHECK(c1->ivs().empty() && c1->subsamples().empty());
                MOQ_CHECK(c1->gen_boxes().empty() && c1->raw_boxes().empty());
                // The mdat payload borrows the pushed Object.
                MOQ_CHECK(c1->mdat_payload().size() == 4 &&
                          c1->mdat_payload().data() == f1_object.data() + 6);
                auto w = moq::locmaf::write_cmaf(track_a(), *c1);
                MOQ_CHECK(w.ok() && same(*w, f1_chunk));
            }
            MOQ_CHECK(d->expects(0, 1).ok() && *d->expects(0, 1));
            MOQ_CHECK(d->expects(0, 2).ok() && !*d->expects(0, 2));
            auto c2 = d->push(0, 1, view(f2_object));
            MOQ_CHECK(c2.ok());
            if (c2.ok()) {
                MOQ_CHECK(c2->kind() == MOQ_LOCMAF_OBJECT_DELTA);
                MOQ_CHECK(c2->base_media_decode_time() == 1024);
                auto w = moq::locmaf::write_cmaf(track_a(), *c2);
                MOQ_CHECK(w.ok() && same(*w, f2_chunk));
            }
            auto st = d->stats();
            MOQ_CHECK(st.ok());
            if (st.ok()) {
                MOQ_CHECK(st->objects_decoded == 2);
                MOQ_CHECK(st->full_headers_decoded == 1 && st->delta_headers_decoded == 1);
            }
        }
    }

    // decode refusals: each C result is the binding's error, with its reason.
    {
        auto d = moq::locmaf::decoder::make(track_a());
        MOQ_CHECK(d.ok());
        if (d.ok()) {
            moq_locmaf_reason_t why = 0;
            auto unref = d->push(0, 0, view(f2_object), &why);
            MOQ_CHECK(!unref.ok() && unref.error().code() == moq::errc::wrong_state);
            MOQ_CHECK(why == MOQ_LOCMAF_REASON_NO_REFERENCE);
            const uint8_t unknown[] = {0x05};
            auto bad = d->push(0, 1, moq::bytes_view(unknown, 1), &why);
            MOQ_CHECK(!bad.ok() && bad.error().code() == moq::errc::protocol);
            MOQ_CHECK(why == MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
            MOQ_CHECK(std::string_view(moq::locmaf::reason_name(why)) ==
                      std::string_view(moq_locmaf_reason_name(why)));
            auto back = d->push(0, 0, view(f1_object), &why);
            MOQ_CHECK(!back.ok() && back.error().code() == moq::errc::invalid);
            auto st = d->stats();
            MOQ_CHECK(st.ok() && st->objects_unreferenced == 1 && st->objects_malformed == 1 &&
                      st->last_reason == MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT);
            MOQ_CHECK(d->reset().ok());
            auto again = d->push(1, 0, view(f1_object));
            MOQ_CHECK(again.ok() && again->kind() == MOQ_LOCMAF_OBJECT_FULL);
        }
        // Limits above their maxima, and a track the C refuses.
        begin_tracking();
        {
            moq::locmaf::limits big;
            big.max_samples = 65537;
            auto r = moq::locmaf::decoder::make(track_a(), big);
            MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::invalid);
        }
        end_tracking();
        MOQ_CHECK(count(ev::plain_alloc) == 0 && count(ev::aligned_alloc) == 0);
        begin_tracking();
        {
            moq::locmaf::track t = track_a();
            t.is_protected = true;
            t.default_per_sample_iv_size = 3;
            auto r = moq::locmaf::decoder::make(t);
            MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::invalid);
        }
        end_tracking();
        MOQ_CHECK(count(ev::aligned_alloc, dec_bytes) == 1 &&
                  count(ev::aligned_free, dec_bytes) == 1);
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);
    }

    // write_cmaf re-validates a chunk the caller built.
    {
        auto d = moq::locmaf::decoder::make(track_a());
        if (d.ok()) {
            auto c = d->push(0, 0, view(f1_object));
            MOQ_CHECK(c.ok());
            if (c.ok()) {
                moq_locmaf_chunk_t raw = c->raw();
                raw.mdat_payload.len = 3;   // the sample sizes no longer sum to it
                auto w = moq::locmaf::write_cmaf(track_a(), moq::locmaf::chunk_view(raw));
                MOQ_CHECK(!w.ok() && w.error().code() == moq::errc::invalid);
            }
        }
    }

    // storage_balance: make, push, destroy => one aligned allocation of the
    // storage size at MOQ_LOCMAF_STORAGE_ALIGN, freed once with the same
    // alignment, and the state freed once, before its storage.
    {
        begin_tracking();
        {
            auto d = moq::locmaf::decoder::make(track_a());
            MOQ_CHECK(d.ok());
            if (d.ok()) MOQ_CHECK(d->push(0, 0, view(f1_object)).ok());
        }
        end_tracking();
        MOQ_CHECK(count(ev::aligned_alloc) == 1 && count(ev::aligned_free) == 1);
        MOQ_CHECK(count(ev::aligned_alloc, dec_bytes) == 1);
        for (std::size_t i = 0; i < event_count; i++)
            if (events[i].kind == ev::aligned_alloc)
                MOQ_CHECK(events[i].align == MOQ_LOCMAF_STORAGE_ALIGN);
        const void *st = alloc_of(ev::plain_alloc, sizeof(moq_locmaf_decoder_t));
        const void *mem = alloc_of(ev::aligned_alloc, dec_bytes);
        MOQ_CHECK(st && mem);
        MOQ_CHECK(free_position(st) < free_position(mem));
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);
    }

    // move_construct_keeps_view: moving the wrapper relocates nothing, so a
    // chunk view taken before the move still reads its bytes, and the chain
    // continues on the new owner.
    {
        auto made = moq::locmaf::decoder::make(track_a());
        MOQ_CHECK(made.ok());
        if (made.ok()) {
            moq::locmaf::decoder d = std::move(*made);
            auto c1 = d.push(0, 0, view(f1_object));
            MOQ_CHECK(c1.ok());
            if (c1.ok()) {
                const moq_locmaf_sample_t *table = c1->samples().data();
                moq::locmaf::decoder moved(std::move(d));
                MOQ_CHECK(c1->samples().data() == table && c1->samples().size() == 1);
                MOQ_CHECK(c1->samples()[0].size == 4 && c1->samples()[0].duration == 1024);
                auto w = moq::locmaf::write_cmaf(track_a(), *c1);
                MOQ_CHECK(w.ok() && same(*w, f1_chunk));
                // The new owner continues the group; its table is the same storage.
                auto c2 = moved.push(0, 1, view(f2_object));
                MOQ_CHECK(c2.ok() && c2->kind() == MOQ_LOCMAF_OBJECT_DELTA);
                if (c2.ok()) MOQ_CHECK(c2->samples().data() == table);
            }
        }
    }

    // move_assign_releases_target: assigning over a live decoder frees the
    // target's state and then its storage, each once; the source's storage
    // moves without being freed.
    {
        begin_tracking();
        const void *target_state = nullptr;
        const void *target_mem = nullptr;
        {
            auto a = moq::locmaf::decoder::make(track_a());
            target_state = alloc_of(ev::plain_alloc, sizeof(moq_locmaf_decoder_t));
            target_mem = alloc_of(ev::aligned_alloc, dec_bytes);
            auto b = moq::locmaf::decoder::make(track_a());
            MOQ_CHECK(a.ok() && b.ok());
            if (a.ok() && b.ok()) {
                MOQ_CHECK(b->push(0, 0, view(f1_object)).ok());
                std::size_t frees_before = count(ev::aligned_free) + count(ev::plain_free);
                *a = std::move(*b);
                MOQ_CHECK(count(ev::aligned_free) == 1 && count(ev::plain_free, sizeof(moq_locmaf_decoder_t)) == 1);
                MOQ_CHECK(count(ev::aligned_free) + count(ev::plain_free) == frees_before + 2);
                MOQ_CHECK(free_position(target_state) < free_position(target_mem));
                MOQ_CHECK(free_position(target_mem) < max_records);
                // The target now continues the source's chain.
                auto c2 = a->push(0, 1, view(f2_object));
                MOQ_CHECK(c2.ok() && c2->kind() == MOQ_LOCMAF_OBJECT_DELTA);
                MOQ_CHECK(!b->push(0, 1, view(f2_object)).ok());
            }
        }
        end_tracking();
        MOQ_CHECK(count(ev::aligned_alloc) == 2 && count(ev::aligned_free) == 2);
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);
    }

    // moved_from_is_inert: every call on a moved-from decoder is refused with
    // errc::wrong_state, frees nothing, and its destruction frees nothing.
    {
        auto other = moq::locmaf::decoder::make(track_a());
        begin_tracking();
        auto made = moq::locmaf::decoder::make(track_a());
        MOQ_CHECK(made.ok() && other.ok());
        if (made.ok() && other.ok()) {
            {
                moq::locmaf::decoder live(std::move(*made));
                moq::locmaf::decoder &gone = *made;
                moq_locmaf_reason_t why = 7;
                auto p = gone.push(0, 0, view(f1_object), &why);
                MOQ_CHECK(!p.ok() && p.error().code() == moq::errc::wrong_state);
                MOQ_CHECK(why == MOQ_LOCMAF_REASON_NONE);
                auto r = gone.reset();
                MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::wrong_state);
                auto e = gone.expects(0, 0);
                MOQ_CHECK(!e.ok() && e.error().code() == moq::errc::wrong_state);
                auto s = gone.stats();
                MOQ_CHECK(!s.ok() && s.error().code() == moq::errc::wrong_state);
                auto into = gone.copy_from(*other);
                MOQ_CHECK(!into.ok() && into.error().code() == moq::errc::wrong_state);
                auto from = other->copy_from(gone);
                MOQ_CHECK(!from.ok() && from.error().code() == moq::errc::wrong_state);
                MOQ_CHECK(count(ev::plain_free) == 0 && count(ev::aligned_free) == 0);
                gone = moq::locmaf::decoder();   // assigning an empty one frees nothing either
                MOQ_CHECK(count(ev::plain_free) == 0 && count(ev::aligned_free) == 0);
                MOQ_CHECK(live.push(0, 0, view(f1_object)).ok());
            }
            // Only `live` owned anything: one state and one storage freed.
            MOQ_CHECK(count(ev::aligned_free) == 1 &&
                      count(ev::plain_free, sizeof(moq_locmaf_decoder_t)) == 1);
        }
        end_tracking();
        MOQ_CHECK(bad_frees == 0 && !overflowed);
    }

    // copy_from_independent: after copy_from, pushes on both give identical
    // chunks, and destroying either leaves the other usable.
    {
        auto src = moq::locmaf::decoder::make(track_a());
        auto dst = moq::locmaf::decoder::make(track_a());
        MOQ_CHECK(src.ok() && dst.ok());
        if (src.ok() && dst.ok()) {
            MOQ_CHECK(src->push(0, 0, view(f1_object)).ok());
            MOQ_CHECK(dst->copy_from(*src).ok());
            MOQ_CHECK(dst->expects(0, 1).ok() && *dst->expects(0, 1));
            auto a = src->push(0, 1, view(f2_object));
            auto b = dst->push(0, 1, view(f2_object));
            MOQ_CHECK(a.ok() && b.ok());
            if (a.ok() && b.ok()) {
                MOQ_CHECK(a->samples().data() != b->samples().data());
                auto wa = moq::locmaf::write_cmaf(track_a(), *a);
                auto wb = moq::locmaf::write_cmaf(track_a(), *b);
                MOQ_CHECK(wa.ok() && wb.ok() && same(*wa, *wb) && same(*wa, f2_chunk));
            }
            *src = moq::locmaf::decoder();   // destroy the source's state and storage
            auto c = dst->push(0, 2, view(f3_object));
            MOQ_CHECK(c.ok() && c->base_media_decode_time() == 2048);
            if (c.ok()) {
                auto w = moq::locmaf::write_cmaf(track_a(), *c);
                MOQ_CHECK(w.ok() && same(*w, f3_chunk));
            }
            // Differing track facts are refused, and so is a copy onto itself.
            moq::locmaf::track other_track = track_a();
            other_track.trex_default_sample_duration = 960;
            auto o = moq::locmaf::decoder::make(other_track);
            MOQ_CHECK(o.ok());
            if (o.ok()) {
                auto diff = o->copy_from(*dst);
                MOQ_CHECK(!diff.ok() && diff.error().code() == moq::errc::invalid);
            }
            auto self = dst->copy_from(*dst);
            MOQ_CHECK(!self.ok() && self.error().code() == moq::errc::invalid);
        }
    }

    // make_bad_alloc: the storage allocation throws => make propagates
    // std::bad_alloc, the state allocated before it is freed, and the counts
    // balance.
    {
        begin_tracking();
        fail_next_aligned = true;
        bool threw = false;
        try {
            auto r = moq::locmaf::decoder::make(track_a());
            (void)r;
        } catch (const std::bad_alloc &) {
            threw = true;
        }
        end_tracking();
        MOQ_CHECK(threw);
        MOQ_CHECK(count(ev::aligned_alloc) == 0);
        MOQ_CHECK(count(ev::plain_alloc, sizeof(moq_locmaf_decoder_t)) == 1 &&
                  count(ev::plain_free, sizeof(moq_locmaf_decoder_t)) == 1);
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);

        begin_tracking();
        fail_next_aligned = true;
        threw = false;
        try {
            auto r = moq::locmaf::encoder::make(track_a());
            (void)r;
        } catch (const std::bad_alloc &) {
            threw = true;
        }
        end_tracking();
        MOQ_CHECK(threw);
        MOQ_CHECK(count(ev::plain_alloc, sizeof(moq_locmaf_encoder_t)) == 1 &&
                  count(ev::plain_free, sizeof(moq_locmaf_encoder_t)) == 1);
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);
    }

    // encode: the canonical chunks become the fixture Objects; nothing is the
    // reference until commit; refusals carry their reason.
    {
        auto e = moq::locmaf::encoder::make(track_a());
        MOQ_CHECK(e.ok());
        if (e.ok()) {
            auto bound = e->check(view(f1_chunk));
            MOQ_CHECK(bound.ok());
            auto o1 = e->encode_chunk(0, 0, view(f1_chunk));
            MOQ_CHECK(o1.ok() && same(*o1, f1_object));
            if (bound.ok() && o1.ok()) MOQ_CHECK(o1->size() <= *bound);
            // Retried before commit: the same bytes.
            auto again = e->encode_chunk(0, 0, view(f1_chunk));
            MOQ_CHECK(again.ok() && same(*again, f1_object));
            MOQ_CHECK(e->commit().ok());
            auto o2 = e->encode_chunk(0, 1, view(f2_chunk));
            MOQ_CHECK(o2.ok() && same(*o2, f2_object));
            MOQ_CHECK(e->commit().ok());

            moq_locmaf_reason_t why = 0;
            const uint8_t junk[] = {0x00, 0x00, 0x00, 0x08, 'j', 'u', 'n', 'k'};
            auto bad = e->encode_chunk(0, 2, moq::bytes_view(junk, sizeof(junk)), &why);
            MOQ_CHECK(!bad.ok() && why != MOQ_LOCMAF_REASON_NONE);
            auto checked = e->check(moq::bytes_view(junk, sizeof(junk)), &why);
            MOQ_CHECK(!checked.ok() && why != MOQ_LOCMAF_REASON_NONE);
            auto below = e->encode_chunk(0, 1, view(f2_chunk));
            MOQ_CHECK(!below.ok() && below.error().code() == moq::errc::invalid);

            // rawBoxes: the boxes verbatim, decoded and written back unchanged.
            auto raw = e->encode_raw_boxes(1, 0, view(f1_chunk));
            MOQ_CHECK(raw.ok());
            MOQ_CHECK(e->commit().ok());
            auto d = moq::locmaf::decoder::make(track_a());
            if (raw.ok() && d.ok()) {
                auto c = d->push(1, 0, view(*raw));
                MOQ_CHECK(c.ok() && c->kind() == MOQ_LOCMAF_OBJECT_RAW_BOXES);
                if (c.ok()) {
                    MOQ_CHECK(c->raw_boxes().size() == f1_chunk.size());
                    auto w = moq::locmaf::write_cmaf(track_a(), *c);
                    MOQ_CHECK(w.ok() && same(*w, f1_chunk));
                }
            }
            auto st = e->stats();
            MOQ_CHECK(st.ok());
            if (st.ok()) {
                MOQ_CHECK(st->full_headers_written == 2 && st->delta_headers_written == 1);
                MOQ_CHECK(st->raw_boxes_written == 1 && st->stages_committed == 3);
                MOQ_CHECK(st->chunks_refused == 2 && st->stages_replaced == 1);
            }
            MOQ_CHECK(e->reset().ok());
            auto fresh = e->encode_chunk(1, 1, view(f2_chunk));
            MOQ_CHECK(fresh.ok() && (*fresh)[0] == MOQ_LOCMAF_ELEMENT_FULL_HEADER);
        }
    }

    // encoder_storage_balance: make, encode, commit, destroy => one aligned
    // allocation of the encoder storage, freed once after its state.
    {
        begin_tracking();
        {
            auto e = moq::locmaf::encoder::make(track_a());
            MOQ_CHECK(e.ok());
            if (e.ok()) {
                MOQ_CHECK(e->encode_chunk(0, 0, view(f1_chunk)).ok());
                MOQ_CHECK(e->commit().ok());
            }
        }
        end_tracking();
        MOQ_CHECK(count(ev::aligned_alloc) == 1 && count(ev::aligned_free) == 1);
        MOQ_CHECK(count(ev::aligned_alloc, enc_bytes) == 1);
        const void *st = alloc_of(ev::plain_alloc, sizeof(moq_locmaf_encoder_t));
        const void *mem = alloc_of(ev::aligned_alloc, enc_bytes);
        MOQ_CHECK(st && mem && free_position(st) < free_position(mem));
        MOQ_CHECK(live_blocks() == 0 && bad_frees == 0 && !overflowed);
    }

    // encoder_move: the committed chain survives a move construction and a
    // move assignment; the moved-from encoder is inert and frees nothing.
    {
        auto made = moq::locmaf::encoder::make(track_a());
        MOQ_CHECK(made.ok());
        if (made.ok()) {
            MOQ_CHECK(made->encode_chunk(0, 0, view(f1_chunk)).ok());
            MOQ_CHECK(made->commit().ok());
            moq::locmaf::encoder moved(std::move(*made));
            auto o2 = moved.encode_chunk(0, 1, view(f2_chunk));
            MOQ_CHECK(o2.ok() && same(*o2, f2_object));
            MOQ_CHECK(moved.commit().ok());

            begin_tracking();
            auto target = moq::locmaf::encoder::make(track_a());
            MOQ_CHECK(target.ok());
            const void *target_state = alloc_of(ev::plain_alloc, sizeof(moq_locmaf_encoder_t));
            const void *target_mem = alloc_of(ev::aligned_alloc, enc_bytes);
            *target = std::move(moved);
            end_tracking();
            MOQ_CHECK(count(ev::aligned_free, enc_bytes) == 1 &&
                      count(ev::plain_free, sizeof(moq_locmaf_encoder_t)) == 1);
            MOQ_CHECK(target_state && target_mem &&
                      free_position(target_state) < free_position(target_mem));
            auto o3 = target->encode_chunk(0, 2, view(f3_chunk));
            MOQ_CHECK(o3.ok() && same(*o3, f3_object));

            begin_tracking();
            for (moq::locmaf::encoder *gone : {&*made, &moved}) {
                auto c = gone->check(view(f1_chunk));
                MOQ_CHECK(!c.ok() && c.error().code() == moq::errc::wrong_state);
                auto x = gone->encode_chunk(0, 3, view(f1_chunk));
                MOQ_CHECK(!x.ok() && x.error().code() == moq::errc::wrong_state);
                auto rb = gone->encode_raw_boxes(0, 3, view(f1_chunk));
                MOQ_CHECK(!rb.ok() && rb.error().code() == moq::errc::wrong_state);
                auto cm = gone->commit();
                MOQ_CHECK(!cm.ok() && cm.error().code() == moq::errc::wrong_state);
                auto rs = gone->reset();
                MOQ_CHECK(!rs.ok() && rs.error().code() == moq::errc::wrong_state);
                auto st = gone->stats();
                MOQ_CHECK(!st.ok() && st.error().code() == moq::errc::wrong_state);
            }
            end_tracking();
            MOQ_CHECK(count(ev::plain_free) == 0 && count(ev::aligned_free) == 0);
            MOQ_CHECK(bad_frees == 0 && !overflowed);
        }
    }

    MOQ_PASS("cpp_locmaf");
    if (failures) std::printf("FAIL: cpp_locmaf: %d failures\n", failures);
    return failures ? 1 : 0;
}
