/*
 * The LOCMAF decoder: the in-group reference (section 12.1), full and delta
 * headers (11, 12), deletions (12.3), the BMDT continuation (12.2), sample
 * sizes (11.2), effective values (15.1) and the CENC derivation (10.3, 13,
 * 15.8), with every bound of section 18 checked before it is used.
 */
#include "locmaf_internal.h"

/* -- Storage layout ------------------------------------------------------------ */

void *locmaf_carve_take(locmaf_carve_t *c, size_t count, size_t elem)
{
    if (!c->ok) return NULL;
    size_t off = c->off, rem = off % MOQ_LOCMAF_STORAGE_ALIGN;
    if (rem && !lm_size_add(off, MOQ_LOCMAF_STORAGE_ALIGN - rem, &off)) {
        c->ok = false;
        return NULL;
    }
    if (elem && count > SIZE_MAX / elem) { c->ok = false; return NULL; }
    size_t end;
    if (!lm_size_add(off, count * elem, &end)) { c->ok = false; return NULL; }
    c->off = end;
    return c->base ? c->base + off : NULL;
}

void locmaf_carve_ref(locmaf_carve_t *c, const locmaf_limits_t *lim,
                      moq_locmaf_ref_state_t *ref)
{
    size_t s = lim->samples, u = lim->subsamples;
    ref->_sizes = locmaf_carve_take(c, s, sizeof(uint32_t));
    ref->_durations = locmaf_carve_take(c, s, sizeof(uint32_t));
    ref->_flags = locmaf_carve_take(c, s, sizeof(uint32_t));
    ref->_subsample_counts = locmaf_carve_take(c, s, sizeof(uint32_t));
    ref->_ctos = locmaf_carve_take(c, s, sizeof(int64_t));
    ref->_ivs = locmaf_carve_take(c, s, 16);
    ref->_clear = locmaf_carve_take(c, u, sizeof(uint32_t));
    ref->_protected = locmaf_carve_take(c, u, sizeof(uint32_t));
}

void locmaf_ref_clear(moq_locmaf_ref_state_t *ref)
{
    ref->_present = 0;
    memset(ref->_count, 0, sizeof(ref->_count));
    memset(ref->_scalar, 0, sizeof(ref->_scalar));
}

int64_t locmaf_ref_elem(const moq_locmaf_ref_state_t *ref, unsigned id, size_t i)
{
    switch (id) {
    case 1:  return ref->_sizes[i];
    case 3:  return ref->_durations[i];
    case 5:  return ref->_ctos[i];
    case 7:  return ref->_flags[i];
    case 11: return ref->_subsample_counts[i];
    case 13: return ref->_clear[i];
    default: return ref->_protected[i];
    }
}

/* Store a value already checked against the field's range. */
static void ref_store(moq_locmaf_ref_state_t *ref, unsigned id, size_t i, int64_t v)
{
    switch (id) {
    case 1:  ref->_sizes[i] = (uint32_t)v; break;
    case 3:  ref->_durations[i] = (uint32_t)v; break;
    case 5:  ref->_ctos[i] = v; break;
    case 7:  ref->_flags[i] = (uint32_t)v; break;
    case 11: ref->_subsample_counts[i] = (uint32_t)v; break;
    case 13: ref->_clear[i] = (uint32_t)v; break;
    default: ref->_protected[i] = (uint32_t)v; break;
    }
}

/* The decoder's regions after the state: one reference slot, then the
 * output tables. Measures when base is NULL. */
static bool decoder_layout(const locmaf_limits_t *lim, uint8_t *base,
                           moq_locmaf_decoder_t *d, size_t *total)
{
    locmaf_carve_t c = { base, 0, true };
    moq_locmaf_ref_state_t scratch;
    moq_locmaf_ref_state_t *ref = d ? &d->_ref : &scratch;
    locmaf_carve_ref(&c, lim, ref);
    moq_locmaf_sample_t *samples =
        locmaf_carve_take(&c, lim->samples, sizeof(moq_locmaf_sample_t));
    moq_locmaf_subsample_t *subs =
        locmaf_carve_take(&c, lim->subsamples, sizeof(moq_locmaf_subsample_t));
    moq_locmaf_gen_box_t *boxes =
        locmaf_carve_take(&c, lim->gen_boxes, sizeof(moq_locmaf_gen_box_t));
    if (!c.ok) return false;
    if (d) {
        d->_samples = samples;
        d->_subsamples = subs;
        d->_gen_boxes = boxes;
    }
    *total = c.off;
    return true;
}

/* -- Configuration --------------------------------------------------------------- */

void moq_locmaf_decoder_cfg_init_sized(moq_locmaf_decoder_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < MOQ_LOCMAF_DECODER_CFG_V0_SIZE) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

/* The caller's cfg, byte-copied at its one extent. */
static moq_result_t read_cfg(const moq_locmaf_decoder_cfg_t *cfg,
                             moq_locmaf_decoder_cfg_t *out, locmaf_limits_t *lim)
{
    if (!cfg) return MOQ_ERR_INVAL;
    uint32_t size;
    memcpy(&size, &cfg->struct_size, sizeof(size));
    if (size < MOQ_LOCMAF_DECODER_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    memcpy(out, cfg, MOQ_LOCMAF_DECODER_CFG_V0_SIZE);
    return locmaf_limits_resolve(out->max_samples, out->max_subsamples,
                                 out->max_gen_boxes, lim);
}

moq_result_t moq_locmaf_decoder_storage_size(const moq_locmaf_decoder_cfg_t *cfg,
                                             size_t *out_bytes)
{
    moq_locmaf_decoder_cfg_t c;
    locmaf_limits_t lim;
    if (!out_bytes) return MOQ_ERR_INVAL;
    moq_result_t rc = read_cfg(cfg, &c, &lim);
    if (rc != MOQ_OK) return rc;
    size_t total;
    if (!decoder_layout(&lim, NULL, NULL, &total)) return MOQ_ERR_INVAL;
    *out_bytes = total;
    return MOQ_OK;
}

moq_result_t moq_locmaf_decoder_init(moq_locmaf_decoder_t *d, size_t d_size,
                                     const moq_locmaf_decoder_cfg_t *cfg)
{
    if (!d || d_size < sizeof(*d)) return MOQ_ERR_INVAL;
    moq_locmaf_decoder_cfg_t c;
    locmaf_limits_t lim;
    moq_result_t rc = read_cfg(cfg, &c, &lim);
    if (rc != MOQ_OK) return rc;
    moq_locmaf_track_t track;
    rc = locmaf_track_read(c.track, &track);
    if (rc != MOQ_OK) return rc;
    size_t need;
    if (!c.storage || (uintptr_t)c.storage % MOQ_LOCMAF_STORAGE_ALIGN != 0 ||
        !decoder_layout(&lim, NULL, NULL, &need) || c.storage_size < need)
        return MOQ_ERR_INVAL;

    memset(d, 0, sizeof(*d));
    d->struct_size = (uint32_t)sizeof(*d);
    d->_track = track;
    d->_max_samples = lim.samples;
    d->_max_subsamples = lim.subsamples;
    d->_max_gen_boxes = lim.gen_boxes;
    (void)decoder_layout(&lim, c.storage, d, &need);
    d->_stats.struct_size = (uint32_t)MOQ_LOCMAF_DECODER_STATS_V0_SIZE;
    return MOQ_OK;
}

moq_result_t moq_locmaf_decoder_get_stats(const moq_locmaf_decoder_t *d,
                                          moq_locmaf_decoder_stats_t *out,
                                          size_t out_size)
{
    if (!d || !out || out_size < MOQ_LOCMAF_DECODER_STATS_V0_SIZE ||
        d->struct_size != sizeof(*d))
        return MOQ_ERR_INVAL;
    moq_locmaf_decoder_stats_t s = d->_stats;
    s.struct_size = (uint32_t)MOQ_LOCMAF_DECODER_STATS_V0_SIZE;
    memcpy(out, &s, MOQ_LOCMAF_DECODER_STATS_V0_SIZE);
    return MOQ_OK;
}

/* -- Position and reference ------------------------------------------------------- */

static void drop_reference(moq_locmaf_decoder_t *d)
{
    if (d->_has_reference) {
        d->_has_reference = false;
        d->_stats.references_discarded++;
    }
}

void moq_locmaf_decoder_reset(moq_locmaf_decoder_t *d)
{
    if (!d || d->struct_size != sizeof(*d)) return;
    drop_reference(d);
    d->_has_position = false;
    d->_position_at_end = false;
    d->_after_raw_boxes = false;
    d->_group = 0;
    d->_next_object = 0;
}

bool moq_locmaf_decoder_expects(const moq_locmaf_decoder_t *d, uint64_t group_id,
                                uint64_t object_id)
{
    return d && d->struct_size == sizeof(*d) && d->_has_reference &&
           d->_has_position && !d->_position_at_end && d->_group == group_id &&
           d->_next_object == object_id;
}

static bool track_equal(const moq_locmaf_track_t *a, const moq_locmaf_track_t *b)
{
    return a->track_id == b->track_id &&
           a->trex_default_sample_description_index ==
               b->trex_default_sample_description_index &&
           a->trex_default_sample_duration == b->trex_default_sample_duration &&
           a->trex_default_sample_size == b->trex_default_sample_size &&
           a->trex_default_sample_flags == b->trex_default_sample_flags &&
           a->is_protected == b->is_protected &&
           a->default_per_sample_iv_size == b->default_per_sample_iv_size;
}

/* Whether a reference fits limits: every list within its capacity. */
static bool ref_fits(const moq_locmaf_ref_state_t *r, uint32_t samples,
                     uint32_t subsamples)
{
    static const unsigned per_sample[] = { 1, 3, 5, 7, 11 };
    if (r->_scalar[14] > samples) return false;
    for (size_t i = 0; i < sizeof(per_sample) / sizeof(per_sample[0]); i++) {
        unsigned id = per_sample[i];
        if ((r->_present & LM_BIT(id)) && r->_count[id] > samples) return false;
    }
    if ((r->_present & LM_BIT(13)) && r->_count[13] > subsamples) return false;
    if ((r->_present & LM_BIT(15)) && r->_count[15] > subsamples) return false;
    if ((r->_present & LM_BIT(9)) && r->_count[9] > 16u * samples) return false;
    if (r->_present & LM_BIT(11)) {
        uint64_t sum = 0;
        for (uint32_t i = 0; i < r->_count[11]; i++) sum += r->_subsample_counts[i];
        if (sum > subsamples) return false;
    }
    return true;
}

void locmaf_ref_copy(moq_locmaf_ref_state_t *dst, const moq_locmaf_ref_state_t *src)
{
    dst->_present = src->_present;
    memcpy(dst->_count, src->_count, sizeof(dst->_count));
    memcpy(dst->_scalar, src->_scalar, sizeof(dst->_scalar));
#define LM_COPY_LIST(id, field) \
    if ((src->_present & LM_BIT(id)) && src->_count[id]) \
        memcpy(dst->field, src->field, (size_t)src->_count[id] * sizeof(*dst->field))
    LM_COPY_LIST(1, _sizes);
    LM_COPY_LIST(3, _durations);
    LM_COPY_LIST(5, _ctos);
    LM_COPY_LIST(7, _flags);
    LM_COPY_LIST(11, _subsample_counts);
    LM_COPY_LIST(13, _clear);
    LM_COPY_LIST(15, _protected);
    LM_COPY_LIST(9, _ivs);
#undef LM_COPY_LIST
}

moq_result_t moq_locmaf_decoder_copy(moq_locmaf_decoder_t *dst,
                                     const moq_locmaf_decoder_t *src)
{
    if (!dst || !src || dst == src || dst->struct_size != sizeof(*dst) ||
        src->struct_size != sizeof(*src) || !track_equal(&dst->_track, &src->_track))
        return MOQ_ERR_INVAL;
    if (src->_has_reference &&
        !ref_fits(&src->_ref, dst->_max_samples, dst->_max_subsamples))
        return MOQ_ERR_INVAL;
    dst->_has_position = src->_has_position;
    dst->_position_at_end = src->_position_at_end;
    dst->_has_reference = src->_has_reference;
    dst->_after_raw_boxes = src->_after_raw_boxes;
    dst->_group = src->_group;
    dst->_next_object = src->_next_object;
    dst->_bmdt = src->_bmdt;
    dst->_duration_sum = src->_duration_sum;
    if (src->_has_reference) locmaf_ref_copy(&dst->_ref, &src->_ref);
    else locmaf_ref_clear(&dst->_ref);
    return MOQ_OK;
}

/* -- Header application ------------------------------------------------------------ */

static moq_result_t bad(moq_locmaf_reason_t *why, moq_locmaf_reason_t r)
{
    *why = r;
    return r == MOQ_LOCMAF_REASON_LIMIT ? MOQ_ERR_LIMIT : MOQ_ERR_PROTO;
}

/* Section 12.3: deletions, before any delta. */
static moq_result_t apply_deletions(moq_locmaf_ref_state_t *ref,
                                    const locmaf_tuples_t *tp, moq_locmaf_reason_t *why)
{
    moq_bytes_t b = tp->bytes[MOQ_LOCMAF_FIELD_DELETED_IDS];
    size_t pos = 0;
    while (pos < b.len) {
        uint64_t id = 0;
        size_t k = locmaf_vi64(b.data + pos, b.len - pos, &id);
        if (!k) return bad(why, MOQ_LOCMAF_REASON_TRUNCATED);
        pos += k;
        if (id >= LM_IDS || !(LM_KNOWN_IDS & LM_BIT(id)) ||
            id == MOQ_LOCMAF_FIELD_TFDT_BMDT || id == MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT ||
            id == MOQ_LOCMAF_FIELD_DELETED_IDS || !(ref->_present & LM_BIT(id)) ||
            (tp->carried & LM_BIT(id)))
            return bad(why, MOQ_LOCMAF_REASON_DELETION);
        ref->_present &= ~LM_BIT(id);
        if (id & 1u) ref->_count[id] = 0;
        else ref->_scalar[id] = 0;
    }
    return MOQ_OK;
}

/* The even fields in ascending order (section 12.1, Table 5). */
static moq_result_t apply_scalars(moq_locmaf_decoder_t *d, const locmaf_tuples_t *tp,
                                  bool full, moq_locmaf_reason_t *why)
{
    static const unsigned ids[] = { 2, 4, 6, 8, 12, 14, 16 };
    moq_locmaf_ref_state_t *ref = &d->_ref;
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        unsigned id = ids[i];
        if (!(tp->carried & LM_BIT(id))) continue;
        int64_t hi = id == MOQ_LOCMAF_FIELD_SENC_PER_SAMPLE_IV_SIZE ? 16 : (int64_t)UINT32_MAX;
        uint64_t raw = tp->scalar[id];
        int64_t v;
        if (full) {
            if (raw > (uint64_t)hi) return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
            v = (int64_t)raw;
        } else {
            int64_t prev = (ref->_present & LM_BIT(id)) ? (int64_t)ref->_scalar[id] : 0;
            if (!locmaf_add_in_range(prev, locmaf_zz_dec(raw), 0, hi, &v))
                return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
        }
        if (id == MOQ_LOCMAF_FIELD_SENC_PER_SAMPLE_IV_SIZE && v != 0 && v != 8 && v != 16)
            return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
        if (id == MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT && (uint64_t)v > d->_max_samples)
            return bad(why, MOQ_LOCMAF_REASON_LIMIT);
        ref->_scalar[id] = (uint64_t)v;
        ref->_present |= LM_BIT(id);
    }
    return MOQ_OK;
}

/*
 * One list field (sections 10.2, 12.1.1): a carried list must hold exactly
 * `expected` elements, each checked against [lo, hi] before it is stored;
 * an inherited one is truncated to `expected` and refused when shorter.
 */
static moq_result_t apply_list(moq_locmaf_ref_state_t *ref, const locmaf_tuples_t *tp,
                               bool full, unsigned id, uint64_t expected,
                               int64_t lo, int64_t hi, moq_locmaf_reason_t *why)
{
    if (!(tp->carried & LM_BIT(id))) {
        if (!(ref->_present & LM_BIT(id))) return MOQ_OK;
        if (ref->_count[id] < expected) return bad(why, MOQ_LOCMAF_REASON_LIST_LENGTH);
        ref->_count[id] = (uint32_t)expected;
        return MOQ_OK;
    }
    moq_bytes_t b = tp->bytes[id];
    /* Count first: a partial element or a wrong count is refused before
     * anything is stored. The count is bounded by the bytes. */
    uint64_t count = 0;
    for (size_t pos = 0; pos < b.len; count++) {
        uint64_t z = 0;
        size_t k = locmaf_vi64(b.data + pos, b.len - pos, &z);
        if (!k) return bad(why, MOQ_LOCMAF_REASON_TRUNCATED);
        pos += k;
    }
    if (count != expected) return bad(why, MOQ_LOCMAF_REASON_LIST_LENGTH);

    size_t prev_len = (!full && (ref->_present & LM_BIT(id))) ? ref->_count[id] : 0;
    size_t pos = 0;
    for (size_t i = 0; i < expected; i++) {
        uint64_t z = 0;
        pos += locmaf_vi64(b.data + pos, b.len - pos, &z);
        int64_t v;
        if (full && id != MOQ_LOCMAF_FIELD_TRUN_CTO) {
            if (z > (uint64_t)hi) return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
            v = (int64_t)z;
        } else if (full || i >= prev_len) {
            /* A zigzag absolute value: ID 5 in a full header, or an element
             * past the previous list's length in a delta (12.1.1). */
            v = locmaf_zz_dec(z);
            if (v < lo || v > hi) return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
        } else if (!locmaf_add_in_range(locmaf_ref_elem(ref, id, i), locmaf_zz_dec(z),
                                        lo, hi, &v)) {
            return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
        }
        ref_store(ref, id, i, v);
    }
    ref->_count[id] = (uint32_t)expected;
    ref->_present |= LM_BIT(id);
    return MOQ_OK;
}

static moq_result_t apply_header(moq_locmaf_decoder_t *d, const locmaf_tuples_t *tp,
                                 bool full, moq_locmaf_reason_t *why)
{
    moq_locmaf_ref_state_t *ref = &d->_ref;
    moq_result_t rc;
    if (full) {
        locmaf_ref_clear(ref);
    } else if (tp->carried & LM_BIT(MOQ_LOCMAF_FIELD_DELETED_IDS)) {
        rc = apply_deletions(ref, tp, why);
        if (rc != MOQ_OK) return rc;
    }
    rc = apply_scalars(d, tp, full, why);
    if (rc != MOQ_OK) return rc;
    if (full && (!(ref->_present & LM_BIT(MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT)) ||
                 !(tp->carried & LM_BIT(MOQ_LOCMAF_FIELD_TFDT_BMDT))))
        return bad(why, MOQ_LOCMAF_REASON_MISSING_FIELD);
    uint64_t n = ref->_scalar[MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT];
    if (n > d->_max_samples) return bad(why, MOQ_LOCMAF_REASON_LIMIT);

    const int64_t u32 = (int64_t)UINT32_MAX;
    rc = apply_list(ref, tp, full, 1, n ? n - 1 : 0, 0, u32, why);
    if (rc == MOQ_OK) rc = apply_list(ref, tp, full, 3, n, 0, u32, why);
    if (rc == MOQ_OK) rc = apply_list(ref, tp, full, 5, n, INT32_MIN, u32, why);
    if (rc == MOQ_OK) rc = apply_list(ref, tp, full, 7, n, 0, u32, why);
    if (rc == MOQ_OK) rc = apply_list(ref, tp, full, 11, n, 0, 65535, why);
    if (rc != MOQ_OK) return rc;

    /* trun holds int32 offsets in version 1 and uint32 in version 0: a list
     * with a negative offset must fit int32 throughout. */
    if (ref->_present & LM_BIT(5)) {
        bool negative = false, wide = false;
        for (uint32_t i = 0; i < ref->_count[5]; i++) {
            negative |= ref->_ctos[i] < 0;
            wide |= ref->_ctos[i] > INT32_MAX;
        }
        if (negative && wide) return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
    }

    /* Subsample maps: counts first (bounded before the entry lists are
     * read), then the entries. */
    bool counts = (ref->_present & LM_BIT(11)) != 0;
    uint64_t total = 0;
    if (counts) {
        for (uint32_t i = 0; i < ref->_count[11]; i++) {
            total += ref->_subsample_counts[i];
            if (total > d->_max_subsamples) return bad(why, MOQ_LOCMAF_REASON_LIMIT);
        }
    }
    uint32_t entry_lists = (ref->_present | tp->carried) & (LM_BIT(13) | LM_BIT(15));
    if (entry_lists && !counts) return bad(why, MOQ_LOCMAF_REASON_CENC_LAYOUT);
    if (counts) {
        rc = apply_list(ref, tp, full, 13, total, 0, 65535, why);
        if (rc == MOQ_OK) rc = apply_list(ref, tp, full, 15, total, 0, u32, why);
        if (rc != MOQ_OK) return rc;
        if (total > 0 && (ref->_present & (LM_BIT(13) | LM_BIT(15))) !=
                             (LM_BIT(13) | LM_BIT(15)))
            return bad(why, MOQ_LOCMAF_REASON_CENC_LAYOUT);
    }

    /* Section 13: per-sample IVs are raw bytes, overwritten whole. */
    if (tp->carried & LM_BIT(MOQ_LOCMAF_FIELD_SENC_IVS)) {
        moq_bytes_t ivs = tp->bytes[MOQ_LOCMAF_FIELD_SENC_IVS];
        if (ivs.len > 16u * (size_t)d->_max_samples)
            return bad(why, MOQ_LOCMAF_REASON_LIMIT);
        if (ivs.len) memcpy(ref->_ivs, ivs.data, ivs.len);
        ref->_count[MOQ_LOCMAF_FIELD_SENC_IVS] = (uint32_t)ivs.len;
        ref->_present |= LM_BIT(MOQ_LOCMAF_FIELD_SENC_IVS);
    }
    return MOQ_OK;
}

/* -- Derivations ------------------------------------------------------------------- */

/* Section 11.2: the sample-size ladder, n >= 1. */
static moq_result_t derive_sizes(moq_locmaf_decoder_t *d, uint32_t n, uint64_t p,
                                 moq_locmaf_reason_t *why)
{
    const moq_locmaf_ref_state_t *ref = &d->_ref;
    moq_locmaf_sample_t *s = d->_samples;
    if (ref->_present & LM_BIT(1)) {
        uint64_t sum = 0;
        for (uint32_t i = 0; i + 1 < n; i++) {
            if (!locmaf_u64_add(sum, ref->_sizes[i], &sum))
                return bad(why, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
            s[i].size = ref->_sizes[i];
        }
        if (sum > p) return bad(why, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
        s[n - 1].size = (uint32_t)(p - sum);       /* p <= 0xFFFFFFF7 */
        return MOQ_OK;
    }
    uint64_t each;
    if (ref->_present & LM_BIT(6)) {
        each = ref->_scalar[6];
    } else if (n == 1) {
        s[0].size = (uint32_t)p;
        return MOQ_OK;
    } else if (d->_track.trex_default_sample_size != 0) {
        each = d->_track.trex_default_sample_size;
    } else if (p == 0) {
        each = 0;
    } else {
        return bad(why, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    }
    if (each == 0 ? p != 0 : (n > UINT64_MAX / each || (uint64_t)n * each != p))
        return bad(why, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    for (uint32_t i = 0; i < n; i++) s[i].size = (uint32_t)each;
    return MOQ_OK;
}

typedef struct derived {
    uint64_t duration_sum;
    uint8_t  iv;
    bool     has_aux_info;
    bool     has_subsamples;
    size_t   subsample_total;
} derived_t;

/* Sections 15.1 and 15.8: effective values, the CENC derivation and its
 * checks (section 18). */
static moq_result_t derive(moq_locmaf_decoder_t *d, uint64_t p, derived_t *out,
                           moq_locmaf_reason_t *why)
{
    const moq_locmaf_ref_state_t *ref = &d->_ref;
    const moq_locmaf_track_t *t = &d->_track;
    uint32_t n = (uint32_t)ref->_scalar[MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT];
    if (!locmaf_check_mdat_len(p, why)) return MOQ_ERR_PROTO;
    if (n == 0 && p > 0) return bad(why, MOQ_LOCMAF_REASON_SAMPLE_SIZES);
    if (n > 0) {
        moq_result_t rc = derive_sizes(d, n, p, why);
        if (rc != MOQ_OK) return rc;
    }

    uint32_t pres = ref->_present;
    uint8_t iv = t->is_protected
        ? (uint8_t)((pres & LM_BIT(16)) ? ref->_scalar[16] : t->default_per_sample_iv_size)
        : 0;
    bool has_sub = (pres & LM_BIT(11)) != 0;
    bool has_ivs = (pres & LM_BIT(9)) != 0;
    bool has_aux = has_sub || (has_ivs && iv > 0);
    if (has_ivs && ref->_count[9] != (uint64_t)n * iv)
        return bad(why, MOQ_LOCMAF_REASON_IV_LENGTH);
    /* Sections 13.1, 15.8: with a per-sample IV size above zero every
     * sample's IV travels in field 9; without it there is no senc to
     * rebuild. A size of 0 (a constant IV in the CMAF Header) needs none. */
    if (iv > 0 && n > 0 && !has_ivs) return bad(why, MOQ_LOCMAF_REASON_IV_LENGTH);
    if (has_aux && n == 0) return bad(why, MOQ_LOCMAF_REASON_CENC_LAYOUT);

    uint64_t dur_sum = 0, offset = 0;
    size_t sub = 0;
    for (uint32_t i = 0; i < n; i++) {
        moq_locmaf_sample_t *s = &d->_samples[i];
        s->duration = (pres & LM_BIT(3)) ? ref->_durations[i]
                    : (pres & LM_BIT(4)) ? (uint32_t)ref->_scalar[4]
                    : t->trex_default_sample_duration;
        s->flags = (pres & LM_BIT(7)) ? ref->_flags[i]
                 : ((pres & LM_BIT(12)) && i == 0) ? (uint32_t)ref->_scalar[12]
                 : (pres & LM_BIT(8)) ? (uint32_t)ref->_scalar[8]
                 : t->trex_default_sample_flags;
        s->composition_offset = (pres & LM_BIT(5)) ? ref->_ctos[i] : 0;
        s->offset = offset;
        offset += s->size;                          /* the sizes sum to p */
        s->subsample_count = has_sub ? ref->_subsample_counts[i] : 0;
        s->subsample_index = (uint32_t)sub;
        s->_reserved = 0;
        if (!locmaf_u64_add(dur_sum, s->duration, &dur_sum))
            return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
        if (s->subsample_count > 0) {
            uint64_t bytes = 0;
            for (uint32_t k = 0; k < s->subsample_count; k++) {
                uint64_t pair = (uint64_t)ref->_clear[sub + k] + ref->_protected[sub + k];
                bytes += pair;                      /* at most 2^20 pairs of < 2^33 */
            }
            if (bytes != s->size) return bad(why, MOQ_LOCMAF_REASON_SUBSAMPLE_SUM);
        }
        if (has_aux && iv + (has_sub ? 2u + 6u * (uint64_t)s->subsample_count : 0u) > 255u)
            return bad(why, MOQ_LOCMAF_REASON_AUX_INFO_SIZE);
        sub += s->subsample_count;
    }
    for (size_t j = 0; j < sub; j++) {
        d->_subsamples[j].clear_bytes = ref->_clear[j];
        d->_subsamples[j].protected_bytes = ref->_protected[j];
    }
    out->duration_sum = dur_sum;
    out->iv = iv;
    out->has_aux_info = has_aux;
    out->has_subsamples = has_sub;
    out->subsample_total = sub;
    return MOQ_OK;
}

/* -- Push ---------------------------------------------------------------------------- */

static moq_result_t decode_object(moq_locmaf_decoder_t *d, moq_bytes_t payload,
                                  moq_locmaf_chunk_t *c, moq_locmaf_reason_t *why)
{
    locmaf_split_t sp;
    moq_result_t rc = locmaf_split(&d->_track, payload, &sp, d->_gen_boxes,
                                   d->_max_gen_boxes, why);
    if (rc != MOQ_OK) return rc;
    if (sp.gen_box_count > d->_max_gen_boxes) return bad(why, MOQ_LOCMAF_REASON_LIMIT);

    memset(c, 0, sizeof(*c));
    c->struct_size = (uint32_t)MOQ_LOCMAF_CHUNK_V0_SIZE;
    c->kind = sp.kind;
    if (sp.kind == MOQ_LOCMAF_OBJECT_RAW_BOXES) {
        /* Section 9.3: delivered verbatim; no reference survives it. */
        drop_reference(d);
        d->_after_raw_boxes = true;
        c->raw_boxes = sp.raw_boxes;
        return MOQ_OK;
    }

    bool full = sp.kind == MOQ_LOCMAF_OBJECT_FULL;
    locmaf_tuples_t tp;
    rc = locmaf_tuples(sp.properties, full, &d->_track, &tp, why);
    if (rc != MOQ_OK) return rc;
    if (!full && !d->_has_reference) {
        if (d->_after_raw_boxes) return bad(why, MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES);
        *why = MOQ_LOCMAF_REASON_NO_REFERENCE;
        return MOQ_ERR_WRONG_STATE;
    }

    rc = apply_header(d, &tp, full, why);
    if (rc != MOQ_OK) return rc;

    /* Section 12.2: a delta's BMDT continues the previous chunk's. */
    uint64_t bmdt;
    if (full) bmdt = tp.scalar[MOQ_LOCMAF_FIELD_TFDT_BMDT];
    else if (!locmaf_u64_add(d->_bmdt, d->_duration_sum, &bmdt))
        return bad(why, MOQ_LOCMAF_REASON_VALUE_RANGE);
    d->_ref._scalar[MOQ_LOCMAF_FIELD_TFDT_BMDT] = bmdt;

    derived_t dv;
    rc = derive(d, sp.mdat_payload.len, &dv, why);
    if (rc != MOQ_OK) return rc;

    /* Every chunk's represented fields become the reference: the decoder
     * applied this header to its one reference in place. */
    (void)locmaf_ref_next(&d->_ref, &d->_ref, sp.kind);
    d->_has_reference = true;
    d->_after_raw_boxes = false;
    d->_bmdt = bmdt;
    d->_duration_sum = dv.duration_sum;
    d->_stats.unknown_fields_skipped += tp.unknown;

    const moq_locmaf_ref_state_t *ref = &d->_ref;
    uint32_t n = (uint32_t)ref->_scalar[MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT];
    c->base_media_decode_time = bmdt;
    c->sample_description_index = (ref->_present & LM_BIT(2))
        ? (uint32_t)ref->_scalar[2] : d->_track.trex_default_sample_description_index;
    c->sample_count = n;
    c->samples = n ? d->_samples : NULL;
    c->per_sample_iv_size = dv.iv;
    c->has_aux_info = dv.has_aux_info;
    c->has_subsamples = dv.has_subsamples;
    size_t iv_bytes = (size_t)n * dv.iv;
    if (dv.has_aux_info && iv_bytes)
        c->ivs = (moq_bytes_t){ ref->_ivs, iv_bytes };
    c->subsamples = dv.subsample_total ? d->_subsamples : NULL;
    c->subsample_total = dv.subsample_total;
    c->gen_boxes = sp.gen_box_count ? d->_gen_boxes : NULL;
    c->gen_box_count = sp.gen_box_count;
    c->mdat_payload = sp.mdat_payload;
    return MOQ_OK;
}

moq_result_t moq_locmaf_decoder_push(moq_locmaf_decoder_t *d, uint64_t group_id,
                                     uint64_t object_id, moq_bytes_t payload,
                                     moq_locmaf_chunk_t *out, size_t out_size,
                                     moq_locmaf_reason_t *reason)
{
    lm_why(reason, MOQ_LOCMAF_REASON_NONE);
    if (!d || !out || out_size < MOQ_LOCMAF_CHUNK_V0_SIZE ||
        (!payload.data && payload.len) || d->struct_size != sizeof(*d))
        return MOQ_ERR_INVAL;

    /* Section 3: Objects of a group in ascending Object ID order; a gap or
     * a new group leaves no reference. */
    bool same_group = d->_has_position && d->_group == group_id;
    if (same_group && (d->_position_at_end || object_id < d->_next_object))
        return MOQ_ERR_INVAL;
    if (!same_group || object_id > d->_next_object) {
        drop_reference(d);
        d->_after_raw_boxes = false;
    }
    d->_has_position = true;
    d->_group = group_id;
    d->_position_at_end = object_id == UINT64_MAX;
    d->_next_object = d->_position_at_end ? 0 : object_id + 1;

    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_locmaf_chunk_t chunk;
    moq_result_t rc = decode_object(d, payload, &chunk, &why);
    moq_locmaf_decoder_stats_t *st = &d->_stats;
    if (rc != MOQ_OK) {
        drop_reference(d);
        st->last_reason = why;
        switch (rc) {
        case MOQ_ERR_WRONG_STATE: st->objects_unreferenced++; break;
        case MOQ_ERR_LIMIT:       st->objects_limited++; break;
        case MOQ_ERR_UNSUPPORTED: st->objects_unsupported++; break;
        default:                  st->objects_malformed++; break;
        }
        lm_why(reason, why);
        return rc;
    }
    st->objects_decoded++;
    if (chunk.kind == MOQ_LOCMAF_OBJECT_FULL) st->full_headers_decoded++;
    else if (chunk.kind == MOQ_LOCMAF_OBJECT_DELTA) st->delta_headers_decoded++;
    else st->raw_boxes_decoded++;
    memcpy(out, &chunk, MOQ_LOCMAF_CHUNK_V0_SIZE);
    return MOQ_OK;
}
