/*
 * The LOCMAF encoder: a CMAF chunk read losslessly (locmaf_chunk.c), its
 * canonical field set (sections 11.1, 15.9, decided in locmaf_canon.c), a
 * full header or the differences against the in-group reference (12), and
 * a two-phase commit, so a refused send can be retried with the same bytes.
 */
#include "locmaf_internal.h"

/* -- Storage ------------------------------------------------------------------ */

static bool encoder_layout(const locmaf_limits_t *lim, uint8_t *base,
                           moq_locmaf_encoder_t *e, size_t *total)
{
    locmaf_carve_t c = { base, 0, true };
    moq_locmaf_ref_state_t scratch[2];
    moq_locmaf_ref_state_t *refs = e ? e->_ref : scratch;
    locmaf_carve_ref(&c, lim, &refs[0]);
    locmaf_carve_ref(&c, lim, &refs[1]);
    moq_locmaf_sample_t *samples =
        locmaf_carve_take(&c, lim->samples, sizeof(moq_locmaf_sample_t));
    moq_locmaf_subsample_t *subs =
        locmaf_carve_take(&c, lim->subsamples, sizeof(moq_locmaf_subsample_t));
    moq_locmaf_gen_box_t *boxes =
        locmaf_carve_take(&c, lim->gen_boxes, sizeof(moq_locmaf_gen_box_t));
    uint8_t *ivs = locmaf_carve_take(&c, lim->samples, 16);
    if (!c.ok) return false;
    if (e) {
        e->_src_samples = samples;
        e->_src_subsamples = subs;
        e->_src_gen_boxes = boxes;
        e->_src_ivs = ivs;
    }
    *total = c.off;
    return true;
}

void moq_locmaf_encoder_cfg_init_sized(moq_locmaf_encoder_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < MOQ_LOCMAF_ENCODER_CFG_V0_SIZE) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

static moq_result_t read_cfg(const moq_locmaf_encoder_cfg_t *cfg,
                             moq_locmaf_encoder_cfg_t *out, locmaf_limits_t *lim)
{
    if (!cfg) return MOQ_ERR_INVAL;
    uint32_t size;
    memcpy(&size, &cfg->struct_size, sizeof(size));
    if (size < MOQ_LOCMAF_ENCODER_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    memcpy(out, cfg, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE);
    return locmaf_limits_resolve(out->max_samples, out->max_subsamples,
                                 out->max_gen_boxes, lim);
}

moq_result_t moq_locmaf_encoder_storage_size(const moq_locmaf_encoder_cfg_t *cfg,
                                             size_t *out_bytes)
{
    moq_locmaf_encoder_cfg_t c;
    locmaf_limits_t lim;
    if (!out_bytes) return MOQ_ERR_INVAL;
    moq_result_t rc = read_cfg(cfg, &c, &lim);
    if (rc != MOQ_OK) return rc;
    size_t total;
    if (!encoder_layout(&lim, NULL, NULL, &total)) return MOQ_ERR_INVAL;
    *out_bytes = total;
    return MOQ_OK;
}

static void limits_of(const moq_locmaf_encoder_t *e, locmaf_limits_t *lim)
{
    lim->samples = e->_max_samples;
    lim->subsamples = e->_max_subsamples;
    lim->gen_boxes = e->_max_gen_boxes;
}

moq_result_t moq_locmaf_encoder_init(moq_locmaf_encoder_t *e, size_t e_size,
                                     const moq_locmaf_encoder_cfg_t *cfg)
{
    if (!e || e_size < sizeof(*e)) return MOQ_ERR_INVAL;
    moq_locmaf_encoder_cfg_t c;
    locmaf_limits_t lim;
    moq_result_t rc = read_cfg(cfg, &c, &lim);
    if (rc != MOQ_OK) return rc;
    moq_locmaf_track_t track;
    rc = locmaf_track_read(c.track, &track);
    if (rc != MOQ_OK) return rc;
    size_t need;
    if (!c.storage || (uintptr_t)c.storage % MOQ_LOCMAF_STORAGE_ALIGN != 0 ||
        !encoder_layout(&lim, NULL, NULL, &need) || c.storage_size < need)
        return MOQ_ERR_INVAL;
    memset(e, 0, sizeof(*e));
    e->struct_size = (uint32_t)sizeof(*e);
    e->_track = track;
    e->_max_samples = lim.samples;
    e->_max_subsamples = lim.subsamples;
    e->_max_gen_boxes = lim.gen_boxes;
    (void)encoder_layout(&lim, c.storage, e, &need);
    e->_stats.struct_size = (uint32_t)MOQ_LOCMAF_ENCODER_STATS_V0_SIZE;
    return MOQ_OK;
}

moq_result_t moq_locmaf_encoder_get_stats(const moq_locmaf_encoder_t *e,
                                          moq_locmaf_encoder_stats_t *out,
                                          size_t out_size)
{
    if (!e || !out || out_size < MOQ_LOCMAF_ENCODER_STATS_V0_SIZE ||
        e->struct_size != sizeof(*e))
        return MOQ_ERR_INVAL;
    moq_locmaf_encoder_stats_t s = e->_stats;
    s.struct_size = (uint32_t)MOQ_LOCMAF_ENCODER_STATS_V0_SIZE;
    memcpy(out, &s, MOQ_LOCMAF_ENCODER_STATS_V0_SIZE);
    return MOQ_OK;
}

void moq_locmaf_encoder_reset(moq_locmaf_encoder_t *e)
{
    if (!e || e->struct_size != sizeof(*e)) return;
    memset(&e->_committed, 0, sizeof(e->_committed));
    memset(&e->_staged, 0, sizeof(e->_staged));
    e->_has_stage = false;
    e->_stage_raw_boxes = false;
    locmaf_ref_clear(&e->_ref[0]);
    locmaf_ref_clear(&e->_ref[1]);
}

/* -- The represented set of a source chunk ----------------------------------------- */

/* The fields a chunk is represented by (sections 11.1, 12.1), viewed over
 * the reader's tables until it is staged. ID 10 is the chunk's BMDT and is
 * never part of the reference's presence. */
typedef struct rep {
    uint32_t                      present;
    uint64_t                      scalar[LM_IDS];
    uint32_t                      count[LM_IDS];
    const moq_locmaf_sample_t    *samples;
    const moq_locmaf_subsample_t *subs;
    const uint8_t                *ivs;
} rep_t;

static int64_t rep_elem(const rep_t *r, unsigned id, size_t i)
{
    switch (id) {
    case 1:  return r->samples[i].size;
    case 3:  return r->samples[i].duration;
    case 5:  return r->samples[i].composition_offset;
    case 7:  return r->samples[i].flags;
    case 11: return r->samples[i].subsample_count;
    case 13: return r->subs[i].clear_bytes;
    default: return r->subs[i].protected_bytes;
    }
}

static void represent(const moq_locmaf_track_t *t, const locmaf_src_t *src,
                      const locmaf_scratch_t *scr, rep_t *r)
{
    memset(r, 0, sizeof(*r));
    r->samples = scr->samples;
    r->subs = scr->subsamples;
    r->ivs = scr->ivs;
    uint32_t n = src->n;
    r->present = LM_BIT(14);
    r->scalar[14] = n;
    r->scalar[10] = src->bmdt;
    if (n == 0) return;

    locmaf_canon_t k;
    locmaf_decide(t, src->sdi, scr->samples, n, src->mdat_payload.len, &k);
#define SCALAR(id, v) do { r->present |= LM_BIT(id); r->scalar[id] = (v); } while (0)
#define LIST(id, c)   do { r->present |= LM_BIT(id); r->count[id] = (uint32_t)(c); } while (0)
    if (k.sdi) SCALAR(2, src->sdi);
    if (k.dur_list) LIST(3, n);
    if (k.dur_default) SCALAR(4, k.dur);
    if (k.size_list) LIST(1, n - 1);
    if (k.size_wire) SCALAR(6, k.size);
    if (k.first_flags) SCALAR(12, k.first);
    if (k.flags_list) LIST(7, n);
    if (k.flags_default) SCALAR(8, k.flags);
    if (k.cto) LIST(5, n);
    /* CENC (section 11.1): the IV size always comes from tenc, so field 16
     * is never written. */
    if (src->has_aux_info) {
        if (src->iv) LIST(9, (size_t)n * src->iv);
        if (src->has_subsamples) {
            LIST(11, n);
            LIST(13, src->subsample_total);
            LIST(15, src->subsample_total);
        }
    }
#undef SCALAR
#undef LIST
}

/* Copy a represented set into a reference slot's own regions. */
static void stage_rep(moq_locmaf_ref_state_t *slot, const rep_t *r)
{
    locmaf_ref_clear(slot);
    slot->_present = r->present;
    for (unsigned id = 0; id <= 16; id++) {
        if (!(r->present & LM_BIT(id))) continue;
        if (!(id & 1u)) { slot->_scalar[id] = r->scalar[id]; continue; }
        slot->_count[id] = r->count[id];
        for (size_t i = 0; i < r->count[id] && id != 9; i++) {
            int64_t v = rep_elem(r, id, i);
            switch (id) {
            case 1:  slot->_sizes[i] = (uint32_t)v; break;
            case 3:  slot->_durations[i] = (uint32_t)v; break;
            case 5:  slot->_ctos[i] = v; break;
            case 7:  slot->_flags[i] = (uint32_t)v; break;
            case 11: slot->_subsample_counts[i] = (uint32_t)v; break;
            case 13: slot->_clear[i] = (uint32_t)v; break;
            default: slot->_protected[i] = (uint32_t)v; break;
            }
        }
        if (id == 9 && r->count[9]) memcpy(slot->_ivs, r->ivs, r->count[9]);
    }
    slot->_scalar[MOQ_LOCMAF_FIELD_TFDT_BMDT] = r->scalar[MOQ_LOCMAF_FIELD_TFDT_BMDT];
}

/* -- Emission -------------------------------------------------------------------- */

/* A writer that only measures when out is NULL. */
typedef struct sink {
    uint8_t *out;
    size_t   n;
} sink_t;

static void s_vi(sink_t *s, uint64_t v)
{
    if (s->out) s->n += moq_vi64_encode(v, s->out + s->n, MOQ_VI64_MAX_LEN);
    else s->n += moq_vi64_len(v);
}

static void s_raw(sink_t *s, const uint8_t *p, size_t len)
{
    if (s->out && len) memcpy(s->out + s->n, p, len);
    s->n += len;
}

static size_t vi_len(uint64_t v) { return moq_vi64_len(v); }

/* One list field: absolute values in a full header (zigzag for ID 5),
 * element differences against prev in a delta, absolute zigzag past prev's
 * length (12.1.1). */
static void emit_list(sink_t *s, unsigned id, const rep_t *cur,
                      const moq_locmaf_ref_state_t *prev, bool delta)
{
    uint32_t n = cur->count[id];
    size_t prev_len = (delta && (prev->_present & LM_BIT(id))) ? prev->_count[id] : 0;
    uint64_t len = 0;
    for (int pass = 0; pass < 2; pass++) {
        if (pass == 1) { s_vi(s, id); s_vi(s, len); }
        for (size_t i = 0; i < n; i++) {
            int64_t v = rep_elem(cur, id, i);
            uint64_t z;
            if (!delta) z = id == MOQ_LOCMAF_FIELD_TRUN_CTO ? locmaf_zz_enc(v) : (uint64_t)v;
            else if (i < prev_len) z = locmaf_zz_enc(v - locmaf_ref_elem(prev, id, i));
            else z = locmaf_zz_enc(v);
            /* Every difference lies within [lo - hi, hi - lo] of its field,
             * well inside int64_t. */
            if (pass == 0) len += vi_len(z);
            else s_vi(s, z);
        }
    }
}

static bool list_changed(unsigned id, const rep_t *cur, const moq_locmaf_ref_state_t *prev)
{
    if (prev->_count[id] != cur->count[id]) return true;
    if (id == MOQ_LOCMAF_FIELD_SENC_IVS)
        return cur->count[9] && memcmp(prev->_ivs, cur->ivs, cur->count[9]) != 0;
    for (size_t i = 0; i < cur->count[id]; i++)
        if (locmaf_ref_elem(prev, id, i) != rep_elem(cur, id, i)) return true;
    return false;
}

/* The property block, ascending IDs (section 15.9). */
static void emit_props(sink_t *s, const rep_t *cur, const moq_locmaf_ref_state_t *prev,
                       bool delta)
{
    for (unsigned id = 1; id <= 16; id++) {
        if (id == MOQ_LOCMAF_FIELD_TFDT_BMDT) {
            if (!delta) { s_vi(s, id); s_vi(s, cur->scalar[id]); }
            continue;
        }
        if (!(cur->present & LM_BIT(id))) continue;
        bool had = delta && (prev->_present & LM_BIT(id));
        if (!(id & 1u)) {
            int64_t v = (int64_t)cur->scalar[id];
            int64_t p = had ? (int64_t)prev->_scalar[id] : 0;
            if (had && v == p) continue;
            s_vi(s, id);
            s_vi(s, delta ? locmaf_zz_enc(v - p) : (uint64_t)v);
        } else if (id == MOQ_LOCMAF_FIELD_SENC_IVS) {
            if (had && !list_changed(id, cur, prev)) continue;
            s_vi(s, id);
            s_vi(s, cur->count[9]);
            s_raw(s, cur->ivs, cur->count[9]);
        } else {
            if (had && !list_changed(id, cur, prev)) continue;
            emit_list(s, id, cur, prev, delta);
        }
    }
    if (!delta) return;
    /* Section 12.3: the fields that leave the reference, in one list. */
    uint32_t gone = prev->_present & ~cur->present & LM_KNOWN_IDS;
    if (!gone) return;
    size_t len = 0;
    for (unsigned id = 1; id <= 16; id++)
        if (gone & LM_BIT(id)) len += vi_len(id);
    s_vi(s, MOQ_LOCMAF_FIELD_DELETED_IDS);
    s_vi(s, len);
    for (unsigned id = 1; id <= 16; id++)
        if (gone & LM_BIT(id)) s_vi(s, id);
}

static void emit_object(sink_t *s, const locmaf_src_t *src, const locmaf_scratch_t *scr,
                        const rep_t *cur, const moq_locmaf_ref_state_t *prev, bool delta)
{
    for (size_t k = 0; k < src->gen_box_count; k++) {
        const moq_locmaf_gen_box_t *g = &scr->gen_boxes[k];
        uint8_t type[4];
        lm_wr32(type, g->type);
        s_vi(s, MOQ_LOCMAF_ELEMENT_GEN_BOX);
        s_vi(s, 4 + (uint64_t)g->payload.len);
        s_raw(s, type, 4);
        s_raw(s, g->payload.data, g->payload.len);
    }
    sink_t measure = { NULL, 0 };
    emit_props(&measure, cur, prev, delta);
    s_vi(s, delta ? MOQ_LOCMAF_ELEMENT_DELTA_HEADER : MOQ_LOCMAF_ELEMENT_FULL_HEADER);
    s_vi(s, measure.n);
    emit_props(s, cur, prev, delta);
    s_raw(s, src->mdat_payload.data, src->mdat_payload.len);
}

/* -- Entry points ----------------------------------------------------------------- */

static void refused(moq_locmaf_encoder_t *e, moq_locmaf_reason_t why)
{
    e->_stats.chunks_refused++;
    e->_stats.last_reason = why;
}

static moq_result_t read_source(moq_locmaf_encoder_t *e, moq_bytes_t chunk,
                                locmaf_src_t *src, locmaf_scratch_t *scr,
                                moq_locmaf_reason_t *why)
{
    locmaf_limits_t lim;
    limits_of(e, &lim);
    scr->samples = e->_src_samples;
    scr->subsamples = e->_src_subsamples;
    scr->gen_boxes = e->_src_gen_boxes;
    scr->ivs = e->_src_ivs;
    moq_result_t rc = locmaf_read_chunk(&e->_track, chunk, &lim, src, scr, why);
    if (rc != MOQ_OK) refused(e, *why);
    return rc;
}

moq_result_t moq_locmaf_encoder_check(moq_locmaf_encoder_t *e, moq_bytes_t cmaf_chunk,
                                      size_t *out_bound, moq_locmaf_reason_t *reason)
{
    lm_why(reason, MOQ_LOCMAF_REASON_NONE);
    if (!e || e->struct_size != sizeof(*e) || (!cmaf_chunk.data && cmaf_chunk.len))
        return MOQ_ERR_INVAL;
    locmaf_src_t src;
    locmaf_scratch_t scr;
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_result_t rc = read_source(e, cmaf_chunk, &src, &scr, &why);
    if (rc != MOQ_OK) { lm_why(reason, why); return rc; }
    size_t bound;
    if (!locmaf_encode_bound(&src, scr.gen_boxes, &bound)) return MOQ_ERR_INVAL;
    if (out_bound) *out_bound = bound;
    return MOQ_OK;
}

/* INVAL for an Object ID at or below the committed position in its group. */
static bool position_ok(const moq_locmaf_enc_position_t *c, uint64_t g, uint64_t o)
{
    if (!c->_has_position || c->_group != g) return true;
    return !c->_position_at_end && o >= c->_next_object;
}

static void stage_position(moq_locmaf_enc_position_t *p, uint64_t g, uint64_t o)
{
    p->_has_position = true;
    p->_group = g;
    p->_position_at_end = o == UINT64_MAX;
    p->_next_object = p->_position_at_end ? 0 : o + 1;
}

moq_result_t moq_locmaf_encode_chunk(moq_locmaf_encoder_t *e, uint64_t group_id,
                                     uint64_t object_id, moq_bytes_t cmaf_chunk,
                                     uint8_t *out, size_t out_cap, size_t *out_len,
                                     moq_locmaf_reason_t *reason)
{
    lm_why(reason, MOQ_LOCMAF_REASON_NONE);
    if (!e || e->struct_size != sizeof(*e) || !out_len || (!out && out_cap) ||
        (!cmaf_chunk.data && cmaf_chunk.len) ||
        !position_ok(&e->_committed, group_id, object_id))
        return MOQ_ERR_INVAL;
    locmaf_src_t src;
    locmaf_scratch_t scr;
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_result_t rc = read_source(e, cmaf_chunk, &src, &scr, &why);
    if (rc != MOQ_OK) { lm_why(reason, why); return rc; }

    rep_t cur;
    represent(&e->_track, &src, &scr, &cur);
    const moq_locmaf_enc_position_t *c = &e->_committed;
    const moq_locmaf_ref_state_t *prev = &e->_ref[e->_committed_slot];
    /* A full header at a new group, a gap, with no reference, after a
     * rawBoxes Object, and where the BMDT does not continue (12.2). */
    uint64_t next_bmdt = 0;
    bool delta = c->_has_position && c->_group == group_id &&
                 object_id == c->_next_object && c->_has_reference &&
                 !c->_after_raw_boxes &&
                 locmaf_u64_add(c->_bmdt, c->_duration_sum, &next_bmdt) &&
                 next_bmdt == src.bmdt;

    sink_t size = { NULL, 0 };
    emit_object(&size, &src, &scr, &cur, prev, delta);
    *out_len = size.n;
    if (out_cap < size.n) return MOQ_ERR_BUFFER;
    sink_t w = { out, 0 };
    emit_object(&w, &src, &scr, &cur, prev, delta);

    uint64_t dur_sum = 0;
    for (uint32_t i = 0; i < src.n; i++) dur_sum += scr.samples[i].duration;  /* < 2^48 */
    if (e->_has_stage) e->_stats.stages_replaced++;
    moq_locmaf_ref_state_t *slot = &e->_ref[1 - e->_committed_slot];
    stage_rep(slot, &cur);
    /* What the next delta applies against (section 12.1): this chunk's own
     * fields, which commit adopts by swapping the slots. */
    if (locmaf_ref_next(prev, slot, delta ? MOQ_LOCMAF_OBJECT_DELTA
                                          : MOQ_LOCMAF_OBJECT_FULL) != slot)
        locmaf_ref_copy(slot, prev);
    moq_locmaf_enc_position_t *st = &e->_staged;
    stage_position(st, group_id, object_id);
    st->_has_reference = true;
    st->_after_raw_boxes = false;
    st->_bmdt = src.bmdt;
    st->_duration_sum = dur_sum;
    e->_has_stage = true;
    e->_stage_raw_boxes = false;
    e->_stats.chunks_encoded++;
    if (delta) e->_stats.delta_headers_written++;
    else e->_stats.full_headers_written++;
    return MOQ_OK;
}

moq_result_t moq_locmaf_encode_raw_boxes(moq_locmaf_encoder_t *e, uint64_t group_id,
                                         uint64_t object_id, moq_bytes_t boxes,
                                         uint8_t *out, size_t out_cap, size_t *out_len,
                                         moq_locmaf_reason_t *reason)
{
    lm_why(reason, MOQ_LOCMAF_REASON_NONE);
    if (!e || e->struct_size != sizeof(*e) || !out_len || (!out && out_cap) ||
        (!boxes.data && boxes.len) || !position_ok(&e->_committed, group_id, object_id))
        return MOQ_ERR_INVAL;
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    moq_result_t rc = MOQ_OK;
    if (!locmaf_raw_boxes_ok(boxes)) {
        why = MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED;
        rc = MOQ_ERR_PROTO;
    } else {
        /* The decoder's rule, so the encoder never writes rawBoxes a decoder
         * from this library refuses. */
        rc = locmaf_raw_chunk_check(&e->_track, boxes, &why);
    }
    if (rc != MOQ_OK) {
        refused(e, why);
        lm_why(reason, why);
        return rc;
    }
    size_t need;
    if (!lm_size_add(boxes.len, 1, &need)) return MOQ_ERR_INVAL;
    *out_len = need;
    if (out_cap < need) return MOQ_ERR_BUFFER;
    out[0] = MOQ_LOCMAF_ELEMENT_RAW_BOXES;
    memcpy(out + 1, boxes.data, boxes.len);

    if (e->_has_stage) e->_stats.stages_replaced++;
    moq_locmaf_enc_position_t *st = &e->_staged;
    memset(st, 0, sizeof(*st));
    stage_position(st, group_id, object_id);
    st->_after_raw_boxes = true;
    e->_has_stage = true;
    e->_stage_raw_boxes = true;
    e->_stats.raw_boxes_written++;
    return MOQ_OK;
}

void moq_locmaf_encoder_commit(moq_locmaf_encoder_t *e)
{
    if (!e || e->struct_size != sizeof(*e) || !e->_has_stage) return;
    /* A chunk's stage becomes the reference; a rawBoxes stage leaves none
     * (section 9.3), so its slots stay as they are. */
    if (!e->_stage_raw_boxes) e->_committed_slot = (uint8_t)(1 - e->_committed_slot);
    e->_committed = e->_staged;
    e->_has_stage = false;
    e->_stage_raw_boxes = false;
    e->_stats.stages_committed++;
}
