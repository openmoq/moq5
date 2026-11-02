#include "media_sender_internal.h"
#include "media_sender_timeline_internal.h"

/* -- Generated SAP timeline emission (network thread, s->mu held) ----- */

static size_t u64_to_dec(uint8_t *dst, uint64_t v)
{
    char tmp[20];
    size_t i = 0;
    if (v == 0) { dst[0] = '0'; return 1; }
    while (v) { tmp[i++] = (char)('0' + (int)(v % 10)); v /= 10; }
    for (size_t j = 0; j < i; j++) dst[j] = (uint8_t)tmp[i - 1 - j];
    return i;
}


/* Serialize records[start,end) as a CMSF §3.6.1 SAP timeline JSON array:
 * [{"l":[group,object],"data":[sapType,ept_ms]},...]. *out is a new rcbuf. */
static moq_result_t sap_encode_records(const moq_alloc_t *alloc,
                                       const sender_sap_rec_t *recs,
                                       uint32_t start, uint32_t end,
                                       moq_rcbuf_t **out)
{
    *out = NULL;
    /* Conservative upper bound: the "[]" wrapper (2) + per record 20 bytes of
     * fixed punctuation and a generous 20 decimal digits budgeted for each of
     * the three integers -- the uint64 worst case; sap_type is only 0-3, so this
     * deliberately over-reserves -- + one comma BETWEEN records (count - 1) + 1
     * byte of slack. */
    size_t count = (size_t)(end - start);
    size_t cap = 2 + count * (20 + 3 * 20) + (count ? count - 1 : 0) + 1;
    uint8_t *buf = (uint8_t *)alloc->alloc(cap, alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;
    size_t n = 0;
    buf[n++] = '[';
    for (uint32_t i = start; i < end; i++) {
        if (i > start) buf[n++] = ',';
        memcpy(buf + n, "{\"l\":[", 6); n += 6;
        n += u64_to_dec(buf + n, recs[i].media_group);
        buf[n++] = ',';
        n += u64_to_dec(buf + n, recs[i].media_object);
        memcpy(buf + n, "],\"data\":[", 10); n += 10;
        n += u64_to_dec(buf + n, recs[i].sap_type);
        buf[n++] = ',';
        n += u64_to_dec(buf + n, recs[i].ept_ms);
        memcpy(buf + n, "]}", 2); n += 2;
    }
    buf[n++] = ']';
    moq_result_t rc = moq_rcbuf_create(alloc, buf, n, out);
    alloc->free(buf, cap, alloc->ctx);
    return rc;
}


/* CMSF-01 3.6.1: the SAP-type timeline EPT is the earliest media presentation
 * timestamp "rounded to the nearest millisecond" -- round, not floor (the
 * MSF media-timeline mediaTime in 7 is floor-integral ms and stays floored).
 * Overflow-safe near UINT64_MAX: compute quotient + remainder rather than
 * (pts_us + 500), which would wrap for pts_us > UINT64_MAX - 500. */
uint64_t sap_ept_ms_from_us(uint64_t pts_us)
{
    uint64_t ms = pts_us / 1000u;
    if (pts_us % 1000u >= 500u) ms++;   /* ms++ cannot overflow: ms <= ~1.8e16 */
    return ms;
}


/* Append one record to timeline track `tl`'s bounded history, evicting whole
 * leading media groups once more than sap_history_groups are present. Records
 * arrive in (group, object) order, so a group's records form a contiguous run.
 * s->mu held. A realloc failure drops the record rather than failing media. */
void sap_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                        uint64_t media_group, uint64_t media_object,
                        uint8_t sap_type, uint64_t ept_ms)
{
    if (tl->sap_rec_count == tl->sap_rec_cap) {
        uint32_t nc = tl->sap_rec_cap ? tl->sap_rec_cap * 2 : 8;
        sender_sap_rec_t *nr = (sender_sap_rec_t *)s->alloc.realloc(
            tl->sap_recs, tl->sap_rec_cap * sizeof(sender_sap_rec_t),
            nc * sizeof(sender_sap_rec_t), s->alloc.ctx);
        if (!nr) return;
        tl->sap_recs = nr;
        tl->sap_rec_cap = nc;
    }
    tl->sap_recs[tl->sap_rec_count].media_group  = media_group;
    tl->sap_recs[tl->sap_rec_count].media_object = media_object;
    tl->sap_recs[tl->sap_rec_count].ept_ms       = ept_ms;
    tl->sap_recs[tl->sap_rec_count].sap_type     = sap_type;
    tl->sap_rec_count++;

    uint32_t k = tl->sap_history_groups ? tl->sap_history_groups
                                        : SENDER_DEFAULT_SAP_HISTORY_GROUPS;
    uint32_t distinct = 0;
    uint64_t prev = 0;
    for (uint32_t i = 0; i < tl->sap_rec_count; i++)
        if (i == 0 || tl->sap_recs[i].media_group != prev) {
            distinct++; prev = tl->sap_recs[i].media_group;
        }
    if (distinct <= k) return;

    /* Keep from the start of group index (distinct - k). */
    uint32_t to_drop = distinct - k, gseen = 0, keep_from = tl->sap_rec_count;
    uint64_t cur = 0;
    for (uint32_t i = 0; i < tl->sap_rec_count; i++)
        if (i == 0 || tl->sap_recs[i].media_group != cur) {
            cur = tl->sap_recs[i].media_group;
            if (gseen == to_drop) { keep_from = i; break; }
            gseen++;
        }
    if (keep_from == 0 || keep_from >= tl->sap_rec_count) return;
    memmove(tl->sap_recs, tl->sap_recs + keep_from,
            (tl->sap_rec_count - keep_from) * sizeof(sender_sap_rec_t));
    tl->sap_rec_count -= keep_from;
    tl->sap_base_seq  += keep_from;
    s->stats.sap_records_evicted += keep_from;
    if (tl->sap_lead < tl->sap_base_seq) tl->sap_lead = tl->sap_base_seq;
}


/* Whether a track wants data on the wire: a subscriber (pull), or the
 * publication established with forward on (publish-initiated). */
bool sender_track_demand(moq_media_sender_t *s, moq_media_track_t *t)
{
    if (!t || !t->pub_track) return false;
    if (moq_pub_has_subscriber(s->pub, t->pub_track)) return true;
    if (s->publish_tracks)
        return moq_pub_track_forward(s->pub, t->pub_track);
    return false;
}


/* Emit timeline objects for `tl` from its side history, gated on the timeline
 * track's OWN subscriber so it can never head-of-line-block media (MSF §8.3).
 * The timeline shares the media group ids: each new media group boundary emits
 * its own independent Object 0 carrying ALL retained accessible records up to
 * that group (so several media groups draining in one pump do NOT collapse into
 * one object); later records of an already-open group emit as incrementals. The
 * loop catches up across every pending boundary; a transport WOULD_BLOCK stops
 * it with the cursor intact for the next pump. s->mu held. */
void sender_emit_sap_for(moq_media_sender_t *s, moq_media_track_t *tl,
                         uint64_t now_us)
{
    if (!tl->pub_track) return;
    if (!sender_track_demand(s, tl)) return;

    uint64_t next_seq = tl->sap_base_seq + tl->sap_rec_count;
    while (tl->tl_pend || tl->sap_lead < next_seq) {
        uint64_t g, obj, lead;
        bool independent;
        moq_rcbuf_t *payload = NULL;
        if (tl->tl_pend) {
            payload = tl->tl_pend;   /* the refused write, retried as it was */
            tl->tl_pend = NULL;
            g = tl->tl_pend_group;
            obj = tl->tl_pend_obj;
            lead = tl->tl_pend_lead;
            independent = tl->tl_pend_independent;
        } else {
            uint32_t start = (uint32_t)(tl->sap_lead - tl->sap_base_seq);
            g = tl->sap_recs[start].media_group;
            independent = !tl->sap_open || g != tl->sap_cur_media_group;

            uint32_t from, to;
            if (independent) {
                /* Object 0 of timeline group g: all retained records through g. */
                from = 0;
                to = 0;
                while (to < tl->sap_rec_count &&
                       tl->sap_recs[to].media_group <= g) to++;
                obj = 0;
            } else {
                /* Incremental: the records of g not yet emitted. */
                from = start;
                to = start;
                while (to < tl->sap_rec_count &&
                       tl->sap_recs[to].media_group == g) to++;
                obj = tl->sap_obj_id;
            }
            if (sap_encode_records(&s->alloc, tl->sap_recs, from, to,
                                   &payload) != MOQ_OK)
                return;
            lead = tl->sap_base_seq + to;
        }
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = g;
        ocfg.object_id = obj;
        ocfg.payload = payload;
        ocfg.end_of_group = true;   /* one subgroup per group, mirroring media */
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, tl->pub_track, &ocfg,
                                                   now_us);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {   /* retry next pump, cursor intact */
            tl->tl_pend = payload;
            tl->tl_pend_group = g;
            tl->tl_pend_obj = obj;
            tl->tl_pend_lead = lead;
            tl->tl_pend_independent = independent;
            return;
        }
        moq_rcbuf_decref(payload);
        if (wrc < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        if (independent) {
            tl->sap_open = true;
            tl->sap_cur_media_group = g;
            tl->sap_obj_id = 1;
        } else {
            tl->sap_obj_id++;
        }
        /* Records evicted meanwhile were already carried by this Object. */
        tl->sap_lead = lead > tl->sap_base_seq ? lead : tl->sap_base_seq;
    }
}


/* Append one explicit §7.1.1 record to media-timeline track `tl`'s bounded
 * history, evicting whole leading media groups once more than mt_history_groups
 * are present. Records arrive in (group, object) order. s->mu held. A realloc
 * failure drops the record rather than failing media. Mirrors sap_history_append
 * with the media-timeline record shape. */
void mt_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                       uint64_t media_group, uint64_t media_object,
                       uint64_t media_time_ms, uint64_t wallclock_ms)
{
    if (tl->mt_rec_count == tl->mt_rec_cap) {
        uint32_t nc = tl->mt_rec_cap ? tl->mt_rec_cap * 2 : 8;
        moq_msf_media_timeline_record_t *nr =
            (moq_msf_media_timeline_record_t *)s->alloc.realloc(
                tl->mt_recs, tl->mt_rec_cap * sizeof(*nr),
                nc * sizeof(*nr), s->alloc.ctx);
        if (!nr) return;
        tl->mt_recs = nr;
        tl->mt_rec_cap = nc;
    }
    tl->mt_recs[tl->mt_rec_count].media_time_ms = media_time_ms;
    tl->mt_recs[tl->mt_rec_count].group         = media_group;
    tl->mt_recs[tl->mt_rec_count].object        = media_object;
    tl->mt_recs[tl->mt_rec_count].wallclock_ms  = wallclock_ms;
    tl->mt_rec_count++;

    uint32_t k = tl->mt_history_groups ? tl->mt_history_groups
                            : SENDER_DEFAULT_MEDIA_TIMELINE_HISTORY_GROUPS;
    uint32_t distinct = 0;
    uint64_t prev = 0;
    for (uint32_t i = 0; i < tl->mt_rec_count; i++)
        if (i == 0 || tl->mt_recs[i].group != prev) {
            distinct++; prev = tl->mt_recs[i].group;
        }
    if (distinct <= k) return;

    uint32_t to_drop = distinct - k, gseen = 0, keep_from = tl->mt_rec_count;
    uint64_t cur = 0;
    for (uint32_t i = 0; i < tl->mt_rec_count; i++)
        if (i == 0 || tl->mt_recs[i].group != cur) {
            cur = tl->mt_recs[i].group;
            if (gseen == to_drop) { keep_from = i; break; }
            gseen++;
        }
    if (keep_from == 0 || keep_from >= tl->mt_rec_count) return;
    memmove(tl->mt_recs, tl->mt_recs + keep_from,
            (tl->mt_rec_count - keep_from) * sizeof(*tl->mt_recs));
    tl->mt_rec_count -= keep_from;
    tl->mt_base_seq  += keep_from;
    if (tl->mt_lead < tl->mt_base_seq) tl->mt_lead = tl->mt_base_seq;
}


/* Emit media-timeline objects for `tl` from its side history, gated on the
 * timeline track's OWN subscriber so it never head-of-line-blocks media (MSF
 * §7.3). Mirrors sender_emit_sap_for: the timeline shares media group ids and
 * each new media-group boundary emits an independent Object 0 carrying ALL
 * retained accessible records through that group. Since the sender records one
 * entry per media group, the incremental branch is not normally exercised, but
 * it is kept for structural parity and safety. Serialization uses the explicit
 * §7.1.1 codec. s->mu held. */
void sender_emit_mt_for(moq_media_sender_t *s, moq_media_track_t *tl,
                        uint64_t now_us)
{
    if (!tl->pub_track) return;
    if (!sender_track_demand(s, tl)) return;

    uint64_t next_seq = tl->mt_base_seq + tl->mt_rec_count;
    while (tl->tl_pend || tl->mt_lead < next_seq) {
        uint64_t g, obj, lead;
        bool independent;
        moq_rcbuf_t *payload = NULL;
        if (tl->tl_pend) {
            payload = tl->tl_pend;   /* the refused write, retried as it was */
            tl->tl_pend = NULL;
            g = tl->tl_pend_group;
            obj = tl->tl_pend_obj;
            lead = tl->tl_pend_lead;
            independent = tl->tl_pend_independent;
        } else {
            uint32_t start = (uint32_t)(tl->mt_lead - tl->mt_base_seq);
            g = tl->mt_recs[start].group;
            independent = !tl->mt_open || g != tl->mt_cur_media_group;

            uint32_t from, to;
            if (independent) {
                from = 0;
                to = 0;
                while (to < tl->mt_rec_count && tl->mt_recs[to].group <= g) to++;
                obj = 0;
            } else {
                from = start;
                to = start;
                while (to < tl->mt_rec_count && tl->mt_recs[to].group == g) to++;
                obj = tl->mt_obj_id;
            }

            /* Serialize records[from,to) via the §7.1.1 codec: size, then encode. */
            size_t needed = 0;
            if (moq_msf_media_timeline_encode(tl->mt_recs + from, to - from,
                                              NULL, 0, &needed) != MOQ_ERR_BUFFER)
                return;
            uint8_t *buf = (uint8_t *)s->alloc.alloc(needed, s->alloc.ctx);
            if (!buf) return;
            size_t wrote = 0;
            if (moq_msf_media_timeline_encode(tl->mt_recs + from, to - from,
                                              buf, needed, &wrote) != MOQ_OK) {
                s->alloc.free(buf, needed, s->alloc.ctx);
                return;
            }
            moq_result_t crc = moq_rcbuf_create(&s->alloc, buf, wrote, &payload);
            s->alloc.free(buf, needed, s->alloc.ctx);
            if (crc != MOQ_OK) return;
            lead = tl->mt_base_seq + to;
        }

        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = g;
        ocfg.object_id = obj;
        ocfg.payload = payload;
        ocfg.end_of_group = true;
        moq_result_t wrc = moq_pub_write_object_ex(s->pub, tl->pub_track, &ocfg,
                                                   now_us);
        if (wrc == MOQ_ERR_WOULD_BLOCK) {
            tl->tl_pend = payload;
            tl->tl_pend_group = g;
            tl->tl_pend_obj = obj;
            tl->tl_pend_lead = lead;
            tl->tl_pend_independent = independent;
            return;
        }
        moq_rcbuf_decref(payload);
        if (wrc < 0) {
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return;
        }

        if (independent) {
            tl->mt_open = true;
            tl->mt_cur_media_group = g;
            tl->mt_obj_id = 1;
        } else {
            tl->mt_obj_id++;
        }
        tl->mt_lead = lead > tl->mt_base_seq ? lead : tl->mt_base_seq;
    }
}
