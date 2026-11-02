#include "shards_internal.h"
#include "demand_internal.h"
#include "journal_internal.h"
#include "placement_internal.h"

#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)


/* -- test/debug introspection ----------------------------------------------- */

/* Hash a namespace directly from its parts, streaming the SAME bytes (count,
 * then all lengths, then all data) that canon_build lays out, so this matches
 * canon_hash(canon_build(parts)) without allocating. */
static uint64_t
ns_hash_parts(const moq_bytes_t *parts, uint32_t count)
{
    uint64_t h = 0xCBF29CE484222325ull;
    uint8_t cb[4];
    wr32(cb, count);
    for (int b = 0; b < 4; b++) {
        h ^= cb[b];
        h *= 0x100000001B3ull;
    }
    for (uint32_t i = 0; i < count; i++) {
        wr32(cb, (uint32_t)parts[i].len);
        for (int b = 0; b < 4; b++) {
            h ^= cb[b];
            h *= 0x100000001B3ull;
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        for (uint32_t b = 0; b < parts[i].len; b++) {
            h ^= parts[i].data[b];
            h *= 0x100000001B3ull;
        }
    }
    return h;
}


void
moqr_shards_debug_journal(moqr_shards_t *s, uint16_t shard,
                          const moq_bytes_t *parts, uint32_t count,
                          moqr_shards_jinfo_t *out)
{
    memset(out, 0, sizeof(*out));
    out->winner = -1;
    out->mirror = -1;
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t len = 0;
    uint8_t *canon = canon_build(parts, count, &len, &m->alloc);
    if (canon == NULL) {
        return;
    }
    uint32_t ins = 0;
    r_jentry *e = jrn_find(m, canon, len, &ins);
    sh_free(&m->alloc, canon, len);
    if (e == NULL) {
        return;
    }
    out->present = true;
    out->candidates = e->candidates;
    out->winner = r_winner_of(e->candidates, e->hash);
    out->mirror = e->mirror;
    out->holdout = e->holdout;
}


int32_t
moqr_shards_debug_hrw_winner(uint64_t candidates, const moq_bytes_t *parts,
                             uint32_t count)
{
    return r_winner_of(candidates, ns_hash_parts(parts, count));
}


uint32_t
moqr_shards_debug_pending_tokens(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t pending = 0;
    for (uint32_t i = 0; i < m->jrn_len; i++) {
        pending += (uint32_t)m->jrn[i].echo_ann + (uint32_t)m->jrn[i].echo_unann;
    }
    return pending;
}


uint32_t
moqr_shards_debug_mailbox_pending(moqr_shards_t *s, uint16_t src, uint16_t dst)
{
    if (s == NULL || s->mbox == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&mb->mu);
    uint32_t n = mb->count;
    pthread_mutex_unlock(&mb->mu);
    return n;
}


uint32_t
moqr_shards_debug_demand_channel_pending(moqr_shards_t *s, uint16_t src,
                                         uint16_t dst)
{
    if (s == NULL || s->dch == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    uint32_t n = ch->count;
    pthread_mutex_unlock(&ch->mu);
    return n;
}


uint64_t
moqr_shards_debug_remote_demand_refused(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_refused;
}


void
moqr_shards_debug_retrieval_meta(moqr_shards_t *s, uint16_t shard, uint32_t *owner_held,
                                 uint32_t *requester_held, uint32_t *max_refs)
{
    *owner_held = *requester_held = *max_refs = 0;
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) return;
    r_mgr *m = s->shards[shard].mgr;
    for (uint32_t i = 0; m->oret != NULL && i < m->fill_cap; i++) {
        if (m->oret[i].meta_props == NULL) continue;
        (*owner_held)++;
        uint32_t n = moq_rcbuf_refcount(m->oret[i].meta_props);
        if (n > *max_refs) *max_refs = n;
    }
    for (uint32_t i = 0; m->fret != NULL && i < m->fill_cap; i++) {
        if (m->fret[i].meta_props == NULL) continue;
        (*requester_held)++;
        uint32_t n = moq_rcbuf_refcount(m->fret[i].meta_props);
        if (n > *max_refs) *max_refs = n;
    }
}


uint32_t
moqr_shards_debug_pending_demand(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->pend_len;
}


uint64_t
moqr_shards_debug_remote_demand_resolved(moqr_shards_t *s, uint16_t shard,
                                         uint64_t *last_code,
                                         bool *last_pre_ack)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (last_code != NULL) {
        *last_code = m->last_done_code;
    }
    if (last_pre_ack != NULL) {
        *last_pre_ack = m->last_done_pre_ack;
    }
    return m->remote_demand_resolved;
}


bool
moqr_shards_debug_remote_demand_last_pd(moqr_shards_t *s, uint16_t shard,
                                        moqr_pd_desc_t *out)
{
    if (s == NULL || out == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    const r_mgr *m = s->shards[shard].mgr;

    if (m->remote_demand_resolved == 0) {
        return false;   /* nothing resolved yet: the field has no meaning */
    }
    *out = m->last_done_pd;
    return true;
}


/* The tagged terminal on the head message of a directed channel, without
 * consuming it: the same bytes a re-peek will read. */
bool
moqr_shards_debug_demand_channel_head_pd(moqr_shards_t *s, uint16_t src,
                                         uint16_t dst, moqr_pd_desc_t *out)
{
    if (s == NULL || out == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return false;
    }
    const r_demand_msg_t *msg = dch_head(s, src, dst);

    if (msg == NULL || msg->kind != D_MSG_DONE) {
        return false;
    }
    *out = msg->pd;
    return true;
}


uint32_t
moqr_shards_debug_owner_pump_subs(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    uint32_t n = 0;
    for (uint32_t i = 0; i < m->psub_cap; i++) {
        n += m->psub[i].used ? 1u : 0u;
    }
    return n;
}


moqr_track_t
moqr_shards_debug_pending_demand_track(moqr_shards_t *s, uint16_t shard,
                                       uint32_t idx, uint64_t *track_gen)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL ||
        idx >= s->shards[shard].mgr->pend_len) {
        return MOQR_TRACK_INVALID;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (track_gen != NULL) {
        *track_gen = m->pend[idx].track_gen;
    }
    return m->pend[idx].track;
}


uint64_t
moqr_shards_debug_demand_channel_bytes(moqr_shards_t *s, uint16_t src,
                                       uint16_t dst)
{
    if (s == NULL || s->dch == NULL || src >= s->shard_count ||
        dst >= s->shard_count) {
        return 0;
    }
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    uint64_t n = ch->bytes;
    pthread_mutex_unlock(&ch->mu);
    return n;
}


void
moqr_shards_debug_pump_counters(moqr_shards_t *s, uint16_t shard,
                                uint64_t *turns, uint64_t *messages,
                                uint64_t *bytes)
{
    uint64_t t = 0, m = 0, b = 0;
    if (s != NULL && shard < s->shard_count && s->shards[shard].mgr != NULL) {
        t = s->shards[shard].mgr->pump_turns;
        m = s->shards[shard].mgr->pump_messages;
        b = s->shards[shard].mgr->pump_bytes;
    }
    if (turns != NULL) {
        *turns = t;
    }
    if (messages != NULL) {
        *messages = m;
    }
    if (bytes != NULL) {
        *bytes = b;
    }
}


uint64_t
moqr_shards_debug_remote_data_rejected(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_data_rejected;
}


uint64_t
moqr_shards_debug_remote_demand_term_capacity(moqr_shards_t *s,
                                              uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_term_capacity;
}


uint64_t
moqr_shards_debug_remote_demand_term_overrun(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    return s->shards[shard].mgr->remote_demand_term_overrun;
}


uint32_t
moqr_shards_debug_owner_progress_slots(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m->psg == NULL) {
        return 0;
    }
    uint32_t n = 0;
    for (size_t i = 0; i < (size_t)m->psub_cap * s->sg_slots; i++) {
        n += m->psg[i].used ? 1u : 0u;
    }
    return n;
}


uint32_t
moqr_shards_debug_requester_open_objects(moqr_shards_t *s, uint16_t shard)
{
    if (s == NULL || shard >= s->shard_count || s->shards[shard].mgr == NULL) {
        return 0;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m->popen == NULL) {
        return 0;
    }
    uint32_t n = 0;
    for (size_t i = 0; i < (size_t)m->pend_cap * s->sg_slots; i++) {
        n += (m->popen[i].used && m->popen[i].object_open &&
              !m->popen[i].discarding)
                 ? 1u
                 : 0u;
    }
    return n;
}

#endif


/* -- production observability ------------------------------------------------ */

const char *
moqr_shards_msg_kind_name(moqr_shards_msg_kind_t kind)
{
    switch (kind) {
    case MOQR_SHARDS_MSG_DEMAND:    return "demand";
    case MOQR_SHARDS_MSG_UNDEMAND:  return "undemand";
    case MOQR_SHARDS_MSG_DONE:      return "done";
    case MOQR_SHARDS_MSG_ACK:       return "ack";
    case MOQR_SHARDS_MSG_OBJ:       return "obj";
    case MOQR_SHARDS_MSG_OBJ_OPEN:  return "obj_open";
    case MOQR_SHARDS_MSG_OBJ_CHUNK: return "obj_chunk";
    case MOQR_SHARDS_MSG_OBJ_END:   return "obj_end";
    case MOQR_SHARDS_MSG_OBJ_RESET: return "obj_reset";
    case MOQR_SHARDS_MSG_GRP_RESET: return "grp_reset";
    case MOQR_SHARDS_MSG_GRP_EVICT: return "grp_evict";
    case MOQR_SHARDS_MSG_SG_SEAL:   return "sg_seal";
    default:                        return "unknown";
    }
}


moqr_result_t
moqr_shards_get_stats(moqr_shards_t *s, uint16_t shard,
                      moqr_shards_stats_t *out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (s == NULL || shard >= s->shard_count) {
        return MOQR_ERR_INVAL;
    }
    r_mgr *m = s->shards[shard].mgr;
    if (m != NULL) {
        out->pump_turns = m->pump_turns;
        out->pump_messages = m->pump_messages;
        out->pump_bytes = m->pump_bytes;
        memcpy(out->enqueued, m->enq, sizeof(out->enqueued));
        out->remote_demand_refused = m->remote_demand_refused;
        out->remote_demand_resolved = m->remote_demand_resolved;
        out->remote_data_rejected = m->remote_data_rejected;
        out->term_capacity = m->remote_demand_term_capacity;
        out->term_overrun = m->remote_demand_term_overrun;
        out->wake_requests_push = m->wake_req_push;
        out->wake_requests_credit = m->wake_req_credit;
        out->wake_requests_local = m->wake_req_local;
        out->turns_msg_budget = m->turns_msg_budget;
        out->turns_byte_budget = m->turns_byte_budget;
        out->turns_blocked = m->turns_blocked;
        out->turns_drained = m->turns_drained;
        out->turns_with_messages = m->turns_with_messages;
        out->arb_class_refusals = m->arb_refusals;
        out->pending_demands = m->pend_len;
        out->journal_epoch = m->journal_epoch;
        /* Pump-subs by their EXACT core state, through the sub-state seam.
         * ONLY a stale/retired handle (a terminal already retired the sub,
         * or the entry never held one) is internal GONE — in neither
         * state. Any other failure is a LIVE sub in an impossible state:
         * the snapshot is poisoned and refused, never silently narrowed. */
        for (uint32_t i = 0; i < m->psub_cap; i++) {
            if (!m->psub[i].used) {
                continue;
            }
            moqr_sub_state_t st;
            moqr_result_t src =
                moqr_core_sub_state(m->core, m->psub[i].sub, &st);
            if (src == MOQR_ERR_STALE_HANDLE) {
                continue;
            }
            if (src != MOQR_OK) {
                memset(out, 0, sizeof(*out));
                return MOQR_ERR_INVAL;
            }
            if (st == MOQR_SUB_PARKED) {
                out->pump_subs_parked++;
            } else {
                out->pump_subs_active++;
            }
        }
        if (m->psg != NULL) {
            for (size_t i = 0; i < (size_t)m->psub_cap * s->sg_slots; i++) {
                out->owner_progress_slots += m->psg[i].used ? 1u : 0u;
            }
        }
        if (m->popen != NULL) {
            for (size_t i = 0; i < (size_t)m->pend_cap * s->sg_slots; i++) {
                out->requester_open_objects +=
                    (m->popen[i].used && m->popen[i].object_open &&
                     !m->popen[i].discarding)
                        ? 1u
                        : 0u;
            }
        }
        /* Manager-owned entities inside this shard core's own gauges: K
         * binding slots (K-1 origin pseudo-bindings + the watcher binding)
         * and the one wildcard namespace subscription. */
        out->internal_bindings = s->shard_count;
        out->internal_ns_subs = 1;
    }
    /* Inbound occupancy + high-water marks, each directed endpoint under
     * its OWN leaf mutex — never a global lock, never another shard's
     * core/bind/journal. */
    for (uint16_t src = 0; s->dch != NULL && src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_demand_channel_t *ch =
            &s->dch[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&ch->mu);
        out->inbound_channel_entries += ch->count + ch->held;
        out->inbound_channel_bytes += ch->bytes + ch->held_bytes;
        if (ch->count_hwm > out->channel_entries_hwm) {
            out->channel_entries_hwm = ch->count_hwm;
        }
        if (ch->bytes_hwm > out->channel_bytes_hwm) {
            out->channel_bytes_hwm = ch->bytes_hwm;
        }
        pthread_mutex_unlock(&ch->mu);
    }
    for (uint16_t src = 0; s->mbox != NULL && src < s->shard_count; src++) {
        if (src == shard) {
            continue;
        }
        r_mailbox *mb = &s->mbox[(uint32_t)src * s->shard_count + shard];
        pthread_mutex_lock(&mb->mu);
        out->mailbox_pending += mb->count;
        pthread_mutex_unlock(&mb->mu);
    }
    return MOQR_OK;
}


moqr_result_t
moqr_shards_get_pair_stats(moqr_shards_t *s, uint16_t src, uint16_t dst,
                           moqr_shards_pair_stats_t *out, size_t out_size)
{
    if (s == NULL || out == NULL || src >= s->shard_count ||
        dst >= s->shard_count || src == dst ||
        out_size < MOQR_SHARDS_PAIR_STATS_V0_SIZE) {
        return MOQR_ERR_INVAL;
    }
    /* sized-output discipline: fill a full local view, copy the bounded
     * prefix — a smaller (older) caller struct gets its prefix, a larger
     * (newer) one gets the rest zeroed */
    moqr_shards_pair_stats_t full;
    memset(&full, 0, sizeof(full));
    r_demand_channel_t *ch = &s->dch[(uint32_t)src * s->shard_count + dst];
    pthread_mutex_lock(&ch->mu);
    full.data_messages = ch->pair_data_msgs;
    full.data_bytes = ch->pair_data_bytes;
    full.control_messages = ch->pair_ctrl_msgs;
    full.refused_entries = ch->refused_entries;
    full.refused_bytes = ch->refused_bytes;
    pthread_mutex_unlock(&ch->mu);
    memset(out, 0, out_size);
    memcpy(out, &full,
           out_size < sizeof(full) ? out_size : sizeof(full));
    return MOQR_OK;
}


/* Bounded journal-dump writer: snprintf-style, counts the full required
 * length past `cap` so the caller learns the size on truncation (the same
 * contract as the metrics/trace/route serializers). */
typedef struct jd_writer {
    char  *buf;
    size_t cap;
    size_t len;
} jd_writer_t;


static void
jd_addf(jd_writer_t *w, const char *fmt, ...)
{
    char  *dst = (w->len < w->cap) ? w->buf + w->len : NULL;
    size_t avail = (w->len < w->cap) ? w->cap - w->len : 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(dst, avail, fmt, ap);
    va_end(ap);
    if (n > 0) {
        w->len += (size_t)n;
    }
}


/* One namespace part, LENGTH-DELIMITED and binary-safe: [<raw len>]"..."
 * with printable ASCII verbatim, backslash/quote escaped, and every other
 * byte as \xNN — lossless (the escape is invertible and the length prefix
 * is the raw byte count), never a C string, never a prefix. */
static void
jd_part(jd_writer_t *w, const uint8_t *p, uint32_t len)
{
    jd_addf(w, "[%u]\"", (unsigned)len);
    for (uint32_t i = 0; i < len; i++) {
        uint8_t b = p[i];
        if (b == (uint8_t)'\\' || b == (uint8_t)'"') {
            jd_addf(w, "\\%c", (char)b);
        } else if (b >= 0x20u && b <= 0x7Eu) {
            jd_addf(w, "%c", (char)b);
        } else {
            jd_addf(w, "\\x%02x", (unsigned)b);
        }
    }
    jd_addf(w, "\"");
}


moqr_result_t
moqr_shards_journal_dump_text(moqr_shards_t *s, uint16_t shard, char *buf,
                              size_t cap, size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (s == NULL || shard >= s->shard_count || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    r_mgr *m = s->shards[shard].mgr;
    jd_writer_t w = { buf, cap, 0 };
    jd_addf(&w, "shard %u\n", (unsigned)shard);
    jd_addf(&w, "journal_epoch=%llu\n",
            (unsigned long long)(m != NULL ? m->journal_epoch : 0));
    uint32_t n = (m != NULL) ? m->jrn_len : 0;
    if (n == 0) {
        jd_addf(&w, "  (none)\n");
    }
    /* The journal array is kept sorted by canonical key, so array order IS
     * the ascending canonical order the dump promises. */
    for (uint32_t i = 0; i < n; i++) {
        const r_jentry *e = &m->jrn[i];
        uint32_t count = rd32(e->canon);
        uint32_t off = 4u + 4u * count;
        jd_addf(&w, "  ns %u:", (unsigned)count);
        for (uint32_t p = 0; p < count; p++) {
            uint32_t plen = rd32(e->canon + 4u + 4u * p);
            if (p != 0) {
                jd_addf(&w, "/");
            }
            jd_part(&w, e->canon + off, plen);
            off += plen;
        }
        int32_t winner = r_winner_of(e->candidates, e->hash);
        jd_addf(&w, " candidates=0x%llx",
                (unsigned long long)e->candidates);
        if (winner >= 0) {
            jd_addf(&w, " winner=%ld", (long)winner);
        } else {
            jd_addf(&w, " winner=-");
        }
        if (e->mirror != R_MIRROR_NONE) {
            jd_addf(&w, " mirror=%ld", (long)e->mirror);
        } else {
            jd_addf(&w, " mirror=-");
        }
        if (e->holdout) {
            jd_addf(&w, " holdout");
        }
        jd_addf(&w, "\n");
    }
    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}

#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)


moqr_result_t
moqr_shards_debug_step_shard(moqr_shards_t *s, uint16_t shard, uint64_t now_us,
                             uint64_t *pushed_dst_mask)
{
    return moqr_shards_step_shard(s, shard, now_us, pushed_dst_mask);
}


void
moqr_shards_debug_round_advance(moqr_shards_t *s)
{
    if (s != NULL) {
        shards_round_advance(s);
    }
}


void
moqr_shards_debug_set_live_visibility(moqr_shards_t *s, bool live)
{
    if (s != NULL) {
        s->live_visibility = live;
    }
}

#endif

#ifdef MOQR_BIND_TESTING



/* Verify-only: copy this shard's newest seal-ingest evidence, OLDEST first,
 * into out[0..cap). Returns the number copied (0 for K==1, a bad shard, or
 * an empty log). seq values are the lifetime ingest order. */
uint32_t
moqr_shards_debug_seal_log(const moqr_shards_t *s, uint16_t shard,
                           moqr_shards_seal_ev_t *out, uint32_t cap,
                           uint64_t *out_total)
{
    if (out_total != NULL) {
        *out_total = 0;
    }
    if (s == NULL || out == NULL || cap == 0 || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return 0;
    }
    const r_mgr *m = s->shards[shard].mgr;
    if (out_total != NULL) {
        *out_total = m->seal_seq;
    }
    uint32_t ring = (uint32_t)(sizeof(m->seal_log) / sizeof(m->seal_log[0]));
    uint64_t have = m->seal_seq < ring ? m->seal_seq : ring;
    if (have > cap) {
        have = cap;
    }
    uint64_t first = m->seal_seq - have;
    for (uint64_t i = 0; i < have; i++) {
        uint64_t seq = first + i;
        uint32_t si = (uint32_t)(seq % ring);
        out[i].seq = m->seal_log[si].seq;
        out[i].src = m->seal_log[si].src;
        out[i].demand_id = m->seal_log[si].demand_id;
        out[i].group_id = m->seal_log[si].group_id;
        out[i].subgroup_id = m->seal_log[si].subgroup_id;
    }
    return (uint32_t)have;
}


/* Verify-only: track (raw + gen) → demand id, by this shard's pending-demand
 * table (the table is keyed by track — the join key a BIND_SG refusal
 * captures). 0 = no live demand matches (bad shard, or a purely LOCAL
 * track) — and ALSO 0 when MORE THAN ONE entry matches: one-demand-per-
 * {track, gen} is a core invariant, and an exact fail-closed diagnostic
 * must refuse to choose if it ever breaks, never silently pick the first. */
uint64_t
moqr_shards_debug_track_demand(const moqr_shards_t *s, uint16_t shard,
                               uint64_t track_raw, uint64_t track_gen)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return 0;
    }
    const r_mgr *m = s->shards[shard].mgr;
    uint64_t found = 0;
    uint32_t hits = 0;
    for (uint32_t i = 0; i < m->pend_len; i++) {
        if (m->pend[i].track._opaque == track_raw &&
            m->pend[i].track_gen == track_gen) {
            found = m->pend[i].demand_id;
            hits++;
        }
    }
    return hits == 1 ? found : 0;
}


/* Verify-only invariant-breaker for the pin above: clone the pending-demand
 * entry for `demand_id` under a NEW demand id (own canon copy, so the
 * normal teardown frees both), then drop it again with _pend_drop. Never a
 * production path — it exists to prove the resolver fails closed on a
 * duplicate {track, gen} rather than silently choosing. */
bool
moqr_shards_debug_pend_dup(moqr_shards_t *s, uint16_t shard,
                           uint64_t demand_id, uint64_t new_demand_id)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    r_mgr *m = s->shards[shard].mgr;
    r_pdemand *src = pend_find(m, demand_id);
    if (src == NULL || pend_find(m, new_demand_id) != NULL ||
        m->pend_len >= m->pend_cap) {
        return false;
    }
    uint8_t *canon = NULL;
    if (src->canon_len > 0) {
        canon = sh_alloc(&m->alloc, src->canon_len);
        if (canon == NULL) {
            return false;
        }
        memcpy(canon, src->canon, src->canon_len);
    }
    r_pdemand *d = &m->pend[m->pend_len++];
    *d = *src;
    d->demand_id = new_demand_id;
    d->canon = canon;
    d->popen_row = UINT32_MAX;   /* the row is OWNED by the source entry —
                                  * sharing it would free it twice on drop */
    return true;
}


bool
moqr_shards_debug_pend_drop(moqr_shards_t *s, uint16_t shard,
                            uint64_t demand_id)
{
    if (s == NULL || shard >= s->shard_count ||
        s->shards[shard].mgr == NULL) {
        return false;
    }
    r_mgr *m = s->shards[shard].mgr;
    r_pdemand *d = pend_find(m, demand_id);
    if (d == NULL) {
        return false;
    }
    pend_retire(m, d);
    return true;
}

#endif
