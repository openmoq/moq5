#include "media_sender_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_write_internal.h"

#ifdef MOQ_SERVICE_HAVE_MPEG2TS


bool sender_ts_complete_step(moq_media_sender_t *s, moq_media_track_t *t);

bool sender_ts_complete_ready(const moq_media_track_t *t);

#endif

#ifdef MOQ_MEDIA_SENDER_TESTING
/* Test-only gap observation in write_mpeg2ts: called while the writer has
 * released s->mu between Objects (sender_ts_yield), so a test can latch a
 * session into that gap. NULL disables. */
static void (*g_test_ts_yield)(void *ctx);

static void *g_test_ts_yield_ctx;

void moq_media_sender_test_set_ts_yield_hook(void (*fn)(void *), void *ctx);

void moq_media_sender_test_set_ts_yield_hook(void (*fn)(void *), void *ctx)
{
    g_test_ts_yield = fn;
    g_test_ts_yield_ctx = ctx;
}

#define SENDER_TS_YIELD_HOOK() \
    do { if (g_test_ts_yield) g_test_ts_yield(g_test_ts_yield_ctx); } while (0)


#else
#define SENDER_TS_YIELD_HOOK() ((void)0)


#endif


void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t);


/* -- Track management (app thread) ------------------------------------ */

/* -- Packaging extensions and MPEG2TS (add_track) ----------------------- */

/* The first published size of a draft-gregoire-moq-msfts record: through
 * `malformed`. */
#define SENDER_TS_RECORD_V0_SIZE \
    (offsetof(moq_msf_mpeg2ts_t, malformed) + sizeof(uint32_t))

#define SENDER_TS_DEFAULT_OBJECT_PACKETS 64u


#ifdef MOQ_SERVICE_HAVE_MPEG2TS
/* SI PIDs the per-program filter holds (no byte-stream track is added in a
 * build without moq::mpeg2ts). */
#define SENDER_TS_MAX_SI_PIDS MOQ_MPEG2TS_FILTER_MAX_SI_PIDS


#else
#define SENDER_TS_MAX_SI_PIDS 0u


#endif

static size_t sender_align8(size_t n) { return (n + 7u) & ~(size_t)7u; }


/* The list rules (the catalog encoder's, restated for add_track): an array
 * when count > 0, a known kind once, a record of at least its first
 * published size with coherent spans, and only on the packaging the kind
 * belongs to. Sets *rec to the MPEG2TS record (or NULL). No allocation. */
moq_result_t sender_check_extensions(const moq_media_track_cfg_t *cfg,
                                     const moq_msf_mpeg2ts_t **rec)
{
    *rec = NULL;
    if (cfg->extension_count > 0 && !cfg->extensions) return MOQ_ERR_INVAL;
    if (!sender_product_fits(cfg->extension_count, sizeof(moq_msf_track_ext_t)))
        return MOQ_ERR_INVAL;
    for (size_t i = 0; i < cfg->extension_count; i++) {
        const moq_msf_track_ext_t *e = &cfg->extensions[i];
        if (e->kind != MOQ_MSF_EXT_MPEG2TS) return MOQ_ERR_INVAL;
        if (*rec) return MOQ_ERR_INVAL;                   /* duplicate kind */
        const moq_msf_mpeg2ts_t *m = e->u.mpeg2ts;
        if (!m || m->struct_size < SENDER_TS_RECORD_V0_SIZE)
            return MOQ_ERR_INVAL;
        if ((m->si_pid_count > 0 && !m->si_pids) ||
            (m->mode_name.len > 0 && !m->mode_name.data) ||
            (m->timestamp_mode_name.len > 0 && !m->timestamp_mode_name.data))
            return MOQ_ERR_INVAL;
        if (!sender_product_fits(m->si_pid_count, sizeof(uint32_t)))
            return MOQ_ERR_INVAL;
        *rec = m;
    }
    bool is_ts = cfg->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
    if ((*rec != NULL) != is_ts) return MOQ_ERR_INVAL;
    return MOQ_OK;
}


/* Deep-copy the checked list into one block (entries, record, SI PIDs,
 * names), so the caller's buffers need not outlive add_track. */
moq_result_t sender_copy_extensions(moq_media_sender_t *s,
                                    moq_media_track_t *t,
                                    const moq_msf_mpeg2ts_t *src)
{
    if (!src) return MOQ_OK;
    size_t pids = src->si_pid_count * sizeof(uint32_t);
    size_t need = sender_align8(sizeof(moq_msf_track_ext_t));
    if (!sender_sum_fits(&need, sender_align8(sizeof(moq_msf_mpeg2ts_t))) ||
        pids > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(pids)) ||
        src->mode_name.len > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(src->mode_name.len)) ||
        src->timestamp_mode_name.len > SIZE_MAX - 8u ||
        !sender_sum_fits(&need, sender_align8(src->timestamp_mode_name.len)))
        return MOQ_ERR_INVAL;
    uint8_t *blk = (uint8_t *)s->alloc.alloc(need, s->alloc.ctx);
    if (!blk) return MOQ_ERR_NOMEM;
    memset(blk, 0, need);
    moq_msf_track_ext_t *list = (moq_msf_track_ext_t *)(void *)blk;
    size_t off = sender_align8(sizeof(moq_msf_track_ext_t));
    moq_msf_mpeg2ts_t *rec = (moq_msf_mpeg2ts_t *)(void *)(blk + off);
    off += sender_align8(sizeof(*rec));
    moq_msf_mpeg2ts_init(rec);
    memcpy(rec, src, src->struct_size < sizeof(*rec) ? src->struct_size
                                                     : sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->si_pids = NULL;
    rec->mode_name = (moq_bytes_t){ NULL, 0 };
    rec->timestamp_mode_name = (moq_bytes_t){ NULL, 0 };
    if (pids > 0) {
        memcpy(blk + off, src->si_pids, pids);
        rec->si_pids = (uint32_t *)(void *)(blk + off);
        off += sender_align8(pids);
    }
    if (src->mode_name.len > 0) {
        memcpy(blk + off, src->mode_name.data, src->mode_name.len);
        rec->mode_name = (moq_bytes_t){ blk + off, src->mode_name.len };
        off += sender_align8(src->mode_name.len);
    }
    if (src->timestamp_mode_name.len > 0) {
        memcpy(blk + off, src->timestamp_mode_name.data,
               src->timestamp_mode_name.len);
        rec->timestamp_mode_name =
            (moq_bytes_t){ blk + off, src->timestamp_mode_name.len };
    }
    list[0].kind = MOQ_MSF_EXT_MPEG2TS;
    list[0].u.mpeg2ts = rec;
    t->ext = list;
    t->ext_count = 1;
    t->ext_block_size = need;
    return MOQ_OK;
}


/* The sender-owned record of a track (NULL for other packagings). */
moq_msf_mpeg2ts_t *sender_ts_record(moq_media_track_t *t)
{
    return t->ext_count ? (moq_msf_mpeg2ts_t *)(uintptr_t)t->ext[0].u.mpeg2ts
                        : NULL;
}


/* Whole source packets of `ps` octets with their sync bytes, at least one. */
static bool sender_ts_whole_packets(moq_bytes_t b, uint32_t ps)
{
    if (b.len == 0 || b.len % ps != 0) return false;
    size_t sync = ps == 192u ? 4u : 0u;
    for (size_t off = 0; off < b.len; off += ps)
        if (b.data[off + sync] != 0x47u) return false;
    return true;
}

#ifdef MOQ_SERVICE_HAVE_MPEG2TS


/* Packetizer emit callback: runs inside packetizer calls, under s->mu. The
 * payload is valid only during the callback, so it is copied. */
static void sender_ts_emit(void *ctx, const moq_mpeg2ts_emitted_object_t *obj)
{
    moq_media_track_t *t = (moq_media_track_t *)ctx;
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_rcbuf_t *b = NULL;
    if (ts->staged_n == SENDER_TS_STAGE_MAX) {
        /* Beyond the packetizer's bound: the last staged Object gives up its
         * place so the loss still lands in order. */
        sender_ts_obj_t *last = &ts->staged[SENDER_TS_STAGE_MAX - 1u];
        if (last->payload) moq_rcbuf_decref(last->payload);
        last->payload = NULL;
        last->lost = true;
        last->new_group = last->new_group || obj->new_group;
        ts->emit_lost = true;
        return;
    }
    sender_ts_obj_t *o = &ts->staged[ts->staged_n++];
    o->new_group = obj->new_group;
    o->random_access = obj->random_access;
    o->lost = moq_rcbuf_create(&ts->alloc, obj->payload.data,
                               obj->payload.len, &b) != MOQ_OK;
    o->payload = b;
    if (o->lost) ts->emit_lost = true;
}


static moq_result_t sender_ts_new(moq_media_sender_t *s, moq_media_track_t *t,
                                  const moq_media_track_cfg_t *cfg)
{
    const moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
    uint32_t op = cfg->mpeg2ts_object_packets
                      ? cfg->mpeg2ts_object_packets
                      : SENDER_TS_DEFAULT_OBJECT_PACKETS;
    if (!sender_product_fits(op, rec->packet_size)) return MOQ_ERR_INVAL;
    sender_ts_t *ts = (sender_ts_t *)s->alloc.alloc(sizeof(*ts), s->alloc.ctx);
    if (!ts) return MOQ_ERR_NOMEM;
    memset(ts, 0, sizeof(*ts));
    ts->alloc = s->alloc;
    ts->ps = rec->packet_size;
    ts->obj_cap = (size_t)op * rec->packet_size;
    ts->lead_cap = (size_t)(op - 1u) * rec->packet_size;
    ts->obj_buf = (uint8_t *)s->alloc.alloc(ts->obj_cap, s->alloc.ctx);
    if (ts->lead_cap)
        ts->lead_buf = (uint8_t *)s->alloc.alloc(ts->lead_cap, s->alloc.ctx);
    bool per_program = rec->has_mode &&
                       rec->mode == MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM;
    if (per_program)
        ts->filter = (moq_mpeg2ts_program_filter_t *)s->alloc.alloc(
            sizeof(*ts->filter), s->alloc.ctx);
    t->ts = ts;
    if (!ts->obj_buf || (ts->lead_cap && !ts->lead_buf) ||
        (per_program && !ts->filter)) {
        sender_ts_free(s, t);
        return MOQ_ERR_NOMEM;
    }

    moq_mpeg2ts_packetizer_cfg_t pc;
    moq_mpeg2ts_packetizer_cfg_init(&pc);
    pc.packet_size = rec->packet_size;
    pc.object_packets = op;
    pc.group_at_random_access = true;
    pc.program_number = rec->has_program_number
                            ? (uint16_t)rec->program_number : 0;
    pc.min_group_ms = cfg->mpeg2ts_min_group_ms;
    pc.wait_for_random_access = !cfg->mpeg2ts_forward_before_random_access;
    if (rec->has_pcr_pid) pc.pcr_pid = (uint16_t)rec->pcr_pid;
    pc.object_buf = ts->obj_buf;
    pc.object_cap = ts->obj_cap;
    pc.lead_buf = ts->lead_buf;
    pc.lead_cap = ts->lead_cap;
    pc.emit = sender_ts_emit;
    pc.ctx = t;
    moq_result_t rc = moq_mpeg2ts_packetizer_init(&ts->pz, sizeof(ts->pz), &pc);
    if (rc == MOQ_OK && per_program) {
        /* Per-Program: the record's program, its SI PIDs, and the CAT. */
        uint16_t si[MOQ_MPEG2TS_FILTER_MAX_SI_PIDS];
        for (size_t i = 0; i < rec->si_pid_count; i++)
            si[i] = (uint16_t)rec->si_pids[i];
        moq_mpeg2ts_program_filter_cfg_t fc;
        moq_mpeg2ts_program_filter_cfg_init(&fc);
        fc.packet_size = rec->packet_size;
        fc.program_number = (uint16_t)rec->program_number;
        fc.si_pids = rec->si_pid_count ? si : NULL;
        fc.si_pid_count = rec->si_pid_count;
        rc = moq_mpeg2ts_program_filter_init(ts->filter, sizeof(*ts->filter),
                                             &fc);
    }
    if (rc != MOQ_OK) {
        sender_ts_free(s, t);
        return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}


static void sender_ts_drop_list(sender_ts_obj_t *list, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (list[i].payload) moq_rcbuf_decref(list[i].payload);
}


void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    if (!ts) return;
    sender_ts_drop_list(ts->staged, ts->staged_n);
    sender_ts_drop_list(ts->parked, ts->parked_n);
    if (ts->obj_buf) s->alloc.free(ts->obj_buf, ts->obj_cap, s->alloc.ctx);
    if (ts->lead_buf) s->alloc.free(ts->lead_buf, ts->lead_cap, s->alloc.ctx);
    if (ts->filter)
        s->alloc.free(ts->filter, sizeof(*ts->filter), s->alloc.ctx);
    s->alloc.free(ts, sizeof(*ts), s->alloc.ctx);
    t->ts = NULL;
}

#else

void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    (void)s; (void)t;
}

#endif


/* add_track's MPEG2TS rules (draft-gregoire-moq-msfts) and the record the
 * service authors: everything checked before the track is registered. */
moq_result_t sender_ts_prepare(moq_media_sender_t *s,
                               moq_media_track_t *t,
                               const moq_media_track_cfg_t *cfg)
{
    moq_msf_mpeg2ts_t *rec = sender_ts_record(t);
    /* ES-packets authoring needs the multi-track split and the PAT/PMT
     * tracks, which stay helper-level; timelines need per-Object
     * presentation times, which TS keeps inside the packets. */
    if (rec->has_mode && rec->mode == MOQ_MSF_MPEG2TS_MODE_ES_PACKETS)
        return MOQ_ERR_INVAL;
    if (cfg->emit_sap_timeline || cfg->emit_media_timeline)
        return MOQ_ERR_INVAL;
    if (!rec->has_packet_size ||
        (rec->packet_size != 188u && rec->packet_size != 192u))
        return MOQ_ERR_INVAL;
    if (t->init_data.len > 0 &&
        !sender_ts_whole_packets(t->init_data, rec->packet_size))
        return MOQ_ERR_INVAL;

    if (!cfg->mpeg2ts_app_objects) {
        /* Byte stream: the program filter the record implies needs its
         * program number, and SI PIDs it can hold. */
        if (rec->has_mode && rec->mode == MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM &&
            (!rec->has_program_number ||
             rec->si_pid_count > SENDER_TS_MAX_SI_PIDS))
            return MOQ_ERR_INVAL;
        /* mpeg2tsRandomAccess is the service's to declare: Groups open at
         * random access points, so it holds unless packets before the first
         * one are forwarded. An unmodified multiplex without a reference
         * program names no stream to hold it on (the field is absent). */
        bool multiplex_unref =
            rec->has_mode &&
            rec->mode == MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX &&
            !rec->has_program_number;
        bool derived = !cfg->mpeg2ts_forward_before_random_access;
        if (rec->has_random_access &&
            (multiplex_unref || rec->random_access != derived))
            return MOQ_ERR_INVAL;
        if (!multiplex_unref) {
            rec->has_random_access = true;
            rec->random_access = derived;
        }
    } else {
        t->ts_app_objects = true;
        t->ts_random_access = rec->has_random_access && rec->random_access;
    }

    /* The track as the catalog will carry it must pass the subscriber's
     * check, so a bad track fails here rather than at catalog encode. */
    moq_msf_track_t m;
    track_fill_msf(t, &m);
    if (moq_msf_track_mpeg2ts_check(&m, NULL) != MOQ_OK) return MOQ_ERR_INVAL;

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    if (!cfg->mpeg2ts_app_objects) return sender_ts_new(s, t, cfg);
#else
    (void)s;
#endif
    return MOQ_OK;
}


/* -- MPEG-2 TS byte stream (app thread) -------------------------------- */

/* The first published size of moq_media_send_mpeg2ts_t: through
 * end_object. */
#define MEDIA_SEND_MPEG2TS_V0_SIZE \
    (offsetof(moq_media_send_mpeg2ts_t, end_object) + sizeof(bool))


#ifdef MOQ_SERVICE_HAVE_MPEG2TS

/* The track's open Group lost an Object that never reached the queue:
 * drop what is still queued of it (counted, as any drop) and, when its wire
 * subgroup is open, arm the reset that abandons it. The Group is closed for
 * writes. s->mu held. */
static void sender_abandon_open_group_locked(moq_media_sender_t *s,
                                             moq_media_track_t *t)
{
    if (!t->group_open) return;
    uint64_t g = t->group_seq;
    preq_evict_group(s, t, g);
    if (sender_holds_group(t, g)) t->pending_reset = true;
    t->group_open = false;
    t->cur_group_anchored = false;
}


/* Queue the Objects of `list` (the staged or the parked ones) in order,
 * through the same admission as write(). A continuation of a Group that lost
 * media is discarded and counted, never queued: the next Group start ends
 * that. When an Object cannot be queued, it and every later one are parked,
 * in order, and the next call queues them first -- unless the sender is
 * closed or the track can take no more writes (MOQ_ERR_CLOSED,
 * MOQ_ERR_WRONG_STATE), when they are released. So a refusal that can pass
 * (WOULD_BLOCK, an interrupt, a clock failure in the wait) never leaves a
 * hole in a Group. s->mu held (a BLOCK_TIMEOUT wait releases it while
 * waiting). */
moq_result_t sender_ts_queue(moq_media_sender_t *s,
                             moq_media_track_t *t,
                             sender_ts_obj_t *list, size_t *n,
                             bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_result_t rc = MOQ_OK;
    size_t i = 0;
    for (; i < *n; i++) {
        sender_ts_obj_t *o = &list[i];
        if (o->lost) {
            /* The rest of its Group is discarded (broken). A lost
             * continuation abandons the open Group: what is still queued of
             * it is dropped and an open wire subgroup is reset, so the
             * truncated Group is never finished as if complete. A lost Group
             * start takes its Group ID with it, so the next Group follows a
             * visible hole, and leaves the Group before it, already
             * complete, alone. */
            ts->broken = true;
            if (o->new_group) {
                t->group_seq = t->group_started ? t->group_seq + 1
                                                : sender_group_base(s);
                t->group_started = true;
                t->group_open = false;
                t->cur_group_anchored = false;
            } else {
                sender_abandon_open_group_locked(s, t);
            }
            o->lost = false;
            continue;
        }
        if (o->new_group) {
            ts->broken = false;
        } else if (ts->broken || !t->group_open) {
            moq_rcbuf_decref(o->payload);
            o->payload = NULL;
            ts->continuation_discarded++;
            continue;
        }
        sender_admit_t a;
        memset(&a, 0, sizeof(a));
        a.payload = o->payload;
        a.is_sync = o->random_access;
        a.anchors_group = o->new_group; /* every Group start is the anchor */
        a.starts_group = o->new_group;
        a.completing = t->complete_end_owed;
        rc = sender_admit_locked(s, t, &a);
        if (rc != MOQ_OK) break;
        o->payload = NULL;
        *queued_any = true;
    }
    size_t rest = *n - i;
    *n = 0;
    if (rc != MOQ_OK && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_WRONG_STATE) {
        /* Staged Objects are queued only once nothing is parked, so the
         * parked list is either this list or empty. */
        sender_ts_obj_t keep[SENDER_TS_STAGE_MAX];
        memcpy(keep, &list[i], rest * sizeof(*keep));
        memcpy(ts->parked, keep, rest * sizeof(*keep));
        ts->parked_n = rest;
    } else {
        sender_ts_drop_list(&list[i], rest);
    }
    return rc;
}


/* Queue what the packetizer just emitted (an Object it could not copy is a
 * local loss of its Group). */
moq_result_t sender_ts_queue_staged(moq_media_sender_t *s,
                                    moq_media_track_t *t,
                                    bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_result_t rc = sender_ts_queue(s, t, ts->staged, &ts->staged_n,
                                      queued_any);
    if (rc == MOQ_OK && ts->emit_lost) {
        ts->emit_lost = false;
        return MOQ_ERR_NOMEM;
    }
    return rc;
}


/* Filter (per-program) and packetize one whole source packet, then queue
 * what it completed. */
static moq_result_t sender_ts_packet(moq_media_sender_t *s,
                                     moq_media_track_t *t,
                                     const uint8_t *pkt, bool *queued_any)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    moq_bytes_t in = { pkt, ts->ps };
    uint8_t out[MOQ_MPEG2TS_M2TS_PACKET_SIZE];
    if (ts->filter) {
        moq_mpeg2ts_filter_action_t act = MOQ_MPEG2TS_FILTER_DROP;
        /* A packet the filter cannot parse is one it does not keep. */
        if (moq_mpeg2ts_program_filter_push(ts->filter, in, out, sizeof(out),
                                            &act) != MOQ_OK ||
            act == MOQ_MPEG2TS_FILTER_DROP)
            return MOQ_OK;
        in = (moq_bytes_t){ out, ts->ps };
    }
    if (moq_mpeg2ts_packetizer_push(&ts->pz, in) != MOQ_OK)
        return MOQ_ERR_PROTO;
    return sender_ts_queue_staged(s, t, queued_any);
}


/* After an Object was queued the writer lets go of s->mu, so the drain is
 * never shut out for longer than one Object; the busy latch keeps the track
 * to this writer meanwhile. Returns non-OK when the track or sender changed
 * state in between, including a companion the draft latched meanwhile
 * refuses: no further byte is then taken. */
static moq_result_t sender_ts_yield(moq_media_sender_t *s,
                                    moq_media_track_t *t, bool *queued_any)
{
    if (!*queued_any) return MOQ_OK;
    *queued_any = false;
    bool wake = s->ready;
    pthread_mutex_unlock(&s->mu);
    if (wake) moq_endpoint_wake(s->ep);
    SENDER_TS_YIELD_HOOK();
    pthread_mutex_lock(&s->mu);
    if (s->fatal || sender_ep_closed(s)) return MOQ_ERR_CLOSED;
    /* complete() latched meanwhile: no further byte is taken; what this call
     * took already is the source complete() finishes. */
    if (t->removed || t->auth_failed || t->vod_converted || t->complete_end_owed)
        return MOQ_ERR_WRONG_STATE;
    if (sender_companion_refused_locked(s)) return MOQ_ERR_UNSUPPORTED;
    return MOQ_OK;
}


/* A TS track's source complete() still has to finish, with no writer
 * holding it: the next pump can take that step. */
bool sender_ts_complete_ready(const moq_media_track_t *t)
{
    const sender_ts_t *ts = (const sender_ts_t *)t->ts;
    return ts && !ts->complete_flushed && !ts->busy;
}


/* complete() finishes a byte-stream TS track's source as end_track does, on
 * the network thread and without waiting: the Objects held back, then the
 * packetizer's last ones, each admitted without evicting queued media or
 * waiting for room; what cannot be queued yet stays parked for a later pump.
 * A partial packet is discarded and counted. While a writer is in a call the
 * source is its: the step waits for that call to return. An Object lost on
 * the way (its copy failed) fails the completion (complete_failed). s->mu
 * held. True once the source is finished. */
bool sender_ts_complete_step(moq_media_sender_t *s, moq_media_track_t *t)
{
    sender_ts_t *ts = (sender_ts_t *)t->ts;
    if (s->complete_failed) return false;
    if (!ts || ts->complete_flushed) return true;
    if (ts->busy) return false;
    ts->busy = true;
    bool queued_any = false;
    moq_result_t rc = sender_ts_queue(s, t, ts->parked, &ts->parked_n, &queued_any);
    if (rc == MOQ_OK && !ts->ending) {
        ts->ending = true;
        if (moq_mpeg2ts_packetizer_flush(&ts->pz) != MOQ_OK)
            rc = MOQ_ERR_INTERNAL;
        else
            rc = sender_ts_queue_staged(s, t, &queued_any);
    }
    ts->busy = false;
    if (rc == MOQ_ERR_WOULD_BLOCK) return false;   /* parked: a later pump */
    if (rc != MOQ_OK) {
        s->complete_failed = true;
        s->stats.last_error = rc;
        sender_note_completion_failure(s, rc);
        return false;
    }
    ts->partial_bytes_discarded += ts->carry_len;
    ts->carry_len = 0;
    ts->complete_flushed = true;
    t->group_open = false;
    return true;
}

#endif


moq_result_t moq_media_sender_write_mpeg2ts(moq_media_sender_t *s,
                                            moq_media_track_t *track,
                                            const moq_media_send_mpeg2ts_t *in,
                                            size_t *consumed)
{
    if (consumed) *consumed = 0;
    if (!s || !track || !in || !consumed) return MOQ_ERR_INVAL;
    if (in->struct_size < MEDIA_SEND_MPEG2TS_V0_SIZE) return MOQ_ERR_INVAL;
    if (in->data.len > 0 && !in->data.data) return MOQ_ERR_INVAL;
#ifndef MOQ_SERVICE_HAVE_MPEG2TS
    (void)track;
    return MOQ_ERR_UNSUPPORTED;
#else
    if (s->ep && moq_endpoint_interrupted_internal(s->ep))
        return MOQ_ERR_INTERRUPTED;
    if (sender_terminal(s)) return MOQ_ERR_CLOSED;

    pthread_mutex_lock(&s->mu);
    if (s->fatal) { pthread_mutex_unlock(&s->mu); return MOQ_ERR_CLOSED; }
    bool owned = false;
    for (size_t i = 0; i < s->track_count; i++)
        if (s->tracks[i] == track) { owned = true; break; }
    if (!owned || track->packaging != MOQ_MEDIA_PACKAGING_MPEG2TS) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_INVAL;
    }
    sender_ts_t *ts = (sender_ts_t *)track->ts;
    if (!ts || ts->busy || ts->ending || track->end_requested ||
        track->removed || track->auth_failed || track->vod_converted) {
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_WRONG_STATE;
    }
    /* The same companion refusal as write(), before any carry, parked
     * Object, packetizer or queue state is touched: nothing is consumed. */
    if (sender_companion_refused_locked(s)) {
        s->stats.last_error = MOQ_ERR_UNSUPPORTED;
        pthread_mutex_unlock(&s->mu);
        return MOQ_ERR_UNSUPPORTED;
    }
    ts->busy = true;

    bool queued_any = false, progressed = false;
    /* What a previous call could not queue goes first. */
    moq_result_t rc = sender_ts_queue(s, track, ts->parked, &ts->parked_n,
                                      &queued_any);
    const uint8_t *data = in->data.data;
    size_t len = in->data.len, off = 0;
    const size_t ps = ts->ps;
    const size_t sync = ps == MOQ_MPEG2TS_M2TS_PACKET_SIZE
                            ? MOQ_MPEG2TS_PREFIX_SIZE : 0u;
    while (rc == MOQ_OK && off < len) {
        if (ts->carry_len > 0 || len - off < ps) {
            /* Assemble a packet that spans calls. Its sync byte is checked
             * as it arrives, before any byte of it is taken. */
            size_t take = ps - ts->carry_len;
            if (take > len - off) take = len - off;
            if (ts->carry_len <= sync && ts->carry_len + take > sync &&
                data[off + (sync - ts->carry_len)] != MOQ_MPEG2TS_SYNC_BYTE) {
                /* The bytes kept from earlier calls belong to the bad
                 * packet: discarded and counted, so the next call starts at
                 * a packet boundary. */
                ts->partial_bytes_discarded += ts->carry_len;
                ts->carry_len = 0;
                rc = MOQ_ERR_PROTO;
                break;
            }
            memcpy(ts->carry + ts->carry_len, data + off, take);
            ts->carry_len += take;
            off += take;
            *consumed = off;
            if (ts->carry_len < ps) break;
            ts->carry_len = 0;
            rc = sender_ts_packet(s, track, ts->carry, &queued_any);
        } else {
            if (data[off + sync] != MOQ_MPEG2TS_SYNC_BYTE) {
                rc = MOQ_ERR_PROTO;
                break;
            }
            const uint8_t *pkt = data + off;
            off += ps;
            *consumed = off;
            rc = sender_ts_packet(s, track, pkt, &queued_any);
        }
        if (queued_any) progressed = true;
        if (rc == MOQ_OK) rc = sender_ts_yield(s, track, &queued_any);
    }
    /* end_object: this call's last packet closes an Object. */
    if (rc == MOQ_OK && in->end_object) {
        progressed = true;
        if (moq_mpeg2ts_packetizer_end_object(&ts->pz) != MOQ_OK)
            rc = MOQ_ERR_INTERNAL;
        else
            rc = sender_ts_queue_staged(s, track, &queued_any);
    }
    /* A short write: Objects held back after any progress are reported by
     * *consumed and the parked count, not by an error. */
    if (queued_any || *consumed > 0) progressed = true;
    if (rc == MOQ_ERR_WOULD_BLOCK && progressed) rc = MOQ_OK;
    if (rc != MOQ_OK) s->stats.last_error = rc;
    ts->busy = false;
    bool wake = queued_any && s->ready;
    pthread_mutex_unlock(&s->mu);
    if (wake) moq_endpoint_wake(s->ep);
    return rc;
#endif
}
