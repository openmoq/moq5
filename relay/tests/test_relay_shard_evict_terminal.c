/*
 * A subgroup cut off by eviction at a remote owner ends with a reset, never a
 * FIN, on draft-16, draft-18 and draft-22 alike.
 *
 * The publisher sits on shard 0, which owns the track; the subscriber sits
 * on shard 1, whose copy of the track the owner feeds. Each shard's log
 * retains two groups. The publisher keeps group 0's subgroup open while
 * group 1 is still arriving, then starts group 2, which evicts group 0 at
 * the owner while shard 1 still holds it. Nothing more of group 0 can reach
 * the subscriber, and nothing proves it received the whole subgroup, so the
 * subgroup's stream is reset (draft-16 §10.4.3, draft-18 §11.4.3, draft-22
 * §11.3.2): after every object it was sent, with the eviction cause, exactly
 * once, and without disturbing the subscription. The other groups still end
 * with a FIN, the publisher's late objects of group 0 never arrive, and when
 * the publisher ends the subscription the subscriber sees SUBSCRIBE_DONE
 * promptly, so the reset stream was counted.
 *
 * Rows: objects ingested whole, so the destination's journal resets the
 * subgroup; ingested as chunks, so its progress slot does; and whole objects
 * with the owner frozen right after it pushes the watermark, so that no
 * later object wakes the subscriber's shard and the watermark alone must
 * make the reset deliverable. The rig is test_relay_shard_fill's.
 */
#define main relay_shard_fill_main
#include "test_relay_shard_fill.c"
#undef main

/* What the subscriber saw of one subgroup. */
typedef struct ev_sg {
    uint64_t group, subgroup;
    int      objects;
    uint64_t last_object;
    bool     bodies_ok;      /* every object carried the publisher's bytes */
    int      fins, resets;
    int      after_terminal; /* objects received after the terminal */
    uint64_t reset_code;
    bool     reset_has_last;
    uint64_t reset_last;
} ev_sg_t;

typedef struct ev_sub {
    bool    subscribe_ok;
    int     dones;
    uint64_t done_at;
    bool    closed;
    bool    overflow;
    ev_sg_t sgs[8];
    int     n_sgs;
} ev_sub_t;

static ev_sg_t *
ev_lookup(ev_sub_t *s, uint64_t group, uint64_t subgroup)
{
    for (int i = 0; i < s->n_sgs; i++) {
        if (s->sgs[i].group == group && s->sgs[i].subgroup == subgroup) {
            return &s->sgs[i];
        }
    }
    return NULL;
}

static ev_sg_t *
ev_entry(ev_sub_t *s, uint64_t group, uint64_t subgroup)
{
    ev_sg_t *e = ev_lookup(s, group, subgroup);
    if (e != NULL) {
        return e;
    }
    if (s->n_sgs == (int)(sizeof(s->sgs) / sizeof(s->sgs[0]))) {
        s->overflow = true;
        return NULL;
    }
    e = &s->sgs[s->n_sgs++];
    memset(e, 0, sizeof(*e));
    e->group = group;
    e->subgroup = subgroup;
    e->bodies_ok = true;
    return e;
}

static void
ev_drain(sfrig_t *r, sfconn_t *cn, ev_sub_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        ev_sg_t *e = NULL;
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_OK:
            s->subscribe_ok = true;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *o = &ev.u.object_received;
            e = ev_entry(s, o->group_id, o->subgroup_id);
            if (e == NULL) {
                break;
            }
            if (e->fins + e->resets > 0) {
                e->after_terminal++;
            }
            e->objects++;
            e->last_object = o->object_id;
            if (o->payload == NULL || moq_rcbuf_len(o->payload) != 4 ||
                moq_rcbuf_data(o->payload)[0] != (uint8_t)o->group_id ||
                moq_rcbuf_data(o->payload)[1] != (uint8_t)o->object_id) {
                e->bodies_ok = false;
            }
            break;
        }
        case MOQ_EVENT_SUBGROUP_FINISHED:
            e = ev_entry(s, ev.u.subgroup_finished.group_id,
                         ev.u.subgroup_finished.subgroup_id);
            if (e != NULL) {
                e->fins++;
            }
            break;
        case MOQ_EVENT_SUBGROUP_RESET: {
            const moq_subgroup_reset_event_t *x = &ev.u.subgroup_reset;
            e = ev_entry(s, x->group_id, x->subgroup_id);
            if (e == NULL) {
                break;
            }
            e->resets++;
            e->reset_code = x->error_code;
            e->reset_has_last =
                ev.detail_size >= offsetof(moq_subgroup_reset_event_t,
                                           last_object_id) +
                                      sizeof(x->last_object_id) &&
                x->has_last_object;
            e->reset_last = x->last_object_id;
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_DONE:
            if (s->dones++ == 0) {
                s->done_at = r->now;
            }
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            s->closed = true;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

/* `cycles` 1 ms rig cycles, draining the publisher and the subscriber. */
static void
ev_run(sfrig_t *r, sf_origin_t *o, sfconn_t *sub, ev_sub_t *s, int cycles)
{
    for (int i = 0; i < cycles && r->failures == 0; i++) {
        sfrig_cycle(r, 1000);
        sfps_drain(r, o->pub, &o->pub_ps);
        ev_drain(r, sub, s);
    }
}

static moq_subgroup_handle_t
ev_open(sfrig_t *r, sf_origin_t *o, uint64_t group)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_cfg_init(&sgc);
    sgc.group_id = group;
    sgc.subgroup_id = 0;
    sgc.publisher_priority = 0;
    moq_subgroup_handle_t h;
    memset(&h, 0, sizeof(h));
    SF_CHECK(r, moq_session_open_subgroup(o->pub->peer, o->pub_ps.up_sub, &sgc,
                                          r->now, &h) == MOQ_OK);
    return h;
}

static void
ev_write(sfrig_t *r, sf_origin_t *o, moq_subgroup_handle_t h, uint64_t group,
         uint64_t object)
{
    uint8_t body[4] = { (uint8_t)group, (uint8_t)object, 0xE5, 0x5E };
    moq_rcbuf_t *pl = NULL;
    SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                    MOQ_OK);
    SF_CHECK(r, moq_session_write_object(o->pub->peer, h, object, pl, r->now) ==
                    MOQ_OK);
    moq_rcbuf_decref(pl);
}

static void
ev_close(sfrig_t *r, sf_origin_t *o, moq_subgroup_handle_t h)
{
    SF_CHECK(r, moq_session_close_subgroup(o->pub->peer, h, r->now) == MOQ_OK);
}

typedef enum ev_row {
    EV_WHOLE,
    EV_CHUNKS,
    EV_WATERMARK_ONLY,
} ev_row_t;

static const char *const k_ev_rows[] = { "whole", "chunks", "watermark-only" };

static int
ev_case(moq_version_t version, ev_row_t row)
{
    char label[96];
    snprintf(label, sizeof(label), "shard_evict_terminal %s (draft-%d)",
             k_ev_rows[row],
             version == MOQ_VERSION_DRAFT_16   ? 16
             : version == MOQ_VERSION_DRAFT_18 ? 18
                                               : 22);
    ca_t a;
    ca_init(&a);
    g_sf_version = version;
    g_sf_server_streaming = row == EV_CHUNKS;
    g_sf_log_groups = 2;
    sfrig_t rig;
    /* One entry per channel keeps the watermark a step ahead of the record
     * that follows it. The frozen row runs the wake-driven runtime, where a
     * shard steps only on a wake it earned. */
    moqr_result_t crc =
        sfrig_create_ex(&rig, &a, row == EV_WATERMARK_ONLY, 1, 0);
    g_sf_log_groups = 4;
    if (crc != MOQR_OK) {
        g_sf_version = MOQ_VERSION_DRAFT_22;
        g_sf_server_streaming = false;
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    sfrig_t *r = &rig;
    moqr_reset_desc_t evicted;
    uint64_t want_code = UINT64_MAX;
    SF_CHECK(r, moqr_reset_desc_internal(MOQR_RESET_CODE_EVICTED, &evicted) ==
                        MOQR_OK &&
                    moqr_reset_desc_emit(evicted, version, &want_code) ==
                        MOQR_OK);

    sf_origin_t o;
    memset(&o, 0, sizeof(o));
    sf_announce(r, &o);
    sfconn_t *sub = sfrig_connect(r, 1);
    SF_CHECK(r, o.pub != NULL && sub != NULL);
    ev_sub_t s;
    memset(&s, 0, sizeof(s));
    if (o.pub != NULL && sub != NULL) {
        sfrig_pump(r, 4);   /* the subscriber's session sets up */
        moq_subscribe_cfg_t scfg;
        sf_subscribe_cfg(&scfg);
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t sh;
        SF_CHECK(r, moq_session_subscribe(sub->peer, &scfg, r->now, &sh) ==
                        MOQ_OK);
        for (int i = 0; i < 60 && !(o.pub_ps.up_seen && s.subscribe_ok); i++) {
            ev_run(r, &o, sub, &s, 1);
        }
        SF_CHECK(r, o.pub_ps.up_seen && s.subscribe_ok);
    }
    if (r->failures == 0) {
        /* Group 0 stays open; group 1 is still arriving. */
        moq_subgroup_handle_t g0 = ev_open(r, &o, 0);
        for (uint64_t i = 0; i < 3; i++) {
            ev_write(r, &o, g0, 0, i);
        }
        moq_subgroup_handle_t g1 = ev_open(r, &o, 1);
        ev_write(r, &o, g1, 1, 0);
        ev_run(r, &o, sub, &s, 30);
        ev_sg_t *e0 = ev_lookup(&s, 0, 0);
        SF_CHECK(r, e0 != NULL && e0->objects == 3 && e0->fins + e0->resets == 0);

        /* Group 2 evicts group 0 at the owner. Group 1's next object follows
         * the watermark across, which wakes the subscriber's shard while it
         * still holds group 0; in the frozen row nothing follows it. */
        if (row == EV_WATERMARK_ONLY) {
            r->evict_base = sfrig_owner_evicts(r);
            r->freeze_owner_on_evict = true;
        }
        ev_write(r, &o, g1, 1, 1);
        moq_subgroup_handle_t g2 = ev_open(r, &o, 2);
        ev_write(r, &o, g2, 2, 0);
        ev_close(r, &o, g2);
        ev_run(r, &o, sub, &s, 40);
        if (row == EV_WATERMARK_ONLY) {
            SF_CHECK(r, (r->frozen & 1u) != 0);
            ev_sg_t *e2 = ev_lookup(&s, 2, 0);
            ev_sg_t *e1 = ev_lookup(&s, 1, 0);
            /* Only the watermark crossed: nothing later reached shard 1. */
            SF_CHECK(r, e2 == NULL && e1 != NULL && e1->objects == 1);
            e0 = ev_lookup(&s, 0, 0);
            SF_CHECK(r, e0 != NULL && e0->resets == 1 && e0->fins == 0);
            r->frozen = 0;
            ev_run(r, &o, sub, &s, 40);
        }

        /* The publisher goes on with group 0 and then closes it: the owner
         * refuses the late object (and may stop the stream for it), and the
         * FIN changes nothing. */
        uint8_t late[4] = { 0, 3, 0xE5, 0x5E };
        moq_rcbuf_t *lpl = NULL;
        SF_CHECK(r, moq_rcbuf_create(&r->alloc->vt, late, sizeof(late), &lpl) ==
                        MOQ_OK);
        moq_result_t wrc =
            moq_session_write_object(o.pub->peer, g0, 3, lpl, r->now);
        moq_rcbuf_decref(lpl);
        SF_CHECK(r, wrc == MOQ_OK || wrc == MOQ_ERR_STALE_HANDLE);
        moq_result_t crc0 = moq_session_close_subgroup(o.pub->peer, g0, r->now);
        SF_CHECK(r, crc0 == MOQ_OK || crc0 == MOQ_ERR_STALE_HANDLE);
        ev_close(r, &o, g1);
        ev_run(r, &o, sub, &s, 40);

        e0 = ev_lookup(&s, 0, 0);
        SF_CHECK(r, e0 != NULL);
        if (e0 != NULL) {
            if (e0->fins != 0 || e0->resets != 1) {
                printf("  [%s] group 0: %d object(s), %d FIN(s), %d reset(s)\n",
                       label, e0->objects, e0->fins, e0->resets);
            }
            SF_CHECK(r, e0->objects == 3 && e0->last_object == 2 &&
                            e0->bodies_ok);
            SF_CHECK(r, e0->fins == 0 && e0->resets == 1 &&
                            e0->after_terminal == 0);
            SF_CHECK(r, e0->reset_code == want_code);
            SF_CHECK(r, e0->reset_has_last && e0->reset_last == 2);
        }
        ev_sg_t *e1 = ev_lookup(&s, 1, 0);
        ev_sg_t *e2 = ev_lookup(&s, 2, 0);
        SF_CHECK(r, e1 != NULL && e1->objects == 2 && e1->bodies_ok &&
                        e1->fins == 1 && e1->resets == 0);
        SF_CHECK(r, e2 != NULL && e2->objects == 1 && e2->bodies_ok &&
                        e2->fins == 1 && e2->resets == 0);
        SF_CHECK(r, !s.overflow && s.n_sgs == 3);
        SF_CHECK(r, s.dones == 0 && !s.closed);

        /* The publisher ends the subscription: the subscriber's session
         * counted the reset stream, so the end is not held to a timeout. */
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x2;
        SF_CHECK(r, moq_session_done_subscribe(o.pub->peer, o.pub_ps.up_sub,
                                               &dcfg, r->now) == MOQ_OK);
        const uint64_t ended = r->now;
        for (int i = 0; i < 200 && s.dones == 0 && r->failures == 0; i++) {
            ev_run(r, &o, sub, &s, 1);
        }
        SF_CHECK(r, s.dones == 1 && s.done_at - ended <= 200000u);
        SF_CHECK(r, !s.closed);
        for (uint16_t i = 0; i < 2; i++) {
            moqr_bind_stats_t bs;
            moqr_bind_get_stats(moqr_shards_bind(r->s, i), &bs);
            SF_CHECK(r, bs.session_errors == 0);
        }
    }
    int f = sf_finish(r, &a, label);
    g_sf_version = MOQ_VERSION_DRAFT_22;
    g_sf_server_streaming = false;
    return f;
}

int
main(int argc, char **argv)
{
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    int failures = 0;
    for (size_t row = 0; row < sizeof(k_ev_rows) / sizeof(k_ev_rows[0]); row++) {
        if (argc > 1 && strcmp(argv[1], k_ev_rows[row]) != 0) {
            continue;
        }
        for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
            failures += ev_case(versions[v], (ev_row_t)row);
        }
    }
    return failures == 0 ? 0 : 1;
}
