/*
 * A subgroup whose stream ends early -- reset by the upstream while one of its
 * objects is streaming in -- ends with a reset for every subscriber, on
 * draft-16, draft-18 and draft-22.
 *
 * In streaming receive mode the relay learns of an upstream RESET_STREAM
 * that cuts an object off from that object's terminal chunk, and nothing
 * else follows it: the session reports a reset on exactly one surface. A
 * subscriber that had begun the object resets its downstream stream with
 * it. One that had not -- still delivering an earlier object, or joining
 * afterwards -- is sent the retained objects and must then still have its
 * stream reset, neither left open nor closed with a FIN (draft-16 §10.4.3,
 * draft-18 §11.4.3, draft-22 §11.3.2), without waiting for an eviction, a
 * FIN, new media or the end of the Track.
 *
 * Group 1 carries subgroup 0, whose object 0 completes and whose object 1
 * is cut off, and a sibling subgroup 1 with objects 2 and 3 (Object IDs are
 * unique within a Group). The subscribers:
 *   C  reads promptly;
 *   A  begins object 1, then is held behind a full send queue, so the
 *      relay's reset for it is refused once and taken when the queue frees;
 *   B  is held behind a full send queue on the sibling's object while object
 *      1 arrives and is reset, so it never begins object 1;
 *   D  subscribes after the reset.
 * Each sees object 0 whole and then exactly one reset with the publisher's
 * code; B and D see nothing of object 1. The sibling subgroup is untouched.
 *
 * Rows:
 *   unbegun  the reset cuts object 1 off and nothing arrives after it;
 *   sibling  as unbegun, and the sibling subgroup finishes while A's and
 *            B's terminals are held;
 *   live     object 1 completes and subgroup 0 ends with a FIN: no reset;
 *   idle     object 1 completes and the reset arrives between objects, which
 *            the session reports with SUBGROUP_RESET: one reset after both;
 *   reopen   as idle, and the publisher then opens the subgroup again on a
 *            new stream with object 4 and a FIN: after the reset, a new
 *            stream carries object 4 and ends with that FIN. B's send queue
 *            takes the reset and the new stream before it has room for
 *            object 4, which then goes out on that stream, with no further
 *            reset.
 */
#include "relay_session_binding_harness.h"

#define SR_CHUNK   32u   /* bytes in each written chunk */
#define SR_OBJS    8u    /* object ids recorded per subgroup */
#define SR_ACTIONS 2u    /* send queue of the held subscribers' sessions */
#define SR_CODE    0x99u /* the publisher's reset code */

typedef enum { SR_UNBEGUN, SR_SIBLING, SR_LIVE, SR_IDLE, SR_REOPEN,
               SR_ROWS } sr_row_t;
static const char *const sr_row_names[SR_ROWS] = { "unbegun", "sibling", "live",
                                                   "idle", "reopen" };

/* What one subscriber's session surfaced for one subgroup. */
typedef struct sr_sg {
    uint64_t group, subgroup;
    uint32_t begun;            /* object ids whose begin chunk arrived */
    uint32_t done;             /* object ids that ended NORMAL */
    int      chunks[SR_OBJS];  /* data chunks per object id */
    bool     bodies_ok;        /* every byte was the publisher's */
    int      fins;             /* SUBGROUP_FINISHED */
    int      chunk_resets;     /* an object ended by its RESET terminal */
    int      sg_resets;        /* SUBGROUP_RESET */
    uint64_t reset_code;
    uint64_t reset_object;     /* the object a RESET terminal ended */
    bool     reset_has_last;
    uint64_t reset_last;
    int      after_terminal;   /* events for the subgroup after its terminal */
    char     seq[24];          /* in order: an object id when it completes,
                                * x<id> when its RESET terminal ends it, S for
                                * SUBGROUP_RESET, F for SUBGROUP_FINISHED */
    size_t   seq_len;
} sr_sg_t;

typedef struct sr_peer {
    bool    subscribe_ok;
    int     dones;
    bool    closed;
    bool    unexpected;        /* an event a streaming subscriber never gets */
    bool    overflow;
    sr_sg_t sgs[4];
    int     n_sgs;
} sr_peer_t;

static uint8_t
sr_tag(uint64_t group, uint64_t subgroup, uint64_t object)
{
    return (uint8_t)(0x80u | ((group & 3u) << 5) | ((subgroup & 3u) << 3) |
                     (object & 7u));
}

static void
sr_seq(sr_sg_t *e, char c)
{
    if (e->seq_len + 1 < sizeof(e->seq)) {
        e->seq[e->seq_len++] = c;
        e->seq[e->seq_len] = '\0';
    }
}

static sr_sg_t *
sr_entry(sr_peer_t *p, uint64_t group, uint64_t subgroup)
{
    for (int i = 0; i < p->n_sgs; i++) {
        if (p->sgs[i].group == group && p->sgs[i].subgroup == subgroup) {
            return &p->sgs[i];
        }
    }
    if (p->n_sgs == (int)(sizeof(p->sgs) / sizeof(p->sgs[0]))) {
        p->overflow = true;
        return NULL;
    }
    sr_sg_t *e = &p->sgs[p->n_sgs++];
    memset(e, 0, sizeof(*e));
    e->group = group;
    e->subgroup = subgroup;
    e->bodies_ok = true;
    return e;
}

static int
sr_terminals(const sr_sg_t *e)
{
    return e->fins + e->chunk_resets + e->sg_resets;
}

static void
sr_drain(conn_t *cn, sr_peer_t *p)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        sr_sg_t *e = NULL;
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_OK:
            p->subscribe_ok = true;
            break;
        case MOQ_EVENT_OBJECT_CHUNK: {
            const moq_object_chunk_event_t *oc = &ev.u.object_chunk;
            e = sr_entry(p, oc->group_id, oc->subgroup_id);
            if (e == NULL) {
                break;
            }
            if (sr_terminals(e) > 0) {
                e->after_terminal++;
            }
            if (oc->object_id >= SR_OBJS) {
                e->bodies_ok = false;
                break;
            }
            const uint32_t bit = 1u << oc->object_id;
            if (oc->begin) {
                e->begun |= bit;
            }
            if (oc->chunk != NULL && moq_rcbuf_len(oc->chunk) > 0) {
                const uint8_t want =
                    sr_tag(oc->group_id, oc->subgroup_id, oc->object_id);
                const uint8_t *d = moq_rcbuf_data(oc->chunk);
                for (size_t i = 0; i < moq_rcbuf_len(oc->chunk); i++) {
                    if (d[i] != want) {
                        e->bodies_ok = false;
                    }
                }
                e->chunks[oc->object_id]++;
            }
            if (oc->end) {
                if (oc->terminal == MOQ_OBJECT_TERMINAL_NORMAL) {
                    e->done |= bit;
                    sr_seq(e, (char)('0' + oc->object_id));
                } else if (oc->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    e->chunk_resets++;
                    e->reset_code = oc->error_code;
                    e->reset_object = oc->object_id;
                    sr_seq(e, 'x');
                    sr_seq(e, (char)('0' + oc->object_id));
                } else {
                    p->unexpected = true;   /* no STOP terminal is sent */
                }
            }
            break;
        }
        case MOQ_EVENT_OBJECT_RECEIVED:
            p->unexpected = true;   /* the subscribers stream their objects */
            break;
        case MOQ_EVENT_SUBGROUP_FINISHED:
            e = sr_entry(p, ev.u.subgroup_finished.group_id,
                         ev.u.subgroup_finished.subgroup_id);
            if (e != NULL) {
                if (sr_terminals(e) > 0) {
                    e->after_terminal++;
                }
                e->fins++;
                sr_seq(e, 'F');
            }
            break;
        case MOQ_EVENT_SUBGROUP_RESET: {
            const moq_subgroup_reset_event_t *x = &ev.u.subgroup_reset;
            e = sr_entry(p, x->group_id, x->subgroup_id);
            if (e == NULL) {
                break;
            }
            if (sr_terminals(e) > 0) {
                e->after_terminal++;
            }
            e->sg_resets++;
            sr_seq(e, 'S');
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
            p->dones++;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            p->closed = true;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static const sr_sg_t *
sr_find(const sr_peer_t *p, uint64_t group, uint64_t subgroup)
{
    for (int i = 0; i < p->n_sgs; i++) {
        if (p->sgs[i].group == group && p->sgs[i].subgroup == subgroup) {
            return &p->sgs[i];
        }
    }
    return NULL;
}

static void
sr_subscribe(rig_t *r, conn_t *cn, uint64_t start_group)
{
    moq_bytes_t nsp[1] = { B("demo") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    sc.track_name = B("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.start_group = start_group;
    sc.start_object = 0;
    moq_subscription_t h;
    R_CHECK(r, moq_session_subscribe(cn->peer, &sc, r->now, &h) == MOQ_OK);
}

static moq_subgroup_handle_t
sr_open(rig_t *r, conn_t *pub, moq_subscription_t up, uint64_t group,
        uint64_t subgroup)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    c.subgroup_id = subgroup;
    moq_subgroup_handle_t h;
    memset(&h, 0, sizeof(h));
    R_CHECK(r, moq_session_open_subgroup(pub->peer, up, &c, r->now, &h) ==
                   MOQ_OK);
    return h;
}

/* `n` chunks of `object`, beginning it first (declared `of` chunks long)
 * when `of` is nonzero. */
static void
sr_write(rig_t *r, conn_t *pub, moq_subgroup_handle_t h, uint64_t group,
         uint64_t subgroup, uint64_t object, uint32_t of, uint32_t n)
{
    if (of != 0) {
        R_CHECK(r, moq_session_begin_object(pub->peer, h, object,
                                            (uint64_t)of * SR_CHUNK,
                                            r->now) == MOQ_OK);
    }
    for (uint32_t i = 0; i < n; i++) {
        uint8_t body[SR_CHUNK];
        memset(body, sr_tag(group, subgroup, object), sizeof(body));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) ==
                       MOQ_OK);
        R_CHECK(r, moq_session_write_object_data(pub->peer, h, pl, r->now) ==
                       MOQ_OK);
        moq_rcbuf_decref(pl);
    }
}

/* Run the relay to quiescence, reading each subscriber's events between
 * pumps as an application would: a session surfaces no more than its event
 * queue holds until they are read. */
static void
sr_settle(rig_t *r, conn_t *pub, peer_state_t *pub_ps, conn_t *const subs[4],
          sr_peer_t *const peers[4])
{
    for (int i = 0; i < 8; i++) {
        relay_bind_test_rig_pump(r, 8);
        relay_bind_test_peer_drain(r, pub, pub_ps, true, false, 0, 0);
        for (int k = 0; k < 4; k++) {
            sr_drain(subs[k], peers[k]);
        }
    }
}

/* Every subscriber's view at the end of a row. */
static void
sr_check_end(rig_t *r, sr_row_t row, const char *who, const sr_peer_t *p,
             bool began)
{
    R_CHECK(r, p->subscribe_ok && p->dones == 0 && !p->closed &&
                   !p->unexpected && !p->overflow);
    const sr_sg_t *s0 = sr_find(p, 1, 0);
    const sr_sg_t *s1 = sr_find(p, 1, 1);
    R_CHECK(r, s0 != NULL && s1 != NULL);
    if (s0 == NULL || s1 == NULL) {
        return;
    }
    R_CHECK(r, s0->bodies_ok && s1->bodies_ok);
    R_CHECK(r, s0->chunks[0] == 1 && (s0->done & 1u) != 0);
    R_CHECK(r, (row == SR_REOPEN || s0->after_terminal == 0) &&
                   s1->after_terminal == 0);
    switch (row) {
    case SR_LIVE:
        R_CHECK(r, s0->done == 3u && s0->chunks[1] == 4);
        R_CHECK(r, s0->fins == 1 && s0->chunk_resets == 0 && s0->sg_resets == 0);
        break;
    case SR_IDLE:
        /* Both objects whole, then the reset on its own. */
        R_CHECK(r, s0->done == 3u && s0->chunks[1] == 4);
        R_CHECK(r, s0->fins == 0 && s0->chunk_resets == 0 && s0->sg_resets == 1);
        R_CHECK(r, s0->reset_code == SR_CODE);
        R_CHECK(r, s0->reset_has_last && s0->reset_last == 1);
        break;
    case SR_REOPEN:
        /* Both objects whole and the reset; then object 4 on a new stream,
         * ended by the FIN. */
        if (strcmp(s0->seq, "01S4F") != 0) {
            printf("  %s, subgroup 0: saw \"%s\", want \"01S4F\"\n", who,
                   s0->seq);
        }
        R_CHECK(r, strcmp(s0->seq, "01S4F") == 0);
        R_CHECK(r, s0->done == 0x13u && s0->chunks[1] == 4 && s0->chunks[4] == 2);
        R_CHECK(r, s0->reset_code == SR_CODE);
        R_CHECK(r, s0->reset_has_last && s0->reset_last == 1);
        break;
    case SR_UNBEGUN:
    case SR_SIBLING:
        R_CHECK(r, s0->done == 1u && s0->fins == 0);
        R_CHECK(r, s0->reset_code == SR_CODE);
        if (began) {
            /* Object 1 as far as it came, then its own reset. */
            R_CHECK(r, (s0->begun & 2u) != 0 && s0->chunks[1] == 3);
            R_CHECK(r, s0->chunk_resets == 1 && s0->sg_resets == 0 &&
                           s0->reset_object == 1);
        } else {
            /* Nothing of object 1; the stream's reset after object 0. */
            R_CHECK(r, (s0->begun & 2u) == 0 && s0->chunks[1] == 0);
            R_CHECK(r, s0->sg_resets == 1 && s0->chunk_resets == 0);
            R_CHECK(r, s0->reset_has_last && s0->reset_last == 0);
        }
        break;
    case SR_ROWS:
        break;
    }
    /* The sibling: whole, and ended only where its publisher ended it. */
    if (s1->chunk_resets != 0 || s1->sg_resets != 0 ||
        s1->fins != (row == SR_SIBLING ? 1 : 0) ||
        s1->done != (row == SR_SIBLING ? 0xCu : 0x4u)) {
        printf("  %s, subgroup 1: saw \"%s\", %d chunk(s) of object 2\n", who,
               s1->seq, s1->chunks[2]);
    }
    R_CHECK(r, s1->chunk_resets == 0 && s1->sg_resets == 0);
    if (row == SR_SIBLING) {
        R_CHECK(r, s1->done == 0xCu && s1->fins == 1);
    } else {
        R_CHECK(r, s1->done == 0x4u && s1->fins == 0);
    }
}

static int
sr_case(moq_version_t v, sr_row_t row)
{
    char label[64];
    snprintf(label, sizeof(label), "stream_reset_%s d%d", sr_row_names[row],
             v == MOQ_VERSION_DRAFT_16 ? 16 : v == MOQ_VERSION_DRAFT_18 ? 18 : 22);
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        relay_bind_test_server_streaming_objects = false;
        relay_bind_test_client_streaming_objects = false;
        printf("FAIL: %s: rig create\n", label);
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, v);
    conn_t *c = relay_bind_test_rig_connect(&rig, v);
    conn_t *d = relay_bind_test_rig_connect(&rig, v);
    relay_bind_test_max_actions = SR_ACTIONS;
    conn_t *ah = relay_bind_test_rig_connect(&rig, v);
    conn_t *bh = relay_bind_test_rig_connect(&rig, v);
    relay_bind_test_max_actions = 0;
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    R_CHECK(&rig, pub && c && d && ah && bh);
    if (!(pub && c && d && ah && bh)) {
        relay_bind_test_rig_destroy(&rig);
        printf("FAIL: %s: connect\n", label);
        return 1;
    }
    relay_bind_test_rig_pump(&rig, 8);

    moq_bytes_t nsp[1] = { B("demo") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&rig, moq_session_publish_namespace(pub->peer, &pcfg, rig.now,
                                                &ann) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 8);
    sr_subscribe(&rig, c, 0);
    sr_subscribe(&rig, ah, 0);
    sr_subscribe(&rig, bh, 0);
    relay_bind_test_rig_pump(&rig, 8);
    peer_state_t pub_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);
    R_CHECK(&rig, pub_ps.up_seen);
    relay_bind_test_rig_pump(&rig, 8);

    sr_peer_t cp, ap, bp, dp;
    memset(&cp, 0, sizeof(cp));
    memset(&ap, 0, sizeof(ap));
    memset(&bp, 0, sizeof(bp));
    memset(&dp, 0, sizeof(dp));

    /* Subgroup 0's object 0, whole. */
    moq_subgroup_handle_t h0 = sr_open(&rig, pub, pub_ps.up_sub, 1, 0);
    sr_write(&rig, pub, h0, 1, 0, 0, 1, 1);
    R_CHECK(&rig, moq_session_end_object(pub->peer, h0, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 12);
    sr_drain(c, &cp);
    sr_drain(ah, &ap);
    sr_drain(bh, &bp);
    {
        const sr_sg_t *e[3] = { sr_find(&cp, 1, 0), sr_find(&ap, 1, 0),
                                sr_find(&bp, 1, 0) };
        for (int i = 0; i < 3; i++) {
            R_CHECK(&rig, e[i] != NULL && e[i]->done == 1u);
        }
    }

    /* B is held on the sibling's object while object 1 arrives. */
    bh->frozen = true;
    moq_subgroup_handle_t h1 = sr_open(&rig, pub, pub_ps.up_sub, 1, 1);
    sr_write(&rig, pub, h1, 1, 1, 2, 3, 3);
    R_CHECK(&rig, moq_session_end_object(pub->peer, h1, rig.now) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 12);
    sr_drain(c, &cp);
    sr_drain(ah, &ap);

    /* Object 1 begins: the subscribers it reaches begin it. */
    sr_write(&rig, pub, h0, 1, 0, 1, 4, 1);
    relay_bind_test_rig_pump(&rig, 12);
    sr_drain(c, &cp);
    sr_drain(ah, &ap);
    {
        const sr_sg_t *ce = sr_find(&cp, 1, 0);
        const sr_sg_t *ae = sr_find(&ap, 1, 0);
        R_CHECK(&rig, ce != NULL && (ce->begun & 2u) != 0 && ce->chunks[1] == 1);
        R_CHECK(&rig, ae != NULL && (ae->begun & 2u) != 0 && ae->chunks[1] == 1);
    }

    /* A's send queue fills with object 1's next two chunks. */
    ah->frozen = true;
    sr_write(&rig, pub, h0, 1, 0, 1, 0, 2);
    relay_bind_test_rig_pump(&rig, 12);
    sr_drain(c, &cp);

    switch (row) {
    case SR_LIVE:
        sr_write(&rig, pub, h0, 1, 0, 1, 0, 1);
        R_CHECK(&rig, moq_session_end_object(pub->peer, h0, rig.now) == MOQ_OK);
        R_CHECK(&rig, moq_session_close_subgroup(pub->peer, h0, rig.now) ==
                          MOQ_OK);
        break;
    case SR_IDLE:
    case SR_REOPEN:
        sr_write(&rig, pub, h0, 1, 0, 1, 0, 1);
        R_CHECK(&rig, moq_session_end_object(pub->peer, h0, rig.now) == MOQ_OK);
        relay_bind_test_rig_pump(&rig, 12);
        R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, h0, SR_CODE,
                                                 rig.now) == MOQ_OK);
        if (row == SR_REOPEN) {
            relay_bind_test_rig_pump(&rig, 12);
            sr_drain(c, &cp);
            /* The subgroup again, on a new stream: object 4, then the FIN. */
            moq_subgroup_handle_t h2 = sr_open(&rig, pub, pub_ps.up_sub, 1, 0);
            sr_write(&rig, pub, h2, 1, 0, 4, 2, 2);
            R_CHECK(&rig, moq_session_end_object(pub->peer, h2, rig.now) ==
                              MOQ_OK);
            R_CHECK(&rig, moq_session_close_subgroup(pub->peer, h2, rig.now) ==
                              MOQ_OK);
        }
        break;
    case SR_UNBEGUN:
    case SR_SIBLING:
        R_CHECK(&rig, moq_session_reset_subgroup(pub->peer, h0, SR_CODE,
                                                 rig.now) == MOQ_OK);
        break;
    case SR_ROWS:
        break;
    }
    relay_bind_test_rig_pump(&rig, 12);
    sr_drain(c, &cp);
    if (row == SR_SIBLING) {
        /* The sibling finishes while A's and B's terminals are held. */
        sr_write(&rig, pub, h1, 1, 1, 3, 1, 1);
        R_CHECK(&rig, moq_session_end_object(pub->peer, h1, rig.now) == MOQ_OK);
        R_CHECK(&rig, moq_session_close_subgroup(pub->peer, h1, rig.now) ==
                          MOQ_OK);
        relay_bind_test_rig_pump(&rig, 12);
        sr_drain(c, &cp);
        const sr_sg_t *cs = sr_find(&cp, 1, 1);
        R_CHECK(&rig, cs != NULL && cs->done == 0xCu && cs->fins == 1);
    }
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, true, false, 0, 0);

    if (row == SR_UNBEGUN || row == SR_SIBLING) {
        /* A's reset was refused while its queue held the two chunks: they
         * arrive without it. Once the queue has room, one bind pump takes the
         * reset. */
        sr_drain(ah, &ap);
        relay_bind_test_rig_step_pair(&rig, ah);
        sr_drain(ah, &ap);
        const sr_sg_t *ae = sr_find(&ap, 1, 0);
        R_CHECK(&rig, ae != NULL && ae->chunks[1] == 3 && sr_terminals(ae) == 0);
        relay_bind_test_rig_bind_pump_once(&rig);
        relay_bind_test_rig_step_pair(&rig, ah);
        sr_drain(ah, &ap);
        ae = sr_find(&ap, 1, 0);
        R_CHECK(&rig, ae != NULL && ae->chunk_resets == 1);
    }

    /* D joins after the reset; then A and B run free. */
    sr_subscribe(&rig, d, 1);
    relay_bind_test_rig_pump(&rig, 12);
    ah->frozen = false;
    bh->frozen = false;
    sr_settle(&rig, pub, &pub_ps, (conn_t *[]){ c, ah, bh, d },
              (sr_peer_t *[]){ &cp, &ap, &bp, &dp });

    sr_check_end(&rig, row, "C", &cp, true);
    sr_check_end(&rig, row, "A", &ap, true);
    sr_check_end(&rig, row, "B", &bp, false);
    sr_check_end(&rig, row, "D", &dp, false);
    {
        /* B began the sibling's object, not object 1, before the reset. */
        const sr_sg_t *bs = sr_find(&bp, 1, 1);
        R_CHECK(&rig, bs != NULL && bs->chunks[2] == 3);
    }
    moqr_bind_stats_t bst;
    moqr_bind_get_stats(rig.bind, &bst);
    R_CHECK(&rig, bst.session_errors == 0);
    R_CHECK(&rig, !pub_ps.session_closed);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) {
        printf("PASS: %s\n", label);
    } else {
        printf("FAIL: %s: %d check(s)\n", label, rig.failures);
    }
    return rig.failures;
}

int
main(int argc, char **argv)
{
    static const moq_version_t drafts[] = { MOQ_VERSION_DRAFT_16,
                                            MOQ_VERSION_DRAFT_18,
                                            MOQ_VERSION_DRAFT_22 };
    int failures = 0;
    for (int row = 0; row < SR_ROWS; row++) {
        if (argc == 2 && strcmp(argv[1], sr_row_names[row]) != 0) {
            continue;
        }
        for (size_t i = 0; i < sizeof(drafts) / sizeof(drafts[0]); i++) {
            failures += sr_case(drafts[i], (sr_row_t)row);
        }
    }
    return failures == 0 ? 0 : 1;
}
