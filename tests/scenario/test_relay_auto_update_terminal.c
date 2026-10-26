/*
 * A downstream request the relay's session ends on its own -- an update it
 * refuses automatically (here: two identical authorization tokens, refused
 * with REQUEST_ERROR 0x4, then PUBLISH_DONE UPDATE_FAILED; draft-22 section
 * 9.5.1) -- while the binding still owns it. The session gives no event for
 * that (session.h: "the handle is then stale and no event names the refused
 * update"), so the binding learns of it only when the track's source later
 * ends and it tries to send its own terminal. That must retire the binding's
 * owner without closing the subscriber's session: the session's terminal
 * already went out (or is owed and goes out after the owed resets).
 *
 * Both kinds of downstream request (an ordinary SUBSCRIBE, and a PUBLISH the
 * relay offered on SUBSCRIBE_TRACKS), each in two states at the source end:
 *   HELD  the refusal met real action pressure: the open subgroup's reset was
 *         owed (MOQ_SG_RESET_OWED) and the terminal held (MOQ_*_DONE_HELD),
 *         both observed before anything drained;
 *   FREED the refusal went out at once and the session freed the request.
 * Plus the peer's own cancellation under the same pressure (cancellation
 * semantics: no terminal is manufactured), and slot reuse: a new request the
 * same subscriber makes afterwards is untouched by the old one's end. After
 * a refusal, data keeps coming on the old subgroups and in a new group:
 * nothing more of it goes to the watcher. On a freed request the new one
 * takes the freed request's own session slot (a later generation), and is
 * served after the old one's end.
 *
 * A stream's own end is never taken for its request's: one subgroup the
 * relay's session ended by itself -- a local reset (AU_CHILD), or the
 * watcher's STOP_SENDING (AU_CHILD_STOP) -- while the request lives on.
 * The source later seals that subgroup (a FIN; an upstream reset) and ends
 * normally: the request keeps receiving its other subgroups and a new one
 * that reuses the ended child's session slot. A local reset releases its
 * binding slot at the seal; a peer STOP retains the key until eviction or
 * request teardown to prevent reopening (draft-22 section 11.3.2). Neither
 * sends a FIN or second reset, and the request ends with the ordinary terminal.
 * A stream mid-object refuses the seal's FIN with WRONG_STATE, which is no
 * stale stream: the relay fails closed (AU_MIDOBJ).
 *
 * An update answer the relay holds when the session ends the request
 * (AU_HELD_ANSWER, an ordinary SUBSCRIBE from a draft-18 watcher) is never
 * given afterwards: no acknowledgement reaches the watcher, the held answer
 * is not left owed, and the request is cleaned up once. A confirmation failure when the relay retires an ended request's
 * outstanding delivery fails the session closed (AU_GATE_FAULT).
 *
 * A request the peer cancels gives back its subgroup slots: a subscriber
 * that cancels and subscribes again, cycle after cycle, on one connection,
 * more often than the binding has subgroup slots, is served every time,
 * beside a live request it keeps (au_cycles).
 *
 * Draft-22 sessions over the production binding and a real simulated
 * transport; white-box reads of the relay's session state only observe.
 */
#define COMPLETION_BIND_SOURCE "../../relay/src/bind/moqr_bind.c"
#include COMPLETION_BIND_SOURCE
#include "../../relay/tests/relay_session_binding_harness.c"
#define main relay_session_binding_d22_main
#include "../../relay/tests/test_relay_session_binding_d22.c"
#undef main
#include "../../core/src/session/session_internal.h"

enum {
    AU_HELD, AU_FREED, AU_CANCEL, AU_CHILD, AU_CHILD_STOP, AU_MIDOBJ,
    AU_HELD_ANSWER, AU_GATE_FAULT, AU_MODES
};

static const char *const au_mode_name[AU_MODES] = {
    "held", "freed", "cancel", "child", "child-stop", "mid-object",
    "held-answer", "gate-fault",
};
enum { AU_SG = 4 };   /* open subgroups toward the watcher */

/* What the watching peer saw, in order. */
typedef struct au_obs {
    int      objects;
    int      sub_dones, pub_finished;
    uint64_t last_status;
    int      resets;
    uint64_t reset_code;
    int      reset_before_terminal;   /* resets seen before any terminal */
    bool     closed;
    int      update_oks;
    bool     sub_ok;
    int      pubs_accepted;
    moq_publication_t pub;
} au_obs_t;

static void
au_drain(rig_t *r, conn_t *c, au_obs_t *o)
{
    moq_event_t evs[16];
    for (int guard = 0; guard < 64; guard++) {
        size_t n = moq_session_poll_events(c->peer, evs, 16);
        if (n == 0) break;
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            switch (ev->kind) {
            case MOQ_EVENT_PUBLISH_REQUEST: {
                moq_accept_publish_cfg_t a;
                moq_accept_publish_cfg_init(&a);
                R_CHECK(r, moq_session_accept_publish(
                               c->peer, ev->u.publish_request.pub, &a,
                               r->now) == MOQ_OK);
                o->pub = ev->u.publish_request.pub;
                o->pubs_accepted++;
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_OK: o->sub_ok = true; break;
            case MOQ_EVENT_OBJECT_RECEIVED: o->objects++; break;
            case MOQ_EVENT_SUBGROUP_RESET:
                o->resets++;
                o->reset_code = ev->u.subgroup_reset.error_code;
                if (o->sub_dones + o->pub_finished == 0)
                    o->reset_before_terminal++;
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                o->sub_dones++;
                o->last_status = ev->u.subscribe_done.status_code;
                break;
            case MOQ_EVENT_PUBLISH_FINISHED:
                o->pub_finished++;
                o->last_status = ev->u.publish_finished.status_code;
                break;
            case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK:
            case MOQ_EVENT_PUBLICATION_UPDATE_OK:
                o->update_oks++;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                o->closed = true;
                printf("  watcher session closed: code=0x%llx reason=%.*s\n",
                       (unsigned long long)ev->u.closed.code,
                       (int)ev->u.closed.reason.len,
                       (const char *)ev->u.closed.reason.data);
                break;
            default: break;
            }
            moq_event_cleanup(ev);
        }
    }
}

static size_t
au_queued(const moq_session_t *s)
{
    return s->action_tail - s->action_head;
}

/* Relay-side subgroups toward the watcher in `state`, for observation. */
static int
au_sg_count(const moq_session_t *s, int state)
{
    int n = 0;
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next)
        n += (int)s->subgroups[i].state == state;
    return n;
}

/* Observed on the relay's session the moment the watcher's input has been
 * handled -- inside the transport step, before the relay's own queued output
 * is taken (which would let it pay what it owes). */
typedef struct au_probe {
    moq_session_t *rs;
    bool armed;
    int  max_owed;
    bool held_seen;
    int  resets_sent;          /* RESET_DATA the relay put on the wire */
    int  resets_other_code;    /* ... with a code other than CANCELLED */
} au_probe_t;
static au_probe_t au_probe;

static bool
au_any_held(const moq_session_t *s)
{
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_DONE_HELD) return true;
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_DONE_HELD) return true;
    return false;
}

static void
au_probe_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    au_probe_t *p = ctx;
    if (r->kind == MOQ_SIM_TRACE_ACTION && r->from == MOQ_PERSPECTIVE_SERVER &&
        r->action_kind == MOQ_ACTION_RESET_DATA) {
        p->resets_sent++;
        if (r->error_code != 0x1) p->resets_other_code++;
        return;
    }
    if (!p->armed || p->rs == NULL || r->kind != MOQ_SIM_TRACE_INPUT ||
        r->to != MOQ_PERSPECTIVE_SERVER)
        return;
    int owed = au_sg_count(p->rs, MOQ_SG_RESET_OWED);
    if (owed > p->max_owed) p->max_owed = owed;
    if (au_any_held(p->rs)) p->held_seen = true;
}

static void
au_pump(rig_t *r, conn_t *ws, au_obs_t *wo, conn_t *sib, au_obs_t *so, int n)
{
    for (int k = 0; k < n; k++) {
        relay_bind_test_rig_pump(r, 1);
        au_drain(r, ws, wo);
        if (sib != NULL) au_drain(r, sib, so);
    }
}

static int
au_dsub_count(moqr_bind_t *b, const moq_session_t *rs)
{
    b_conn_t *cn = conn_by_session(b, rs);
    int n = 0;
    if (cn) for (uint32_t i = 0; i < b->n_dsubs; i++) n += cn->dsubs[i].used;
    return n;
}

static b_dsub_t *
au_only_dsub(moqr_bind_t *b, const moq_session_t *rs)
{
    b_conn_t *cn = conn_by_session(b, rs);
    if (cn) for (uint32_t i = 0; i < b->n_dsubs; i++)
        if (cn->dsubs[i].used) return &cn->dsubs[i];
    return NULL;
}

static int
au_case(bool publication, int mode)
{
    const bool child = mode == AU_CHILD || mode == AU_CHILD_STOP;
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) return 1;
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *sib = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    /* Under pressure the watcher's pair runs with a three-slot action queue:
     * with AU_SG subgroups open, the teardown's resets cannot all fit beside
     * the slots it reserves, so some are owed. */
    relay_bind_test_max_actions = (mode == AU_HELD || mode == AU_CANCEL) ? 3 : 0;
    memset(&au_probe, 0, sizeof(au_probe));
    relay_bind_test_trace_fn = au_probe_fn;
    relay_bind_test_trace_ctx = &au_probe;
    conn_t *ws = relay_bind_test_rig_connect(&rig, mode == AU_HELD_ANSWER ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22);
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    relay_bind_test_max_actions = 0;
    if (ws != NULL) au_probe.rs = ws->rsess;
    R_CHECK(&rig, pub && sib && ws);
    relay_bind_test_rig_pump(&rig, 6);

    moq_bytes_t part = B("a");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    pc.track_name = B("t1");
    moq_publication_t pubh = { 0 };
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) == MOQ_OK);
    /* A second track, so the watcher's later request (slot reuse) is
     * accepted and owned. */
    moq_publication_t pubh2 = { 0 };
    moq_bytes_t part2 = B("b");   /* outside the watcher's discovery prefix */
    pc.track_namespace = (moq_namespace_t){ .parts = &part2, .count = 1 };
    pc.track_name = B("t2");
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh2) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);

    /* The gate-fault row runs without the sibling, so the one injected
     * confirmation failure can only meet the watcher's connection. */
    conn_t *sibp = mode == AU_GATE_FAULT ? NULL : sib;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    sc.track_name = B("t1");
    moq_subscription_t sib_h, ws_h = { 0 };
    if (sibp != NULL)
        R_CHECK(&rig, moq_session_subscribe(sib->peer, &sc, rig.now, &sib_h) ==
                          MOQ_OK);
    if (publication) {
        moq_subscribe_tracks_cfg_t tc;
        moq_subscribe_tracks_cfg_init(&tc);
        tc.track_namespace_prefix = (moq_namespace_t){ .parts = &part, .count = 1 };
        tc.has_forward = true;
        tc.forward = true;
        moq_track_sub_handle_t th;
        R_CHECK(&rig, moq_session_subscribe_tracks(ws->peer, &tc, rig.now, &th) ==
                          MOQ_OK);
    } else {
        R_CHECK(&rig, moq_session_subscribe(ws->peer, &sc, rig.now, &ws_h) == MOQ_OK);
    }
    au_obs_t wo, so;
    memset(&wo, 0, sizeof(wo));
    memset(&so, 0, sizeof(so));
    au_pump(&rig, ws, &wo, sibp, &so, 16);
    R_CHECK(&rig, sibp == NULL || so.sub_ok);
    R_CHECK(&rig, publication ? wo.pubs_accepted == 1 : wo.sub_ok);
    const b_dsub_t *old_ds = au_only_dsub(rig.bind, ws->rsess);
    R_CHECK(&rig, old_ds != NULL);
    const uint64_t old_raw = old_ds ? old_ds->sub_raw : 0;

    /* The origin opens AU_SG subgroups (one per group) and sends Object 0 on
     * each, leaving them open; every one reaches both subscribers. */
    moq_subgroup_handle_t sgh[AU_SG];
    uint8_t body[4] = { 1, 2, 3, 4 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
    for (int g = 0; g < AU_SG; g++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = (uint64_t)g;
        DP_RETRY(&rig, moq_session_open_pub_subgroup(pub->peer, pubh, &sgc,
                                                     rig.now, &sgh[g]));
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh[g], 0, pl, rig.now));
        au_pump(&rig, ws, &wo, sibp, &so, 6);
    }
    au_pump(&rig, ws, &wo, sibp, &so, 12);
    R_CHECK(&rig, wo.objects == AU_SG &&
                      (sibp == NULL || so.objects == AU_SG));
    R_CHECK(&rig, au_sg_count(ws->rsess, MOQ_SG_OPEN) == AU_SG);
    R_CHECK(&rig, au_queued(ws->rsess) == 0);
    moq_auth_token_t dup[2] = {
        { .token_type = 1, .token_value = B("same") },
        { .token_type = 1, .token_value = B("same") },
    };
    /* The child's relay-side handle, kept for the slot-reuse check. */
    moq_subgroup_handle_t dh = { 0 };
    if (child) {
        R_CHECK(&rig, moqr_bind_debug_sg_handle(rig.bind, ws->rsess, 0, 0, &dh));
    }
    if (mode == AU_HELD_ANSWER) {
        /* A first, valid update the relay decides but cannot give yet: its
         * answer is held on the request's tracking slot. */
        moqr_bind_debug_block_update_answers(1 << 20);
        if (publication) {
            moq_publication_update_cfg_t uc;
            moq_publication_update_cfg_init(&uc);
            uc.has_forward = true;
            uc.forward = false;
            R_CHECK(&rig, moq_session_update_publication(ws->peer, wo.pub, &uc,
                                                         rig.now) == MOQ_OK);
        } else {
            moq_subscription_update_cfg_t uc;
            moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
            uc.has_forward = true;
            uc.forward = false;
            R_CHECK(&rig, moq_session_update_subscription(ws->peer, ws_h, &uc,
                                                          rig.now) == MOQ_OK);
        }
        au_pump(&rig, ws, &wo, sibp, &so, 6);
        const b_dsub_t *hd = au_only_dsub(rig.bind, ws->rsess);
        R_CHECK(&rig, hd != NULL && hd->upd_answer != B_UPD_NONE);
        R_CHECK(&rig, rig.bind->upd_answers_held == 1);
        R_CHECK(&rig, wo.update_oks == 0);
        /* The watcher sends its next update before that answer arrives:
         * draft-18 sets no limit on outstanding updates and lets the
         * receiver coalesce several (section 10.9), so this row runs the
         * watcher on draft-18 (on draft-22 this relay
         * advertises MAX_REQUEST_UPDATES 1, and a second update while one
         * is outstanding closes the session, section 9.1.7). This client
         * keeps one update outstanding, so its own latch is lifted for the
         * next call. */
        if (publication) {
            int ps = pub_resolve_handle(ws->peer, wo.pub);
            R_CHECK(&rig, ps >= 0);
            if (ps >= 0) ws->peer->publishes[ps].update_pending = false;
        } else {
            int ss = sub_resolve_handle(ws->peer, ws_h);
            R_CHECK(&rig, ss >= 0);
            if (ss >= 0) ws->peer->subs[ss].update_pending = false;
        }
    }
    if (mode == AU_CHILD) {
        R_CHECK(&rig, moq_session_reset_subgroup(ws->rsess, dh, 0x2, rig.now) ==
                          MOQ_OK);
    } else if (mode == AU_CHILD_STOP) {
        /* The watcher's STOP_SENDING on that stream, as the transport
         * reports it: the relay's session answers with its own reset. */
        uint32_t slot = moq_handle_slot(dh._opaque);
        R_CHECK(&rig, slot < ws->rsess->sg_cap);
        moq_stream_ref_t ref = ws->rsess->subgroups[slot].stream_ref;
        R_CHECK(&rig, moq_session_on_data_stop(ws->rsess, ref, 0x0, rig.now) ==
                          MOQ_OK);
    } else if (mode == AU_MIDOBJ) {
        /* Group 1's downstream stream is left mid-object: an object begun
         * on it and not finished. */
        moq_subgroup_handle_t mh;
        R_CHECK(&rig, moqr_bind_debug_sg_handle(rig.bind, ws->rsess, 1, 0, &mh));
        R_CHECK(&rig, moq_session_begin_object(ws->rsess, mh, 1, sizeof(body),
                                               rig.now) == MOQ_OK);
    } else if (mode == AU_CANCEL) {
        if (publication) {
            R_CHECK(&rig, moq_session_cancel_publication(
                              ws->peer, wo.pub, MOQ_REQUEST_ERROR_UNINTERESTED,
                              rig.now) == MOQ_OK);
        } else {
            R_CHECK(&rig, moq_session_unsubscribe(ws->peer, ws_h, rig.now) ==
                              MOQ_OK);
        }
    } else if (publication) {
        moq_publication_update_cfg_t uc;
        moq_publication_update_cfg_init(&uc);
        uc.auth_tokens = dup;
        uc.auth_token_count = 2;
        R_CHECK(&rig, moq_session_update_publication(ws->peer, wo.pub, &uc,
                                                     rig.now) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
        uc.auth_tokens = dup;
        uc.auth_token_count = 2;
        R_CHECK(&rig, moq_session_update_subscription(ws->peer, ws_h, &uc,
                                                      rig.now) == MOQ_OK);
    }
    /* Deliver the request; the probe samples the relay's state as it is
     * handled, before the relay's queued output is taken. */
    au_probe.armed = true;
    for (int k = 0; k < 2; k++) {
        relay_bind_test_rig_pump(&rig, 1);
        au_drain(&rig, ws, &wo);
    }
    au_probe.armed = false;
    if (mode == AU_HELD || mode == AU_CANCEL) {
        printf("  %s: relay resets owed when the request was handled: %d of "
               "%d; terminal held: %d\n", au_mode_name[mode],
               au_probe.max_owed, AU_SG, (int)au_probe.held_seen);
        R_CHECK(&rig, au_probe.max_owed > 0);
        if (mode == AU_HELD) R_CHECK(&rig, au_probe.held_seen);
    } else {
        R_CHECK(&rig, au_probe.max_owed == 0 && !au_probe.held_seen);
    }
    if (mode == AU_HELD_ANSWER) {
        /* The session ended the request with the answer still held; the
         * relay may now try to give it. */
        moqr_bind_debug_block_update_answers(0);
    }

    if (mode == AU_GATE_FAULT) {
        /* The next confirmation fails: the first is the retirement of the
         * ended request's outstanding delivery, before anything is written
         * for it. */
        moqr_bind_debug_fail_confirm(1);
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh[1], 1, pl, rig.now));
        au_pump(&rig, ws, &wo, NULL, &so, 12);
        int left = atomic_load(&bind_dbg_fail_confirms);
        moqr_bind_debug_fail_confirm(0);
        moqr_bind_stats_t gbs;
        moqr_bind_get_stats(rig.bind, &gbs);
        R_CHECK(&rig, left == 0);
        R_CHECK(&rig, wo.closed);
        R_CHECK(&rig, gbs.session_errors == 1);
        R_CHECK(&rig, conn_by_session(rig.bind, ws->rsess) == NULL);
        moq_rcbuf_decref(pl);
        relay_bind_test_rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
        int gf = rig.failures;
        printf("%s: auto_update_terminal(%s, %s): injected failure consumed=%d "
               "watcher closed=%d session errors=%llu\n",
               gf == 0 ? "PASS" : "FAIL",
               publication ? "publication" : "subscription", au_mode_name[mode],
               left == 0, (int)wo.closed,
               (unsigned long long)gbs.session_errors);
        return gf;
    }
    if (mode == AU_MIDOBJ) {
        /* The source FINs group 1: the seal's FIN meets a stream mid-object,
         * which the session refuses with WRONG_STATE -- not a stale stream,
         * so the relay neither swallows it nor sends a FIN: it fails closed. */
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh[1], rig.now));
        au_pump(&rig, ws, &wo, sibp, &so, 16);
        moqr_bind_stats_t mbs;
        moqr_bind_get_stats(rig.bind, &mbs);
        R_CHECK(&rig, wo.closed);
        R_CHECK(&rig, conn_by_session(rig.bind, ws->rsess) == NULL);
        R_CHECK(&rig, mbs.session_errors == 1);
        R_CHECK(&rig, !so.closed);
        moq_rcbuf_decref(pl);
        relay_bind_test_rig_destroy(&rig);
        R_CHECK(&rig, a.live == 0);
        int mf = rig.failures;
        printf("%s: auto_update_terminal(%s, %s): watcher closed=%d "
               "session errors=%llu\n",
               mf == 0 ? "PASS" : "FAIL",
               publication ? "publication" : "subscription", au_mode_name[mode],
               (int)wo.closed,
               (unsigned long long)mbs.session_errors);
        return mf;
    }

    /* Data keeps coming: Object 1 on every subgroup but the one a child
     * reset ended, and a new group. */
    int before = wo.objects;
    for (int g = 0; g < AU_SG; g++) {
        if (child && g == 0) continue;
        DP_RETRY(&rig, moq_session_write_object(pub->peer, sgh[g], 1, pl, rig.now));
    }
    moq_subgroup_handle_t extra;
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = AU_SG;
        DP_RETRY(&rig, moq_session_open_pub_subgroup(pub->peer, pubh, &sgc,
                                                     rig.now, &extra));
        DP_RETRY(&rig, moq_session_write_object(pub->peer, extra, 0, pl, rig.now));
    }
    au_pump(&rig, ws, &wo, sibp, &so, 16);
    if (child) {
        /* The live request got every new object (AU_SG - 1 + the new
         * group's), kept its owner, and its session is untouched. The new
         * group's stream took the ended child's session slot, as a later
         * generation. */
        R_CHECK(&rig, wo.objects == before + AU_SG);
        R_CHECK(&rig, !wo.closed);
        R_CHECK(&rig, au_dsub_count(rig.bind, ws->rsess) == 1);
        moq_subgroup_handle_t xh = { 0 };
        R_CHECK(&rig, moqr_bind_debug_sg_handle(rig.bind, ws->rsess, AU_SG, 0,
                                                &xh));
        R_CHECK(&rig, moq_handle_slot(xh._opaque) == moq_handle_slot(dh._opaque));
        R_CHECK(&rig, xh._opaque != dh._opaque);
        R_CHECK(&rig, au_probe.resets_sent == 1);

        /* The source seals the child: a FIN after a local reset, an
         * upstream reset after the watcher's STOP_SENDING. */
        if (mode == AU_CHILD) {
            DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh[0], rig.now));
        } else {
            DP_RETRY(&rig, moq_session_reset_subgroup(pub->peer, sgh[0], 0x3,
                                                      rig.now));
        }
        au_pump(&rig, ws, &wo, sibp, &so, 16);
        /* A peer STOP keeps the exact stopped key across source resets:
         * the source may publish more on that key without renewed downstream
         * interest. A local reset has no such suppression record. Neither
         * touches the stream that reused the old session slot. */
        moq_subgroup_handle_t retained = { 0 };
        bool has_retained = moqr_bind_debug_sg_handle(rig.bind, ws->rsess, 0, 0,
                                                       &retained);
        R_CHECK(&rig, has_retained == (mode == AU_CHILD_STOP));
        if (has_retained) {
            R_CHECK(&rig, moq_subgroup_eq(retained, dh));
            b_conn_t *stopped_cn = conn_by_session(rig.bind, ws->rsess);
            int stopped_count = 0;
            if (stopped_cn != NULL) {
                for (uint32_t i = 0; i < rig.bind->n_sgs; i++) {
                    const b_sg_t *sg = &stopped_cn->sgs[i];
                    if (!sg->used || !moq_subgroup_eq(sg->h, dh)) continue;
                    R_CHECK(&rig, sg->stopped && sg->sub_raw == old_raw &&
                                   sg->group == 0 && sg->subgroup == 0);
                    stopped_count++;
                }
            }
            R_CHECK(&rig, stopped_count == 1);
        }
        R_CHECK(&rig, !wo.closed);
        R_CHECK(&rig, au_dsub_count(rig.bind, ws->rsess) == 1);
        R_CHECK(&rig, au_probe.resets_sent == 1);
        moqr_bind_stats_t cbs;
        moqr_bind_get_stats(rig.bind, &cbs);
        R_CHECK(&rig, cbs.session_errors == 0);
        R_CHECK(&rig, au_sg_count(ws->rsess, MOQ_SG_OPEN) == AU_SG);
        int mid = wo.objects;
        DP_RETRY(&rig, moq_session_write_object(pub->peer, extra, 1, pl, rig.now));
        au_pump(&rig, ws, &wo, sibp, &so, 12);
        R_CHECK(&rig, wo.objects == mid + 1);
    } else {
        R_CHECK(&rig, wo.objects == before);   /* nothing more after its end */
    }
    if (mode == AU_HELD_ANSWER) {
        /* The held answer was never given and is no longer owed. */
        R_CHECK(&rig, wo.update_oks == 0);
        R_CHECK(&rig, rig.bind->upd_answers_held == 0);
    }

    /* Slot reuse: the watcher makes a new, unrelated request; the old
     * request's end must never touch it. A freed request's session slot is
     * taken by the new one (the same kind of request, so the same pool). */
    int pubs_before = wo.pubs_accepted;
    moq_subscription_t ws_new = { 0 };
    moq_bytes_t part_b = B("b");
    sc.track_namespace = (moq_namespace_t){ .parts = &part_b, .count = 1 };
    sc.track_name = B("t2");
    if (mode != AU_CANCEL) {
        if (publication) {
            moq_subscribe_tracks_cfg_t tc;
            moq_subscribe_tracks_cfg_init(&tc);
            tc.track_namespace_prefix =
                (moq_namespace_t){ .parts = &part_b, .count = 1 };
            tc.has_forward = true;
            tc.forward = true;
            moq_track_sub_handle_t th2;
            DP_RETRY(&rig, moq_session_subscribe_tracks(ws->peer, &tc, rig.now,
                                                        &th2));
        } else {
            DP_RETRY(&rig, moq_session_subscribe(ws->peer, &sc, rig.now, &ws_new));
        }
        au_pump(&rig, ws, &wo, sibp, &so, 12);
        if (publication) R_CHECK(&rig, wo.pubs_accepted == pubs_before + 1);
    }
    (void)ws_new;
    uint64_t new_raw = 0;
    if (mode != AU_CANCEL) {
        b_conn_t *ncn = conn_by_session(rig.bind, ws->rsess);
        if (ncn) for (uint32_t i = 0; i < rig.bind->n_dsubs; i++)
            if (ncn->dsubs[i].used && ncn->dsubs[i].sub_raw != old_raw)
                new_raw = ncn->dsubs[i].sub_raw;
        R_CHECK(&rig, new_raw != 0);
    }
    if (mode == AU_FREED) {
        printf("  freed: old request slot %u gen %u, new request slot %u gen %u\n",
               moq_handle_slot(old_raw), moq_handle_generation(old_raw),
               moq_handle_slot(new_raw), moq_handle_generation(new_raw));
        R_CHECK(&rig, moq_handle_slot(new_raw) == moq_handle_slot(old_raw));
        R_CHECK(&rig, new_raw != old_raw);
    }

    /* The source ends: the subgroups FIN, then the publication finishes. */
    for (int g = 0; g < AU_SG; g++) {
        if (child && g == 0) continue;   /* sealed above */
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sgh[g], rig.now));
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, extra, rig.now));
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    fc.status_code = 0x2;
    fc.stream_count = AU_SG + 1;
    DP_RETRY(&rig, moq_session_finish_publish(pub->peer, pubh, &fc, rig.now));
    au_pump(&rig, ws, &wo, sibp, &so, 40);

    /* The new request is still served after the old one's end. */
    int new_before = wo.objects;
    if (mode != AU_CANCEL) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        moq_subgroup_handle_t t2sg;
        DP_RETRY(&rig, moq_session_open_pub_subgroup(pub->peer, pubh2, &sgc,
                                                     rig.now, &t2sg));
        DP_RETRY(&rig, moq_session_write_object(pub->peer, t2sg, 0, pl, rig.now));
        au_pump(&rig, ws, &wo, sibp, &so, 16);
        R_CHECK(&rig, wo.objects == new_before + 1);
    }
    moq_rcbuf_decref(pl);

    /* The watcher: its session open; one terminal -- the session's
     * UPDATE_FAILED -- on a refused update, none on a cancellation; its open
     * subgroup reset exactly once, with CANCELLED, before any terminal. */
    R_CHECK(&rig, !wo.closed);
    R_CHECK(&rig, wo.update_oks == 0);
    int terminals = wo.sub_dones + wo.pub_finished;
    if (mode == AU_CANCEL) {
        R_CHECK(&rig, terminals == 0);
    } else if (child) {
        R_CHECK(&rig, terminals == 1);
        R_CHECK(&rig, wo.last_status == 0x2);   /* the ordinary end */
    } else {
        R_CHECK(&rig, terminals == 1);
        R_CHECK(&rig, wo.last_status == 0x8);   /* UPDATE_FAILED */
    }
    /* Each open subgroup reset exactly once on the wire, with CANCELLED --
     * owed ones included, none sent twice. A subscriber that kept the request
     * sees every reset before its terminal; one that cancelled it has stopped
     * reading those streams. */
    if (child) {
        /* Only the child the session ended itself was reset, with its own
         * code; every other stream ended with its FIN. */
        R_CHECK(&rig, au_probe.resets_sent == 1);
        R_CHECK(&rig, wo.resets == 1);
    } else {
        R_CHECK(&rig, au_probe.resets_sent == AU_SG);
        R_CHECK(&rig, au_probe.resets_other_code == 0);
    }
    if (mode != AU_CANCEL && !child) {
        R_CHECK(&rig, wo.resets == AU_SG);
        R_CHECK(&rig, wo.reset_code == 0x1);
        R_CHECK(&rig, wo.reset_before_terminal == AU_SG);
    }
    /* The sibling: everything, then the ordinary end. */
    R_CHECK(&rig, !so.closed);
    R_CHECK(&rig, so.sub_dones == 1 && so.last_status == 0x2);
    R_CHECK(&rig, so.objects == 2 * AU_SG + 1);
    R_CHECK(&rig, so.resets == (mode == AU_CHILD_STOP ? 1 : 0));
    /* Owners released: no binding owner and no relay subscription left for
     * the watcher's old request, and none of its subgroup slots; the new
     * request still owned. */
    b_conn_t *cn = conn_by_session(rig.bind, ws->rsess);
    int dsubs = au_dsub_count(rig.bind, ws->rsess);
    int old_sgs = 0;
    if (cn) for (uint32_t i = 0; i < rig.bind->n_sgs; i++)
        old_sgs += cn->sgs[i].used && cn->sgs[i].sub_raw == old_raw;
    R_CHECK(&rig, dsubs == (mode == AU_CANCEL ? 0 : 1));
    R_CHECK(&rig, old_sgs == 0);
    R_CHECK(&rig, rig.bind->upd_answers_held == 0);
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);

    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    printf("%s: auto_update_terminal(%s, %s): watcher closed=%d terminals=%d "
           "status=0x%llx resets seen=%d sent=%d update oks=%d; sibling "
           "dones=%d; session errors=%llu\n",
           f == 0 ? "PASS" : "FAIL", publication ? "publication" : "subscription",
           au_mode_name[mode], (int)wo.closed, terminals,
           (unsigned long long)wo.last_status, wo.resets, au_probe.resets_sent,
           wo.update_oks, so.sub_dones, (unsigned long long)bs.session_errors);
    return f;
}

/* Cancel and subscribe again, AU_CYCLES times, on one connection whose
 * binding has AU_CYC_SLOTS subgroup slots, beside a live request on another
 * track that keeps one of them. Each request gets an object on a new group
 * (a slot), then is cancelled; the next takes the cancelled one's session
 * slot as a later generation, and only then does the source seal the old
 * group. Every cycle: the cancelled request holds no slot, the new one is
 * served, the live request keeps its own. */
enum { AU_CYC_SLOTS = 3, AU_CYCLES = 2 * AU_CYC_SLOTS + 1 };

static b_dsub_t *
au_dsub_other(moqr_bind_t *b, const moq_session_t *rs, uint64_t not_raw)
{
    b_conn_t *cn = conn_by_session(b, rs);
    if (cn) for (uint32_t i = 0; i < b->n_dsubs; i++)
        if (cn->dsubs[i].used && cn->dsubs[i].sub_raw != not_raw)
            return &cn->dsubs[i];
    return NULL;
}

static int
au_sgs_used(moqr_bind_t *b, const moq_session_t *rs, uint64_t raw, bool only)
{
    b_conn_t *cn = conn_by_session(b, rs);
    int n = 0;
    if (cn) for (uint32_t i = 0; i < b->n_sgs; i++)
        n += cn->sgs[i].used && (!only || cn->sgs[i].sub_raw == raw);
    return n;
}

static int
au_cycles(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    relay_bind_test_max_open_subgroups = AU_CYC_SLOTS;
    moqr_result_t crc = relay_bind_test_rig_create(&rig, &a);
    relay_bind_test_max_open_subgroups = 0;
    if (crc != MOQR_OK) return 1;
    R_CHECK(&rig, rig.bind->n_sgs == AU_CYC_SLOTS);
    conn_t *pub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *sib = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    conn_t *ws = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, pub && sib && ws);
    relay_bind_test_rig_pump(&rig, 6);

    moq_bytes_t part = B("a"), part_b = B("b");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    pc.track_name = B("t1");
    moq_publication_t pubh = { 0 }, pubh2 = { 0 };
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh) == MOQ_OK);
    pc.track_namespace = (moq_namespace_t){ .parts = &part_b, .count = 1 };
    pc.track_name = B("t2");
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &pubh2) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);

    moq_subscribe_cfg_t sc1, sc2;
    moq_subscribe_cfg_init(&sc1);
    sc1.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    sc1.track_name = B("t1");
    moq_subscribe_cfg_init(&sc2);
    sc2.track_namespace = (moq_namespace_t){ .parts = &part_b, .count = 1 };
    sc2.track_name = B("t2");
    au_obs_t wo, so;
    memset(&wo, 0, sizeof(wo));
    memset(&so, 0, sizeof(so));
    moq_subscription_t sib_h, live_h, ws_h;
    R_CHECK(&rig, moq_session_subscribe(sib->peer, &sc1, rig.now, &sib_h) == MOQ_OK);
    R_CHECK(&rig, moq_session_subscribe(ws->peer, &sc2, rig.now, &live_h) ==
                      MOQ_OK);
    au_pump(&rig, ws, &wo, sib, &so, 16);
    const b_dsub_t *lds = au_only_dsub(rig.bind, ws->rsess);
    R_CHECK(&rig, lds != NULL);
    const uint64_t live_raw = lds ? lds->sub_raw : 0;

    uint8_t body[4] = { 5, 6, 7, 8 };
    moq_rcbuf_t *pl = NULL;
    R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
    /* The live request's stream stays open throughout: one slot is its. */
    moq_subgroup_handle_t live_sg;
    {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        DP_RETRY(&rig, moq_session_open_pub_subgroup(pub->peer, pubh2, &sgc,
                                                     rig.now, &live_sg));
        DP_RETRY(&rig, moq_session_write_object(pub->peer, live_sg, 0, pl,
                                                rig.now));
    }
    au_pump(&rig, ws, &wo, sib, &so, 12);
    R_CHECK(&rig, wo.objects == 1);
    R_CHECK(&rig, au_sgs_used(rig.bind, ws->rsess, live_raw, true) == 1);

    R_CHECK(&rig, moq_session_subscribe(ws->peer, &sc1, rig.now, &ws_h) == MOQ_OK);
    au_pump(&rig, ws, &wo, sib, &so, 12);
    const b_dsub_t *cds = au_dsub_other(rig.bind, ws->rsess, live_raw);
    uint64_t cur_raw = cds ? cds->sub_raw : 0;
    R_CHECK(&rig, cur_raw != 0);
    moq_subgroup_handle_t gsg[AU_CYCLES];
    int served = 0;
    for (int k = 0; k < AU_CYCLES; k++) {
        /* An object on a new group: a slot for the current request. */
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = (uint64_t)k;
        DP_RETRY(&rig, moq_session_open_pub_subgroup(pub->peer, pubh, &sgc,
                                                     rig.now, &gsg[k]));
        int before = wo.objects;
        DP_RETRY(&rig, moq_session_write_object(pub->peer, gsg[k], 0, pl,
                                                rig.now));
        au_pump(&rig, ws, &wo, sib, &so, 12);
        R_CHECK(&rig, wo.objects == before + 1);
        served += wo.objects == before + 1;
        R_CHECK(&rig, au_sgs_used(rig.bind, ws->rsess, cur_raw, true) == 1);
        if (k == AU_CYCLES - 1) break;

        /* Cancelled: its slot comes back, the live request keeps its own. */
        DP_RETRY(&rig, moq_session_unsubscribe(ws->peer, ws_h, rig.now));
        au_pump(&rig, ws, &wo, sib, &so, 12);
        R_CHECK(&rig, au_sgs_used(rig.bind, ws->rsess, cur_raw, true) == 0);
        R_CHECK(&rig, au_sgs_used(rig.bind, ws->rsess, 0, false) == 1);
        R_CHECK(&rig, au_dsub_count(rig.bind, ws->rsess) == 1);

        /* The next request takes the cancelled one's session slot. */
        DP_RETRY(&rig, moq_session_subscribe(ws->peer, &sc1, rig.now, &ws_h));
        au_pump(&rig, ws, &wo, sib, &so, 12);
        const b_dsub_t *nds = au_dsub_other(rig.bind, ws->rsess, live_raw);
        uint64_t new_raw = nds ? nds->sub_raw : 0;
        R_CHECK(&rig, new_raw != 0 && new_raw != cur_raw);
        R_CHECK(&rig, moq_handle_slot(new_raw) == moq_handle_slot(cur_raw));
        cur_raw = new_raw;

        /* Only now does the source seal the cancelled request's group: the
         * new request in its session slot is untouched. */
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, gsg[k], rig.now));
        au_pump(&rig, ws, &wo, sib, &so, 12);
        R_CHECK(&rig, !wo.closed);
        R_CHECK(&rig, au_dsub_count(rig.bind, ws->rsess) == 2);
    }
    /* The live request is still served, the sibling got everything, and no
     * session error was counted. */
    int lb = wo.objects;
    DP_RETRY(&rig, moq_session_write_object(pub->peer, live_sg, 1, pl, rig.now));
    au_pump(&rig, ws, &wo, sib, &so, 12);
    R_CHECK(&rig, wo.objects == lb + 1);
    R_CHECK(&rig, !wo.closed && !so.closed);
    R_CHECK(&rig, so.objects == AU_CYCLES);
    R_CHECK(&rig, wo.sub_dones == 0);   /* no terminal for a cancellation */
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(rig.bind, &bs);
    R_CHECK(&rig, bs.session_errors == 0);
    moq_rcbuf_decref(pl);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    printf("%s: auto_update_terminal(cancel cycles): %d of %d requests served "
           "with %d slots; live request served=%d; session errors=%llu\n",
           f == 0 ? "PASS" : "FAIL", served, AU_CYCLES, AU_CYC_SLOTS,
           wo.objects == lb + 1, (unsigned long long)bs.session_errors);
    return f;
}

int
main(void)
{
    int failures = 0;
    for (int pubn = 0; pubn < 2; pubn++) {
        for (int mode = 0; mode < AU_MODES; mode++) {
            /* The relay offers discovery publications on draft-22 only. */
            if (pubn != 0 && mode == AU_HELD_ANSWER) continue;
            failures += au_case(pubn != 0, mode);
        }
    }
    failures += au_cycles();
    return failures ? 1 : 0;
}
