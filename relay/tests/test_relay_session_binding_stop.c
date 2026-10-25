/* Subgroup-local peer STOP regressions through the production binding.
 * The injection uses the traced sender-local ref, not SimPair's receive ref.
 * These rows deliberately do not exercise the transport bridge or STOP retry. */
#include "relay_session_binding_harness.h"
#include <moq/control.h>
#include <moq/control_d18.h>

enum { TARGET, SIBLING, NEXT_GROUP, STREAM_KEYS };
typedef enum stop_case { SEAL, CONTINUATION, SIBLINGS, NO_REOPEN, CAPACITY } stop_case_t;
static const char *const case_names[] = {
    "seal", "continuation", "siblings", "no-reopen", "capacity"
};

typedef struct stop_stream {
    moq_stream_ref_t ref;
    int key, fins, resets, sends_after_stop;
    size_t bytes;
    uint64_t reset_code;
} stop_stream_t;

typedef struct stop_trace {
    moq_version_t version;
    stop_stream_t streams[8];
    int count, errors, opens[STREAM_KEYS];
    bool stopped;
} stop_trace_t;

typedef struct stop_peer {
    moq_subscription_t sub;
    int accepted, terminals, complete[STREAM_KEYS], fins[STREAM_KEYS];
    int object_resets[STREAM_KEYS], subgroup_resets[STREAM_KEYS];
    size_t bytes[STREAM_KEYS];
    size_t object_bytes[STREAM_KEYS][3];
    int object_complete[STREAM_KEYS][3];
    uint64_t last_object[STREAM_KEYS];
    bool seen[STREAM_KEYS];
} stop_peer_t;

static int
stream_key(uint64_t group, uint64_t subgroup)
{
    if (group == 10 && subgroup == 7) return TARGET;
    if (group == 10 && subgroup == 8) return SIBLING;
    if (group == 11 && subgroup == 7) return NEXT_GROUP;
    return -1;
}

static void
stop_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    stop_trace_t *t = ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION ||
        r->from != MOQ_PERSPECTIVE_SERVER ||
        (r->action_kind != MOQ_ACTION_SEND_DATA &&
         r->action_kind != MOQ_ACTION_RESET_DATA)) return;
    if (r->struct_size < offsetof(moq_sim_trace_record_t, fin) + sizeof(r->fin)) {
        t->errors++;
        return;
    }
    int i;
    for (i = 0; i < t->count; i++)
        if (t->streams[i].ref._v == r->stream_ref._v) break;
    if (i == t->count) {
        if (i == 8 || r->action_kind != MOQ_ACTION_SEND_DATA) {
            t->errors++;
            return;
        }
        moq_buf_reader_t br;
        moq_buf_reader_init(&br, r->bytes.data, r->bytes.len);
        uint64_t group, subgroup;
        if (t->version == MOQ_VERSION_DRAFT_16) {
            moq_d16_subgroup_header_t h;
            if (moq_d16_decode_subgroup_header(&br, &h) != MOQ_OK ||
                h.subgroup_id_mode != MOQ_SUBGROUP_ID_MODE_PRESENT) {
                t->errors++;
                return;
            }
            group = h.group_id;
            subgroup = h.subgroup_id;
        } else {
            /* The current D22 profile uses the D18 subgroup framing ops. */
            moq_d18_subgroup_header_t h;
            if (moq_d18_decode_subgroup_header(&br, &h) != MOQ_OK ||
                h.subgroup_id_mode != MOQ_SUBGROUP_ID_MODE_PRESENT) {
                t->errors++;
                return;
            }
            group = h.group_id;
            subgroup = h.subgroup_id;
        }
        int key = stream_key(group, subgroup);
        if (key < 0) {
            t->errors++;
            return;
        }
        t->streams[i].ref = r->stream_ref;
        t->streams[i].key = key;
        t->opens[key]++;
        t->count++;
    }
    stop_stream_t *s = &t->streams[i];
    if (r->action_kind == MOQ_ACTION_RESET_DATA) {
        s->resets++;
        s->reset_code = r->error_code;
    } else {
        s->bytes += r->detail_bytes.len;
        s->fins += r->fin ? 1 : 0;
        if (t->stopped && s->key == TARGET) s->sends_after_stop++;
    }
}

static void
stop_drain(rig_t *r, conn_t *sub, stop_peer_t *p)
{
    moq_event_t e;
    while (moq_session_poll_events(sub->peer, &e, 1) > 0) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            R_CHECK(r, moq_subscription_eq(e.u.subscribe_ok.sub, p->sub));
            p->accepted++;
        } else if (e.kind == MOQ_EVENT_OBJECT_CHUNK) {
            const moq_object_chunk_event_t *o = &e.u.object_chunk;
            int key = stream_key(o->group_id, o->subgroup_id);
            R_CHECK(r, key >= 0 && moq_subscription_eq(o->sub, p->sub));
            if (key >= 0) {
                bool valid_object = (key == TARGET && o->object_id <= 2) ||
                    (key == SIBLING && (o->object_id == 10 || o->object_id == 11)) ||
                    (key == NEXT_GROUP && o->object_id == 0);
                R_CHECK(r, valid_object);
                if (p->seen[key]) R_CHECK(r, o->object_id >= p->last_object[key]);
                p->seen[key] = true;
                p->last_object[key] = o->object_id;
                size_t n = moq_rcbuf_len(o->chunk);
                const uint8_t *b = moq_rcbuf_data(o->chunk);
                for (size_t k = 0; k < n; k++)
                    R_CHECK(r, b[k] == (key == TARGET ? 0xA5 :
                                        key == SIBLING ? 0xB6 : 0xC7));
                p->bytes[key] += n;
                if (valid_object) {
                    size_t index = (size_t)(key == SIBLING ? o->object_id - 10 : o->object_id);
                    p->object_bytes[key][index] += n;
                    if (o->end && o->terminal == MOQ_OBJECT_TERMINAL_NORMAL)
                        p->object_complete[key][index]++;
                }
                if (o->end && o->terminal == MOQ_OBJECT_TERMINAL_NORMAL)
                    p->complete[key]++;
                if (o->end && o->terminal == MOQ_OBJECT_TERMINAL_RESET) {
                    R_CHECK(r, key == TARGET && o->error_code == 0x1);
                    p->object_resets[key]++;
                }
            }
        } else if (e.kind == MOQ_EVENT_SUBGROUP_RESET) {
            const moq_subgroup_reset_event_t *s = &e.u.subgroup_reset;
            int key = stream_key(s->group_id, s->subgroup_id);
            R_CHECK(r, moq_subscription_eq(s->sub, p->sub));
            R_CHECK(r, key == TARGET && s->error_code == 0x1);
            if (key >= 0) p->subgroup_resets[key]++;
        } else if (e.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            int key = stream_key(e.u.subgroup_finished.group_id,
                                 e.u.subgroup_finished.subgroup_id);
            R_CHECK(r, key >= 0);
            R_CHECK(r, moq_subscription_eq(e.u.subgroup_finished.sub, p->sub));
            if (key >= 0) p->fins[key]++;
        } else if (e.kind == MOQ_EVENT_SESSION_CLOSED ||
                   e.kind == MOQ_EVENT_SUBSCRIBE_DONE ||
                   e.kind == MOQ_EVENT_SUBSCRIBE_ERROR ||
                   e.kind == MOQ_EVENT_UNSUBSCRIBED) {
            p->terminals++;
        }
        moq_event_cleanup(&e);
    }
}

static void
stop_pump(rig_t *r, conn_t *pub, conn_t *sub, peer_state_t *origin,
          stop_peer_t *peer, int *full)
{
    for (int i = 0; i < 32; i++) {
        relay_bind_test_rig_cycle(r);
        if (moq_session_action_capacity(sub->rsess) == 0) (*full)++;
        relay_bind_test_peer_drain(r, pub, origin, false, false, 0, 0);
        stop_drain(r, sub, peer);
    }
}

static void
stop_inventory(rig_t *r, conn_t *pub, conn_t *sub,
               const peer_state_t *origin, const stop_peer_t *peer,
               const char *phase)
{
    moqr_core_stats_t cs;
    moqr_bind_stats_t bs;
    moqr_core_get_stats(r->core, &cs);
    moqr_bind_get_stats(r->bind, &bs);
    printf("PHASE %s: session=%d subs=%u active=%u conns=%u errors=%llu "
           "upstream_subs=%d upstream_unsub=%d\n", phase,
           (int)moq_session_state(sub->rsess), cs.subs, cs.subs_active,
           bs.conns, (unsigned long long)bs.session_errors,
           origin->up_subs, origin->unsubscribed);
    R_CHECK(r, moq_session_state(sub->rsess) == MOQ_SESS_ESTABLISHED);
    R_CHECK(r, moq_session_state(sub->peer) == MOQ_SESS_ESTABLISHED);
    R_CHECK(r, moq_session_state(pub->rsess) == MOQ_SESS_ESTABLISHED);
    R_CHECK(r, moq_session_state(pub->peer) == MOQ_SESS_ESTABLISHED);
    R_CHECK(r, cs.subs == 1 && cs.subs_active == 1 && cs.subs_parked == 0);
    R_CHECK(r, cs.tracks == 1 && cs.bindings == 2);
    R_CHECK(r, bs.conns == 2 && bs.session_errors == 0);
    R_CHECK(r, origin->up_subs == 1 && origin->unsubscribed == 0);
    R_CHECK(r, !origin->session_closed && peer->terminals == 0);
}

static void
stop_write(rig_t *r, conn_t *pub, moq_subgroup_handle_t h,
           bool chunk, uint64_t object, uint8_t byte)
{
    uint8_t body[4];
    memset(body, byte, sizeof(body));
    moq_rcbuf_t *pl = NULL;
    R_CHECK(r, moq_rcbuf_create(&r->alloc->vt, body, sizeof(body), &pl) == MOQ_OK);
    if (!pl) return;
    if (chunk) {
        DP_RETRY(r, moq_session_write_object_data(pub->peer, h, pl, r->now));
    } else {
        DP_RETRY(r, moq_session_write_object(pub->peer, h, object, pl, r->now));
    }
    moq_rcbuf_decref(pl);
}

static void
stop_wire_check(rig_t *r, const stop_trace_t *tr, bool stop)
{
    R_CHECK(r, tr->errors == 0);
    for (int key = 0; key < STREAM_KEYS; key++) R_CHECK(r, tr->opens[key] <= 1);
    for (int i = 0; i < tr->count; i++) {
        const stop_stream_t *s = &tr->streams[i];
        R_CHECK(r, s->resets == (stop && s->key == TARGET ? 1 : 0));
        R_CHECK(r, s->sends_after_stop == 0);
        if (stop && s->key == TARGET) {
            R_CHECK(r, s->reset_code == 0x1 && s->fins == 0 && s->bytes == 4);
        }
    }
}

static void
stop_knobs_reset(void)
{
    relay_bind_test_max_actions = 0;
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_client_streaming_objects = false;
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    relay_bind_test_max_open_subgroups = 0;
}

static int
stop_case(moq_version_t version, int draft, stop_case_t mode, bool stop, bool pressure)
{
    printf("CASE subgroup-stop d%d %s %s %s\n", draft, case_names[mode],
           stop ? "STOP" : "control", pressure ? "pressure" : "normal");
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    stop_trace_t tr = { .version = version };
    stop_peer_t peer = {0};
    peer_state_t origin = {0};
    int full = 0;
    bool mid = mode == CONTINUATION;
    bool next_started = false;
    if (mode == CAPACITY) relay_bind_test_max_open_subgroups = 2;
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_client_streaming_objects = true;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        stop_knobs_reset();
        printf("FAIL: subgroup-stop rig create, live=%ld\n", a.live);
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, version);
    relay_bind_test_max_actions = pressure ? 1 : 0;
    relay_bind_test_trace_fn = stop_trace;
    relay_bind_test_trace_ctx = &tr;
    conn_t *sub = relay_bind_test_rig_connect(&rig, version);
    stop_knobs_reset();
    R_CHECK(&rig, pub && sub);
    if (!pub || !sub) goto cleanup;
    relay_bind_test_rig_pump(&rig, 8);
    moq_bytes_t ns[] = { B("stop-local") };
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    moq_announcement_t ann;
    DP_RETRY(&rig, moq_session_publish_namespace(pub->peer, &pc, rig.now, &ann));
    if (rig.failures) goto cleanup;
    relay_bind_test_rig_pump(&rig, 8);
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = pc.track_namespace;
    sc.track_name = B("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    DP_RETRY(&rig, moq_session_subscribe(sub->peer, &sc, rig.now, &peer.sub));
    if (rig.failures) goto cleanup;
    relay_bind_test_rig_pump(&rig, 8);
    relay_bind_test_peer_drain(&rig, pub, &origin, true, false, 0, 0);
    stop_pump(&rig, pub, sub, &origin, &peer, &full);
    R_CHECK(&rig, origin.up_seen && peer.accepted == 1);
    if (rig.failures) goto cleanup;
    full = 0; /* Count action saturation during data delivery only. */

    moq_subgroup_cfg_t sg;
    moq_subgroup_cfg_init(&sg);
    sg.group_id = 10;
    sg.subgroup_id = 7;
    moq_subgroup_handle_t target, sibling, next;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, origin.up_sub, &sg,
                                            rig.now, &target));
    if (rig.failures) goto cleanup;
    if (mid) DP_RETRY(&rig, moq_session_begin_object(pub->peer, target, 0, 8, rig.now));
    if (rig.failures) goto cleanup;
    stop_write(&rig, pub, target, mid, 0, 0xA5);
    if (rig.failures) goto cleanup;
    stop_pump(&rig, pub, sub, &origin, &peer, &full);
    if (rig.failures) goto cleanup;
    sg.subgroup_id = 8;
    DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, origin.up_sub, &sg,
                                            rig.now, &sibling));
    if (rig.failures) goto cleanup;
    stop_write(&rig, pub, sibling, false, 10, 0xB6);
    if (rig.failures) goto cleanup;
    stop_pump(&rig, pub, sub, &origin, &peer, &full);
    R_CHECK(&rig, tr.count == 2 && tr.opens[TARGET] == 1 && tr.opens[SIBLING] == 1);
    R_CHECK(&rig, tr.streams[0].key == TARGET && tr.streams[1].key == SIBLING);
    R_CHECK(&rig, peer.bytes[TARGET] == 4 && peer.complete[TARGET] == (mid ? 0 : 1));
    R_CHECK(&rig, peer.bytes[SIBLING] == 4 && peer.complete[SIBLING] == 1);
    R_CHECK(&rig, peer.fins[TARGET] == 0 && peer.fins[SIBLING] == 0);
    if (pressure) R_CHECK(&rig, full > 0);
    stop_wire_check(&rig, &tr, false);
    stop_inventory(&rig, pub, sub, &origin, &peer, "before-stop");
    if (rig.failures) goto cleanup;
    if (stop) {
        R_CHECK(&rig, moq_session_action_capacity(sub->rsess) > 0);
        if (rig.failures) goto cleanup;
        tr.stopped = true;
        R_CHECK(&rig, moq_session_on_data_stop(sub->rsess, tr.streams[0].ref,
                                              0x1, rig.now) == MOQ_OK);
        if (rig.failures) goto cleanup;
        stop_pump(&rig, pub, sub, &origin, &peer, &full);
    }
    stop_wire_check(&rig, &tr, stop);
    R_CHECK(&rig, peer.object_resets[TARGET] == (stop && mid ? 1 : 0));
    R_CHECK(&rig, peer.subgroup_resets[TARGET] == (stop && !mid ? 1 : 0));
    stop_inventory(&rig, pub, sub, &origin, &peer, "after-stop");
    if (rig.failures) goto cleanup;

    if (mode == CAPACITY) {
        /* Fill both binder slots: stopped target plus an already-open healthy
         * sibling (both live in the control). Queue the higher-priority NEW
         * key and the sibling continuation before one event/delivery pass.
         * No later publisher input may be needed to service the sibling. */
        sg.group_id = 11;
        sg.subgroup_id = 7;
        sg.publisher_priority = 0;
        DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, origin.up_sub, &sg,
                                                rig.now, &next));
        if (rig.failures) goto cleanup;
        stop_write(&rig, pub, next, false, 0, 0xC7);
        if (rig.failures) goto cleanup;
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, next, rig.now));
        if (rig.failures) goto cleanup;
        next_started = true;
    }
    /* An already-live same-group sibling must continue on its original ref. */
    stop_write(&rig, pub, sibling, false, 11, 0xB6);
    if (rig.failures) goto cleanup;
    if (mode == CAPACITY) {
        relay_bind_test_rig_step_pair(&rig, pub);
        relay_bind_test_rig_bind_pump_once(&rig);
        for (unsigned i = 0; i < 32; i++) {
            relay_bind_test_rig_step_pair(&rig, sub);
            stop_drain(&rig, sub, &peer);
            relay_bind_test_rig_bind_pump_once(&rig);
        }
        R_CHECK(&rig, tr.opens[NEXT_GROUP] == 0);
        R_CHECK(&rig, peer.bytes[SIBLING] == 8 && peer.complete[SIBLING] == 2);
        stop_inventory(&rig, pub, sub, &origin, &peer, "full-table-existing-sibling");
        if (rig.failures) goto cleanup;
    }
    DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, sibling, rig.now));
    if (rig.failures) goto cleanup;
    stop_pump(&rig, pub, sub, &origin, &peer, &full);
    stop_inventory(&rig, pub, sub, &origin, &peer, "same-group-sibling");
    stop_wire_check(&rig, &tr, stop);
    R_CHECK(&rig, peer.bytes[SIBLING] == 8 && peer.complete[SIBLING] == 2 &&
                   peer.fins[SIBLING] == 1 && peer.last_object[SIBLING] == 11);
    R_CHECK(&rig, tr.streams[1].bytes == 8 && tr.streams[1].fins == 1);
    if (rig.failures) goto cleanup;

    if (mid) {
        stop_write(&rig, pub, target, true, 0, 0xA5);
        if (rig.failures) goto cleanup;
        DP_RETRY(&rig, moq_session_end_object(pub->peer, target, rig.now));
        if (rig.failures) goto cleanup;
        stop_pump(&rig, pub, sub, &origin, &peer, &full);
        stop_inventory(&rig, pub, sub, &origin, &peer, "after-continuation-before-seal");
        stop_wire_check(&rig, &tr, stop);
        if (rig.failures) goto cleanup;
    } else if (mode == NO_REOPEN) {
        /* Two later records exercise suppression beyond a one-shot discard. */
        for (uint64_t object = 1; object <= 2; object++) {
            stop_write(&rig, pub, target, false, object, 0xA5);
            if (rig.failures) goto cleanup;
            stop_pump(&rig, pub, sub, &origin, &peer, &full);
            stop_inventory(&rig, pub, sub, &origin, &peer, "later-record-on-stopped-key");
            stop_wire_check(&rig, &tr, stop);
            R_CHECK(&rig, peer.bytes[TARGET] == (stop ? 4u : 4u * (object + 1)));
            R_CHECK(&rig, peer.object_bytes[TARGET][object] == (stop ? 0u : 4u));
            R_CHECK(&rig, peer.object_complete[TARGET][object] == (stop ? 0 : 1));
            if (rig.failures) goto cleanup;
        }
    }
    if (mode != SIBLINGS) {
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, target, rig.now));
        if (rig.failures) goto cleanup;
        stop_pump(&rig, pub, sub, &origin, &peer, &full);
        stop_inventory(&rig, pub, sub, &origin, &peer, "after-seal");
        stop_wire_check(&rig, &tr, stop);
        if (rig.failures) goto cleanup;
    }

    /* The sibling-only row leaves the target unsealed through this check. */
    if (!next_started) {
        sg.group_id = 11;
        sg.subgroup_id = 7;
        DP_RETRY(&rig, moq_session_open_subgroup(pub->peer, origin.up_sub, &sg,
                                                rig.now, &next));
        if (rig.failures) goto cleanup;
        stop_write(&rig, pub, next, false, 0, 0xC7);
        if (rig.failures) goto cleanup;
        DP_RETRY(&rig, moq_session_close_subgroup(pub->peer, next, rig.now));
        if (rig.failures) goto cleanup;
    }
    stop_pump(&rig, pub, sub, &origin, &peer, &full);
    stop_inventory(&rig, pub, sub, &origin, &peer, "next-group");
    stop_wire_check(&rig, &tr, stop);
    R_CHECK(&rig, tr.count == 3 && tr.opens[NEXT_GROUP] == 1);
    R_CHECK(&rig, tr.streams[2].key == NEXT_GROUP && tr.streams[2].bytes == 4 &&
                   tr.streams[2].fins == 1);
    R_CHECK(&rig, peer.bytes[NEXT_GROUP] == 4 && peer.complete[NEXT_GROUP] == 1 &&
                   peer.fins[NEXT_GROUP] == 1);
    size_t target_bytes = stop ? 4u : mode == NO_REOPEN ? 12u : mid ? 8u : 4u;
    int target_complete = mid && stop ? 0 : mode == NO_REOPEN && !stop ? 3 : 1;
    int target_fins = !stop && mode != SIBLINGS ? 1 : 0;
    R_CHECK(&rig, tr.streams[0].bytes == target_bytes &&
                   tr.streams[0].fins == target_fins);
    R_CHECK(&rig, peer.bytes[TARGET] == target_bytes &&
                   peer.complete[TARGET] == target_complete && peer.fins[TARGET] == target_fins);
    R_CHECK(&rig, peer.object_resets[TARGET] == (stop && mid ? 1 : 0));
    R_CHECK(&rig, peer.subgroup_resets[TARGET] == (stop && !mid ? 1 : 0));
    R_CHECK(&rig, peer.object_bytes[TARGET][0] == (mid && !stop ? 8u : 4u));
    R_CHECK(&rig, peer.object_complete[TARGET][0] == (mid && stop ? 0 : 1));
    for (int object = 1; object <= 2; object++) {
        bool delivered = mode == NO_REOPEN && !stop;
        R_CHECK(&rig, peer.object_bytes[TARGET][object] == (delivered ? 4u : 0u));
        R_CHECK(&rig, peer.object_complete[TARGET][object] == (delivered ? 1 : 0));
    }
    for (int object = 0; object < 2; object++) {
        R_CHECK(&rig, peer.object_bytes[SIBLING][object] == 4 &&
                       peer.object_complete[SIBLING][object] == 1);
    }
    R_CHECK(&rig, peer.object_bytes[NEXT_GROUP][0] == 4 &&
                   peer.object_complete[NEXT_GROUP][0] == 1);
    for (int key = SIBLING; key < STREAM_KEYS; key++) {
        R_CHECK(&rig, peer.object_resets[key] == 0 && peer.subgroup_resets[key] == 0);
    }
cleanup:
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) printf("PASS: subgroup-stop d%d %s %s %s\n", draft,
        case_names[mode], stop ? "STOP" : "control", pressure ? "pressure" : "normal");
    return rig.failures;
}

int
main(int argc, char **argv)
{
    if (argc != 5) {
        fprintf(stderr, "usage: %s 16|18|22 seal|continuation|siblings|no-reopen|capacity "
                        "stop|control normal|pressure\n", argv[0]);
        return 2;
    }
    int draft;
    moq_version_t version;
    if (strcmp(argv[1], "16") == 0) { draft = 16; version = MOQ_VERSION_DRAFT_16; }
    else if (strcmp(argv[1], "18") == 0) { draft = 18; version = MOQ_VERSION_DRAFT_18; }
    else if (strcmp(argv[1], "22") == 0) { draft = 22; version = MOQ_VERSION_DRAFT_22; }
    else return 2;
    int mode;
    for (mode = SEAL; mode <= CAPACITY; mode++)
        if (strcmp(argv[2], case_names[mode]) == 0) break;
    if (mode > CAPACITY ||
        (strcmp(argv[3], "stop") != 0 && strcmp(argv[3], "control") != 0) ||
        (strcmp(argv[4], "normal") != 0 && strcmp(argv[4], "pressure") != 0)) return 2;
    return stop_case(version, draft, (stop_case_t)mode,
                     strcmp(argv[3], "stop") == 0,
                     strcmp(argv[4], "pressure") == 0) == 0 ? 0 : 1;
}
