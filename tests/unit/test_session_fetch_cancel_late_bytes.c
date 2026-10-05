/*
 * Fetcher cancel racing the response data stream (core contract, both drafts).
 *
 * The publisher has accepted a FETCH and written two objects (its actions are
 * queued, not yet transported) when the fetcher cancels the request. One
 * transport step then carries the cancel one way and the already-written
 * response the other way. The fetcher's session, holding the cancel
 * tombstone for that request, must absorb every byte of that late response
 * stream: it STOPs the stream at its header, and the bytes that were already
 * on the wire behind that header -- legal input, STOP_SENDING cannot recall
 * them and the transport bridge only promises an idempotent stop -- must be
 * discarded, never read as a fresh stream. The session stays established and
 * the publisher observes FETCH_CANCELLED.
 *
 * Exact SimPair action/input trace is printed for the run.
 */
#include <moq/session.h>
#include <moq/sim.h>
#include "test_support.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

typedef struct tr {
    int  n;
    char line[96][160];
} tr_t;

static void
trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    tr_t *t = (tr_t *)ctx;
    if (r->kind != MOQ_SIM_TRACE_INPUT && r->kind != MOQ_SIM_TRACE_ACTION) return;
    if (t->n >= 96) return;
    uint64_t stream = 0;
    if (r->struct_size >= offsetof(moq_sim_trace_record_t, stream_ref) + sizeof(r->stream_ref)) {
        stream = r->stream_ref._v;
    }
    char first[32] = "";
    size_t c = r->bytes.len < 8 ? r->bytes.len : 8;
    for (size_t i = 0; i < c && r->bytes.data; i++) {
        (void)snprintf(first + 2 * i, sizeof(first) - 2 * i, "%02x", r->bytes.data[i]);
    }
    (void)snprintf(t->line[t->n++], sizeof(t->line[0]),
                   "step=%llu %s %s %d->%d stream=%llu len=%zu rc=%d first=%s",
                   (unsigned long long)r->step,
                   r->kind == MOQ_SIM_TRACE_INPUT ? "INPUT " : "ACTION",
                   r->kind == MOQ_SIM_TRACE_INPUT
                       ? (r->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES ? "control"
                          : r->input_kind == MOQ_SIM_INPUT_DATA_BYTES ? "data"
                          : r->input_kind == MOQ_SIM_INPUT_DATA_RESET ? "data-reset"
                          : r->input_kind == MOQ_SIM_INPUT_DATA_STOP ? "data-stop"
                          : r->input_kind == MOQ_SIM_INPUT_BIDI_BYTES ? "bidi"
                          : r->input_kind == MOQ_SIM_INPUT_BIDI_RESET ? "bidi-reset"
                          : r->input_kind == MOQ_SIM_INPUT_BIDI_STOP ? "bidi-stop"
                          : r->input_kind == MOQ_SIM_INPUT_TICK ? "tick" : "other")
                       : (r->action_kind == MOQ_ACTION_SEND_CONTROL ? "SEND_CONTROL"
                          : r->action_kind == MOQ_ACTION_SEND_DATA ? "SEND_DATA"
                          : r->action_kind == MOQ_ACTION_RESET_DATA ? "RESET_DATA"
                          : r->action_kind == MOQ_ACTION_STOP_DATA ? "STOP_DATA"
                          : r->action_kind == MOQ_ACTION_OPEN_BIDI_STREAM ? "OPEN_BIDI"
                          : r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM ? "SEND_BIDI"
                          : r->action_kind == MOQ_ACTION_CLOSE_BIDI_STREAM ? "CLOSE_BIDI"
                          : r->action_kind == MOQ_ACTION_RESET_BIDI_STREAM ? "RESET_BIDI"
                          : r->action_kind == MOQ_ACTION_STOP_BIDI_STREAM ? "STOP_BIDI"
                          : r->action_kind == MOQ_ACTION_ABORT_BIDI_STREAM ? "ABORT_BIDI"
                          : r->action_kind == MOQ_ACTION_CLOSE_SESSION ? "CLOSE_SESSION"
                          : "other"),
                   (int)r->from, (int)r->to, (unsigned long long)stream, r->bytes.len,
                   (int)r->result, first);
}

typedef struct obs {
    int      request, cancelled, ok, object, complete, error, reset, closed;
    moq_fetch_t req;
    uint64_t close_code;
    char     close_reason[64];
} obs_t;

static void
drain(moq_session_t *s, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_REQUEST:   o->request++; o->req = ev.u.fetch_request.fetch; break;
        case MOQ_EVENT_FETCH_CANCELLED: o->cancelled++; break;
        case MOQ_EVENT_FETCH_OK:        o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT:    o->object++; break;
        case MOQ_EVENT_FETCH_COMPLETE:  o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR:     o->error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED: {
            o->closed++;
            o->close_code = ev.u.closed.code;
            size_t n = ev.u.closed.reason.len < sizeof(o->close_reason) - 1
                           ? ev.u.closed.reason.len : sizeof(o->close_reason) - 1;
            if (n) memcpy(o->close_reason, ev.u.closed.reason.data, n);
            o->close_reason[n] = '\0';
            break;
        }
        default: break;
        }
        moq_event_cleanup(&ev);
    }
}

static moq_result_t
write_obj(moq_session_t *pub, moq_fetch_t fh, uint64_t oid, uint64_t now)
{
    uint8_t body[16];
    memset(body, (int)(0xd0 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.subgroup_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, now);
    moq_rcbuf_decref(pl);
    return rc;
}

/* bound = false: the whole response crosses the cancel (the fetcher first
 * sees the stream after it cancelled: tombstone -> STOP at the header, then
 * the bytes already behind it). bound = true: the first object arrived and
 * bound the stream before the cancel; the publisher's next object is on the
 * wire when the cancel's STOP goes out. */
static int
run(moq_version_t version, bool bound)
{
    char lbl[16];
    (void)snprintf(lbl, sizeof(lbl), "%s/%s", version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18",
                   bound ? "bound" : "unbound");
    int before = failures;
    static tr_t tr;
    memset(&tr, 0, sizeof(tr));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 0xC4A5u;
    cfg.version = version;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 64;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 64;
    cfg.trace_fn = trace_fn;
    cfg.trace_ctx = &tr;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
    moq_session_t *fetcher = moq_simpair_client(sp);
    moq_session_t *pub = moq_simpair_server(sp);
    uint64_t now = 1;
#define STEP()                                                            \
    do {                                                                  \
        now += 1000;                                                      \
        (void)moq_simpair_advance_to(sp, now);                            \
        size_t steps_ = 0;                                                \
        (void)moq_simpair_run_until_quiescent(sp, 64, &steps_);           \
    } while (0)
    for (int i = 0; i < 8; i++) STEP();
    obs_t fo, po;
    memset(&fo, 0, sizeof(fo));
    memset(&po, 0, sizeof(po));
    drain(fetcher, &fo);
    drain(pub, &po);
    MOQ_TEST_CHECK(moq_session_state(fetcher) == MOQ_SESS_ESTABLISHED);

    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_group = 0;
    fc.end_object = 3;
    moq_fetch_t fh;
    MOQ_TEST_CHECK(moq_session_fetch(fetcher, &fc, now, &fh) == MOQ_OK);
    for (int i = 0; i < 6 && po.request == 0; i++) { STEP(); drain(pub, &po); }
    MOQ_TEST_CHECK(po.request == 1);
    /* The publisher answers in full; nothing is transported yet. */
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    MOQ_TEST_CHECK(moq_session_accept_fetch(pub, po.req, &ac, now) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(pub, po.req, 0, now) == MOQ_OK);
    if (bound) {
        /* OK + the first object reach the fetcher and bind the stream. */
        for (int i = 0; i < 4 && fo.object == 0; i++) { STEP(); drain(fetcher, &fo); }
        MOQ_TEST_CHECK(fo.ok == 1 && fo.object == 1);
    }
    /* The fetcher cancels; the publisher's next object is already written. */
    MOQ_TEST_CHECK(moq_session_fetch_cancel(fetcher, fh, now) == MOQ_OK);
    MOQ_TEST_CHECK(write_obj(pub, po.req, 1, now) == MOQ_OK);
    tr.n = 0;   /* trace from here: the crossing */
    for (int i = 0; i < 4; i++) { STEP(); drain(fetcher, &fo); drain(pub, &po); }

    printf("RESULT %s: fetcher state=%d closed=%d code=0x%llx reason=\"%s\" ok=%d object=%d "
           "reset=%d | publisher cancelled=%d\n", lbl, (int)moq_session_state(fetcher),
           fo.closed, (unsigned long long)fo.close_code, fo.close_reason, fo.ok, fo.object,
           fo.reset, po.cancelled);
    for (int i = 0; i < tr.n; i++) printf("  %s\n", tr.line[i]);
    /* The contract under test. */
    MOQ_TEST_CHECK(moq_session_state(fetcher) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(fo.closed == 0);
    MOQ_TEST_CHECK(po.cancelled == 1);
    MOQ_TEST_CHECK(moq_session_state(pub) == MOQ_SESS_ESTABLISHED);
#undef STEP
    moq_simpair_destroy(sp);
    if (failures == before) printf("PASS: session_fetch_cancel_late_bytes %s\n", lbl);
    return failures - before;
}

int main(void)
{
    (void)run(MOQ_VERSION_DRAFT_16, false);
    (void)run(MOQ_VERSION_DRAFT_18, false);
    (void)run(MOQ_VERSION_DRAFT_16, true);
    (void)run(MOQ_VERSION_DRAFT_18, true);
    if (failures == 0) MOQ_TEST_PASS("session_fetch_cancel_late_bytes");
    return failures ? 1 : 0;
}
