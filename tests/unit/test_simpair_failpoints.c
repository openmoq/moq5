/*
 * SimPair end-to-end allocation-failure lane: one PUBLIC representative of
 * bidi hard-error propagation. A draft-16 namespace subscription is
 * established entirely through public APIs, the server queues one NAMESPACE
 * response, and the pump delivers it into the subscriber's suffix-tracking
 * allocation path. Every ordinal in that operation's declared allocation
 * window is swept; each failure must surface from
 * moq_simpair_run_until_quiescent() as exactly MOQ_ERR_NOMEM -- never a
 * wait -- with a deterministic semantic trace and allocation-attempt
 * sequence, and teardown must leave the allocator balanced, empty, and
 * clean of size-contract violations.
 *
 * The claim is DELIBERATELY NARROW. Documented limitations:
 *
 *  - One cfg.alloc serves both sessions and the pair itself, so this lane
 *    cannot identify a receiver-side allocation origin; origin isolation is
 *    the session-level suite's job (its per-origin fixtures and declared
 *    signatures), and the bridge terminal contract is the bridge suite's.
 *  - The pump consumes and cleans the outbound action when delivery
 *    returns a hard error, so no empty-refeed, redelivery,
 *    recovery-equivalence, peer-reaction, or semantic-state-equivalence
 *    claim is made here, and no replay machinery is added.
 *  - The draft-18 pending-join token-copy path is N/A THROUGH THIS PUBLIC
 *    SURFACE, not skipped and not a defect: the receiver buffers a joining
 *    FETCH only while the referenced subscription is still pending, while
 *    the public sender requires an established subscription with a current
 *    largest -- mutually exclusive through any public flow. That path's
 *    transactional behavior is pinned by the session-level joining
 *    fixtures and the bridge lane.
 */

#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../support/failpoint.h"
#include <string.h>

/* Pointer-free FNV-1a-style semantic trace hash over the same field set the
 * main SimPair suite uses: seed, step, time, record/input/action kinds,
 * result and code, and the actual wire bytes. Deterministic across process
 * runs for the same seed. Alongside the hash, the summary keeps the counts
 * that pin the delivery this lane exists to observe -- without them, two
 * empty or action-only traces would compare equal and the determinism
 * claim would be vacuous. */
typedef struct trace_summary {
    uint64_t     hash;
    size_t       count;
    size_t       send_bidi_actions;   /* SEND_BIDI_STREAM action records */
    size_t       bidi_byte_inputs;    /* BIDI_BYTES input records */
    moq_result_t bidi_input_result;   /* result of the last such input */
    size_t       bidi_input_bytes;    /* its wire byte length */
} trace_summary_t;

static void trace_hash_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    trace_summary_t *s = (trace_summary_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len; h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i];
            h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->count++;

    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM)
        s->send_bidi_actions++;
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_BIDI_BYTES) {
        s->bidi_byte_inputs++;
        s->bidi_input_result = r->result;
        s->bidi_input_bytes  = r->bytes.len;
    }
}

/* Everything one run of the operation produced, for exact cross-run
 * comparison and for the teardown oracle. */
typedef struct ns_fault_run {
    moq_result_t rc;            /* result of the pumped operation window */
    uint64_t     hash;          /* window-scoped semantic trace hash */
    size_t       trace_count;
    size_t       send_bidi_actions;
    size_t       bidi_byte_inputs;
    moq_result_t bidi_input_result;
    size_t       bidi_input_bytes;
    fp_attempt_t log[FP_LOG_CAP];
    size_t       log_len;
    uint64_t     pre;           /* call_count at the window boundary */
    uint64_t     reached;       /* call_count when the window closed */
    int          found_count;   /* valid byte-checked NAMESPACE_FOUND events */
    int          client_events; /* all client events after the window */
    int          server_events; /* all server events after the window */
} ns_fault_run_t;

/*
 * One complete fixture: a fixed-seed draft-16 pair on the failpoint
 * allocator, a namespace subscription established through public calls
 * only, one NAMESPACE("found") queued by the server, and the operation
 * window opened immediately before the pump. k == 0 runs without a fault;
 * k >= 1 arms the kth allocation attempt of the window. The trace summary
 * is reset at the window boundary so setup traffic cannot mask operation
 * divergence. Returns the number of failed checks.
 */
static int run_ns_fault_case(uint64_t seed, uint64_t k, ns_fault_run_t *out)
{
    int failures = 0;
    memset(out, 0, sizeof(*out));

    fp_alloc_state_t fs;
    memset(&fs, 0, sizeof(fs));
    moq_alloc_t alloc = fp_allocator(&fs);
    trace_summary_t ts;
    memset(&ts, 0, sizeof(ts));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = seed;
    cfg.initial_now_us = 1000;
    cfg.version = MOQ_VERSION_DRAFT_16;   /* pin the route to draft 16 */
    cfg.trace_fn = trace_hash_fn;
    cfg.trace_ctx = &ts;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;

    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&cfg, &sp), (int)MOQ_OK);
    if (!sp) return failures + 1;
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_simpair_run_until_quiescent(sp, 16, NULL), (int)MOQ_OK);
    moq_event_t ev;
    while (moq_session_poll_events(client, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(server, &ev, 1) > 0) moq_event_cleanup(&ev);

    /* A public namespace subscription, end to end. */
    moq_subscribe_namespace_cfg_t nc;
    moq_subscribe_namespace_cfg_init(&nc);
    moq_bytes_t pfx_parts[] = { MOQ_BYTES_LITERAL("live") };
    nc.track_namespace_prefix = (moq_namespace_t){ pfx_parts, 1 };
    moq_ns_sub_handle_t ch;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe_namespace(client, &nc,
            moq_simpair_now_us(sp), &ch),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_simpair_run_until_quiescent(sp, 16, NULL), (int)MOQ_OK);

    moq_ns_sub_handle_t shdl = MOQ_NS_SUB_HANDLE_INVALID;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
            shdl = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(shdl));
    moq_accept_ns_sub_cfg_t ac;
    moq_accept_ns_sub_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_ns_sub(server, shdl, &ac,
            moq_simpair_now_us(sp)),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_simpair_run_until_quiescent(sp, 16, NULL), (int)MOQ_OK);
    bool got_ok = false;
    while (moq_session_poll_events(client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_OK) got_ok = true;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(got_ok);

    /* Queue the NAMESPACE; its SEND_BIDI_STREAM action stays queued until
     * the pump, so the failure is observed through run_until_quiescent. */
    moq_bytes_t sfx_parts[] = { MOQ_BYTES_LITERAL("found") };
    moq_namespace_t suffix = { sfx_parts, 1 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_send_namespace(server, shdl, &suffix,
            moq_simpair_now_us(sp)),
        (int)MOQ_OK);

    /* The operation window and the trace window open together, HERE. */
    memset(&ts, 0, sizeof(ts));
    fs.log_from = fs.call_count;
    fs.log_len = 0;
    out->pre = fs.call_count;
    if (k > 0) fs.fail_at = fs.call_count + k;

    out->rc = moq_simpair_run_until_quiescent(sp, 16, NULL);
    out->reached = fs.call_count;
    fs.fail_at = 0;
    out->hash = ts.hash;
    out->trace_count = ts.count;
    out->send_bidi_actions = ts.send_bidi_actions;
    out->bidi_byte_inputs = ts.bidi_byte_inputs;
    out->bidi_input_result = ts.bidi_input_result;
    out->bidi_input_bytes = ts.bidi_input_bytes;
    memcpy(out->log, fs.log, fs.log_len * sizeof(fp_attempt_t));
    out->log_len = fs.log_len;

    /* Every event on either side after the window counts; a valid
     * NAMESPACE_FOUND beside an unexpected event must not pass. */
    while (moq_session_poll_events(client, &ev, 1) > 0) {
        out->client_events++;
        if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) {
            const moq_namespace_t *ns =
                &ev.u.namespace_found.track_namespace_suffix;
            if (ns->count == 1 && ns->parts[0].len == 5 &&
                memcmp(ns->parts[0].data, "found", 5) == 0)
                out->found_count++;
        }
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        out->server_events++;
        moq_event_cleanup(&ev);
    }

    moq_simpair_destroy(sp);

    /* Teardown facts, AFTER destruction: balanced, empty, contract-clean. */
    failures += fp_sticky_clean(&fs, "simpair-ns");
    if (fs.balance != 0 || fs.live_bytes != 0 || fs.table_len != 0) {
        fprintf(stderr, "FAILPOINT simpair-ns: destroy left balance %lld, "
                "live %lld, table %zu\n", (long long)fs.balance,
                (long long)fs.live_bytes, fs.table_len);
        failures++;
    }
    return failures;
}

/* Exact cross-run agreement: result, event outcome, window boundaries,
 * semantic trace, delivery counts, and the complete allocation-attempt
 * log. Returns the number of failed checks. */
static int ns_fault_runs_equal(const ns_fault_run_t *a, const ns_fault_run_t *b)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_INT((int)a->rc, (int)b->rc);
    MOQ_TEST_CHECK_EQ_INT(a->found_count, b->found_count);
    MOQ_TEST_CHECK_EQ_INT(a->client_events, b->client_events);
    MOQ_TEST_CHECK_EQ_INT(a->server_events, b->server_events);
    MOQ_TEST_CHECK_EQ_U64(a->pre, b->pre);
    MOQ_TEST_CHECK_EQ_U64(a->reached, b->reached);
    MOQ_TEST_CHECK_EQ_HEX(a->hash, b->hash);
    MOQ_TEST_CHECK_EQ_SIZE(a->trace_count, b->trace_count);
    MOQ_TEST_CHECK_EQ_SIZE(a->send_bidi_actions, b->send_bidi_actions);
    MOQ_TEST_CHECK_EQ_SIZE(a->bidi_byte_inputs, b->bidi_byte_inputs);
    MOQ_TEST_CHECK_EQ_INT((int)a->bidi_input_result, (int)b->bidi_input_result);
    MOQ_TEST_CHECK_EQ_SIZE(a->bidi_input_bytes, b->bidi_input_bytes);
    MOQ_TEST_CHECK_EQ_SIZE(a->log_len, b->log_len);
    MOQ_TEST_CHECK(a->log_len == b->log_len &&
                   memcmp(a->log, b->log,
                          a->log_len * sizeof(fp_attempt_t)) == 0);
    return failures;
}

/* The window must actually contain the bidi delivery: exactly one
 * SEND_BIDI_STREAM action, exactly one BIDI_BYTES input carrying nonempty
 * wire bytes, with the input result the run's expected one. Without this
 * the twin-run comparison could pass on two empty traces. */
static int ns_fault_delivery_seen(const ns_fault_run_t *r,
                                  moq_result_t expect_input_rc)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_SIZE(r->send_bidi_actions, 1u);
    MOQ_TEST_CHECK_EQ_SIZE(r->bidi_byte_inputs, 1u);
    MOQ_TEST_CHECK_EQ_INT((int)r->bidi_input_result, (int)expect_input_rc);
    MOQ_TEST_CHECK(r->bidi_input_bytes > 0);
    return failures;
}

int main(void)
{
    int failures = 0;
    const uint64_t seed = 0xC0FFEE;

    /* The four-attempt signature the session-level suite froze for this
     * operation; the window reaching exactly it proves the pump landed in
     * the suffix-tracking path rather than some unrelated allocation. The
     * canonical key of the one-part 5-byte suffix "found" is
     * [count u8][u16 len][bytes] = 8 bytes; the stored copy repeats it. */
    static const fp_expect_t k_sig[4] = {
        { FP_ALLOC, FP_SIZE_EXACT, 8, 0 },      /* canonical key */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* tracker (private) */
        { FP_ALLOC, FP_SIZE_SAME_AS, 0, 0 },    /* stored key copy */
        { FP_ALLOC, FP_SIZE_ANY, 0, 0 },        /* key array (private) */
    };

    /* No-fault baseline, run twice: exactly one byte-checked delivery and
     * nothing else on either side, the declared signature, N == 4, the
     * delivery visible in the trace, and run-for-run determinism. */
    ns_fault_run_t base, base2;
    failures += run_ns_fault_case(seed, 0, &base);
    MOQ_TEST_CHECK_EQ_INT((int)base.rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(base.found_count, 1);
    MOQ_TEST_CHECK_EQ_INT(base.client_events, 1);
    MOQ_TEST_CHECK_EQ_INT(base.server_events, 0);
    failures += ns_fault_delivery_seen(&base, MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(base.log_len, 4u);
    {
        /* Rebuild a state view for the shared checker: signature checks run
         * over the recorded window log. */
        fp_alloc_state_t view;
        memset(&view, 0, sizeof(view));
        memcpy(view.log, base.log, base.log_len * sizeof(fp_attempt_t));
        view.log_len = base.log_len;
        failures += fp_check_signature(&view, 0, k_sig, 4, "simpair-ns");
    }
    failures += run_ns_fault_case(seed, 0, &base2);
    failures += ns_fault_runs_equal(&base, &base2);

    /* Sweep every ordinal on fresh fixtures; each seed+ordinal runs twice
     * and must agree exactly. */
    for (uint64_t k = 1; k <= 4; k++) {
        ns_fault_run_t a, b;
        failures += run_ns_fault_case(seed, k, &a);
        fprintf(stderr, "FAILPOINT simpair-ns seed=0x%llx ordinal=%llu/4 "
                "(replay: seed=0x%llx k=%llu)\n",
                (unsigned long long)seed, (unsigned long long)k,
                (unsigned long long)seed, (unsigned long long)k);
        MOQ_TEST_CHECK_EQ_INT((int)a.rc, (int)MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(a.reached, a.pre + k);
        MOQ_TEST_CHECK_EQ_INT(a.found_count, 0);
        MOQ_TEST_CHECK_EQ_INT(a.client_events, 0);
        MOQ_TEST_CHECK_EQ_INT(a.server_events, 0);
        failures += ns_fault_delivery_seen(&a, MOQ_ERR_NOMEM);
        {
            fp_alloc_state_t view;
            memset(&view, 0, sizeof(view));
            memcpy(view.log, a.log, a.log_len * sizeof(fp_attempt_t));
            view.log_len = a.log_len;
            failures += fp_check_prefix(&view, 0, base.log, (size_t)k,
                                        "simpair-ns");
        }
        failures += run_ns_fault_case(seed, k, &b);
        failures += ns_fault_runs_equal(&a, &b);
    }

    MOQ_TEST_PASS("simpair_failpoints");
    return failures != 0;
}
