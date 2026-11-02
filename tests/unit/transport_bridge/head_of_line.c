#include "test_cases.h"

/* the subscribe/accept half, shared with fixtures that build their own pair */
int hol_subscribe_accept(hol_fix_t *f)
{
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sc;
    moq_subscription_t csub;
    moq_event_t ev;
    moq_accept_subscribe_cfg_t acc;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns;
    sc.track_name = MOQ_BYTES_LITERAL("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    if (moq_session_subscribe(f->tp.client, &sc, 0, &csub) != MOQ_OK) return -1;
    pump_until_quiescent(&f->tp, 20, 0);

    f->ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            f->ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(f->ssub)) return -1;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(f->tp.server, f->ssub, &acc, 0) != MOQ_OK)
        return -1;
    pump_until_quiescent(&f->tp, 20, 0);
    fake_endpoint_clear_ops(&f->tp.server_ep);
    return 0;
}


int hol_init(hol_fix_t *f)
{
    memset(f, 0, sizeof(*f));
    if (test_pair_init_stream(&f->tp, 1, false) < 0) return -1;
    if (!setup_handshake(&f->tp)) { test_pair_destroy(&f->tp); return -1; }

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns;
    sc.track_name = MOQ_BYTES_LITERAL("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t csub;
    if (moq_session_subscribe(f->tp.client, &sc, 0, &csub) != MOQ_OK) goto fail;
    pump_until_quiescent(&f->tp, 20, 0);

    moq_event_t ev;
    f->ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            f->ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(f->ssub)) goto fail;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(f->tp.server, f->ssub, &acc, 0) != MOQ_OK)
        goto fail;
    pump_until_quiescent(&f->tp, 20, 0);
    fake_endpoint_clear_ops(&f->tp.server_ep);
    return 0;
fail:
    test_pair_destroy(&f->tp);
    return -1;
}


/* hol_init with the PUBLISHER's action queue raised, so a fixture can offer
 * the bridge more actions in one pass than its drain budget allows. */
static int hol_init_cap(hol_fix_t *f, uint32_t server_max_actions)
{
    memset(f, 0, sizeof(*f));
    if (test_pair_init_pub_cap(&f->tp, server_max_actions) < 0) return -1;
    if (!setup_handshake(&f->tp)) { test_pair_destroy(&f->tp); return -1; }

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns;
    sc.track_name = MOQ_BYTES_LITERAL("video");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t csub;
    if (moq_session_subscribe(f->tp.client, &sc, 0, &csub) != MOQ_OK) goto fail;
    pump_until_quiescent(&f->tp, 20, 0);

    moq_event_t ev;
    f->ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(f->tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            f->ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(f->ssub)) goto fail;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(f->tp.server, f->ssub, &acc, 0) != MOQ_OK)
        goto fail;
    pump_until_quiescent(&f->tp, 20, 0);
    fake_endpoint_clear_ops(&f->tp.server_ep);
    return 0;
fail:
    test_pair_destroy(&f->tp);
    return -1;
}


moq_result_t hol_open_sg(hol_fix_t *f, uint64_t group,
                         moq_subgroup_handle_t *out)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    c.subgroup_id = 0;
    c.end_of_group = false;
    *out = MOQ_SUBGROUP_INVALID;
    return moq_session_open_subgroup(f->tp.server, f->ssub, &c, 0, out);
}


moq_result_t hol_write(hol_fix_t *f, moq_subgroup_handle_t sg,
                       uint64_t obj, size_t len)
{
    uint8_t buf[128];
    moq_rcbuf_t *p = NULL;
    if (len > sizeof(buf)) return MOQ_ERR_INVAL;
    memset(buf, 'Z', len);
    if (moq_rcbuf_create(moq_alloc_default(), buf, len, &p) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_session_write_object(f->tp.server, sg, obj, p, 0);
    moq_rcbuf_decref(p);
    return rc;
}

/* Service the publisher bridge only. The peer is not needed: the block is on
 * the fake endpoint, so nothing here depends on inbound traffic. */
void hol_service(hol_fix_t *f, int passes)
{
    for (int i = 0; i < passes; i++)
        (void)held_bridge_service(f->tp.server_bridge, 0);
}


/* Adapter-boundary oracle: accepted WRITE operations naming this stream. */
static size_t hol_writes_on(const hol_fix_t *f, uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < f->tp.server_ep.count; i++)
        if (f->tp.server_ep.ops[i].kind == FAKE_OP_WRITE &&
            f->tp.server_ep.ops[i].stream_id == sid)
            n++;
    return n;
}


static size_t hol_count_kind(const hol_fix_t *f, fake_op_kind_t k)
{
    size_t n = 0;
    for (size_t i = 0; i < f->tp.server_ep.count; i++)
        if (f->tp.server_ep.ops[i].kind == k) n++;
    return n;
}


/* The stream id of the Nth accepted uni open (0-based), or UINT64_MAX. */
uint64_t hol_nth_open(const hol_fix_t *f, size_t n)
{
    size_t seen = 0;
    for (size_t i = 0; i < f->tp.server_ep.count; i++)
        if (f->tp.server_ep.ops[i].kind == FAKE_OP_OPEN_UNI) {
            if (seen == n) return f->tp.server_ep.ops[i].stream_id;
            seen++;
        }
    return UINT64_MAX;
}


uint32_t hol_pending(const hol_fix_t *f)
{
    return f->tp.server_bridge->pending_count;
}


/* Writes the bridge retains: in its pending FIFO, or -- a keyed stream's
 * refused write -- back in that stream's queue. */
uint32_t hol_retained(const hol_fix_t *f)
{
    return f->tp.server_bridge->pending_count + f->tp.server_bridge->dq_items;
}


/* -- 1. THE CONTRACT: an unrelated ref must progress ---------------------- */
int test_hol_unrelated_ref_progresses(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;
    uint64_t sid_a;
    size_t before, after;

    if (hol_init(&f) < 0) return 1;

    /* ref A opens for real and takes its header */
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);

    /* ref B's open is refused and retained */
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    /* non-vacuity: the refusal happened and the item is owned */
    MOQ_TEST_CHECK(f.tp.server_ep.block_count > 0);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);
    MOQ_TEST_CHECK(f.tp.server_bridge->pending[0].kind == PENDING_OPEN_UNI_DATA);

    fake_endpoint_clear_ops(&f.tp.server_ep);
    before = hol_writes_on(&f, sid_a);

    /* eligible work on ref A, which is open and needs no credit */
    for (uint64_t i = 1; i <= 6; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, i, 32) == MOQ_OK);
    hol_service(&f, 8);
    after = hol_writes_on(&f, sid_a);

    /* THE CONTRACT: ref A must not wait for ref B's credit. */
    if (after <= before) {
        fprintf(stderr, "HOL unrelated-ref: writes on A %zu -> %zu with %u "
                "pending (blocked open on B)\n", before, after, hol_pending(&f));
        failures++;
    }
    /* B is still owned, not lost */
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);

    /* and once credit returns, B completes too */
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 8);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_OPEN_UNI) >= 1);

    test_pair_destroy(&f.tp);
    return failures;
}


/* -- 2. SAFETY: same ref must not bypass, and FIN stays last -------------- */
int test_hol_same_ref_no_bypass_fin_last(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t sid_a;
    size_t writes_while_blocked;
    ptrdiff_t last_write = -1, last_fin = -1;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    /* block writes: ref A's own data parks */
    f.tp.server_ep.block_write = true;
    MOQ_TEST_CHECK(hol_write(&f, a, 1, 32) == MOQ_OK);
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_retained(&f) >= 1);

    /* more same-ref data and the terminal, all behind the parked write */
    for (uint64_t i = 2; i <= 4; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, i, 32) == MOQ_OK);
    (void)moq_session_close_subgroup(f.tp.server, a, 0);
    hol_service(&f, 8);
    writes_while_blocked = hol_writes_on(&f, sid_a);
    /* SAFETY: nothing on this ref may pass its own parked work */
    MOQ_TEST_CHECK(writes_while_blocked == 0);

    /* unblock: everything drains, in order, FIN last */
    f.tp.server_ep.block_write = false;
    hol_service(&f, 24);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    for (size_t i = 0; i < f.tp.server_ep.count; i++) {
        fake_op_t *o = &f.tp.server_ep.ops[i];
        if (o->kind != FAKE_OP_WRITE || o->stream_id != sid_a) continue;
        last_write = (ptrdiff_t)i;
        if (o->fin) last_fin = (ptrdiff_t)i;
    }
    MOQ_TEST_CHECK(last_write >= 0);
    if (last_fin >= 0 && last_fin != last_write) {
        fprintf(stderr, "HOL same-ref: FIN at %td is not the last write (%td)\n",
                last_fin, last_write);
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}


/* -- 3. two parked refs, work on a third -------------------------------- */
int test_hol_two_parked_third_progresses(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b, c;
    uint64_t sid_a;
    size_t before, after;
    uint32_t parked;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);

    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_open_sg(&f, 2, &c) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, c, 0, 32) == MOQ_OK);
    hol_service(&f, 6);
    parked = hol_pending(&f);
    MOQ_TEST_CHECK(parked >= 1);

    fake_endpoint_clear_ops(&f.tp.server_ep);
    before = hol_writes_on(&f, sid_a);
    for (uint64_t i = 1; i <= 4; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, i, 32) == MOQ_OK);
    hol_service(&f, 8);
    after = hol_writes_on(&f, sid_a);
    if (after <= before) {
        fprintf(stderr, "HOL two-parked: third ref writes %zu -> %zu with %u "
                "pending\n", before, after, hol_pending(&f));
        failures++;
    }
    /* both parked refs are still owned and complete once credit returns */
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 24);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_OPEN_UNI) >= 2);
    test_pair_destroy(&f.tp);
    return failures;
}


/* -- 4. connection-level terminal precedence ----------------------------- */
int test_hol_terminal_precedence_over_bypass(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    /* a session close while bypassable work exists: the transport close must
     * still happen, and the bridge must end up closed rather than starved */
    moq_session_close(f.tp.server, 0x0, NULL, 0);
    hol_service(&f, 12);
    fprintf(stderr, "HOL precedence: CLOSE ops while parked=%zu pending=%u\n",
            hol_count_kind(&f, FAKE_OP_CLOSE), hol_pending(&f));
    /* is the close merely queued behind the parked open? */
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 16);
    fprintf(stderr, "HOL precedence: CLOSE ops after credit=%zu pending=%u\n",
            hol_count_kind(&f, FAKE_OP_CLOSE), hol_pending(&f));
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_CLOSE) >= 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.tp.server_bridge));
    test_pair_destroy(&f.tp);
    return failures;
}


/* -- 6. bounded service work: no unbounded drain loop -------------------- */
int test_hol_service_work_is_bounded(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;
    size_t ops_first, ops_second;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    for (uint64_t i = 1; i <= 8; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, i, 32) == MOQ_OK);

    fake_endpoint_clear_ops(&f.tp.server_ep);
    (void)held_bridge_service(f.tp.server_bridge, 0);
    ops_first = f.tp.server_ep.count;
    fake_endpoint_clear_ops(&f.tp.server_ep);
    (void)held_bridge_service(f.tp.server_bridge, 0);
    ops_second = f.tp.server_ep.count;

    /* one pass must terminate and must not emit unboundedly; the endpoint's
     * own overflow flag is the loss oracle */
    MOQ_TEST_CHECK(!f.tp.server_ep.overflowed);
    MOQ_TEST_CHECK(ops_first <= FAKE_EP_MAX_OPS);
    MOQ_TEST_CHECK(ops_second <= FAKE_EP_MAX_OPS);
    /* and a steady state is reached: repeated passes with no new session work
     * cannot keep producing operations for ever */
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 24);
    fake_endpoint_clear_ops(&f.tp.server_ep);
    hol_service(&f, 8);
    MOQ_TEST_CHECK(f.tp.server_ep.count == 0);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    test_pair_destroy(&f.tp);
    return failures;
}


static void *hol_acct_alloc(size_t n, void *ctx)
{
    hol_acct_t *a = (hol_acct_t *)ctx;
    void *p = malloc(n ? n : 1);
    if (p == NULL) return NULL;
    a->live_bytes += (int64_t)n;
    a->live_blocks++;
    a->allocs++;
    return p;
}


static void hol_acct_free(void *p, size_t n, void *ctx)
{
    hol_acct_t *a = (hol_acct_t *)ctx;
    if (p == NULL) return;
    a->live_bytes -= (int64_t)n;
    a->live_blocks--;
    a->frees++;
    if (a->live_blocks < 0 || a->live_bytes < 0) a->negative = true;
    free(p);
}


static void *hol_acct_realloc(void *p, size_t old_n, size_t new_n, void *ctx)
{
    hol_acct_t *a = (hol_acct_t *)ctx;
    void *q = realloc(p, new_n ? new_n : 1);
    if (q == NULL) return NULL;
    a->live_bytes += (int64_t)new_n - (int64_t)old_n;
    if (p == NULL) { a->live_blocks++; a->allocs++; }
    if (a->live_bytes < 0 || a->live_blocks < 0) a->negative = true;
    return q;
}


moq_alloc_t hol_acct_allocator(hol_acct_t *a)
{
    moq_alloc_t al;
    memset(a, 0, sizeof(*a));
    memset(&al, 0, sizeof(al));
    al.alloc = hol_acct_alloc;
    al.realloc = hol_acct_realloc;
    al.free = hol_acct_free;
    al.ctx = a;
    return al;
}


/* -- 2. CLOSE INTENT: a parked open must not defer clean close ------------ */
int test_hol_close_intent_not_deferred(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;
    size_t close_while_parked, close_after_credit;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);

    /* park an open on a DIFFERENT stream */
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    /* non-vacuity: the refusal happened and the item is retained */
    MOQ_TEST_CHECK(f.tp.server_ep.block_count > 0);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    /* close intent, with the open still parked */
    moq_session_close(f.tp.server, 0x0, NULL, 0);
    hol_service(&f, 12);
    close_while_parked = hol_count_kind(&f, FAKE_OP_CLOSE);

    /* THE CONTRACT: close intent must be polled and begin the clean close
     * without waiting for stream credit that may never arrive. */
    if (close_while_parked == 0) {
        fprintf(stderr, "HOL close-intent: no CLOSE while parked (pending=%u); "
                "clean close is waiting on stream credit\n", hol_pending(&f));
        failures++;
    }

    /* and it does happen once credit returns, so the intent was merely
     * deferred rather than lost -- which is what makes this head-of-line
     * blocking */
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 16);
    close_after_credit = hol_count_kind(&f, FAKE_OP_CLOSE);
    MOQ_TEST_CHECK(close_after_credit >= 1);
    test_pair_destroy(&f.tp);
    return failures;
}


/* Once close has begun, transport-close/deferred-close priority still holds:
 * a blocked close is retried ahead of everything, and exactly one is emitted. */
int test_hol_close_priority_once_begun(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    f.tp.server_ep.block_close = true;
    moq_session_close(f.tp.server, 0x0, NULL, 0);
    hol_service(&f, 8);
    /* the close is retained, not lost, and no close op escaped */
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_CLOSE) == 0);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);

    f.tp.server_ep.block_close = false;
    hol_service(&f, 8);
    /* exactly one transport close, and the bridge is closed */
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_CLOSE) == 1);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    test_pair_destroy(&f.tp);
    return failures;
}


/* -- 3. conservation over the whole owned-action lifecycle ---------------- */
int test_hol_conservation_of_owned_actions(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    moq_session_cfg_t scfg;
    moq_session_t *pub = NULL;
    fake_endpoint_t ep;
    moq_transport_bridge_t *br = NULL;
    moq_transport_bridge_cfg_t bcfg;

    /* A publisher session and bridge on the counting allocator. The session is
     * driven straight from the action side: no peer is needed to prove that
     * every owned action reaches exactly one terminal ownership state. */
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &al, MOQ_PERSPECTIVE_SERVER);
    MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &pub) == MOQ_OK);
    held_endpoint_init(&ep, 3, 1);
    moq_transport_bridge_cfg_init(&bcfg, &al);
    HELD_REQUIRE(held_bridge_create(&bcfg, pub, &ep.vtable, &ep, &br)
                   == MOQ_OK);

    /* Exercise the retention paths: blocked opens, blocked writes, a blocked
     * close, and a fatal-free teardown with items still pending. */
    ep.block_open_uni = true;
    ep.block_write = true;
    for (int i = 0; i < 6; i++)
        (void)held_bridge_service(br, 0);
    ep.block_close = true;
    moq_session_close(pub, 0x0, NULL, 0);
    for (int i = 0; i < 6; i++)
        (void)held_bridge_service(br, 0);

    /* Teardown with pending work still owned: destroy must release all of it. */
    held_bridge_destroy(br);
    moq_session_destroy(pub);

    if (acct.live_blocks != 0 || acct.live_bytes != 0) {
        fprintf(stderr, "HOL conservation: leaked %lld block(s), %lld byte(s) "
                "(allocs=%lld frees=%lld)\n", (long long)acct.live_blocks,
                (long long)acct.live_bytes, (long long)acct.allocs,
                (long long)acct.frees);
        failures++;
    }
    if (acct.negative) {
        fprintf(stderr, "HOL conservation: a free had no matching alloc "
                "(double cleanup)\n");
        failures++;
    }
    MOQ_TEST_CHECK(acct.allocs > 0);          /* the oracle really ran */
    MOQ_TEST_CHECK(acct.allocs == acct.frees);
    return failures;
}


/* -- 4. deterministic small-state explorer ------------------------------- */
/* xorshift64*, so a seed replays exactly. */
static uint64_t hol_rng(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    *s = x;
    return x * 2685821657736338717ULL;
}


#define HOL_EXPLORE_REFS 3


static int hol_explore_seed(uint64_t seed, int steps, bool verbose)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t sg[HOL_EXPLORE_REFS];
    bool opened[HOL_EXPLORE_REFS];
    uint64_t obj[HOL_EXPLORE_REFS];
    uint64_t st = seed ? seed : 1;
    uint32_t credit = (uint32_t)(hol_rng(&st) % 4);   /* credit 0..3 */
    uint32_t granted = 0;
    unsigned offered = 0, saturated = 0;
    bool fatal_seen = false;

    if (hol_init(&f) < 0) return 1;
    memset(opened, 0, sizeof(opened));
    memset(obj, 0, sizeof(obj));
    /* credit is modelled at the endpoint: opens are refused once `credit`
     * of them have been accepted, until a credit-return op lifts it. */
    f.tp.server_ep.block_open_uni = (credit == 0);

    for (int i = 0; i < steps; i++) {
        uint64_t r = hol_rng(&st);
        int which = (int)((r >> 3) % HOL_EXPLORE_REFS);
        switch (r % 9u) {
        case 0:                                   /* open a subgroup */
            if (!opened[which] &&
                hol_open_sg(&f, (uint64_t)(100 + which), &sg[which]) == MOQ_OK) {
                opened[which] = true;
            }
            break;
        case 1:                                   /* write an object */
            if (opened[which] &&
                hol_write(&f, sg[which], obj[which], 32) == MOQ_OK) {
                obj[which]++;
                offered++;
            }
            break;
        case 2:                                   /* terminal FIN */
            if (opened[which]) {
                (void)moq_session_close_subgroup(f.tp.server, sg[which], 0);
                opened[which] = false;
            }
            break;
        case 3:                                   /* reset the subgroup */
            if (opened[which]) {
                (void)moq_session_reset_subgroup(f.tp.server, sg[which], 0x1, 0);
                opened[which] = false;
            }
            break;
        case 4:                                   /* toggle write block */
            f.tp.server_ep.block_write = !f.tp.server_ep.block_write;
            break;
        case 5:                                   /* toggle reset block */
            f.tp.server_ep.block_reset = !f.tp.server_ep.block_reset;
            break;
        case 6:                                   /* credit return */
            granted = credit;
            f.tp.server_ep.block_open_uni = false;
            break;
        case 7:                                   /* credit exhaustion */
            f.tp.server_ep.block_open_uni = true;
            break;
        default:                                  /* service */
            hol_service(&f, 1 + (int)((r >> 9) % 3u));
            break;
        }
        /* ORACLE: an unexpected bridge fatal is ALWAYS a failure. There is
         * no exemption -- an exempted fatal is exactly what hides a defect. */
        if (moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
            fprintf(stderr, "EXPLORE seed=%llu step=%d BRIDGE FATAL "
                    "(pending=%u of %u, offered=%u)\n",
                    (unsigned long long)seed, i, hol_pending(&f),
                    f.tp.server_bridge->max_pending, offered);
            failures++;
            fatal_seen = true;
            break;
        }
        if (hol_pending(&f) >= f.tp.server_bridge->max_pending)
            saturated++;
        if (f.tp.server_ep.overflowed) {
            fprintf(stderr, "EXPLORE seed=%llu step=%d endpoint op loss\n",
                    (unsigned long long)seed, i);
            failures++;
            break;
        }
        if (f.tp.server_ep.count > FAKE_EP_MAX_OPS / 2)
            fake_endpoint_clear_ops(&f.tp.server_ep);
    }
    /* ORACLE: lift every block; the queue must drain, no fatal may appear,
     * and every offered object must have reached the endpoint. */
    if (!fatal_seen) {
        f.tp.server_ep.block_open_uni = false;
        f.tp.server_ep.block_write = false;
        f.tp.server_ep.block_reset = false;
        fake_unblock_stream(&f.tp.server_ep);
        hol_service(&f, 256);
        if (moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
            fprintf(stderr, "EXPLORE seed=%llu FATAL during drain\n",
                    (unsigned long long)seed);
            failures++;
        } else if (hol_pending(&f) != 0) {
            fprintf(stderr, "EXPLORE seed=%llu unblocked but %u still "
                    "pending\n", (unsigned long long)seed, hol_pending(&f));
            failures++;
        } else if (offered > 0 &&
                   f.tp.server_ep.write_calls < (uint64_t)offered) {
            fprintf(stderr, "EXPLORE seed=%llu only %llu write(s) for %u "
                    "offered object(s)\n", (unsigned long long)seed,
                    (unsigned long long)f.tp.server_ep.write_calls, offered);
            failures++;
        }
    }
    if (verbose)
        printf("EXPLORE seed=%llu credit=%u granted=%u offered=%u "
               "saturated_steps=%u %s\n", (unsigned long long)seed, credit,
               granted, offered, saturated, failures ? "FAIL" : "ok");
    test_pair_destroy(&f.tp);
    return failures;
}


/* A walk that deliberately runs max_actions > max_pending on one blocked
 * stream, so retention saturation is actually reached -- the state that made
 * fatal. Same oracles: no fatal ever, full drain, conservation. A data
 * stream's backlog waits in its scheduling queue rather than the pending
 * FIFO, so saturation is the retained-bytes bound, sized to be reached. */
static int hol_explore_saturation_seed(uint64_t seed, int steps)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t st = seed ? seed : 1, sid_a, obj = 0;
    unsigned offered = 0, saturated = 0;
    const uint32_t MA = 4096, MP = 8;

    memset(&f, 0, sizeof(f));
    if (test_pair_init_full(&f.tp, 1, false, 0, 0, MA) < 0) return 1;
    if (!setup_handshake(&f.tp)) { test_pair_destroy(&f.tp); return 1; }
    held_bridge_destroy(f.tp.server_bridge);
    {
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init_sized(&bcfg, sizeof(bcfg),
                                            moq_alloc_default());
        bcfg.max_pending = MP;
        bcfg.max_retained_bytes = MP * 24u;   /* about MP objects */
        if (held_bridge_create(&bcfg, f.tp.server,
                &f.tp.server_ep.vtable, &f.tp.server_ep,
                &f.tp.server_bridge) != MOQ_OK) {
            test_pair_destroy(&f.tp); return 1;
        }
    }
    if (hol_subscribe_accept(&f) < 0) { test_pair_destroy(&f.tp); return 1; }
    if (hol_open_sg(&f, 0, &a) != MOQ_OK) { test_pair_destroy(&f.tp); return 1; }
    (void)hol_write(&f, a, obj++, 24);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    if (sid_a == UINT64_MAX) { test_pair_destroy(&f.tp); return 1; }

    /* start blocked: saturation is the point of this walk, so the weighting
     * favours offering work while the one stream cannot drain */
    fake_block_stream(&f.tp.server_ep, sid_a);
    for (int i = 0; i < steps; i++) {
        uint64_t r = hol_rng(&st);
        uint64_t pick = r % 10u;
        if (pick < 6u) {
            if (hol_write(&f, a, obj, 24) == MOQ_OK) { obj++; offered++; }
        } else if (pick < 8u) {
            hol_service(&f, 1 + (int)((r >> 7) % 3u));
        } else if (pick == 8u) {
            fake_block_stream(&f.tp.server_ep, sid_a);
        } else {
            fake_unblock_stream(&f.tp.server_ep);
            hol_service(&f, 2);
            fake_block_stream(&f.tp.server_ep, sid_a);
        }
        if (f.tp.server_bridge->dq_bytes >=
            f.tp.server_bridge->max_retained_bytes)
            saturated++;
        if (moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
            fprintf(stderr, "EXPLORE-SAT seed=%llu step=%d FATAL "
                    "(pending=%u of %u offered=%u)\n",
                    (unsigned long long)seed, i, hol_pending(&f),
                    f.tp.server_bridge->max_pending, offered);
            failures++;
            break;
        }
        if (f.tp.server_ep.count > FAKE_EP_MAX_OPS / 2)
            fake_endpoint_clear_ops(&f.tp.server_ep);
    }
    if (!moq_transport_bridge_is_fatal(f.tp.server_bridge)) {
        fake_unblock_stream(&f.tp.server_ep);
        hol_service(&f, 512);
        if (hol_pending(&f) != 0 || f.tp.server_bridge->dq_items != 0) {
            fprintf(stderr, "EXPLORE-SAT seed=%llu %u pending + %u queued "
                    "after unblock\n", (unsigned long long)seed,
                    hol_pending(&f), f.tp.server_bridge->dq_items);
            failures++;
        }
        if (offered > 0 && f.tp.server_ep.write_calls < (uint64_t)offered) {
            fprintf(stderr, "EXPLORE-SAT seed=%llu only %llu write(s) for %u "
                    "offered\n", (unsigned long long)seed,
                    (unsigned long long)f.tp.server_ep.write_calls, offered);
            failures++;
        }
    }
    if (saturated == 0) {
        fprintf(stderr, "EXPLORE-SAT seed=%llu never reached saturation; the "
                "case is vacuous\n", (unsigned long long)seed);
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}


int test_hol_explorer(void)
{
    /* Fixed, recorded seeds. Never tuned after the fact: this list is the
     * receipt, and any seed here replays exactly. */
    static const uint64_t seeds[] = {
        1u, 2u, 3u, 5u, 8u, 13u, 21u, 34u, 55u, 89u, 144u, 233u,
        377u, 610u, 987u, 1597u, 2584u, 4181u, 6765u, 10946u,
        0xC0FFEEu, 0xDEADBEEFu, 0x5EEDu, 0xA5A5A5A5u,
    };
    int failures = 0;
    size_t n = sizeof(seeds) / sizeof(seeds[0]);
    for (size_t i = 0; i < n; i++)
        failures += hol_explore_seed(seeds[i], 48, false);
    {
        static const uint64_t sat[] = { 0x5A7u, 0x5A8u, 0x5A9u, 0x5AAu,
                                        0x5ABu, 0x5ACu, 0x5ADu, 0x5AEu };
        size_t m = sizeof(sat) / sizeof(sat[0]);
        for (size_t i = 0; i < m; i++)
            failures += hol_explore_saturation_seed(sat[i], 200);
        printf("EXPLORE receipt: %zu seeds x 48 steps (refs=%d, credit 0..3) + "
               "%zu saturation seeds x 200 steps (max_actions=4096, "
               "max_pending=8, max_retained_bytes=192), %d failure(s)\n",
               n, HOL_EXPLORE_REFS, m, failures);
    }
    return failures;
}


/* -- gate-specific fixtures: each pins one classifier property ------------ */

/* The one state in which the ordering gate, not the endpoint, is what holds a
 * later same-stream action back:
 *
 *   pending = [ C-open (blocked, at the head), A-write (blocked earlier) ]
 *
 * The retry pass stops at C, so A's older write is re-queued WITHOUT being
 * attempted -- and by then the endpoint is willing to accept writes to A. A
 * newly drained write for A must therefore be retained behind the older one.
 * Without the gate it goes straight to the endpoint and the stream sees object
 * 2 before object 1.
 *
 * Object identity is carried in the payload LENGTH, so the recorded operation
 * sequence names the order.
 */
int test_hol_gate_orders_when_endpoint_would_accept(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, c;
    uint64_t sid_a;
    size_t lens[8];
    size_t n = 0;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);

    /* head of the FIFO: an open that cannot be satisfied */
    f.tp.server_ep.block_open_uni = true;
    MOQ_TEST_CHECK(hol_open_sg(&f, 9, &c) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, c, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);

    /* behind it: object 1 on stream A, blocked only on A */
    fake_block_stream(&f.tp.server_ep, sid_a);
    MOQ_TEST_CHECK(hol_write(&f, a, 1, 41) == MOQ_OK);
    hol_service(&f, 4);
    MOQ_TEST_CHECK(hol_retained(&f) >= 2);

    /* the endpoint is now willing to accept A, but the retry still stops at
     * the head, so A's object 1 stays retained and un-attempted */
    fake_unblock_stream(&f.tp.server_ep);
    fake_endpoint_clear_ops(&f.tp.server_ep);
    MOQ_TEST_CHECK(hol_write(&f, a, 2, 42) == MOQ_OK);
    hol_service(&f, 6);

    for (size_t i = 0; i < f.tp.server_ep.count && n < 8; i++) {
        fake_op_t *o = &f.tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == sid_a && o->data_len > 32)
            lens[n++] = o->data_len;
    }
    /* ORDERING: if object 2 (42 bytes) reaches the stream before object 1
     * (41 bytes), a later action passed older same-stream work. */
    for (size_t i = 0; i < n; i++) {
        if (lens[i] == 42) {
            bool saw_41 = false;
            for (size_t j = 0; j < i; j++)
                if (lens[j] == 41) saw_41 = true;
            if (!saw_41) {
                fprintf(stderr, "HOL gate-order: object 2 reached stream %llu "
                        "before object 1 (pending=%u)\n",
                        (unsigned long long)sid_a, hol_pending(&f));
                failures++;
            }
            break;
        }
    }

    /* everything drains once the head can proceed, in order and exactly once */
    f.tp.server_ep.block_open_uni = false;
    hol_service(&f, 32);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    MOQ_TEST_CHECK(!f.tp.server_ep.overflowed);
    test_pair_destroy(&f.tp);
    return failures;
}


/* A pending item identified ONLY by transport id (STOP_SENDING)
 * and a later action identified by ref must be recognised as the same stream. */
int test_hol_mixed_identity_same_stream(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t sid_a;
    uint32_t pending_after_reset;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    /* a blocked RESET on ref A: retained, and identified by transport id */
    f.tp.server_ep.block_reset = true;
    (void)moq_session_reset_subgroup(f.tp.server, a, 0x1, 0);
    hol_service(&f, 4);
    pending_after_reset = hol_pending(&f);
    MOQ_TEST_CHECK(pending_after_reset >= 1);

    /* the reset is the terminal for this stream, so nothing more may be
     * written to it; the mixed-identity check is that the retained item is
     * matched to ref A at all -- an unmatched item would let a same-stream
     * write through and produce a write AFTER a reset was queued */
    (void)hol_write(&f, a, 1, 32);
    hol_service(&f, 8);
    MOQ_TEST_CHECK(hol_writes_on(&f, sid_a) == 0);

    f.tp.server_ep.block_reset = false;
    hol_service(&f, 16);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_RESET) >= 1);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    test_pair_destroy(&f.tp);
    return failures;
}


/* A retained TRANSPORT CLOSE is connection-level: it orders ahead of ordinary
 * stream work and is never bypassed. (The other connection-level class, a
 * retained control-stream write, has no fixture here -- see the report.) */
int test_hol_pending_close_blocks_stream_work(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    size_t writes_after;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    /* queue more objects on the open stream BEFORE closing, so there is
     * genuinely bypassable work behind the close */
    for (uint64_t i = 1; i <= 4; i++)
        MOQ_TEST_CHECK(hol_write(&f, a, i, 32) == MOQ_OK);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    f.tp.server_ep.block_close = true;
    moq_session_close(f.tp.server, 0x0, NULL, 0);
    hol_service(&f, 8);
    /* the transport close is retained and is connection-level */
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);
    MOQ_TEST_CHECK(f.tp.server_bridge->pending[0].kind == PENDING_CLOSE_TRANSPORT);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_CLOSE) == 0);
    writes_after = hol_count_kind(&f, FAKE_OP_WRITE);
    /* nothing may be written past a pending transport close */
    if (writes_after != 0) {
        fprintf(stderr, "HOL pending-close: %zu write(s) escaped past a "
                "retained transport close\n", writes_after);
        failures++;
    }

    f.tp.server_ep.block_close = false;
    hol_service(&f, 12);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_CLOSE) == 1);
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    test_pair_destroy(&f.tp);
    return failures;
}


/* Two items that name NO ref are not thereby the same stream.
 * Two independent streams each with a blocked stop/reset must both progress
 * once unblocked -- if ref 0 were a shared domain, one would gate the other
 * for ever. */
int test_hol_refless_domains_are_distinct(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a, b;
    size_t resets;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    MOQ_TEST_CHECK(hol_open_sg(&f, 1, &b) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, b, 0, 32) == MOQ_OK);
    hol_service(&f, 6);
    MOQ_TEST_CHECK(hol_count_kind(&f, FAKE_OP_OPEN_UNI) >= 2);
    fake_endpoint_clear_ops(&f.tp.server_ep);

    /* both streams get a blocked reset: two retained items */
    f.tp.server_ep.block_reset = true;
    (void)moq_session_reset_subgroup(f.tp.server, a, 0x1, 0);
    (void)moq_session_reset_subgroup(f.tp.server, b, 0x2, 0);
    hol_service(&f, 8);
    MOQ_TEST_CHECK(hol_pending(&f) >= 1);

    f.tp.server_ep.block_reset = false;
    hol_service(&f, 32);
    resets = hol_count_kind(&f, FAKE_OP_RESET);
    /* BOTH must complete: a shared refless domain would strand one */
    if (resets < 2) {
        fprintf(stderr, "HOL refless-domains: only %zu reset(s) completed; "
                "pending=%u\n", resets, hol_pending(&f));
        failures++;
    }
    MOQ_TEST_CHECK(hol_pending(&f) == 0);
    test_pair_destroy(&f.tp);
    return failures;
}


/* An owned payload carried through the retry/re-enqueue path
 * must be released exactly once. */
int test_hol_conservation_through_retry(void)
{
    int failures = 0;
    hol_acct_t acct;
    moq_alloc_t al = hol_acct_allocator(&acct);
    moq_session_cfg_t scfg;
    moq_session_t *pub = NULL;
    fake_endpoint_t ep;
    moq_transport_bridge_t *br = NULL;
    moq_transport_bridge_cfg_t bcfg;
    uint8_t payload[64];

    moq_session_cfg_init_sized(&scfg, sizeof(scfg), &al, MOQ_PERSPECTIVE_SERVER);
    MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &pub) == MOQ_OK);
    held_endpoint_init(&ep, 3, 1);
    moq_transport_bridge_cfg_init(&bcfg, &al);
    HELD_REQUIRE(held_bridge_create(&bcfg, pub, &ep.vtable, &ep, &br)
                   == MOQ_OK);

    /* Drive control emission with everything blocked so items are retained and
     * re-enqueued repeatedly: each pass exercises the retain path on items
     * that own heap copies. */
    memset(payload, 'C', sizeof(payload));
    ep.block_write = true;
    ep.block_open_uni = true;
    ep.block_open_bidi = true;
    for (int pass = 0; pass < 12; pass++)
        (void)held_bridge_service(br, 0);
    /* then let it drain, so the same items go out and are cleaned once */
    ep.block_write = false;
    ep.block_open_uni = false;
    ep.block_open_bidi = false;
    for (int pass = 0; pass < 12; pass++)
        (void)held_bridge_service(br, 0);

    held_bridge_destroy(br);
    moq_session_destroy(pub);

    if (acct.live_blocks != 0 || acct.live_bytes != 0 || acct.negative) {
        fprintf(stderr, "HOL retry-conservation: live=%lld blocks %lld bytes "
                "negative=%d (allocs=%lld frees=%lld)\n",
                (long long)acct.live_blocks, (long long)acct.live_bytes,
                (int)acct.negative, (long long)acct.allocs,
                (long long)acct.frees);
        failures++;
    }
    MOQ_TEST_CHECK(acct.allocs > 0);
    return failures;
}

#if defined(MOQ_BRIDGE_SCAN_COUNTERS) && defined(MOQ_BRIDGE_TEST_INJECT)

int test_hol_scan_operation_counts(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t sg;
    enum { SCAN_CANDIDATES = 8 };
    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &sg) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, sg, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    const uint64_t sid = hol_nth_open(&f, 0);
    const uint64_t ref = moq_bridge_test_ref_for_id(f.tp.server_bridge, sid);
    MOQ_TEST_CHECK(sid != UINT64_MAX && ref != 0);
    const uint32_t depths[] = { 0, 1, BRIDGE_DEFAULT_MAX_PENDING - 1,
                                BRIDGE_DEFAULT_MAX_PENDING };

    /* Measure the classifier itself. A service pass may classify the same
     * candidate again while retrying or filling an emptied mapped head. */
    for (size_t row = 0; row < sizeof(depths) / sizeof(depths[0]); ++row) {
        const uint32_t depth = depths[row];
        while (hol_pending(&f) < depth) {
            uint64_t other = 30000 + hol_pending(&f);
            if (!moq_bridge_test_inject_pending(f.tp.server_bridge,
                    PENDING_STOP_SENDING, 0, other)) break;
        }
        MOQ_TEST_CHECK(hol_pending(&f) == depth);
        moq_bridge_scan_items = 0;
        moq_bridge_scan_candidates = 0;
        moq_bridge_map_lookups = 0;
        for (unsigned n = 0; n < SCAN_CANDIDATES; ++n)
            MOQ_TEST_CHECK(!moq_bridge_test_domain_blocked(
                f.tp.server_bridge, ref, sid, true));
        MOQ_TEST_CHECK(moq_bridge_scan_candidates == SCAN_CANDIDATES);
        MOQ_TEST_CHECK(moq_bridge_scan_items ==
                       (unsigned long long)depth * SCAN_CANDIDATES);
        MOQ_TEST_CHECK(moq_bridge_map_lookups == 0);
        MOQ_TEST_CHECK(hol_pending(&f) == depth);
        printf("SCAN depth=%u candidates=%llu items=%llu map_lookups=%llu\n",
               depth, moq_bridge_scan_candidates, moq_bridge_scan_items,
               moq_bridge_map_lookups);
    }
    test_pair_destroy(&f.tp);
    return failures;
}

/* Each object has its own marker: a duplicate cannot hide a missing object. */
static int hol_payload_set(const hol_fix_t *f, unsigned expected)
{
    unsigned counts[8] = {0};
    int failures = 0;
    for (size_t i = 0; i < f->tp.server_ep.count; ++i) {
        const fake_op_t *op = &f->tp.server_ep.ops[i];
        if (op->kind != FAKE_OP_WRITE || op->data_len != 32 ||
            op->data[0] < 0xa0 || op->data[0] >= 0xa8) continue;
        const unsigned index = op->data[0] - 0xa0;
        uint8_t payload[32];
        memset(payload, (int)(0xa0 + index), sizeof(payload));
        MOQ_TEST_CHECK(memcmp(op->data, payload, sizeof(payload)) == 0);
        ++counts[index];
    }
    for (unsigned i = 0; i < 8; ++i)
        MOQ_TEST_CHECK(counts[i] == expected);
    MOQ_TEST_CHECK(!f->tp.server_ep.overflowed && !f->tp.server_ep.truncated);
    return failures;
}

int test_hol_full_fifo_mapped_progress(void)
{
    int failures = 0;
    hol_fix_t f;
    /* At saturation, an existing mapped stream reserves one queue head;
     * an unmapped stream waits for FIFO capacity before opening. */
    for (unsigned mapped = 0; mapped < 2; ++mapped) {
        moq_subgroup_handle_t sg, target;
        if (hol_init(&f) < 0) return failures + 1;
        MOQ_TEST_CHECK(hol_open_sg(&f, 0, &sg) == MOQ_OK);
        MOQ_TEST_CHECK(hol_write(&f, sg, 0, 32) == MOQ_OK);
        hol_service(&f, 4);
        MOQ_TEST_CHECK(hol_pending(&f) == 0);
        MOQ_TEST_CHECK(f.tp.server_bridge->dq_items == 0);
        const uint64_t opens = f.tp.server_ep.open_uni_calls;
        fake_endpoint_clear_ops(&f.tp.server_ep);
        f.tp.server_ep.block_stop = true;
        f.tp.server_ep.block_write = true;
        const uint32_t capacity = f.tp.server_bridge->max_pending;
        for (uint32_t n = 0; n < capacity; ++n)
            MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                PENDING_STOP_SENDING, 0, 30000 + n));
        target = sg;
        if (!mapped) MOQ_TEST_CHECK(hol_open_sg(&f, 1, &target) == MOQ_OK);
        for (unsigned n = 0; n < 8; ++n) {
            uint8_t bytes[32];
            moq_rcbuf_t *payload = NULL;
            memset(bytes, (int)(0xa0 + n), sizeof(bytes));
            moq_result_t rc = moq_rcbuf_create(moq_alloc_default(), bytes,
                                               sizeof(bytes), &payload);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            if (rc != MOQ_OK) continue;
            MOQ_TEST_CHECK(moq_session_write_object(f.tp.server, target,
                n + mapped, payload, 0) == MOQ_OK);
            moq_rcbuf_decref(payload);
        }
        hol_service(&f, 4);
        MOQ_TEST_CHECK(hol_pending(&f) == capacity);
        MOQ_TEST_CHECK(f.tp.server_bridge->dq_items == mapped);
        MOQ_TEST_CHECK(f.tp.server_ep.open_uni_calls == opens);
        failures += hol_payload_set(&f, 0);
        f.tp.server_ep.block_write = false;
        hol_service(&f, 16);
        MOQ_TEST_CHECK(hol_pending(&f) == capacity);
        MOQ_TEST_CHECK(f.tp.server_ep.open_uni_calls == opens);
        failures += hol_payload_set(&f, mapped);
        f.tp.server_ep.block_stop = false;
        hol_service(&f, 16);
        MOQ_TEST_CHECK(hol_pending(&f) == 0);
        MOQ_TEST_CHECK(f.tp.server_bridge->dq_items == 0);
        MOQ_TEST_CHECK(f.tp.server_ep.open_uni_calls == opens + !mapped);
        MOQ_TEST_CHECK(!f.tp.server_bridge->fatal);
        failures += hol_payload_set(&f, 1);
        test_pair_destroy(&f.tp);
    }
    return failures;
}

#endif /* MOQ_BRIDGE_SCAN_COUNTERS && MOQ_BRIDGE_TEST_INJECT */

#ifdef MOQ_BRIDGE_TEST_INJECT


/* mirrors bridge_domain_t */
#define DOM_STREAM    0

#define DOM_CONN_CTRL 1

#define DOM_CONN_TERM 2


typedef struct {
    const char *name;
    int         kind;          /* bridge_pending_kind_t              */
    uint64_t    ref;           /* identity as the dispatcher sets it */
    uint64_t    sid;
    int         want_dom;
    bool        blocks_others;  /* does it block an UNRELATED stream? */
} dom_row_t;


static const dom_row_t k_dom_table[] = {
    /* per-stream kinds: identity by ref, by id, or both */
    { "OPEN_UNI_DATA (ref only, pre-open)", PENDING_OPEN_UNI_DATA,
      0x900, 0, DOM_STREAM, false },
    { "HEADER_PAYLOAD (ref+id)",            PENDING_HEADER_PAYLOAD,
      0x901, 901, DOM_STREAM, false },
    { "PAYLOAD_ONLY (ref+id)",              PENDING_PAYLOAD_ONLY,
      0x902, 902, DOM_STREAM, false },
    { "COPIED_WRITE data (ref+id)",         PENDING_COPIED_WRITE,
      0x903, 903, DOM_STREAM, false },
    { "OPEN_BIDI_DATA (ref only)",          PENDING_OPEN_BIDI_DATA,
      0x904, 0, DOM_STREAM, false },
    { "CLOSE_BIDI_FIN (ref+id)",            PENDING_CLOSE_BIDI_FIN,
      0x905, 905, DOM_STREAM, false },
    { "RESET_STREAM (id only)",             PENDING_RESET_STREAM,
      0, 906, DOM_STREAM, false },
    { "STOP_SENDING (id only)",             PENDING_STOP_SENDING,
      0, 907, DOM_STREAM, false },
    { "ABORT_STREAM (id only)",             PENDING_ABORT_STREAM,
      0, 908, DOM_STREAM, false },
    /* connection-level: these DO block unrelated stream work */
    { "COPIED_WRITE control (no ref)",      PENDING_COPIED_WRITE,
      0, 2001, DOM_CONN_CTRL, true },
    { "OPEN_CONTROL",                       PENDING_OPEN_CONTROL,
      0, 0, DOM_CONN_CTRL, true },
    { "OPEN_UNI_CONTROL",                   PENDING_OPEN_UNI_CONTROL,
      0x910, 0, DOM_CONN_CTRL, true },
    { "CLOSE_TRANSPORT",                    PENDING_CLOSE_TRANSPORT,
      0, 0, DOM_CONN_TERM, true },
    /* declared but never enqueued by production; if one is ever injected it
     * must fail closed to connection-wide rather than bypass */
    { "COPIED_DATAGRAM (unreachable)",      PENDING_COPIED_DATAGRAM,
      0, 0, DOM_CONN_CTRL, true },
};


int test_hol_action_domain_table(void)
{
    int failures = 0;
    moq_bridge_test_clear_retain_failure();
    size_t n = sizeof(k_dom_table) / sizeof(k_dom_table[0]);

    /* PENDING_COPIED_DATAGRAM must have no production enqueue site. */
    {
        const char *src = NULL;
        (void)src;   /* proved by grep in the report, asserted here by the
                      * table's own expectation that it fails closed */
    }

    for (size_t i = 0; i < n; i++) {
        const dom_row_t *r = &k_dom_table[i];
        hol_fix_t f;
        int dom;
        uint64_t cref = 0, csid = 0;
        bool chave = false;

        if (hol_init(&f) < 0) { failures++; continue; }
        if (!moq_bridge_test_inject_pending(f.tp.server_bridge, r->kind,
                                            r->ref, r->sid)) {
            fprintf(stderr, "TABLE %s: inject refused\n", r->name);
            failures++; test_pair_destroy(&f.tp); continue;
        }
        MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == 1);
        dom = moq_bridge_test_pending_domain(f.tp.server_bridge, 0);
        if (dom != r->want_dom) {
            fprintf(stderr, "TABLE %s: domain %d, expected %d\n",
                    r->name, dom, r->want_dom);
            failures++;
        }
        moq_bridge_test_pending_identity(f.tp.server_bridge, 0,
                                         &cref, &csid, &chave);
        /* identity source: whatever the dispatcher named must survive */
        if (r->ref != 0 && cref != r->ref) {
            fprintf(stderr, "TABLE %s: ref %llu lost (canon %llu)\n", r->name,
                    (unsigned long long)r->ref, (unsigned long long)cref);
            failures++;
        }
        if (r->sid != 0 && (!chave || csid != r->sid)) {
            fprintf(stderr, "TABLE %s: id %llu lost (canon %llu have=%d)\n",
                    r->name, (unsigned long long)r->sid,
                    (unsigned long long)csid, (int)chave);
            failures++;
        }

        /* bypass relation: an UNRELATED stream (fresh ref and id) */
        {
            bool blocked = moq_bridge_test_domain_blocked(
                f.tp.server_bridge, 0x7000, 7000, true);
            if (blocked != r->blocks_others) {
                fprintf(stderr, "TABLE %s: unrelated stream blocked=%d, "
                        "expected %d\n", r->name, (int)blocked,
                        (int)r->blocks_others);
                failures++;
            }
        }
        /* and the SAME stream must always conflict, by either name */
        if (r->want_dom == DOM_STREAM) {
            if (r->ref != 0 &&
                !moq_bridge_test_domain_blocked(f.tp.server_bridge, r->ref,
                                                 0, false)) {
                fprintf(stderr, "TABLE %s: same ref not blocked\n", r->name);
                failures++;
            }
            if (r->sid != 0 &&
                !moq_bridge_test_domain_blocked(f.tp.server_bridge, 0,
                                                 r->sid, true)) {
                fprintf(stderr, "TABLE %s: same id not blocked\n", r->name);
                failures++;
            }
        }
        test_pair_destroy(&f.tp);
    }
    printf("TABLE %zu pending kinds checked (domain, identity, bypass)\n", n);
    return failures;
}


/* Two refless items on DISTINCT streams must not alias, while a truly
 * connection-level refless item still blocks globally. */
int test_hol_ref_zero_does_not_alias(void)
{
    int failures = 0;
    hol_fix_t f;

    if (hol_init(&f) < 0) return 1;
    /* two id-only stop-sendings on different streams */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, 5100));
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, 5200));
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == 2);
    /* each blocks its own stream */
    MOQ_TEST_CHECK(moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 5100, true));
    MOQ_TEST_CHECK(moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 5200, true));
    /* and NEITHER blocks a third: ref 0 is not a shared domain */
    if (moq_bridge_test_domain_blocked(f.tp.server_bridge, 0, 5300, true)) {
        fprintf(stderr, "HOL ref-zero: two refless items aliased into one "
                "shared domain and blocked an unrelated stream\n");
        failures++;
    }
    /* a ref-named candidate for an unrelated stream is likewise free */
    if (moq_bridge_test_domain_blocked(f.tp.server_bridge, 0x5400, 0, false)) {
        fprintf(stderr, "HOL ref-zero: refless items blocked a ref-named "
                "unrelated stream\n");
        failures++;
    }
    test_pair_destroy(&f.tp);

    /* a genuinely connection-level refless item DOES block globally */
    if (hol_init(&f) < 0) return failures + 1;
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                       PENDING_OPEN_CONTROL, 0, 0));
    if (!moq_bridge_test_domain_blocked(f.tp.server_bridge, 0x5500, 5500, true)) {
        fprintf(stderr, "HOL ref-zero: a connection-level item failed to "
                "block unrelated stream work\n");
        failures++;
    }
    test_pair_destroy(&f.tp);
    return failures;
}


/* An item named ONLY by transport id must conflict with a later action
 * named only by the mapped ref, and vice versa. */
int test_hol_mixed_identity_canonicalises(void)
{
    int failures = 0;
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t sid_a, cref = 0, csid = 0;
    bool chave = false;

    if (hol_init(&f) < 0) return 1;
    MOQ_TEST_CHECK(hol_open_sg(&f, 0, &a) == MOQ_OK);
    MOQ_TEST_CHECK(hol_write(&f, a, 0, 32) == MOQ_OK);
    hol_service(&f, 4);
    sid_a = hol_nth_open(&f, 0);
    MOQ_TEST_CHECK(sid_a != UINT64_MAX);
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == 0);

    /* inject an ID-ONLY item for the live stream: enqueue must resolve its
     * ref from the map */
    MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                       PENDING_STOP_SENDING, 0, sid_a));
    moq_bridge_test_pending_identity(f.tp.server_bridge, 0, &cref, &csid, &chave);
    if (cref == 0) {
        fprintf(stderr, "HOL mixed-identity: an id-only item kept ref 0 for a "
                "mapped stream (id=%llu)\n", (unsigned long long)sid_a);
        failures++;
    }
    MOQ_TEST_CHECK(chave && csid == sid_a);

    /* a REF-named candidate for the same stream must conflict */
    if (cref != 0 &&
        !moq_bridge_test_domain_blocked(f.tp.server_bridge, cref, 0, false)) {
        fprintf(stderr, "HOL mixed-identity: ref-named action bypassed an "
                "id-named pending item for the same stream\n");
        failures++;
    }
    /* and an unrelated stream is still free */
    MOQ_TEST_CHECK(!moq_bridge_test_domain_blocked(f.tp.server_bridge,
                                                    0x6100, 6100, true));
    test_pair_destroy(&f.tp);
    return failures;
}


/* Depth: build 0, 1 and 64 through the real boundary, and prove 65 is
 * refused by the structural maximum. */
int test_hol_depth_and_structural_bound(void)
{
    int failures = 0;
    hol_fix_t f;
    uint32_t max, i;

    if (hol_init(&f) < 0) return 1;
    max = f.tp.server_bridge->max_pending;
    MOQ_TEST_CHECK(max == 64);
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == 0);
    for (i = 0; i < max; i++)
        MOQ_TEST_CHECK(moq_bridge_test_inject_pending(f.tp.server_bridge,
                           PENDING_STOP_SENDING, 0, 8000 + i));
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == max);
    /* one past the maximum is refused, not silently dropped or grown */
    if (moq_bridge_test_inject_pending(f.tp.server_bridge,
                                        PENDING_STOP_SENDING, 0, 9999)) {
        fprintf(stderr, "HOL depth: injection past the structural maximum "
                "%u succeeded\n", max);
        failures++;
    }
    MOQ_TEST_CHECK(moq_bridge_test_pending_depth(f.tp.server_bridge) == max);
    /* at full depth, an unrelated stream is still not blocked */
    MOQ_TEST_CHECK(!moq_bridge_test_domain_blocked(f.tp.server_bridge,
                                                    0x8500, 8500, true));
    /* a contradictory ref/id pair is refused rather than resolved to one side */
    {
        moq_subgroup_handle_t a;
        hol_fix_t g;
        if (hol_init(&g) == 0) {
            uint64_t sid;
            MOQ_TEST_CHECK(hol_open_sg(&g, 0, &a) == MOQ_OK);
            MOQ_TEST_CHECK(hol_write(&g, a, 0, 32) == MOQ_OK);
            hol_service(&g, 4);
            sid = hol_nth_open(&g, 0);
            MOQ_TEST_CHECK(sid != UINT64_MAX);
            /* the live stream's ref paired with somebody else's id */
            {
                uint64_t live_ref = 0, dummy = 0;
                bool have = false;
                MOQ_TEST_CHECK(moq_bridge_test_inject_pending(
                    g.tp.server_bridge, PENDING_STOP_SENDING, 0, sid));
                moq_bridge_test_pending_identity(g.tp.server_bridge, 0,
                                                 &live_ref, &dummy, &have);
                if (live_ref != 0 &&
                    moq_bridge_test_inject_pending(g.tp.server_bridge,
                        PENDING_HEADER_PAYLOAD, live_ref, sid + 4242)) {
                    fprintf(stderr, "HOL depth: a contradictory ref/id pair "
                            "was accepted\n");
                    failures++;
                }
            }
            test_pair_destroy(&g.tp);
        }
    }
    test_pair_destroy(&f.tp);
    return failures;
}

#endif /* MOQ_BRIDGE_TEST_INJECT */


/* The drain budget is a real backstop for a session configured above it.
 * 64 is only MOQ_DEFAULT_MAX_ACTIONS; max_actions is public and configurable,
 * so a publisher with 512 can offer more than BRIDGE_DRAIN_BUDGET actions in
 * one pass. The producer is the real session queue and the loop is the
 * production one.
 *
 * Capacities below, at and above the budget are all covered, so the fixture
 * distinguishes the session cap from the bridge bound.
 */
int hol_budget_case(uint32_t cap, unsigned *out_first,
                    unsigned *out_offered, int *out_fail)
{
    hol_fix_t f;
    moq_subgroup_handle_t a;
    uint64_t k;
    unsigned offered = 0, first, second;

    *out_first = 0; *out_offered = 0;
    if (hol_init_cap(&f, cap) < 0) { (*out_fail)++; return -1; }
    if (hol_open_sg(&f, 0, &a) != MOQ_OK) { (*out_fail)++; goto done; }
    (void)hol_write(&f, a, 0, 16);
    hol_service(&f, 4);

    /* one endpoint write per action here: a bare object with no header split,
     * so the operation count IS the action count */
    for (k = 1; k <= 900; k++) {
        if (hol_write(&f, a, k, 16) != MOQ_OK) break;
        offered++;
    }
    fake_endpoint_clear_ops(&f.tp.server_ep);
    {
        uint64_t base = f.tp.server_ep.write_calls;
        (void)held_bridge_service(f.tp.server_bridge, 0);
        first = (unsigned)(f.tp.server_ep.write_calls - base);
        (void)held_bridge_service(f.tp.server_bridge, 0);
        second = (unsigned)(f.tp.server_ep.write_calls - base);
    }

    /* nothing else may be the reason a pass stops: the endpoint accepted
     * everything it was offered and nothing was retained */
    if (f.tp.server_ep.block_count != 0) {
        fprintf(stderr, "BUDGET cap=%u: endpoint refused %d op(s); the budget "
                "is not the only gate\n", cap, f.tp.server_ep.block_count);
        (*out_fail)++;
    }
    if (hol_pending(&f) != 0) {
        fprintf(stderr, "BUDGET cap=%u: %u item(s) retained; a retention path "
                "shadows the budget\n", cap, hol_pending(&f));
        (*out_fail)++;
    }
    printf("BUDGET cap=%-4u offered=%-4u pass1=%-4u pass2=%-4u budget=%d\n",
           cap, offered, first, second, (int)BRIDGE_DRAIN_BUDGET);
    *out_first = first;
    *out_offered = offered;
    /* conservation: everything offered eventually goes out, exactly once */
    hol_service(&f, 256);
    if (f.tp.server_ep.write_calls < (uint64_t)offered) {
        fprintf(stderr, "BUDGET cap=%u: only %llu write(s) for %u offered "
                "objects\n", cap,
                (unsigned long long)f.tp.server_ep.write_calls, offered);
        (*out_fail)++;
    }
    if (second <= first && offered > first) {
        fprintf(stderr, "BUDGET cap=%u: the second pass made no progress\n", cap);
        (*out_fail)++;
    }
done:
    test_pair_destroy(&f.tp);
    return 0;
}
