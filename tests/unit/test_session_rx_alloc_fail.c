#include "test_session_support.h"

/* One whole-object payload allocation that fails.
 *
 * A subgroup stream carries object 0 (properties, payload P) and object 1
 * (properties, payload "BBB") and its FIN, all in one delivery. P is the
 * exact encoding of a further object, so payload bytes reparsed as an object
 * header would be accepted as one. The allocation of object 0's payload is
 * refused, exactly once, and that refusal must fire exactly once in every
 * row. Each row runs on a fresh pair of the default (draft-16) profile:
 * - state: the call reports NOMEM and leaves the stream consistent: nothing
 *   of object 0 consumed or held, its header and everything after it staged,
 *   no properties buffer charged; and the blocks the call left live are
 *   exactly the stream's staging buffer and the Track's malformed-track
 *   record (made by the stream's first object header, kept by the Track),
 *   each identified by address and size, with nothing unexplained;
 * - empty advance: delivers object 0 and object 1 with their exact bytes,
 *   then the subgroup terminal, and frees the stream;
 * - reset: a peer RESET reports the subgroup reset with no completed object;
 * - destroy: destroying the sessions returns every allocation.
 * Every event and action a row polls is released, expected or not, and every
 * row ends with zero blocks and zero bytes outstanding.
 *
 * Each row prints its verdict: PASS, FAIL (a product check failed) or
 * PREREQUISITE FAILED (the fixture could not set the row up, or the targeted
 * refusal did not fire exactly once; the row then stops and proves nothing).
 * Exit status: 0 all rows pass, 1 a product check failed, 2 a prerequisite
 * failed (whatever else failed). */

static int failures = 0;

/* -- Row verdicts ---------------------------------------------------------- */

typedef struct af_row {
    const char *name;
    int         failures_at_start;
    bool        prereq;   /* a fixture prerequisite failed */
} af_row_t;

static int g_prereq_failures = 0;

static void af_row_begin(af_row_t *r, const char *name)
{
    r->name = name;
    r->failures_at_start = failures;
    r->prereq = false;
}

static void af_prereq(af_row_t *r, const char *what)
{
    fprintf(stderr, "PREREQUISITE FAILED: %s: %s\n", r->name, what);
    r->prereq = true;
}

static void af_row_end(const af_row_t *r)
{
    const char *v = r->prereq ? "PREREQUISITE FAILED"
                  : failures != r->failures_at_start ? "FAIL" : "PASS";
    if (r->prereq) g_prereq_failures++;
    printf("ROW %s: %s\n", r->name, v);
}

/* -- Counting allocator with one size-targeted refusal ---------------------- */

#define RXAF_TRACK 16

typedef struct af_alloc {
    int64_t  balance;
    int64_t  live;
    size_t   fail_size;   /* refuse the next request of exactly this size */
    uint64_t fail_hits;
    /* Blocks allocated while `tracking` is set and still live, by address
     * and size (a realloc of one keeps it recorded under its new address). */
    bool     tracking;
    bool     track_overflow;
    size_t   tracked;
    void    *track_ptr[RXAF_TRACK];
    size_t   track_size[RXAF_TRACK];
} af_alloc_t;

static int af_track_find(const af_alloc_t *a, const void *p)
{
    for (size_t i = 0; i < a->tracked; i++)
        if (a->track_ptr[i] == p) return (int)i;
    return -1;
}

static void af_track_add(af_alloc_t *a, void *p, size_t size)
{
    if (!a->tracking) return;
    if (a->tracked == RXAF_TRACK) {
        a->track_overflow = true;
        return;
    }
    a->track_ptr[a->tracked] = p;
    a->track_size[a->tracked] = size;
    a->tracked++;
}

static void af_track_drop(af_alloc_t *a, const void *p)
{
    int i = af_track_find(a, p);
    if (i < 0) return;
    a->tracked--;
    a->track_ptr[i] = a->track_ptr[a->tracked];
    a->track_size[i] = a->track_size[a->tracked];
}

static void *af_alloc_fn(size_t size, void *ctx)
{
    af_alloc_t *a = (af_alloc_t *)ctx;
    if (a->fail_size && size == a->fail_size) {
        a->fail_size = 0;
        a->fail_hits++;
        return NULL;
    }
    void *p = malloc(size ? size : 1);
    if (p) {
        a->balance++;
        a->live += (int64_t)size;
        af_track_add(a, p, size);
    }
    return p;
}

static void *af_realloc_fn(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    af_alloc_t *a = (af_alloc_t *)ctx;
    if (a->fail_size && new_sz == a->fail_size) {
        a->fail_size = 0;
        a->fail_hits++;
        return NULL;
    }
    /* Everything about the old block is read before the call: after a
     * successful realloc the old pointer is not evaluated again, not even
     * against NULL. A failed realloc leaves the ledger as it was. */
    bool had_ptr = ptr != NULL;
    int idx = had_ptr ? af_track_find(a, ptr) : -1;
    void *p = realloc(ptr, new_sz ? new_sz : 1);
    if (!p) return NULL;
    if (!had_ptr) {
        a->balance++;
        old_sz = 0;
        af_track_add(a, p, new_sz);
    } else if (idx >= 0) {
        a->track_ptr[idx] = p;
        a->track_size[idx] = new_sz;
    }
    a->live += (int64_t)new_sz - (int64_t)old_sz;
    return p;
}

static void af_free_fn(void *ptr, size_t size, void *ctx)
{
    af_alloc_t *a = (af_alloc_t *)ctx;
    if (ptr) {
        a->balance--;
        a->live -= (int64_t)size;
        af_track_drop(a, ptr);
    }
    free(ptr);
}

static test_alloc_state_t g_plain_state;
static moq_alloc_t g_plain;   /* the test's own payloads */

static const uint8_t g_kvp[] = { 0x02, 0x05 };   /* one property: type 2, value 5 */

#define RXAF_CAP 512
#define RXAF_REF 300u

typedef struct af_bytes {
    uint8_t b[RXAF_CAP];
    size_t  len;
    bool    fin;
} af_bytes_t;

typedef struct af_pair {
    af_alloc_t         as;
    moq_alloc_t        alloc;
    moq_session_t     *c, *sv;
    moq_subscription_t sub;
    moq_subscription_t ssub;
    moq_subgroup_handle_t sg;
    size_t             mt_before;   /* the client's malformed-track records */
} af_pair_t;

/* Poll and release every queued event and action of `s`; counts them. */
static void af_drain(moq_session_t *s, size_t *events, size_t *actions)
{
    size_t ne = 0, na = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        moq_event_cleanup(&ev);
        ne++;
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) == 1) {
        moq_action_cleanup(&a);
        na++;
    }
    if (events) *events = ne;
    if (actions) *actions = na;
}

static bool af_open(af_pair_t *p, moq_bytes_t track_name)
{
    memset(p, 0, sizeof(*p));
    p->alloc = (moq_alloc_t){ &p->as, af_alloc_fn, af_realloc_fn, af_free_fn };
    establish_pair(&p->alloc, 10, 10, &p->c, &p->sv, NULL, NULL);
    if (!p->c || !p->sv) return false;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = track_name;
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    if (moq_session_subscribe(p->c, &cfg, 0, &p->sub) != MOQ_OK) return false;
    pump_actions_to_peer(p->c, p->sv, 0);
    moq_event_t ev;
    if (moq_session_poll_events(p->sv, &ev, 1) != 1) return false;
    bool req = ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST;
    if (req) p->ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    if (!req) return false;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(p->sv, p->ssub, &ac, 0) != MOQ_OK) return false;
    pump_actions_to_peer(p->sv, p->c, 0);
    if (moq_session_poll_events(p->c, &ev, 1) != 1) return false;
    bool ok = ev.kind == MOQ_EVENT_SUBSCRIBE_OK;
    moq_event_cleanup(&ev);
    if (!ok) return false;
    /* Anything else setup left on the client is setup's, not the row's. */
    af_drain(p->c, NULL, NULL);
    moq_subgroup_cfg_t g;
    moq_subgroup_cfg_init(&g);
    g.object_properties = true;
    return moq_session_open_subgroup(p->sv, p->ssub, &g, 0, &p->sg) == MOQ_OK;
}

static void af_close(af_pair_t *p)
{
    if (p->c) moq_session_destroy(p->c);
    if (p->sv) moq_session_destroy(p->sv);
    p->c = p->sv = NULL;
}

/* Append every queued server SEND_DATA to `out`; every action is released. */
static bool af_collect(af_pair_t *p, af_bytes_t *out)
{
    bool ok = true;
    moq_action_t a;
    while (moq_session_poll_actions(p->sv, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            size_t hl = a.u.send_data.header_len;
            size_t pl = a.u.send_data.payload ? moq_rcbuf_len(a.u.send_data.payload) : 0;
            if (out->len + hl + pl > RXAF_CAP) {
                ok = false;
            } else {
                if (hl) memcpy(out->b + out->len, a.u.send_data.header, hl);
                if (pl) memcpy(out->b + out->len + hl, moq_rcbuf_data(a.u.send_data.payload), pl);
                out->len += hl + pl;
                if (a.u.send_data.fin) out->fin = true;
            }
        }
        moq_action_cleanup(&a);
    }
    return ok;
}

static bool af_write(af_pair_t *p, uint64_t id, const uint8_t *payload, size_t n)
{
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    if (moq_rcbuf_create(&g_plain, payload, n, &pl) != MOQ_OK) return false;
    if (moq_rcbuf_create(&g_plain, g_kvp, sizeof(g_kvp), &pr) != MOQ_OK) {
        moq_rcbuf_decref(pl);
        return false;
    }
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    oc.object_id = id;
    oc.payload = pl;
    oc.properties = pr;
    moq_result_t rc = moq_session_write_object_ex(p->sv, p->sg, &oc, 0);
    moq_rcbuf_decref(pl);
    moq_rcbuf_decref(pr);
    return rc == MOQ_OK;
}

/* P: the encoding of object 1 (properties, payload "CCC") following an
 * object 0 on the same stream. */
static af_bytes_t g_p;
/* The stream under test, and where object 0's header begins in it. */
static af_bytes_t g_stream;
static size_t g_obj0_off;
/* The refused request: the allocation size of object 0's payload. */
static size_t g_want;

static bool af_build(void)
{
    af_pair_t p;
    af_bytes_t skip = { .len = 0 };
    bool ok = af_open(&p, MOQ_BYTES_LITERAL("video")) && af_collect(&p, &skip) &&
              af_write(&p, 0, (const uint8_t *)"AAAAA", 5) && af_collect(&p, &skip) &&
              af_write(&p, 1, (const uint8_t *)"CCC", 3) && af_collect(&p, &g_p);
    af_close(&p);
    if (!ok || g_p.len == 0) return false;

    af_bytes_t hdr = { .len = 0 };
    ok = af_open(&p, MOQ_BYTES_LITERAL("video")) && af_collect(&p, &hdr) &&
         af_write(&p, 0, g_p.b, g_p.len) &&
         af_write(&p, 1, (const uint8_t *)"BBB", 3) &&
         moq_session_close_subgroup(p.sv, p.sg, 0) == MOQ_OK;
    g_stream = hdr;
    g_obj0_off = hdr.len;
    ok = ok && af_collect(&p, &g_stream);
    af_close(&p);
    return ok && g_stream.fin && g_obj0_off > 0 && g_stream.len > g_obj0_off;
}

/* The refused size must be no other allocation this delivery makes: not the
 * properties buffer, not object 1's payload, not a staging buffer of the
 * stream's length. */
static bool af_sizes_disjoint(void)
{
    size_t props = 0, obj1 = 0;
    if (moq_rcbuf_allocation_size(g_p.len, &g_want) != MOQ_OK ||
        moq_rcbuf_allocation_size(sizeof(g_kvp), &props) != MOQ_OK ||
        moq_rcbuf_allocation_size(3, &obj1) != MOQ_OK)
        return false;
    return g_want != props && g_want != obj1 && g_want != g_stream.len;
}

static const moq_rx_stream_t *af_entry(const moq_session_t *s)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (s->rx_streams[i].active && s->rx_streams[i].stream_ref._v == RXAF_REF)
            return &s->rx_streams[i];
    return NULL;
}

/* A fresh pair whose client has just had object 0's payload allocation
 * refused; `*rc` is that delivery's result. False (with the row marked and
 * nothing dereferenced) when the fixture could not get there: setup, the
 * server's subgroup header, or a refusal that did not fire exactly once, or
 * that hit the staging buffer. */
static bool af_prepare(af_row_t *row, af_pair_t *p, moq_result_t *rc, int64_t *before)
{
    if (!af_open(p, MOQ_BYTES_LITERAL("video"))) {
        af_prereq(row, "pair setup");
        return false;
    }
    af_bytes_t hdr = { .len = 0 };
    if (!af_collect(p, &hdr) || hdr.len != g_obj0_off ||
        memcmp(hdr.b, g_stream.b, g_obj0_off) != 0) {
        af_prereq(row, "the server's subgroup header differs from the built stream");
        return false;
    }
    *before = p->as.balance;
    p->mt_before = p->c->mt_count;
    p->as.tracked = 0;
    p->as.track_overflow = false;
    p->as.tracking = true;
    p->as.fail_size = g_want;
    *rc = moq_session_on_data_bytes(p->c, moq_stream_ref_from_u64(RXAF_REF),
                                    g_stream.b, g_stream.len, true, 0);
    p->as.tracking = false;
    bool consumed = p->as.fail_size == 0;
    p->as.fail_size = 0;
    if (p->as.fail_hits != 1 || !consumed) {
        af_prereq(row, "the targeted refusal did not fire exactly once");
        return false;
    }
    const moq_rx_stream_t *x = af_entry(p->c);
    if (x && x->input_cap == g_want) {
        af_prereq(row, "the refused size equals the staging capacity");
        return false;
    }
    return true;
}

/* End a row: the targeted refusal fired once and never again, then both
 * sessions are destroyed and nothing may remain, neither blocks nor bytes. */
static void af_finish(af_row_t *row, af_pair_t *p)
{
    if (!row->prereq && p->as.fail_hits != 1)
        af_prereq(row, "the targeted refusal fired again");
    af_close(p);
    if (row->prereq) {
        if (p->as.balance != 0 || p->as.live != 0)
            fprintf(stderr, "note: %s: %lld blocks, %lld bytes left after a failed prerequisite\n",
                    row->name, (long long)p->as.balance, (long long)p->as.live);
        return;
    }
    MOQ_TEST_CHECK(p->as.balance == 0);
    MOQ_TEST_CHECK(p->as.live == 0);
}

static bool af_bytes_eq(const moq_rcbuf_t *b, const uint8_t *want, size_t n)
{
    return b && moq_rcbuf_len(b) == n && (n == 0 || memcmp(moq_rcbuf_data(b), want, n) == 0);
}

/* `ev` is OBJECT_RECEIVED for `id` with exactly these payload bytes and the
 * properties; `sole` also requires the payload to be owned by the event only. */
static bool af_is_object(const moq_event_t *ev, uint64_t id, const uint8_t *payload,
                         size_t n, bool sole)
{
    if (ev->kind != MOQ_EVENT_OBJECT_RECEIVED) return false;
    const moq_object_received_event_t *o = &ev->u.object_received;
    return o->object_id == id && af_bytes_eq(o->payload, payload, n) &&
           af_bytes_eq(o->properties, g_kvp, sizeof(g_kvp)) &&
           (!sole || moq_rcbuf_refcount(o->payload) == 1);
}

/* The canonical key length of the Track a client subscription names, by the
 * rule the malformed-track record takes its key with (the request's track
 * id, else its history's key); 0 when unresolved. */
static size_t af_track_key_len(moq_session_t *c, moq_subscription_t sub)
{
    int slot = test_sub_resolve_handle(c, sub);
    if (slot < 0) return 0;
    const moq_sub_entry_t *e = &c->subs[slot];
    if (e->track_id_buf && e->track_id_len > 0) return e->track_id_len;
    if (e->hist && e->hist->key_len > 0) return e->hist->key_len;
    return 0;
}

/* The malformed-track record's size less its key length, measured on a
 * second pair whose subscription names a Track with a key of another length
 * ("v" rather than "video"), fed the same stream with no refusal. The state
 * row's record must be this base plus its own key length: the record's size
 * follows its key exactly as it is allocated. */
static size_t g_mt_base;
static size_t g_cal_key_len;

static bool af_calibrate(af_row_t *row)
{
    af_pair_t p;
    bool ok = false;
    if (!af_open(&p, MOQ_BYTES_LITERAL("v"))) {
        af_prereq(row, "calibration pair setup");
    } else {
        af_bytes_t hdr = { .len = 0 };
        if (!af_collect(&p, &hdr) || hdr.len != g_obj0_off ||
            memcmp(hdr.b, g_stream.b, g_obj0_off) != 0) {
            af_prereq(row, "the calibration subgroup header differs from the built stream");
        } else {
            p.as.tracked = 0;
            p.as.track_overflow = false;
            p.as.tracking = true;
            moq_result_t rc = moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(RXAF_REF),
                                                        g_stream.b, g_stream.len, true, 0);
            p.as.tracking = false;
            int rec = af_track_find(&p.as, p.c->mt_head);
            size_t key = af_track_key_len(p.c, p.sub);
            if (rc != MOQ_OK || p.c->mt_count != 1 || rec < 0 || p.as.track_overflow ||
                key == 0 || p.as.track_size[rec] <= key) {
                af_prereq(row, "the calibration record was not observed");
            } else {
                g_mt_base = p.as.track_size[rec] - key;
                g_cal_key_len = key;
                ok = true;
            }
            af_drain(p.c, NULL, NULL);
        }
    }
    af_close(&p);
    if (ok && (p.as.balance != 0 || p.as.live != 0)) {
        af_prereq(row, "the calibration pair left blocks behind");
        ok = false;
    }
    return ok;
}

/* Each block the targeted delivery left live, named: the stream's staging
 * buffer, the Track's malformed-track record, the held properties buffer
 * (never acceptable), or UNEXPLAINED. Printed when the inventory fails. */
static const char *af_block_name(const af_pair_t *p, const moq_rx_stream_t *x, size_t i)
{
    const void *b = p->as.track_ptr[i];
    if (x && b == (const void *)x->input_buf) return "staging";
    if (b == (const void *)p->c->mt_head) return "mt-record";
    if (x && x->cur_extensions && b == (const void *)x->cur_extensions)
        return "properties (held)";
    return "UNEXPLAINED";
}

static void af_inventory_dump(const char *row, const af_pair_t *p, const moq_rx_stream_t *x)
{
    for (size_t i = 0; i < p->as.tracked; i++)
        fprintf(stderr, "INVENTORY %s: block %zu: size %zu: %s\n", row, i,
                p->as.track_size[i], af_block_name(p, x, i));
    if (p->as.track_overflow)
        fprintf(stderr, "INVENTORY %s: more blocks than the ledger holds\n", row);
    fprintf(stderr, "INVENTORY %s: record base %zu, key %zu, staging capacity %zu\n", row,
            g_mt_base, af_track_key_len(p->c, p->sub), x ? x->input_cap : (size_t)0);
}

/* -- Rows ------------------------------------------------------------------- */

/* The state the refused allocation leaves: nothing emitted, object 0 not
 * consumed and nothing of it held. */
static void af_row_state(void)
{
    af_row_t row;
    af_row_begin(&row, "state");
    if (!af_calibrate(&row)) {
        af_row_end(&row);
        return;
    }
    af_pair_t p;
    moq_result_t rc = MOQ_OK;
    int64_t before = 0;
    if (af_prepare(&row, &p, &rc, &before) &&
        af_track_key_len(p.c, p.sub) == g_cal_key_len)
        af_prereq(&row, "the calibration key length equals the row's");
    if (!row.prereq) {
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        size_t ne = 0, na = 0;
        af_drain(p.c, &ne, &na);
        MOQ_TEST_CHECK_EQ_SIZE(ne, 0);
        MOQ_TEST_CHECK_EQ_SIZE(na, 0);
        const moq_rx_stream_t *x = af_entry(p.c);
        MOQ_TEST_CHECK(x != NULL);
        if (x) {
            size_t staged = g_stream.len - g_obj0_off;
            MOQ_TEST_CHECK(x->parse_state == MOQ_RX_AWAITING_OBJECT);
            MOQ_TEST_CHECK(x->pending_fin);
            MOQ_TEST_CHECK(x->cur_extensions == NULL);
            MOQ_TEST_CHECK(x->payload_rcbuf == NULL);
            MOQ_TEST_CHECK_EQ_SIZE(x->input_len, staged);
            MOQ_TEST_CHECK(x->input_buf && x->input_len == staged &&
                           memcmp(x->input_buf, g_stream.b + g_obj0_off, staged) == 0);
        }
        MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, 0);
        /* The exact inventory of blocks the call left live: every new block
         * is in the ledger, and the ledger holds the stream's staging buffer
         * (its address and capacity) and the Track's new malformed-track
         * record (the session's newest record, of the base size plus its
         * own key length) and nothing else -- no properties buffer, nothing
         * unexplained. */
        int fails_before = failures;
        size_t key_len = af_track_key_len(p.c, p.sub);
        int staging = x ? af_track_find(&p.as, x->input_buf) : -1;
        int record = af_track_find(&p.as, p.c->mt_head);
        size_t unexplained = 0;
        for (size_t i = 0; i < p.as.tracked; i++)
            if ((int)i != staging && (int)i != record) unexplained++;
        MOQ_TEST_CHECK(!p.as.track_overflow);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)(p.as.balance - before), (uint64_t)p.as.tracked);
        MOQ_TEST_CHECK_EQ_SIZE(unexplained, 0);
        MOQ_TEST_CHECK_EQ_SIZE(p.as.tracked, 2);
        MOQ_TEST_CHECK(staging >= 0 && x && p.as.track_size[staging] == x->input_cap);
        MOQ_TEST_CHECK(record >= 0 && record != staging);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->mt_count, p.mt_before + 1);
        MOQ_TEST_CHECK(key_len > 0 && record >= 0 &&
                       p.as.track_size[record] == g_mt_base + key_len);
        if (failures != fails_before) af_inventory_dump(row.name, &p, x);
    }
    af_finish(&row, &p);
    af_row_end(&row);
}

/* An empty advance completes the stream with the exact objects. */
static void af_row_advance(void)
{
    af_row_t row;
    af_row_begin(&row, "empty-advance");
    af_pair_t p;
    moq_result_t rc = MOQ_OK;
    int64_t before = 0;
    if (af_prepare(&row, &p, &rc, &before)) {
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_on_data_bytes(p.c, moq_stream_ref_from_u64(RXAF_REF),
                                                 NULL, 0, false, 0) == MOQ_OK);
        size_t n = 0;
        bool o0 = false, o1 = false, fin = false;
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) == 1) {
            if (n == 0) o0 = af_is_object(&ev, 0, g_p.b, g_p.len, true);
            else if (n == 1) o1 = af_is_object(&ev, 1, (const uint8_t *)"BBB", 3, false);
            else if (n == 2)
                fin = ev.kind == MOQ_EVENT_SUBGROUP_FINISHED &&
                      ev.u.subgroup_finished.has_last_object &&
                      ev.u.subgroup_finished.last_object_id == 1;
            moq_event_cleanup(&ev);
            n++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(n, 3);
        MOQ_TEST_CHECK(o0);
        MOQ_TEST_CHECK(o1);
        MOQ_TEST_CHECK(fin);
        size_t na = 0;
        af_drain(p.c, NULL, &na);
        MOQ_TEST_CHECK_EQ_SIZE(na, 0);
        MOQ_TEST_CHECK(af_entry(p.c) == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, 0);
    }
    af_finish(&row, &p);
    af_row_end(&row);
}

/* A peer RESET reports the subgroup reset; no object had completed. Any
 * action it queues is released. */
static void af_row_reset(void)
{
    af_row_t row;
    af_row_begin(&row, "reset");
    af_pair_t p;
    moq_result_t rc = MOQ_OK;
    int64_t before = 0;
    if (af_prepare(&row, &p, &rc, &before)) {
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_on_data_reset(p.c, moq_stream_ref_from_u64(RXAF_REF),
                                                 5, 0) == MOQ_OK);
        size_t n = 0;
        bool reset = false;
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) == 1) {
            if (n == 0)
                reset = ev.kind == MOQ_EVENT_SUBGROUP_RESET &&
                        ev.u.subgroup_reset.error_code == 5 &&
                        !ev.u.subgroup_reset.has_last_object;
            moq_event_cleanup(&ev);
            n++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(n, 1);
        MOQ_TEST_CHECK(reset);
        af_drain(p.c, NULL, NULL);
        MOQ_TEST_CHECK(af_entry(p.c) == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(p.c->recv_payload_bytes, 0);
    }
    af_finish(&row, &p);
    af_row_end(&row);
}

/* Destroying the sessions right after the refusal returns every allocation. */
static void af_row_destroy(void)
{
    af_row_t row;
    af_row_begin(&row, "destroy");
    af_pair_t p;
    moq_result_t rc = MOQ_OK;
    int64_t before = 0;
    if (af_prepare(&row, &p, &rc, &before))
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
    af_finish(&row, &p);
    af_row_end(&row);
}

int main(void)
{
    static const char *const rows[] = { "state", "empty-advance", "reset", "destroy" };
    g_plain = test_allocator(&g_plain_state);
    bool built = af_build();
    if (!built || !af_sizes_disjoint()) {
        fprintf(stderr, "PREREQUISITE FAILED: %s\n",
                built ? "the refused size is not disjoint from the delivery's other sizes"
                      : "building the stream");
        for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++)
            printf("ROW %s: PREREQUISITE FAILED\n", rows[i]);
        return 2;
    }
    af_row_state();
    af_row_advance();
    af_row_reset();
    af_row_destroy();
    MOQ_TEST_CHECK(g_plain_state.balance == 0);   /* the test's own buffers */
    if (g_prereq_failures) return 2;
    MOQ_TEST_PASS("session_rx_alloc_fail");
    return failures ? 1 : 0;
}
