/*
 * Retained catalog bootstrap through the production relay binding, driven
 * by the publisher/subscriber FACADES (the shapes an origin and an MSF-01
 * consumer actually use): an origin facade retains the catalog generation
 * {0: base, 1: delta}; a subscriber facade behind the relay subscribes
 * LargestObject and issues a Relative Joining FETCH(0). The direct control
 * (no relay) and the relay rows must observe the same exact contract.
 *
 * Lives outside relay/ because the relay sources themselves never include
 * the facades (relay boundary); this is an end-to-end scenario over the
 * public relay API.
 */
#include <moq/relay/relay.h>
#include <moq/relay/moqr_bind.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/publisher.h>
#include <moq/subscriber.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* counting allocator */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
    long attempts;   /* every alloc/realloc attempt (success or injected fail) */
    long fail_at;    /* fail EXACTLY the fail_at-th attempt; 0 = never. Test-
                      * local OOM injection; the single failure lets later
                      * cleanup allocations still succeed.                     */
    size_t fail_size;/* fail the FIRST alloc of EXACTLY this many bytes, once
                      * (cleared on firing); 0 = never. Targets a distinctly-
                      * sized allocation (the FETCH coalesce buffer) where an
                      * attempt index is not predictable through the SimPair.   */
} ca_t;
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* injected OOM at this attempt index */
    }
    if (a->fail_size != 0 && n == a->fail_size) {
        a->fail_size = 0;   /* one-shot: later cleanup allocs succeed */
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* realloc failure: caller keeps the old buffer p */
    }
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* -- rig: SimPair conns attached to the production binding ------------------ */

#define MAX_CONNS 8

typedef struct conn {
    bool           used;
    moq_simpair_t *sp;
    moq_session_t *peer;    /* client side: the test's endpoint          */
    moq_session_t *rsess;   /* server side: the relay's session          */
} conn_t;

typedef struct rig {
    ca_t         *alloc;
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
    conn_t        conns[MAX_CONNS];
    uint64_t      now;
    int           failures;
} rig_t;

#define R_CHECK(rig, expr)                                                \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);        \
            (rig)->failures++;                                            \
        }                                                                 \
    } while (0)

static conn_t *
rig_connect(rig_t *r, moq_version_t version)
{
    int slot = -1;
    for (int i = 0; i < MAX_CONNS; i++) {
        if (!r->conns[i].used) {
            slot = i;
            break;
        }
    }
    if (slot < 0) {
        return NULL;
    }
    conn_t *cn = &r->conns[slot];
    memset(cn, 0, sizeof(*cn));

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc->vt;
    cfg.seed = 0x5EED0000u + (uint64_t)slot;
    cfg.version = version;
    uint64_t init_cap = 1024;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = init_cap;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = init_cap;
    if (moq_simpair_create(&cfg, &cn->sp) != MOQ_OK) {
        return NULL;
    }
    cn->peer = moq_simpair_client(cn->sp);
    cn->rsess = moq_simpair_server(cn->sp);
    if (moq_simpair_start(cn->sp) != MOQ_OK ||
        moqr_bind_conn_open(r->bind, cn->rsess, version) != MOQR_OK) {
        moq_simpair_destroy(cn->sp);
        return NULL;
    }
    cn->used = true;
    return cn;
}

/* One deterministic pump cycle: transport steps, then the production
 * binding does everything else (events -> core -> intents -> deliveries). */
static void
rig_cycle(rig_t *r)
{
    r->now += 1000;
    for (int i = 0; i < MAX_CONNS; i++) {
        conn_t *cn = &r->conns[i];
        if (!cn->used) {
            continue;
        }
        (void)moq_simpair_advance_to(cn->sp, r->now);
        size_t steps = 0;
        (void)moq_simpair_run_until_quiescent(cn->sp, 64, &steps);
    }
    (void)moqr_bind_pump(r->bind, r->now);
}

static void
rig_pump(rig_t *r, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        rig_cycle(r);
    }
}

static moqr_result_t
rig_create(rig_t *r, ca_t *a)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->now = 1;
    if (moqr_trace_create(&a->vt, 512, &r->trace) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = r->trace;
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 500;
    if (moqr_core_create(&cfg, &r->core) != MOQR_OK) {
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), &a->vt);
    bcfg.core = r->core;
    bcfg.max_conns = MAX_CONNS;
    if (moqr_bind_create(&bcfg, &r->bind) != MOQR_OK) {
        moqr_core_destroy(r->core);
        moqr_trace_destroy(r->trace);
        return MOQR_ERR_NOMEM;
    }
    return MOQR_OK;
}

static void
rig_destroy(rig_t *r)
{
    moqr_bind_destroy(r->bind);
    moqr_core_destroy(r->core);
    for (int i = 0; i < MAX_CONNS; i++) {
        if (r->conns[i].used) {
            moq_simpair_destroy(r->conns[i].sp);
            r->conns[i].used = false;
        }
    }
    moqr_trace_destroy(r->trace);
}

/* ---- Retained catalog bootstrap through the relay ------------------------- *
 * MSF-01 §5: a catalog subscriber uses SUBSCRIBE + Joining FETCH(offset 0) to
 * obtain the latest complete catalog. The origin (publisher facade) holds the
 * current generation ONLY as a retained group (base object 0 + one delta
 * object 1) installed before the downstream joins; the relay cache starts
 * cold. Expected contract: the downstream LargestObject SUBSCRIBE succeeds and
 * the Relative Joining FETCH(0) delivers exactly the retained objects, in
 * order, with their exact bytes, followed by FETCH_COMPLETE. */
#define BOOT_BASE_LEN  48
#define BOOT_DELTA_LEN 24

#define BOOT_LEDGER_MAX 8
typedef struct boot_fetch_obs {
    int      ok, error, objects, gaps, complete, other;
    uint64_t error_code;
    bool     ok_eot;
    uint64_t ok_end_group, ok_end_object;
    uint64_t obj_group[8], obj_id[8];
    size_t   obj_len[8];
    uint8_t  obj_first[8];
    bool     obj_exact[8];     /* bytes == expected payload for that id */
    /* Bounded ordered ledger of EVERY item kind polled for the request, in
     * arrival order; sticky failures for overflow and for items that name a
     * request other than the one under test. */
    int      ledger[BOOT_LEDGER_MAX];
    int      ledger_n;
    bool     overflow;
    bool     unexpected_request;
} boot_fetch_obs_t;

static void
boot_fill(uint8_t *buf, size_t len, uint8_t seed)
{
    for (size_t i = 0; i < len; i++) {
        buf[i] = (uint8_t)(seed + (uint8_t)i);
    }
}

static bool
boot_payload_is(const moq_rcbuf_t *pl, uint8_t seed, size_t len)
{
    if (pl == NULL || moq_rcbuf_len(pl) != len) {
        return false;
    }
    uint8_t want[64];
    boot_fill(want, len, seed);
    return memcmp(moq_rcbuf_data(pl), want, len) == 0;
}

/* Drain every queued fetch item of `req` into obs (exact bytes checked
 * against the retained base/delta seeds). Other requests are ignored. */
static void
boot_poll_fetch(moq_subscriber_t *sub, const moq_sub_fetch_req_t *req,
                boot_fetch_obs_t *o)
{
    moq_sub_fetch_item_t it;
    while (moq_sub_poll_fetch(sub, &it) == MOQ_OK) {
        if (it.request != req) {
            o->unexpected_request = true;
        } else {
            if (o->ledger_n < BOOT_LEDGER_MAX) {
                o->ledger[o->ledger_n++] = (int)it.kind;
            } else {
                o->overflow = true;
            }
            switch (it.kind) {
            case MOQ_SUB_FETCH_OK:
                o->ok++;
                o->ok_eot = it.u.ok.end_of_track;
                o->ok_end_group = it.u.ok.end_group;
                o->ok_end_object = it.u.ok.end_object;
                break;
            case MOQ_SUB_FETCH_ERROR:
                o->error++;
                o->error_code = it.u.error.error_code;
                break;
            case MOQ_SUB_FETCH_OBJECT:
                if (o->objects < 8) {
                    int i = o->objects;
                    o->obj_group[i] = it.u.object.group_id;
                    o->obj_id[i] = it.u.object.object_id;
                    o->obj_len[i] = it.u.object.payload
                                        ? moq_rcbuf_len(it.u.object.payload) : 0;
                    o->obj_first[i] = o->obj_len[i]
                                          ? moq_rcbuf_data(it.u.object.payload)[0] : 0;
                    o->obj_exact[i] =
                        it.u.object.object_id == 0
                            ? boot_payload_is(it.u.object.payload, 0xB0, BOOT_BASE_LEN)
                            : boot_payload_is(it.u.object.payload, 0xD0, BOOT_DELTA_LEN);
                }
                o->objects++;
                break;
            case MOQ_SUB_FETCH_GAP:
                o->gaps++;
                break;
            case MOQ_SUB_FETCH_COMPLETE:
                o->complete++;
                break;
            default:
                o->other++;
                break;
            }
        }
        moq_sub_fetch_item_cleanup(&it);
    }
}

/* The exact expected contract for one Relative Joining FETCH(0) against the
 * retained generation: OK{eot=false, end 0/2} -> object (0,0) base bytes ->
 * object (0,1) delta bytes -> COMPLETE, nothing else, no foreign request,
 * no ledger overflow. Returns the number of violated checks and prints each. */
static int
boot_check_contract(const boot_fetch_obs_t *o, const char *label)
{
    int bad = 0;
#define BOOT_EXPECT(cond)                                                 \
    do {                                                                  \
        if (!(cond)) {                                                    \
            printf("FAIL: %s:%d: %s: %s\n", __FILE__, __LINE__, label, #cond); \
            bad++;                                                        \
        }                                                                 \
    } while (0)
    BOOT_EXPECT(!o->unexpected_request);
    BOOT_EXPECT(!o->overflow);
    BOOT_EXPECT(o->ledger_n == 4);
    BOOT_EXPECT(o->ledger_n >= 1 && o->ledger[0] == (int)MOQ_SUB_FETCH_OK);
    BOOT_EXPECT(o->ledger_n >= 2 && o->ledger[1] == (int)MOQ_SUB_FETCH_OBJECT);
    BOOT_EXPECT(o->ledger_n >= 3 && o->ledger[2] == (int)MOQ_SUB_FETCH_OBJECT);
    BOOT_EXPECT(o->ledger_n >= 4 && o->ledger[3] == (int)MOQ_SUB_FETCH_COMPLETE);
    BOOT_EXPECT(o->ok == 1 && o->error == 0 && o->gaps == 0 && o->other == 0);
    BOOT_EXPECT(o->ok == 1 && !o->ok_eot && o->ok_end_group == 0 && o->ok_end_object == 2);
    BOOT_EXPECT(o->objects == 2);
    BOOT_EXPECT(o->objects >= 1 && o->obj_group[0] == 0 && o->obj_id[0] == 0 &&
                o->obj_len[0] == BOOT_BASE_LEN && o->obj_exact[0]);
    BOOT_EXPECT(o->objects >= 2 && o->obj_group[1] == 0 && o->obj_id[1] == 1 &&
                o->obj_len[1] == BOOT_DELTA_LEN && o->obj_exact[1]);
    BOOT_EXPECT(o->complete == 1);
#undef BOOT_EXPECT
    return bad;
}

/* Origin publisher facade on `session`: one advertised track ns/name with the
 * retained generation {0: base, 1: delta} installed before any join. */
static moq_publisher_t *
boot_origin_create(ca_t *a, moq_session_t *session, const moq_namespace_t *ns,
                   moq_bytes_t name, moq_pub_track_t **track_out, bool *ok)
{
    *ok = false;
    moq_pub_cfg_t pcfg;
    moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
    pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_result_t crc = moq_pub_create(session, &a->vt, &pcfg, &pub);
    if (crc != MOQ_OK) {
        printf("  boot_origin_create: moq_pub_create rc=%d\n", (int)crc);
        return NULL;
    }
    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
    tcfg.track_namespace = *ns;
    tcfg.track_name = name;
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_result_t trc = moq_pub_add_track(pub, &tcfg, 1, &track);
    if (trc != MOQ_OK) {
        printf("  boot_origin_create: moq_pub_add_track rc=%d\n", (int)trc);
        moq_pub_destroy(pub);
        return NULL;
    }
    uint8_t base[BOOT_BASE_LEN], delta[BOOT_DELTA_LEN];
    boot_fill(base, sizeof(base), 0xB0);
    boot_fill(delta, sizeof(delta), 0xD0);
    moq_rcbuf_t *pb = NULL, *pd = NULL;
    if (moq_rcbuf_create(&a->vt, base, sizeof(base), &pb) != MOQ_OK ||
        moq_rcbuf_create(&a->vt, delta, sizeof(delta), &pd) != MOQ_OK) {
        if (pb) moq_rcbuf_decref(pb);
        moq_pub_destroy(pub);
        return NULL;
    }
    moq_pub_retained_object_t objs[2] = {
        { .object_id = 0, .payload = pb, .properties = NULL, .end_of_group = false },
        { .object_id = 1, .payload = pd, .properties = NULL, .end_of_group = true },
    };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = objs;
    rg.object_count = 2;
    moq_result_t rrc = moq_pub_set_retained_group(pub, track, &rg);
    moq_rcbuf_decref(pb);
    moq_rcbuf_decref(pd);
    if (rrc != MOQ_OK) {
        printf("  boot_origin_create: moq_pub_set_retained_group rc=%d\n", (int)rrc);
        moq_pub_destroy(pub);
        return NULL;
    }
    *track_out = track;
    *ok = true;
    return pub;
}

/* Positive control, no relay: subscriber facade joined DIRECTLY to the origin
 * over one SimPair proves the retained generation is fetchable as such. */
static int
retained_bootstrap_direct_control(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    int failures = 0;
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &a.vt;
    cfg.seed = 0xB007u;
    cfg.version = version;
    /* Same request-credit grants as the rig's pairs: draft-16 needs the
     * peer's MAX_REQUEST_ID before a server-side request can be sent. */
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK || moq_simpair_start(sp) != MOQ_OK) {
        printf("FAIL: retained_bootstrap_direct_control simpair\n");
        return 1;
    }
    moq_session_t *origin = moq_simpair_server(sp);
    moq_session_t *client = moq_simpair_client(sp);
    moq_bytes_t nsp[2] = { B("boot"), B("cat") };
    moq_namespace_t ns = { .parts = nsp, .count = 2 };
    uint64_t now = 1;
    /* Establish the pair before any facade exists (same order as the relay
     * case, whose connections are pumped before the origin is created). */
    for (int i = 0; i < 8; i++) {
        now += 1000;
        (void)moq_simpair_advance_to(sp, now);
        size_t steps0 = 0;
        (void)moq_simpair_run_until_quiescent(sp, 64, &steps0);
    }
    moq_pub_track_t *track = NULL;
    bool ok = false;
    moq_publisher_t *pub = boot_origin_create(&a, origin, &ns, B("catalog"), &track, &ok);
    moq_sub_cfg_t scfg;
    moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    moq_subscriber_t *sub = NULL;
    moq_result_t src = ok ? moq_sub_create(client, &a.vt, &scfg, &sub) : MOQ_ERR_INVAL;
    if (!ok || src != MOQ_OK) {
        printf("FAIL: retained_bootstrap_direct_control facade setup (origin ok=%d, sub_create rc=%d, v%llu)\n",
               ok, (int)src, (unsigned long long)version);
        if (pub) moq_pub_destroy(pub);
        moq_simpair_destroy(sp);
        return 1;
    }
#define DIRECT_CYCLE()                                                    \
    do {                                                                  \
        now += 1000;                                                      \
        (void)moq_simpair_advance_to(sp, now);                            \
        size_t steps_ = 0;                                                \
        (void)moq_simpair_run_until_quiescent(sp, 64, &steps_);           \
        (void)moq_pub_tick(pub, now);                                     \
        (void)moq_sub_tick(sub, now);                                     \
    } while (0)
    for (int i = 0; i < 8; i++) DIRECT_CYCLE();
    moq_sub_track_cfg_t tcfg;
    moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace = ns;
    tcfg.track_name = B("catalog");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *st = NULL;
    if (moq_sub_subscribe(sub, &tcfg, now, &st) != MOQ_OK) { failures++; }
    for (int i = 0; i < 8 && !(st && moq_sub_track_is_active(st)); i++) DIRECT_CYCLE();
    if (!(st && moq_sub_track_is_active(st))) { printf("FAIL: direct control: subscribe not active\n"); failures++; }
    moq_sub_joining_fetch_cfg_t fcfg;
    moq_sub_joining_fetch_cfg_init(&fcfg);
    fcfg.track = st;
    fcfg.relative = true;
    fcfg.joining_start = 0;
    moq_sub_fetch_req_t *req = NULL;
    if (failures == 0 && moq_sub_joining_fetch(sub, &fcfg, now, &req) != MOQ_OK) {
        printf("FAIL: direct control: joining fetch not placed\n"); failures++;
    }
    boot_fetch_obs_t o;
    memset(&o, 0, sizeof(o));
    for (int i = 0; i < 12; i++) { DIRECT_CYCLE(); boot_poll_fetch(sub, req, &o); }
    failures += boot_check_contract(&o, version == MOQ_VERSION_DRAFT_16
                                            ? "direct control v16" : "direct control v18");
#undef DIRECT_CYCLE
    moq_sub_destroy(sub);
    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
    if (a.live != 0) { printf("FAIL: direct control leak live=%zu\n", a.live); failures++; }
    if (failures == 0) {
        printf("PASS: retained_bootstrap_direct_control v%llu\n", (unsigned long long)version);
    }
    return failures;
}

/* The relay case: origin publisher facade behind the relay (its SimPair's
 * client side is the origin, the server side is the relay's session), one
 * downstream subscriber facade on another connection. EXPECTED contract is the
 * same as the direct control; the actual outcome is classified separately. */
static int
retained_bootstrap_through_relay(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: retained_bootstrap_through_relay rig create\n");
        return 1;
    }
    conn_t *oc = rig_connect(&rig, version);    /* origin */
    conn_t *dc = rig_connect(&rig, version);    /* downstream */
    conn_t *lc = rig_connect(&rig, version);    /* live-path control subscriber */
    if (!(oc && dc && lc)) {
        printf("FAIL: retained_bootstrap_through_relay rig connect\n");
        rig_destroy(&rig);
        return 1;
    }
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("boot"), B("cat") };
    moq_namespace_t ns = { .parts = nsp, .count = 2 };
    moq_pub_track_t *track = NULL;
    bool ok = false;
    moq_publisher_t *pub = boot_origin_create(&a, oc->peer, &ns, B("catalog"), &track, &ok);
    R_CHECK(&rig, ok);
    moq_sub_cfg_t scfg;
    moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    moq_subscriber_t *sub = NULL, *lsub = NULL;
    R_CHECK(&rig, moq_sub_create(dc->peer, &a.vt, &scfg, &sub) == MOQ_OK);
    R_CHECK(&rig, moq_sub_create(lc->peer, &a.vt, &scfg, &lsub) == MOQ_OK);
    if (!ok || !sub || !lsub) {
        printf("FAIL: retained_bootstrap_through_relay facade setup\n");
        if (sub) moq_sub_destroy(sub);
        if (lsub) moq_sub_destroy(lsub);
        if (pub) moq_pub_destroy(pub);
        rig_destroy(&rig);
        return 1;
    }
#define RELAY_CYCLE()                                                     \
    do {                                                                  \
        rig_cycle(&rig);                                                  \
        (void)moq_pub_tick(pub, rig.now);                                 \
        (void)moq_sub_tick(sub, rig.now);                                 \
        (void)moq_sub_tick(lsub, rig.now);                                \
    } while (0)
    /* Namespace advertised by the facade and accepted by the relay. */
    for (int i = 0; i < 8 && !moq_pub_namespace_accepted(pub, track); i++) RELAY_CYCLE();
    R_CHECK(&rig, moq_pub_namespace_accepted(pub, track));
    /* Downstream LargestObject SUBSCRIBE through the relay. */
    moq_sub_track_cfg_t tcfg;
    moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace = ns;
    tcfg.track_name = B("catalog");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *st = NULL;
    R_CHECK(&rig, moq_sub_subscribe(sub, &tcfg, rig.now, &st) == MOQ_OK);
    for (int i = 0; i < 12 && !(st && moq_sub_track_is_active(st)); i++) RELAY_CYCLE();
    R_CHECK(&rig, st && moq_sub_track_is_active(st));   /* SUBSCRIBE_OK arrived */
    /* Relative Joining FETCH(0) on the established subscription. */
    moq_sub_joining_fetch_cfg_t fcfg;
    moq_sub_joining_fetch_cfg_init(&fcfg);
    fcfg.track = st;
    fcfg.relative = true;
    fcfg.joining_start = 0;
    moq_sub_fetch_req_t *req = NULL;
    R_CHECK(&rig, moq_sub_joining_fetch(sub, &fcfg, rig.now, &req) == MOQ_OK);
    boot_fetch_obs_t o;
    memset(&o, 0, sizeof(o));
    for (int i = 0; i < 16; i++) { RELAY_CYCLE(); boot_poll_fetch(sub, req, &o); }
    /* Classification of what actually happened (printed, not asserted). */
    char dump[4096];
    size_t dn = 0;
    dump[0] = '\0';
    moqr_result_t drc = moqr_core_route_dump_text(rig.core, dump, sizeof(dump), &dn);
    if (drc != MOQR_OK || dn >= sizeof(dump)) {
        dump[0] = '\0';          /* dump unavailable: classify as "no dump" */
    } else {
        dump[dn] = '\0';
    }
    const char *cat = strstr(dump, "\"catalog\"");
    const char *log = cat ? strstr(cat, "log:") : NULL;
    printf("ACTUAL v%llu: fetch ok=%d error=%d code=%llu objects=%d gaps=%d complete=%d "
           "ledger_n=%d unexpected_request=%d; relay %s%.*s\n",
           (unsigned long long)version, o.ok, o.error,
           (unsigned long long)o.error_code, o.objects, o.gaps, o.complete,
           o.ledger_n, o.unexpected_request, log ? "" : "(route dump unavailable)",
           log ? (int)strcspn(log, "\n") : 0, log ? log : "");
    /* EXPECTED contract (MSF-01 §5 / draft-16 §9.16.2 / draft-18 §10.12.2):
     * the exact retained generation delivered once, in order, then terminal
     * completion. Counted into the rig's failures so the row stays RED. */
    rig.failures += boot_check_contract(&o, version == MOQ_VERSION_DRAFT_16
                                                ? "through relay v16"
                                                : "through relay v18");
    /* Live-path control: the relay subscription/data path is alive. A second
     * origin track "live" (not the catalog: no catalog mutation) is subscribed
     * by a separate connection, then one live object flows origin->relay->
     * subscriber with exact bytes. */
    moq_pub_track_cfg_t ltcfg;
    moq_pub_track_cfg_init_sized(&ltcfg, sizeof(ltcfg));
    ltcfg.track_namespace = ns;
    ltcfg.track_name = B("live");
    moq_pub_track_t *ltrack = NULL;
    R_CHECK(&rig, moq_pub_add_track(pub, &ltcfg, rig.now, &ltrack) == MOQ_OK);
    moq_sub_track_cfg_t ltc;
    moq_sub_track_cfg_init(&ltc);
    ltc.track_namespace = ns;
    ltc.track_name = B("live");
    ltc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *lst = NULL;
    R_CHECK(&rig, moq_sub_subscribe(lsub, &ltc, rig.now, &lst) == MOQ_OK);
    for (int i = 0; i < 12 && !(lst && moq_sub_track_is_active(lst)); i++) RELAY_CYCLE();
    R_CHECK(&rig, lst && moq_sub_track_is_active(lst));
    for (int i = 0; i < 4 && ltrack && !moq_pub_has_subscriber(pub, ltrack); i++) RELAY_CYCLE();
    R_CHECK(&rig, ltrack && moq_pub_has_subscriber(pub, ltrack));
    {
        uint8_t body[32];
        boot_fill(body, sizeof(body), 0xC0);
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = 1;
        ocfg.object_id = 0;
        ocfg.payload = pl;
        ocfg.end_of_group = true;
        moq_result_t wrc = ltrack ? moq_pub_write_object_ex(pub, ltrack, &ocfg, rig.now)
                                  : MOQ_ERR_INVAL;
        moq_rcbuf_decref(pl);
        R_CHECK(&rig, wrc == MOQ_OK);
    }
    int live_objects = 0;
    bool live_exact = false;
    for (int i = 0; i < 12; i++) {
        RELAY_CYCLE();
        moq_sub_object_t obj;
        while (moq_sub_poll_object(lsub, &obj) == MOQ_OK) {
            if (obj.track == lst) {
                live_objects++;
                live_exact = obj.group_id == 1 && obj.object_id == 0 &&
                             boot_payload_is(obj.payload, 0xC0, 32);
            }
            moq_sub_object_cleanup(&obj);
        }
    }
    R_CHECK(&rig, live_objects == 1 && live_exact);
#undef RELAY_CYCLE
    moq_sub_destroy(sub);
    moq_sub_destroy(lsub);
    moq_pub_destroy(pub);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: retained_bootstrap_through_relay v%llu\n", (unsigned long long)version);
    }
    return f;
}

/* The campaign shape: TWO downstream receivers subscribe to the retained-only
 * catalog in the same cycle and each issues its Relative Joining FETCH(0); a
 * third connection carries the live-path control. EXPECTED (same contract as
 * the direct control, per receiver): the exact retained generation, once, in
 * order, then COMPLETE. The actual downstream answer is classified per
 * receiver and the relay log is dumped; the live track proves unrelated media
 * progress and the two LargestObject subscriptions prove unrelated request
 * progress on the same connections. */
static int
retained_bootstrap_two_receivers(moq_version_t version)
{
    ca_t a;
    ca_init(&a);
    rig_t rig;
    if (rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: retained_bootstrap_two_receivers rig create\n");
        return 1;
    }
    conn_t *oc = rig_connect(&rig, version);    /* origin */
    conn_t *d1 = rig_connect(&rig, version);    /* receiver A */
    conn_t *d2 = rig_connect(&rig, version);    /* receiver B */
    conn_t *lc = rig_connect(&rig, version);    /* live-path control subscriber */
    if (!(oc && d1 && d2 && lc)) {
        printf("FAIL: retained_bootstrap_two_receivers rig connect\n");
        rig_destroy(&rig);
        return 1;
    }
    rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("boot"), B("cat") };
    moq_namespace_t ns = { .parts = nsp, .count = 2 };
    moq_pub_track_t *track = NULL;
    bool ok = false;
    moq_publisher_t *pub = boot_origin_create(&a, oc->peer, &ns, B("catalog"), &track, &ok);
    R_CHECK(&rig, ok);
    moq_sub_cfg_t scfg;
    moq_sub_cfg_init_sized(&scfg, sizeof(scfg));
    moq_subscriber_t *s1 = NULL, *s2 = NULL, *lsub = NULL;
    R_CHECK(&rig, moq_sub_create(d1->peer, &a.vt, &scfg, &s1) == MOQ_OK);
    R_CHECK(&rig, moq_sub_create(d2->peer, &a.vt, &scfg, &s2) == MOQ_OK);
    R_CHECK(&rig, moq_sub_create(lc->peer, &a.vt, &scfg, &lsub) == MOQ_OK);
    if (!ok || !s1 || !s2 || !lsub) {
        printf("FAIL: retained_bootstrap_two_receivers facade setup\n");
        if (s1) moq_sub_destroy(s1);
        if (s2) moq_sub_destroy(s2);
        if (lsub) moq_sub_destroy(lsub);
        if (pub) moq_pub_destroy(pub);
        rig_destroy(&rig);
        return 1;
    }
#define TWO_CYCLE()                                                       \
    do {                                                                  \
        rig_cycle(&rig);                                                  \
        (void)moq_pub_tick(pub, rig.now);                                 \
        (void)moq_sub_tick(s1, rig.now);                                  \
        (void)moq_sub_tick(s2, rig.now);                                  \
        (void)moq_sub_tick(lsub, rig.now);                                \
    } while (0)
    for (int i = 0; i < 8 && !moq_pub_namespace_accepted(pub, track); i++) TWO_CYCLE();
    R_CHECK(&rig, moq_pub_namespace_accepted(pub, track));
    /* Both receivers subscribe in the SAME cycle (concurrent), LargestObject. */
    moq_sub_track_cfg_t tcfg;
    moq_sub_track_cfg_init(&tcfg);
    tcfg.track_namespace = ns;
    tcfg.track_name = B("catalog");
    tcfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *st1 = NULL, *st2 = NULL;
    R_CHECK(&rig, moq_sub_subscribe(s1, &tcfg, rig.now, &st1) == MOQ_OK);
    R_CHECK(&rig, moq_sub_subscribe(s2, &tcfg, rig.now, &st2) == MOQ_OK);
    for (int i = 0; i < 12 && !(st1 && st2 && moq_sub_track_is_active(st1) &&
                                moq_sub_track_is_active(st2)); i++) TWO_CYCLE();
    /* Unrelated request progress: both SUBSCRIBEs were accepted. */
    R_CHECK(&rig, st1 && moq_sub_track_is_active(st1));
    R_CHECK(&rig, st2 && moq_sub_track_is_active(st2));
    /* Each receiver issues its Relative Joining FETCH(0) in the same cycle. */
    moq_sub_joining_fetch_cfg_t fcfg;
    moq_sub_joining_fetch_cfg_init(&fcfg);
    fcfg.relative = true;
    fcfg.joining_start = 0;
    moq_sub_fetch_req_t *req1 = NULL, *req2 = NULL;
    fcfg.track = st1;
    R_CHECK(&rig, moq_sub_joining_fetch(s1, &fcfg, rig.now, &req1) == MOQ_OK);
    fcfg.track = st2;
    R_CHECK(&rig, moq_sub_joining_fetch(s2, &fcfg, rig.now, &req2) == MOQ_OK);
    boot_fetch_obs_t o1, o2;
    memset(&o1, 0, sizeof(o1));
    memset(&o2, 0, sizeof(o2));
    for (int i = 0; i < 16; i++) {
        TWO_CYCLE();
        boot_poll_fetch(s1, req1, &o1);
        boot_poll_fetch(s2, req2, &o2);
    }
    char dump[4096];
    size_t dn = 0;
    dump[0] = '\0';
    moqr_result_t drc = moqr_core_route_dump_text(rig.core, dump, sizeof(dump), &dn);
    if (drc != MOQR_OK || dn >= sizeof(dump)) dump[0] = '\0'; else dump[dn] = '\0';
    const char *cat = strstr(dump, "\"catalog\"");
    const char *log = cat ? strstr(cat, "log:") : NULL;
    printf("ACTUAL two-receivers v%llu: A{ok=%d error=%d code=%llu objects=%d complete=%d ledger_n=%d} "
           "B{ok=%d error=%d code=%llu objects=%d complete=%d ledger_n=%d}; relay %s%.*s\n",
           (unsigned long long)version,
           o1.ok, o1.error, (unsigned long long)o1.error_code, o1.objects, o1.complete, o1.ledger_n,
           o2.ok, o2.error, (unsigned long long)o2.error_code, o2.objects, o2.complete, o2.ledger_n,
           log ? "" : "(route dump unavailable)", log ? (int)strcspn(log, "\n") : 0, log ? log : "");
    rig.failures += boot_check_contract(&o1, version == MOQ_VERSION_DRAFT_16
                                                 ? "two receivers A v16" : "two receivers A v18");
    rig.failures += boot_check_contract(&o2, version == MOQ_VERSION_DRAFT_16
                                                 ? "two receivers B v16" : "two receivers B v18");
    /* Live-path control on a fourth connection, unchanged from the single-
     * receiver row: unrelated media progress through the same relay. */
    moq_pub_track_cfg_t ltcfg;
    moq_pub_track_cfg_init_sized(&ltcfg, sizeof(ltcfg));
    ltcfg.track_namespace = ns;
    ltcfg.track_name = B("live");
    moq_pub_track_t *ltrack = NULL;
    R_CHECK(&rig, moq_pub_add_track(pub, &ltcfg, rig.now, &ltrack) == MOQ_OK);
    moq_sub_track_cfg_t ltc;
    moq_sub_track_cfg_init(&ltc);
    ltc.track_namespace = ns;
    ltc.track_name = B("live");
    ltc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_sub_track_t *lst = NULL;
    R_CHECK(&rig, moq_sub_subscribe(lsub, &ltc, rig.now, &lst) == MOQ_OK);
    for (int i = 0; i < 12 && !(lst && moq_sub_track_is_active(lst)); i++) TWO_CYCLE();
    R_CHECK(&rig, lst && moq_sub_track_is_active(lst));
    for (int i = 0; i < 4 && ltrack && !moq_pub_has_subscriber(pub, ltrack); i++) TWO_CYCLE();
    R_CHECK(&rig, ltrack && moq_pub_has_subscriber(pub, ltrack));
    {
        uint8_t body[32];
        boot_fill(body, sizeof(body), 0xC0);
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        moq_pub_object_cfg_t ocfg;
        moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = 1;
        ocfg.object_id = 0;
        ocfg.payload = pl;
        ocfg.end_of_group = true;
        moq_result_t wrc = ltrack ? moq_pub_write_object_ex(pub, ltrack, &ocfg, rig.now)
                                  : MOQ_ERR_INVAL;
        moq_rcbuf_decref(pl);
        R_CHECK(&rig, wrc == MOQ_OK);
    }
    int live_objects = 0;
    bool live_exact = false;
    for (int i = 0; i < 12; i++) {
        TWO_CYCLE();
        moq_sub_object_t obj;
        while (moq_sub_poll_object(lsub, &obj) == MOQ_OK) {
            if (obj.track == lst) {
                live_objects++;
                live_exact = obj.group_id == 1 && obj.object_id == 0 &&
                             boot_payload_is(obj.payload, 0xC0, 32);
            }
            moq_sub_object_cleanup(&obj);
        }
    }
    R_CHECK(&rig, live_objects == 1 && live_exact);
#undef TWO_CYCLE
    moq_sub_destroy(s1);
    moq_sub_destroy(s2);
    moq_sub_destroy(lsub);
    moq_pub_destroy(pub);
    rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    int f = rig.failures;
    if (f == 0) {
        printf("PASS: retained_bootstrap_two_receivers v%llu\n", (unsigned long long)version);
    }
    return f;
}


int
main(void)
{
    int failures = 0;
    failures += retained_bootstrap_direct_control(MOQ_VERSION_DRAFT_16);
    failures += retained_bootstrap_direct_control(MOQ_VERSION_DRAFT_18);
    failures += retained_bootstrap_through_relay(MOQ_VERSION_DRAFT_16);
    failures += retained_bootstrap_through_relay(MOQ_VERSION_DRAFT_18);
    failures += retained_bootstrap_two_receivers(MOQ_VERSION_DRAFT_16);
    failures += retained_bootstrap_two_receivers(MOQ_VERSION_DRAFT_18);
    return failures == 0 ? 0 : 1;
}
