/* A standalone FETCH answered across shards states the owner's Track
 * Properties in the requester's draft. The origin and a prime subscriber are
 * on shard 0, which owns the track; the FETCH lands on shard 1, which holds
 * no copy of it. The owner fixes the answer's set when it admits the
 * request; the requester converts it for its connection or, when it cannot
 * be stated, refuses the request before any FETCH_OK. Type 0x10 = 1000 is
 * 10 43 E8 in draft-16 and 10 83 E8 in vi64.
 *
 *   same          d22 origin -> d22 fetcher, byte-exact
 *   convert       d16 origin -> d22 fetcher, converted
 *   to16          d22 origin -> d16 fetcher, converted to QUIC varints
 *   to18          d16 origin -> d18 fetcher, converted to vi64
 *   immutable     d16 Immutable entry the vi64 draft would read differently:
 *                 FETCH_ERROR, no FETCH_OK
 *   oom           the owner's copy of the set cannot be allocated: the same
 *                 refusal, nothing leaks
 *   parked        the owner admits, its answer waits at the held requester
 *                 shard, the owner's latest set is replaced: the FETCH_OK
 *                 states the set admitted, not the newer one
 *   cancel        the fetcher cancels while the answer waits: no session
 *                 closes, nothing leaks
 *   plainNN-local, plainNN-cross
 *                 no properties, a draft-NN fetcher on the owner shard or the
 *                 other one
 *   ceiling       three FETCHes of a 2000-byte set over two-entry channels,
 *                 the owner-to-requester one filled by a remote subscriber's
 *                 live Objects: the owner keeps the copies whose OPEN push is
 *                 refused, each
 *                 copy is only ever held by one slot (one reference), one
 *                 FETCH is cancelled, and the runtime's peak allocation stays
 *                 within the ceiling moqr_shards_capacity_describe reports
 *
 * The rig runs every shard on one thread, so these rows show who owns each
 * copy at each step; they do not exercise concurrent reference counting. */
#define main relay_shard_fill_main
#include "test_relay_shard_fill.c"
#undef main
#include "../src/bind/moqr_bind_metadata.h"
#include <moq/relay/moqr_bind_test.h>
#include <moq/relay/moqr_shards_test.h>

static const uint8_t spx_q1000[] = { 0x10, 0x43, 0xE8 };
static const uint8_t spx_v1000[] = { 0x10, 0x83, 0xE8 };
static const uint8_t spx_q2000[] = { 0x10, 0x47, 0xD0 };
/* Immutable holding Type 122 = 2 (40 7a 02), one byte shorter in vi64. */
static const uint8_t spx_qimm[] = { 0x0B, 0x03, 0x40, 0x7A, 0x02 };
/* Type 0x11 holding 41 bytes: 43 bytes, a size nothing else here allocates. */
static uint8_t spx_qbig[2 + 41];

typedef struct spx_obs {
    int fetch_ok, objects, complete, error, reset, closed;
    uint64_t error_code;
    uint8_t ok_props[16];   /* the first bytes */
    size_t ok_len;
} spx_obs_t;

static void
spx_drain(sfconn_t *cn, spx_obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK:
            o->fetch_ok++;
            o->ok_len = ev.u.fetch_ok.track_properties.len;
            if (o->ok_len != 0)
                memcpy(o->ok_props, ev.u.fetch_ok.track_properties.data,
                       o->ok_len < sizeof(o->ok_props) ? o->ok_len : sizeof(o->ok_props));
            break;
        case MOQ_EVENT_FETCH_OBJECT: o->objects++; break;
        case MOQ_EVENT_FETCH_COMPLETE: o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR:
            o->error++;
            o->error_code = ev.u.fetch_error.error_code;
            break;
        case MOQ_EVENT_FETCH_RESET: o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED: o->closed++; break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
}

typedef struct spx_rig {
    ca_t a;
    sfrig_t r;
    sfconn_t *origin, *prime, *fetcher;
    sfx_peer_t po, pp;
    spx_obs_t fo;
} spx_rig_t;

static void
spx_pump(spx_rig_t *x, int n)
{
    for (int i = 0; i < n; i++) {
        sfrig_pump(&x->r, 1);
        sfx_drain(x->origin, &x->po);
        sfx_drain(x->prime, &x->pp);
        spx_drain(x->fetcher, &x->fo);
    }
}

/* The owner's track, through a core-only observer on shard 0. */
static bool
spx_owner_track(spx_rig_t *x, moqr_track_t *track, uint64_t *gen)
{
    moqr_core_t *core = moqr_shards_core(x->r.s, 0);
    moqr_binding_t observer;
    if (moqr_core_binding_open(core, 903, &observer) != MOQR_OK) return false;
    moq_bytes_t ns[] = { B("props"), B("live") };
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = (moqr_ns_t){ .parts = ns, .count = 2 };
    req.name = B("video");
    req.forward = false;
    moqr_sub_t sub = MOQR_SUB_INVALID;
    if (moqr_core_subscribe(core, observer, &req, &sub) != MOQR_OK) return false;
    moqr_result_t rc = moqr_core_sub_track(core, sub, track, gen);
    (void)moqr_core_unsubscribe(core, sub, x->r.now);
    return rc == MOQR_OK;
}

static int
spx_row(const char *row)
{
    bool same = strcmp(row, "same") == 0;
    bool to16 = strcmp(row, "to16") == 0, to18 = strcmp(row, "to18") == 0;
    /* plainNN-{local,cross}: no properties, a draft-NN fetcher on the owner
     * shard or the other one. */
    bool plain = strncmp(row, "plain", 5) == 0;
    moq_version_t pv = plain && strncmp(row + 5, "16", 2) == 0 ? MOQ_VERSION_DRAFT_16
                     : plain && strncmp(row + 5, "18", 2) == 0 ? MOQ_VERSION_DRAFT_18
                                                               : MOQ_VERSION_DRAFT_22;
    uint16_t fshard = plain && strstr(row, "local") != NULL ? 0 : 1;
    bool imm = strcmp(row, "immutable") == 0, oom = strcmp(row, "oom") == 0;
    bool parked = strcmp(row, "parked") == 0, cancel = strcmp(row, "cancel") == 0;
    moq_version_t ov = same || plain || to16 ? MOQ_VERSION_DRAFT_22 : MOQ_VERSION_DRAFT_16;
    moq_version_t fv = plain ? pv : to16 ? MOQ_VERSION_DRAFT_16
                                  : to18 ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
    moq_bytes_t src = ov == MOQ_VERSION_DRAFT_16
                          ? (moq_bytes_t){ spx_q1000, sizeof(spx_q1000) }
                          : (moq_bytes_t){ spx_v1000, sizeof(spx_v1000) };
    const uint8_t *want = to16 ? spx_q1000 : spx_v1000;
    if (imm) src = (moq_bytes_t){ spx_qimm, sizeof(spx_qimm) };
    if (plain) src = (moq_bytes_t){ NULL, 0 };
    if (oom) {
        spx_qbig[0] = 0x11; spx_qbig[1] = 41;
        for (int i = 0; i < 41; i++) spx_qbig[2 + i] = (uint8_t)(0x40 + i);
        src = (moq_bytes_t){ spx_qbig, sizeof(spx_qbig) };
    }
    spx_rig_t x;
    memset(&x, 0, sizeof(x));
    ca_init(&x.a);
    moq_rcbuf_t *payload = NULL;
    const char *missing = NULL;
#define SPX_FIX(e) do { if (!(e)) { \
    printf("FIXTURE: props_shard(%s):%d: %s\n", row, __LINE__, #e); x.r.failures++; \
    goto cleanup; } } while (0)
#define SPX_NEED(e, label) do { if (!(e) && missing == NULL) missing = label; } while (0)
    SPX_FIX(sfrig_create(&x.r, &x.a, true) == MOQR_OK);
    g_sf_version = ov;
    x.origin = sfrig_connect(&x.r, 0);
    x.prime = sfrig_connect(&x.r, 0);
    g_sf_version = fv;
    x.fetcher = sfrig_connect(&x.r, fshard);
    g_sf_version = MOQ_VERSION_DRAFT_22;
    SPX_FIX(x.origin != NULL && x.prime != NULL && x.fetcher != NULL &&
            x.fetcher->shard == fshard);
    spx_pump(&x, 4);

    moq_bytes_t ns[] = { B("props"), B("live") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    moq_announcement_t ann;
    SPX_FIX(moq_session_publish_namespace(x.origin->peer, &pcfg, x.r.now, &ann) == MOQ_OK);
    spx_pump(&x, 8);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t psub;
    SPX_FIX(moq_session_subscribe(x.prime->peer, &scfg, x.r.now, &psub) == MOQ_OK);
    spx_pump(&x, 8);
    SPX_FIX(x.po.requests == 1);
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.track_properties = src;
    SPX_FIX(moq_session_accept_subscribe(x.origin->peer, x.po.sub, &acfg, x.r.now) == MOQ_OK);
    spx_pump(&x, 8);
    SPX_FIX(x.pp.acks == 1);
    static const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    SPX_FIX(moq_rcbuf_create(&x.a.vt, body, sizeof(body), &payload) == MOQ_OK);
    SPX_FIX(moq_session_send_object_datagram(x.origin->peer, x.po.sub, 0, 0, 90, false,
                                             payload, NULL, 0, x.r.now) == MOQ_OK);
    moq_rcbuf_decref(payload);
    payload = NULL;
    spx_pump(&x, 8);
    SPX_FIX(x.pp.objects == 1);

    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    fc.track_name = B("video");
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 1;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    moq_fetch_t fetch;
    if (parked || cancel) x.r.frozen = 1u;   /* the owner holds the request */
    if (oom) x.a.fail_size = sizeof(spx_qbig);   /* the owner's copy of the set */
    SPX_FIX(moq_session_fetch(x.fetcher->peer, &fc, x.r.now, &fetch) == MOQ_OK);
    if (parked || cancel) {
        spx_pump(&x, 4);              /* the requester routes it to the owner */
        x.r.frozen = 2u;              /* then holds the answer */
        spx_pump(&x, 4);              /* the owner admits and answers */
        SPX_FIX(x.fo.fetch_ok == 0 && x.fo.error == 0);
        {
            /* OPEN is pushed: the copy is the channel's, no slot holds it. */
            uint32_t oh, rh, refs;
            moqr_shards_debug_retrieval_meta(x.r.s, 0, &oh, &rh, &refs);
            uint32_t oh1, rh1, refs1;
            moqr_shards_debug_retrieval_meta(x.r.s, 1, &oh1, &rh1, &refs1);
            SPX_FIX(oh == 0 && rh == 0 && oh1 == 0 && rh1 == 0);
        }
        if (parked) {
            moqr_track_t track;
            uint64_t gen = 0;
            SPX_FIX(spx_owner_track(&x, &track, &gen));
            moqr_bind_t *bind = moqr_shards_bind(x.r.s, 0);
            moqr_bind_meta_ref_t latest = {0};
            SPX_FIX(moqr_bind_meta_latest(bind, track, &latest) == MOQR_OK &&
                    latest.state == MOQR_BIND_META_PRESENT);
            moqr_bind_meta_ref_t newer = latest;
            newer.bytes = (moq_bytes_t){ spx_q2000, sizeof(spx_q2000) };
            uint64_t rev = 0;
            moqr_result_t rrc = moqr_bind_meta_replace(bind, track, gen, &newer, x.r.now,
                                                       &rev);
            moqr_bind_meta_release(bind, &latest);
            SPX_FIX(rrc == MOQR_OK && rev != 0);
        } else {
            SPX_FIX(moq_session_fetch_cancel(x.fetcher->peer, fetch, x.r.now) == MOQ_OK);
        }
        x.r.frozen = 0;
    }
    spx_pump(&x, 24);
    for (uint16_t sh = 0; sh < 2; sh++) {
        uint32_t oh, rh, refs;
        moqr_shards_debug_retrieval_meta(x.r.s, sh, &oh, &rh, &refs);
        SPX_FIX(oh == 0 && rh == 0);   /* every copy released */
    }
    x.a.fail_size = 0;

    printf("props_shard(%s): fetch_ok=%d ok_len=%zu objects=%d complete=%d error=%d "
           "code=%llu reset=%d closed=%d\n", row, x.fo.fetch_ok, x.fo.ok_len, x.fo.objects,
           x.fo.complete, x.fo.error, (unsigned long long)x.fo.error_code, x.fo.reset,
           x.fo.closed + x.po.closed + x.pp.closed);
    SPX_NEED(x.fo.closed + x.po.closed + x.pp.closed == 0 &&
             moq_session_state(x.fetcher->peer) == MOQ_SESS_ESTABLISHED, "sessions-live");
    if (imm || oom) {
        SPX_NEED(x.fo.fetch_ok == 0 && x.fo.objects == 0 && x.fo.error == 1 &&
                 x.fo.error_code == MOQ_REQUEST_ERROR_INTERNAL_ERROR, "refused-before-ok");
    } else if (cancel) {
        SPX_NEED(x.fo.complete == 0 && x.fo.error == 0, "cancelled-quietly");
    } else {
        SPX_NEED(x.fo.fetch_ok == 1 && x.fo.complete == 1 && x.fo.objects == 1,
                 "answered");
        if (!plain)
            SPX_NEED(x.fo.ok_len == 3 && memcmp(x.fo.ok_props, want, 3) == 0,
                     parked ? "admitted-set-not-newer" : "owner-set-stated");
    }
cleanup:
    moq_rcbuf_decref(payload);
    if (x.r.s != NULL) {
        sf_expect_channels_idle(&x.r);
        sfrig_destroy(&x.r);
    }
    int errors = x.r.failures + (x.a.live != 0);
    printf("%s: props_shard(%s) causal=%s fixture_failures=%d live=%ld\n",
           errors ? "FIXTURE" : missing != NULL ? "FAIL" : "PASS", row,
           missing != NULL ? missing : "none", errors, x.a.live);
#undef SPX_FIX
#undef SPX_NEED
    return errors ? 2 : missing != NULL ? 1 : 0;
}

static int
spx_ceiling(void)
{
    const char *row = "ceiling";
    enum { N_FETCH = 3, SET_LEN = 2000 };
    static uint8_t big[SET_LEN];
    /* Type 0x11 holding 1997 bytes; its vi64 Length is 87 CD. */
    big[0] = 0x11; big[1] = 0x87; big[2] = 0xCD;
    for (int i = 3; i < SET_LEN; i++) big[i] = (uint8_t)i;
    ca_t ra, sa;   /* the runtime's allocator; the sessions' */
    ca_init(&ra);
    ca_init(&sa);
    sfrig_t r;
    memset(&r, 0, sizeof(r));
    sfx_peer_t po, pp;
    memset(&po, 0, sizeof(po));
    memset(&pp, 0, sizeof(pp));
    spx_obs_t fo[N_FETCH];
    memset(fo, 0, sizeof(fo));
    sfconn_t *origin = NULL, *prime = NULL, *fetcher[N_FETCH] = { NULL };
    moq_rcbuf_t *payload = NULL;
    const char *missing = NULL;
    uint32_t max_owner = 0, max_requester = 0, max_refs = 0;
#define SPX_FIX(e) do { if (!(e)) { \
    printf("FIXTURE: props_shard(%s):%d: %s\n", row, __LINE__, #e); r.failures++; \
    goto cleanup; } } while (0)
#define SPX_NEED(e, label) do { if (!(e) && missing == NULL) missing = label; } while (0)
    moqr_shards_cfg_t cfg;
    sfrig_cfg(&cfg, &ra, true, 2, 4);
    moqr_shards_capacity_t cap;
    SPX_FIX(moqr_shards_capacity_describe(&cfg, &cap) == MOQR_OK);
    SPX_FIX(sfrig_create_ex(&r, &ra, true, 2, 4) == MOQR_OK);
    r.alloc = &sa;   /* sessions apart from the runtime's ceiling */
    origin = sfrig_connect(&r, 0);
    prime = sfrig_connect(&r, 1);   /* its Objects cross owner -> requester */
    for (int k = 0; k < N_FETCH; k++) fetcher[k] = sfrig_connect(&r, 1);
    SPX_FIX(origin != NULL && prime != NULL && fetcher[N_FETCH - 1] != NULL);
#define SPX_PUMP(n) do { for (int i_ = 0; i_ < (n); i_++) { sfrig_pump(&r, 1); \
    sfx_drain(origin, &po); sfx_drain(prime, &pp); \
    for (int k_ = 0; k_ < N_FETCH; k_++) spx_drain(fetcher[k_], &fo[k_]); \
    for (uint16_t sh_ = 0; sh_ < 2; sh_++) { uint32_t oh_, rh_, rf_; \
        moqr_shards_debug_retrieval_meta(r.s, sh_, &oh_, &rh_, &rf_); \
        if (sh_ == 0 && oh_ > max_owner) max_owner = oh_; \
        if (sh_ == 1 && rh_ > max_requester) max_requester = rh_; \
        if (rf_ > max_refs) max_refs = rf_; } } } while (0)
    SPX_PUMP(4);
    moq_bytes_t ns[] = { B("props"), B("live") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    moq_announcement_t ann;
    SPX_FIX(moq_session_publish_namespace(origin->peer, &pcfg, r.now, &ann) == MOQ_OK);
    SPX_PUMP(8);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    scfg.track_name = B("video");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t psub;
    SPX_FIX(moq_session_subscribe(prime->peer, &scfg, r.now, &psub) == MOQ_OK);
    SPX_PUMP(8);
    SPX_FIX(po.requests == 1);
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.track_properties = (moq_bytes_t){ big, sizeof(big) };
    SPX_FIX(moq_session_accept_subscribe(origin->peer, po.sub, &acfg, r.now) == MOQ_OK);
    SPX_PUMP(8);
    SPX_FIX(pp.acks == 1);
    static const uint8_t body[] = { 0x44, 0x00, 0xab, 0x19 };
    SPX_FIX(moq_rcbuf_create(&sa.vt, body, sizeof(body), &payload) == MOQ_OK);
    SPX_FIX(moq_session_send_object_datagram(origin->peer, po.sub, 0, 0, 90, false,
                                             payload, NULL, 0, r.now) == MOQ_OK);
    moq_rcbuf_decref(payload);
    payload = NULL;
    SPX_PUMP(8);
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init_sized(&fc, sizeof(fc));
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    fc.track_name = B("video");
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 1;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    moq_fetch_t fh[N_FETCH];
    for (int k = 0; k < N_FETCH; k++)
        SPX_FIX(moq_session_fetch(fetcher[k]->peer, &fc, r.now, &fh[k]) == MOQ_OK);
    /* The owner is held while the requester routes the FETCHes (two fit
     * its channel); then the requester is held, live Objects fill the
     * owner's channel to it, and the owner admits: its OPEN pushes are
     * refused and it keeps their copies. */
    r.frozen = 1u;
    SPX_PUMP(6);
    r.frozen = 2u;
    for (uint64_t g = 1; g <= 2; g++) {
        SPX_FIX(moq_rcbuf_create(&sa.vt, body, sizeof(body), &payload) == MOQ_OK);
        SPX_FIX(moq_session_send_object_datagram(origin->peer, po.sub, g, 0, 90, false,
                                                 payload, NULL, 0, r.now) == MOQ_OK);
        moq_rcbuf_decref(payload);
        payload = NULL;
        SPX_PUMP(2);
    }
    SPX_PUMP(6);
    uint32_t oh, rh, refs;
    moqr_shards_debug_retrieval_meta(r.s, 0, &oh, &rh, &refs);
    printf("props_shard(ceiling): owner holds=%u refs=%u while the requester is held\n", oh,
           refs);
    SPX_NEED(oh >= 1 && refs == 1, "refused-open-kept-by-owner");
    SPX_FIX(moq_session_fetch_cancel(fetcher[N_FETCH - 1]->peer, fh[N_FETCH - 1], r.now) ==
            MOQ_OK);
    r.frozen = 0;
    SPX_PUMP(40);
    int answered = 0;
    for (int k = 0; k < N_FETCH; k++) {
        printf("props_shard(ceiling): fetcher %d ok=%d ok_len=%zu complete=%d error=%d "
               "closed=%d\n", k, fo[k].fetch_ok, fo[k].ok_len, fo[k].complete, fo[k].error,
               fo[k].closed);
        SPX_NEED(fo[k].closed == 0, "sessions-live");
        if (k < N_FETCH - 1) {
            answered += fo[k].complete == 1 && fo[k].ok_len == sizeof(big) &&
                        memcmp(fo[k].ok_props, big, sizeof(fo[k].ok_props)) == 0;
        }
    }
    SPX_NEED(answered == N_FETCH - 1, "answered-with-set");
    /* The requester consumes OPEN and closes within one shard step, so its
     * hold is not seen between cycles; any copy seen has one reference. */
    SPX_NEED(max_refs == 1, "one-holder-per-copy");
    for (uint16_t sh = 0; sh < 2; sh++) {
        moqr_shards_debug_retrieval_meta(r.s, sh, &oh, &rh, &refs);
        SPX_NEED(oh == 0 && rh == 0, "copies-released");
    }
    printf("props_shard(ceiling): max_owner_held=%u max_requester_held=%u max_refs=%u "
           "peak=%ld ceiling=%llu\n", max_owner, max_requester, max_refs, ra.peak,
           (unsigned long long)cap.relay_alloc_ceiling);
    SPX_NEED((uint64_t)ra.peak <= cap.relay_alloc_ceiling, "peak-within-ceiling");
cleanup:
    moq_rcbuf_decref(payload);
    if (r.s != NULL) {
        sf_expect_channels_idle(&r);
        sfrig_destroy(&r);
    }
    int errors = r.failures + (ra.live != 0) + (sa.live != 0);
    printf("%s: props_shard(%s) causal=%s fixture_failures=%d live=%ld/%ld\n",
           errors ? "FIXTURE" : missing != NULL ? "FAIL" : "PASS", row,
           missing != NULL ? missing : "none", errors, ra.live, sa.live);
#undef SPX_FIX
#undef SPX_NEED
#undef SPX_PUMP
    return errors ? 2 : missing != NULL ? 1 : 0;
}

int
main(int argc, char **argv)
{
    if (argc != 2) return 2;
    if (strcmp(argv[1], "ceiling") == 0) return spx_ceiling();
    return spx_row(argv[1]);
}
