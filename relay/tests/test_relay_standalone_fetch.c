/* Draft-22 standalone FETCH through one relay binding: upstream retrieval of
 * what the cache does not hold, descending group order, the one total wait
 * budget, request-local cancellation and abort, and the finite outcome of
 * each failure, observed through the public session API at the origin and
 * the subscriber. Each row is its own test; the binding rig is
 * relay_session_binding_harness's, and the legacy row reuses the FETCH
 * suite's retained flows. */
#include "relay_session_binding_harness.c"
#define main relay_session_binding_fetch_main
#include "test_relay_session_binding_fetch.c"
#undef main
#include "standalone_fetch_observation.h"

typedef struct rfs_bind {
    ca_t alloc;
    rig_t rig;
    conn_t *origin, *consumer, *prime;
    moq_publication_t pub;
    moq_subscription_t upstream, prime_sub, sibling;
    moq_announcement_t ann;
    rfs_observation_t origin_obs, downstream;
    unsigned setups, admitted, subscribed, prime_acks, sibling_acks, live;
    bool pull;
} rfs_bind_t;

static void
rfs_drain(rfs_bind_t *t, conn_t *cn)
{
    if (cn == NULL) return;
    moq_event_t ev;
    while (moq_session_poll_events(cn->peer, &ev, 1) == 1) {
        rfs_observation_t *obs = cn == t->origin ? &t->origin_obs : &t->downstream;
        if (!rfs_observe(obs, &ev, t->rig.now)) {
            switch (ev.kind) {
            case MOQ_EVENT_SETUP_COMPLETE: t->setups++; break;
            case MOQ_EVENT_REQUEST_READY: break;
            case MOQ_EVENT_NAMESPACE_ACCEPTED:
                R_CHECK(&t->rig, cn == t->origin &&
                    ev.u.namespace_accepted.ann._opaque == t->ann._opaque);
                t->admitted++;
                break;
            case MOQ_EVENT_PUBLISH_OK:
                R_CHECK(&t->rig, cn == t->origin &&
                    ev.u.publish_ok.pub._opaque == t->pub._opaque && ev.u.publish_ok.send_allowed);
                t->admitted++;
                break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST: {
                R_CHECK(&t->rig, t->pull && cn == t->origin &&
                    rfs_key(ev.u.subscribe_request.track_namespace, ev.u.subscribe_request.track_name));
                t->upstream = ev.u.subscribe_request.sub;
                t->subscribed++;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                R_CHECK(&t->rig, moq_session_accept_subscribe(cn->peer, t->upstream,
                    &ac, t->rig.now) == MOQ_OK);
                break;
            }
            case MOQ_EVENT_SUBSCRIBE_OK:
                if (cn == t->prime) t->prime_acks++;
                else {
                    R_CHECK(&t->rig, cn == t->consumer &&
                        ev.u.subscribe_ok.sub._opaque == t->sibling._opaque);
                    t->sibling_acks++;
                }
                break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                if (cn == t->consumer) t->live++;
                else R_CHECK(&t->rig, cn == t->prime);
                break;
            case MOQ_EVENT_SUBSCRIBE_DONE:
                R_CHECK(&t->rig, cn == t->prime);
                break;
            default:
                printf("FIXTURE: standalone_bind unexpected event=%d\n", (int)ev.kind);
                t->rig.failures++;
                break;
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void
rfs_pump(rfs_bind_t *t, unsigned cycles)
{
    for (unsigned i = 0; i < cycles; i++) {
        relay_bind_test_rig_cycle(&t->rig);
        rfs_drain(t, t->origin);
        rfs_drain(t, t->consumer);
        rfs_drain(t, t->prime);
    }
}

static bool
rfs_subscribe(rfs_bind_t *t, conn_t *cn, moq_subscription_t *out)
{
    moq_bytes_t ns[] = { B("live"), B("cam") };
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    cfg.track_name = B("v"); cfg.location = &loc;
    return moq_session_subscribe(cn->peer, &cfg, t->rig.now, out) == MOQ_OK;
}

/* The origin's draft, and the FETCH subscriber's. A subscriber on a draft
 * without FILL_TIMEOUT waits the relay's default (set to the 100 ms these
 * rows ask for), and a run unavailable at the deadline is Unknown there,
 * where draft-22 says Timed-Out. */
static moq_version_t rfs_origin_version = MOQ_VERSION_DRAFT_22;
static moq_version_t rfs_consumer_version = MOQ_VERSION_DRAFT_22;

/* UNKNOWN_OBJECT_STATUS on the wire, from the local drafts' tables: 0x4 in
 * draft-16 (13.4.4), 0x6 in draft-18 (15.10.4) and draft-22. */
static uint64_t
rfs_unknown_status_code(void)
{
    return rfs_consumer_version == MOQ_VERSION_DRAFT_16 ? 0x4u : 0x6u;
}

static uint32_t
rfs_unavailable_kind(void)
{
    return rfs_consumer_version == MOQ_VERSION_DRAFT_22 ? MOQ_FETCH_RANGE_TIMED_OUT
                                                       : MOQ_FETCH_RANGE_UNKNOWN;
}
/* Track Properties the origin's PUBLISH carries (empty: none). */
static moq_bytes_t rfs_publish_properties;
/* The origin states its Object Properties as draft-16 integers: Type 124 is
 * two bytes there (40 7c), one in vi64 (7c); the subscriber reads vi64. */
static bool rfs_origin_d16_props;

static bool
rfs_create(rfs_bind_t *t, bool pull, bool streaming, unsigned consumer_actions)
{
    memset(t, 0, sizeof(*t));
    relay_bind_test_ca_init(&t->alloc);
    t->pull = pull;
    relay_bind_test_linger_us = 1000000;
    if (rfs_consumer_version != MOQ_VERSION_DRAFT_22)
        relay_bind_test_fill_timeout_default_ms = 100;
    moqr_result_t rc = relay_bind_test_rig_create(&t->rig, &t->alloc);
    relay_bind_test_linger_us = 0;
    relay_bind_test_fill_timeout_default_ms = 0;
    if (rc != MOQR_OK) return false;
    relay_bind_test_server_streaming_objects = streaming;
    t->origin = relay_bind_test_rig_connect(&t->rig, rfs_origin_version);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_max_actions = consumer_actions;
    t->consumer = relay_bind_test_rig_connect(&t->rig, rfs_consumer_version);
    relay_bind_test_max_actions = 0;
    if (t->origin == NULL || t->consumer == NULL) return false;
    rfs_pump(t, 6);
    moq_bytes_t ns[] = { B("live"), B("cam") };
    if (pull) {
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
        if (moq_session_publish_namespace(t->origin->peer, &cfg, t->rig.now, &t->ann) != MOQ_OK)
            return false;
        rfs_pump(t, 6);
        t->prime = relay_bind_test_rig_connect(&t->rig, MOQ_VERSION_DRAFT_22);
        if (t->prime == NULL) return false;
        rfs_pump(t, 4);
        if (!rfs_subscribe(t, t->prime, &t->prime_sub)) return false;
        rfs_pump(t, 10);
        if (t->subscribed != 1 || t->prime_acks != 1) return false;
    } else {
        moq_publish_cfg_t cfg;
        moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
        cfg.track_name = B("v");
        cfg.track_properties = rfs_publish_properties;
        if (moq_session_publish(t->origin->peer, &cfg, t->rig.now, &t->pub) != MOQ_OK)
            return false;
        rfs_pump(t, 8);
    }
    return t->rig.failures == 0 && t->admitted == 1 &&
        t->setups == (pull ? 3u : 2u) && t->origin_obs.nr == 0;
}

static bool
rfs_live_end(rfs_bind_t *t, uint8_t group, uint8_t object, bool end_group)
{
    uint8_t body[] = { 0x52, group, object, 0xc3, 0, 0xfe };
    static const uint8_t vi64[] = { 0x7c, 0x23 }, quic[] = { 0x40, 0x7c, 0x23 };
    const uint8_t *props = rfs_origin_d16_props ? quic : vi64;
    size_t props_len = rfs_origin_d16_props ? sizeof(quic) : sizeof(vi64);
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&t->alloc.vt, body, sizeof(body), &payload) != MOQ_OK) return false;
    moq_result_t rc = t->pull
        ? moq_session_send_object_datagram(t->origin->peer, t->upstream, group, object,
            73, end_group, payload, props, props_len, t->rig.now)
        : moq_session_send_pub_object_datagram(t->origin->peer, t->pub, group, object,
            73, end_group, payload, props, props_len, t->rig.now);
    moq_rcbuf_decref(payload);
    rfs_pump(t, 4);
    return rc == MOQ_OK && t->rig.failures == 0;
}

static bool
rfs_live(rfs_bind_t *t, uint8_t group, uint8_t object)
{
    return rfs_live_end(t, group, object, false);
}

static bool
rfs_issue(rfs_bind_t *t, uint64_t sg, uint64_t so, uint64_t eg, uint64_t eo,
          uint64_t timeout, bool descending, moq_fetch_t *out)
{
    moq_bytes_t ns[] = { B("live"), B("cam") };
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace = (moq_namespace_t){ .parts = ns, .count = 2 };
    cfg.track_name = B("v");
    cfg.start_group = sg; cfg.start_object = so; cfg.end_group = eg; cfg.end_object = eo;
    cfg.group_order = descending ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING;
    if (rfs_consumer_version == MOQ_VERSION_DRAFT_22) {
        cfg.has_fill_timeout = true; cfg.fill_timeout_ms = timeout;
    }
    cfg.has_subscriber_priority = true; cfg.subscriber_priority = 61;
    return moq_session_fetch(t->consumer->peer, &cfg, t->rig.now, out) == MOQ_OK;
}

static bool
rfs_accept(rfs_bind_t *t, unsigned index)
{
    const rfs_request_t *r = &t->origin_obs.requests[index];
    moq_accept_fetch_cfg_t cfg;
    moq_accept_fetch_cfg_init(&cfg);
    cfg.end_group = r->eg; cfg.end_object = r->eo;
    return moq_session_accept_fetch(t->origin->peer, r->fetch, &cfg, t->rig.now) == MOQ_OK;
}

static bool
rfs_reply_end(rfs_bind_t *t, unsigned index, bool finish)
{
    const rfs_request_t *r = &t->origin_obs.requests[index];
    uint8_t body[] = { 0x52, (uint8_t)r->sg, (uint8_t)r->so, 0xa1, 0, 0xfe };
    static const uint8_t vi64[] = { 0x7c, 0x31 }, quic[] = { 0x40, 0x7c, 0x31 };
    moq_rcbuf_t *payload = NULL, *properties = NULL;
    bool ok = moq_rcbuf_create(&t->alloc.vt, body, sizeof(body), &payload) == MOQ_OK &&
        moq_rcbuf_create(&t->alloc.vt, rfs_origin_d16_props ? quic : vi64,
                         rfs_origin_d16_props ? sizeof(quic) : sizeof(vi64),
                         &properties) == MOQ_OK;
    if (ok) {
        moq_fetch_object_cfg_t cfg;
        moq_fetch_object_cfg_init(&cfg);
        cfg.group_id = r->sg; cfg.object_id = r->so;
        cfg.publisher_priority = 73; cfg.payload = payload; cfg.properties = properties;
        ok = moq_session_write_fetch_object(t->origin->peer, r->fetch, &cfg, t->rig.now) == MOQ_OK &&
             (!finish || moq_session_end_fetch(t->origin->peer, r->fetch, t->rig.now) == MOQ_OK);
    }
    moq_rcbuf_decref(payload); moq_rcbuf_decref(properties);
    return ok;
}

static bool
rfs_reply(rfs_bind_t *t, unsigned index)
{
    return rfs_reply_end(t, index, true);
}

static bool
rfs_object(const rfs_event_t *e, uint8_t group, uint8_t object, bool retained)
{
    uint8_t body[] = { 0x52, group, object, retained ? 0xa1 : 0xc3, 0, 0xfe };
    /* Type 124 in the subscriber's integers: 40 7c on draft-16, 7c in vi64. */
    uint8_t q[] = { 0x40, 0x7c, retained ? 0x31 : 0x23 };
    uint8_t v[] = { 0x7c, retained ? 0x31 : 0x23 };
    bool d16 = rfs_consumer_version == MOQ_VERSION_DRAFT_16;
    const uint8_t *props = d16 ? q : v;
    size_t plen = d16 ? sizeof(q) : sizeof(v);
    return e != NULL && e->group == group && e->object == object &&
        e->bytes == sizeof(body) && e->properties == plen &&
        memcmp(e->body, body, sizeof(body)) == 0 && memcmp(e->props, props, plen) == 0;
}

static bool
rfs_alive(const rfs_bind_t *t)
{
    return t->downstream.closed == 0 && t->origin_obs.closed == 0 &&
        moq_session_state(t->consumer->peer) == MOQ_SESS_ESTABLISHED &&
        moq_session_state(t->consumer->rsess) == MOQ_SESS_ESTABLISHED;
}

static int
rfs_finish(rfs_bind_t *t, const char *row, const char *missing)
{
    rfs_dump(row, &t->origin_obs);
    printf("OBS: %s requests=%u cancels=%u downstream_events=%u closed=%u\n",
        row, t->origin_obs.nr, t->origin_obs.cancelled, t->downstream.n, t->downstream.closed);
    t->rig.failures += t->origin_obs.invalid + t->downstream.invalid;
    int errors = t->rig.failures;
    if (t->rig.bind != NULL) relay_bind_test_rig_destroy(&t->rig);
    if (t->alloc.live != 0) errors++;
    printf("%s: standalone_bind(%s) causal=%s fixture_failures=%d\n",
        errors ? "FIXTURE" : missing != NULL ? "FAIL" : "PASS", row,
        missing != NULL ? missing : "none", errors);
    return errors ? 2 : missing != NULL ? 1 : 0;
}

#define RFS_FIX(e) do { if (!(e)) { \
    printf("FIXTURE: %s:%d: %s\n", row, __LINE__, #e); t.rig.failures++; goto cleanup; \
} } while (0)
#define RFS_NEED(e, label) do { if (!(e)) { missing = label; goto cleanup; } } while (0)

static int
rfs_case(const char *row)
{
    rfs_bind_t t = {0};
    const char *missing = NULL;
    bool pull = strcmp(row, "pull-gap") == 0;
    bool two = strcmp(row, "total-deadline") == 0;
    bool descending = strcmp(row, "descending") == 0;
    bool capacity = strcmp(row, "capacity-recovery") == 0;
    bool cancel = strcmp(row, "cancel-reuse") == 0;
    bool error = strcmp(row, "source-error") == 0;
    bool reset = strcmp(row, "source-reset") == 0;
    bool zero = strcmp(row, "timeout-zero") == 0;
    bool absent = strcmp(row, "absent-source") == 0;
    bool cache = strcmp(row, "cached-only") == 0;
    moq_fetch_t fetch = {0}, other = {0};
    relay_bind_test_max_fetches = capacity ? 1u : 0u;
    bool created = rfs_create(&t, pull, false, 0);
    relay_bind_test_max_fetches = 0;
    RFS_FIX(created);
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live_end(&t, 0, 2, descending));
    if (two) RFS_FIX(rfs_live(&t, 0, 4));
    if (descending) RFS_FIX(rfs_live(&t, 1, 0) && rfs_live_end(&t, 1, 2, true));
    moqr_core_stats_t before;
    moqr_core_get_stats(t.rig.core, &before);
    RFS_FIX(before.ingested_total == (two ? 3u : descending ? 4u : 2u));
    if (absent) {
        moq_finish_publish_cfg_t cfg;
        moq_finish_publish_cfg_init(&cfg);
        RFS_FIX(moq_session_finish_publish(t.origin->peer, t.pub, &cfg, t.rig.now) == MOQ_OK);
        rfs_pump(&t, 8);
    }
    uint64_t issued = t.rig.now;
    RFS_FIX(rfs_issue(&t, 0, cache ? 2 : 0, descending ? 1 : 0, two ? 5 : 3,
        zero ? 0 : 100, descending, &fetch));
    rfs_pump(&t, 16);
    RFS_FIX(t.rig.failures == 0 && t.origin_obs.invalid == 0 && t.downstream.invalid == 0);
    if (cache || zero || absent) {
        if (absent) rfs_pump(&t, 110);
        RFS_NEED(t.origin_obs.nr == 0, "no-unnecessary-upstream");
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_ERROR) == 0 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 0 && rfs_alive(&t),
            "finite-cache-result");
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == (cache ? 1u : 2u),
            "cache-object-count");
        RFS_NEED(rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 0),
            0, cache ? 2 : 0, false), "cache-prefix-bytes");
        if (!cache) {
            const rfs_event_t *gap = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP, 0);
            RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 1 &&
                gap != NULL && gap->group == 0 && gap->object == 1 &&
                (gap->code == rfs_unavailable_kind() ||
                 (absent && gap->code == MOQ_FETCH_RANGE_UNKNOWN)) &&
                rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 1), 0, 2, false),
                "exact-unavailable-gap-tail");
        } else RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 0, "cache-no-gap");
        goto cleanup;
    }
    RFS_NEED(t.origin_obs.nr == 1, "origin-request");
    RFS_NEED(rfs_request_exact(&t.origin_obs.requests[0], descending ? 1 : 0, 1,
        descending ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING), "origin-tuple");
    uint64_t deadline = moqr_bind_next_deadline_us(t.rig.bind);
    RFS_NEED(deadline >= issued + 100000 && deadline <= issued + 105000 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
        "original-deadline-no-early-fin");
    if (error || reset) {
        RFS_FIX(rfs_subscribe(&t, t.consumer, &t.sibling));
        rfs_pump(&t, 8);
        RFS_FIX(t.sibling_acks == 1);
        if (error) {
            moq_reject_fetch_cfg_t cfg;
            moq_reject_fetch_cfg_init(&cfg);
            cfg.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
            RFS_FIX(moq_session_reject_fetch(t.origin->peer,
                t.origin_obs.requests[0].fetch, &cfg, t.rig.now) == MOQ_OK);
        } else {
            RFS_FIX(rfs_accept(&t, 0));
            rfs_pump(&t, 4);
            RFS_FIX(moq_session_abort_fetch(t.origin->peer, t.origin_obs.requests[0].fetch,
                MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS, t.rig.now) == MOQ_OK);
        }
        rfs_pump(&t, 16);
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 1 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 && rfs_alive(&t),
            "request-local-source-terminal");
        const rfs_event_t *rst = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET, 0);
        printf("OBS: %s reset code=0x%llx\n", row, rst ? (unsigned long long)rst->code : 0ull);
        RFS_NEED(rst != NULL && rst->code == rfs_unknown_status_code(),
            "source-terminal-unknown-object-status");
        RFS_FIX(rfs_issue(&t, 0, 2, 0, 3, 0, false, &other));
        rfs_pump(&t, 12);
        RFS_NEED(rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_COMPLETE) == 1,
            "unrelated-fetch-survives");
        RFS_FIX(rfs_live(&t, 1, 0));
        rfs_pump(&t, 8);
        RFS_NEED(t.live == 1 && rfs_alive(&t), "subscription-survives");
        goto cleanup;
    }
    if (capacity) {
        RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &other));
        rfs_pump(&t, 12);
        RFS_NEED(rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_ERROR) == 1 &&
            rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_COMPLETE) == 0 &&
            t.origin_obs.nr == 1 && rfs_alive(&t), "bounded-capacity-refusal");
    }
    RFS_FIX(rfs_accept(&t, 0));
    rfs_pump(&t, 4);
    if (cancel || capacity) {
        t.origin->frozen = true;
        RFS_FIX(moq_session_fetch_cancel(t.consumer->peer, fetch, t.rig.now) == MOQ_OK);
        rfs_pump(&t, 6);
        RFS_NEED(rfs_alive(&t) && rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
            "request-local-cancel");
        if (cancel) RFS_FIX(rfs_reply_end(&t, 0, false)); /* queued before cancellation arrives */
        t.origin->frozen = false;
        rfs_pump(&t, 12);
        RFS_NEED(t.origin_obs.cancelled == 1 && rfs_alive(&t), "cancel-settlement");
        RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &other));
        rfs_pump(&t, 12);
        RFS_NEED(t.origin_obs.nr == 2 && t.origin_obs.requests[1].fetch._opaque !=
            t.origin_obs.requests[0].fetch._opaque, "slot-reuse-new-generation");
        RFS_FIX(rfs_accept(&t, 1) && rfs_reply(&t, 1));
        rfs_pump(&t, 16);
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) <= 1 &&
            rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_OBJECT) == 3 &&
            rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_COMPLETE) == 1 && rfs_alive(&t),
            "no-old-output-recovery");
        goto cleanup;
    }
    if (two) {
        RFS_FIX(t.rig.now < deadline - 20000);
        t.rig.now = deadline - 20000;
        rfs_pump(&t, 1);
    }
    RFS_FIX(rfs_reply(&t, 0));
    rfs_pump(&t, 8);
    if (two || descending) {
        RFS_NEED(t.origin_obs.nr == 2 && rfs_request_exact(&t.origin_obs.requests[1],
            0, two ? 3 : 1, descending ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING),
            "second-exact-span");
        RFS_NEED(moqr_bind_next_deadline_us(t.rig.bind) == deadline,
            "no-deadline-renewal");
        if (two) {
            RFS_NEED(!t.origin_obs.requests[1].timeout &&
                t.origin_obs.requests[1].timeout_ms == 0,
                "timeout-budget-remains-hop-local");
            t.rig.now = deadline - 1001;
            rfs_pump(&t, 1);
            RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
                "no-predeadline-timeout");
            rfs_pump(&t, 10);
            const rfs_event_t *gap = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP, 0);
            RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 1 &&
                gap != NULL && gap->group == 0 && gap->object == 3 &&
                gap->code == rfs_unavailable_kind() && t.origin_obs.cancelled == 1,
                "one-budget-timeout-tail");
        } else {
            RFS_FIX(rfs_accept(&t, 1) && rfs_reply(&t, 1));
            rfs_pump(&t, 12);
        }
    }
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_ERROR) == 0 && rfs_alive(&t),
        "one-complete");
    unsigned objects = descending ? 6u : two ? 4u : 3u;
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == objects &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == (two ? 1u : 0u), "exact-coverage");
    for (unsigned i = 0; i < objects; i++) {
        uint8_t g = descending && i < 3 ? 1 : 0;
        uint8_t object = descending ? (uint8_t)(i % 3) : two && i == 3 ? 4 : (uint8_t)i;
        RFS_NEED(rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, i),
            g, object, object == 1), "ordered-exact-bytes-properties");
    }
    moqr_core_stats_t after;
    moqr_core_get_stats(t.rig.core, &after);
    RFS_NEED(after.ingested_total == before.ingested_total, "no-live-cache-rescue");
cleanup:
    return rfs_finish(&t, row, missing);
}

/* An Object that has expired from the cache (MAX_CACHE_DURATION, §10.3)
 * is unknown, never known not to exist: a FETCH of exactly that Object, with
 * the publisher still live, gets FETCH_OK and then, with a FILL_TIMEOUT of 0,
 * one Timed-Out range ending at it and no object, without asking the origin;
 * with a budget, the origin is asked for exactly that Object, which is then
 * served. Neither ends with a bare FIN after nothing. */
static int
rfs_expired(bool budget)
{
    const char *row = budget ? "expired-budget" : "expired-zero";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    /* MAX_CACHE_DURATION = 500 ms, a two-byte vi64. */
    static const uint8_t props[3] = { 0x04, 0x81, 0xF4 };
    rfs_publish_properties = (moq_bytes_t){ props, sizeof(props) };
    bool created = rfs_create(&t, false, false, 0);
    rfs_publish_properties = (moq_bytes_t){ NULL, 0 };
    RFS_FIX(created);
    RFS_FIX(rfs_live(&t, 0, 0));
    t.rig.now += 600000;   /* object {0,0} expires */
    rfs_pump(&t, 2);
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 0, budget ? 100 : 0, false, &fetch));
    rfs_pump(&t, 16);
    RFS_FIX(t.rig.failures == 0 && t.origin_obs.invalid == 0 && t.downstream.invalid == 0);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OK) == 1, "fetch-ok");
    if (!budget) {
        const rfs_event_t *gap = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP, 0);
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 0,
            "expired-not-served");
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 1 &&
            gap != NULL && gap->group == 0 && gap->object == 0 &&
            gap->code == MOQ_FETCH_RANGE_TIMED_OUT, "expired-timed-out");
        RFS_NEED(t.origin_obs.nr == 0, "no-upstream-without-budget");
        RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
            rfs_alive(&t), "complete-after-marker");
        goto cleanup;
    }
    RFS_NEED(t.origin_obs.nr == 1 &&
        rfs_request_exact(&t.origin_obs.requests[0], 0, 0, MOQ_GROUP_ORDER_ASCENDING),
        "upstream-exact-expired-location");
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
        "no-early-fin");
    RFS_FIX(rfs_accept(&t, 0) && rfs_reply(&t, 0));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1 &&
        rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 0), 0, 0, true) &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 && rfs_alive(&t),
        "retrieved-object-served");
cleanup:
    return rfs_finish(&t, row, missing);
}

static int
rfs_cached_cancel(void)
{
    const char *row = "cancel-cached";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0}, other = {0};
    RFS_FIX(rfs_create(&t, false, false, 4));
    for (uint8_t i = 0; i < 32; i++) RFS_FIX(rfs_live(&t, 0, i));
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 32, 100, false, &fetch));
    rfs_pump(&t, 3);
    RFS_FIX(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OK) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) < 32);
    RFS_FIX(moq_session_fetch_cancel(t.consumer->peer, fetch, t.rig.now) == MOQ_OK);
    rfs_pump(&t, 8);
    RFS_NEED(rfs_alive(&t) && rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0,
        "request-local-cached-cancel");
    RFS_FIX(rfs_issue(&t, 0, 31, 0, 32, 0, false, &other));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, other, MOQ_EVENT_FETCH_OBJECT) == 1 &&
        rfs_object(rfs_nth(&t.downstream, other, MOQ_EVENT_FETCH_OBJECT, 0), 0, 31, false),
        "cached-cancel-slot-reuse");
cleanup:
    return rfs_finish(&t, row, missing);
}

static int
rfs_coalesce(bool fail)
{
    const char *row = fail ? "coalesce-oom" : "coalesce-control";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    RFS_FIX(rfs_create(&t, false, true, 0));
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    moq_subgroup_handle_t stream;
    RFS_FIX(moq_session_open_pub_subgroup(t.origin->peer, t.pub, &cfg,
        t.rig.now, &stream) == MOQ_OK);
    RFS_FIX(moq_session_begin_object(t.origin->peer, stream, 0, 333, t.rig.now) == MOQ_OK);
    for (unsigned i = 0; i < 3; i++) relay_bind_test_parity_write_chunk(&t.rig, t.origin, stream, (uint8_t)(0xc1 + i), 111);
    RFS_FIX(moq_session_end_object(t.origin->peer, stream, t.rig.now) == MOQ_OK &&
        moq_session_close_subgroup(t.origin->peer, stream, t.rig.now) == MOQ_OK);
    rfs_pump(&t, 10);
    RFS_FIX(t.rig.failures == 0 && rfs_issue(&t, 0, 0, 0, 1, 100, false, &fetch));
    t.alloc.fail_size = fail ? 333 : 0;
    rfs_pump(&t, 16);
    if (fail) {
        RFS_FIX(t.alloc.fail_size == 0);
        RFS_NEED(rfs_alive(&t) && rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 1 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 0,
            "request-local-coalesce-abort");
    } else {
        RFS_NEED(rfs_alive(&t) && rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
            rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1,
            "coalesce-positive");
        const rfs_event_t *v = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 0);
        RFS_NEED(v != NULL && v->bytes == 333, "coalesce-length");
        for (unsigned i = 0; i < 333; i++)
            RFS_NEED(v->body[i] == (uint8_t)(0xc1 + i / 111), "coalesce-bytes");
    }
cleanup:
    return rfs_finish(&t, row, missing);
}

/* Answer upstream request `index` with Objects {g, first} .. {g, last} of
 * its group (retained bytes), then FIN. */
static bool
rfs_reply_run(rfs_bind_t *t, unsigned index, uint8_t first, uint8_t last)
{
    const rfs_request_t *r = &t->origin_obs.requests[index];
    static const uint8_t props[] = { 0x7c, 0x31 };
    bool ok = true;
    for (unsigned o = first; ok && o <= last; o++) {
        uint8_t body[] = { 0x52, (uint8_t)r->sg, (uint8_t)o, 0xa1, 0, 0xfe };
        moq_rcbuf_t *payload = NULL, *properties = NULL;
        ok = moq_rcbuf_create(&t->alloc.vt, body, sizeof(body), &payload) == MOQ_OK &&
            moq_rcbuf_create(&t->alloc.vt, props, sizeof(props), &properties) == MOQ_OK;
        if (ok) {
            moq_fetch_object_cfg_t cfg;
            moq_fetch_object_cfg_init(&cfg);
            cfg.group_id = r->sg; cfg.object_id = o;
            cfg.publisher_priority = 73; cfg.payload = payload; cfg.properties = properties;
            ok = moq_session_write_fetch_object(t->origin->peer, r->fetch, &cfg,
                                                t->rig.now) == MOQ_OK;
        }
        moq_rcbuf_decref(payload); moq_rcbuf_decref(properties);
    }
    return ok && moq_session_end_fetch(t->origin->peer, r->fetch, t->rig.now) == MOQ_OK;
}

static bool
rfs_request_span(const rfs_request_t *r, uint64_t g, uint64_t first,
                 uint64_t raw_end, moq_group_order_t order)
{
    return r->fetch._opaque != 0 && r->sg == g && r->so == first && r->eg == g &&
        r->eo == raw_end && r->order == order && r->priority == 61 &&
        !r->timeout && r->timeout_ms == 0;
}

/* Descending group order over three groups: group 2 is fully cached, group 1
 * was never seen here, and group 0 has a hole. Groups are delivered from the
 * end group down, each in Object order; the cache serves what it holds, and
 * each run it does not hold is one upstream FETCH for exactly that run --
 * the whole of group 1, then {0,1} -- under the request's one deadline. The
 * end of group 1 past what the origin's FETCH_OK covered is known not to
 * exist and stays unmarked. */
static int
rfs_descending_groups(void)
{
    const char *row = "descending-groups";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    RFS_FIX(rfs_create(&t, false, false, 0));
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live_end(&t, 0, 2, true));
    RFS_FIX(rfs_live(&t, 2, 0) && rfs_live_end(&t, 2, 1, true));
    moqr_core_stats_t before;
    moqr_core_get_stats(t.rig.core, &before);
    RFS_FIX(before.ingested_total == 4u);
    uint64_t issued = t.rig.now;
    RFS_FIX(rfs_issue(&t, 0, 0, 2, 2, 100, true, &fetch));
    rfs_pump(&t, 16);
    RFS_NEED(t.origin_obs.nr == 1 &&
        rfs_request_span(&t.origin_obs.requests[0], 1, 0, 0, MOQ_GROUP_ORDER_DESCENDING),
        "whole-missing-group-request");
    uint64_t deadline = moqr_bind_next_deadline_us(t.rig.bind);
    RFS_NEED(deadline >= issued + 100000 && deadline <= issued + 105000,
        "one-deadline");
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 2 &&
        rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 0), 2, 0, false) &&
        rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 1), 2, 1, false),
        "cached-end-group-first");
    RFS_FIX(rfs_accept(&t, 0) && rfs_reply_run(&t, 0, 0, 1));
    rfs_pump(&t, 12);
    RFS_NEED(t.origin_obs.nr == 2 &&
        rfs_request_span(&t.origin_obs.requests[1], 0, 1, 2, MOQ_GROUP_ORDER_DESCENDING),
        "hole-request-after-group");
    RFS_NEED(moqr_bind_next_deadline_us(t.rig.bind) == deadline, "no-deadline-renewal");
    RFS_FIX(rfs_accept(&t, 1) && rfs_reply(&t, 1));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 0 && rfs_alive(&t),
        "one-complete");
    static const struct { uint8_t g, o; bool retained; } want[] = {
        { 2, 0, false }, { 2, 1, false }, { 1, 0, true }, { 1, 1, true },
        { 0, 0, false }, { 0, 1, true }, { 0, 2, false },
    };
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 7, "exact-coverage");
    for (unsigned i = 0; i < 7; i++) {
        RFS_NEED(rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, i),
            want[i].g, want[i].o, want[i].retained), "group-order-exact-bytes");
    }
    moqr_core_stats_t after;
    moqr_core_get_stats(t.rig.core, &after);
    RFS_NEED(after.ingested_total == before.ingested_total, "no-live-cache-rescue");
cleanup:
    return rfs_finish(&t, row, missing);
}

/* A range this relay no longer holds is retrieved, not refused: with the log
 * bounded to two groups, group 0 has been evicted by the time the FETCH for
 * it arrives. The track's Largest Object is known, so the range is valid
 * (FETCH_OK, not INVALID_RANGE) and the relay asks the origin for it. */
static int
rfs_evicted_range(void)
{
    const char *row = "evicted-range";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    relay_bind_test_log_max_groups = 2;
    bool created = rfs_create(&t, false, false, 0);
    relay_bind_test_log_max_groups = 0;
    RFS_FIX(created);
    for (uint8_t g = 0; g < 4; g++) RFS_FIX(rfs_live_end(&t, g, 0, true));
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 1, 100, false, &fetch));
    rfs_pump(&t, 16);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_ERROR) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OK) == 1, "evicted-range-admitted");
    RFS_NEED(t.origin_obs.nr == 1 && rfs_request_exact(&t.origin_obs.requests[0], 0, 0,
        MOQ_GROUP_ORDER_ASCENDING), "evicted-range-request");
    RFS_FIX(rfs_accept(&t, 0) && rfs_reply(&t, 0));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_GAP) == 0 &&
        rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, 0), 0, 0, true) &&
        rfs_alive(&t), "evicted-range-served");
cleanup:
    return rfs_finish(&t, row, missing);
}

/* The request-local reset is an obligation: when the subscriber's connection
 * cannot take it (its session's action queue is full), it is retried until
 * it commits. The origin refuses the upstream FETCH while the subscriber's
 * transport is stalled behind live Objects of another subscription. */
static int
rfs_abort_blocked(void)
{
    const char *row = "abort-blocked";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    RFS_FIX(rfs_create(&t, false, false, 4));
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live(&t, 0, 2));
    RFS_FIX(rfs_subscribe(&t, t.consumer, &t.sibling));
    rfs_pump(&t, 8);
    RFS_FIX(t.sibling_acks == 1);
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &fetch));
    rfs_pump(&t, 16);
    RFS_FIX(t.origin_obs.nr == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1);
    t.consumer->frozen = true;
    for (uint8_t o = 3; o < 9; o++) RFS_FIX(rfs_live(&t, 0, o));
    moq_reject_fetch_cfg_t cfg;
    moq_reject_fetch_cfg_init(&cfg);
    cfg.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    RFS_FIX(moq_session_reject_fetch(t.origin->peer, t.origin_obs.requests[0].fetch,
        &cfg, t.rig.now) == MOQ_OK);
    rfs_pump(&t, 8);
    t.consumer->frozen = false;
    rfs_pump(&t, 24);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 && rfs_alive(&t),
        "blocked-reset-retried");
    {
        const rfs_event_t *rst = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET, 0);
        printf("OBS: abort-blocked reset code=0x%llx\n",
            rst ? (unsigned long long)rst->code : 0ull);
        RFS_NEED(rst != NULL && rst->code == rfs_unknown_status_code(),
            "blocked-reset-unknown-object-status");
    }
    RFS_NEED(t.live == 6, "subscription-delivery-intact");
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(t.rig.bind, &bs);
    RFS_NEED(bs.session_errors == 0, "no-session-error");
cleanup:
    return rfs_finish(&t, row, missing);
}

/* The origin goes away while the relay's upstream FETCH for the hole is
 * open: the run's state can no longer be determined, so the response is
 * reset with UNKNOWN_OBJECT_STATUS (draft-22 3.2.2, 12.5) -- after the cached
 * prefix, never with a FIN -- and the subscriber's session is unaffected. A
 * blocked upstream reset of the same kind is covered by abort-blocked. */
static int
rfs_source_lost(void)
{
    const char *row = "source-lost";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    RFS_FIX(rfs_create(&t, false, false, 0));
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live(&t, 0, 2));
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &fetch));
    rfs_pump(&t, 16);
    RFS_FIX(t.origin_obs.nr == 1 && rfs_accept(&t, 0));
    rfs_pump(&t, 4);
    RFS_FIX(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1);
    RFS_FIX(moq_session_close(t.origin->peer, 0, "gone", t.rig.now) == MOQ_OK);
    rfs_pump(&t, 16);
    const rfs_event_t *reset = rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET, 0);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_RESET) == 1 && reset != NULL &&
        reset->code == MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 0 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 1 &&
        t.downstream.closed == 0 &&
        moq_session_state(t.consumer->peer) == MOQ_SESS_ESTABLISHED,
        "unknown-status-reset");
    moqr_bind_stats_t bs;
    moqr_bind_get_stats(t.rig.bind, &bs);
    RFS_NEED(bs.session_errors == 0, "no-session-error");
cleanup:
    return rfs_finish(&t, row, missing);
}

/* A draft-22 subscriber's FETCH retrieves from a draft-18 origin: the
 * upstream FETCH is a draft-18 one (no FILL_TIMEOUT; the relay keeps the
 * deadline itself), and the response is the same. */
static int
rfs_d18_origin(void)
{
    const char *row = "d18-origin";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    rfs_origin_version = MOQ_VERSION_DRAFT_18;
    bool created = rfs_create(&t, false, false, 0);
    rfs_origin_version = MOQ_VERSION_DRAFT_22;
    RFS_FIX(created);
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live(&t, 0, 2));
    uint64_t issued = t.rig.now;
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &fetch));
    rfs_pump(&t, 16);
    const rfs_request_t *r = &t.origin_obs.requests[0];
    RFS_NEED(t.origin_obs.nr == 1 && r->sg == 0 && r->so == 1 && r->eg == 0 &&
        r->eo == 2 && r->priority == 61 && !r->timeout, "d18-upstream-request");
    uint64_t deadline = moqr_bind_next_deadline_us(t.rig.bind);
    RFS_NEED(deadline >= issued + 100000 && deadline <= issued + 105000, "relay-deadline");
    RFS_FIX(rfs_accept(&t, 0) && rfs_reply(&t, 0));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 3 && rfs_alive(&t),
        "one-complete");
    for (unsigned i = 0; i < 3; i++) {
        RFS_NEED(rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, i),
            0, (uint8_t)i, i == 1), "ordered-exact-bytes-properties");
    }
cleanup:
    return rfs_finish(&t, row, missing);
}

/* The same over a draft-16 origin: the cached live Objects and the one the
 * upstream FETCH collects all reach the draft-22 subscriber with their
 * properties in its integer encoding, in order. */
static int
rfs_d16_origin(void)
{
    const char *row = "d16-origin";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    moq_fetch_t fetch = {0};
    rfs_origin_version = MOQ_VERSION_DRAFT_16;
    rfs_origin_d16_props = true;
    bool created = rfs_create(&t, false, false, 0);
    rfs_origin_version = MOQ_VERSION_DRAFT_22;
    RFS_FIX(created);
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live(&t, 0, 2));
    RFS_FIX(rfs_issue(&t, 0, 0, 0, 3, 100, false, &fetch));
    rfs_pump(&t, 16);
    const rfs_request_t *r = &t.origin_obs.requests[0];
    RFS_NEED(t.origin_obs.nr == 1 && r->sg == 0 && r->so == 1 && r->eg == 0 && r->eo == 2,
        "d16-upstream-request");
    RFS_FIX(rfs_accept(&t, 0) && rfs_reply(&t, 0));
    rfs_pump(&t, 12);
    RFS_NEED(rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_COMPLETE) == 1 &&
        rfs_count(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT) == 3 && rfs_alive(&t),
        "one-complete");
    for (unsigned i = 0; i < 3; i++) {
        RFS_NEED(rfs_object(rfs_nth(&t.downstream, fetch, MOQ_EVENT_FETCH_OBJECT, i),
            0, (uint8_t)i, i == 1), "ordered-converted-properties");
    }
cleanup:
    rfs_origin_d16_props = false;
    return rfs_finish(&t, row, missing);
}

/* -- a draft-18 descending FETCH from an independently encoded peer ------- *
 * Served for any draft-18 peer, not only one built on libmoq. The
 * consumer's transport is held and the test is that peer: the FETCH is written byte by byte from the
 * draft-18 message layout (10.12, 10.2, 1.4.1) into the relay's session for
 * that connection, and the response is read straight from that session's
 * actions and parsed here from the draft-18 FETCH_OK and Fetch Object layouts
 * (10.13, 11.4.4) -- none of libmoq's draft-18 codec is used on either side. The cache holds 0:0, 0:2, 1:0 and 1:2; the relay must
 * fetch 1:1 and then 0:1 upstream and serve groups 1 then 0, each in Object
 * order, with the properties converted to the subscriber's (vi64) form. */

typedef struct rfs_wire {
    uint8_t  ctl[256];       /* the request stream's bytes (FETCH_OK) */
    size_t   ctl_len;
    uint8_t  data[4096];     /* the response data stream's bytes */
    size_t   data_len;
    uint64_t data_ref;
    int      data_streams, resets;
    bool     data_fin;
} rfs_wire_t;

static void
rfs_wire_collect(moq_session_t *rs, uint64_t req_ref, rfs_wire_t *w)
{
    moq_action_t a;
    while (moq_session_poll_actions(rs, &a, 1) == 1) {
        if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            a.u.send_bidi_stream.stream_ref._v == req_ref) {
            size_t n = a.u.send_bidi_stream.len;
            if (w->ctl_len + n <= sizeof(w->ctl)) {
                memcpy(w->ctl + w->ctl_len, a.u.send_bidi_stream.data, n);
                w->ctl_len += n;
            }
        } else if (a.kind == MOQ_ACTION_SEND_DATA) {
            const moq_send_data_action_t *d = &a.u.send_data;
            uint64_t ref = d->stream_ref._v;
            if (w->data_streams == 0) {
                w->data_ref = ref;
                w->data_streams = 1;
            }
            if (ref == w->data_ref) {
                size_t hl = d->header_len, pl = d->payload ? moq_rcbuf_len(d->payload) : 0;
                if (w->data_len + hl + pl <= sizeof(w->data)) {
                    memcpy(w->data + w->data_len, d->header, hl);
                    w->data_len += hl;
                    if (pl) memcpy(w->data + w->data_len, moq_rcbuf_data(d->payload), pl);
                    w->data_len += pl;
                }
                if (d->fin) w->data_fin = true;
            } else {
                w->data_streams++;
            }
        } else if (a.kind == MOQ_ACTION_RESET_DATA) {
            w->resets++;
        }
        moq_action_cleanup(&a);
    }
}

typedef struct rfs_wire_obj {
    uint64_t group, object;
    uint8_t  props[8], body[8];
    size_t   props_len, body_len;
} rfs_wire_obj_t;

/* A draft-18 variable-length integer (1.4.1): the leading 1 bits of the
 * first byte give the extra length; nine bytes carry a full 64 bits. */
static bool
rfs_vi(const uint8_t *b, size_t n, size_t *pos, uint64_t *out)
{
    if (*pos >= n) return false;
    uint8_t first = b[*pos];
    unsigned ones = 0;
    while (ones < 8 && (first & (0x80u >> ones)) != 0) ones++;
    size_t len = ones + 1u;
    if (len > n - *pos) return false;
    uint64_t v = ones >= 7 ? 0 : (uint64_t)(first & (0xFFu >> (ones + 1u)));
    for (size_t i = 1; i < len; i++) v = (v << 8) | b[*pos + i];
    *out = v;
    *pos += len;
    return true;
}

/* Parse FETCH_HEADER and the Objects after it; false on anything that is not
 * an Object (an End of Range included) or does not parse. */
static bool
rfs_wire_parse(const rfs_wire_t *w, bool descending, uint64_t *req_id, rfs_wire_obj_t *objs,
               int cap, int *n_out)
{
    size_t pos = 0;
    uint64_t type = 0;
    if (!rfs_vi(w->data, w->data_len, &pos, &type) || type != 0x05) return false;
    if (!rfs_vi(w->data, w->data_len, &pos, req_id)) return false;
    int n = 0;
    uint64_t pg = 0, po = 0;
    while (pos < w->data_len) {
        uint64_t flags = 0, v = 0;
        if (!rfs_vi(w->data, w->data_len, &pos, &flags) || flags >= 128 || n == cap)
            return false;
        rfs_wire_obj_t *o = &objs[n];
        memset(o, 0, sizeof(*o));
        if (flags & 0x08) {
            if (!rfs_vi(w->data, w->data_len, &pos, &v)) return false;
            o->group = n == 0 ? v : descending ? pg - (v + 1) : pg + v + 1;
        } else {
            if (n == 0) return false;
            o->group = pg;
        }
        if ((flags & 0x40) == 0 && (flags & 0x03) == 0x03 &&
            !rfs_vi(w->data, w->data_len, &pos, &v)) return false;   /* Subgroup ID */
        if (flags & 0x04) {
            if (!rfs_vi(w->data, w->data_len, &pos, &v)) return false;
            o->object = n == 0 || (flags & 0x08) ? v : po + v;
        } else {
            if (n == 0) return false;
            o->object = po + 1;
        }
        if (flags & 0x10) {
            if (pos >= w->data_len) return false;
            pos++;   /* Publisher Priority */
        }
        if (flags & 0x20) {
            if (!rfs_vi(w->data, w->data_len, &pos, &v) || v > sizeof(o->props) ||
                v > w->data_len - pos) return false;
            memcpy(o->props, w->data + pos, (size_t)v);
            o->props_len = (size_t)v;
            pos += (size_t)v;
        }
        if (!rfs_vi(w->data, w->data_len, &pos, &v) || v > sizeof(o->body) ||
            v > w->data_len - pos) return false;
        memcpy(o->body, w->data + pos, (size_t)v);
        o->body_len = (size_t)v;
        pos += (size_t)v;
        pg = o->group;
        po = o->object;
        n++;
    }
    *n_out = n;
    return true;
}

/* d18_origin: the origin speaks draft-18 too, so the runs the relay does not
 * hold are fetched with a draft-18 descending FETCH of its own. */
static int
rfs_d18_descending_wire(bool d18_origin)
{
    const char *row = d18_origin ? "d18-descending-wire-d18-origin" : "d18-descending-wire";
    const char *missing = NULL;
    rfs_bind_t t = {0};
    rfs_consumer_version = MOQ_VERSION_DRAFT_18;
    if (d18_origin) rfs_origin_version = MOQ_VERSION_DRAFT_18;
    bool created = rfs_create(&t, false, false, 0);
    RFS_FIX(created);
    RFS_FIX(rfs_live(&t, 0, 0) && rfs_live_end(&t, 0, 2, true));
    RFS_FIX(rfs_live(&t, 1, 0) && rfs_live_end(&t, 1, 2, true));
    t.consumer->frozen = true;
    /* FETCH (10.12, Figure 15) -- every value fits a one-byte vi64:
     * Request ID 0, Fetch Type 1 (Standalone), Track Namespace {live, cam},
     * Track Name v, Start 0:0, End 1:3 (the end plus one), and two
     * Parameters in ascending Type: SUBSCRIBER_PRIORITY (0x20) = 61,
     * GROUP_ORDER (0x22, delta 2) = 2 (descending, 10.2.8). */
    static const uint8_t payload[] = {
        0x00, 0x01,                                     /* Request ID, Type */
        0x02, 0x04, 'l', 'i', 'v', 'e', 0x03, 'c', 'a', 'm',   /* Namespace */
        0x01, 'v',                                      /* Track Name */
        0x00, 0x00, 0x01, 0x03,                         /* Start, End */
        0x02, 0x20, 61, 0x02, 0x02,                     /* Parameters */
    };
    uint8_t buf[3 + sizeof(payload)];
    buf[0] = 0x16;                                      /* Type: FETCH */
    buf[1] = 0x00;
    buf[2] = (uint8_t)sizeof(payload);                  /* Length (16) */
    memcpy(buf + 3, payload, sizeof(payload));
    const uint64_t req_ref = 0x9000;
    RFS_FIX(moq_session_on_bidi_stream_bytes(t.consumer->rsess, moq_stream_ref_from_u64(req_ref),
                                             buf, sizeof(buf), false, t.rig.now) == MOQ_OK);
    rfs_wire_t w;
    memset(&w, 0, sizeof(w));
    unsigned answered = 0;
    for (int i = 0; i < 48; i++) {
        rfs_pump(&t, 1);
        rfs_wire_collect(t.consumer->rsess, req_ref, &w);
        while (answered < t.origin_obs.nr) {
            RFS_FIX(rfs_accept(&t, answered) && rfs_reply(&t, answered));
            answered++;
        }
    }
    printf("OBS: %s upstream=%u ctl=%zu data=%zu fin=%d streams=%d resets=%d\n", row,
           t.origin_obs.nr, w.ctl_len, w.data_len, (int)w.data_fin, w.data_streams, w.resets);
    /* FETCH_OK on the request stream (10.13, Figure 16): vi64 Type 0x18,
     * 16-bit Length, End Of Track (8), End Location -- here 1:3 -- then the
     * Parameters count and Track Properties. */
    size_t cpos = 0;
    uint64_t ctype = 0;
    RFS_NEED(rfs_vi(w.ctl, w.ctl_len, &cpos, &ctype) && ctype == 0x18 &&
                 w.ctl_len >= cpos + 2, "fetch-ok-first");
    if (missing == NULL) {
        size_t clen = ((size_t)w.ctl[cpos] << 8) | w.ctl[cpos + 1];
        size_t p = cpos + 2, pend = p + clen;
        uint64_t eg = 0, eo = 0, np = 0;
        RFS_NEED(pend <= w.ctl_len && p < pend && w.ctl[p] == 0 /* End Of Track */ &&
                     (p++, rfs_vi(w.ctl, pend, &p, &eg)) && rfs_vi(w.ctl, pend, &p, &eo) &&
                     eg == 1 && eo == 3 && rfs_vi(w.ctl, pend, &p, &np) && np == 0,
                 "fetch-ok-fields");
    }
    RFS_NEED(t.origin_obs.nr == 2 &&
                 rfs_request_exact(&t.origin_obs.requests[0], 1, 1, MOQ_GROUP_ORDER_DESCENDING) &&
                 rfs_request_exact(&t.origin_obs.requests[1], 0, 1, MOQ_GROUP_ORDER_DESCENDING),
             "upstream-runs-descending");
    rfs_wire_obj_t objs[8];
    int n = 0;
    uint64_t rid = UINT64_MAX;
    RFS_NEED(w.data_fin && w.resets == 0 && w.data_streams == 1 &&
                 rfs_wire_parse(&w, true, &rid, objs, 8, &n) && rid == 0,
             "one-stream-objects-only");
    static const uint64_t want_g[] = { 1, 1, 1, 0, 0, 0 }, want_o[] = { 0, 1, 2, 0, 1, 2 };
    bool order_ok = n == 6;
    for (int i = 0; order_ok && i < 6; i++) {
        bool retained = want_o[i] == 1;   /* the run fetched upstream */
        uint8_t body[] = { 0x52, (uint8_t)want_g[i], (uint8_t)want_o[i], retained ? 0xa1 : 0xc3,
                           0, 0xfe };
        uint8_t props[] = { 0x7c, retained ? 0x31 : 0x23 };
        order_ok = objs[i].group == want_g[i] && objs[i].object == want_o[i] &&
                   objs[i].body_len == sizeof(body) &&
                   memcmp(objs[i].body, body, sizeof(body)) == 0 &&
                   objs[i].props_len == sizeof(props) &&
                   memcmp(objs[i].props, props, sizeof(props)) == 0;
        if (!order_ok)
            printf("OBS: %s object %d = %llu:%llu props=%zu body=%zu\n", row, i,
                   (unsigned long long)objs[i].group, (unsigned long long)objs[i].object,
                   objs[i].props_len, objs[i].body_len);
    }
    RFS_NEED(order_ok, "groups-descending-objects-ascending-exact");
cleanup:
    rfs_consumer_version = MOQ_VERSION_DRAFT_22;
    rfs_origin_version = MOQ_VERSION_DRAFT_22;
    return rfs_finish(&t, row, missing);
}

int
main(int argc, char **argv)
{
    if (argc != 2) return 2;
    const char *row = argv[1];
    /* d18x:<row>: the same row with a draft-18 origin and subscriber, so the
     * relay's upstream FETCH is a draft-18 one too. */
    if (strncmp(row, "d18x:", 5) == 0) {
        rfs_origin_version = MOQ_VERSION_DRAFT_18;
        rfs_consumer_version = MOQ_VERSION_DRAFT_18;
        return rfs_case(row + 5);
    }
    /* d16:<row> / d18:<row>: the same row for a draft-16/18 FETCH subscriber. */
    if (strncmp(row, "d16:", 4) == 0 || strncmp(row, "d18:", 4) == 0) {
        rfs_consumer_version = row[2] == '6' ? MOQ_VERSION_DRAFT_16 : MOQ_VERSION_DRAFT_18;
        row += 4;
        if (strcmp(row, "cancel-cached") == 0) return rfs_cached_cancel();
        if (strcmp(row, "abort-blocked") == 0) return rfs_abort_blocked();
        return rfs_case(row);
    }
    if (strcmp(argv[1], "d18-descending-wire") == 0) return rfs_d18_descending_wire(false);
    if (strcmp(argv[1], "d18-descending-wire-d18-origin") == 0)
        return rfs_d18_descending_wire(true);
    if (strcmp(argv[1], "cancel-cached") == 0) return rfs_cached_cancel();
    if (strcmp(argv[1], "descending-groups") == 0) return rfs_descending_groups();
    if (strcmp(argv[1], "evicted-range") == 0) return rfs_evicted_range();
    if (strcmp(argv[1], "abort-blocked") == 0) return rfs_abort_blocked();
    if (strcmp(argv[1], "d18-origin") == 0) return rfs_d18_origin();
    if (strcmp(argv[1], "d16-origin") == 0) return rfs_d16_origin();
    if (strcmp(argv[1], "source-lost") == 0) return rfs_source_lost();
    if (strcmp(argv[1], "expired-zero") == 0) return rfs_expired(false);
    if (strcmp(argv[1], "expired-budget") == 0) return rfs_expired(true);
    if (strcmp(argv[1], "coalesce-oom") == 0) return rfs_coalesce(true);
    if (strcmp(argv[1], "coalesce-control") == 0) return rfs_coalesce(false);
    if (strcmp(argv[1], "legacy") == 0) {
        int failures = fetch_flow(MOQ_VERSION_DRAFT_16) + fetch_flow(MOQ_VERSION_DRAFT_18);
        failures += fetch_evicted_marked_unknown() + chunked_fetch_oom_terminates();
        return failures != 0 ? 2 : 0;
    }
    static const char *const cases[] = { "publish-gap", "pull-gap", "cached-only",
        "timeout-zero", "absent-source", "source-error", "source-reset",
        "total-deadline", "cancel-reuse", "capacity-recovery", "descending" };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        if (strcmp(argv[1], cases[i]) == 0) return rfs_case(cases[i]);
    return 2;
}
