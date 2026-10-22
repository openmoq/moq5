/* Real session/binding acceptance: ordinary publisher -> A -> B -> subscriber.
 * Every hop is a SimPair byte transport; no direct engine route injection. */
#include <moq/moq.h>
#include <moq/relay/moqr_bind.h>
#include <moq/relay/moqr_shards.h>
#include <moq/sim.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NEED(x)                                                                                    \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #x);                           \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

typedef struct test_alloc {
    moq_alloc_t api;
    size_t live;
    bool fail;
} test_alloc_t;
static void *test_alloc_new(size_t n, void *ctx)
{
    test_alloc_t *a = ctx;
    if (a->fail)
        return NULL;
    void *p = malloc(n);
    if (p)
        a->live += n;
    return p;
}
static void *test_alloc_resize(void *p, size_t old, size_t n, void *ctx)
{
    test_alloc_t *a = ctx;
    if (a->fail)
        return NULL;
    void *q = realloc(p, n);
    if (q)
        a->live = a->live - old + n;
    return q;
}
static void test_alloc_free(void *p, size_t n, void *ctx)
{
    test_alloc_t *a = ctx;
    if (p) {
        a->live -= n;
        free(p);
    }
}
typedef struct node {
    moqr_core_t *core;
    moqr_bind_t *bind;
    moqr_shards_t *shards;
    test_alloc_t alloc;
} node_t;
typedef struct mesh {
    node_t a, b;
    moq_simpair_t *origin, *peer, *sink, *redundant;
    bool cluster_publisher, redundant_link, sharded, deny_discovery, free_peer, limited,
        fault_updates;
    moq_session_t *publisher, *subscriber;
    moq_subscription_t pub_sub[2], sub[2];
    uint64_t now;
    unsigned found, got_catalog, got_video, sub_ok;
} mesh_t;
static moq_bytes_t bytes(const char *s) { return (moq_bytes_t){(const uint8_t *)s, strlen(s)}; }
static bool eq(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && memcmp(b.data, s, b.len) == 0;
}
static moqr_bind_t *node_bind(node_t *n, uint16_t shard)
{
    return n->shards ? moqr_shards_bind(n->shards, shard) : n->bind;
}
static void deny_namespace(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *verdict)
{
    (void)ctx;
    verdict->decision =
        req->action == MOQR_AUTH_PUBLISH_NAMESPACE ? MOQR_AUTH_DENY : MOQR_AUTH_ALLOW;
    verdict->error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
}
static bool node_create(node_t *n, uint64_t id, bool sharded, bool deny)
{
    n->alloc.api = (moq_alloc_t){.alloc = test_alloc_new,
                                 .realloc = test_alloc_resize,
                                 .free = test_alloc_free,
                                 .ctx = &n->alloc};
    if (sharded) {
        moqr_shards_cfg_t s;
        moqr_shards_cfg_init_sized(&s, sizeof(s), &n->alloc.api);
        if (deny)
            s.core_cfg.authorize = deny_namespace;
        s.shards = 2;
        s.cluster_enabled = true;
        s.cluster_hop_id = id;
        s.admit_remote_demand = true;
        NEED(moqr_shards_create(&s, &n->shards) == MOQR_OK);
        n->core = moqr_shards_core(n->shards, 1);
        n->bind = moqr_shards_bind(n->shards, 0);
        return true;
    }
    moqr_core_relay_cfg_t c;
    moqr_core_relay_cfg_init_sized(&c, sizeof(c), &n->alloc.api);
    if (deny)
        c.authorize = deny_namespace;
    moqr_cluster_cfg_t cl = {.struct_size = sizeof(cl), .enabled = true, .local_hop_id = id};
    NEED(moqr_core_create_cluster(&c, &cl, &n->core) == MOQR_OK);
    moqr_bind_cfg_t b;
    moqr_bind_cfg_init_sized(&b, sizeof(b), &n->alloc.api);
    b.core = n->core;
    b.max_conns = 8;
    NEED(moqr_bind_create(&b, &n->bind) == MOQR_OK);
    return true;
}
static bool connect_pair(moq_simpair_t **out, bool client_cluster, uint64_t client_id,
                         bool server_cluster, uint64_t server_id, bool limited)
{
    moq_simpair_cfg_t c = MOQ_SIMPAIR_CFG_INIT;
    c.alloc = moq_alloc_default();
    c.version = MOQ_VERSION_DRAFT_22;
    if (limited) {
        c.max_actions = 2;
        c.max_events = 2;
    }
    c.client_cluster_enabled = client_cluster;
    c.client_cluster_hop_id = client_id;
    c.server_cluster_enabled = server_cluster;
    c.server_cluster_hop_id = server_id;
    NEED(moq_simpair_create(&c, out) == MOQ_OK);
    NEED(moq_simpair_start(*out) == MOQ_OK);
    NEED(moq_simpair_run_until_quiescent(*out, 128, NULL) == MOQ_OK);
    return true;
}
static bool poll_endpoints(mesh_t *m)
{
    moq_event_t e;
    while (moq_session_poll_events(m->publisher, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            const moq_subscribe_request_event_t *r = &e.u.subscribe_request;
            unsigned i = eq(r->track_name, "catalog") ? 0u : 1u;
            m->pub_sub[i] = r->sub;
            moq_accept_subscribe_cfg_t a;
            moq_accept_subscribe_cfg_init(&a);
            NEED(moq_session_accept_subscribe(m->publisher, r->sub, &a, m->now) == MOQ_OK);
        }
        NEED(e.kind != MOQ_EVENT_SESSION_CLOSED);
        moq_event_cleanup(&e);
    }
    while (moq_session_poll_events(m->subscriber, &e, 1) == 1) {
        if (e.kind == MOQ_EVENT_NAMESPACE_FOUND)
            m->found++;
        if (e.kind == MOQ_EVENT_SUBSCRIBE_OK)
            m->sub_ok++;
        if (e.kind == MOQ_EVENT_OBJECT_RECEIVED && e.u.object_received.payload) {
            const moq_object_received_event_t *o = &e.u.object_received;
            if (o->sub._opaque == m->sub[0]._opaque) {
                const char *catalog = "{\"tracks\":[{\"name\":\"video\"}]}";
                NEED(moq_rcbuf_len(o->payload) == strlen(catalog));
                NEED(memcmp(moq_rcbuf_data(o->payload), catalog, strlen(catalog)) == 0);
                m->got_catalog++;
            }
            if (o->sub._opaque == m->sub[1]._opaque) {
                NEED(o->group_id == m->got_video + 1u);
                NEED(moq_rcbuf_len(o->payload) == 5);
                NEED(memcmp(moq_rcbuf_data(o->payload), "media", 5) == 0);
                m->got_video++;
            }
        }
        NEED(e.kind != MOQ_EVENT_SESSION_CLOSED);
        NEED(e.kind != MOQ_EVENT_SUBSCRIBE_ERROR);
        moq_event_cleanup(&e);
    }
    return true;
}
static bool drive(mesh_t *m, unsigned rounds)
{
    for (unsigned i = 0; i < rounds; i++) {
        m->now += 1000;
        moq_simpair_t *links[] = {m->origin, m->peer, m->sink, m->redundant};
        for (unsigned j = 0; j < 4; j++) {
            if (links[j] == NULL)
                continue;
            NEED(moq_simpair_advance_to(links[j], m->now) == MOQ_OK);
            NEED(moq_simpair_run_until_quiescent(links[j], 128, NULL) == MOQ_OK);
        }
        NEED((m->a.shards ? moqr_shards_step(m->a.shards, m->now)
                          : moqr_bind_pump(m->a.bind, m->now)) == MOQR_OK);
        NEED((m->b.shards ? moqr_shards_step(m->b.shards, m->now)
                          : moqr_bind_pump(m->b.bind, m->now)) == MOQR_OK);
        NEED(poll_endpoints(m));
    }
    return true;
}
static bool write_group(mesh_t *m, unsigned track, uint64_t group, const char *payload)
{
    moq_subgroup_cfg_t c;
    moq_subgroup_cfg_init(&c);
    c.group_id = group;
    moq_subgroup_handle_t sg;
    NEED(moq_session_open_subgroup(m->publisher, m->pub_sub[track], &c, m->now, &sg) == MOQ_OK);
    if (m->limited)
        NEED(drive(m, 2));
    moq_rcbuf_t *p = NULL;
    NEED(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload, strlen(payload), &p) ==
         MOQ_OK);
    moq_result_t rc = moq_session_write_object(m->publisher, sg, 0, p, m->now);
    moq_rcbuf_decref(p);
    NEED(rc == MOQ_OK);
    if (m->limited)
        NEED(drive(m, 2));
    NEED(moq_session_close_subgroup(m->publisher, sg, m->now) == MOQ_OK);
    return true;
}
static bool run_mesh(mesh_t *m)
{
    NEED(node_create(&m->a, 31, m->sharded, false));
    NEED(node_create(&m->b, 32, m->sharded, m->deny_discovery));
    NEED(connect_pair(&m->origin, m->cluster_publisher, 19, m->cluster_publisher, 31, m->limited));
    NEED(connect_pair(&m->peer, true, 31, true, 32, m->limited));
    NEED(connect_pair(&m->sink, false, 0, false, 0, m->limited));
    m->publisher = moq_simpair_client(m->origin);
    m->subscriber = moq_simpair_client(m->sink);
    NEED(moqr_bind_conn_open(m->a.bind, moq_simpair_server(m->origin), MOQ_VERSION_DRAFT_22) ==
         MOQR_OK);
    NEED(moqr_bind_conn_open(node_bind(&m->a, 1), moq_simpair_client(m->peer),
                             MOQ_VERSION_DRAFT_22) == MOQR_OK);
    if (m->free_peer) {
        moqr_bind_cluster_peer_cfg_t policy = {
            .struct_size = sizeof(policy), .has_relay_cost = true, .relay_cost = 0};
        NEED(moqr_bind_conn_open_cluster(m->b.bind, moq_simpair_server(m->peer),
                                         MOQ_VERSION_DRAFT_22, &policy, sizeof(policy)) == MOQR_OK);
    } else {
        NEED(moqr_bind_conn_open(m->b.bind, moq_simpair_server(m->peer), MOQ_VERSION_DRAFT_22) ==
             MOQR_OK);
    }
    NEED(moqr_bind_conn_open(node_bind(&m->b, 1), moq_simpair_server(m->sink),
                             MOQ_VERSION_DRAFT_22) == MOQR_OK);
    if (m->redundant_link) {
        NEED(connect_pair(&m->redundant, true, 32, true, 31, m->limited));
        NEED(moqr_bind_conn_open(node_bind(&m->b, 1), moq_simpair_client(m->redundant),
                                 MOQ_VERSION_DRAFT_22) == MOQR_OK);
        NEED(moqr_bind_conn_open(m->a.bind, moq_simpair_server(m->redundant),
                                 MOQ_VERSION_DRAFT_22) == MOQR_OK);
    }
    NEED(drive(m, 4));
    moq_bytes_t ns = bytes("cluster-demo");
    moq_publish_namespace_cfg_t p;
    moq_publish_namespace_cfg_init(&p);
    p.track_namespace = (moq_namespace_t){&ns, 1};
    moq_announcement_t ann;
    const uint64_t publisher_hop = 19;
    if (m->cluster_publisher)
        p.cluster = (moq_cluster_metadata_t){&publisher_hop, 1, true, 10};
    NEED(moq_session_publish_namespace(m->publisher, &p, m->now, &ann) == MOQ_OK);
    moq_subscribe_namespace_cfg_t d;
    moq_subscribe_namespace_cfg_init(&d);
    d.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    if (m->limited)
        d.track_namespace_prefix = (moq_namespace_t){&ns, 1};
    moq_ns_sub_handle_t dh;
    NEED(moq_session_subscribe_namespace(m->subscriber, &d, m->now, &dh) == MOQ_OK);
    NEED(drive(m, 24));
    moqr_cluster_route_t route;
    if (m->deny_discovery) {
        NEED(m->found == 0);
        NEED(moqr_core_cluster_route(m->b.core, MOQR_BINDING_INVALID, (moqr_ns_t){&ns, 1},
                                     &route) == MOQR_DONE);
        return true;
    }
    NEED(m->found == 1);
    NEED(moqr_core_cluster_route(m->b.core, MOQR_BINDING_INVALID, (moqr_ns_t){&ns, 1}, &route) ==
         MOQR_OK);
    NEED(route.metadata.hop_count == 2);
    NEED(route.metadata.hop_ids[0] == (m->cluster_publisher ? 19u : 0u));
    NEED(route.metadata.hop_ids[1] == 31);
    NEED(route.accumulated_cost == (m->cluster_publisher ? 10u : 0u) + (m->free_peer ? 1u : 2u));
    if (m->cluster_publisher) {
        const uint64_t costs[] = {5, 0};
        for (unsigned i = 0; i < 2; i++) {
            moq_namespace_update_cfg_t update;
            moq_namespace_update_cfg_init(&update);
            update.cluster.has_route_cost = true;
            update.cluster.route_cost = costs[i];
            if (m->fault_updates)
                m->b.alloc.fail = true;
            NEED(moq_session_update_namespace(m->publisher, ann, &update, m->now, NULL) == MOQ_OK);
            NEED(drive(m, 24));
            m->b.alloc.fail = false;
            NEED(moqr_core_cluster_route(m->b.core, MOQR_BINDING_INVALID, (moqr_ns_t){&ns, 1},
                                         &route) == MOQR_OK);
            NEED(route.accumulated_cost == costs[i] + (m->free_peer ? 1u : 2u));
        }
    }
    for (unsigned i = 0; i < 2; i++) {
        moq_subscribe_cfg_t s;
        moq_subscribe_cfg_init(&s);
        s.track_namespace = (moq_namespace_t){&ns, 1};
        s.track_name = bytes(i == 0 ? "catalog" : "video");
        NEED(moq_session_subscribe(m->subscriber, &s, m->now, &m->sub[i]) == MOQ_OK);
        if (m->limited)
            NEED(drive(m, 8));
    }
    NEED(drive(m, 24));
    NEED(m->sub_ok == 2);
    NEED(m->pub_sub[0]._opaque != 0 && m->pub_sub[1]._opaque != 0);
    NEED(write_group(m, 0, 0, "{\"tracks\":[{\"name\":\"video\"}]}"));
    for (unsigned i = 1; i <= 3; i++) {
        NEED(write_group(m, 1, i, "media"));
        NEED(drive(m, 8));
    }
    NEED(m->got_catalog == 1);
    NEED(m->got_video == 3);
    NEED(moq_session_publish_namespace_done(m->publisher, ann, m->now) == MOQ_OK);
    NEED(drive(m, 24));
    NEED(moqr_core_cluster_route(m->b.core, MOQR_BINDING_INVALID, (moqr_ns_t){&ns, 1}, &route) ==
         MOQR_DONE);
    return true;
}
static void cleanup(mesh_t *m)
{
    node_t *nodes[] = {&m->a, &m->b};
    for (unsigned i = 0; i < 2; i++) {
        node_t *n = nodes[i];
        if (n->shards)
            moqr_shards_destroy(n->shards);
        else {
            moqr_bind_destroy(n->bind);
            moqr_core_destroy(n->core);
        }
    }
    moq_simpair_destroy(m->origin);
    moq_simpair_destroy(m->peer);
    moq_simpair_destroy(m->sink);
    moq_simpair_destroy(m->redundant);
}
int main(int argc, char **argv)
{
    unsigned first = 0, last = 9;
    if (argc == 2) {
        if (argv[1][0] < '0' || argv[1][0] > '8' || argv[1][1] != '\0')
            return 2;
        first = (unsigned)(argv[1][0] - '0');
        last = first + 1;
    } else if (argc != 1)
        return 2;
    for (unsigned i = first; i < last; i++) {
        mesh_t m = {0};
        m.cluster_publisher = i != 0;
        m.redundant_link = i == 2 || i == 4;
        m.sharded = i == 3 || i == 4;
        m.deny_discovery = i == 5;
        m.free_peer = i == 6;
        m.limited = i == 7;
        m.fault_updates = i == 8;
        bool ok = run_mesh(&m);
        m.a.alloc.fail = false;
        m.b.alloc.fail = false;
        cleanup(&m);
        if (m.a.alloc.live != 0 || m.b.alloc.live != 0) {
            fprintf(stderr, "cluster mesh leaked allocator bytes: %zu %zu\n", m.a.alloc.live,
                    m.b.alloc.live);
            return 1;
        }
        if (!ok) {
            fprintf(stderr, "cluster mesh case %u failed\n", i);
            return 1;
        }
    }
    puts("PASS: cluster mesh catalog, progressing media, partial cost updates, "
         "redundant links and withdrawal");
    return 0;
}
