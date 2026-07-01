/*
 * Publisher/Subscriber Session Example
 *
 * Demonstrates end-to-end object delivery: a publisher writes objects
 * through the session API, and a subscriber receives them as
 * OBJECT_RECEIVED events.
 *
 * The control plane (setup, subscribe, accept) is wired back-to-back
 * between two sessions using byte pumping. The data plane (SEND_DATA
 * actions) is fed into the subscriber's data input, simulating what
 * a QUIC adapter would do.
 *
 * Steps:
 *   1. Create client + server sessions with request credit
 *   2. Setup handshake via action pumping
 *   3. Client subscribes to a track
 *   4. Server accepts the subscription
 *   5. Server opens a subgroup and publishes objects
 *   6. Feed data actions to subscriber as incoming data bytes
 *   7. Subscriber receives OBJECT_RECEIVED events with exact payloads
 *   8. Zero memory leaks verified by counting allocator
 */

#include <moq/moq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/*  Counting Allocator                                                */
/* ================================================================== */

typedef struct {
    int64_t allocs;
    int64_t frees;
} alloc_stats_t;

static void *counted_alloc(size_t size, void *ctx)
{
    if (size == 0) return NULL;
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    void *p = malloc(size);
    if (p) s->allocs++;
    return p;
}

static void *counted_realloc(void *ptr, size_t old_size, size_t new_size,
                              void *ctx)
{
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    (void)old_size;
    if (new_size == 0) {
        if (ptr) { free(ptr); s->frees++; }
        return NULL;
    }
    if (!ptr) {
        void *p = malloc(new_size);
        if (p) s->allocs++;
        return p;
    }
    return realloc(ptr, new_size);
}

static void counted_free(void *ptr, size_t size, void *ctx)
{
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    (void)size;
    if (ptr) s->frees++;
    free(ptr);
}

/* ================================================================== */
/*  Pump: one session's outbound actions → the other's inbound bytes  */
/* ================================================================== */

static void pump(moq_session_t *from, moq_session_t *to, uint64_t now)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, now);
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

/* ================================================================== */
/*  Main                                                              */
/* ================================================================== */

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return 1; \
    } \
} while (0)

int main(void)
{
    alloc_stats_t stats = {0};
    moq_alloc_t alloc = { &stats, counted_alloc, counted_realloc, counted_free };
    uint64_t now = 1000;

    /* ============================================================== */
    printf("1. Setup\n");
    /* ============================================================== */

    moq_session_cfg_t client_cfg;
    moq_session_cfg_init_sized(&client_cfg, sizeof(client_cfg), &alloc, MOQ_PERSPECTIVE_CLIENT);
    client_cfg.send_request_capacity = true;      
    client_cfg.initial_request_capacity = 10;     

    moq_session_cfg_t server_cfg;
    moq_session_cfg_init_sized(&server_cfg, sizeof(server_cfg), &alloc, MOQ_PERSPECTIVE_SERVER);
    server_cfg.send_request_capacity = true;      
    server_cfg.initial_request_capacity = 10;     

    moq_session_t *client = NULL, *server = NULL;
    CHECK(moq_session_create(&client_cfg, now, &client) == MOQ_OK);
    CHECK(moq_session_create(&server_cfg, now, &server) == MOQ_OK);

    CHECK(moq_session_start(client, now) == MOQ_OK);
    pump(client, server, now);
    pump(server, client, now);

    /* Drain setup events. */
    moq_event_t ev;
    moq_session_poll_events(client, &ev, 1);
    moq_session_poll_events(server, &ev, 1);

    CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    printf("   client + server: ESTABLISHED\n");

    /* ============================================================== */
    printf("2. Subscribe\n");
    /* ============================================================== */

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("example.com") };
    moq_namespace_t ns = { ns_parts, 1 };

    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

    moq_subscription_t sub;
    CHECK(moq_session_subscribe(client, &sub_cfg, now, &sub) == MOQ_OK);
    printf("   client: SUBSCRIBE sent\n");

    pump(client, server, now);

    CHECK(moq_session_poll_events(server, &ev, 1) == 1);
    CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    printf("   server: received SUBSCRIBE for \"%.*s\"\n",
           (int)ev.u.subscribe_request.track_name.len,
           (const char *)ev.u.subscribe_request.track_name.data);

    /* ============================================================== */
    printf("3. Accept\n");
    /* ============================================================== */

    moq_subscription_t server_sub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.has_largest = true;
    accept.largest_group = 0;
    accept.largest_object = 0;

    CHECK(moq_session_accept_subscribe(server, server_sub, &accept, now) == MOQ_OK);
    pump(server, client, now);

    CHECK(moq_session_poll_events(client, &ev, 1) == 1);
    CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
    printf("   client: SUBSCRIBE_OK, alias=%llu\n",
           (unsigned long long)ev.u.subscribe_ok.track_alias);

    /* ============================================================== */
    printf("4. Publish objects\n");
    /* ============================================================== */

    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 128;

    moq_subgroup_handle_t sg;
    CHECK(moq_session_open_subgroup(server, server_sub, &sg_cfg, now, &sg) == MOQ_OK);
    printf("   server: opened subgroup (group=0, subgroup=0)\n");

    /* Send three objects. */
    const char *frames[] = { "frame-0", "frame-1", "frame-2" };
    for (int i = 0; i < 3; i++) {
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(&alloc, (const uint8_t *)frames[i],
                                strlen(frames[i]), &payload) == MOQ_OK);

        CHECK(moq_session_write_object(server, sg, (uint64_t)i,
                                        payload, now) == MOQ_OK);
        moq_rcbuf_decref(payload);
        printf("   server: wrote object %d (%zu bytes)\n",
               i, strlen(frames[i]));
    }

    /* ============================================================== */
    printf("5. Close subgroup\n");
    /* ============================================================== */

    CHECK(moq_session_close_subgroup(server, sg, now) == MOQ_OK);
    printf("   server: subgroup closed with FIN\n");

    /* ============================================================== */
    printf("6. Deliver data to subscriber\n");
    /* ============================================================== */

    /* A real adapter would send SEND_DATA actions to the QUIC transport,
     * and the peer adapter would feed received bytes into on_data_bytes.
     * Here we short-circuit: poll SEND_DATA from the publisher and feed
     * header bytes then payload bytes directly to the subscriber. */
    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(1);
    moq_action_t acts[16];
    size_t na;
    while ((na = moq_session_poll_actions(server, acts, 16)) > 0) {
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                bool has_payload = (acts[i].u.send_data.payload != NULL);
                bool data_fin = acts[i].u.send_data.fin;

                if (acts[i].u.send_data.header_len > 0) {
                    CHECK(moq_session_on_data_bytes(client, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        data_fin && !has_payload, now) == MOQ_OK);
                }
                if (has_payload) {
                    CHECK(moq_session_on_data_bytes(client, rx_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        data_fin, now) == MOQ_OK);
                }
                if (!has_payload && acts[i].u.send_data.header_len == 0 && data_fin) {
                    CHECK(moq_session_on_data_bytes(client, rx_ref,
                        NULL, 0, true, now) == MOQ_OK);
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    /* ============================================================== */
    printf("7. Receive objects\n");
    /* ============================================================== */

    for (int i = 0; i < 3; i++) {
        CHECK(moq_session_poll_events(client, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        CHECK(ev.u.object_received.object_id == (uint64_t)i);
        CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
        CHECK(ev.u.object_received.payload != NULL);
        CHECK(moq_rcbuf_len(ev.u.object_received.payload) == strlen(frames[i]));
        CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                     frames[i], strlen(frames[i])) == 0);
        printf("   client: received object %d \"%.*s\" (%zu bytes)\n",
               i,
               (int)moq_rcbuf_len(ev.u.object_received.payload),
               (const char *)moq_rcbuf_data(ev.u.object_received.payload),
               moq_rcbuf_len(ev.u.object_received.payload));
        moq_event_cleanup(&ev);
    }

    /* ============================================================== */
    printf("8. Cleanup\n");
    /* ============================================================== */

    moq_session_destroy(client);
    moq_session_destroy(server);

    printf("   allocs: %lld, frees: %lld, balance: %lld\n",
           (long long)stats.allocs, (long long)stats.frees,
           (long long)(stats.allocs - stats.frees));
    CHECK(stats.allocs == stats.frees);
    printf("   zero leaks confirmed\n");

    printf("\ndone.\n");
    return 0;
}
