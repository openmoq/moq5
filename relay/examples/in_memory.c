#include <moq/relay/relay.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <string.h>

#define REQUIRE(expr) do { if (!(expr)) { \
    fprintf(stderr, "in_memory:%d: %s\n", __LINE__, #expr); goto done; \
} } while (0)

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    moqr_core_relay_cfg_t cfg;
    moqr_core_t *core = NULL;
    moq_rcbuf_t *payload = NULL;
    moqr_binding_t publisher, subscriber;
    moqr_track_t track;
    moqr_sub_t sub;
    moq_bytes_t part = {(const uint8_t *)"example", 7};
    moqr_ns_t ns = {&part, 1};
    moq_bytes_t name = {(const uint8_t *)"track", 5};
    const uint8_t message[] = "one retained object";
    const uint64_t now_us = 100;
    int result = 1;

    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), alloc);
    REQUIRE(moqr_core_create(&cfg, &core) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(core, 1, &publisher) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(core, 2, &subscriber) == MOQR_OK);
    REQUIRE(moqr_core_announce(core, publisher, ns) == MOQR_OK);
    REQUIRE(moqr_core_publish_open(core, publisher, ns, name, 10, &track) == MOQR_OK);
    moqr_intent_t intent;
    REQUIRE(moqr_core_poll_intents(core, &intent, 1) == 1);
    REQUIRE(intent.kind == MOQR_INTENT_ACCEPT_PUBLISH);

    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.cookie = 20;
    req.filter.type = MOQR_FILTER_ABSOLUTE_START;
    REQUIRE(moqr_core_subscribe(core, subscriber, &req, &sub) == MOQR_OK);
    REQUIRE(moqr_core_poll_intents(core, &intent, 1) == 1);
    REQUIRE(intent.kind == MOQR_INTENT_ACCEPT_SUB && intent.cookie == 20);

    REQUIRE(moq_rcbuf_create(alloc, message, sizeof(message), &payload) == MOQ_OK);
    moqr_log_append_desc_t desc;
    moqr_log_append_desc_init(&desc);
    desc.group_id = 1;
    desc.payload = payload;
    desc.now_us = now_us;
    REQUIRE(moqr_core_ingest(core, track, &desc) == MOQR_OK);
    payload = NULL; /* Successful ingest transferred this reference. */

    moqr_delivery_t delivery;
    REQUIRE(moqr_core_next_delivery(core, subscriber, now_us, &delivery) == MOQR_OK);
    REQUIRE(delivery.notice == MOQR_DELIVERY_NOTICE_NONE);
    REQUIRE(moqr_core_delivery_done(core, subscriber, MOQR_DELIVERY_WOULD_BLOCK,
                                    now_us) == MOQR_OK);
    /* A refused write must be retried, not acknowledged as delivered. */
    REQUIRE(moqr_core_next_delivery(core, subscriber, now_us, &delivery) == MOQR_OK);
    REQUIRE(delivery.sub_cookie == 20 && delivery.rec.group_id == 1);
    REQUIRE(moq_rcbuf_len(delivery.rec.payload) == sizeof(message));
    REQUIRE(memcmp(moq_rcbuf_data(delivery.rec.payload), message, sizeof(message)) == 0);
    REQUIRE(moqr_core_delivery_done(core, subscriber, MOQR_DELIVERY_DELIVERED,
                                    now_us) == MOQR_OK);
    REQUIRE(moqr_core_next_delivery(core, subscriber, now_us, &delivery) == MOQR_DONE);
    moqr_core_stats_t stats;
    REQUIRE(moqr_core_get_stats_sized(core, &stats, sizeof(stats)) == MOQR_OK);
    result = 0;
done:
    if (payload != NULL) {
        moq_rcbuf_decref(payload);
    }
    /* No external callbacks or sessions exist here; destruction releases the
     * remaining bindings, subscription and retained log references. */
    moqr_core_destroy(core);
    return result;
}
