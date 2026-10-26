#ifndef MOQ_TEST_CLUSTER_TRANSPORT_FLOW_H
#define MOQ_TEST_CLUSTER_TRANSPORT_FLOW_H

#include <moq/moq.h>
#include <stdio.h>
#include <string.h>

/* Shared assertions run through independent native QUIC and H3/WT packet
 * simulators. The harness owns transport progress; no bytes bypass it. */
static int cluster_packet_track(moq_session_t *client, moq_session_t *server, void *context,
                                int (*pump_packets)(void *), uint64_t *now,
                                const moq_namespace_t *ns, const char *track, unsigned groups)
{
    int errors = 0;
#define CP_CHECK(expr)                                                                             \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "cluster packet flow %s:%d: %s\n", __FILE__, __LINE__, #expr);         \
            ++errors;                                                                              \
        }                                                                                          \
    } while (0)
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = *ns;
    sub_cfg.track_name = (moq_bytes_t){(const uint8_t *)track, strlen(track)};
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    moq_result_t rc = moq_session_subscribe(client, &sub_cfg, *now, &sub);
    CP_CHECK(rc == MOQ_OK);
    if (rc < 0)
        return errors;
    CP_CHECK(pump_packets(context) == 0);
    moq_event_t ev;
    moq_subscription_t incoming = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(server, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            incoming = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    CP_CHECK(moq_subscription_is_valid(incoming));
    if (!moq_subscription_is_valid(incoming))
        return errors;
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    CP_CHECK(moq_session_accept_subscribe(server, incoming, &accept, *now) == MOQ_OK);
    CP_CHECK(pump_packets(context) == 0);
    unsigned accepted = 0;
    while (moq_session_poll_events(client, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK)
            ++accepted;
        moq_event_cleanup(&ev);
    }
    CP_CHECK(accepted == 1);
    for (unsigned group = 0; group < groups; group++) {
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = group;
        moq_subgroup_handle_t sg;
        rc = moq_session_open_subgroup(server, incoming, &cfg, *now, &sg);
        CP_CHECK(rc == MOQ_OK);
        if (rc < 0)
            return errors;
        const char *catalog = "{\"version\":1,\"tracks\":[{\"name\":\"video\"}]}";
        uint8_t media[] = {0, 0, 0, 1, 0x65, (uint8_t)group};
        const uint8_t *data = groups == 1 ? (const uint8_t *)catalog : media;
        size_t length = groups == 1 ? strlen(catalog) : sizeof(media);
        moq_rcbuf_t *payload = NULL;
        rc = moq_rcbuf_create(moq_alloc_default(), data, length, &payload);
        CP_CHECK(rc == MOQ_OK);
        if (rc < 0)
            return errors;
        CP_CHECK(moq_session_write_object(server, sg, 0, payload, *now) == MOQ_OK);
        moq_rcbuf_decref(payload);
        CP_CHECK(moq_session_close_subgroup(server, sg, *now) == MOQ_OK);
        CP_CHECK(pump_packets(context) == 0);
        unsigned objects = 0;
        while (moq_session_poll_events(client, &ev, 1)) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                ++objects;
                CP_CHECK(o->group_id == group && o->object_id == 0 && o->payload);
                if (o->payload) {
                    CP_CHECK(moq_rcbuf_len(o->payload) == length);
                    if (moq_rcbuf_len(o->payload) == length)
                        CP_CHECK(!memcmp(moq_rcbuf_data(o->payload), data, length));
                }
            }
            moq_event_cleanup(&ev);
        }
        CP_CHECK(objects == 1);
    }
#undef CP_CHECK
    return errors;
}

static int cluster_packet_flow(moq_session_t *client, moq_session_t *server, void *context,
                               int (*pump_packets)(void *), uint64_t *now)
{
    int errors = 0;
#define CP_CHECK(expr)                                                                             \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            fprintf(stderr, "cluster packet flow %s:%d: %s\n", __FILE__, __LINE__, #expr);         \
            ++errors;                                                                              \
        }                                                                                          \
    } while (0)
    moq_cluster_info_t ci = {0}, si = {0};
    CP_CHECK(moq_session_cluster_info(client, &ci, sizeof(ci)) == MOQ_OK);
    CP_CHECK(moq_session_cluster_info(server, &si, sizeof(si)) == MOQ_OK);
    CP_CHECK(ci.negotiated && si.negotiated);
    CP_CHECK(ci.peer_hop_id == 202 && si.peer_hop_id == 101);
    CP_CHECK(!ci.peer_has_relay_cost && ci.peer_relay_cost == 1);
    CP_CHECK(si.peer_has_relay_cost && si.peer_relay_cost == 0);
    moq_bytes_t part = {(const uint8_t *)"packet-cluster", 14};
    moq_namespace_t ns = {&part, 1};
    uint64_t path[] = {202};
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = ns;
    cfg.cluster = (moq_cluster_metadata_t){path, 1, true, 19};
    moq_announcement_t outgoing = MOQ_ANNOUNCEMENT_INVALID;
    moq_result_t rc = moq_session_publish_namespace(server, &cfg, *now, &outgoing);
    CP_CHECK(rc == MOQ_OK);
    if (rc < 0)
        return errors;
    CP_CHECK(pump_packets(context) == 0);
    moq_event_t ev;
    moq_announcement_t incoming = MOQ_ANNOUNCEMENT_INVALID;
    unsigned announces = 0;
    while (moq_session_poll_events(client, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            ++announces;
            incoming = ev.u.namespace_published.ann;
            const moq_cluster_metadata_t *m = &ev.u.namespace_published.cluster;
            CP_CHECK(m->hop_count == 1 && m->hop_ids && m->hop_ids[0] == 202);
            CP_CHECK(m->has_route_cost && m->route_cost == 19);
        }
        moq_event_cleanup(&ev);
    }
    CP_CHECK(announces == 1);
    if (announces != 1)
        return errors;
    moq_accept_namespace_cfg_t accept;
    moq_accept_namespace_cfg_init(&accept);
    CP_CHECK(moq_session_accept_namespace(client, incoming, &accept, *now) == MOQ_OK);
    CP_CHECK(pump_packets(context) == 0);
    unsigned accepted = 0;
    while (moq_session_poll_events(server, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED)
            ++accepted;
        moq_event_cleanup(&ev);
    }
    CP_CHECK(accepted == 1);
    moq_namespace_update_cfg_t update;
    moq_namespace_update_cfg_init(&update);
    update.cluster.has_route_cost = true;
    update.cluster.route_cost = 0;
    uint64_t id = 0;
    CP_CHECK(moq_session_update_namespace(server, outgoing, &update, *now, &id) == MOQ_OK);
    CP_CHECK(pump_packets(context) == 0);
    unsigned results = 0;
    while (moq_session_poll_events(server, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_UPDATE_RESULT) {
            ++results;
            CP_CHECK(ev.u.namespace_update_result.update_request_id == id);
            CP_CHECK(ev.u.namespace_update_result.outcome == MOQ_NAMESPACE_UPDATE_OUTCOME_OK);
        }
        moq_event_cleanup(&ev);
    }
    CP_CHECK(results == 1);
    while (moq_session_poll_events(client, &ev, 1))
        moq_event_cleanup(&ev);
    moq_cluster_metadata_t metadata = {0};
    CP_CHECK(moq_session_namespace_cluster(client, incoming, &metadata, sizeof(metadata)) ==
             MOQ_OK);
    CP_CHECK(metadata.hop_count == 1 && metadata.hop_ids[0] == 202 && metadata.has_route_cost &&
             metadata.route_cost == 0);
    errors += cluster_packet_track(client, server, context, pump_packets, now, &ns, "catalog", 1);
    errors += cluster_packet_track(client, server, context, pump_packets, now, &ns, "video", 3);
    CP_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
    CP_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
#undef CP_CHECK
    return errors;
}
#endif
