/*
 * Track Namespace cardinality is profile-specific: draft-18 permits the root
 * namespace (zero fields), while draft-16 requires at least one field.  Keep
 * the public request APIs aligned with their selected wire profile without
 * weakening the shared draft-16 namespace codec.
 */
#include <moq/moq.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/publisher.h>
#include "test_support.h"
#include "../../core/src/session/profile.h"
#include "../../core/src/session/session_internal.h"

#include <stdlib.h>
#include <string.h>

typedef struct facade_alloc_state {
    size_t alloc_calls;
    size_t realloc_calls;
    size_t free_calls;
    size_t live_allocations;
} facade_alloc_state_t;

static void *facade_alloc(size_t size, void *ctx)
{
    facade_alloc_state_t *state = (facade_alloc_state_t *)ctx;
    void *ptr = malloc(size);
    if (ptr) {
        state->alloc_calls++;
        state->live_allocations++;
    }
    return ptr;
}

static void *facade_realloc(void *ptr, size_t old_size, size_t new_size,
                            void *ctx)
{
    facade_alloc_state_t *state = (facade_alloc_state_t *)ctx;
    (void)old_size;
    if (!ptr)
        return facade_alloc(new_size, ctx);
    if (new_size == 0) {
        state->free_calls++;
        state->live_allocations--;
        free(ptr);
        return NULL;
    }
    void *next = realloc(ptr, new_size);
    if (next)
        state->realloc_calls++;
    return next;
}

static void facade_free(void *ptr, size_t size, void *ctx)
{
    facade_alloc_state_t *state = (facade_alloc_state_t *)ctx;
    (void)size;
    if (ptr) {
        state->free_calls++;
        state->live_allocations--;
    }
    free(ptr);
}

static moq_session_t *make_established_d18(moq_perspective_t perspective)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               perspective);
    cfg.version = MOQ_VERSION_DRAFT_18;

    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK)
        return NULL;
    if (moq_session_start(s, 0) != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }

    moq_action_t action;
    while (moq_session_poll_actions(s, &action, 1) > 0)
        moq_action_cleanup(&action);

    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    if (moq_d18_encode_setup(&w) != MOQ_OK ||
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0)
            != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }

    moq_event_t event;
    while (moq_session_poll_events(s, &event, 1) > 0)
        moq_event_cleanup(&event);
    while (moq_session_poll_actions(s, &action, 1) > 0)
        moq_action_cleanup(&action);

    if (moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static moq_session_t *make_established_d16(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_16;

    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK)
        return NULL;
    if (moq_session_start(s, 0) != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }

    moq_action_t action;
    while (moq_session_poll_actions(s, &action, 1) > 0)
        moq_action_cleanup(&action);

    uint8_t setup[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    if (moq_d16_encode_server_setup(&w, NULL, 0) != MOQ_OK ||
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0)
            != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }

    moq_event_t event;
    while (moq_session_poll_events(s, &event, 1) > 0)
        moq_event_cleanup(&event);
    while (moq_session_poll_actions(s, &action, 1) > 0)
        moq_action_cleanup(&action);

    if (moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static int check_root_request(moq_session_t *s, uint64_t want_type)
{
    int failures = 0;
    moq_action_t action;
    size_t n = moq_session_poll_actions(s, &action, 1);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    if (n != 1)
        return failures;

    MOQ_TEST_CHECK_EQ_INT((int)action.kind,
                          (int)MOQ_ACTION_OPEN_BIDI_STREAM);
    if (action.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
        MOQ_TEST_CHECK(action.u.open_bidi_stream.fin ==
                       (want_type == MOQ_D18_TRACK_STATUS));

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, action.u.open_bidi_stream.data,
                            action.u.open_bidi_stream.len);
        moq_control_envelope_t env;
        moq_result_t rc = moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK_EQ_U64(env.msg_type, want_type);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

            moq_bytes_t parts[1];
            if (want_type == MOQ_D18_PUBLISH_NAMESPACE) {
                moq_d18_publish_namespace_t out;
                rc = moq_d18_decode_publish_namespace(
                    env.payload, env.payload_len, parts, 1, &out);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) {
                    MOQ_TEST_CHECK_EQ_U64(out.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);
                }
            } else if (want_type == MOQ_D18_PUBLISH) {
                moq_d18_publish_t out;
                rc = moq_d18_decode_publish(env.payload, env.payload_len,
                                             parts, 1, &out);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) {
                    MOQ_TEST_CHECK_EQ_U64(out.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);
                    MOQ_TEST_CHECK(out.track_name.len == 1 &&
                                   out.track_name.data != NULL &&
                                   out.track_name.data[0] == 'v');
                }
            } else if (want_type == MOQ_D18_SUBSCRIBE) {
                moq_d18_subscribe_t out;
                rc = moq_d18_decode_subscribe(env.payload, env.payload_len,
                                               parts, 1, &out);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) {
                    MOQ_TEST_CHECK_EQ_U64(out.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);
                    MOQ_TEST_CHECK(out.track_name.len == 1 &&
                                   out.track_name.data != NULL &&
                                   out.track_name.data[0] == 'v');
                }
            } else if (want_type == MOQ_D18_FETCH) {
                moq_d18_fetch_t out;
                rc = moq_d18_decode_fetch(env.payload, env.payload_len,
                                           parts, 1, &out);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) {
                    MOQ_TEST_CHECK_EQ_U64(out.request_id, 0);
                    MOQ_TEST_CHECK_EQ_U64(out.fetch_type,
                                          MOQ_D18_FETCH_TYPE_STANDALONE);
                    MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);
                    MOQ_TEST_CHECK(out.track_name.len == 1 &&
                                   out.track_name.data != NULL &&
                                   out.track_name.data[0] == 'v');
                }
            } else if (want_type == MOQ_D18_TRACK_STATUS) {
                moq_d18_track_status_t out;
                rc = moq_d18_decode_track_status(env.payload, env.payload_len,
                                                  parts, 1, &out);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) {
                    MOQ_TEST_CHECK_EQ_U64(out.request_id, 0);
                    MOQ_TEST_CHECK_EQ_SIZE(out.track_namespace.count, 0);
                    MOQ_TEST_CHECK(out.track_name.len == 1 &&
                                   out.track_name.data != NULL &&
                                   out.track_name.data[0] == 'v');
                }
            } else {
                MOQ_TEST_CHECK(false);
            }
        }
    }
    moq_action_cleanup(&action);

    n = moq_session_poll_actions(s, &action, 1);
    MOQ_TEST_CHECK_EQ_SIZE(n, 0);
    if (n == 1)
        moq_action_cleanup(&action);
    return failures;
}

static int test_d18_public_requests(void)
{
    int failures = 0;
    const moq_namespace_t root = { NULL, 0 };
    const moq_bytes_t name = MOQ_BYTES_LITERAL("v");

    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_publish_namespace_cfg_t cfg;
            moq_publish_namespace_cfg_init(&cfg);
            cfg.track_namespace = root;
            moq_announcement_t handle;
            moq_result_t rc = moq_session_publish_namespace(s, &cfg, 1,
                                                             &handle);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK)
                failures += check_root_request(
                    s, MOQ_D18_PUBLISH_NAMESPACE);
            moq_session_destroy(s);
        }
    }

    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_publish_cfg_t cfg;
            moq_publish_cfg_init(&cfg);
            cfg.track_namespace = root;
            cfg.track_name = name;
            moq_publication_t handle;
            moq_result_t rc = moq_session_publish(s, &cfg, 1, &handle);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK)
                failures += check_root_request(s, MOQ_D18_PUBLISH);
            moq_session_destroy(s);
        }
    }

    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_subscribe_cfg_t cfg;
            moq_subscribe_cfg_init(&cfg);
            cfg.track_namespace = root;
            cfg.track_name = name;
            cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            moq_subscription_t handle;
            moq_result_t rc = moq_session_subscribe(s, &cfg, 1, &handle);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK)
                failures += check_root_request(s, MOQ_D18_SUBSCRIBE);
            moq_session_destroy(s);
        }
    }

    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_fetch_cfg_t cfg;
            moq_fetch_cfg_init(&cfg);
            cfg.track_namespace = root;
            cfg.track_name = name;
            cfg.start_group = 0;
            cfg.start_object = 0;
            cfg.end_group = 10;
            cfg.end_object = 0;
            moq_fetch_t handle;
            moq_result_t rc = moq_session_fetch(s, &cfg, 1, &handle);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK)
                failures += check_root_request(s, MOQ_D18_FETCH);
            moq_session_destroy(s);
        }
    }

    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_track_status_cfg_t cfg;
            moq_track_status_cfg_init(&cfg);
            cfg.track_namespace = root;
            cfg.track_name = name;
            moq_track_status_handle_t handle;
            moq_result_t rc = moq_session_track_status(s, &cfg, 1, &handle);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK)
                failures += check_root_request(s, MOQ_D18_TRACK_STATUS);
            moq_session_destroy(s);
        }
    }

    /* Non-wire consumers of a full track name use the same profile rule. */
    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_note_object_published(s, &root, name, 1, 2),
                (int)MOQ_OK);
            moq_session_destroy(s);
        }
    }
    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_pub_cfg_t pub_cfg;
            moq_pub_cfg_init_sized(&pub_cfg, sizeof(pub_cfg));
            pub_cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
            moq_publisher_t *pub = NULL;
            moq_result_t rc = moq_pub_create(s, moq_alloc_default(),
                                             &pub_cfg, &pub);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            if (rc == MOQ_OK) {
                moq_pub_track_cfg_t track_cfg;
                moq_pub_track_cfg_init(&track_cfg);
                track_cfg.track_namespace = root;
                track_cfg.track_name = name;
                moq_pub_track_t *track = NULL;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_pub_add_track(pub, &track_cfg, 1, &track),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK(track != NULL);
                moq_pub_destroy(pub);
            }
            moq_session_destroy(s);
        }
    }

    /* The same root namespace is accepted on the inbound request path. */
    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            uint8_t msg[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, msg, sizeof(msg));
            moq_d18_msg_params_t params = { 0 };
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_publish_namespace(
                    &w, 0, &root, &params),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(
                    s, moq_stream_ref_from_u64(900), msg,
                    moq_buf_writer_offset(&w), false, 1),
                (int)MOQ_OK);

            moq_event_t event;
            size_t n = moq_session_poll_events(s, &event, 1);
            MOQ_TEST_CHECK_EQ_SIZE(n, 1);
            if (n == 1) {
                MOQ_TEST_CHECK_EQ_INT(
                    (int)event.kind, (int)MOQ_EVENT_NAMESPACE_PUBLISHED);
                if (event.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                    MOQ_TEST_CHECK_EQ_SIZE(
                        event.u.namespace_published.track_namespace.count, 0);
                moq_event_cleanup(&event);
            }
            n = moq_session_poll_events(s, &event, 1);
            MOQ_TEST_CHECK_EQ_SIZE(n, 0);
            if (n == 1)
                moq_event_cleanup(&event);
            moq_session_destroy(s);
        }
    }

    /* A present field must remain non-empty under draft-18. */
    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            const moq_bytes_t empty_part = { NULL, 0 };
            moq_publish_namespace_cfg_t cfg;
            moq_publish_namespace_cfg_init(&cfg);
            cfg.track_namespace = (moq_namespace_t){ &empty_part, 1 };
            moq_announcement_t handle;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_publish_namespace(s, &cfg, 1, &handle),
                (int)MOQ_ERR_INVAL);
            moq_action_t action;
            size_t n = moq_session_poll_actions(s, &action, 1);
            MOQ_TEST_CHECK_EQ_SIZE(n, 0);
            if (n == 1)
                moq_action_cleanup(&action);
            moq_session_destroy(s);
        }
    }

    return failures;
}

static int test_d16_public_requests(void)
{
    int failures = 0;
    const moq_namespace_t root = { NULL, 0 };
    const moq_bytes_t name = MOQ_BYTES_LITERAL("v");
    moq_session_t *s = make_established_d16();
    MOQ_TEST_CHECK(s != NULL);
    if (!s)
        return failures;

    moq_publish_namespace_cfg_t pns;
    moq_publish_namespace_cfg_init(&pns);
    pns.track_namespace = root;
    moq_announcement_t ann;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish_namespace(s, &pns, 1, &ann),
        (int)MOQ_ERR_INVAL);

    moq_publish_cfg_t pub;
    moq_publish_cfg_init(&pub);
    pub.track_namespace = root;
    pub.track_name = name;
    moq_publication_t publication;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_publish(s, &pub, 1, &publication),
        (int)MOQ_ERR_INVAL);

    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    sub.track_namespace = root;
    sub.track_name = name;
    sub.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t subscription;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_subscribe(s, &sub, 1, &subscription),
        (int)MOQ_ERR_INVAL);

    moq_fetch_cfg_t fetch;
    moq_fetch_cfg_init(&fetch);
    fetch.track_namespace = root;
    fetch.track_name = name;
    fetch.end_group = 10;
    moq_fetch_t fetch_handle;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_fetch(s, &fetch, 1, &fetch_handle),
        (int)MOQ_ERR_INVAL);

    moq_track_status_cfg_t status;
    moq_track_status_cfg_init(&status);
    status.track_namespace = root;
    status.track_name = name;
    moq_track_status_handle_t status_handle;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_track_status(s, &status, 1, &status_handle),
        (int)MOQ_ERR_INVAL);

    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_note_object_published(s, &root, name, 1, 2),
        (int)MOQ_ERR_INVAL);
    moq_action_t action;
    size_t n = moq_session_poll_actions(s, &action, 1);
    MOQ_TEST_CHECK_EQ_SIZE(n, 0);
    if (n == 1)
        moq_action_cleanup(&action);
    moq_session_destroy(s);
    return failures;
}

static int test_d16_publisher_facade(void)
{
    int failures = 0;
    const moq_namespace_t root = { NULL, 0 };
    const moq_bytes_t name = MOQ_BYTES_LITERAL("v");
    moq_session_t *s = make_established_d16();
    MOQ_TEST_CHECK(s != NULL);
    if (!s)
        return failures;

    facade_alloc_state_t alloc_state = { 0 };
    const moq_alloc_t alloc = {
        .ctx = &alloc_state,
        .alloc = facade_alloc,
        .realloc = facade_realloc,
        .free = facade_free,
    };
    moq_pub_cfg_t pub_cfg;
    moq_pub_cfg_init_sized(&pub_cfg, sizeof(pub_cfg));
    pub_cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_result_t rc = moq_pub_create(s, &alloc, &pub_cfg, &pub);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(pub != NULL);
    if (rc == MOQ_OK && pub) {
        const facade_alloc_state_t before = alloc_state;
        int sentinel;
        moq_pub_track_t *track = (moq_pub_track_t *)(void *)&sentinel;
        moq_pub_track_cfg_t track_cfg;
        moq_pub_track_cfg_init(&track_cfg);
        track_cfg.track_namespace = root;
        track_cfg.track_name = name;

        rc = moq_pub_add_track(pub, &track_cfg, 1, &track);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(track == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(alloc_state.alloc_calls,
                               before.alloc_calls);
        MOQ_TEST_CHECK_EQ_SIZE(alloc_state.realloc_calls,
                               before.realloc_calls);
        MOQ_TEST_CHECK_EQ_SIZE(alloc_state.free_calls,
                               before.free_calls);
        MOQ_TEST_CHECK_EQ_SIZE(alloc_state.live_allocations,
                               before.live_allocations);

        moq_action_t action;
        size_t n = moq_session_poll_actions(s, &action, 1);
        MOQ_TEST_CHECK_EQ_SIZE(n, 0);
        if (n == 1)
            moq_action_cleanup(&action);
        moq_event_t event;
        n = moq_session_poll_events(s, &event, 1);
        MOQ_TEST_CHECK_EQ_SIZE(n, 0);
        if (n == 1)
            moq_event_cleanup(&event);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s),
                              (int)MOQ_SESS_ESTABLISHED);
        moq_pub_destroy(pub);
        MOQ_TEST_CHECK_EQ_SIZE(alloc_state.live_allocations, 0);
    }
    moq_session_destroy(s);
    return failures;
}

static int test_profile_wire_boundaries(void)
{
    int failures = 0;
    const moq_namespace_t root = { NULL, 0 };
    uint8_t buf[128];

    const moq_profile_ops_t *d16 = moq_profile_lookup(MOQ_VERSION_DRAFT_16);
    const moq_profile_ops_t *d18 = moq_profile_lookup(MOQ_VERSION_DRAFT_18);
    MOQ_TEST_CHECK(d16 != NULL);
    MOQ_TEST_CHECK(d18 != NULL);
    if (d16)
        MOQ_TEST_CHECK_EQ_SIZE(d16->min_track_namespace_fields, 1);
    if (d18)
        MOQ_TEST_CHECK_EQ_SIZE(d18->min_track_namespace_fields, 0);

    /* The shared draft-16 Track Namespace codec remains 1..32. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_namespace(&w, &root),
                              (int)MOQ_ERR_INVAL);
    }
    {
        const uint8_t encoded_root[] = { 0 };
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, encoded_root, sizeof(encoded_root));
        moq_bytes_t parts[1];
        moq_namespace_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_buf_read_namespace(&r, parts, 1, &out),
            (int)MOQ_ERR_PROTO);
    }
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d16_encode_subscribe(&w, 0, &root,
                                           MOQ_BYTES_LITERAL("v"), NULL, 0),
            (int)MOQ_ERR_INVAL);
    }
    {
        const uint8_t payload[] = { 0, 0 }; /* Request ID, namespace count. */
        moq_bytes_t parts[1];
        moq_d16_subscribe_t out = { .params = NULL, .params_cap = 0 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d16_decode_subscribe(payload, sizeof(payload), parts, 1,
                                           &out),
            (int)MOQ_ERR_PROTO);
    }

    /* Draft-18 permits zero fields, but never a zero-length present field. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_buf_write_vi64(&w, 0);  /* Request ID. */
        moq_buf_write_vi64(&w, 1);  /* One namespace field. */
        moq_buf_write_vi64(&w, 0);  /* Invalid zero-length field. */
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        moq_buf_write_vi64(&w, 0);  /* No parameters. */
        moq_bytes_t parts[1];
        moq_d18_subscribe_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(buf, moq_buf_writer_offset(&w),
                                           parts, 1, &out),
            (int)MOQ_ERR_PROTO);
    }

    return failures;
}

/*
 * The public capability query a forwarder asks before handing a namespace to
 * this session. Draft-16 requires 1..32 fields, draft-18 permits the root, and
 * the answer must come from the session's own profile -- never from a draft
 * number the caller tests itself.
 */
static int test_zero_field_namespace_capability(void)
{
    int failures = 0;

    /* NULL is false, and asking is safe. */
    MOQ_TEST_CHECK(!moq_session_supports_zero_field_track_namespace(NULL));

    /* Draft-16: the root namespace is not representable. */
    {
        moq_session_t *s = make_established_d16();
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            MOQ_TEST_CHECK(!moq_session_supports_zero_field_track_namespace(s));
            moq_session_destroy(s);
        }
    }

    /* Draft-18: it is. Both perspectives -- the capability is the profile's,
     * not a role's. */
    {
        const moq_perspective_t persp[2] = { MOQ_PERSPECTIVE_CLIENT,
                                             MOQ_PERSPECTIVE_SERVER };
        for (size_t i = 0; i < 2; i++) {
            moq_session_t *s = make_established_d18(persp[i]);
            MOQ_TEST_CHECK(s != NULL);
            if (!s) continue;
            MOQ_TEST_CHECK(moq_session_supports_zero_field_track_namespace(s));
            moq_session_destroy(s);
        }
    }

    /* Observing: repeated queries change no session state and produce no
     * output. State, borrow epoch and both queue depths are captured before
     * and compared after, and the answer is stable across the calls. */
    {
        moq_session_t *s = make_established_d18(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            moq_session_state_t st0 = moq_session_state(s);
            uint64_t epoch0 = s->borrow_epoch;
            size_t events0 = s->event_tail - s->event_head;
            size_t actions0 = s->action_tail - s->action_head;

            for (int i = 0; i < 4; i++)
                MOQ_TEST_CHECK(
                    moq_session_supports_zero_field_track_namespace(s));

            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)st0);
            MOQ_TEST_CHECK_EQ_U64(s->borrow_epoch, epoch0);
            MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, events0);
            MOQ_TEST_CHECK_EQ_SIZE(s->action_tail - s->action_head, actions0);

            moq_event_t ev;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events(s, &ev, 1), 0);
            moq_action_t act;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions(s, &act, 1), 0);
            moq_session_destroy(s);
        }
    }

    /* The capability tracks the profile field it is derived from, so the two
     * cannot drift apart silently. */
    {
        const moq_profile_ops_t *d16 = moq_profile_lookup(MOQ_VERSION_DRAFT_16);
        const moq_profile_ops_t *d18 = moq_profile_lookup(MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(d16 != NULL && d18 != NULL);
        if (d16) MOQ_TEST_CHECK(d16->min_track_namespace_fields > 0);
        if (d18) MOQ_TEST_CHECK_EQ_SIZE(d18->min_track_namespace_fields, 0);
    }

    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_d18_public_requests();
    failures += test_d16_public_requests();
    failures += test_d16_publisher_facade();
    failures += test_profile_wire_boundaries();
    failures += test_zero_field_namespace_capability();
    MOQ_TEST_PASS("namespace_cardinality");
    return failures;
}
