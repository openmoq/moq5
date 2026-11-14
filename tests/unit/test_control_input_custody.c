/*
 * Control-input custody: what moq_session_on_control_bytes() and
 * moq_session_process_pending() say about the bytes they were given.
 * MOQ_ERR_BUFFER means no byte of the delivery was copied, and the caller
 * keeps it. Once the bytes are copied, a message that waits for outputs to
 * drain is MOQ_ERR_WOULD_BLOCK, and one whose handling can never fit this
 * side's own output capacity is MOQ_ERR_LIMIT; either way the message stays
 * buffered, once, and the caller does not feed it again.
 *
 * Rows:
 *   - a draft-16 server whose SERVER_SETUP encodes but is larger than its
 *     send buffer: the valid CLIENT_SETUP is buffered exactly once, every
 *     attempt is LIMIT, nothing is queued, the state stays, and the session's
 *     allocations balance at destroy;
 *   - the same CLIENT_SETUP when an allocation fails while it is handled:
 *     NOMEM with the input buffered, then one SERVER_SETUP once memory
 *     returns;
 *   - a refusal the server sends while its send buffer holds earlier output:
 *     WOULD_BLOCK, then MOQ_OK after a drain, with one REQUEST_ERROR;
 *   - the same refusal while its action queue is full: WOULD_BLOCK, then
 *     MOQ_OK after a drain;
 *   - drafts 18 and 22: a SETUP whose handling fails after its commit (an
 *     allocation refused while dispatching a request that arrived before it)
 *     stays consumed, so a retry does not handle it a second time;
 *   - a delivery larger than the control buffer's free room, on drafts 16,
 *     18 and 22: BUFFER with nothing copied, and the same session then takes
 *     a delivery that fits and completes its setup.
 */
#include <moq/moq.h>
#include <moq/control.h>
#include "test_binding_support.h"
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* -- An allocator that counts, and can refuse one allocation ------------ */

typedef struct {
    int64_t     balance;
    unsigned    fail_in;   /* refuse the fail_in-th allocation from now; 0: none */
    moq_alloc_t alloc;     /* the session's allocator, this state its context */
} fail_alloc_t;

static void *fa_alloc(size_t size, void *ctx)
{
    fail_alloc_t *st = (fail_alloc_t *)ctx;
    if (st->fail_in > 0 && --st->fail_in == 0) return NULL;
    void *p = malloc(size);
    if (p) st->balance++;
    return p;
}

static void *fa_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}

static void fa_free(void *ptr, size_t size, void *ctx)
{
    fail_alloc_t *st = (fail_alloc_t *)ctx;
    (void)size;
    if (ptr) st->balance--;
    free(ptr);
}

/* -- Sessions --------------------------------------------------------------- */

typedef struct {
    moq_version_t           version;
    moq_perspective_t       perspective;
    uint32_t                send_buffer_size;
    uint32_t                recv_buffer_size;
    uint32_t                max_actions;
    const moq_auth_token_t *tokens;
    size_t                  token_count;
    fail_alloc_t           *alloc;
} opts_t;

static moq_session_t *make(const opts_t *o)
{
    const moq_alloc_t *alloc = moq_alloc_default();
    if (o->alloc) {
        o->alloc->alloc = (moq_alloc_t){ .ctx = o->alloc, .alloc = fa_alloc,
                                         .realloc = fa_realloc, .free = fa_free };
        alloc = &o->alloc->alloc;
    }
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, o->perspective);
    cfg.version = o->version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.send_buffer_size = o->send_buffer_size;
    cfg.recv_buffer_size = o->recv_buffer_size;
    cfg.max_actions = o->max_actions;
    cfg.setup_auth_tokens = o->tokens;
    cfg.setup_auth_token_count = o->token_count;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK) return NULL;
    if (test_bind_if_required(s) != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

/* The control bytes `from` queued, concatenated, every action dropped. */
static size_t take_control(moq_session_t *from, uint8_t *out, size_t cap)
{
    size_t n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        const uint8_t *d = NULL;
        size_t len = 0;
        if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            d = a.u.send_control.data;
            len = a.u.send_control.len;
        } else if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
            d = a.u.open_uni_control.data;
            len = a.u.open_uni_control.len;
        } else if (a.kind == MOQ_ACTION_SEND_UNI_CONTROL) {
            d = a.u.send_uni_control.data;
            len = a.u.send_uni_control.len;
        }
        if (d && len <= cap - n) {
            memcpy(out + n, d, len);
            n += len;
        }
        moq_action_cleanup(&a);
    }
    return n;
}

/* Deliver the control bytes `from` queued to `to`. */
static moq_result_t pass_control(moq_session_t *from, moq_session_t *to, uint64_t now)
{
    uint8_t buf[4096];
    size_t n = take_control(from, buf, sizeof(buf));
    if (n == 0) return MOQ_OK;
    return moq_session_on_control_bytes(to, buf, n, now);
}

/* Queued actions, and how many of them are SEND_CONTROL; all dropped. */
static size_t drain_actions(moq_session_t *s, size_t *send_control, uint8_t *first_type)
{
    size_t n = 0;
    if (send_control) *send_control = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        n++;
        if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            if (send_control) {
                if (*send_control == 0 && first_type && a.u.send_control.len > 0)
                    *first_type = a.u.send_control.data[0];
                (*send_control)++;
            }
        }
        moq_action_cleanup(&a);
    }
    return n;
}

/* Queued events, and how many are SETUP_COMPLETE; all dropped. */
static size_t drain_events(moq_session_t *s, size_t *setup_complete)
{
    size_t n = 0;
    if (setup_complete) *setup_complete = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        n++;
        if (setup_complete && ev.kind == MOQ_EVENT_SETUP_COMPLETE) (*setup_complete)++;
        moq_event_cleanup(&ev);
    }
    return n;
}

/* The bytes of one CLIENT_SETUP from a fresh draft-16 client. */
static size_t d16_client_setup(uint8_t *out, size_t cap)
{
    moq_session_t *c = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                       .perspective = MOQ_PERSPECTIVE_CLIENT });
    if (!c) return 0;
    size_t n = 0;
    if (moq_session_start(c, 0) == MOQ_OK) n = take_control(c, out, cap);
    moq_session_destroy(c);
    return n;
}

/* A setup token larger than a 64-byte send buffer, though the SETUP that
 * carries it encodes. */
static const uint8_t g_long_value[96] = { 0x61 };
static const moq_auth_token_t g_long_token = {
    .token_type = 1,
    .token_value = { g_long_value, sizeof(g_long_value) },
};

/* -- Rows ------------------------------------------------------------------ */

static int row_server_setup_never_fits(void)
{
    failures = 0;
    uint8_t setup[512];
    size_t n = d16_client_setup(setup, sizeof(setup));
    MOQ_TEST_CHECK(n > 0);

    fail_alloc_t fa = { .balance = 0 };
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER,
                                        .send_buffer_size = 64,
                                        .tokens = &g_long_token, .token_count = 1,
                                        .alloc = &fa });
    MOQ_TEST_CHECK(sv != NULL);
    if (!sv || n == 0) return failures;
    const moq_session_state_t before = moq_session_state(sv);

    moq_result_t rc = moq_session_on_control_bytes(sv, setup, n, 1);
    /* BUFFER would say nothing was copied. */
    MOQ_TEST_CHECK(!(rc == MOQ_ERR_BUFFER && sv->recv_len > 0));
    MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_LIMIT);
    /* The CLIENT_SETUP is buffered, once, byte for byte. */
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, n);
    MOQ_TEST_CHECK(sv->recv_len == n && memcmp(sv->recv_buf, setup, n) == 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)before);
    MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, NULL), 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->send_len, 0);

    /* Every retry meets the same limit and changes nothing. */
    for (uint64_t i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 2 + i), MOQ_ERR_LIMIT);
        MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, n);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)before);
        MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, NULL), 0);
    }
    moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_INT((int)fa.balance, 0);
    MOQ_TEST_PASS("a draft-16 SERVER_SETUP larger than the send buffer is LIMIT, input kept once");
    return failures;
}

static int row_server_setup_without_memory(void)
{
    failures = 0;
    uint8_t setup[512];
    size_t n = d16_client_setup(setup, sizeof(setup));
    MOQ_TEST_CHECK(n > 0);

    fail_alloc_t fa = { .balance = 0 };
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER,
                                        .tokens = &g_long_token, .token_count = 1,
                                        .alloc = &fa });
    MOQ_TEST_CHECK(sv != NULL);
    if (!sv || n == 0) return failures;
    const moq_session_state_t before = moq_session_state(sv);

    fa.fail_in = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(sv, setup, n, 1), MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_INT((int)fa.fail_in, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, n);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)before);
    MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_SIZE(drain_events(sv, NULL), 0);

    /* Memory back: the same buffered CLIENT_SETUP is handled, once. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 2), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    size_t sends = 0, complete = 0;
    uint8_t type = 0;
    drain_actions(sv, &sends, &type);
    MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
    MOQ_TEST_CHECK_EQ_INT((int)type, (int)MOQ_D16_SERVER_SETUP);
    drain_events(sv, &complete);
    MOQ_TEST_CHECK_EQ_SIZE(complete, 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 3), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL), 0);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK_EQ_INT((int)fa.balance, 0);
    MOQ_TEST_PASS("an allocation failure while handling CLIENT_SETUP is NOMEM, then one SERVER_SETUP");
    return failures;
}

static void subscribe_cfg(moq_subscribe_cfg_t *sc)
{
    static moq_bytes_t ns_part[] = { { (const uint8_t *)"live", 4 } };
    moq_subscribe_cfg_init(sc);
    sc->track_namespace.parts = ns_part;
    sc->track_namespace.count = 1;
    sc->track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
    sc->filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
}

/* An established draft-16 pair; the client's SUBSCRIBE bytes after the
 * server sent GOAWAY, which the server refuses with REQUEST_ERROR. */
static size_t refused_subscribe(moq_session_t *c, moq_session_t *sv, uint8_t *out, size_t cap)
{
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pass_control(c, sv, 1), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)pass_control(sv, c, 1), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
    drain_events(c, NULL);
    drain_events(sv, NULL);

    moq_subscribe_cfg_t sc;
    subscribe_cfg(&sc);
    moq_subscription_t sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &sc, 2, &sub), MOQ_OK);
    return take_control(c, out, cap);
}

static int row_refusal_waits_for_send_room(void)
{
    failures = 0;
    moq_session_t *c = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                       .perspective = MOQ_PERSPECTIVE_CLIENT });
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER,
                                        .send_buffer_size = 64 });
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;
    uint8_t sub[256];
    size_t n = refused_subscribe(c, sv, sub, sizeof(sub));
    MOQ_TEST_CHECK(n > 0);

    /* A GOAWAY fills most of the server's send buffer, unpolled. */
    uint8_t uri[56];
    memset(uri, 'u', sizeof(uri));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(sv, uri, sizeof(uri), 3), MOQ_OK);
    MOQ_TEST_CHECK(sv->send_len > 0);

    moq_result_t rc = moq_session_on_control_bytes(sv, sub, n, 4);
    MOQ_TEST_CHECK(!(rc == MOQ_ERR_BUFFER && sv->recv_len > 0));
    MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, n);
    size_t sends = 0;
    uint8_t type = 0;
    /* Draining releases the GOAWAY's room; the refusal follows, once. */
    drain_actions(sv, &sends, &type);
    MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
    MOQ_TEST_CHECK_EQ_INT((int)type, (int)MOQ_D16_GOAWAY);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, 0);
    drain_actions(sv, &sends, &type);
    MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
    MOQ_TEST_CHECK_EQ_INT((int)type, (int)MOQ_D16_REQUEST_ERROR);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 6), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain_actions(sv, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_DRAINING);
    moq_session_destroy(sv);
    moq_session_destroy(c);
    MOQ_TEST_PASS("a refusal behind earlier output is WOULD_BLOCK, then sent once after a drain");
    return failures;
}

static int row_refusal_waits_for_action_room(void)
{
    failures = 0;
    moq_session_t *c = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                       .perspective = MOQ_PERSPECTIVE_CLIENT });
    moq_session_t *sv = make(&(opts_t){ .version = MOQ_VERSION_DRAFT_16,
                                        .perspective = MOQ_PERSPECTIVE_SERVER,
                                        .max_actions = 2 });
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) return failures;
    uint8_t sub[256];
    size_t n = refused_subscribe(c, sv, sub, sizeof(sub));
    MOQ_TEST_CHECK(n > 0);

    /* Two unpolled actions fill the server's action queue. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(sv, NULL, 0, 3), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_grant_request_capacity(sv, 128, 3), MOQ_OK);

    moq_result_t rc = moq_session_on_control_bytes(sv, sub, n, 4);
    MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, n);
    size_t sends = 0;
    drain_actions(sv, &sends, NULL);
    MOQ_TEST_CHECK_EQ_SIZE(sends, 2);
    uint8_t type = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 5), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, 0);
    drain_actions(sv, &sends, &type);
    MOQ_TEST_CHECK_EQ_SIZE(sends, 1);
    MOQ_TEST_CHECK_EQ_INT((int)type, (int)MOQ_D16_REQUEST_ERROR);
    moq_session_destroy(sv);
    moq_session_destroy(c);
    MOQ_TEST_PASS("a refusal behind a full action queue is WOULD_BLOCK, then sent once");
    return failures;
}

/* Drafts 18 and 22. The server receives the client's SUBSCRIBE before the
 * client's SETUP, so handling the SETUP dispatches that request after the
 * SETUP is committed. An allocation refused there fails the call (an
 * admitted result, never BUFFER) and leaves the SETUP consumed: retrying
 * does not handle it a second time, which would close the session as if the
 * peer had sent two. The row tries each allocation in turn until one fails
 * after the commit; finding none fails the row. */
static int row_committed_setup_stays_consumed(void)
{
    failures = 0;
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        const moq_version_t v = versions[i];
        const int before = failures;
        bool found = false;
        for (unsigned k = 1; k <= 256 && !found; k++) {
            fail_alloc_t fa = { .balance = 0 };
            moq_session_t *c = make(&(opts_t){ .version = v,
                                               .perspective = MOQ_PERSPECTIVE_CLIENT });
            moq_session_t *sv = make(&(opts_t){ .version = v,
                                                .perspective = MOQ_PERSPECTIVE_SERVER,
                                                .alloc = &fa });
            if (!c || !sv) {
                MOQ_TEST_CHECK(c != NULL && sv != NULL);
                if (c) moq_session_destroy(c);
                if (sv) moq_session_destroy(sv);
                break;
            }
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c, 0), MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv, 0), MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)pass_control(sv, c, 1), MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
            drain_events(c, NULL);
            moq_subscribe_cfg_t sc;
            subscribe_cfg(&sc);
            moq_subscription_t sub;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &sc, 2, &sub), MOQ_OK);
            /* The client's SETUP is held; its SUBSCRIBE reaches the server
             * first. */
            uint8_t setup[512];
            size_t n = 0;
            moq_action_t a;
            while (moq_session_poll_actions(c, &a, 1) > 0) {
                if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL && a.u.open_uni_control.len <= sizeof(setup) - n) {
                    memcpy(setup + n, a.u.open_uni_control.data, a.u.open_uni_control.len);
                    n += a.u.open_uni_control.len;
                } else if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(sv,
                        a.u.open_bidi_stream.stream_ref, a.u.open_bidi_stream.data,
                        a.u.open_bidi_stream.len, a.u.open_bidi_stream.fin, 3), MOQ_OK);
                }
                moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK(n > 0);
            MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_ESTABLISHED);

            fa.fail_in = k;
            moq_result_t rc = moq_session_on_control_bytes(sv, setup, n, 4);
            const bool committed = moq_session_state(sv) == MOQ_SESS_ESTABLISHED;
            fa.fail_in = 0;
            if (rc < 0 && committed) {
                found = true;
                printf("  %s: allocation %u from the SETUP on failed after its commit: %d\n",
                       v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-22", k, (int)rc);
                MOQ_TEST_CHECK(rc != MOQ_ERR_BUFFER);
                MOQ_TEST_CHECK_EQ_SIZE(sv->recv_len, 0);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_process_pending(sv, 5), MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), (int)MOQ_SESS_ESTABLISHED);
                size_t complete = 0;
                drain_events(sv, &complete);
                MOQ_TEST_CHECK_EQ_SIZE(complete, 1);
            }
            moq_session_destroy(sv);
            moq_session_destroy(c);
            MOQ_TEST_CHECK_EQ_INT((int)fa.balance, 0);
        }
        MOQ_TEST_CHECK(found);
        if (failures != before)
            fprintf(stderr, "  in row committed_setup_stays_consumed (%s)\n",
                    v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-22");
    }
    MOQ_TEST_PASS("a SETUP committed before a later failure stays consumed");
    return failures;
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? "draft-16"
         : v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-22";
}

static int row_delivery_larger_than_room(void)
{
    failures = 0;
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                       MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        const moq_version_t v = versions[i];
        const int before = failures;
        /* The receiving side: the draft-16 server, or a started draft-18/22
         * client; its peer supplies the SETUP it then takes. */
        const bool d16 = v == MOQ_VERSION_DRAFT_16;
        moq_session_t *rx = make(&(opts_t){ .version = v,
            .perspective = d16 ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT,
            .recv_buffer_size = 128 });
        moq_session_t *peer = make(&(opts_t){ .version = v,
            .perspective = d16 ? MOQ_PERSPECTIVE_CLIENT : MOQ_PERSPECTIVE_SERVER });
        MOQ_TEST_CHECK(rx != NULL && peer != NULL);
        if (!rx || !peer) continue;
        if (!d16) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(rx, 0), MOQ_OK);
            drain_actions(rx, NULL, NULL);
        }
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(peer, 0), MOQ_OK);
        uint8_t setup[512];
        size_t n = take_control(peer, setup, sizeof(setup));
        MOQ_TEST_CHECK(n > 0 && n <= 128);

        uint8_t big[200];
        memset(big, 0, sizeof(big));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(rx, big, sizeof(big), 1),
                              MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK_EQ_SIZE(rx->recv_len, 0);
        MOQ_TEST_CHECK_EQ_SIZE(drain_events(rx, NULL), 0);

        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(rx, setup, n, 2), MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(rx->recv_len, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(rx), (int)MOQ_SESS_ESTABLISHED);
        size_t complete = 0;
        drain_events(rx, &complete);
        MOQ_TEST_CHECK_EQ_SIZE(complete, 1);
        if (failures != before) fprintf(stderr, "  in row delivery_larger_than_room (%s)\n", vname(v));
        moq_session_destroy(peer);
        moq_session_destroy(rx);
    }
    MOQ_TEST_PASS("a delivery larger than the control buffer's room is BUFFER, nothing copied");
    return failures;
}

int main(void)
{
    int total = 0;
    total += row_server_setup_never_fits();
    total += row_server_setup_without_memory();
    total += row_refusal_waits_for_send_room();
    total += row_refusal_waits_for_action_room();
    total += row_committed_setup_stays_consumed();
    total += row_delivery_larger_than_room();
    if (total) fprintf(stderr, "test_control_input_custody: %d failure(s)\n", total);
    return total ? 1 : 0;
}
