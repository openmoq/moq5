/*
 * Reliable resets over real MsQuic on localhost (managed facade), observed
 * from the receiving application.
 *
 * The publisher opens a data stream and resets it in the same pump, so the
 * stream header has barely been handed to MsQuic when the reset comes. Both
 * managed sides enable ReliableResetEnabled; once MsQuic reports
 * RELIABLE_RESET_NEGOTIATED the adapter resets with RESET_STREAM_AT keeping
 * the header, and the subscriber can still identify the stream:
 *
 *   done_count (draft-16, draft-22): subgroup opened, written, reset, then
 *     PUBLISH_DONE counting it -- the subscriber surfaces the done promptly
 *     (well inside its 30 s done-wait) only with the header;
 *   aborted_fill (draft-22): the publisher aborts a fill the moment it is
 *     requested (FETCH_HEADER then reset, §3.4.1) -- the subscriber ends that
 *     fill only with the header.
 *
 * done_count follows moq_session_done_subscribe's contract: called in the
 * same pump as the reset, it is refused with MOQ_ERR_WOULD_BLOCK (the
 * subgroup's stream-opening decision is still owed to the transport) and
 * the refusal changes nothing. The pump then rings its own lane; the sweep
 * that ran the pump services the connection, which settles the open, and
 * the next pump submits the DONE once, after checking that the session
 * counts exactly one opened stream. The subscriber must see exactly one
 * PUBLISH_DONE, carrying that count.
 *
 * Each scenario runs RUNS fresh connections; every run must deliver, and
 * neither side may go fatal. The publisher's reset is issued from whichever
 * thread runs its pump (a worker after a transport batch, or the lane
 * doorbell), so both of the adapter's reliable-reset paths are reachable.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>

#include "../../../core/src/session/session_internal.h"

#include "msq_loopback_port.h"

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define RUNS 5

enum scenario { DONE_COUNT, ABORTED_FILL };

struct side {
    pthread_mutex_t mu;
    enum scenario sc;
    int setup;
    bool subscribed;
    bool published;
    bool delivered;          /* client: the reset stream was identified */
    bool close_sent;
    int errors;
    moq_subscription_t sub;
    bool have_sub;
    int done_events;         /* client: PUBLISH_DONEs observed */
    /* server, done_count: the refused DONE and what it left untouched */
    bool done_owed;
    int done_calls;
    size_t owed_head, owed_tail, owed_send_len;
    uint64_t owed_service_passes;
    uint64_t settle_service_passes; /* service passes before the DONE */
};

/* Publisher-side entry of an accepted subscription (white-box). */
static moq_sub_entry_t *sub_entry(moq_session_t *s, moq_subscription_t sub)
{
    for (size_t i = 0; i < s->sub_cap; i++)
        if (moq_subscription_eq(s->subs[i].handle, sub))
            return &s->subs[i];
    return NULL;
}

static bool lane_service_passes(moq_msquic_managed_lane_t *lane,
                                uint64_t *out)
{
    moq_msquic_lane_stats_t st;
    memset(&st, 0, sizeof(st));
    st.struct_size = (uint32_t)sizeof(st);
    if (moq_msquic_lane_get_stats(lane, &st, sizeof(st)) != MOQ_OK)
        return false;
    *out = st.service_passes;
    return true;
}

static const moq_bytes_t ns_part = { (const uint8_t *)"msq", 3 };

static int client_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                       uint64_t now, void *ctx)
{
    struct side *sd = ctx;
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;
    (void)lane;
    pthread_mutex_lock(&sd->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) sd->setup++;
        if (sd->sc == DONE_COUNT && ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            sd->done_events++;
            if (ev.u.subscribe_done.stream_count != 1 ||
                ev.u.subscribe_done.status_code != 0x2)
                sd->errors++;
            sd->delivered = true;
        }
        if (sd->sc == ABORTED_FILL && ev.kind == MOQ_EVENT_FILL_TERMINAL &&
            ev.u.fill_terminal.reason == MOQ_FILL_PEER_CANCELLED_OR_RESET)
            sd->delivered = true;
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->setup > 0 && !sd->subscribed) {
        moq_subscribe_cfg_t sc;
        moq_location_filter_cfg_t loc;
        moq_fill_cfg_t fill;
        moq_subscription_t sub;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)&ns_part;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        if (sd->sc == ABORTED_FILL) {
            moq_location_filter_cfg_init(&loc);
            loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
            loc.start_group = 3;
            moq_fill_cfg_init(&fill);
            fill.location = &loc;
            sc.fill = &fill;
        }
        sd->subscribed = true;
        if (moq_session_subscribe(s, &sc, now, &sub) < 0) sd->errors++;
    }
    if (s != NULL && sd->delivered && !sd->close_sent) {
        sd->close_sent = true;
        (void)moq_session_close(s, 0, NULL, now);
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

static int server_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                       uint64_t now, void *ctx)
{
    struct side *sd = ctx;
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c) : NULL;
    moq_event_t ev;
    (void)m;
    pthread_mutex_lock(&sd->mu);
    if (s != NULL && sd->done_owed) {
        /* The sweep that ran the refusing pump serviced this connection
         * after it; the open must have settled to exactly one stream. */
        moq_sub_entry_t *e = sub_entry(s, sd->sub);
        moq_done_subscribe_cfg_t dc;
        uint64_t passes = 0;
        sd->done_owed = false;
        if (!lane_service_passes(lane, &passes) ||
            passes - sd->owed_service_passes < 1)
            sd->errors++;
        sd->settle_service_passes = passes - sd->owed_service_passes;
        if (e == NULL || s->pending_open_count != 0 ||
            e->opened.subgroup != 1 || e->opened.fill != 0 ||
            e->opened.subgroup_external || e->opened.fill_external) {
            sd->errors++;
        } else {
            moq_done_subscribe_cfg_init(&dc);
            dc.status_code = 0x2;
            dc.stream_count = 1;
            sd->done_calls++;
            if (moq_session_done_subscribe(s, sd->sub, &dc, now) != MOQ_OK)
                sd->errors++;
        }
    }
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sd->sub = ev.u.subscribe_request.sub;
            sd->have_sub = true;
        }
        if (ev.kind == MOQ_EVENT_FILL_REQUEST &&
            moq_session_abort_fill(s, ev.u.fill_request.fill, 0x7, now) < 0)
            sd->errors++;
        moq_event_cleanup(&ev);
    }
    if (s != NULL && sd->have_sub && !sd->published) {
        sd->published = true;
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
        if (moq_session_accept_subscribe(s, sd->sub, &ac, now) < 0) sd->errors++;
        if (sd->sc == DONE_COUNT) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;
            moq_rcbuf_t *b = NULL;
            moq_done_subscribe_cfg_t dc;
            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 6;
            sgc.publisher_priority = 128;
            moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"object", 6, &b);
            moq_done_subscribe_cfg_init(&dc);
            dc.status_code = 0x2;
            dc.stream_count = 1;
            if (moq_session_open_subgroup(s, sd->sub, &sgc, now, &sg) < 0 ||
                moq_session_write_object(s, sg, 0, b, now) < 0 ||
                moq_session_reset_subgroup(s, sg, 0x1, now) < 0) {
                sd->errors++;
            } else {
                /* The stream's opening decision is still owed: the DONE is
                 * refused and nothing moves. */
                moq_sub_entry_t *e = sub_entry(s, sd->sub);
                const size_t head = s->action_head, tail = s->action_tail;
                const size_t send_len = s->send_len;
                if (e == NULL || s->pending_open_count != 1 ||
                    e->opened.subgroup != 0)
                    sd->errors++;
                sd->done_calls++;
                if (moq_session_done_subscribe(s, sd->sub, &dc, now) !=
                        MOQ_ERR_WOULD_BLOCK ||
                    e == NULL || s->action_head != head ||
                    s->action_tail != tail || s->send_len != send_len ||
                    s->pending_open_count != 1 ||
                    e->state != MOQ_SUB_ESTABLISHED ||
                    e->opened.subgroup != 0 ||
                    !lane_service_passes(lane, &sd->owed_service_passes) ||
                    moq_msquic_lane_wake(lane) != MOQ_OK)
                    sd->errors++;
                else
                    sd->done_owed = true;
            }
            moq_rcbuf_decref(b);
        }
    }
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

static bool wait_delivered(moq_msquic_managed_t *m, struct side *sd, int max_ms)
{
    for (int i = 0; i < max_ms / 10; i++) {
        pthread_mutex_lock(&sd->mu);
        bool d = sd->delivered;
        pthread_mutex_unlock(&sd->mu);
        if (d) return true;
        if (moq_msquic_managed_is_fatal(m)) return false;
        (void)moq_msquic_managed_wait(m, 10 * 1000);
    }
    return false;
}

static const QUIC_API_TABLE *msq_api; /* the process's library reference */
/* Most lane service passes seen between a refused DONE and its submission,
 * per scenario (reported, not a limit: deadline service may add passes). */
static uint64_t max_settle_passes;

static QUIC_STATUS QUIC_API refuse_listener_event(HQUIC listener, void *ctx,
                                                  QUIC_LISTENER_EVENT *ev)
{
    (void)listener; (void)ctx; (void)ev;
    return QUIC_STATUS_NOT_SUPPORTED;
}

/*
 * The managed create reports only a moq_result_t. When it fails, replay the
 * server's MsQuic sequence (registration, configuration, credential from the
 * same files, listener on the same host) directly and print each call's
 * QUIC_STATUS, stopping at the first failure. It runs right after the
 * failure, in the same process, but it is a second attempt: it names the
 * failing call only if the cause persists.
 */
static void report_server_create_failure(moq_result_t rc, const char *cert,
                                         const char *key, const char *host)
{
    HQUIC reg = NULL, config = NULL, listener = NULL;
    QUIC_STATUS st;
    fprintf(stderr, "server create: moq_msquic_managed_create -> %d (%s)\n",
            (int)rc, moq_strerror(rc));
    if (msq_api == NULL)
        return;
    const QUIC_REGISTRATION_CONFIG rcfg = {
        .AppName = "moq-msquic-rr-replay",
        .ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY,
    };
    st = msq_api->RegistrationOpen(&rcfg, &reg);
    fprintf(stderr, "server create replay: RegistrationOpen -> 0x%lx\n",
            (unsigned long)st);
    if (QUIC_FAILED(st))
        return;
    const QUIC_BUFFER alpn = {
        .Length = 6, .Buffer = (uint8_t *)(uintptr_t)"moq-00" };
    QUIC_SETTINGS settings;
    moq_msquic_settings_init(&settings);
    st = msq_api->ConfigurationOpen(reg, &alpn, 1, &settings,
                                    (uint32_t)sizeof(settings), NULL, &config);
    fprintf(stderr, "server create replay: ConfigurationOpen -> 0x%lx\n",
            (unsigned long)st);
    if (QUIC_SUCCEEDED(st)) {
        QUIC_CERTIFICATE_FILE files = {
            .PrivateKeyFile = key, .CertificateFile = cert };
        QUIC_CREDENTIAL_CONFIG cred;
        memset(&cred, 0, sizeof(cred));
        cred.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        cred.CertificateFile = &files;
        st = msq_api->ConfigurationLoadCredential(config, &cred);
        fprintf(stderr,
                "server create replay: ConfigurationLoadCredential -> 0x%lx\n",
                (unsigned long)st);
    }
    if (QUIC_SUCCEEDED(st)) {
        st = msq_api->ListenerOpen(reg, refuse_listener_event, NULL,
                                   &listener);
        fprintf(stderr, "server create replay: ListenerOpen -> 0x%lx\n",
                (unsigned long)st);
    }
    if (QUIC_SUCCEEDED(st)) {
        QUIC_ADDR addr;
        memset(&addr, 0, sizeof(addr));
        QuicAddrSetFamily(&addr, QUIC_ADDRESS_FAMILY_UNSPEC);
        if (!QuicAddrFromString(host, 0, &addr)) {
            fprintf(stderr, "server create replay: bad host %s\n", host);
        } else {
            st = msq_api->ListenerStart(listener, &alpn, 1, &addr);
            fprintf(stderr, "server create replay: ListenerStart -> 0x%lx\n",
                    (unsigned long)st);
            if (QUIC_SUCCEEDED(st))
                msq_api->ListenerStop(listener);
        }
    }
    if (listener != NULL)
        msq_api->ListenerClose(listener);
    if (config != NULL)
        msq_api->ConfigurationClose(config);
    msq_api->RegistrationClose(reg);
}

/* One fresh connection; returns 1 when the subscriber identified the reset
 * stream within two seconds. */
static int run_once(const char *cert, const char *key, moq_version_t v, enum scenario sc)
{
    struct side sv, cl;
    memset(&sv, 0, sizeof(sv));
    memset(&cl, 0, sizeof(cl));
    pthread_mutex_init(&sv.mu, NULL);
    pthread_mutex_init(&cl.mu, NULL);
    sv.sc = cl.sc = sc;

    moq_msquic_managed_t *srv = NULL, *cli = NULL;
    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.cert_path = cert;
    scfg.key_path = key;
    scfg.on_lane_pump = server_pump;
    scfg.on_lane_pump_user = &sv;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.version = v;
    moq_result_t src = msqt_server_create(&scfg, &srv);
    if (src != MOQ_OK)
        report_server_create_failure(src, cert, key, scfg.host);
    CHECK(src == MOQ_OK);
    int got = 0;
    const bool srv_made = srv != NULL;
    if (srv != NULL) {
        moq_msquic_managed_cfg_t ccfg;
        moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
        ccfg.alloc = moq_alloc_default();
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.host = "127.0.0.1";
        ccfg.port = moq_msquic_managed_port(srv);
        ccfg.insecure_skip_verify = true;
        ccfg.on_lane_pump = client_pump;
        ccfg.on_lane_pump_user = &cl;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 16;
        ccfg.version = v;
        CHECK(moq_msquic_managed_create(&ccfg, &cli) == MOQ_OK);
        if (cli != NULL) {
            got = wait_delivered(cli, &cl, 2000) ? 1 : 0;
            CHECK(!moq_msquic_managed_is_fatal(cli));
            CHECK(!moq_msquic_managed_is_fatal(srv));
            CHECK(moq_msquic_managed_stop(cli) == MOQ_OK);
            moq_msquic_managed_destroy(cli);
        }
        CHECK(moq_msquic_managed_stop(srv) == MOQ_OK);
        moq_msquic_managed_destroy(srv);
    }
    CHECK(sv.errors == 0);
    CHECK(cl.errors == 0);
    if (sc == DONE_COUNT && srv_made) {
        CHECK(sv.done_calls == 2);
        CHECK(cl.done_events == 1);
        CHECK(sv.settle_service_passes >= 1);
        if (sv.settle_service_passes > max_settle_passes)
            max_settle_passes = sv.settle_service_passes;
    }
    pthread_mutex_destroy(&sv.mu);
    pthread_mutex_destroy(&cl.mu);
    return got;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    /* One library reference for the whole process (see msquic_loopback). */
    if (QUIC_FAILED(MsQuicOpen2(&msq_api))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }
    static const struct {
        const char *name; moq_version_t v; enum scenario sc;
    } runs[] = {
        { "done_count_d16", MOQ_VERSION_DRAFT_16, DONE_COUNT },
        { "done_count_d22", MOQ_VERSION_DRAFT_22, DONE_COUNT },
        { "aborted_fill_d22", MOQ_VERSION_DRAFT_22, ABORTED_FILL },
    };
    for (size_t i = 0; i < sizeof(runs) / sizeof(runs[0]); i++) {
        int got = 0;
        max_settle_passes = 0;
        for (int r = 0; r < RUNS; r++)
            got += run_once(argv[1], argv[2], runs[i].v, runs[i].sc);
        printf("msquic %s: header delivered in %d/%d runs\n", runs[i].name, got, RUNS);
        if (runs[i].sc == DONE_COUNT)
            printf("msquic %s: DONE settled after at most %llu service passes\n",
                   runs[i].name, (unsigned long long)max_settle_passes);
        CHECK(got == RUNS);
    }
    if (failures == 0)
        printf("PASS: msquic_reliable_reset\n");
    return failures != 0;
}
