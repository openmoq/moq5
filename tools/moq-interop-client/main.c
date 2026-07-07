/*
 * moq-interop-client — MoQ interop test runner.
 *
 * Connects to a relay via the libmoq service tier (moq_endpoint_t) and runs
 * named test cases. Outputs TAP on stdout; logs on stderr.
 *
 * The endpoint tier owns URL parsing, transport selection (raw QUIC for
 * moqt://, H3 WebTransport for https://), version negotiation (offers every
 * draft this build supports and reads back the negotiated one), TLS/SNI, and
 * the network thread. The test cases drive the sans-I/O core directly via the
 * endpoint's post() executor, which runs moq_session_* calls on the network
 * thread where they are legal. This is the protocol/conformance dogfood of the
 * service tier (design slice 4); application integrators should start with
 * examples/service/ instead.
 */

#include <moq/endpoint.h>
#include <moq/session.h>

#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Monotonic clock                                                     */
/* ------------------------------------------------------------------ */

static uint64_t now_us_mono(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ------------------------------------------------------------------ */
/* Test modes                                                          */
/* ------------------------------------------------------------------ */

typedef enum {
    TEST_SETUP_ONLY,
    TEST_ANNOUNCE_ONLY,
    TEST_PUBLISH_NAMESPACE_DONE,
    TEST_SUBSCRIBE_ERROR,
} test_mode_t;

/* ------------------------------------------------------------------ */
/* Shared test state (mutex-protected)                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    pthread_mutex_t     mu;
    bool                verbose;
    test_mode_t         mode;

    /* State flags (written by pump task on the net thread, read by app). */
    bool                setup_complete;
    bool                session_closed;

    /* announce-only / publish-namespace-done */
    bool                req_publish_ns;
    bool                publish_ns_sent;
    bool                publish_ns_failed;
    int                 publish_ns_rc;
    bool                ns_accepted;
    bool                ns_rejected;
    moq_announcement_t  ann;

    bool                req_publish_ns_done;
    bool                publish_ns_done_sent;
    bool                publish_ns_done_failed;
    int                 publish_ns_done_rc;

    /* subscribe-error / dual-session subscriber side */
    bool                req_subscribe;
    bool                subscribe_sent;
    bool                subscribe_failed;
    int                 subscribe_rc;
    bool                subscribe_ok;
    bool                subscribe_error;
    moq_subscription_t  sub;
    moq_bytes_t        *subscribe_ns;
    size_t              subscribe_ns_count;

    /* dual-session publisher side: accept incoming subscribe */
    bool                req_accept_sub;
    bool                subscribe_request_received;
    moq_subscription_t  incoming_sub;
    bool                accept_sub_sent;
    bool                accept_sub_failed;
    int                 accept_sub_rc;

    int                 last_state;
    const char         *log_prefix;
} test_ctx_t;

static void ctx_init(test_ctx_t *c, test_mode_t mode, bool verbose,
                      const char *prefix)
{
    memset(c, 0, sizeof(*c));
    pthread_mutex_init(&c->mu, NULL);
    c->verbose = verbose;
    c->mode = mode;
    c->log_prefix = prefix ? prefix : "";
}

static void ctx_destroy(test_ctx_t *c)
{
    pthread_mutex_destroy(&c->mu);
}

/* ------------------------------------------------------------------ */
/* Shared constants                                                    */
/* ------------------------------------------------------------------ */

static moq_bytes_t ns_interop_parts[] = {
    { (const uint8_t *)"moq-test", 8 },
    { (const uint8_t *)"interop", 7 },
    { NULL, 0 },   /* optional per-case label; see set_case_ns() */
};
static size_t ns_interop_count = 2;

/* Run-all runs every case in one process against one relay. If they all shared
 * the "moq-test/interop" namespace, a case that announces it and then tears the
 * session down (currently without a graceful CONNECTION_CLOSE) leaves the relay
 * holding that namespace until its idle timeout, so the NEXT announce-case
 * collides ("namespace rejected"). Giving each case a distinct 3rd label keeps
 * them independent regardless of teardown timing. NULL/empty -> 2-part (the
 * single-case default, unchanged). */
static void set_case_ns(const char *tag)
{
    if (tag && *tag) {
        ns_interop_parts[2].data = (const uint8_t *)tag;
        ns_interop_parts[2].len  = strlen(tag);
        ns_interop_count = 3;
    } else {
        ns_interop_parts[2].data = NULL;
        ns_interop_parts[2].len  = 0;
        ns_interop_count = 2;
    }
}

static moq_bytes_t ns_nonexistent_parts[] = {
    { (const uint8_t *)"nonexistent", 11 },
    { (const uint8_t *)"namespace", 9 },
};

/* ------------------------------------------------------------------ */
/* Pump task — posted onto the endpoint's network thread each cycle.    */
/*                                                                      */
/* Runs EXACTLY ONCE per moq_endpoint_post() call (with session, or     */
/* session == NULL on terminal drain). It performs any pending session  */
/* calls and drains queued session events into the shared ctx. The app  */
/* thread re-posts it each wait cycle to keep events flowing — the       */
/* endpoint's own pump does not consume session events.                 */
/* ------------------------------------------------------------------ */

static moq_result_t interop_pump_task(moq_endpoint_t *ep, moq_session_t *sess,
                                      uint64_t now, void *ctx)
{
    (void)ep;
    test_ctx_t *c = (test_ctx_t *)ctx;
    if (!sess) {
        /* Terminal drain marker: session gone. */
        pthread_mutex_lock(&c->mu);
        c->session_closed = true;
        pthread_mutex_unlock(&c->mu);
        return MOQ_OK;
    }

    /* Track session state -> setup_complete. */
    moq_session_state_t state = moq_session_state(sess);
    pthread_mutex_lock(&c->mu);
    if ((int)state != c->last_state) {
        if (c->verbose)
            fprintf(stderr, "  [%s]  session state %d \xe2\x86\x92 %d\n",
                c->log_prefix, c->last_state, (int)state);
        c->last_state = (int)state;
    }
    if (state == MOQ_SESS_ESTABLISHED && !c->setup_complete)
        c->setup_complete = true;

    /* Snapshot requested actions. */
    bool do_publish_ns = c->req_publish_ns && !c->publish_ns_sent;
    bool do_publish_ns_done = c->req_publish_ns_done && !c->publish_ns_done_sent;
    bool do_subscribe = c->req_subscribe && !c->subscribe_sent;
    bool do_accept_sub = c->req_accept_sub && c->subscribe_request_received
                         && !c->accept_sub_sent;
    moq_announcement_t ann_copy = c->ann;
    moq_subscription_t incoming_sub_copy = c->incoming_sub;
    pthread_mutex_unlock(&c->mu);

    if (do_publish_ns) {
        moq_publish_namespace_cfg_t ncfg;
        moq_publish_namespace_cfg_init(&ncfg);
        ncfg.track_namespace.parts = ns_interop_parts;
        ncfg.track_namespace.count = ns_interop_count;
        moq_announcement_t ann;
        moq_result_t rc = moq_session_publish_namespace(sess, &ncfg, now, &ann);
        pthread_mutex_lock(&c->mu);
        if (rc == MOQ_OK) {
            c->publish_ns_sent = true;
            c->ann = ann;
            if (c->verbose)
                fprintf(stderr, "  [%s] publish_namespace sent\n", c->log_prefix);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* Leave the request flag set; the next posted cycle retries. */
        } else {
            c->publish_ns_sent = true;
            c->publish_ns_failed = true;
            c->publish_ns_rc = (int)rc;
            if (c->verbose)
                fprintf(stderr, "  [%s] publish_namespace failed: %d\n", c->log_prefix, rc);
        }
        pthread_mutex_unlock(&c->mu);
    }

    if (do_publish_ns_done) {
        moq_result_t rc = moq_session_publish_namespace_done(sess, ann_copy, now);
        pthread_mutex_lock(&c->mu);
        if (rc == MOQ_OK) {
            c->publish_ns_done_sent = true;
            if (c->verbose)
                fprintf(stderr, "  [%s] publish_namespace_done sent\n", c->log_prefix);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* retry next cycle */
        } else {
            c->publish_ns_done_sent = true;
            c->publish_ns_done_failed = true;
            c->publish_ns_done_rc = (int)rc;
            if (c->verbose)
                fprintf(stderr, "  [%s] publish_namespace_done failed: %d\n", c->log_prefix, rc);
        }
        pthread_mutex_unlock(&c->mu);
    }

    if (do_subscribe) {
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = c->subscribe_ns
            ? c->subscribe_ns : ns_nonexistent_parts;
        scfg.track_namespace.count = c->subscribe_ns
            ? c->subscribe_ns_count : 2;
        scfg.track_name = (moq_bytes_t){ (const uint8_t *)"test-track", 10 };
        scfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(sess, &scfg, now, &sub);
        pthread_mutex_lock(&c->mu);
        if (rc == MOQ_OK) {
            c->subscribe_sent = true;
            c->sub = sub;
            if (c->verbose)
                fprintf(stderr, "  [%s] subscribe sent\n", c->log_prefix);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* retry next cycle */
        } else {
            c->subscribe_sent = true;
            c->subscribe_failed = true;
            c->subscribe_rc = (int)rc;
            if (c->verbose)
                fprintf(stderr, "  [%s] subscribe failed: %d\n", c->log_prefix, rc);
        }
        pthread_mutex_unlock(&c->mu);
    }

    if (do_accept_sub) {
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_result_t rc = moq_session_accept_subscribe(
            sess, incoming_sub_copy, &acfg, now);
        pthread_mutex_lock(&c->mu);
        if (rc == MOQ_OK) {
            c->accept_sub_sent = true;
            if (c->verbose)
                fprintf(stderr, "  [%s] accept_subscribe sent\n", c->log_prefix);
        } else if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* retry next cycle */
        } else {
            c->accept_sub_sent = true;
            c->accept_sub_failed = true;
            c->accept_sub_rc = (int)rc;
            if (c->verbose)
                fprintf(stderr, "  [%s] accept_subscribe failed: %d\n",
                    c->log_prefix, rc);
        }
        pthread_mutex_unlock(&c->mu);
    }

    /* Drain events. */
    moq_event_t ev;
    while (moq_session_poll_events(sess, &ev, 1) == 1) {
        if (c->verbose)
            fprintf(stderr, "  [%s] event kind=%u\n", c->log_prefix, ev.kind);
        pthread_mutex_lock(&c->mu);
        switch (ev.kind) {
        case MOQ_EVENT_SESSION_CLOSED:
            c->session_closed = true;
            break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            c->ns_accepted = true;
            break;
        case MOQ_EVENT_NAMESPACE_REJECTED:
            c->ns_rejected = true;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            c->subscribe_ok = true;
            break;
        case MOQ_EVENT_SUBSCRIBE_ERROR:
            c->subscribe_error = true;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            if (!c->subscribe_request_received) {
                c->subscribe_request_received = true;
                c->incoming_sub = ev.u.subscribe_request.sub;
            }
            break;
        default:
            break;
        }
        pthread_mutex_unlock(&c->mu);
        moq_event_cleanup(&ev);
    }

    return MOQ_OK;
}

/* ------------------------------------------------------------------ */
/* Transport label (for diagnostics): derived from the URL scheme.     */
/* ------------------------------------------------------------------ */

static const char *transport_label(const char *url)
{
    if (strncmp(url, "https://", 8) == 0) return "webtransport";
    return "quic"; /* moqt:// / moq:// */
}

/* ------------------------------------------------------------------ */
/* CLI / env                                                           */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *relay_url;
    const char *test_name;
    bool        tls_disable_verify;
    bool        verbose;
    int         draft;   /* 0 = auto (offer every draft); 16/18 = pin exactly */
} cli_opts_t;

static void print_usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s --relay URL --test NAME [options]\n"
        "\n"
        "  --relay URL             moqt://host:port (raw QUIC) or\n"
        "                          https://host:port/path (WebTransport)\n"
        "  --test NAME             test case name (default: run all cases)\n"
        "  --draft N               pin the offered draft to 16 or 18\n"
        "                          (default: offer every draft, negotiate one)\n"
        "  --tls-disable-verify    skip TLS certificate verification\n"
        "  --verbose               print session state and events to stderr\n"
        "  --help                  show this help\n"
        "\n"
        "Environment fallback (CLI wins):\n"
        "  RELAY_URL, TESTCASE, MOQT_DRAFT, TLS_DISABLE_VERIFY=1\n"
        "\n"
        "Transport (raw QUIC vs WebTransport) is selected automatically from the\n"
        "URL scheme. The draft version is auto-negotiated (the endpoint offers\n"
        "every draft this build supports) unless pinned with --draft/MOQT_DRAFT.\n"
        "\n"
        "Supported: setup-only, announce-only, publish-namespace-done,\n"
        "  subscribe-error, announce-subscribe, subscribe-before-announce\n",
        prog);
}

/* Strict draft parse: NULL/empty -> 0 (auto), exactly "16" or "18" -> that
 * draft, anything else -> -1 (rejected). atoi would silently accept "16junk"
 * as 16 and "abc" as 0 (auto), falsifying per-draft interop results. */
static int parse_draft(const char *s)
{
    if (!s || !*s) return 0;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (*end != '\0' || (v != 16 && v != 18)) return -1;
    return (int)v;
}

static cli_opts_t parse_cli(int argc, char **argv)
{
    cli_opts_t opts = {0};
    const char *draft_str = NULL;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--relay") == 0 && i + 1 < argc)
            opts.relay_url = argv[++i];
        else if (strcmp(argv[i], "--test") == 0 && i + 1 < argc)
            opts.test_name = argv[++i];
        else if (strcmp(argv[i], "--draft") == 0 && i + 1 < argc)
            draft_str = argv[++i];
        else if (strcmp(argv[i], "--tls-disable-verify") == 0)
            opts.tls_disable_verify = true;
        else if (strcmp(argv[i], "--verbose") == 0)
            opts.verbose = true;
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(1);
        }
    }
    if (!opts.relay_url) opts.relay_url = getenv("RELAY_URL");
    if (!opts.test_name) opts.test_name = getenv("TESTCASE");
    /* No test named (bare `make test`, which sets RELAY_URL but no TESTCASE):
     * leave test_name NULL so main() runs the whole suite, like the other
     * interop clients (moq-rs, imquic, ...). */
    if (!opts.tls_disable_verify) {
        const char *env = getenv("TLS_DISABLE_VERIFY");
        if (env && strcmp(env, "1") == 0) opts.tls_disable_verify = true;
    }
    /* Draft pin: --draft wins, else MOQT_DRAFT, else auto (offer all). */
    if (!draft_str) {
        const char *env = getenv("MOQT_DRAFT");
        if (env && *env) draft_str = env;
    }
    opts.draft = parse_draft(draft_str);
    if (opts.draft < 0) {
        fprintf(stderr, "error: --draft/MOQT_DRAFT must be exactly 16 or 18 "
                "(got \"%s\"); omit for auto-negotiation\n", draft_str);
        exit(1);
    }
    return opts;
}

/* ------------------------------------------------------------------ */
/* TAP output                                                          */
/* ------------------------------------------------------------------ */

static void tap_plan(int n)
{
    printf("TAP version 14\n");
    printf("1..%d\n", n);
}

static void tap_result(int num, bool ok, const char *desc,
                        uint64_t duration_ms, const char *message)
{
    printf("%s %d - %s\n", ok ? "ok" : "not ok", num, desc);
    printf("  ---\n");
    printf("  duration_ms: %" PRIu64 "\n", duration_ms);
    if (message)
        printf("  message: \"%s\"\n", message);
    printf("  ...\n");
    /* Stream each result: a relay that hangs a later sub-test, or a kill on
     * the runner's per-test timeout, then still leaves partial TAP captured. */
    fflush(stdout);
}

static void tap_skip(const char *test_name, const char *reason)
{
    printf("TAP version 14\n");
    printf("1..1\n");
    printf("ok 1 - %s # SKIP %s\n", test_name, reason);
}

/* Emit a TAP comment naming the negotiated draft + transport, once setup is
 * complete. Comments are ignored by harness pass/fail accounting. */
static void tap_negotiated(moq_endpoint_t *ep, const char *url)
{
    printf("# negotiated: draft-%u over %s\n",
        (unsigned)moq_endpoint_negotiated_version(ep), transport_label(url));
    fflush(stdout);
}

/* ------------------------------------------------------------------ */
/* Endpoint helpers                                                     */
/* ------------------------------------------------------------------ */

/* Draft to pin for this run: 0 = auto (offer every draft this build supports),
 * 16/18 = offer exactly that one. Set once from the CLI/env in main(). */
static int g_draft_pin = 0;

/* Create + start an endpoint. Transport, draft offer, and TLS are all owned
 * by the endpoint: AUTO protocol derives the transport from the URL scheme;
 * the zero-init version offer means "offer every draft this build supports"
 * unless a specific draft was pinned (--draft / MOQT_DRAFT). */
static moq_endpoint_t *make_endpoint(const char *url, bool insecure)
{
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init(&cfg);
    cfg.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    cfg.protocol = MOQ_TRANSPORT_PROTOCOL_AUTO;
    cfg.backend = MOQ_TRANSPORT_BACKEND_AUTO;
    /* cfg.versions left zero-init == MOQ_VERSION_POLICY_AUTO (offer all). */
    if (g_draft_pin != 0) {
        /* EXACT offer: propose only the pinned draft; a peer that can't speak
         * it fails the connect (that is the point of a pinned interop run).
         * The offer is copied by connect, so a static single-element array is
         * a safe backing store for the borrowed pointer. */
        static moq_version_t pinned;
        pinned = (moq_version_t)g_draft_pin;
        cfg.versions.struct_size  = sizeof(moq_version_offer_t);
        cfg.versions.policy       = MOQ_VERSION_POLICY_EXACT;
        cfg.versions.versions     = &pinned;
        cfg.versions.version_count = 1;
    }
    cfg.insecure_skip_verify = insecure;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* cfg.sni empty -> defaults to URL host; cfg.wt_path empty -> URL path. */

    moq_endpoint_t *ep = NULL;
    if (moq_endpoint_connect(&cfg, &ep) != MOQ_OK) return NULL;
    return ep;
}

static void close_endpoint(moq_endpoint_t *ep)
{
    if (!ep) return;
    moq_endpoint_stop(ep);
    moq_endpoint_destroy(ep);
}

/* One pump cycle: post the drain/execute task onto the net thread, then block
 * up to rem_us for it to run / the endpoint to wake.
 *
 * Concurrency invariant (do not break): the posted task (interop_pump_task)
 * holds c->mu only to read/write ctx fields and releases it before every
 * moq_session_* call; the app thread holds c->mu only to read/write ctx and
 * never while calling a moq_endpoint_* function. That ordering is what keeps
 * the endpoint's internal lock and c->mu from forming an ABBA cycle. */
static moq_result_t ep_cycle(moq_endpoint_t *ep, test_ctx_t *c, uint64_t rem_us)
{
    /* post() returning terminal means the endpoint is already closed; the
     * wait() below then returns MOQ_ERR_CLOSED and the caller's loop exits. */
    moq_endpoint_post(ep, interop_pump_task, c);
    return moq_endpoint_wait(ep, rem_us);
}

/* Wait until *flag is set, or the session/endpoint goes terminal, or timeout. */
static bool wait_for_flag(moq_endpoint_t *ep, test_ctx_t *c, bool *flag,
                           uint64_t timeout_us, const char **out_reason)
{
    uint64_t deadline = now_us_mono() + timeout_us;
    for (;;) {
        pthread_mutex_lock(&c->mu);
        bool done = *flag;
        bool closed = c->session_closed;
        pthread_mutex_unlock(&c->mu);
        if (done) return true;
        if (closed || moq_endpoint_is_closed(ep)) {
            *out_reason = "session closed"; return false;
        }
        if (moq_endpoint_is_fatal(ep)) { *out_reason = "fatal"; return false; }
        uint64_t now = now_us_mono();
        if (now >= deadline) { *out_reason = "timeout"; return false; }
        uint64_t rem = deadline - now;
        if (rem > 100000) rem = 100000;
        moq_result_t wr = ep_cycle(ep, c, rem);
        if (wr == MOQ_ERR_CLOSED) { *out_reason = "connection closed"; return false; }
    }
}

/* ------------------------------------------------------------------ */
/* Test: setup-only                                                    */
/* ------------------------------------------------------------------ */

static int run_setup_only(const char *url, bool insecure, bool verbose)
{
    test_ctx_t c;
    ctx_init(&c, TEST_SETUP_ONLY, verbose, NULL);

    moq_endpoint_t *ep = make_endpoint(url, insecure);
    if (!ep) {
        tap_plan(1);
        tap_result(1, false, "setup handshake", 0, "failed to create endpoint");
        ctx_destroy(&c);
        return 1;
    }

    if (verbose) fprintf(stderr, "connecting to %s...\n", url);

    uint64_t t0 = now_us_mono();
    const char *reason = NULL;
    bool ok = wait_for_flag(ep, &c, &c.setup_complete, 3000000, &reason);
    uint64_t ms = (now_us_mono() - t0) / 1000;

    tap_plan(1);
    if (ok) tap_negotiated(ep, url);
    tap_result(1, ok, "setup handshake", ms,
        ok ? "MoQ setup complete" : reason);

    close_endpoint(ep);
    ctx_destroy(&c);
    return ok ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* Test: announce-only                                                 */
/* ------------------------------------------------------------------ */

static int run_announce_only(const char *url, bool insecure, bool verbose)
{
    test_ctx_t c;
    ctx_init(&c, TEST_ANNOUNCE_ONLY, verbose, NULL);
    int exit_code = 0;

    moq_endpoint_t *ep = make_endpoint(url, insecure);
    if (!ep) {
        tap_plan(2);
        tap_result(1, false, "setup", 0, "failed to create endpoint");
        tap_result(2, false, "namespace accepted", 0, "no connection");
        ctx_destroy(&c);
        return 1;
    }

    if (verbose) fprintf(stderr, "connecting to %s...\n", url);

    uint64_t t0 = now_us_mono();
    const char *reason = NULL;
    bool setup_ok = wait_for_flag(ep, &c, &c.setup_complete, 3000000, &reason);
    uint64_t ms1 = (now_us_mono() - t0) / 1000;

    tap_plan(2);
    if (setup_ok) tap_negotiated(ep, url);
    tap_result(1, setup_ok, "setup", ms1,
        setup_ok ? "MoQ setup complete" : reason);
    if (!setup_ok) { exit_code = 1; goto done; }

    /* Request publish_namespace. */
    pthread_mutex_lock(&c.mu);
    c.req_publish_ns = true;
    pthread_mutex_unlock(&c.mu);

    uint64_t t1 = now_us_mono();
    uint64_t ns_deadline = t1 + 3000000;
    const char *ns_reason = "timeout";
    while (true) {
        pthread_mutex_lock(&c.mu);
        bool accepted = c.ns_accepted;
        bool rejected = c.ns_rejected;
        bool failed = c.publish_ns_failed;
        bool closed = c.session_closed;
        pthread_mutex_unlock(&c.mu);
        if (accepted || rejected || failed) break;
        if (closed || moq_endpoint_is_closed(ep)) { ns_reason = "session closed"; break; }
        if (moq_endpoint_is_fatal(ep)) { ns_reason = "fatal"; break; }
        uint64_t now = now_us_mono();
        if (now >= ns_deadline) break;
        uint64_t rem = ns_deadline - now;
        if (rem > 100000) rem = 100000;
        moq_result_t wr = ep_cycle(ep, &c, rem);
        if (wr == MOQ_ERR_CLOSED) { ns_reason = "connection closed"; break; }
    }
    uint64_t ms2 = (now_us_mono() - t1) / 1000;

    pthread_mutex_lock(&c.mu);
    bool ns_accepted = c.ns_accepted;
    bool ns_rejected = c.ns_rejected;
    bool ns_failed = c.publish_ns_failed;
    int ns_rc = c.publish_ns_rc;
    pthread_mutex_unlock(&c.mu);

    if (ns_failed) {
        char msg[64];
        snprintf(msg, sizeof(msg), "publish_namespace failed: %d", ns_rc);
        tap_result(2, false, "namespace accepted", ms2, msg);
        exit_code = 1;
    } else if (ns_rejected) {
        tap_result(2, false, "namespace accepted", ms2,
            "namespace rejected by relay");
        exit_code = 1;
    } else {
        tap_result(2, ns_accepted, "namespace accepted", ms2,
            ns_accepted ? "NAMESPACE_ACCEPTED received" : ns_reason);
        if (!ns_accepted) exit_code = 1;
    }

done:
    close_endpoint(ep);
    ctx_destroy(&c);
    return exit_code;
}

/* ------------------------------------------------------------------ */
/* Test: publish-namespace-done                                        */
/* ------------------------------------------------------------------ */

static int run_publish_namespace_done(const char *url, bool insecure,
                                       bool verbose)
{
    test_ctx_t c;
    ctx_init(&c, TEST_PUBLISH_NAMESPACE_DONE, verbose, NULL);
    int exit_code = 0;

    moq_endpoint_t *ep = make_endpoint(url, insecure);
    if (!ep) {
        tap_plan(3);
        tap_result(1, false, "setup", 0, "failed to create endpoint");
        tap_result(2, false, "namespace accepted", 0, "no connection");
        tap_result(3, false, "namespace done sent", 0, "no connection");
        ctx_destroy(&c);
        return 1;
    }

    if (verbose) fprintf(stderr, "connecting to %s...\n", url);

    uint64_t t0 = now_us_mono();
    const char *reason = NULL;
    bool setup_ok = wait_for_flag(ep, &c, &c.setup_complete, 3000000, &reason);
    uint64_t ms1 = (now_us_mono() - t0) / 1000;

    tap_plan(3);
    if (setup_ok) tap_negotiated(ep, url);
    tap_result(1, setup_ok, "setup", ms1,
        setup_ok ? "MoQ setup complete" : reason);
    if (!setup_ok) { exit_code = 1; goto done; }

    /* Publish namespace. */
    pthread_mutex_lock(&c.mu);
    c.req_publish_ns = true;
    pthread_mutex_unlock(&c.mu);

    uint64_t t1 = now_us_mono();
    uint64_t ns_deadline = t1 + 3000000;
    const char *ns_reason = "timeout";
    while (true) {
        pthread_mutex_lock(&c.mu);
        bool accepted = c.ns_accepted;
        bool rejected = c.ns_rejected;
        bool failed = c.publish_ns_failed;
        bool closed = c.session_closed;
        pthread_mutex_unlock(&c.mu);
        if (accepted || rejected || failed) break;
        if (closed || moq_endpoint_is_closed(ep)) { ns_reason = "session closed"; break; }
        if (moq_endpoint_is_fatal(ep)) { ns_reason = "fatal"; break; }
        uint64_t now = now_us_mono();
        if (now >= ns_deadline) break;
        uint64_t rem = ns_deadline - now;
        if (rem > 100000) rem = 100000;
        moq_result_t wr = ep_cycle(ep, &c, rem);
        if (wr == MOQ_ERR_CLOSED) { ns_reason = "connection closed"; break; }
    }
    uint64_t ms2 = (now_us_mono() - t1) / 1000;

    pthread_mutex_lock(&c.mu);
    bool ns_accepted2 = c.ns_accepted;
    bool ns_failed2 = c.publish_ns_failed;
    int ns_rc2 = c.publish_ns_rc;
    pthread_mutex_unlock(&c.mu);

    if (ns_failed2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "publish_namespace failed: %d", ns_rc2);
        tap_result(2, false, "namespace accepted", ms2, msg);
        exit_code = 1; goto done;
    }
    tap_result(2, ns_accepted2, "namespace accepted", ms2,
        ns_accepted2 ? "NAMESPACE_ACCEPTED received" : ns_reason);
    if (!ns_accepted2) { exit_code = 1; goto done; }

    /* Send publish_namespace_done. */
    pthread_mutex_lock(&c.mu);
    c.req_publish_ns_done = true;
    pthread_mutex_unlock(&c.mu);

    uint64_t t2 = now_us_mono();
    reason = NULL;
    bool done_ok = wait_for_flag(ep, &c, &c.publish_ns_done_sent, 3000000, &reason);
    uint64_t ms3 = (now_us_mono() - t2) / 1000;

    pthread_mutex_lock(&c.mu);
    bool done_failed = c.publish_ns_done_failed;
    int done_rc = c.publish_ns_done_rc;
    pthread_mutex_unlock(&c.mu);

    if (done_failed) {
        char msg[64];
        snprintf(msg, sizeof(msg), "publish_namespace_done failed: %d", done_rc);
        tap_result(3, false, "namespace done sent", ms3, msg);
        exit_code = 1;
    } else {
        tap_result(3, done_ok, "namespace done sent", ms3,
            done_ok ? "PUBLISH_NAMESPACE_DONE sent" : reason);
        if (!done_ok) exit_code = 1;
    }

done:
    close_endpoint(ep);
    ctx_destroy(&c);
    return exit_code;
}

/* ------------------------------------------------------------------ */
/* Test: subscribe-error                                               */
/* ------------------------------------------------------------------ */

static int run_subscribe_error(const char *url, bool insecure, bool verbose)
{
    test_ctx_t c;
    ctx_init(&c, TEST_SUBSCRIBE_ERROR, verbose, NULL);
    int exit_code = 0;

    moq_endpoint_t *ep = make_endpoint(url, insecure);
    if (!ep) {
        tap_plan(2);
        tap_result(1, false, "setup", 0, "failed to create endpoint");
        tap_result(2, false, "subscribe error or timeout", 0, "no connection");
        ctx_destroy(&c);
        return 1;
    }

    if (verbose) fprintf(stderr, "connecting to %s...\n", url);

    uint64_t t0 = now_us_mono();
    const char *reason = NULL;
    bool setup_ok = wait_for_flag(ep, &c, &c.setup_complete, 3000000, &reason);
    uint64_t ms1 = (now_us_mono() - t0) / 1000;

    tap_plan(2);
    if (setup_ok) tap_negotiated(ep, url);
    tap_result(1, setup_ok, "setup", ms1,
        setup_ok ? "MoQ setup complete" : reason);
    if (!setup_ok) { exit_code = 1; goto done; }

    /* Subscribe to nonexistent namespace. */
    pthread_mutex_lock(&c.mu);
    c.req_subscribe = true;
    pthread_mutex_unlock(&c.mu);

    uint64_t t1 = now_us_mono();
    uint64_t sub_deadline = t1 + 3000000;
    bool got_error = false;
    bool got_ok = false;
    bool sub_call_failed = false;
    bool sub_was_sent = false;
    while (true) {
        pthread_mutex_lock(&c.mu);
        got_error = c.subscribe_error;
        got_ok = c.subscribe_ok;
        sub_call_failed = c.subscribe_failed;
        sub_was_sent = c.subscribe_sent && !c.subscribe_failed;
        bool closed = c.session_closed;
        pthread_mutex_unlock(&c.mu);
        if (got_error || got_ok || sub_call_failed) break;
        if (closed || moq_endpoint_is_closed(ep) || moq_endpoint_is_fatal(ep)) break;
        uint64_t now = now_us_mono();
        if (now >= sub_deadline) break;
        uint64_t rem = sub_deadline - now;
        if (rem > 100000) rem = 100000;
        moq_result_t wr = ep_cycle(ep, &c, rem);
        if (wr == MOQ_ERR_CLOSED) break;
    }
    uint64_t ms2 = (now_us_mono() - t1) / 1000;

    /* Re-read final state after loop exit. */
    pthread_mutex_lock(&c.mu);
    got_error = c.subscribe_error;
    got_ok = c.subscribe_ok;
    sub_call_failed = c.subscribe_failed;
    sub_was_sent = c.subscribe_sent && !c.subscribe_failed;
    bool sub_closed = c.session_closed;
    pthread_mutex_unlock(&c.mu);
    if (moq_endpoint_is_closed(ep) || moq_endpoint_is_fatal(ep)) sub_closed = true;

    if (sub_call_failed) {
        pthread_mutex_lock(&c.mu);
        int src = c.subscribe_rc;
        pthread_mutex_unlock(&c.mu);
        char msg[64];
        snprintf(msg, sizeof(msg), "subscribe call failed: %d", src);
        tap_result(2, false, "subscribe error or timeout", ms2, msg);
        exit_code = 1;
    } else if (got_ok) {
        tap_result(2, false, "subscribe error or timeout", ms2,
            "unexpected SUBSCRIBE_OK for nonexistent namespace");
        exit_code = 1;
    } else if (got_error) {
        tap_result(2, true, "subscribe error or timeout", ms2,
            "SUBSCRIBE_ERROR received");
    } else if (sub_closed) {
        tap_result(2, false, "subscribe error or timeout", ms2,
            "session closed before response");
        exit_code = 1;
    } else if (sub_was_sent) {
        tap_result(2, true, "subscribe error or timeout", ms2,
            "timeout after SUBSCRIBE sent (acceptable per interop spec)");
    } else {
        tap_result(2, false, "subscribe error or timeout", ms2,
            "SUBSCRIBE never sent");
        exit_code = 1;
    }

done:
    close_endpoint(ep);
    ctx_destroy(&c);
    return exit_code;
}

/* ------------------------------------------------------------------ */
/* Dual-session: common pub+sub test logic                             */
/* ------------------------------------------------------------------ */

static int run_dual_session(const char *url, bool insecure, bool verbose,
                            bool subscribe_first)
{
    test_ctx_t pub_ctx, sub_ctx;
    ctx_init(&pub_ctx, TEST_ANNOUNCE_ONLY, verbose, "pub");
    ctx_init(&sub_ctx, TEST_SUBSCRIBE_ERROR, verbose, "sub");
    sub_ctx.subscribe_ns = ns_interop_parts;
    sub_ctx.subscribe_ns_count = ns_interop_count;
    pub_ctx.req_accept_sub = true;

    moq_endpoint_t *pub_ep = NULL, *sub_ep = NULL;

    static const char *names[5] = {
        "publisher setup",
        "publisher namespace accepted",
        "subscriber setup",
        "publisher accepted subscribe",
        "subscriber got SubscribeOk",
    };
    bool     ok[5]  = { false, false, false, false, false };
    uint64_t ms[5]  = { 0 };
    char     msg[5][128];
    for (int i = 0; i < 5; i++) snprintf(msg[i], sizeof(msg[i]), "skipped");
    bool announced_version = false;

#define FAIL(n, ...) do { \
    ok[n] = false; \
    snprintf(msg[n], sizeof(msg[n]), __VA_ARGS__); \
} while (0)
#define PASS(n, ...) do { \
    ok[n] = true; \
    snprintf(msg[n], sizeof(msg[n]), __VA_ARGS__); \
} while (0)

    /* -- subscribe-before-announce: subscriber connects first -- */
    if (subscribe_first) {
        sub_ep = make_endpoint(url, insecure);
        if (!sub_ep) { FAIL(2, "failed to create subscriber"); goto emit; }
        if (verbose) fprintf(stderr, "[sub] connecting to %s...\n", url);

        uint64_t t0 = now_us_mono();
        const char *r = NULL;
        bool s = wait_for_flag(sub_ep, &sub_ctx, &sub_ctx.setup_complete, 3000000, &r);
        ms[2] = (now_us_mono() - t0) / 1000;
        if (s) { PASS(2, "MoQ setup complete"); }
        else   { FAIL(2, "%s", r); goto emit; }

        pthread_mutex_lock(&sub_ctx.mu);
        sub_ctx.req_subscribe = true;
        pthread_mutex_unlock(&sub_ctx.mu);
        /* drive one cycle so the SUBSCRIBE is sent before the publisher exists */
        ep_cycle(sub_ep, &sub_ctx, 50000);

        struct timespec sl = { 0, 500000000 };
        nanosleep(&sl, NULL);

        pub_ep = make_endpoint(url, insecure);
        if (!pub_ep) { FAIL(0, "failed to create publisher"); goto emit; }
        if (verbose) fprintf(stderr, "[pub] connecting to %s...\n", url);
    } else {
        pub_ep = make_endpoint(url, insecure);
        if (!pub_ep) { FAIL(0, "failed to create publisher"); goto emit; }
        if (verbose) fprintf(stderr, "[pub] connecting to %s...\n", url);
    }

    /* 1: publisher setup */
    {
        uint64_t t0 = now_us_mono();
        const char *r = NULL;
        bool s = wait_for_flag(pub_ep, &pub_ctx, &pub_ctx.setup_complete, 3000000, &r);
        ms[0] = (now_us_mono() - t0) / 1000;
        if (s) {
            PASS(0, "MoQ setup complete");
            if (!announced_version) { tap_negotiated(pub_ep, url); announced_version = true; }
        } else { FAIL(0, "%s", r); goto emit; }
    }

    /* 2: publisher namespace accepted */
    {
        pthread_mutex_lock(&pub_ctx.mu);
        pub_ctx.req_publish_ns = true;
        pthread_mutex_unlock(&pub_ctx.mu);

        uint64_t t0 = now_us_mono();
        uint64_t dl = t0 + 3000000;
        const char *nr = "timeout";
        while (true) {
            pthread_mutex_lock(&pub_ctx.mu);
            bool accepted = pub_ctx.ns_accepted;
            bool rejected = pub_ctx.ns_rejected;
            bool failed = pub_ctx.publish_ns_failed;
            bool closed = pub_ctx.session_closed;
            pthread_mutex_unlock(&pub_ctx.mu);
            if (accepted || rejected || failed) break;
            if (closed || moq_endpoint_is_closed(pub_ep)) { nr = "session closed"; break; }
            if (moq_endpoint_is_fatal(pub_ep)) { nr = "fatal"; break; }
            uint64_t now = now_us_mono();
            if (now >= dl) break;
            uint64_t rem = dl - now;
            if (rem > 100000) rem = 100000;
            moq_result_t wr = ep_cycle(pub_ep, &pub_ctx, rem);
            if (wr == MOQ_ERR_CLOSED) { nr = "connection closed"; break; }
        }
        ms[1] = (now_us_mono() - t0) / 1000;

        pthread_mutex_lock(&pub_ctx.mu);
        bool na = pub_ctx.ns_accepted;
        bool nf = pub_ctx.publish_ns_failed;
        int nrc = pub_ctx.publish_ns_rc;
        bool nj = pub_ctx.ns_rejected;
        pthread_mutex_unlock(&pub_ctx.mu);

        if (nf)       { FAIL(1, "publish_namespace failed: %d", nrc); goto emit; }
        else if (nj)  { FAIL(1, "namespace rejected by relay"); goto emit; }
        else if (na)  { PASS(1, "NAMESPACE_ACCEPTED received"); }
        else          { FAIL(1, "%s", nr); goto emit; }
    }

    /* announce-subscribe: subscriber connects after announce. */
    if (!subscribe_first) {
        sub_ep = make_endpoint(url, insecure);
        if (!sub_ep) { FAIL(2, "failed to create subscriber"); goto emit; }
        if (verbose) fprintf(stderr, "[sub] connecting to %s...\n", url);

        uint64_t t0 = now_us_mono();
        const char *r = NULL;
        bool s = wait_for_flag(sub_ep, &sub_ctx, &sub_ctx.setup_complete, 3000000, &r);
        ms[2] = (now_us_mono() - t0) / 1000;
        if (s) { PASS(2, "MoQ setup complete"); }
        else   { FAIL(2, "%s", r); goto emit; }

        pthread_mutex_lock(&sub_ctx.mu);
        sub_ctx.req_subscribe = true;
        pthread_mutex_unlock(&sub_ctx.mu);
    }

    /* 4: publisher accepted subscribe. Both endpoints must be pumped: the
     * subscriber's SUBSCRIBE has to reach the relay and be forwarded to the
     * publisher, whose pump task then accepts it. */
    {
        uint64_t t0 = now_us_mono();
        uint64_t dl = t0 + 5000000;
        const char *ar = "timeout";
        while (true) {
            pthread_mutex_lock(&pub_ctx.mu);
            bool sent = pub_ctx.accept_sub_sent;
            bool afail = pub_ctx.accept_sub_failed;
            bool closed = pub_ctx.session_closed;
            pthread_mutex_unlock(&pub_ctx.mu);
            if (sent || afail) break;
            if (subscribe_first) {
                /* Premature SUBSCRIBE rejected by the relay -> the publisher
                 * will never see one to accept; stop waiting (that rejection
                 * is the expected outcome, resolved below). */
                pthread_mutex_lock(&sub_ctx.mu);
                bool serr_early = sub_ctx.subscribe_error;
                pthread_mutex_unlock(&sub_ctx.mu);
                if (serr_early) break;
            }
            if (closed || moq_endpoint_is_closed(pub_ep)) { ar = "session closed"; break; }
            if (moq_endpoint_is_fatal(pub_ep)) { ar = "fatal"; break; }
            uint64_t now = now_us_mono();
            if (now >= dl) break;
            /* pump the subscriber too, so its SUBSCRIBE flows */
            if (sub_ep) ep_cycle(sub_ep, &sub_ctx, 5000);
            moq_result_t wr = ep_cycle(pub_ep, &pub_ctx, 45000);
            if (wr == MOQ_ERR_CLOSED) { ar = "connection closed"; break; }
        }
        ms[3] = (now_us_mono() - t0) / 1000;

        pthread_mutex_lock(&pub_ctx.mu);
        bool asent = pub_ctx.accept_sub_sent;
        bool afail = pub_ctx.accept_sub_failed;
        int arc = pub_ctx.accept_sub_rc;
        pthread_mutex_unlock(&pub_ctx.mu);

        /* subscribe-before-announce: a relay that rejected the premature
         * SUBSCRIBE correctly leaves the publisher with nothing to accept.
         * Treat that as the expected pass; step 5 then confirms the
         * SUBSCRIBE_ERROR the subscriber saw. */
        bool sub_rejected = false;
        if (subscribe_first) {
            pthread_mutex_lock(&sub_ctx.mu);
            sub_rejected = sub_ctx.subscribe_error;
            pthread_mutex_unlock(&sub_ctx.mu);
        }

        if (afail)             { FAIL(3, "accept_subscribe failed: %d", arc); goto emit; }
        else if (asent)        { PASS(3, "SUBSCRIBE accepted"); }
        else if (sub_rejected) { PASS(3, "relay rejected premature SUBSCRIBE (expected)"); }
        else                   { FAIL(3, "%s", ar); goto emit; }
    }

    /* 5: subscriber got SubscribeOk */
    {
        uint64_t t0 = now_us_mono();
        uint64_t dl = t0 + 3000000;
        const char *sr = "timeout";
        while (true) {
            pthread_mutex_lock(&sub_ctx.mu);
            bool sok = sub_ctx.subscribe_ok;
            bool serr = sub_ctx.subscribe_error;
            bool sfail = sub_ctx.subscribe_failed;
            bool closed = sub_ctx.session_closed;
            pthread_mutex_unlock(&sub_ctx.mu);
            if (sok || serr || sfail) break;
            if (closed || moq_endpoint_is_closed(sub_ep)) { sr = "session closed"; break; }
            if (moq_endpoint_is_fatal(sub_ep)) { sr = "fatal"; break; }
            uint64_t now = now_us_mono();
            if (now >= dl) break;
            uint64_t rem = dl - now;
            if (rem > 100000) rem = 100000;
            moq_result_t wr = ep_cycle(sub_ep, &sub_ctx, rem);
            if (wr == MOQ_ERR_CLOSED) { sr = "connection closed"; break; }
        }
        ms[4] = (now_us_mono() - t0) / 1000;

        pthread_mutex_lock(&sub_ctx.mu);
        bool sok = sub_ctx.subscribe_ok;
        bool serr = sub_ctx.subscribe_error;
        bool sfail = sub_ctx.subscribe_failed;
        int src = sub_ctx.subscribe_rc;
        pthread_mutex_unlock(&sub_ctx.mu);

        if (sfail)      { FAIL(4, "subscribe failed: %d", src); }
        else if (subscribe_first) {
            /* subscribe-before-announce: the subscriber issued SUBSCRIBE before
             * the namespace existed. A relay that rejects it (SUBSCRIBE_ERROR)
             * is behaving correctly -- that is the expected pass. A relay that
             * instead holds the request and delivers SUBSCRIBE_OK once the
             * publisher announces is also valid. Either outcome passes; only a
             * transport failure or timeout fails. (announce-subscribe below
             * still requires SUBSCRIBE_OK.) */
            if (serr)      { PASS(4, "SUBSCRIBE_ERROR (expected before announce)"); }
            else if (sok)  { PASS(4, "SUBSCRIBE_OK received"); }
            else           { FAIL(4, "%s", sr); }
        }
        else if (serr)  { FAIL(4, "SUBSCRIBE_ERROR received"); }
        else if (sok)   { PASS(4, "SUBSCRIBE_OK received"); }
        else            { FAIL(4, "%s", sr); }
    }

emit:
    close_endpoint(sub_ep);
    close_endpoint(pub_ep);

    tap_plan(5);
    int exit_code = 0;
    for (int i = 0; i < 5; i++) {
        tap_result(i + 1, ok[i], names[i], ms[i], msg[i]);
        if (!ok[i]) exit_code = 1;
    }

    ctx_destroy(&sub_ctx);
    ctx_destroy(&pub_ctx);
    return exit_code;

#undef FAIL
#undef PASS
}

static int run_announce_subscribe(const char *url, bool insecure, bool verbose)
{
    return run_dual_session(url, insecure, verbose, false);
}

static int run_subscribe_before_announce(const char *url, bool insecure,
                                          bool verbose)
{
    return run_dual_session(url, insecure, verbose, true);
}

/* ------------------------------------------------------------------ */
/* Known test table                                                    */
/* ------------------------------------------------------------------ */

static const char *unsupported_tests[] = {
    NULL,
};

static bool is_unsupported(const char *name)
{
    for (const char **p = unsupported_tests; *p; p++)
        if (strcmp(name, *p) == 0) return true;
    return false;
}

typedef struct {
    const char *name;
    int (*run)(const char *, bool, bool);
} test_entry_t;

static const test_entry_t supported_tests[] = {
    { "setup-only",                run_setup_only },
    { "announce-only",             run_announce_only },
    { "publish-namespace-done",    run_publish_namespace_done },
    { "subscribe-error",           run_subscribe_error },
    { "announce-subscribe",        run_announce_subscribe },
    { "subscribe-before-announce", run_subscribe_before_announce },
    { NULL, NULL },
};

/* No explicit test selected (bare `make test` sets RELAY_URL but no TESTCASE):
 * run the whole suite and emit ONE TAP line per case, matching the other
 * interop clients (moq-rs, imquic, ...). Each case's own multi-step TAP is
 * suppressed here (redirected off stdout) so the run-all view stays one line
 * per case; use --test/TESTCASE to run a single case in full detail. The exit
 * code is lenient: 0 as long as at least one case ran to a result (the runner
 * grades per-case from the TAP, and reserves a non-zero exit for a client that
 * could not run at all). */
static int run_all_tests(const char *url, bool insecure, bool verbose)
{
    int total = 0;
    for (const test_entry_t *e = supported_tests; e->name; e++) total++;
    tap_plan(total);

    int num = 0, passed = 0;
    for (const test_entry_t *e = supported_tests; e->name; e++) {
        num++;
        /* Distinct namespace per case so back-to-back announces don't collide
         * on a relay still holding a prior case's namespace. */
        set_case_ns(e->name);
        /* Redirect the case's stdout (its own plan + sub-steps) to /dev/null,
         * keeping only the per-case summary line below. stderr (verbose diag)
         * is left alone. Restore stdout before reporting. */
        fflush(stdout);
        int saved = dup(STDOUT_FILENO);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDOUT_FILENO); close(devnull); }
        int rc = e->run(url, insecure, verbose);
        fflush(stdout);
        if (saved >= 0) { dup2(saved, STDOUT_FILENO); close(saved); }

        bool ok = (rc == 0);
        if (ok) passed++;
        tap_result(num, ok, e->name, 0, ok ? NULL : "case did not pass");
    }
    return passed > 0 ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    cli_opts_t opts = parse_cli(argc, argv);
    g_draft_pin = opts.draft;

    if (!opts.relay_url) {
        fprintf(stderr, "error: --relay is required "
                "(or set RELAY_URL)\n");
        print_usage(argv[0]);
        return 1;
    }

    /* No test named -> run the whole suite (one TAP line per case). */
    if (!opts.test_name) {
        return run_all_tests(opts.relay_url, opts.tls_disable_verify,
                             opts.verbose);
    }

    if (is_unsupported(opts.test_name)) {
        tap_skip(opts.test_name, "not yet implemented");
        return 127;
    }

    const test_entry_t *entry = NULL;
    for (const test_entry_t *e = supported_tests; e->name; e++) {
        if (strcmp(opts.test_name, e->name) == 0) {
            entry = e;
            break;
        }
    }
    if (!entry) {
        tap_skip(opts.test_name, "unknown test case");
        return 127;
    }

    return entry->run(opts.relay_url, opts.tls_disable_verify, opts.verbose);
}
