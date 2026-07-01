/*
 * Managed-server event-driven on_pump test.
 *
 * A reactive managed server (accepts a subscribe and publishes a small burst,
 * all inside on_pump) is driven with NO explicit server wakes after startup --
 * the test only ever wakes the client. This proves the server's on_pump runs
 * from EventBase-driven wakes alone (inbound handshake/subscribe callbacks
 * scheduling the pump on server_evb), with the 5ms manager-thread poll removed.
 *
 * Deterministic RED anchor: neuter the server pump scheduling and, with no poll
 * fallback, the server never accepts the subscribe or publishes -- nothing is
 * delivered and the test stalls. So delivery (progress), not latency, is the
 * signal.
 *
 * Separate binary: one QuicServer per process.
 */

#include <moq/mvfst.h>
#include <moq/session.h>
#include <moq/rcbuf.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <folly/ssl/OpenSSLPtrTypes.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Cert generation (same as the other managed-server tests) -------- */

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

static test_cert gen_cert()
{
    test_cert r;
    auto pkey = folly::ssl::EvpPkeyUniquePtr(EVP_PKEY_new());
    if (!pkey) return r;
    auto *ec = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (!ec) return r;
    if (!EC_KEY_generate_key(ec)) { EC_KEY_free(ec); return r; }
    if (!EVP_PKEY_assign_EC_KEY(pkey.get(), ec)) { EC_KEY_free(ec); return r; }
    auto x509 = folly::ssl::X509UniquePtr(X509_new());
    if (!x509) return r;
    if (!ASN1_INTEGER_set(X509_get_serialNumber(x509.get()), 1)) return r;
    if (!X509_gmtime_adj(X509_get_notBefore(x509.get()), 0)) return r;
    if (!X509_gmtime_adj(X509_get_notAfter(x509.get()), 365*24*3600)) return r;
    if (!X509_set_pubkey(x509.get(), pkey.get())) return r;
    auto *name = X509_get_subject_name(x509.get());
    if (!X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>("localhost"),
            -1, -1, 0)) return r;
    if (!X509_set_issuer_name(x509.get(), name)) return r;
    X509V3_CTX v3ctx;
    X509V3_set_ctx_nodb(&v3ctx);
    X509V3_set_ctx(&v3ctx, x509.get(), x509.get(), nullptr, nullptr, 0);
    auto *bc = X509V3_EXT_conf_nid(nullptr, &v3ctx,
                                    NID_basic_constraints, "CA:TRUE");
    if (!bc) return r;
    if (!X509_add_ext(x509.get(), bc, -1)) { X509_EXTENSION_free(bc); return r; }
    X509_EXTENSION_free(bc);
    auto *san = X509V3_EXT_conf_nid(nullptr, &v3ctx,
                                     NID_subject_alt_name,
                                     "DNS:localhost,IP:127.0.0.1");
    if (!san) return r;
    if (!X509_add_ext(x509.get(), san, -1)) { X509_EXTENSION_free(san); return r; }
    X509_EXTENSION_free(san);
    if (!X509_sign(x509.get(), pkey.get(), EVP_sha256())) return r;
    auto cb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!cb || !PEM_write_bio_X509(cb.get(), x509.get())) return r;
    char *cd = nullptr; auto cl = BIO_get_mem_data(cb.get(), &cd);
    if (cl <= 0 || !cd) return r;
    r.cert_pem.assign(cd, cl);
    auto kb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!kb || !PEM_write_bio_PrivateKey(kb.get(), pkey.get(),
            nullptr, nullptr, 0, nullptr, nullptr)) return r;
    char *kd = nullptr; auto kl = BIO_get_mem_data(kb.get(), &kd);
    if (kl <= 0 || !kd) return r;
    r.key_pem.assign(kd, kl);
    r.ok = true;
    return r;
}

struct tmp_files {
    char cert_path[64] = "/tmp/moq_sev_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_sev_key_XXXXXX";
    bool cert_created = false;
    bool key_created = false;
    bool ok = false;

    tmp_files(const test_cert &cm) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        cert_created = true;
        ssize_t cw = write(cfd, cm.cert_pem.data(), cm.cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(cm.cert_pem.size())) return;
        int kfd = mkstemp(key_path);
        if (kfd < 0) return;
        key_created = true;
        ssize_t kw = write(kfd, cm.key_pem.data(), cm.key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(cm.key_pem.size())) return;
        ok = true;
    }
    /* Unlink whatever was created, independent of ok, so a partial-write
     * failure never leaves a temp file behind. */
    ~tmp_files() {
        if (cert_created) unlink(cert_path);
        if (key_created) unlink(key_path);
    }
};

static bool ns_eq(const moq_namespace_t &ns, const char *a, const char *b) {
    if (ns.count != 2) return false;
    return ns.parts[0].len == std::strlen(a) &&
           std::memcmp(ns.parts[0].data, a, ns.parts[0].len) == 0 &&
           ns.parts[1].len == std::strlen(b) &&
           std::memcmp(ns.parts[1].data, b, ns.parts[1].len) == 0;
}
static bool bytes_eq(moq_bytes_t b, const char *s) {
    return b.len == std::strlen(s) && std::memcmp(b.data, s, b.len) == 0;
}

static constexpr int BURST = 6;

/* -- Reactive server pump: accept, then publish the burst ------------- */

struct server_state {
    std::atomic<bool> sub_accepted{false};
    std::atomic<bool> published{false};
    std::atomic<bool> error{false};
    moq_subscription_t sub_handle = {};
};

static int server_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *ss = static_cast<server_state *>(ctx);
    moq_mvfst_conn_t *conn = nullptr;
    while ((conn = moq_mvfst_managed_next_conn(m, conn)) != nullptr) {
        moq_session_t *s = moq_mvfst_conn_session(conn);
        if (!s) continue;

        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !ss->sub_accepted.load()) {
                auto &req = ev[i].u.subscribe_request;
                if (!ns_eq(req.track_namespace, "mvfst", "eventdriven") ||
                    !bytes_eq(req.track_name, "video")) {
                    ss->error.store(true);
                } else {
                    ss->sub_handle = req.sub;
                    moq_accept_subscribe_cfg_t acfg;
                    moq_accept_subscribe_cfg_init(&acfg);
                    if (moq_session_accept_subscribe(
                            s, ss->sub_handle, &acfg, 0) < 0)
                        ss->error.store(true);
                    else
                        ss->sub_accepted.store(true);
                }
            }
            moq_event_cleanup(&ev[i]);
        }

        /* Reactive publish: as soon as the subscribe is accepted, publish the
         * burst. This runs on a later pump pass (the settle pass), driven by
         * EventBase wakes -- never an explicit server wake. */
        if (ss->sub_accepted.load() && !ss->published.load()) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 0;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            bool ok = moq_session_open_subgroup(s, ss->sub_handle,
                                                 &sgcfg, 0, &sg) >= 0;
            for (int j = 0; ok && j < BURST; j++) {
                char pl[24];
                int pn = std::snprintf(pl, sizeof(pl), "ev-obj-%d", j);
                moq_rcbuf_t *buf = nullptr;
                if (moq_rcbuf_create(moq_alloc_default(),
                        reinterpret_cast<const uint8_t *>(pl),
                        static_cast<size_t>(pn), &buf) < 0) { ok = false; break; }
                if (moq_session_write_object(
                        s, sg, static_cast<uint64_t>(j), buf, 0) < 0)
                    ok = false;
                moq_rcbuf_decref(buf);
            }
            if (ok) ok = moq_session_close_subgroup(s, sg, 0) >= 0;
            if (!ok) ss->error.store(true);
            else ss->published.store(true);
        }
    }
    return 0;
}

/* -- Client pump ----------------------------------------------------- */

struct client_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> error{false};
    std::atomic<int> received{0};
};

static int client_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *cs = static_cast<client_state *>(ctx);
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    if (cs->setup_complete.load() && !cs->subscribed.load()) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns_parts[] = {
            {(const uint8_t *)"mvfst", 5},
            {(const uint8_t *)"eventdriven", 11}
        };
        sc.track_namespace.parts = ns_parts;
        sc.track_namespace.count = 2;
        sc.track_name = {(const uint8_t *)"video", 5};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_result_t rc = moq_session_subscribe(s, &sc, 0, &sub);
        if (rc >= 0) cs->subscribed.store(true);
        else cs->error.store(true);
    }

    moq_event_t ev[16]; size_t ne;
    moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
            cs->setup_complete.store(true);
        if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED)
            cs->received.fetch_add(1);
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Client-only drive: NEVER wakes the server ----------------------- */

static bool wait_client_only(moq_mvfst_managed_t *srv, moq_mvfst_managed_t *cli,
                             server_state &ss, client_state &cs,
                             std::function<bool()> pred, int ms_timeout = 10000)
{
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms_timeout);
    while (std::chrono::steady_clock::now() < dl) {
        if (ss.error.load() || cs.error.load()) return false;
        if (moq_mvfst_managed_is_fatal(srv) ||
            moq_mvfst_managed_is_fatal(cli)) return false;
        moq_mvfst_managed_wake(cli);            /* client only */
        moq_mvfst_managed_wait(cli, 50000);
        if (pred()) return true;
    }
    return pred();
}

/* ================================================================== */

static void test_server_event_driven_no_wakes()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    server_state ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init(&scfg);
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = server_pump;
    scfg.user_ctx = &ss;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *srv = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&scfg, &srv) == MOQ_OK);
    if (!srv) return;
    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);
    if (port == 0) { moq_mvfst_managed_destroy(srv); return; }

    client_state cs;
    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init(&ccfg);
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.cert_path = tf.cert_path;
    ccfg.on_pump = client_pump;
    ccfg.user_ctx = &cs;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&ccfg, &cli) == MOQ_OK);
    if (!cli) { moq_mvfst_managed_destroy(srv); return; }

    /* The whole flow -- server setup, subscribe accept, publish, delivery --
     * runs with the client as the ONLY explicitly-woken handle. The server's
     * on_pump is driven entirely by EventBase wakes (its inbound handshake and
     * SUBSCRIBE callbacks scheduling the pump on server_evb). */
    MVFST_CHECK(wait_client_only(srv, cli, ss, cs, [&]() {
        return cs.received.load() >= BURST;
    }));
    MVFST_CHECK(cs.received.load() == BURST);
    MVFST_CHECK(ss.sub_accepted.load());
    MVFST_CHECK(ss.published.load());
    MVFST_CHECK(!ss.error.load());
    MVFST_CHECK(!cs.error.load());
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));

    moq_mvfst_managed_destroy(cli);
    moq_mvfst_managed_destroy(srv);
}

int main()
{
    test_server_event_driven_no_wakes();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
