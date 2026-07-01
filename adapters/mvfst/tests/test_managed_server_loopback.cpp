/*
 * Managed server + managed client loopback test.
 *
 * Both sides use the managed C API. Phases:
 *   1. Setup handshake
 *   2. Client subscribes, server accepts
 *   3. Server writes single object
 *   4. Server writes multi-object subgroup
 *   5. Server writes datagram object
 *   6. Server closes connection via conn_close
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
#include <mutex>
#include <string>
#include <vector>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Cert generation ------------------------------------------------- */

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
    char cert_path[64] = "/tmp/moq_slb_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_slb_key_XXXXXX";
    bool ok = false;

    tmp_files(const test_cert &cm) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        ssize_t cw = write(cfd, cm.cert_pem.data(), cm.cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(cm.cert_pem.size())) return;
        int kfd = mkstemp(key_path);
        if (kfd < 0) { unlink(cert_path); return; }
        ssize_t kw = write(kfd, cm.key_pem.data(), cm.key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(cm.key_pem.size())) {
            unlink(cert_path); return;
        }
        ok = true;
    }
    ~tmp_files() { if (ok) { unlink(cert_path); unlink(key_path); } }
};

/* -- Helpers --------------------------------------------------------- */

static bool ns_eq(const moq_namespace_t &ns,
                   const char *a, const char *b) {
    if (ns.count != 2) return false;
    return ns.parts[0].len == std::strlen(a) &&
           std::memcmp(ns.parts[0].data, a, ns.parts[0].len) == 0 &&
           ns.parts[1].len == std::strlen(b) &&
           std::memcmp(ns.parts[1].data, b, ns.parts[1].len) == 0;
}

static bool bytes_eq(moq_bytes_t b, const char *s) {
    return b.len == std::strlen(s) &&
           std::memcmp(b.data, s, b.len) == 0;
}

static moq_result_t write_buf(moq_session_t *s,
                               moq_subgroup_handle_t sg,
                               uint64_t obj_id,
                               const char *data, size_t len)
{
    moq_rcbuf_t *buf = nullptr;
    moq_result_t rc = moq_rcbuf_create(moq_alloc_default(),
        reinterpret_cast<const uint8_t *>(data), len, &buf);
    if (rc < 0) return rc;
    rc = moq_session_write_object(s, sg, obj_id, buf, 0);
    moq_rcbuf_decref(buf);
    return rc;
}

/* -- Server pump state machine --------------------------------------- */

struct server_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> sub_accepted{false};
    std::atomic<bool> may_write{false};
    std::atomic<bool> single_written{false};
    std::atomic<bool> multi_written{false};
    std::atomic<bool> dg_written{false};
    std::atomic<bool> may_close{false};
    std::atomic<bool> close_sent{false};
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
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                ss->setup_complete.store(true);
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !ss->sub_accepted.load()) {
                auto &req = ev[i].u.subscribe_request;
                if (!ns_eq(req.track_namespace, "mvfst", "managed-server") ||
                    !bytes_eq(req.track_name, "video")) {
                    ss->error.store(true);
                } else {
                    ss->sub_handle = req.sub;
                    moq_accept_subscribe_cfg_t acfg;
                    moq_accept_subscribe_cfg_init(&acfg);
                    moq_result_t rc = moq_session_accept_subscribe(
                        s, ss->sub_handle, &acfg, 0);
                    if (rc < 0) ss->error.store(true);
                    else ss->sub_accepted.store(true);
                }
            }
            moq_event_cleanup(&ev[i]);
        }

        if (ss->may_write.load() && !ss->single_written.load()) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 0;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, ss->sub_handle,
                                           &sgcfg, 0, &sg) < 0) {
                ss->error.store(true);
            } else if (write_buf(s, sg, 0,
                        "hello-managed-server", 20) < 0) {
                ss->error.store(true);
            } else if (moq_session_close_subgroup(s, sg, 0) < 0) {
                ss->error.store(true);
            } else {
                ss->single_written.store(true);
            }
        }

        if (ss->single_written.load() && !ss->multi_written.load()) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 1;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            bool ok = moq_session_open_subgroup(s, ss->sub_handle,
                                                 &sgcfg, 0, &sg) >= 0;
            if (ok) ok = write_buf(s, sg, 0, "one", 3) >= 0;
            if (ok) ok = write_buf(s, sg, 1, "two", 3) >= 0;
            if (ok) ok = write_buf(s, sg, 2, "three", 5) >= 0;
            if (ok) ok = moq_session_close_subgroup(s, sg, 0) >= 0;
            if (!ok) ss->error.store(true);
            else ss->multi_written.store(true);
        }

        if (ss->multi_written.load() && !ss->dg_written.load()) {
            moq_rcbuf_t *buf = nullptr;
            const char *pl = "datagram-managed-server";
            if (moq_rcbuf_create(moq_alloc_default(),
                    reinterpret_cast<const uint8_t *>(pl),
                    std::strlen(pl), &buf) < 0) {
                ss->error.store(true);
            } else {
                moq_result_t rc = moq_session_send_object_datagram(
                    s, ss->sub_handle, 9, 7, 200, false,
                    buf, nullptr, 0, 0);
                moq_rcbuf_decref(buf);
                if (rc < 0) ss->error.store(true);
                else ss->dg_written.store(true);
            }
        }

        if (ss->dg_written.load() && ss->may_close.load() &&
            !ss->close_sent.load()) {
            moq_mvfst_conn_close(conn, 0);
            ss->close_sent.store(true);
        }
    }
    return 0;
}

/* -- Client pump ----------------------------------------------------- */

struct recv_obj {
    uint64_t group_id = 0;
    uint64_t object_id = 0;
    bool is_datagram = false;
    std::vector<uint8_t> payload;
};

struct client_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> sub_ok{false};
    std::atomic<bool> error{false};

    std::mutex result_mu;
    std::vector<recv_obj> stream_objects;
    recv_obj dg_obj;
    std::atomic<bool> dg_received{false};
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
            {(const uint8_t *)"managed-server", 14}
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
        if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_OK)
            cs->sub_ok.store(true);
        if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            auto &o = ev[i].u.object_received;
            recv_obj ro;
            ro.group_id = o.group_id;
            ro.object_id = o.object_id;
            ro.is_datagram = o.datagram;
            if (o.payload) {
                auto *d = moq_rcbuf_data(o.payload);
                auto l = moq_rcbuf_len(o.payload);
                ro.payload.assign(d, d + l);
            }
            std::lock_guard<std::mutex> lk(cs->result_mu);
            if (o.datagram) {
                cs->dg_obj = std::move(ro);
                cs->dg_received.store(true);
            } else {
                cs->stream_objects.push_back(std::move(ro));
            }
        }
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Wait helper ----------------------------------------------------- */

static bool wait_for(moq_mvfst_managed_t *ms, moq_mvfst_managed_t *mc,
                      server_state &ss, client_state &cs,
                      std::function<bool()> pred, int ms_timeout = 5000)
{
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms_timeout);
    while (std::chrono::steady_clock::now() < dl) {
        if (ss.error.load() || cs.error.load()) return false;
        if (ms) moq_mvfst_managed_wake(ms);
        if (mc) moq_mvfst_managed_wake(mc);
        if (mc) {
            moq_result_t wr = moq_mvfst_managed_wait(mc, 50000);
            if (wr == MOQ_ERR_CLOSED && !pred()) return false;
        }
        if (ms) {
            moq_result_t wr = moq_mvfst_managed_wait(ms, 50000);
            if (wr == MOQ_ERR_CLOSED && !pred()) return false;
        }
        if (pred()) return true;
    }
    return pred();
}

/* ================================================================== */
/* Test: full managed server/client flow                              */
/* ================================================================== */

static void test_managed_server_client_flow()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* --- Create managed server --- */
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
    moq_result_t rc = moq_mvfst_managed_create(&scfg, &srv);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);
    if (port == 0) { moq_mvfst_managed_destroy(srv); return; }

    /* --- Create managed client --- */
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
    rc = moq_mvfst_managed_create(&ccfg, &cli);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_mvfst_managed_destroy(srv); return; }

    /* --- Phase 1: setup --- */
    MVFST_CHECK(wait_for(srv, cli, ss, cs, [&]() {
        return cs.setup_complete.load() && ss.setup_complete.load();
    }));
    MVFST_CHECK(cs.setup_complete.load());
    MVFST_CHECK(ss.setup_complete.load());
    MVFST_CHECK(!ss.error.load());
    MVFST_CHECK(!cs.error.load());

    /* --- Phase 2: subscribe --- */
    MVFST_CHECK(wait_for(srv, cli, ss, cs, [&]() {
        return cs.sub_ok.load() && ss.sub_accepted.load();
    }));
    MVFST_CHECK(cs.sub_ok.load());
    MVFST_CHECK(ss.sub_accepted.load());
    MVFST_CHECK(!ss.error.load());

    ss.may_write.store(true);

    /* --- Phase 3+4: all stream objects --- */
    MVFST_CHECK(wait_for(srv, cli, ss, cs, [&]() {
        std::lock_guard<std::mutex> lk(cs.result_mu);
        return cs.stream_objects.size() >= 4;
    }));
    {
        std::lock_guard<std::mutex> lk(cs.result_mu);
        MVFST_CHECK(cs.stream_objects.size() >= 4);

        auto find_obj = [&](uint64_t g, uint64_t o) -> recv_obj * {
            for (auto &obj : cs.stream_objects)
                if (obj.group_id == g && obj.object_id == o) return &obj;
            return nullptr;
        };

        auto *single = find_obj(0, 0);
        MVFST_CHECK(single != nullptr);
        if (single) {
            MVFST_CHECK(!single->is_datagram);
            std::vector<uint8_t> exp(
                (const uint8_t *)"hello-managed-server",
                (const uint8_t *)"hello-managed-server" + 20);
            MVFST_CHECK(single->payload == exp);
        }

        const char *multi_payloads[] = {"one", "two", "three"};
        for (int j = 0; j < 3; j++) {
            auto *mo = find_obj(1, static_cast<uint64_t>(j));
            MVFST_CHECK(mo != nullptr);
            if (mo) {
                MVFST_CHECK(!mo->is_datagram);
                std::vector<uint8_t> exp(
                    multi_payloads[j],
                    multi_payloads[j] + std::strlen(multi_payloads[j]));
                MVFST_CHECK(mo->payload == exp);
            }
        }
    }

    /* --- Phase 5: datagram --- */
    MVFST_CHECK(wait_for(srv, cli, ss, cs, [&]() {
        return cs.dg_received.load();
    }));
    {
        std::lock_guard<std::mutex> lk(cs.result_mu);
        MVFST_CHECK(cs.dg_obj.group_id == 9);
        MVFST_CHECK(cs.dg_obj.object_id == 7);
        MVFST_CHECK(cs.dg_obj.is_datagram);
        const char *dp = "datagram-managed-server";
        std::vector<uint8_t> dexp(dp, dp + std::strlen(dp));
        MVFST_CHECK(cs.dg_obj.payload == dexp);
    }

    /* --- Phase 6: server closes connection --- */
    ss.may_close.store(true);
    MVFST_CHECK(wait_for(srv, cli, ss, cs, [&]() {
        return moq_mvfst_managed_conn_count(srv) == 0;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 0);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(ss.close_sent.load());
    MVFST_CHECK(!ss.error.load());

    /* --- Clean shutdown --- */
    moq_mvfst_managed_destroy(cli);
    moq_mvfst_managed_destroy(srv);
}

int main()
{
    test_managed_server_client_flow();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
