/*
 * Managed mvfst stress tests.
 *
 * 1. Repeated connect/disconnect: 25 sequential cycles.
 * 2. Multi-client churn: 3 clients, per-client objects,
 *    close one, verify others continue receiving.
 * 3. Datagram burst: burst of datagrams followed by a stream
 *    object; stream object must arrive even if datagrams are lost.
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
#include <map>
#include <mutex>
#include <set>
#include <string>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Cert ------------------------------------------------------------ */

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
    char cert_path[64] = "/tmp/moq_stress_cert_XXXXXX";
    char key_path[64]  = "/tmp/moq_stress_key_XXXXXX";
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

static bool ns_eq(const moq_namespace_t &ns, const char *a, const char *b) {
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

/* -- Server pump: accepts subs, writes objects, sends GOAWAY --------- */

struct server_state {
    std::atomic<bool> error{false};

    struct per_conn {
        moq_subscription_t sub = {};
        bool sub_accepted = false;
        int objs_written = 0;
    };
    std::map<moq_mvfst_conn_t *, per_conn> conns;

    std::atomic<int> write_round{0};
    std::atomic<bool> may_goaway{false};
    std::atomic<bool> goaway_sent{false};
    std::atomic<bool> send_dg_burst{false};
    std::atomic<bool> dg_burst_done{false};
    std::atomic<bool> send_post_burst{false};
    std::atomic<bool> post_burst_done{false};
};

static int server_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    auto *ss = static_cast<server_state *>(ctx);

    std::set<moq_mvfst_conn_t *> live;
    {
        moq_mvfst_conn_t *c = nullptr;
        while ((c = moq_mvfst_managed_next_conn(m, c)) != nullptr)
            live.insert(c);
    }
    for (auto it = ss->conns.begin(); it != ss->conns.end(); ) {
        if (live.count(it->first) == 0)
            it = ss->conns.erase(it);
        else
            ++it;
    }

    for (auto *conn : live) {
        auto &pc = ss->conns[conn];
        moq_session_t *s = moq_mvfst_conn_session(conn);
        if (!s) continue;

        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !pc.sub_accepted) {
                auto &req = ev[i].u.subscribe_request;
                if (ns_eq(req.track_namespace, "stress", "test") &&
                    bytes_eq(req.track_name, "video")) {
                    pc.sub = req.sub;
                    moq_accept_subscribe_cfg_t acfg;
                    moq_accept_subscribe_cfg_init(&acfg);
                    if (moq_session_accept_subscribe(s, pc.sub, &acfg, 0) >= 0)
                        pc.sub_accepted = true;
                    else
                        ss->error.store(true);
                }
            }
            moq_event_cleanup(&ev[i]);
        }

        int wr = ss->write_round.load();
        if (wr > 0 && pc.sub_accepted && pc.objs_written < wr) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = static_cast<uint64_t>(pc.objs_written);
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, pc.sub, &sgcfg, 0, &sg) < 0) {
                ss->error.store(true);
            } else {
                const char *pl = "stress-payload";
                moq_rcbuf_t *buf = nullptr;
                if (moq_rcbuf_create(moq_alloc_default(),
                        reinterpret_cast<const uint8_t *>(pl),
                        std::strlen(pl), &buf) < 0) {
                    ss->error.store(true);
                } else if (moq_session_write_object(s, sg, 0, buf, 0) < 0) {
                    moq_rcbuf_decref(buf);
                    ss->error.store(true);
                } else {
                    moq_rcbuf_decref(buf);
                    if (moq_session_close_subgroup(s, sg, 0) < 0)
                        ss->error.store(true);
                    else
                        pc.objs_written++;
                }
            }
        }

        if (ss->may_goaway.load() && !ss->goaway_sent.load()) {
            if (moq_session_goaway(s, nullptr, 0, now) < 0)
                ss->error.store(true);
            else
                ss->goaway_sent.store(true);
        }

        if (ss->send_dg_burst.load() && pc.sub_accepted &&
            !ss->dg_burst_done.load()) {
            for (int d = 0; d < 20; d++) {
                uint8_t dg_data[4] = {0xD6, 0x00,
                    static_cast<uint8_t>(d >> 8),
                    static_cast<uint8_t>(d & 0xFF)};
                moq_rcbuf_t *dgbuf = nullptr;
                moq_result_t drc = moq_rcbuf_create(moq_alloc_default(),
                    dg_data, sizeof(dg_data), &dgbuf);
                if (drc < 0) { ss->error.store(true); break; }
                moq_result_t src = moq_session_send_object_datagram(
                    s, pc.sub, static_cast<uint64_t>(d), 0,
                    200, false, dgbuf, nullptr, 0, 0);
                moq_rcbuf_decref(dgbuf);
                if (src < 0) { ss->error.store(true); break; }
            }
            ss->dg_burst_done.store(true);
        }

        if (ss->send_post_burst.load() && pc.sub_accepted &&
            !ss->post_burst_done.load()) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 99;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, pc.sub, &sgcfg, 0, &sg) < 0) {
                ss->error.store(true);
            } else {
                const char *pl = "post-burst";
                moq_rcbuf_t *buf = nullptr;
                if (moq_rcbuf_create(moq_alloc_default(),
                        reinterpret_cast<const uint8_t *>(pl),
                        std::strlen(pl), &buf) < 0) {
                    ss->error.store(true);
                } else if (moq_session_write_object(s, sg, 0, buf, 0) < 0) {
                    moq_rcbuf_decref(buf);
                    ss->error.store(true);
                } else {
                    moq_rcbuf_decref(buf);
                    if (moq_session_close_subgroup(s, sg, 0) < 0)
                        ss->error.store(true);
                    else
                        ss->post_burst_done.store(true);
                }
            }
        }
    }
    return 0;
}

/* -- Client pump ----------------------------------------------------- */

struct client_state {
    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> sub_ok{false};
    std::atomic<int> objects_received{0};
    std::atomic<int> stream_objects{0};
    std::atomic<int> datagram_objects{0};
    std::atomic<bool> error{false};

    std::mutex pb_mu;
    bool post_burst_seen = false;
    uint64_t pb_group = 0;
    uint64_t pb_object = 0;
    std::string pb_payload;
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
        moq_bytes_t ns[] = {
            {(const uint8_t *)"stress", 6},
            {(const uint8_t *)"test", 4}
        };
        sc.track_namespace.parts = ns;
        sc.track_namespace.count = 2;
        sc.track_name = {(const uint8_t *)"video", 5};
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        if (moq_session_subscribe(s, &sc, 0, &sub) >= 0)
            cs->subscribed.store(true);
        else
            cs->error.store(true);
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
            cs->objects_received.fetch_add(1);
            if (o.datagram) {
                cs->datagram_objects.fetch_add(1);
            } else {
                cs->stream_objects.fetch_add(1);
                if (o.group_id == 99) {
                    std::lock_guard<std::mutex> lk(cs->pb_mu);
                    cs->pb_group = o.group_id;
                    cs->pb_object = o.object_id;
                    if (o.payload) {
                        auto *d = moq_rcbuf_data(o.payload);
                        auto l = moq_rcbuf_len(o.payload);
                        cs->pb_payload.assign(
                            reinterpret_cast<const char *>(d), l);
                    }
                    cs->post_burst_seen = true;
                }
            }
        }
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Wait helpers ---------------------------------------------------- */

static bool wait_for(moq_mvfst_managed_t *srv, moq_mvfst_managed_t *cli,
                      server_state *ss, client_state *cs,
                      std::function<bool()> pred, int ms = 5000) {
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < dl) {
        if (srv) moq_mvfst_managed_wake(srv);
        if (cli) moq_mvfst_managed_wake(cli);
        if (cli) moq_mvfst_managed_wait(cli, 30000);
        if (srv) moq_mvfst_managed_wait(srv, 30000);
        if (ss && ss->error.load()) return false;
        if (cs && cs->error.load()) return false;
        if (srv && moq_mvfst_managed_is_fatal(srv)) return false;
        if (cli && moq_mvfst_managed_is_fatal(cli)) return false;
        if (pred()) return true;
    }
    if (ss && ss->error.load()) return false;
    if (cs && cs->error.load()) return false;
    if (srv && moq_mvfst_managed_is_fatal(srv)) return false;
    if (cli && moq_mvfst_managed_is_fatal(cli)) return false;
    return pred();
}

static bool wait_multi(moq_mvfst_managed_t *srv,
                        moq_mvfst_managed_t **clients, client_state *states,
                        size_t ncli, server_state *ss,
                        std::function<bool()> pred, int ms = 5000) {
    auto has_error = [&]() -> bool {
        if (ss && ss->error.load()) return true;
        if (srv && moq_mvfst_managed_is_fatal(srv)) return true;
        for (size_t i = 0; i < ncli; i++) {
            if (clients[i] && moq_mvfst_managed_is_fatal(clients[i]))
                return true;
            if (states && states[i].error.load()) return true;
        }
        return false;
    };
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < dl) {
        if (srv) moq_mvfst_managed_wake(srv);
        for (size_t i = 0; i < ncli; i++)
            if (clients[i]) moq_mvfst_managed_wake(clients[i]);
        for (size_t i = 0; i < ncli; i++)
            if (clients[i]) moq_mvfst_managed_wait(clients[i], 20000);
        if (srv) moq_mvfst_managed_wait(srv, 20000);
        if (has_error()) return false;
        if (pred()) return true;
    }
    if (has_error()) return false;
    return pred();
}

/* ================================================================== */
/* 1. Repeated connect/disconnect                                     */
/* ================================================================== */

static void test_repeated_connect_disconnect()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok); if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok); if (!tf.ok) return;

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

    for (int round = 0; round < 25; round++) {
        client_state cs;
        moq_mvfst_managed_cfg_t ccfg;
        moq_mvfst_managed_cfg_init(&ccfg);
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.host = "127.0.0.1";
        ccfg.port = port;
        ccfg.insecure_skip_verify = true;
        ccfg.on_pump = client_pump;
        ccfg.user_ctx = &cs;
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 16;

        moq_mvfst_managed_t *cli = nullptr;
        moq_result_t rc = moq_mvfst_managed_create(&ccfg, &cli);
        MVFST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) break;

        MVFST_CHECK(wait_for(srv, cli, &ss, &cs, [&]() {
            return cs.setup_complete.load();
        }));

        moq_mvfst_managed_destroy(cli);

        MVFST_CHECK(wait_for(srv, nullptr, &ss, nullptr, [&]() {
            return moq_mvfst_managed_conn_count(srv) == 0;
        }));
        MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    }

    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(!ss.error.load());
    moq_mvfst_managed_destroy(srv);
}

/* ================================================================== */
/* 2. Multi-client churn                                              */
/* ================================================================== */

static void test_multi_client_churn()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok); if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok); if (!tf.ok) return;

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

    /* Connect 3 clients. */
    const int N = 3;
    client_state cs[N];
    moq_mvfst_managed_t *cli[N] = {};

    for (int i = 0; i < N; i++) {
        moq_mvfst_managed_cfg_t ccfg;
        moq_mvfst_managed_cfg_init(&ccfg);
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.host = "127.0.0.1";
        ccfg.port = port;
        ccfg.insecure_skip_verify = true;
        ccfg.on_pump = client_pump;
        ccfg.user_ctx = &cs[i];
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = 16;

        MVFST_CHECK(moq_mvfst_managed_create(&ccfg, &cli[i]) == MOQ_OK);
        if (!cli[i]) {
            for (int j = 0; j < i; j++) moq_mvfst_managed_destroy(cli[j]);
            moq_mvfst_managed_destroy(srv);
            return;
        }
    }

    /* Wait for all 3 to complete setup + subscribe. */
    MVFST_CHECK(wait_multi(srv, cli, cs, N, &ss, [&]() {
        for (int i = 0; i < N; i++)
            if (!cs[i].sub_ok.load()) return false;
        return moq_mvfst_managed_conn_count(srv) == 3;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 3);
    MVFST_CHECK(!ss.error.load());

    /* Round 1: server writes one object per connection. */
    ss.write_round.store(1);
    MVFST_CHECK(wait_multi(srv, cli, cs, N, &ss, [&]() {
        for (int i = 0; i < N; i++)
            if (cs[i].objects_received.load() < 1) return false;
        return true;
    }));
    for (int i = 0; i < N; i++)
        MVFST_CHECK(cs[i].objects_received.load() == 1);
    MVFST_CHECK(!ss.error.load());

    /* Destroy client B (index 1). */
    moq_mvfst_managed_destroy(cli[1]);
    cli[1] = nullptr;

    MVFST_CHECK(wait_multi(srv, cli, cs, N, &ss, [&]() {
        return moq_mvfst_managed_conn_count(srv) == 2;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 2);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));

    /* Round 2: server writes another object to remaining conns. */
    ss.write_round.store(2);
    MVFST_CHECK(wait_multi(srv, cli, cs, N, &ss, [&]() {
        return cs[0].objects_received.load() >= 2 &&
               cs[2].objects_received.load() >= 2;
    }));
    MVFST_CHECK(cs[0].objects_received.load() == 2);
    MVFST_CHECK(cs[1].objects_received.load() == 1);
    MVFST_CHECK(cs[2].objects_received.load() == 2);
    MVFST_CHECK(!ss.error.load());

    /* Destroy remaining clients. */
    moq_mvfst_managed_destroy(cli[0]);
    cli[0] = nullptr;
    moq_mvfst_managed_destroy(cli[2]);
    cli[2] = nullptr;

    MVFST_CHECK(wait_multi(srv, cli, cs, 0, &ss, [&]() {
        return moq_mvfst_managed_conn_count(srv) == 0;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 0);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(!ss.error.load());

    moq_mvfst_managed_destroy(srv);
}

/* ================================================================== */
/* 3. Datagram burst then stream object                               */
/* ================================================================== */

static void test_datagram_burst()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok); if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok); if (!tf.ok) return;

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

    client_state cs;
    moq_mvfst_managed_cfg_t ccfg;
    moq_mvfst_managed_cfg_init(&ccfg);
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_pump = client_pump;
    ccfg.user_ctx = &cs;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli = nullptr;
    MVFST_CHECK(moq_mvfst_managed_create(&ccfg, &cli) == MOQ_OK);
    if (!cli) { moq_mvfst_managed_destroy(srv); return; }

    /* Wait for setup + subscribe. */
    MVFST_CHECK(wait_for(srv, cli, &ss, &cs, [&]() {
        return cs.sub_ok.load();
    }));
    MVFST_CHECK(!ss.error.load());

    /* Send datagram burst. */
    ss.send_dg_burst.store(true);
    MVFST_CHECK(wait_for(srv, cli, &ss, &cs, [&]() {
        return ss.dg_burst_done.load();
    }));

    /* Send a reliable stream object after the burst. */
    ss.send_post_burst.store(true);
    MVFST_CHECK(wait_for(srv, cli, &ss, &cs, [&]() {
        std::lock_guard<std::mutex> lk(cs.pb_mu);
        return cs.post_burst_seen;
    }));

    /* Verify the post-burst stream object exactly. */
    {
        std::lock_guard<std::mutex> lk(cs.pb_mu);
        MVFST_CHECK(cs.post_burst_seen);
        MVFST_CHECK(cs.pb_group == 99);
        MVFST_CHECK(cs.pb_object == 0);
        MVFST_CHECK(cs.pb_payload == "post-burst");
    }
    MVFST_CHECK(cs.datagram_objects.load() <= 20);
    MVFST_CHECK(!ss.error.load());
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(cli));

    moq_mvfst_managed_destroy(cli);
    moq_mvfst_managed_destroy(srv);
}

int main()
{
    test_repeated_connect_disconnect();
    test_multi_client_churn();
    test_datagram_burst();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
