/*
 * Managed server multi-client correctness test.
 *
 * Two clients connect to the same managed server. Phases:
 *   1. Both clients complete setup, conn_count == 2
 *   2. Both clients subscribe, server accepts both
 *   3. Server writes distinct payloads to each client
 *   4. Server closes client A, conn_count drops to 1
 *   5. Server writes second object to client B
 *   6. Server closes client B, conn_count == 0
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

struct recv_obj {
    uint64_t group_id = 0;
    uint64_t object_id = 0;
    bool is_datagram = false;
    std::vector<uint8_t> payload;
};

/* -- Multi-client client state and pump ------------------------------ */

struct mc_client_state {
    const char *ns1;
    const char *ns2;
    const char *track;

    std::atomic<bool> setup_complete{false};
    std::atomic<bool> subscribed{false};
    std::atomic<bool> sub_ok{false};
    std::atomic<bool> error{false};

    std::mutex result_mu;
    std::vector<recv_obj> objects;
};

static int mc_client_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *cs = static_cast<mc_client_state *>(ctx);
    moq_session_t *s = moq_mvfst_managed_session(m);
    if (!s) return 0;

    if (cs->setup_complete.load() && !cs->subscribed.load()) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns_parts[] = {
            {(const uint8_t *)cs->ns1, std::strlen(cs->ns1)},
            {(const uint8_t *)cs->ns2, std::strlen(cs->ns2)}
        };
        sc.track_namespace.parts = ns_parts;
        sc.track_namespace.count = 2;
        sc.track_name = {(const uint8_t *)cs->track, std::strlen(cs->track)};
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
            cs->objects.push_back(std::move(ro));
        }
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

/* -- Multi-client server state and pump ------------------------------ */

enum mc_role { mc_role_none, mc_role_a, mc_role_b };

struct mc_per_conn {
    mc_role role = mc_role_none;
    moq_subscription_t sub = {};
    bool setup_done = false;
    bool sub_accepted = false;
    bool obj1_written = false;
    bool obj2_written = false;
};

struct mc_server_state {
    std::map<moq_mvfst_conn_t *, mc_per_conn> conns;
    bool role_a_assigned = false;
    bool role_b_assigned = false;

    std::atomic<bool> both_setup{false};
    std::atomic<bool> both_sub_ok{false};
    std::atomic<bool> may_write{false};
    std::atomic<bool> both_written{false};
    std::atomic<bool> may_close_a{false};
    std::atomic<bool> close_a_sent{false};
    std::atomic<bool> may_write_b2{false};
    std::atomic<bool> b2_written{false};
    std::atomic<bool> may_close_b{false};
    std::atomic<bool> close_b_sent{false};
    std::atomic<bool> error{false};
};

static int mc_server_pump(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    auto *ss = static_cast<mc_server_state *>(ctx);

    /* Collect live conns into a set. */
    std::set<moq_mvfst_conn_t *> live;
    moq_mvfst_conn_t *conn = nullptr;
    while ((conn = moq_mvfst_managed_next_conn(m, conn)) != nullptr)
        live.insert(conn);

    /* Prune stale entries. */
    for (auto it = ss->conns.begin(); it != ss->conns.end(); ) {
        if (live.find(it->first) == live.end())
            it = ss->conns.erase(it);
        else
            ++it;
    }

    /* Iterate live conns, assign roles, process events. */
    for (auto *c : live) {
        mc_per_conn &pc = ss->conns[c];

        /* Assign role on first encounter. */
        if (pc.role == mc_role_none) {
            if (!ss->role_a_assigned) {
                pc.role = mc_role_a;
                ss->role_a_assigned = true;
            } else if (!ss->role_b_assigned) {
                pc.role = mc_role_b;
                ss->role_b_assigned = true;
            }
        }

        moq_session_t *s = moq_mvfst_conn_session(c);
        if (!s) continue;

        moq_event_t ev[16]; size_t ne;
        moq_session_poll_events_ex(s, ev, 16, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE)
                pc.setup_done = true;
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !pc.sub_accepted) {
                auto &req = ev[i].u.subscribe_request;
                if (!ns_eq(req.track_namespace, "mvfst", "multi-client") ||
                    !bytes_eq(req.track_name, "video")) {
                    ss->error.store(true);
                } else {
                    pc.sub = req.sub;
                    moq_accept_subscribe_cfg_t acfg;
                    moq_accept_subscribe_cfg_init(&acfg);
                    moq_result_t rc = moq_session_accept_subscribe(
                        s, pc.sub, &acfg, 0);
                    if (rc < 0) ss->error.store(true);
                    else pc.sub_accepted = true;
                }
            }
            moq_event_cleanup(&ev[i]);
        }

        /* Write first object when may_write. */
        if (ss->may_write.load() && !pc.obj1_written && pc.sub_accepted) {
            const char *payload =
                (pc.role == mc_role_a) ? "client-a" : "client-b";
            size_t plen = std::strlen(payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 0;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, pc.sub,
                                           &sgcfg, 0, &sg) < 0) {
                ss->error.store(true);
            } else if (write_buf(s, sg, 0, payload, plen) < 0) {
                ss->error.store(true);
            } else if (moq_session_close_subgroup(s, sg, 0) < 0) {
                ss->error.store(true);
            } else {
                pc.obj1_written = true;
            }
        }

        /* Close role-a conn when may_close_a. */
        if (pc.role == mc_role_a && ss->may_close_a.load() &&
            !ss->close_a_sent.load()) {
            moq_mvfst_conn_close(c, 0);
            ss->close_a_sent.store(true);
        }

        /* Write second object to role-b when may_write_b2. */
        if (pc.role == mc_role_b && ss->may_write_b2.load() &&
            !pc.obj2_written && pc.sub_accepted) {
            const char *payload = "client-b-after-close";
            size_t plen = std::strlen(payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 1;
            sgcfg.publisher_priority = 200;
            moq_subgroup_handle_t sg;
            if (moq_session_open_subgroup(s, pc.sub,
                                           &sgcfg, 0, &sg) < 0) {
                ss->error.store(true);
            } else if (write_buf(s, sg, 0, payload, plen) < 0) {
                ss->error.store(true);
            } else if (moq_session_close_subgroup(s, sg, 0) < 0) {
                ss->error.store(true);
            } else {
                pc.obj2_written = true;
                ss->b2_written.store(true);
            }
        }

        /* Close role-b conn when may_close_b. */
        if (pc.role == mc_role_b && ss->may_close_b.load() &&
            !ss->close_b_sent.load()) {
            moq_mvfst_conn_close(c, 0);
            ss->close_b_sent.store(true);
        }
    }

    /* Update combined atomics. */
    {
        bool a_setup = false, b_setup = false;
        bool a_sub = false, b_sub = false;
        bool a_wr = false, b_wr = false;
        for (auto &kv : ss->conns) {
            if (kv.second.role == mc_role_a) {
                a_setup = kv.second.setup_done;
                a_sub = kv.second.sub_accepted;
                a_wr = kv.second.obj1_written;
            }
            if (kv.second.role == mc_role_b) {
                b_setup = kv.second.setup_done;
                b_sub = kv.second.sub_accepted;
                b_wr = kv.second.obj1_written;
            }
        }
        if (a_setup && b_setup) ss->both_setup.store(true);
        if (a_sub && b_sub) ss->both_sub_ok.store(true);
        if (a_wr && b_wr) ss->both_written.store(true);
    }

    return 0;
}

/* -- Wait helper ----------------------------------------------------- */

static bool has_error(moq_mvfst_managed_t *srv,
                       moq_mvfst_managed_t *cli_a,
                       moq_mvfst_managed_t *cli_b,
                       mc_server_state &ss,
                       mc_client_state &csa,
                       mc_client_state &csb)
{
    if (ss.error.load() || csa.error.load() || csb.error.load())
        return true;
    if (srv && moq_mvfst_managed_is_fatal(srv)) return true;
    if (cli_a && moq_mvfst_managed_is_fatal(cli_a)) return true;
    if (cli_b && moq_mvfst_managed_is_fatal(cli_b)) return true;
    return false;
}

static bool wait_mc(moq_mvfst_managed_t *srv,
                     moq_mvfst_managed_t *cli_a,
                     moq_mvfst_managed_t *cli_b,
                     mc_server_state &ss,
                     mc_client_state &csa,
                     mc_client_state &csb,
                     std::function<bool()> pred,
                     int ms_timeout = 5000)
{
    auto dl = std::chrono::steady_clock::now() +
              std::chrono::milliseconds(ms_timeout);
    while (std::chrono::steady_clock::now() < dl) {
        if (srv) moq_mvfst_managed_wake(srv);
        if (cli_a) moq_mvfst_managed_wake(cli_a);
        if (cli_b) moq_mvfst_managed_wake(cli_b);

        if (cli_a) moq_mvfst_managed_wait(cli_a, 30000);
        if (cli_b) moq_mvfst_managed_wait(cli_b, 30000);
        if (srv) moq_mvfst_managed_wait(srv, 30000);

        if (has_error(srv, cli_a, cli_b, ss, csa, csb))
            return false;
        if (pred()) return true;
    }
    if (has_error(srv, cli_a, cli_b, ss, csa, csb))
        return false;
    return pred();
}

/* ================================================================== */
/* Test: multi-client correctness                                     */
/* ================================================================== */

static void test_multi_client()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* --- Create managed server --- */
    mc_server_state ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = mc_server_pump;
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

    /* --- Create client A --- */
    mc_client_state csa;
    csa.ns1 = "mvfst";
    csa.ns2 = "multi-client";
    csa.track = "video";

    moq_mvfst_managed_cfg_t ccfg_a;
    moq_mvfst_managed_cfg_init_sized(&ccfg_a, sizeof(ccfg_a));
    ccfg_a.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg_a.host = "127.0.0.1";
    ccfg_a.port = port;
    ccfg_a.cert_path = tf.cert_path;
    ccfg_a.on_pump = mc_client_pump;
    ccfg_a.user_ctx = &csa;
    ccfg_a.send_request_capacity = true;
    ccfg_a.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli_a = nullptr;
    rc = moq_mvfst_managed_create(&ccfg_a, &cli_a);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_mvfst_managed_destroy(srv); return; }

    /* --- Create client B --- */
    mc_client_state csb;
    csb.ns1 = "mvfst";
    csb.ns2 = "multi-client";
    csb.track = "video";

    moq_mvfst_managed_cfg_t ccfg_b;
    moq_mvfst_managed_cfg_init_sized(&ccfg_b, sizeof(ccfg_b));
    ccfg_b.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg_b.host = "127.0.0.1";
    ccfg_b.port = port;
    ccfg_b.cert_path = tf.cert_path;
    ccfg_b.on_pump = mc_client_pump;
    ccfg_b.user_ctx = &csb;
    ccfg_b.send_request_capacity = true;
    ccfg_b.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli_b = nullptr;
    rc = moq_mvfst_managed_create(&ccfg_b, &cli_b);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) {
        moq_mvfst_managed_destroy(cli_a);
        moq_mvfst_managed_destroy(srv);
        return;
    }

    /* --- Phase 1: both clients setup, conn_count == 2 --- */
    MVFST_CHECK(wait_mc(srv, cli_a, cli_b, ss, csa, csb, [&]() {
        return csa.setup_complete.load() && csb.setup_complete.load() &&
               ss.both_setup.load() &&
               moq_mvfst_managed_conn_count(srv) == 2;
    }));
    MVFST_CHECK(csa.setup_complete.load());
    MVFST_CHECK(csb.setup_complete.load());
    MVFST_CHECK(ss.both_setup.load());
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 2);
    MVFST_CHECK(!ss.error.load());
    MVFST_CHECK(!csa.error.load());
    MVFST_CHECK(!csb.error.load());

    /* --- Phase 2: both subscribe --- */
    MVFST_CHECK(wait_mc(srv, cli_a, cli_b, ss, csa, csb, [&]() {
        return csa.sub_ok.load() && csb.sub_ok.load() &&
               ss.both_sub_ok.load();
    }));
    MVFST_CHECK(csa.sub_ok.load());
    MVFST_CHECK(csb.sub_ok.load());
    MVFST_CHECK(ss.both_sub_ok.load());
    MVFST_CHECK(!ss.error.load());

    /* --- Phase 3: distinct objects --- */
    ss.may_write.store(true);
    MVFST_CHECK(wait_mc(srv, cli_a, cli_b, ss, csa, csb, [&]() {
        std::lock_guard<std::mutex> lk_a(csa.result_mu);
        std::lock_guard<std::mutex> lk_b(csb.result_mu);
        return csa.objects.size() >= 1 && csb.objects.size() >= 1;
    }));

    std::vector<uint8_t> pa(
        (const uint8_t *)"client-a",
        (const uint8_t *)"client-a" + 8);
    std::vector<uint8_t> pb(
        (const uint8_t *)"client-b",
        (const uint8_t *)"client-b" + 8);

    /* Infer role mapping: which managed client got role A vs B. */
    moq_mvfst_managed_t *role_a_cli = nullptr;
    moq_mvfst_managed_t *role_b_cli = nullptr;
    mc_client_state *role_a_cs = nullptr;
    mc_client_state *role_b_cs = nullptr;
    {
        std::lock_guard<std::mutex> lk_a(csa.result_mu);
        std::lock_guard<std::mutex> lk_b(csb.result_mu);
        MVFST_CHECK(csa.objects.size() == 1);
        MVFST_CHECK(csb.objects.size() == 1);
        if (csa.objects.size() == 1 && csb.objects.size() == 1) {
            MVFST_CHECK(csa.objects[0].group_id == 0);
            MVFST_CHECK(csa.objects[0].object_id == 0);
            MVFST_CHECK(!csa.objects[0].is_datagram);
            MVFST_CHECK(csb.objects[0].group_id == 0);
            MVFST_CHECK(csb.objects[0].object_id == 0);
            MVFST_CHECK(!csb.objects[0].is_datagram);
            if (csa.objects[0].payload == pa) {
                MVFST_CHECK(csb.objects[0].payload == pb);
                role_a_cli = cli_a; role_a_cs = &csa;
                role_b_cli = cli_b; role_b_cs = &csb;
            } else {
                MVFST_CHECK(csa.objects[0].payload == pb);
                MVFST_CHECK(csb.objects[0].payload == pa);
                role_a_cli = cli_b; role_a_cs = &csb;
                role_b_cli = cli_a; role_b_cs = &csa;
            }
        }
    }
    MVFST_CHECK(role_a_cli != nullptr);
    MVFST_CHECK(role_b_cli != nullptr);
    if (!role_a_cli || !role_b_cli) {
        moq_mvfst_managed_destroy(cli_b);
        moq_mvfst_managed_destroy(cli_a);
        moq_mvfst_managed_destroy(srv);
        return;
    }
    MVFST_CHECK(!ss.error.load());

    /* --- Phase 4: close role A, conn_count drops to 1 --- */
    ss.may_close_a.store(true);
    MVFST_CHECK(wait_mc(srv, cli_a, cli_b, ss, csa, csb, [&]() {
        return moq_mvfst_managed_conn_count(srv) == 1;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 1);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(ss.close_a_sent.load());

    /* --- Phase 5: write second object to role B --- */
    ss.may_write_b2.store(true);
    MVFST_CHECK(wait_mc(srv, nullptr, role_b_cli, ss, csa, csb, [&]() {
        std::lock_guard<std::mutex> lk(role_b_cs->result_mu);
        return role_b_cs->objects.size() >= 2;
    }));
    {
        std::lock_guard<std::mutex> lk(role_a_cs->result_mu);
        MVFST_CHECK(role_a_cs->objects.size() == 1);
    }
    {
        std::lock_guard<std::mutex> lk(role_b_cs->result_mu);
        MVFST_CHECK(role_b_cs->objects.size() == 2);
        if (role_b_cs->objects.size() >= 2) {
            auto &o = role_b_cs->objects[1];
            MVFST_CHECK(o.group_id == 1);
            MVFST_CHECK(o.object_id == 0);
            MVFST_CHECK(!o.is_datagram);
            std::vector<uint8_t> exp_b2(
                (const uint8_t *)"client-b-after-close",
                (const uint8_t *)"client-b-after-close" + 20);
            MVFST_CHECK(o.payload == exp_b2);
        }
    }
    MVFST_CHECK(!ss.error.load());

    /* --- Phase 6: close role B, conn_count == 0 --- */
    ss.may_close_b.store(true);
    MVFST_CHECK(wait_mc(srv, nullptr, nullptr, ss, csa, csb, [&]() {
        return moq_mvfst_managed_conn_count(srv) == 0;
    }));
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 0);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(!ss.error.load());

    /* --- Clean shutdown --- */
    moq_mvfst_managed_destroy(cli_b);
    moq_mvfst_managed_destroy(cli_a);
    moq_mvfst_managed_destroy(srv);
}

/* ================================================================== */
/* Test: server connection cap (max_connections)                      */
/* ================================================================== */
/*
 * With max_connections = 1 the server retains exactly one accepted
 * connection. A first client connects and completes a full setup +
 * subscribe round-trip; a second client's connection is rejected at the
 * accept factory (no per-connection session/adapter allocated), so the
 * server's conn_count never exceeds 1 and the second client never reaches
 * SETUP_COMPLETE, while the first connection stays fully usable.
 *
 * Without the cap, the second client is accepted and
 * conn_count reaches 2 (and its setup completes) before the first client
 * finishes its subscribe round-trip, so the conn_count==1 / !setup_complete
 * assertions below fail.
 */
static void test_max_connections_cap()
{
    auto cm = gen_cert();
    MVFST_CHECK(cm.ok);
    if (!cm.ok) return;
    tmp_files tf(cm);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;

    /* --- Managed server with a cap of one connection --- */
    mc_server_state ss;
    moq_mvfst_managed_cfg_t scfg;
    moq_mvfst_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.port = 0;
    scfg.cert_path = tf.cert_path;
    scfg.key_path = tf.key_path;
    scfg.on_pump = mc_server_pump;
    scfg.user_ctx = &ss;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_connections = 1;

    moq_mvfst_managed_t *srv = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&scfg, &srv);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;

    uint16_t port = moq_mvfst_managed_local_port(srv);
    MVFST_CHECK(port != 0);
    if (port == 0) { moq_mvfst_managed_destroy(srv); return; }

    /* --- Client A --- */
    mc_client_state csa;
    csa.ns1 = "mvfst"; csa.ns2 = "multi-client"; csa.track = "video";
    moq_mvfst_managed_cfg_t ca;
    moq_mvfst_managed_cfg_init_sized(&ca, sizeof(ca));
    ca.perspective = MOQ_PERSPECTIVE_CLIENT;
    ca.host = "127.0.0.1";
    ca.port = port;
    ca.cert_path = tf.cert_path;
    ca.on_pump = mc_client_pump;
    ca.user_ctx = &csa;
    ca.send_request_capacity = true;
    ca.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli_a = nullptr;
    rc = moq_mvfst_managed_create(&ca, &cli_a);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_mvfst_managed_destroy(srv); return; }

    /* Client B state declared up front so wait_mc can reference it even
     * before the second managed client exists. */
    mc_client_state csb;
    csb.ns1 = "mvfst"; csb.ns2 = "multi-client"; csb.track = "video";

    /* --- Phase 1: A connects, conn_count == 1 (B not yet created) --- */
    MVFST_CHECK(wait_mc(srv, cli_a, nullptr, ss, csa, csb, [&]() {
        return csa.setup_complete.load() &&
               moq_mvfst_managed_conn_count(srv) == 1;
    }));
    MVFST_CHECK(csa.setup_complete.load());
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 1);

    /* --- Client B (must be rejected by the cap) --- */
    moq_mvfst_managed_cfg_t cb;
    moq_mvfst_managed_cfg_init_sized(&cb, sizeof(cb));
    cb.perspective = MOQ_PERSPECTIVE_CLIENT;
    cb.host = "127.0.0.1";
    cb.port = port;
    cb.cert_path = tf.cert_path;
    cb.on_pump = mc_client_pump;
    cb.user_ctx = &csb;
    cb.send_request_capacity = true;
    cb.initial_request_capacity = 16;

    moq_mvfst_managed_t *cli_b = nullptr;
    rc = moq_mvfst_managed_create(&cb, &cli_b);
    MVFST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) {
        moq_mvfst_managed_destroy(cli_a);
        moq_mvfst_managed_destroy(srv);
        return;
    }

    /* --- Phase 2: A completes a full subscribe round-trip while B's own
     * network thread autonomously attempts to connect. cli_b is deliberately
     * NOT passed to wait_mc: B's connection is expected to fail (rejected at
     * the cap), and a rejected client goes fatal -- which wait_mc would treat
     * as a global error and abort. B still drives its connection attempts from
     * its own thread. By the time A reaches sub_ok the server has pumped
     * through multiple RTTs; an accepted B would have raised conn_count to 2
     * and completed its own setup by now. --- */
    MVFST_CHECK(wait_mc(srv, cli_a, nullptr, ss, csa, csb, [&]() {
        return csa.sub_ok.load();
    }));
    MVFST_CHECK(csa.sub_ok.load());

    /* The cap held: B was rejected at accept, never retained or set up. */
    MVFST_CHECK(moq_mvfst_managed_conn_count(srv) == 1);
    MVFST_CHECK(!csb.setup_complete.load());
    /* A remains usable and nothing went fatal. */
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(srv));
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(cli_a));
    MVFST_CHECK(!ss.error.load());
    MVFST_CHECK(!csa.error.load());

    moq_mvfst_managed_destroy(cli_b);
    moq_mvfst_managed_destroy(cli_a);
    moq_mvfst_managed_destroy(srv);
}

int main()
{
    test_multi_client();
    test_max_connections_cap();
    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
