/*
 * Managed EventBase lifetime: an external call into a published EventBase
 * never overlaps that EventBase's retirement and destruction.
 *
 * A managed facade publishes its client EventBase (client mode) or the
 * QuicServer worker EventBase (server mode) so that application threads can
 * enqueue a pump, ask the loop to terminate, or ask whether they run on the
 * loop thread. The owner destroys that EventBase when its loop ends -- on an
 * application stop, or on its own when the lane callback requests shutdown.
 * An application call that has already obtained the EventBase must finish
 * before the owner may destroy it.
 *
 * Each race case pauses one such call right after it obtained the EventBase
 * (the test-internals lease probe), makes the owner retire it independently
 * (the lane callback returns nonzero), and lets the paused call finish only
 * once the owner has begun retirement. The probe keeps its own count of
 * granted borrows. Retirement that does not wait for a granted borrow is the
 * defect. The observer records it at either end of retirement -- when it
 * begins without the borrow counted, or when it ends with the borrow still
 * outstanding -- and holds the owner there, before any destruction, until it
 * sees that borrow end. Completed retirement is published only after that.
 * The paused call therefore always finishes on a live EventBase, and no
 * freed memory is touched.
 *
 * Every rendezvous the test's safety depends on is bounded. One that does not
 * happen in time, or a prerequisite that fails (such as creating the paused
 * borrower's thread), ends the process at once (FATAL, exit 3) instead of
 * opening a gate whose condition is unproved.
 *
 * A negative case ends as soon as it has shown the failure it exists to show,
 * at a point where going on would run teardown this process must not run: an
 * ownership violation once every borrow it concerned has ended (before the
 * owner returns into its teardown), and incomplete cleanup once a witness is
 * positively still pending on the EventBase about to be destroyed (before its
 * destruction). It prints that one failed check and exits 1.
 *
 * The two cleanup cases also check what the client EventBase owes before it
 * is destroyed. Teardown closes the transport, which leaves its drain work
 * (callbacks and a drain timer) on the EventBase; the facade's final drain has
 * to run that work while the EventBase and its backend are alive. Each case
 * leaves a witness timer on the EventBase when that drain begins (the private
 * drain_begin probe) and requires it to expire, with the backend alive, before
 * the facade starts destroying the EventBase (destroy_begin). A stop that
 * reached the EventBase after its loop exited (client-notify's schedule) is
 * still pending at that drain; the other case has none.
 *
 * One case per process (one QuicServer per process):
 *   client-enqueue   application wake (pump enqueue) vs the loop ending
 *   client-notify    application stop (loop termination) vs the loop ending
 *   client-drain     client-enqueue, the wake kept paused until retirement has
 *                    ended or the drain window has closed
 *   client-live      ordinary stop of a live facade, then a repeated stop
 *   client-after     stop after the loop ended on its own
 *   server-enqueue   application wake vs teardown after the lane callback ends
 *   server-thread    application stop's thread check vs that teardown
 *   client-notify-cleanup  client-notify, plus the cleanup witness
 *   client-live-cleanup    client-live (no stop after the loop exits), plus the
 *                    cleanup witness
 *   client-drain-error     client-live with the final drain failing as an
 *                    event-loop error would, before it runs (the transport's
 *                    drain still pending): the failure is recorded, and the
 *                    transport is gone before the EventBase is destroyed
 *   client-early-setup     the client thread's exit before its loop on a setup
 *                    failure (a CA file with no certificate): the same final
 *                    drain and cleanup witness, no EventBase published
 *   client-early-keepalive the test-build exit after the transport is
 *                    installed and before start() (the keepalive probe): the
 *                    same final drain and cleanup witness, the transport gone
 *                    before the EventBase is destroyed, no EventBase published
 *
 * The cleanup cases also require the closed transport itself to be gone when
 * the EventBase's destruction begins: it owns itself until its drain is done,
 * so its absence there shows that its work ran while the EventBase lived.
 */
#include <moq/mvfst.h>

#include "../src/mvfst_managed_testing.h"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/HHWheelTimer.h>
#include <folly/ssl/OpenSSLPtrTypes.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static constexpr auto WAIT = std::chrono::seconds(10);

/* How long a paused external call may take to return once released. A stop
 * joins the network thread, which first runs the closed transport's drain
 * (its QUIC drain period), so this is longer than WAIT and shorter than the
 * case's CTest timeout. */
static constexpr std::chrono::seconds JOIN_WAIT(40);

/* How long client-drain keeps the wake paused for an owner that does not wait
 * for its borrows to reach the end of retirement. A lease that drains its
 * borrows cannot get there while the wake is paused, however it is
 * scheduled; the window only bounds how long the case looks. */
static constexpr auto DRAIN_WINDOW = std::chrono::seconds(2);

/* A step the test's safety depends on did not happen. No participant may go
 * on: the process ends here, without running any further test code. */
[[noreturn]] static void die(const char *what)
{
    std::fprintf(stderr, "FATAL: %s\n", what);
    std::fflush(stderr);
    std::_Exit(3);
}

/* The same for a rendezvous that did not happen within its bound. */
[[noreturn]] static void fatal(const char *what,
                               std::chrono::seconds bound = WAIT)
{
    std::fprintf(stderr, "FATAL: %s within %lld s\n", what,
                 static_cast<long long>(bound.count()));
    std::fflush(stderr);
    std::_Exit(3);
}

/* A negative case has shown the one failure it exists to show; report that
 * check and end the process before any teardown it must not run. */
#define NEGATIVE_EXIT(expr) do { \
    std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, (expr)); \
    std::fflush(stderr); \
    std::_Exit(1); \
} while (0)

/* -- Lease observer ----------------------------------------------------- */

struct observer {
    std::mutex mu;
    std::condition_variable cv;
    /* The one borrow to pause. */
    int pause_evb = 0, pause_site = 0;
    bool armed = false, paused = false, released = false, paused_ended = false;
    std::thread::id paused_tid;
    /* Accounting per lease (index MOQ_MVFST_TEST_EVB_*). */
    int outstanding[3] = {0, 0, 0};
    int granted[3][4] = {};
    int granted_after_retire[3] = {0, 0, 0};
    bool retire_began[3] = {false, false, false};
    /* Retirement ended with a borrow outstanding; the owner is held there. */
    bool final_held[3] = {false, false, false};
    bool retired[3] = {false, false, false};
    int violations[3] = {0, 0, 0};
};
static observer g_obs;

static bool on_borrowed(void *, int evb, int site)
{
    std::unique_lock<std::mutex> lk(g_obs.mu);
    g_obs.outstanding[evb]++;
    g_obs.granted[evb][site]++;
    if (g_obs.retire_began[evb]) g_obs.granted_after_retire[evb]++;
    if (g_obs.armed && evb == g_obs.pause_evb && site == g_obs.pause_site) {
        g_obs.armed = false;
        g_obs.paused = true;
        g_obs.paused_tid = std::this_thread::get_id();
        g_obs.cv.notify_all();
        if (!g_obs.cv.wait_for(lk, WAIT, [] { return g_obs.released; }))
            fatal("the paused borrow was not released");
        g_obs.paused = false;
    }
    return true;
}

static void on_borrow_end(void *, int evb, int site)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    g_obs.outstanding[evb]--;
    if (evb == g_obs.pause_evb && site == g_obs.pause_site &&
        g_obs.paused_tid == std::this_thread::get_id())
        g_obs.paused_ended = true;
    g_obs.cv.notify_all();
}

/* Hold the owner, before it can destroy anything, until the observer has seen
 * every outstanding borrow of the lease end. */
static void hold_owner(std::unique_lock<std::mutex> &lk, int evb)
{
    if (!g_obs.cv.wait_for(lk, WAIT, [evb] { return g_obs.outstanding[evb] == 0; }))
        fatal("a borrow the owner is held for did not end");
}

static void on_retire_begin(void *, int evb, unsigned granted)
{
    std::unique_lock<std::mutex> lk(g_obs.mu);
    g_obs.retire_began[evb] = true;
    g_obs.cv.notify_all();
    if (g_obs.outstanding[evb] > 0 && granted == 0) {
        /* The lease does not count a borrow that is outstanding, so it will
         * not wait for it: the overlap. */
        g_obs.violations[evb]++;
        std::fprintf(stderr, "VIOLATION: lease %d retiring with %d granted "
                     "borrow(s) it does not wait for\n", evb, g_obs.outstanding[evb]);
        hold_owner(lk, evb);
        /* Every borrow it did not wait for has ended. Returning would let the
         * owner run its teardown, which in this negative is not safe. */
        NEGATIVE_EXIT("g_obs.violations[evb] == 0");
    }
}

static void on_retired(void *, int evb)
{
    std::unique_lock<std::mutex> lk(g_obs.mu);
    if (g_obs.outstanding[evb] > 0) {
        /* Retirement ended without draining a borrow it counted: the same
         * overlap, seen at its other end. */
        g_obs.violations[evb]++;
        std::fprintf(stderr, "VIOLATION: lease %d finished retiring with %d "
                     "granted borrow(s) still outstanding\n", evb,
                     g_obs.outstanding[evb]);
        g_obs.final_held[evb] = true;
        g_obs.cv.notify_all();
        hold_owner(lk, evb);
        NEGATIVE_EXIT("g_obs.violations[evb] == 0");
    }
    /* Published only once no borrow is outstanding. */
    g_obs.retired[evb] = true;
    g_obs.cv.notify_all();
}

/* -- Cleanup witness ------------------------------------------------------ */

/* A timer left on the client EventBase when its final drain begins, as the
 * closed transport leaves its own drain timer. A cancellation (the EventBase
 * destructor cancels timers still pending) is only recorded: nothing touches
 * the EventBase then. */
struct cleanup_witness final : folly::HHWheelTimer::Callback {
    folly::EventBase *evb = nullptr;
    bool expired = false, backend_alive = false, before_destroy = false,
         canceled = false;
    void timeoutExpired() noexcept override;
    void callbackCanceled() noexcept override;
};

struct cleanup_state {
    bool armed = false, drain_begun = false, destroy_begun = false;
    bool fail_drain = false;              /* the final drain fails (error case) */
    bool transport_released = false;      /* the transport was gone at destroy_begin */
    cleanup_witness witness;
};
static cleanup_state g_cleanup;      /* guarded by g_obs.mu */

void cleanup_witness::timeoutExpired() noexcept
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    expired = true;
    backend_alive = evb && evb->getBackend() != nullptr;
    before_destroy = !g_cleanup.destroy_begun;
}

void cleanup_witness::callbackCanceled() noexcept
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    canceled = true;
}

static void on_drain_begin(void *, void *eventbase)
{
    auto *e = static_cast<folly::EventBase *>(eventbase);
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        if (!g_cleanup.armed || g_cleanup.drain_begun) return;
        g_cleanup.drain_begun = true;
        g_cleanup.witness.evb = e;
    }
    /* Owner thread, with the loop not running: the witness belongs to the
     * drain that follows. The error case's evidence is the transport itself,
     * so it leaves no witness. */
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        if (g_cleanup.fail_drain) return;
    }
    e->timer().scheduleTimeout(&g_cleanup.witness, std::chrono::milliseconds(1));
}

static bool on_drain_fails(void *)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    return g_cleanup.armed && g_cleanup.fail_drain;
}

/* Owner thread, the EventBase and its backend still alive. A witness still
 * scheduled on it, or the closed transport still alive, is positive evidence
 * that the drain left work for the destructor: the failure this negative
 * shows, reported before that destruction runs. */
static void on_destroy_begin(void *, void *eventbase, bool transport_released)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    if (!g_cleanup.armed) return;
    g_cleanup.destroy_begun = true;
    g_cleanup.transport_released = transport_released;
    if (!g_cleanup.drain_begun)
        die("the final drain did not begin before EventBase destruction");
    if (g_cleanup.fail_drain) {
        if (!transport_released) {
            std::fprintf(stderr, "INCOMPLETE: transport alive at EventBase destruction: "
                         "released=0\n");
            NEGATIVE_EXIT("transport_released_before_destroy()");
        }
        return;
    }
    cleanup_witness &w = g_cleanup.witness;
    if (w.evb != eventbase)
        die("the cleanup witness was not queued on the EventBase being destroyed");
    if (w.isScheduled()) {
        std::fprintf(stderr, "INCOMPLETE: cleanup pending at EventBase destruction: "
                     "witness scheduled=1 expired=%d canceled=%d\n", w.expired, w.canceled);
        NEGATIVE_EXIT("cleanup_ran_while_alive()");
    }
    if (!w.expired && !w.canceled)
        die("the cleanup witness neither ran nor is pending at EventBase destruction");
}

static void arm_cleanup(bool fail_drain)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    g_cleanup.armed = true;
    g_cleanup.fail_drain = fail_drain;
}

/* The work the final drain owes ran while the EventBase and its backend were
 * alive, before the facade began destroying the EventBase. */
static bool cleanup_ran_while_alive()
{
    const cleanup_witness &w = g_cleanup.witness;
    return w.expired && w.backend_alive && w.before_destroy && !w.canceled;
}

/* After a failed drain: the failure is recorded, and the closed transport,
 * which owns itself until its drain is done, was gone before the EventBase's
 * destruction began. */
static void check_cleanup_error(moq_mvfst_managed_t *m)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    MVFST_CHECK(g_cleanup.drain_begun);
    MVFST_CHECK(g_cleanup.destroy_begun);
    MVFST_CHECK(g_cleanup.transport_released);
    MVFST_CHECK(moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_fatal_code(m) == 0x1);
}

/* After stop() has joined the network thread: every owner-thread step,
 * EventBase destruction included, has happened. */
static void check_cleanup()
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    MVFST_CHECK(g_cleanup.drain_begun);
    MVFST_CHECK(g_cleanup.destroy_begun);
    MVFST_CHECK(g_cleanup.transport_released);
    if (!cleanup_ran_while_alive()) {
        const cleanup_witness &w = g_cleanup.witness;
        std::fprintf(stderr, "INCOMPLETE: cleanup witness expired=%d backend_alive=%d "
                     "before_destroy=%d canceled=%d\n", w.expired, w.backend_alive,
                     w.before_destroy, w.canceled);
    }
    MVFST_CHECK(cleanup_ran_while_alive());
}

static const moq_mvfst_test_evb_probe g_probe = {
    nullptr, on_borrowed, on_borrow_end, on_retire_begin, on_retired,
    on_drain_begin, on_drain_fails, on_destroy_begin
};

template <class Pred> static void require_obs(Pred pred, const char *what)
{
    std::unique_lock<std::mutex> lk(g_obs.mu);
    if (!g_obs.cv.wait_for(lk, WAIT, pred)) fatal(what);
}

/* -- Lane callback: counted, blockable, and able to request shutdown ---- */

static std::mutex g_mu;
static std::condition_variable g_cv;
static int g_pumps = 0;
static bool g_block = false, g_in_block = false, g_unblock = false, g_exit = false;

static int gated_pump(moq_mvfst_managed_t *, moq_mvfst_managed_lane_t *,
                      uint64_t, void *)
{
    std::unique_lock<std::mutex> lk(g_mu);
    ++g_pumps;
    g_cv.notify_all();
    if (g_block) {
        g_in_block = true;
        g_cv.notify_all();
        if (!g_cv.wait_for(lk, WAIT, [] { return g_unblock; }))
            fatal("the held lane callback was not released");
        g_block = g_in_block = g_unblock = false;
    }
    return g_exit ? 1 : 0;   /* nonzero: the documented clean-shutdown request */
}

template <class Pred> static void require_pump(Pred pred, const char *what)
{
    std::unique_lock<std::mutex> lk(g_mu);
    if (!g_cv.wait_for(lk, WAIT, pred)) fatal(what);
}

/* Hold the network thread inside one lane callback, so that no pump and no
 * transport callback runs until unblock_pump(). */
static void block_pump(moq_mvfst_managed_t *m)
{
    {
        std::lock_guard<std::mutex> lk(g_mu);
        g_block = true;
    }
    if (moq_mvfst_managed_wake(m) != MOQ_OK)
        die("the wake that holds the lane callback was refused");
    require_pump([] { return g_in_block; }, "the lane callback was not held");
}

static void unblock_pump(bool exit_after)
{
    std::lock_guard<std::mutex> lk(g_mu);
    g_exit = exit_after;
    g_unblock = true;
    g_cv.notify_all();
}

/* -- Facades ------------------------------------------------------------ */

/* A UDP socket the client targets: it never answers, so the client's
 * EventBase loop runs with no handshake progress or traffic of note. */
struct silent_peer {
    int fd = -1;
    int port = 0;
    silent_peer() {
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) return;
        struct sockaddr_in a = {};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(a);
        if (bind(fd, reinterpret_cast<struct sockaddr *>(&a), sizeof(a)) != 0 ||
            getsockname(fd, reinterpret_cast<struct sockaddr *>(&a), &len) != 0) {
            close(fd);
            fd = -1;
            return;
        }
        port = ntohs(a.sin_port);
    }
    ~silent_peer() { if (fd >= 0) close(fd); }
};

static moq_mvfst_managed_t *create_client(int port)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = gated_pump;
    /* Slow loss recovery: the unanswered handshake does not end the
     * connection while a case runs. */
    cfg.initial_rtt_us = 2000000;
    cfg.max_num_ptos = 10;
    moq_mvfst_managed_t *m = nullptr;
    if (moq_mvfst_managed_create(&cfg, &m) != MOQ_OK) return nullptr;
    return m;
}

struct test_cert { std::string cert_pem, key_pem; bool ok = false; };

static test_cert gen_server_cert()
{
    test_cert r;
    folly::ssl::EvpPkeyUniquePtr pkey;
    {
        EVP_PKEY_CTX *kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
        EVP_PKEY *raw = nullptr;
        bool keyed = kctx && EVP_PKEY_keygen_init(kctx) > 0 &&
            EVP_PKEY_CTX_set_ec_paramgen_curve_nid(kctx, NID_X9_62_prime256v1) > 0 &&
            EVP_PKEY_keygen(kctx, &raw) > 0;
        EVP_PKEY_CTX_free(kctx);
        if (!keyed) { EVP_PKEY_free(raw); return r; }
        pkey.reset(raw);
    }
    auto x509 = folly::ssl::X509UniquePtr(X509_new());
    if (!x509) return r;
    if (!ASN1_INTEGER_set(X509_get_serialNumber(x509.get()), 1)) return r;
    if (!X509_gmtime_adj(X509_get_notBefore(x509.get()), 0)) return r;
    if (!X509_gmtime_adj(X509_get_notAfter(x509.get()), 365 * 24 * 3600)) return r;
    if (!X509_set_pubkey(x509.get(), pkey.get())) return r;
    auto *name = X509_get_subject_name(x509.get());
    if (!X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char *>("localhost"), -1, -1, 0))
        return r;
    if (!X509_set_issuer_name(x509.get(), name)) return r;
    if (!X509_sign(x509.get(), pkey.get(), EVP_sha256())) return r;
    auto cb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!cb || !PEM_write_bio_X509(cb.get(), x509.get())) return r;
    char *cd = nullptr;
    auto cl = BIO_get_mem_data(cb.get(), &cd);
    if (cl <= 0 || !cd) return r;
    r.cert_pem.assign(cd, cl);
    auto kb = folly::ssl::BioUniquePtr(BIO_new(BIO_s_mem()));
    if (!kb || !PEM_write_bio_PrivateKey(kb.get(), pkey.get(),
            nullptr, nullptr, 0, nullptr, nullptr)) return r;
    char *kd = nullptr;
    auto kl = BIO_get_mem_data(kb.get(), &kd);
    if (kl <= 0 || !kd) return r;
    r.key_pem.assign(kd, kl);
    r.ok = true;
    return r;
}

struct tmp_files {
    char cert_path[64] = "/tmp/moq_evb_cert_XXXXXX";
    char key_path[64] = "/tmp/moq_evb_key_XXXXXX";
    bool ok = false;
    explicit tmp_files(const test_cert &c) {
        int cfd = mkstemp(cert_path);
        if (cfd < 0) return;
        ssize_t cw = write(cfd, c.cert_pem.data(), c.cert_pem.size());
        close(cfd);
        if (cw != static_cast<ssize_t>(c.cert_pem.size())) { unlink(cert_path); return; }
        int kfd = mkstemp(key_path);
        if (kfd < 0) { unlink(cert_path); return; }
        ssize_t kw = write(kfd, c.key_pem.data(), c.key_pem.size());
        close(kfd);
        if (kw != static_cast<ssize_t>(c.key_pem.size())) {
            unlink(cert_path); unlink(key_path); return;
        }
        ok = true;
    }
    ~tmp_files() { if (ok) { unlink(cert_path); unlink(key_path); } }
};

static moq_mvfst_managed_t *create_server(const tmp_files &tf)
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = tf.cert_path;
    cfg.key_path = tf.key_path;
    cfg.on_lane_pump = gated_pump;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    moq_mvfst_managed_t *m = nullptr;
    if (moq_mvfst_managed_create(&cfg, &m) != MOQ_OK) return nullptr;
    return m;
}

/* -- The paused external call ------------------------------------------- */

struct borrower {
    std::thread thread;
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    moq_result_t result = MOQ_ERR_INTERNAL;

    template <class Fn> bool start(Fn fn) {
        try {
            thread = std::thread([this, fn]() {
                moq_result_t rc = fn();
                std::lock_guard<std::mutex> lk(mu);
                result = rc;
                done = true;
                cv.notify_all();
            });
        } catch (const std::system_error &) {
            return false;
        }
        return true;
    }

    /* Join once it has finished; a call still running after JOIN_WAIT cannot
     * be joined safely, so the process ends there. */
    void finish() {
        if (!thread.joinable()) return;
        std::unique_lock<std::mutex> lk(mu);
        if (!cv.wait_for(lk, JOIN_WAIT, [this] { return done; }))
            fatal("the external call did not return", JOIN_WAIT);
        lk.unlock();
        thread.join();
    }
};

/* The race common to every paused case. The borrower is paused after its
 * borrow was granted; the owner then retires the EventBase on its own
 * (unblock_pump(true): the lane callback requests shutdown). Once the owner
 * has begun retirement -- or, with drain, once retirement has ended or the
 * drain window has closed -- the borrower is released and uses the
 * EventBase: alive, because correct retirement waits for it, and an
 * overlapping one is held by the observer before it destroys anything. */
static void run_race(int evb, int site, bool drain)
{
    require_obs([] { return g_obs.paused; }, "the armed borrow did not pause");
    unblock_pump(true);
    require_obs([evb] { return g_obs.retire_began[evb]; },
                "the owner did not begin retirement");
    std::unique_lock<std::mutex> lk(g_obs.mu);
    if (drain)
        (void)g_obs.cv.wait_for(lk, DRAIN_WINDOW,
                                [evb] { return g_obs.final_held[evb]; });
    /* Retirement cannot be complete while the paused borrow is granted. */
    if (g_obs.retired[evb]) die("retirement completed while the paused borrow was granted");
    if (g_obs.granted[evb][site] < 1) die("the paused borrow was not counted as granted");
    g_obs.released = true;
    g_obs.cv.notify_all();
    lk.unlock();
    require_obs([] { return g_obs.paused_ended; }, "the released borrow did not end");
    require_obs([evb] { return g_obs.retired[evb]; },
                "retirement did not complete after the borrow ended");
}

static void arm(int evb, int site)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    g_obs.pause_evb = evb;
    g_obs.pause_site = site;
    g_obs.armed = true;
}

static void check_settled(int evb)
{
    std::lock_guard<std::mutex> lk(g_obs.mu);
    MVFST_CHECK(g_obs.violations[evb] == 0);
    MVFST_CHECK(g_obs.outstanding[evb] == 0);
    MVFST_CHECK(g_obs.granted_after_retire[evb] == 0);
    MVFST_CHECK(!g_obs.armed);
}

/* -- Cases -------------------------------------------------------------- */

static void case_client_race(int site, bool drain, bool cleanup)
{
    silent_peer peer;
    MVFST_CHECK(peer.fd >= 0);
    if (peer.fd < 0) return;
    if (cleanup) arm_cleanup(false);
    moq_mvfst_test_set_evb_probe(&g_probe);
    moq_mvfst_managed_t *m = create_client(peer.port);
    MVFST_CHECK(m != nullptr);
    if (!m) { moq_mvfst_test_set_evb_probe(nullptr); return; }
    require_pump([] { return g_pumps >= 1; }, "the first pump did not run");
    block_pump(m);
    arm(MOQ_MVFST_TEST_EVB_CLIENT, site);
    borrower b;
    bool started = site == MOQ_MVFST_TEST_EVB_ENQUEUE
        ? b.start([m]() { return moq_mvfst_managed_wake(m); })
        : b.start([m]() { return moq_mvfst_managed_stop(m); });
    /* Without the paused borrower there is no safe way on: any further call
     * (a stop included) would be an unpaused borrow racing retirement. */
    if (!started) die("the borrower thread could not be created");
    run_race(MOQ_MVFST_TEST_EVB_CLIENT, site, drain);
    b.finish();
    MVFST_CHECK(b.result == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    check_settled(MOQ_MVFST_TEST_EVB_CLIENT);
    if (cleanup) check_cleanup();
    moq_mvfst_managed_destroy(m);
    moq_mvfst_test_set_evb_probe(nullptr);
}

static void case_client_live(bool cleanup, bool fail_drain)
{
    silent_peer peer;
    MVFST_CHECK(peer.fd >= 0);
    if (peer.fd < 0) return;
    if (cleanup) arm_cleanup(fail_drain);
    moq_mvfst_test_set_evb_probe(&g_probe);
    moq_mvfst_managed_t *m = create_client(peer.port);
    MVFST_CHECK(m != nullptr);
    if (!m) { moq_mvfst_test_set_evb_probe(nullptr); return; }
    require_pump([] { return g_pumps >= 1; }, "the first pump did not run");
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    if (fail_drain) check_cleanup_error(m);
    else if (cleanup) check_cleanup();
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        /* The stop reached the live loop through one granted borrow, and
         * retirement followed it. */
        MVFST_CHECK(g_obs.granted[MOQ_MVFST_TEST_EVB_CLIENT][MOQ_MVFST_TEST_EVB_NOTIFY] == 1);
        MVFST_CHECK(g_obs.retired[MOQ_MVFST_TEST_EVB_CLIENT]);
    }
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);    /* repeated stop/join */
    check_settled(MOQ_MVFST_TEST_EVB_CLIENT);
    moq_mvfst_managed_destroy(m);
    moq_mvfst_test_set_evb_probe(nullptr);
}

static void case_client_after()
{
    silent_peer peer;
    MVFST_CHECK(peer.fd >= 0);
    if (peer.fd < 0) return;
    moq_mvfst_test_set_evb_probe(&g_probe);
    moq_mvfst_managed_t *m = create_client(peer.port);
    MVFST_CHECK(m != nullptr);
    if (!m) { moq_mvfst_test_set_evb_probe(nullptr); return; }
    require_pump([] { return g_pumps >= 1; }, "the first pump did not run");
    {
        std::lock_guard<std::mutex> lk(g_mu);
        g_exit = true;
    }
    MVFST_CHECK(moq_mvfst_managed_wake(m) == MOQ_OK);
    require_obs([] { return g_obs.retired[MOQ_MVFST_TEST_EVB_CLIENT]; },
                "the loop did not end on its own");
    int notify_before;
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        notify_before = g_obs.granted[MOQ_MVFST_TEST_EVB_CLIENT][MOQ_MVFST_TEST_EVB_NOTIFY];
    }
    /* The loop ended on its own: neither a wake nor a stop obtains the
     * retired EventBase. */
    (void)moq_mvfst_managed_wake(m);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        MVFST_CHECK(g_obs.granted[MOQ_MVFST_TEST_EVB_CLIENT][MOQ_MVFST_TEST_EVB_NOTIFY] ==
              notify_before);
    }
    check_settled(MOQ_MVFST_TEST_EVB_CLIENT);
    moq_mvfst_managed_destroy(m);
    moq_mvfst_test_set_evb_probe(nullptr);
}

static void case_server_race(int site)
{
    auto cert = gen_server_cert();
    MVFST_CHECK(cert.ok);
    if (!cert.ok) return;
    tmp_files tf(cert);
    MVFST_CHECK(tf.ok);
    if (!tf.ok) return;
    moq_mvfst_test_set_evb_probe(&g_probe);
    moq_mvfst_managed_t *m = create_server(tf);
    MVFST_CHECK(m != nullptr);
    if (!m) { moq_mvfst_test_set_evb_probe(nullptr); return; }
    require_pump([] { return g_pumps >= 1; }, "the first pump did not run");
    block_pump(m);
    arm(MOQ_MVFST_TEST_EVB_SERVER, site);
    borrower b;
    bool started = site == MOQ_MVFST_TEST_EVB_ENQUEUE
        ? b.start([m]() { return moq_mvfst_managed_wake(m); })
        : b.start([m]() { return moq_mvfst_managed_stop(m); });
    /* Without the paused borrower there is no safe way on: any further call
     * (a stop included) would be an unpaused borrow racing retirement. */
    if (!started) die("the borrower thread could not be created");
    run_race(MOQ_MVFST_TEST_EVB_SERVER, site, false);
    b.finish();
    MVFST_CHECK(b.result == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    check_settled(MOQ_MVFST_TEST_EVB_SERVER);
    moq_mvfst_managed_destroy(m);
    moq_mvfst_test_set_evb_probe(nullptr);
}

/* The client thread's two exits before its loop runs. Each closes what it
 * created and drains the client EventBase through the final drain before
 * destroying it: the cleanup witness expires with the backend alive and before
 * destruction begins, and the transport is gone by then. Neither exit
 * publishes the EventBase, so no borrow is granted and none is retired. */
static void case_client_early(bool keepalive)
{
    silent_peer peer;
    MVFST_CHECK(peer.fd >= 0);
    if (peer.fd < 0) return;
    /* An empty CA file: the client's verifier setup cannot load a
     * certificate from it, which ends the thread's setup. */
    char ca_path[64] = "/tmp/moq_evb_early_ca_XXXXXX";
    if (!keepalive) {
        int cfd = mkstemp(ca_path);
        MVFST_CHECK(cfd >= 0);
        if (cfd < 0) return;
        close(cfd);
    }
    moq_mvfst_keepalive_probe installed{};
    if (keepalive) moq_mvfst_managed_test_set_keepalive_probe(&installed);
    arm_cleanup(false);
    moq_mvfst_test_set_evb_probe(&g_probe);

    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = peer.port;
    if (keepalive) cfg.insecure_skip_verify = true;
    else cfg.cert_path = ca_path;
    cfg.on_lane_pump = gated_pump;
    moq_mvfst_managed_t *m = nullptr;
    moq_result_t rc = moq_mvfst_managed_create(&cfg, &m);
    if (keepalive) {
        /* Setup reports success, then the thread halts before start(). */
        MVFST_CHECK(rc == MOQ_OK);
        MVFST_CHECK(m != nullptr);
        if (m) MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);    /* joins */
        MVFST_CHECK(installed.observed);
    } else {
        /* A failed setup: create joined the thread before returning. */
        MVFST_CHECK(rc == MOQ_ERR_INTERNAL);
        MVFST_CHECK(m == nullptr);
    }
    check_cleanup();
    {
        std::lock_guard<std::mutex> lk(g_obs.mu);
        for (int site = 0; site < 4; site++)
            MVFST_CHECK(g_obs.granted[MOQ_MVFST_TEST_EVB_CLIENT][site] == 0);
        MVFST_CHECK(!g_obs.retire_began[MOQ_MVFST_TEST_EVB_CLIENT]);
    }
    check_settled(MOQ_MVFST_TEST_EVB_CLIENT);
    if (m) moq_mvfst_managed_destroy(m);
    moq_mvfst_test_set_evb_probe(nullptr);
    if (keepalive) moq_mvfst_managed_test_set_keepalive_probe(nullptr);
    else unlink(ca_path);
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        std::fprintf(stderr, "usage: %s CASE\n", argv[0]);
        return 2;
    }
    const std::string c = argv[1];
    if (c == "client-enqueue") case_client_race(MOQ_MVFST_TEST_EVB_ENQUEUE, false, false);
    else if (c == "client-notify") case_client_race(MOQ_MVFST_TEST_EVB_NOTIFY, false, false);
    else if (c == "client-drain") case_client_race(MOQ_MVFST_TEST_EVB_ENQUEUE, true, false);
    else if (c == "client-notify-cleanup") case_client_race(MOQ_MVFST_TEST_EVB_NOTIFY, false, true);
    else if (c == "client-live") case_client_live(false, false);
    else if (c == "client-live-cleanup") case_client_live(true, false);
    else if (c == "client-drain-error") case_client_live(true, true);
    else if (c == "client-after") case_client_after();
    else if (c == "server-enqueue") case_server_race(MOQ_MVFST_TEST_EVB_ENQUEUE);
    else if (c == "server-thread") case_server_race(MOQ_MVFST_TEST_EVB_THREAD_CHECK);
    else if (c == "client-early-setup") case_client_early(false);
    else if (c == "client-early-keepalive") case_client_early(true);
    else {
        std::fprintf(stderr, "unknown case %s\n", c.c_str());
        return 2;
    }
    if (failures == 0) std::printf("PASS: mvfst_managed_evb_lifetime %s\n", c.c_str());
    return failures == 0 ? 0 : 1;
}
