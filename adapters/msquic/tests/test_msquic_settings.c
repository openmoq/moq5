/*
 * Managed MsQuic settings policy: the listener's pinned path MTU.
 *
 * A client may open with an Initial that carries no ClientHello, so the
 * server's first reply goes out before any peer transport parameter has been
 * received. At MsQuic's default that reply measured 1260 bytes of UDP payload;
 * the observed peer sent exclusively 1252-byte datagrams, did not process the
 * reply, and retransmitted until it gave up. Capping server datagrams at 1252
 * in a proxy took it from 0/6 to 6/6. The managed LISTENER therefore pins both
 * MTU bounds so the same cap holds at the source; the managed CLIENT declares
 * no MTU policy at all.
 *
 * RFC 9000 §18.2 defines max_udp_payload_size as the largest UDP payload an
 * endpoint is willing to receive and notes larger datagrams are unlikely to be
 * processed; §14 permits a receiver to discard datagrams exceeding its size
 * constraints. The peer's discard was permitted, not mandated -- these
 * assertions pin our own conservative listener, not an obligation on it.
 *
 * This exercises `mgd_build_settings()` -- the same construction
 * `moq_msquic_managed_create()` hands to ConfigurationOpen() -- rather than
 * rebuilding the desired settings, which would assert nothing. Every case
 * selects its behavior solely through the configuration's own perspective. It
 * performs no I/O and opens no transport.
 */
#include <moq/msquic.h>
#include <moq/msquic_managed.h>

#include <msquic.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* the managed settings builder (msquic_managed.c, MOQ_MSQUIC_TESTING only) --
 * the SAME construction moq_msquic_managed_create() hands to
 * ConfigurationOpen(); static and unreachable in production builds. It takes
 * the configuration alone, so the perspective it acts on is the one the
 * configuration declares -- this test cannot pair a perspective with a config
 * that does not carry it. */
extern void mgd_build_settings(const moq_msquic_managed_cfg_t *cfg,
                               QUIC_SETTINGS *out);

/* 0024: the process-global server MTU floor. `mgd_build_global_server_floor` is
 * the pure builder for the ONE global write a managed server issues. The seams
 * below are MOQ_MSQUIC_TESTING-only (absent from the production symbol surface):
 *
 *  - moq_msq_test_after_global_write: a post-write probe the create path invokes
 *    with the LIVE api table immediately after the REAL api->SetParam. It does
 *    NOT replace the production call, so a mutation of that call (no-op, wrong
 *    parameter, extra global bound) is caught by reading the settings back with
 *    GetParam rather than by inspecting a hook's captured arguments.
 *  - moq_msq_test_force_global_status_active / _force_global_status: override the
 *    real write's returned status to drive the fail-closed path.
 *  - moq_msq_test_abort_after_global: tear the create down right after the
 *    global-floor decision (before RegistrationOpen / any socket).
 *  - moq_msq_test_registration_reached: set just before RegistrationOpen, so a
 *    rejected/aborted floor is proven to stop before the registration is opened.
 */
extern void mgd_build_global_server_floor(QUIC_SETTINGS *out);
extern void (*moq_msq_test_after_global_write)(const QUIC_API_TABLE *api);
extern bool        moq_msq_test_force_global_status_active;
extern QUIC_STATUS moq_msq_test_force_global_status;
extern bool        moq_msq_test_abort_after_global;
extern bool        moq_msq_test_registration_reached;

/* A tracking allocator: exact outstanding-allocation and live-byte balance, so a
 * create that aborts or fails must drive BOTH back to zero. `*out == NULL` alone
 * does not prove the facade, lane, and string allocations were released. */
typedef struct {
    long   outstanding; /* alloc calls minus free calls */
    size_t live_bytes;  /* sum of live allocation sizes */
} track_state_t;

static void *track_alloc(size_t size, void *ctx)
{
    track_state_t *t = ctx;
    void *p = malloc(size);
    if (p != NULL) { t->outstanding++; t->live_bytes += size; }
    return p;
}

static void *track_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    track_state_t *t = ctx;
    void *p = realloc(ptr, new_size);
    if (p != NULL && new_size != 0) {
        if (ptr == NULL) t->outstanding++;
        t->live_bytes = t->live_bytes - old_size + new_size;
    }
    return p;
}

static void track_free(void *ptr, size_t size, void *ctx)
{
    track_state_t *t = ctx;
    if (ptr != NULL) { t->outstanding--; t->live_bytes -= size; free(ptr); }
}

/* Global-settings readback captured by the post-write probe (positive path) from
 * the live api table, plus the pristine baseline captured before any server
 * create writes the floor. */
static int          g_probe_calls;
static bool         g_probe_readback_ok;
static QUIC_SETTINGS g_probe_readback;

static void after_global_write_probe(const QUIC_API_TABLE *api)
{
    g_probe_calls++;
    QUIC_SETTINGS got;
    memset(&got, 0, sizeof(got));
    uint32_t len = (uint32_t)sizeof(got);
    QUIC_STATUS st = api->GetParam(NULL, QUIC_PARAM_GLOBAL_SETTINGS, &len, &got);
    g_probe_readback_ok = QUIC_SUCCEEDED(st) && len == sizeof(got);
    if (g_probe_readback_ok)
        g_probe_readback = got;
}

static int no_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                   uint64_t now_us, void *user)
{
    (void)m; (void)lane; (void)now_us; (void)user; return 0;
}

/* Expected values, owned by this test and written from the interop evidence
 * rather than read back from the product's own constants. */
#define TS_PATH_MTU        1280u
#define TS_IPV4_HEADER       20u
#define TS_IPV6_HEADER       40u
#define TS_UDP_HEADER         8u
#define TS_IPV4_PAYLOAD    1252u   /* what the observed peer sent, and accepted */
#define TS_IPV6_PAYLOAD    1232u

static int failures;

#define CHECK(cond) do {                                                     \
        if (!(cond)) {                                                       \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                      \
        }                                                                    \
    } while (0)

#define CHECK_EQ_U32(got_, want_) do {                                       \
        uint32_t g_ = (uint32_t)(got_), w_ = (uint32_t)(want_);              \
        if (g_ != w_) {                                                      \
            fprintf(stderr, "FAIL: %s:%d: %s == %u, expected %s == %u\n",    \
                    __FILE__, __LINE__, #got_, g_, #want_, w_);              \
            failures++;                                                      \
        }                                                                    \
    } while (0)

/* A minimally valid managed config for each perspective. The perspective is
 * the only thing that selects the MTU policy, so it is the only axis these
 * cases vary. */
static void cfg_init(moq_msquic_managed_cfg_t *cfg, moq_perspective_t persp,
                     uint64_t idle_ms)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
    cfg->alloc = moq_alloc_default();
    cfg->perspective = persp;
    cfg->idle_timeout_ms = idle_ms;
}

int main(void)
{
    /* Base settings both perspectives inherit, established independently of
     * the builder so a regression in moq_msquic_settings_init() is visible
     * here rather than silently adopted. */
    QUIC_SETTINGS base;
    moq_msquic_settings_init(&base);
    CHECK(base.IsSet.SendBufferingEnabled == TRUE);
    CHECK(base.SendBufferingEnabled == FALSE);
    CHECK(base.IsSet.MinimumMtu == FALSE);
    CHECK(base.IsSet.MaximumMtu == FALSE);

    /* -- a SERVER configuration pins both bounds ---------------------- */
    {
        moq_msquic_managed_cfg_t cfg;
        cfg_init(&cfg, MOQ_PERSPECTIVE_SERVER, 0);
        QUIC_SETTINGS s;
        mgd_build_settings(&cfg, &s);

        CHECK(s.IsSet.MinimumMtu == TRUE);
        CHECK(s.IsSet.MaximumMtu == TRUE);
        CHECK_EQ_U32(s.MinimumMtu, TS_PATH_MTU);
        CHECK_EQ_U32(s.MaximumMtu, TS_PATH_MTU);
        /* the two bounds are mutually compatible -- a maximum below the
         * minimum is the failure mode a maximum-only change would have
         * introduced on this MsQuic revision */
        CHECK(s.MinimumMtu <= s.MaximumMtu);

        /* The UDP payloads this path MTU actually caps, computed from the
         * protocol header sizes rather than restated. Both subtractions are
         * guarded so an implausible MTU cannot wrap. */
        const uint32_t mtu = s.MaximumMtu;
        CHECK(mtu > TS_IPV4_HEADER + TS_UDP_HEADER);
        CHECK(mtu > TS_IPV6_HEADER + TS_UDP_HEADER);
        if (mtu > TS_IPV4_HEADER + TS_UDP_HEADER)
            CHECK_EQ_U32(mtu - TS_IPV4_HEADER - TS_UDP_HEADER, TS_IPV4_PAYLOAD);
        if (mtu > TS_IPV6_HEADER + TS_UDP_HEADER)
            CHECK_EQ_U32(mtu - TS_IPV6_HEADER - TS_UDP_HEADER, TS_IPV6_PAYLOAD);
        /* ... and the IPv4 cap does not exceed what the observed peer used,
         * which is what the oversized first reply did. */
        if (mtu > TS_IPV4_HEADER + TS_UDP_HEADER)
            CHECK(mtu - TS_IPV4_HEADER - TS_UDP_HEADER <= TS_IPV4_PAYLOAD);

        /* the base policy survives the MTU addition */
        CHECK(s.IsSet.SendBufferingEnabled == TRUE);
        CHECK(s.SendBufferingEnabled == FALSE);
        /* no idle override was configured */
        CHECK(s.IsSet.IdleTimeoutMs == base.IsSet.IdleTimeoutMs);
        CHECK(s.IsSet.HandshakeIdleTimeoutMs ==
              base.IsSet.HandshakeIdleTimeoutMs);
    }

    /* -- a CLIENT configuration declares no MTU policy ---------------- */
    {
        moq_msquic_managed_cfg_t cfg;
        cfg_init(&cfg, MOQ_PERSPECTIVE_CLIENT, 0);
        QUIC_SETTINGS s;
        mgd_build_settings(&cfg, &s);

        /* Assert the DECLARATION bits, not MsQuic's runtime defaults: the
         * client is unchanged by this fix precisely because LibMoQ sets
         * neither bound. */
        CHECK(s.IsSet.MinimumMtu == FALSE);
        CHECK(s.IsSet.MaximumMtu == FALSE);
        CHECK(s.IsSet.SendBufferingEnabled == TRUE);
        CHECK(s.SendBufferingEnabled == FALSE);
    }

    /* -- the configured idle timeout still bounds both phases, for both
     *    perspectives, and does not disturb the MTU declarations ------- */
    {
        const uint64_t idle = 4321u;
        moq_msquic_managed_cfg_t cfg;
        QUIC_SETTINGS s;

        cfg_init(&cfg, MOQ_PERSPECTIVE_SERVER, idle);
        mgd_build_settings(&cfg, &s);
        CHECK(s.IsSet.IdleTimeoutMs == TRUE);
        CHECK(s.IsSet.HandshakeIdleTimeoutMs == TRUE);
        CHECK(s.IdleTimeoutMs == idle);
        CHECK(s.HandshakeIdleTimeoutMs == idle);
        CHECK(s.IsSet.MinimumMtu == TRUE);
        CHECK(s.IsSet.MaximumMtu == TRUE);

        cfg_init(&cfg, MOQ_PERSPECTIVE_CLIENT, idle);
        mgd_build_settings(&cfg, &s);
        CHECK(s.IsSet.IdleTimeoutMs == TRUE);
        CHECK(s.IsSet.HandshakeIdleTimeoutMs == TRUE);
        CHECK(s.IdleTimeoutMs == idle);
        CHECK(s.HandshakeIdleTimeoutMs == idle);
        CHECK(s.IsSet.MinimumMtu == FALSE);
        CHECK(s.IsSet.MaximumMtu == FALSE);
    }

    /* -- a prefix-sized config is handled exactly as before ----------- */
    {
        moq_msquic_managed_cfg_t cfg;
        cfg_init(&cfg, MOQ_PERSPECTIVE_SERVER, 777u);
        cfg.struct_size =
            (uint32_t)offsetof(moq_msquic_managed_cfg_t, version);
        QUIC_SETTINGS s;
        mgd_build_settings(&cfg, &s);
        /* idle_timeout_ms and perspective both precede `version`, so a
         * prefix-sized caller still supplies them, and the listener MTU is a
         * policy of the perspective rather than of the config size. */
        CHECK(s.IsSet.IdleTimeoutMs == TRUE);
        CHECK(s.IdleTimeoutMs == 777u);
        CHECK(s.IsSet.MinimumMtu == TRUE);
        CHECK(s.IsSet.MaximumMtu == TRUE);
        CHECK_EQ_U32(s.MinimumMtu, TS_PATH_MTU);
        CHECK_EQ_U32(s.MaximumMtu, TS_PATH_MTU);
    }

    /* -- 0024: the PROCESS-GLOBAL server MTU floor (pre-configuration path) -- */

    /* Pure oracle: the global write declares ONLY MinimumMtu = 1280 and nothing
     * else. The exhaustive memcmp against a hand-zeroed reference (only those two
     * fields) proves no unrelated bit leaks -- in particular NOT
     * SendBufferingEnabled, which mgd_build_settings() / moq_msquic_settings_init()
     * would set. */
    {
        QUIC_SETTINGS floor;
        mgd_build_global_server_floor(&floor);
        CHECK(floor.IsSet.MinimumMtu == TRUE);
        CHECK_EQ_U32(floor.MinimumMtu, TS_PATH_MTU);
        /* no global maximum: DPLPMTUD stays available */
        CHECK(floor.IsSet.MaximumMtu == FALSE);
        /* the init helper's bit must NOT leak globally */
        CHECK(floor.IsSet.SendBufferingEnabled == FALSE);
        QUIC_SETTINGS ref;
        memset(&ref, 0, sizeof(ref));
        ref.MinimumMtu = TS_PATH_MTU;
        ref.IsSet.MinimumMtu = TRUE;
        /* exactly one bit, one value, nothing else */
        CHECK(memcmp(&floor, &ref, sizeof(floor)) == 0);
    }

    /* Create-path load-bearing, socket-free. The positive probe runs the REAL
     * api->SetParam and then reads the process-global settings back with
     * GetParam, so it proves the production call's effect rather than a hook's
     * captured arguments. A tracking allocator proves each abort/failure
     * releases everything it allocated. */
    {
        moq_msquic_managed_cfg_t scfg;
        memset(&scfg, 0, sizeof(scfg));
        scfg.struct_size = (uint32_t)sizeof(scfg);
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.on_lane_pump = no_pump;
        /* never loaded: the create aborts before credential handling */
        scfg.cert_path = "/nonexistent/cert.pem";
        scfg.key_path = "/nonexistent/key.pem";

        moq_msquic_managed_cfg_t ccfg;
        memset(&ccfg, 0, sizeof(ccfg));
        ccfg.struct_size = (uint32_t)sizeof(ccfg);
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.on_lane_pump = no_pump;
        ccfg.host = "127.0.0.1";
        ccfg.port = 47999;
        ccfg.insecure_skip_verify = true;

        const moq_alloc_t base_alloc = {
            .ctx = NULL,
            .alloc = track_alloc,
            .realloc = track_realloc,
            .free = track_free,
        };

        /* Capture the PRISTINE global-settings baseline from a private api table
         * before any server create writes the floor. Kept open across the create
         * tests so MsQuic stays initialized (global state stable), closed at the
         * end. */
        const QUIC_API_TABLE *baseline_api = NULL;
        QUIC_SETTINGS baseline;
        bool baseline_valid = false;
        memset(&baseline, 0, sizeof(baseline));
        if (QUIC_SUCCEEDED(MsQuicOpen2(&baseline_api)) && baseline_api != NULL) {
            uint32_t len = (uint32_t)sizeof(baseline);
            if (QUIC_SUCCEEDED(baseline_api->GetParam(
                    NULL, QUIC_PARAM_GLOBAL_SETTINGS, &len, &baseline)) &&
                len == sizeof(baseline))
                baseline_valid = true;
        }
        CHECK(baseline_valid);

        /* A: a SERVER create runs the real global write; the post-write probe
         * reads it back and proves the partial update took effect. Then the
         * create aborts, and the tracking allocator must balance to zero. */
        moq_msq_test_after_global_write = after_global_write_probe;
        moq_msq_test_force_global_status_active = false;
        moq_msq_test_abort_after_global = true;
        moq_msq_test_registration_reached = false;
        g_probe_calls = 0;
        g_probe_readback_ok = false;
        track_state_t tsrv = { 0, 0 };
        moq_alloc_t srv_alloc = base_alloc;
        srv_alloc.ctx = &tsrv;
        scfg.alloc = &srv_alloc;
        moq_msquic_managed_t *sm = (moq_msquic_managed_t *)(uintptr_t)0x1; /* poison */
        moq_result_t rs = moq_msquic_managed_create(&scfg, &sm);
        CHECK(rs != MOQ_OK);                /* aborted after the write */
        CHECK(sm == NULL);                  /* fail-closed: *out cleared */
        /* aborted before RegistrationOpen */
        CHECK(moq_msq_test_registration_reached == false);
        /* the real write ran; the probe saw it */
        CHECK(g_probe_calls == 1);
        CHECK(g_probe_readback_ok == true);
        /* declaration active */
        CHECK(g_probe_readback.IsSet.MinimumMtu == TRUE);
        CHECK_EQ_U32(g_probe_readback.MinimumMtu, TS_PATH_MTU); /* 1280 now in effect */
        /* the pre-existing global MAXIMUM is unchanged -- read back, not inferred
         * from the object we sent (which set no maximum). 1500 is the fresh
         * default; the load-bearing invariant is that our partial write did not
         * move it. */
        CHECK_EQ_U32(g_probe_readback.MaximumMtu, baseline.MaximumMtu);
        CHECK_EQ_U32(baseline.MaximumMtu, 1500u);
        /* no unrelated global setting changed: neutralize the ONE field we set in
         * both snapshots and require the remainder to be byte-identical. */
        {
            QUIC_SETTINGS a = baseline, b = g_probe_readback;
            a.MinimumMtu = 0; b.MinimumMtu = 0;
            a.IsSet.MinimumMtu = 0; b.IsSet.MinimumMtu = 0;
            CHECK(memcmp(&a, &b, sizeof(a)) == 0);
        }
        CHECK(tsrv.outstanding == 0);   /* every facade/lane/string alloc freed */
        CHECK(tsrv.live_bytes == 0);

        /* B: a CLIENT create issues NO global write (the probe never fires) and
         * likewise releases everything on the same post-global abort. */
        moq_msq_test_after_global_write = after_global_write_probe;
        moq_msq_test_abort_after_global = true;
        g_probe_calls = 0;
        track_state_t tcli = { 0, 0 };
        moq_alloc_t cli_alloc = base_alloc;
        cli_alloc.ctx = &tcli;
        ccfg.alloc = &cli_alloc;
        moq_msquic_managed_t *cm = (moq_msquic_managed_t *)(uintptr_t)0x1;
        moq_result_t rc = moq_msquic_managed_create(&ccfg, &cm);
        CHECK(rc != MOQ_OK);
        CHECK(cm == NULL);
        CHECK(g_probe_calls == 0);      /* client never touches process-global policy */
        CHECK(tcli.outstanding == 0);
        CHECK(tcli.live_bytes == 0);

        /* C: an injected SetParam failure maps to create FAILURE + cleanup. The
         * real write still runs; only its returned status is overridden. */
        moq_msq_test_after_global_write = NULL;
        /* the injected failure is the abort */
        moq_msq_test_abort_after_global = false;
        moq_msq_test_force_global_status_active = true;
        moq_msq_test_force_global_status = QUIC_STATUS_INTERNAL_ERROR;
        moq_msq_test_registration_reached = false;
        track_state_t tfail = { 0, 0 };
        moq_alloc_t fail_alloc = base_alloc;
        fail_alloc.ctx = &tfail;
        scfg.alloc = &fail_alloc;
        moq_msquic_managed_t *fm = (moq_msquic_managed_t *)(uintptr_t)0x1;
        moq_result_t rf = moq_msquic_managed_create(&scfg, &fm);
        CHECK(rf != MOQ_OK);    /* rejected floor aborts the create */
        CHECK(fm == NULL);
        /* never reaches RegistrationOpen */
        CHECK(moq_msq_test_registration_reached == false);
        CHECK(tfail.outstanding == 0);
        CHECK(tfail.live_bytes == 0);

        moq_msq_test_after_global_write = NULL;
        moq_msq_test_force_global_status_active = false;
        moq_msq_test_force_global_status = QUIC_STATUS_SUCCESS;
        moq_msq_test_abort_after_global = false;

        /* API-table closure oracle. The tracking allocator cannot see an
         * MsQuicOpen2 reference, so prove each abort/failure closed its own API
         * table via MsQuic's verified v2.5.9 global lifetime: with the three
         * temporary creates done and their tables closed, `baseline_api` holds
         * the LAST open reference. Closing it uninitializes MsQuic's global
         * state; a fresh open then reloads the pristine defaults, so the 1280
         * floor is GONE and the settings equal the pristine baseline again. A
         * leaked API-table reference keeps MsQuic initialized, the floor stays
         * installed, and this readback fails -- which is what makes each real
         * mgd_close_transport() load-bearing. */
        if (baseline_valid && baseline_api != NULL) {
            MsQuicClose(baseline_api);
            baseline_api = NULL;
            const QUIC_API_TABLE *reset_api = NULL;
            QUIC_SETTINGS after;
            memset(&after, 0, sizeof(after));
            bool reset_ok = false;
            if (QUIC_SUCCEEDED(MsQuicOpen2(&reset_api)) && reset_api != NULL) {
                uint32_t len = (uint32_t)sizeof(after);
                if (QUIC_SUCCEEDED(reset_api->GetParam(
                        NULL, QUIC_PARAM_GLOBAL_SETTINGS, &len, &after)) &&
                    len == sizeof(after))
                    reset_ok = true;
            }
            CHECK(reset_ok);
            /* the floor is gone: MinimumMtu is back at the default, not 1280 */
            CHECK(after.MinimumMtu != TS_PATH_MTU);
            CHECK_EQ_U32(after.MinimumMtu, baseline.MinimumMtu);
            CHECK_EQ_U32(after.MaximumMtu, baseline.MaximumMtu);
            /* whole-settings identity to the pristine baseline */
            CHECK(memcmp(&after, &baseline, sizeof(after)) == 0);
            if (reset_api != NULL)
                MsQuicClose(reset_api);
        } else if (baseline_api != NULL) {
            MsQuicClose(baseline_api);
        }
    }

    if (failures != 0) {
        fprintf(stderr, "msquic_settings: %d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: msquic_settings\n");
    return 0;
}
