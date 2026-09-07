/*
 * Compile-only coverage for <moq/wtquic_msquic_managed.h>: references EVERY
 * public declaration through an actual call with placeholder arguments, so a
 * signature drift fails the build under pedantic C11. Compiled as an OBJECT and
 * never linked or run — the probe body is never executed — so the header keeps
 * this coverage even in builds where the managed facade target is disabled.
 */
#include <moq/wtquic_msquic_managed.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Pin the public wire-profile enum values as seen by a C consumer: they are a
 * stable wire contract (mirrored 1:1 onto wtquic), so a drift is a build error. */
_Static_assert((int)MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT == 0,
               "WT profile CURRENT must be 0");
_Static_assert((int)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT == 1,
               "WT profile D13_14_COMPAT must be 1");
_Static_assert((int)MOQ_WTQUIC_MSQUIC_WT_PROFILE_D02_RFC9297_COMPAT == 2,
               "the draft-02 + RFC 9297 profile is wire value 2");

/* The append is gated by arithmetic on offsetof/sizeof, so what must hold on
 * EVERY data model is that each new field sits wholly past the previous one
 * and past the previous full struct, and that the stored policy is a fixed
 * uint32_t rather than an ABI-sized enum. Asserted here so any consumer
 * build -- including a 32-bit one this host cannot produce -- enforces it. */
/* The PREVIOUS FULL config, frozen here field for field. The append's oracle
 * is this struct's sizeof -- padding included -- not the end of its last
 * member: on a target where the two differ (ARM ILP32: 132 vs 136) an old
 * full-size caller would otherwise expose its trailing padding as a complete
 * Origin pointer. Frozen independently of the current declaration, so a
 * change to either side is a compile error. */
typedef struct {
    uint32_t struct_size;
    const moq_alloc_t *alloc;
    moq_perspective_t perspective;
    const char *host;
    uint16_t port;
    const char *cert_path;
    const char *key_path;
    bool insecure_skip_verify;
    uint32_t idle_timeout_ms;
    const char *wt_path;
    const char *const *wt_protocols;
    size_t wt_protocol_count;
    moq_wtquic_msquic_lane_pump_fn on_lane_pump;
    void *on_lane_pump_user;
    moq_wtquic_msquic_activity_fn on_activity;
    void *on_activity_ctx;
    void (*on_stopped)(void *ctx);
    void *on_stopped_ctx;
    bool send_request_capacity;
    uint32_t initial_request_capacity;
    uint32_t max_events;
    uint32_t max_actions;
    uint32_t max_connections;
    uint32_t lane_count;
    moq_wtquic_msquic_choose_lane_fn choose_lane;
    void *choose_lane_user;
    bool streaming_objects;
    uint64_t session_idle_timeout_us;
    uint32_t webtransport_profile;
    uint64_t (*app_deadline_us)(void *ctx);
    void *app_deadline_ctx;
} mmpc_cfg_prev_full_t;

#define MMPC_PREV_OFF(f)                                                  \
    _Static_assert(offsetof(moq_wtquic_msquic_managed_cfg_t, f) ==           \
                       offsetof(mmpc_cfg_prev_full_t, f),                                \
                   "old config field " #f " moved from its frozen offset")
MMPC_PREV_OFF(struct_size);   MMPC_PREV_OFF(alloc);
MMPC_PREV_OFF(perspective);   MMPC_PREV_OFF(host);
MMPC_PREV_OFF(port);          MMPC_PREV_OFF(cert_path);
MMPC_PREV_OFF(key_path);      MMPC_PREV_OFF(insecure_skip_verify);
MMPC_PREV_OFF(idle_timeout_ms); MMPC_PREV_OFF(wt_path);
MMPC_PREV_OFF(wt_protocols);  MMPC_PREV_OFF(wt_protocol_count);
MMPC_PREV_OFF(on_lane_pump);  MMPC_PREV_OFF(on_lane_pump_user);
MMPC_PREV_OFF(on_activity);   MMPC_PREV_OFF(on_activity_ctx);
MMPC_PREV_OFF(on_stopped);    MMPC_PREV_OFF(on_stopped_ctx);
MMPC_PREV_OFF(send_request_capacity);
MMPC_PREV_OFF(initial_request_capacity);
MMPC_PREV_OFF(max_events);    MMPC_PREV_OFF(max_actions);
MMPC_PREV_OFF(max_connections); MMPC_PREV_OFF(lane_count);
MMPC_PREV_OFF(choose_lane);   MMPC_PREV_OFF(choose_lane_user);
MMPC_PREV_OFF(streaming_objects);
MMPC_PREV_OFF(session_idle_timeout_us);
MMPC_PREV_OFF(webtransport_profile);
MMPC_PREV_OFF(app_deadline_us); MMPC_PREV_OFF(app_deadline_ctx);
#undef MMPC_PREV_OFF
_Static_assert(offsetof(moq_wtquic_msquic_managed_cfg_t, origin) >=
                   sizeof(mmpc_cfg_prev_full_t),
               "the Origin append must begin at/after the PREVIOUS FULL "
               "sizeof, never inside its trailing padding");
_Static_assert(sizeof(moq_wtquic_msquic_managed_cfg_t) > sizeof(mmpc_cfg_prev_full_t),
               "the new config must be strictly larger than the old one");

#define MMPC_OFF(f) offsetof(moq_wtquic_msquic_managed_cfg_t, f)
#define MMPC_SZ(f)  sizeof(((moq_wtquic_msquic_managed_cfg_t *)0)->f)
_Static_assert(MMPC_OFF(origin) >=
                   MMPC_OFF(app_deadline_ctx) + MMPC_SZ(app_deadline_ctx),
               "the Origin tail must start at/after the END of the previous "
               "last member -- a weaker property than the frozen-sizeof "
               "assertion above, which is the real oracle");
_Static_assert(MMPC_OFF(origin_policy) >= MMPC_OFF(origin) + MMPC_SZ(origin),
               "origin_policy must follow origin wholly");
_Static_assert(MMPC_OFF(allowed_origins) >=
                   MMPC_OFF(origin_policy) + MMPC_SZ(origin_policy),
               "allowed_origins must follow origin_policy wholly");
_Static_assert(MMPC_OFF(allowed_origin_count) >=
                   MMPC_OFF(allowed_origins) + MMPC_SZ(allowed_origins),
               "allowed_origin_count is the last field of the block");
_Static_assert(sizeof(moq_wtquic_msquic_managed_cfg_t) >=
                   MMPC_OFF(allowed_origin_count) +
                       MMPC_SZ(allowed_origin_count),
               "the struct size must cover the whole appended block");
_Static_assert(MMPC_SZ(origin_policy) == 4,
               "the stored policy is a uint32_t, not an ABI-sized enum");
#undef MMPC_SZ
#undef MMPC_OFF

/* The facade's Origin policy is public and closed; pin its values too. */
_Static_assert((int)MOQ_WTQUIC_MSQUIC_ORIGIN_POLICY_UNSET == 0 &&
                   (int)MOQ_WTQUIC_MSQUIC_ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE == 1 &&
                   (int)MOQ_WTQUIC_MSQUIC_ORIGIN_POLICY_ALLOWLIST == 2 &&
                   (int)MOQ_WTQUIC_MSQUIC_ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL == 3,
               "the Origin policy values are a stable contract");

/* external linkage: not an unused static; never called. */
void moq_wtquic_msquic_managed_public_probe_c(void);
void moq_wtquic_msquic_managed_public_probe_c(void)
{
    moq_wtquic_msquic_managed_t *m = NULL;
    moq_wtquic_msquic_managed_lane_t *lane = NULL;
    moq_wtquic_msquic_managed_conn_t *conn = NULL;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_lane_stats_t st;
    moq_wtquic_msquic_accept_info_t ai;
    moq_wtquic_msquic_choose_lane_fn cl = NULL;
    moq_wtquic_msquic_lane_pump_fn lp = NULL;
    moq_wtquic_msquic_activity_fn ac = NULL;
    moq_wtquic_msquic_wt_profile_t prof = MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT;
    moq_wtquic_msquic_origin_policy_t opol =
        MOQ_WTQUIC_MSQUIC_ORIGIN_POLICY_ALLOWLIST;
    static const char *const origins[1] = { "https://app.example" };
    /* pin the exact close-code width: fails to compile if it regresses off
     * uint32_t (the WebTransport/wtquic close-code width). */
    void (*f_conn_close)(moq_wtquic_msquic_managed_conn_t *, uint32_t) =
        moq_wtquic_msquic_managed_conn_close;
    /* pin the acknowledgment entry: exactly one conn argument, moq_result_t
     * result -- no lane, token, or size parameter may creep in */
    moq_result_t (*f_ack)(moq_wtquic_msquic_managed_conn_t *) =
        moq_wtquic_msquic_managed_conn_ack_terminal;

    volatile moq_result_t r;
    volatile bool b;
    volatile uint64_t u64;
    volatile uint32_t u32;
    volatile uint16_t u16;
    volatile size_t sz;
    volatile moq_version_t ver;
    moq_session_t *volatile sess;
    moq_wtquic_conn_t *volatile adap;

    /* touch appended cfg / stats / accept-info fields + the stats floor */
    cfg.struct_size = 0;
    cfg.wt_protocols = NULL;
    cfg.wt_protocol_count = 0;
    cfg.on_stopped = NULL;
    cfg.on_stopped_ctx = NULL;
    cfg.max_connections = 0;
    cfg.lane_count = 0;
    cfg.streaming_objects = false;
    cfg.session_idle_timeout_us = 0;
    /* the appended wire-profile tail field + its typed enum */
    cfg.webtransport_profile = (uint32_t)prof;
    /* the appended Origin block: referenced so a field rename or type drift
     * is a build error for a public consumer */
    cfg.origin = "https://app.example";
    cfg.origin_policy = (uint32_t)opol;
    cfg.allowed_origins = origins;
    cfg.allowed_origin_count = 1u;
    st.struct_size = 0;
    st.flush_bytes = 0;
    ai.struct_size = 0;
    sz = MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE;

    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof cfg);
    r = moq_wtquic_msquic_managed_create(&cfg, &m);
    b = moq_wtquic_msquic_managed_stop_begin(m);
    r = moq_wtquic_msquic_managed_join(m);
    r = moq_wtquic_msquic_managed_stop(m);
    moq_wtquic_msquic_managed_destroy(m);
    sess = moq_wtquic_msquic_managed_session(m);
    adap = moq_wtquic_msquic_managed_adapter(m);
    u32 = moq_wtquic_msquic_managed_lane_count(m);
    lane = moq_wtquic_msquic_managed_lane(m, 0);
    u32 = moq_wtquic_msquic_lane_index(lane);
    conn = moq_wtquic_msquic_lane_next_conn(lane, conn);
    r = moq_wtquic_msquic_lane_wake(lane);
    r = moq_wtquic_msquic_lane_get_stats(lane, &st, sizeof st);
    sess = moq_wtquic_msquic_managed_conn_session(conn);
    adap = moq_wtquic_msquic_managed_conn_adapter(conn);
    lane = moq_wtquic_msquic_managed_conn_lane(conn);
    ver = moq_wtquic_msquic_managed_conn_negotiated_version(conn);
    moq_wtquic_msquic_managed_conn_set_user(conn, NULL);
    (void)moq_wtquic_msquic_managed_conn_user(conn);
    moq_wtquic_msquic_managed_conn_close(conn, (uint32_t)0);
    r = moq_wtquic_msquic_managed_conn_ack_terminal(conn);
    sz = moq_wtquic_msquic_managed_conn_count(m);
    moq_wtquic_msquic_managed_drain(m);
    u16 = moq_wtquic_msquic_managed_port(m);
    r = moq_wtquic_msquic_managed_wake(m);
    r = moq_wtquic_msquic_managed_wait(m, 0);
    b = moq_wtquic_msquic_managed_is_fatal(m);
    u64 = moq_wtquic_msquic_managed_fatal_code(m);
    b = moq_wtquic_msquic_managed_is_closed(m);
    u64 = moq_wtquic_msquic_managed_close_code(m);
    ver = moq_wtquic_msquic_managed_negotiated_version(m);

    (void)r; (void)b; (void)u64; (void)u32; (void)u16; (void)sz;
    (void)ver; (void)sess; (void)adap; (void)ai; (void)cl; (void)lp;
    (void)ac; (void)conn; (void)lane; (void)m; (void)f_conn_close; (void)f_ack;
    (void)prof;
    (void)opol;
}
