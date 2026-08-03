/*
 * C++17 compile-only coverage for <moq/wtquic_msquic_managed.h>: the header is
 * includable from C++ (extern "C" intact) and every public declaration is
 * well-typed. Compiled as an OBJECT and never linked/run (the managed library
 * is a later slice).
 */
#include <moq/wtquic_msquic_managed.h>

#include <cstddef>
#include <cstdint>

/* Pin the public wire-profile enum values as seen by a C++ consumer. */
static_assert(static_cast<int>(MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT) == 0,
              "WT profile CURRENT must be 0");
static_assert(static_cast<int>(MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT) == 1,
              "WT profile D13_14_COMPAT must be 1");

extern "C" void moq_wtquic_msquic_managed_public_probe_cxx(void);
void moq_wtquic_msquic_managed_public_probe_cxx(void)
{
    moq_wtquic_msquic_managed_t *m = nullptr;
    moq_wtquic_msquic_managed_lane_t *lane = nullptr;
    moq_wtquic_msquic_managed_conn_t *conn = nullptr;
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_lane_stats_t st;
    moq_wtquic_msquic_accept_info_t ai;
    moq_wtquic_msquic_choose_lane_fn cl = nullptr;
    moq_wtquic_msquic_lane_pump_fn lp = nullptr;
    moq_wtquic_msquic_activity_fn ac = nullptr;
    moq_wtquic_msquic_wt_profile_t prof = MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT;
    /* pin the exact close-code width (uint32_t); a regression fails to build. */
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

    cfg.struct_size = 0;
    cfg.wt_protocols = nullptr;
    cfg.wt_protocol_count = 0;
    cfg.on_stopped = nullptr;
    cfg.on_stopped_ctx = nullptr;
    cfg.max_connections = 0;
    cfg.lane_count = 0;
    cfg.streaming_objects = false;
    cfg.session_idle_timeout_us = 0;
    cfg.webtransport_profile = static_cast<uint32_t>(prof);
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
    moq_wtquic_msquic_managed_conn_set_user(conn, nullptr);
    (void)moq_wtquic_msquic_managed_conn_user(conn);
    moq_wtquic_msquic_managed_conn_close(conn, static_cast<uint32_t>(0));
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
}
