#include "simpair_internal.h"

static bool cfg_has_field(const moq_simpair_cfg_t *cfg,
                          size_t offset, size_t size)
{
    return cfg->struct_size >= offset && size <= cfg->struct_size - offset;
}

static moq_result_t make_session(const moq_alloc_t *alloc,
                                  uint64_t initial_now_us,
                                  moq_perspective_t perspective,
                                  bool send_request_capacity,
                                  uint64_t initial_request_capacity,
                                  bool streaming_objects,
                                  uint64_t goaway_timeout_us,
                                  uint32_t max_actions,
                                  uint32_t max_events,
                                  moq_version_t version,
                                  uint32_t max_subscriptions,
                                  moq_session_t **out)
{
    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = alloc;
    scfg.perspective = perspective;
    scfg.send_request_capacity = send_request_capacity;
    scfg.initial_request_capacity = initial_request_capacity;
    scfg.streaming_objects = streaming_objects;
    scfg.goaway_timeout_us = goaway_timeout_us;
    scfg.max_actions = max_actions;
    scfg.max_events = max_events;
    scfg.version = version;
    if (max_subscriptions) scfg.max_subscriptions = max_subscriptions;
    return moq_session_create(&scfg, initial_now_us, out);
}

moq_result_t moq_simpair_create(const moq_simpair_cfg_t *cfg,
                                 moq_simpair_t **out)
{
    if (out)
        *out = NULL;
    if (!cfg || !out)
        return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_simpair_cfg_t, seed))
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->free)
        return MOQ_ERR_INVAL;

    uint64_t seed = 0;
    uint64_t initial_now_us = 0;
    bool client_send_request_capacity = false;
    uint64_t client_initial_request_capacity = 0;
    bool server_send_request_capacity = false;
    uint64_t server_initial_request_capacity = 0;
    moq_sim_trace_fn trace_fn = NULL;
    void *trace_ctx = NULL;

    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, seed),
                      sizeof(cfg->seed)))
        seed = cfg->seed;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, initial_now_us),
                      sizeof(cfg->initial_now_us)))
        initial_now_us = cfg->initial_now_us;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t,
                                    client_send_request_capacity),
                      sizeof(cfg->client_send_request_capacity)))
        client_send_request_capacity = cfg->client_send_request_capacity;
    if (client_send_request_capacity) {
        if (!cfg_has_field(cfg, offsetof(moq_simpair_cfg_t,
                                         client_initial_request_capacity),
                           sizeof(cfg->client_initial_request_capacity)))
            return MOQ_ERR_INVAL;
        client_initial_request_capacity = cfg->client_initial_request_capacity;
    }
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t,
                                    server_send_request_capacity),
                      sizeof(cfg->server_send_request_capacity)))
        server_send_request_capacity = cfg->server_send_request_capacity;
    if (server_send_request_capacity) {
        if (!cfg_has_field(cfg, offsetof(moq_simpair_cfg_t,
                                         server_initial_request_capacity),
                           sizeof(cfg->server_initial_request_capacity)))
            return MOQ_ERR_INVAL;
        server_initial_request_capacity = cfg->server_initial_request_capacity;
    }
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, trace_fn),
                      sizeof(cfg->trace_fn)))
        trace_fn = cfg->trace_fn;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, trace_ctx),
                      sizeof(cfg->trace_ctx)))
        trace_ctx = cfg->trace_ctx;

    uint32_t fault_per_mille = 0;
    uint32_t fault_flags = 0;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, fault_per_mille),
                      sizeof(cfg->fault_per_mille)))
        fault_per_mille = cfg->fault_per_mille;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, fault_flags),
                      sizeof(cfg->fault_flags)))
        fault_flags = cfg->fault_flags;
    bool client_streaming_objects = false;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, client_streaming_objects),
                      sizeof(cfg->client_streaming_objects)))
        client_streaming_objects = cfg->client_streaming_objects;
    uint64_t client_goaway_timeout_us = 0;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, client_goaway_timeout_us),
                      sizeof(cfg->client_goaway_timeout_us)))
        client_goaway_timeout_us = cfg->client_goaway_timeout_us;
    uint64_t server_goaway_timeout_us = 0;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, server_goaway_timeout_us),
                      sizeof(cfg->server_goaway_timeout_us)))
        server_goaway_timeout_us = cfg->server_goaway_timeout_us;

    uint32_t max_actions = 0;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, max_actions),
                      sizeof(cfg->max_actions)))
        max_actions = cfg->max_actions;
    uint32_t max_events = 0;
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, max_events),
                      sizeof(cfg->max_events)))
        max_events = cfg->max_events;
    moq_version_t version = 0;   /* 0 -> session default */
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, version),
                      sizeof(cfg->version)))
        version = cfg->version;
    uint32_t server_max_subscriptions = 0;   /* 0 -> session default */
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, server_max_subscriptions),
                      sizeof(cfg->server_max_subscriptions)))
        server_max_subscriptions = cfg->server_max_subscriptions;
    uint32_t client_max_subscriptions = 0;   /* 0 -> session default */
    if (cfg_has_field(cfg, offsetof(moq_simpair_cfg_t, client_max_subscriptions),
                      sizeof(cfg->client_max_subscriptions)))
        client_max_subscriptions = cfg->client_max_subscriptions;

    if (fault_per_mille > 1000)
        fault_per_mille = 1000;

    if (client_send_request_capacity &&
        client_initial_request_capacity > MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    if (server_send_request_capacity &&
        server_initial_request_capacity > MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;

    moq_simpair_t *sp = (moq_simpair_t *)cfg->alloc->alloc(
        sizeof(moq_simpair_t), cfg->alloc->ctx);
    if (!sp)
        return MOQ_ERR_NOMEM;

    memset(sp, 0, sizeof(*sp));
    sp->alloc = *cfg->alloc;
    sp->seed = seed;
    sp->now_us = initial_now_us;
    sp->trace_fn = trace_fn;
    sp->trace_ctx = trace_ctx;
    sp->next_rx_ref = 1000;
    sp->next_bidi_ref = 2000;
    sp->fault_per_mille = fault_per_mille;
    sp->fault_flags = fault_flags;
    sp->version = version;

    moq_result_t rc = make_session(cfg->alloc, initial_now_us,
                                   MOQ_PERSPECTIVE_CLIENT,
                                   client_send_request_capacity,
                                   client_initial_request_capacity,
                                   client_streaming_objects,
                                   client_goaway_timeout_us,
                                   max_actions, max_events,
                                   version,
                                   client_max_subscriptions,
                                   &sp->client);
    if (rc < 0)
        goto fail;

    rc = make_session(cfg->alloc, initial_now_us,
                      MOQ_PERSPECTIVE_SERVER,
                      server_send_request_capacity,
                      server_initial_request_capacity,
                      false,
                      server_goaway_timeout_us,
                      max_actions, max_events,
                      version,
                      server_max_subscriptions,
                      &sp->server);
    if (rc < 0)
        goto fail;

    *out = sp;
    return MOQ_OK;

fail:
    moq_simpair_destroy(sp);
    return rc;
}

void moq_simpair_destroy(moq_simpair_t *sp)
{
    if (!sp) return;
    sim_delay_clear(sp);
    moq_alloc_t alloc = sp->alloc;
    moq_session_destroy(sp->client);
    moq_session_destroy(sp->server);
    alloc.free(sp, sizeof(moq_simpair_t), alloc.ctx);
}

moq_session_t *moq_simpair_client(moq_simpair_t *sp)
{
    return sp ? sp->client : NULL;
}

moq_session_t *moq_simpair_server(moq_simpair_t *sp)
{
    return sp ? sp->server : NULL;
}

uint64_t moq_simpair_now_us(const moq_simpair_t *sp)
{
    return sp ? sp->now_us : 0;
}

uint64_t moq_simpair_seed(const moq_simpair_t *sp)
{
    return sp ? sp->seed : 0;
}

/* True for profiles where both peers must start and send SETUP (a symmetric
 * handshake over a unidirectional control-channel pair). Profiles that drive
 * setup from the client alone, with a reactive server, return false. New
 * profiles that require a symmetric start are added here. */
static bool version_requires_symmetric_start(moq_version_t version)
{
    return version == MOQ_VERSION_DRAFT_18;
}

moq_result_t moq_simpair_start(moq_simpair_t *sp)
{
    if (!sp) return MOQ_ERR_INVAL;
    moq_result_t rc = moq_session_start(sp->client, sp->now_us);
    trace_input(sp, MOQ_SIM_INPUT_START, MOQ_PERSPECTIVE_CLIENT,
                MOQ_PERSPECTIVE_SERVER, (moq_bytes_t){0}, rc);
    if (rc < 0)
        return rc;
    if (version_requires_symmetric_start(sp->version)) {
        rc = moq_session_start(sp->server, sp->now_us);
        trace_input(sp, MOQ_SIM_INPUT_START, MOQ_PERSPECTIVE_SERVER,
                    MOQ_PERSPECTIVE_CLIENT, (moq_bytes_t){0}, rc);
    }
    return rc;
}

moq_result_t moq_simpair_step(moq_simpair_t *sp, size_t *out_delivered)
{
    if (!sp) return MOQ_ERR_INVAL;
    sp->step++;

    size_t delivered = 0;

    moq_result_t rc = sim_delay_deliver_matured(sp, &delivered);
    if (rc < 0) return rc;

    rc = pump_direction(sp, sp->client, sp->server,
                        MOQ_PERSPECTIVE_CLIENT,
                        MOQ_PERSPECTIVE_SERVER,
                        &delivered);
    if (rc < 0)
        return rc;

    rc = pump_direction(sp, sp->server, sp->client,
                        MOQ_PERSPECTIVE_SERVER,
                        MOQ_PERSPECTIVE_CLIENT,
                        &delivered);
    if (rc < 0)
        return rc;

    if (out_delivered)
        *out_delivered = delivered;
    if (delivered == 0)
        trace_quiescent(sp, delivered);
    return MOQ_OK;
}

moq_result_t moq_simpair_run_until_quiescent(moq_simpair_t *sp,
                                              size_t max_steps,
                                              size_t *out_steps)
{
    if (!sp) return MOQ_ERR_INVAL;
    if (out_steps) *out_steps = 0;

    for (size_t i = 0; i < max_steps; i++) {
        size_t delivered = 0;
        moq_result_t rc = moq_simpair_step(sp, &delivered);
        if (rc < 0)
            return rc;
        if (out_steps)
            (*out_steps)++;
        if (delivered == 0)
            return MOQ_OK;
    }

    return MOQ_ERR_WOULD_BLOCK;
}

void moq_simpair_enable_faults(moq_simpair_t *sp)
{
    if (sp) sp->fault_enabled = true;
}

uint64_t moq_simpair_next_deadline_us(const moq_simpair_t *sp)
{
    if (!sp) return UINT64_MAX;
    uint64_t d = UINT64_MAX;
    if (sp->delay_head && sp->delay_head->due_us < d)
        d = sp->delay_head->due_us;
    uint64_t cd = moq_session_next_deadline_us(sp->client);
    if (cd < d) d = cd;
    uint64_t sd = moq_session_next_deadline_us(sp->server);
    if (sd < d) d = sd;
    return d;
}

size_t moq_simpair_delayed_count(const moq_simpair_t *sp)
{
    if (!sp) return 0;
    size_t n = 0;
    for (const sim_delay_entry_t *e = sp->delay_head; e; e = e->next)
        n++;
    return n;
}

moq_result_t moq_simpair_advance_to(moq_simpair_t *sp, uint64_t now_us)
{
    if (!sp) return MOQ_ERR_INVAL;
    if (now_us < sp->now_us)
        return MOQ_ERR_INVAL;

    sp->now_us = now_us;
    moq_result_t client_rc = moq_session_tick(sp->client, now_us);
    trace_input(sp, MOQ_SIM_INPUT_TICK, MOQ_PERSPECTIVE_CLIENT,
                MOQ_PERSPECTIVE_SERVER, (moq_bytes_t){0}, client_rc);
    if (client_rc < 0)
        return client_rc;

    moq_result_t server_rc = moq_session_tick(sp->server, now_us);
    trace_input(sp, MOQ_SIM_INPUT_TICK, MOQ_PERSPECTIVE_SERVER,
                MOQ_PERSPECTIVE_CLIENT, (moq_bytes_t){0}, server_rc);
    return server_rc;
}
