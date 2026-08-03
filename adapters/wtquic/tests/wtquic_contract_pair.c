/*
 * Production-contract conformance pair runner: the adapter hook drains
 * MoQ events into observations, advances the side's script, and
 * signals the main thread. See wtquic_contract_pair.h for the model.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "wtquic_contract_pair.h"

#include <moq/rcbuf.h>

#define WTQC_PATH "/moq"
#define WTQC_WAIT_SECS 30

void wtqc_payload_fill(uint8_t *dst, uint64_t size, uint64_t idx)
{
    for (size_t i = 0; i < 8 && i < size; i++)
        dst[i] = (uint8_t)(idx >> (8 * i));
    for (uint64_t j = 8; j < size; j++)
        dst[j] = (uint8_t)(idx * 131u + j * 7u + 47u);
}

static void obs_verify_object(wtqc_side_t *sd, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;

    if (payload == NULL) {
        sd->obs.object_errors++;
        return;
    }
    size_t len = moq_rcbuf_len(payload);
    const uint8_t *data = moq_rcbuf_data(payload);

    sd->obs.object_bytes += len;
    if (sd->expect_size != 0 && len != sd->expect_size) {
        sd->obs.object_errors++;
        return;
    }
    if (len >= 8 && sd->expect_objects != 0) {
        uint64_t idx = 0;

        for (size_t i = 0; i < 8; i++)
            idx |= (uint64_t)data[i] << (8 * i);
        if (idx >= sd->expect_objects) {
            sd->obs.object_errors++;
            return;
        }
        for (uint64_t j = 8; j < len; j++)
            if (data[j] != (uint8_t)(idx * 131u + j * 7u + 47u)) {
                sd->obs.object_errors++;
                return;
            }
    }
}

/* --- script actions (transport-worker context) ----------------------------- */

static void act_subscribe(wtqc_side_t *sd, moq_session_t *s)
{
    static const moq_bytes_t ns[] = {
        { (const uint8_t *)"wtqc", 4 },
    };
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    if (moq_session_subscribe(s, &sc, 0, &sub) < 0)
        sd->obs.publish_errors++;
}

static void act_accept(wtqc_side_t *sd, moq_session_t *s)
{
    moq_accept_subscribe_cfg_t ac;

    if (!sd->have_sub) {
        sd->obs.publish_errors++;
        return;
    }
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(s, sd->sub, &ac, 0) < 0)
        sd->obs.publish_errors++;
}

static void act_publish(wtqc_side_t *sd, moq_session_t *s, uint64_t n,
                        uint64_t size, bool reset, uint64_t reset_code)
{
    moq_subgroup_cfg_t sgc;
    moq_subgroup_handle_t sg;

    if (!sd->have_sub) {
        sd->obs.publish_errors++;
        return;
    }
    moq_subgroup_cfg_init(&sgc);
    /* each publish action is its own group: a group id cannot be
     * reused on a subscription once its subgroup ended */
    sgc.group_id = sd->next_group++;
    sgc.publisher_priority = 200;
    if (moq_session_open_subgroup(s, sd->sub, &sgc, 0, &sg) < 0) {
        sd->obs.publish_errors++;
        return;
    }
    for (uint64_t i = 0; i < n; i++) {
        uint8_t *bytes = malloc(size);
        moq_rcbuf_t *buf = NULL;

        if (bytes == NULL) {
            sd->obs.publish_errors++;
            break;
        }
        wtqc_payload_fill(bytes, size, i);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes, size, &buf);
        free(bytes);
        if (rc < 0) {
            sd->obs.publish_errors++;
            break;
        }
        if (moq_session_write_object(s, sg, i, buf, 0) < 0)
            sd->obs.publish_errors++;
        moq_rcbuf_decref(buf);
    }
    if (reset) {
        if (moq_session_reset_subgroup(s, sg, reset_code, 0) < 0)
            sd->obs.publish_errors++;
    } else {
        if (moq_session_close_subgroup(s, sg, 0) < 0)
            sd->obs.publish_errors++;
    }
}

static void run_action(wtqc_side_t *sd, const wtqc_step_t *st)
{
    moq_session_t *s = moq_wtquic_conn_session(sd->conn);

    switch (st->act) {
    case WTQC_DO_NOTHING:
        break;
    case WTQC_DO_SUBSCRIBE:
        act_subscribe(sd, s);
        break;
    case WTQC_DO_ACCEPT:
        act_accept(sd, s);
        break;
    case WTQC_DO_PUBLISH:
        act_publish(sd, s, st->a1, st->a2, false, 0);
        break;
    case WTQC_DO_PUBLISH_RESET:
        act_publish(sd, s, 1, st->a2, true, st->a1);
        break;
    case WTQC_DO_WT_CLOSE:
        (void)wtq_session_close(moq_wtquic_conn_wtq_session(sd->conn),
                                (uint32_t)st->a1, NULL, 0);
        break;
    case WTQC_DO_PROBE_DEAD: {
        static const moq_bytes_t ns[] = {
            { (const uint8_t *)"dead", 4 },
        };
        moq_subscribe_cfg_t sc;
        moq_subscription_t sub;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace.parts = (moq_bytes_t *)ns;
        sc.track_namespace.count = 1;
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        sd->obs.probe_rc = (int)moq_session_subscribe(s, &sc, 0, &sub);
        sd->obs.probe_ran = true;
        break;
    }
    }
}

static bool step_ready(const wtqc_side_t *sd, const wtqc_step_t *st)
{
    switch (st->when) {
    case WTQC_ON_SETUP:
        return sd->obs.setup > 0;
    case WTQC_ON_SUB_REQUEST:
        return sd->obs.sub_request > 0;
    case WTQC_ON_SUB_OK:
        return sd->obs.sub_ok > 0;
    case WTQC_ON_OBJECTS:
        return sd->obs.objects >= (int)st->when_arg;
    case WTQC_ON_OBJECTS_G1:
        return sd->obs.objects_g1 >= (int)st->when_arg;
    case WTQC_ON_CLOSED:
        return sd->obs.closed || sd->obs.fatal;
    }
    return false;
}

/* --- the adapter hook -------------------------------------------------------- */

static void wtqc_hook(moq_wtquic_conn_t *conn, void *user)
{
    wtqc_side_t *sd = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_event_t ev;

    /* the whole batch runs under the side mutex: the main thread reads
     * observations from its wait predicates, and it only ever blocks
     * in a condition wait, so holding the lock across the script
     * actions is safe */
    pthread_mutex_lock(&sd->mu);
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_SETUP_COMPLETE:
            sd->obs.setup++;
            break;
        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            sd->sub = ev.u.subscribe_request.sub;
            sd->have_sub = true;
            sd->obs.sub_request++;
            break;
        case MOQ_EVENT_SUBSCRIBE_OK:
            sd->obs.sub_ok++;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED:
            obs_verify_object(sd, &ev);
            sd->obs.objects++;
            if (ev.u.object_received.group_id == 1)
                sd->obs.objects_g1++;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            sd->obs.session_closed++;
            break;
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }

    sd->obs.closed = moq_wtquic_conn_is_closed(conn);
    sd->obs.fatal = moq_wtquic_conn_is_fatal(conn);

    /* advance the script: every satisfied step runs, in order */
    while (sd->cursor < sd->step_count &&
           step_ready(sd, &sd->steps[sd->cursor])) {
        const wtqc_step_t *st = &sd->steps[sd->cursor];

        sd->cursor++;
        run_action(sd, st);
    }

    pthread_cond_broadcast(&sd->cv);
    pthread_mutex_unlock(&sd->mu);
}

/* --- waits ------------------------------------------------------------------- */

static bool wait_until(wtqc_side_t *sd, bool (*pred)(const wtqc_side_t *,
                                                     const void *, int),
                       const void *field, int least)
{
    struct timespec deadline;
    bool ok = true;
    bool hit;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WTQC_WAIT_SECS;
    pthread_mutex_lock(&sd->mu);
    while (!pred(sd, field, least) && ok)
        ok = pthread_cond_timedwait(&sd->cv, &sd->mu, &deadline) == 0;
    hit = pred(sd, field, least);
    pthread_mutex_unlock(&sd->mu);
    return hit;
}

static bool pred_count(const wtqc_side_t *sd, const void *field, int least)
{
    (void)sd;
    return *(const int *)field >= least;
}

static bool pred_flag(const wtqc_side_t *sd, const void *field, int least)
{
    (void)sd;
    (void)least;
    return *(const bool *)field;
}

bool wtqc_wait_count(wtqc_side_t *sd, const int *field, int least)
{
    return wait_until(sd, pred_count, field, least);
}

bool wtqc_wait_flag(wtqc_side_t *sd, const bool *field)
{
    return wait_until(sd, pred_flag, field, 0);
}

/* --- pair lifecycle ----------------------------------------------------------- */

static int side_init(wtqc_side_t *sd, moq_perspective_t persp,
                     moq_session_t **ms_out)
{
    moq_session_cfg_t cfg;

    pthread_mutex_init(&sd->mu, NULL);
    pthread_cond_init(&sd->cv, NULL);
    sd->sync_init = true;

    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               persp);
    cfg.send_request_capacity = 1;
    cfg.initial_request_capacity = 16;
    cfg.max_events = 256;
    if (moq_session_create(&cfg, 0, ms_out) < 0)
        return -1;

    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = *ms_out;
    ccfg.hook = wtqc_hook;
    ccfg.hook_user = sd;
    if (moq_wtquic_conn_create(&ccfg, &sd->conn) < 0)
        return -1;
    return 0;
}

int wtqc_pair_setup(wtqc_pair_t *p, const wtqc_pair_cfg_t *cfg)
{
    const char *offer[1] = { cfg->client_offer };
    const char *serve[1] = { cfg->server_serve };

    if (side_init(&p->server, MOQ_PERSPECTIVE_SERVER, &p->server_ms) != 0)
        return -1;
    if (side_init(&p->client, MOQ_PERSPECTIVE_CLIENT, &p->client_ms) != 0)
        return -1;

    wtq_msquic_env_cfg_t ecfg = WTQ_MSQUIC_ENV_CFG_INIT;
    if (wtq_msquic_env_open(&ecfg, &p->env) != WTQ_OK)
        return -1;

    wtq_serve_config_t sv = WTQ_SERVE_CONFIG_INIT;
    sv.path = WTQC_PATH;
    sv.subprotocols = serve;
    sv.subprotocol_count = 1;
    sv.require_subprotocol = true;

    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cfg->cert;
    lcfg.key_file = cfg->key;
    lcfg.paths = &sv;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = p->server.conn;
    if (wtq_msquic_listener_start(p->env, &lcfg, &p->listener) != WTQ_OK)
        return -1;

    wtq_connect_config_t wcfg = WTQ_CONNECT_CONFIG_INIT;
    wcfg.authority = "localhost";
    wcfg.path = WTQC_PATH;
    wcfg.subprotocols = offer;
    wcfg.subprotocol_count = 1;
    wcfg.require_subprotocol = true;

    wtq_msquic_client_cfg_t cli = WTQ_MSQUIC_CLIENT_CFG_INIT;
    cli.server_name = "127.0.0.1";
    cli.port = wtq_msquic_listener_port(p->listener);
    cli.insecure_skip_verify = true;
    cli.connect = &wcfg;
    cli.events = moq_wtquic_conn_events();
    cli.user = p->client.conn;
    if (wtq_msquic_client_connect(p->env, &cli, &p->cs) != WTQ_OK)
        return -1;
    return 0;
}

static void side_teardown(wtqc_side_t *sd, moq_session_t *ms)
{
    if (sd->conn != NULL)
        moq_wtquic_conn_destroy(sd->conn);
    if (ms != NULL)
        moq_session_destroy(ms);
    if (sd->sync_init) {
        pthread_mutex_destroy(&sd->mu);
        pthread_cond_destroy(&sd->cv);
        sd->sync_init = false;
    }
}

void wtqc_pair_teardown(wtqc_pair_t *p)
{
    if (p->listener != NULL)
        wtq_msquic_listener_stop(p->listener);
    if (p->env != NULL)
        wtq_msquic_env_close(p->env); /* the quiescence barrier */
    if (p->cs != NULL)
        wtq_session_release(p->cs);
    side_teardown(&p->client, p->client_ms);
    side_teardown(&p->server, p->server_ms);
    /* handles are gone; observations stay readable for the asserts */
    p->listener = NULL;
    p->env = NULL;
    p->cs = NULL;
    p->client_ms = NULL;
    p->server_ms = NULL;
    p->client.conn = NULL;
    p->server.conn = NULL;
}
