/*
 * Production-contract conformance pair runner: each side's managed
 * on_lane_pump drains MoQ events into observations, advances the side's
 * script, and signals the main thread. See msquic_contract_pair.h for
 * the model.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "msquic_contract_pair.h"

#include <moq/rcbuf.h>

#define MSQC_WAIT_SECS 30

void msqc_payload_fill(uint8_t *dst, uint64_t size, uint64_t idx)
{
    for (size_t i = 0; i < 8 && i < size; i++)
        dst[i] = (uint8_t)(idx >> (8 * i));
    for (uint64_t j = 8; j < size; j++)
        dst[j] = (uint8_t)(idx * 131u + j * 7u + 47u);
}

/* Every published payload is index-tagged, so verification is
 * unconditional: the embedded index must equal the event's object_id,
 * the pattern bytes must match it, the length must match the per-group
 * expectation when one is set, and each (group, id) may arrive exactly
 * once (the per-group bitmasks let scenarios assert exact coverage). */
static void obs_verify_object(msqc_side_t *sd, const moq_event_t *ev)
{
    const moq_rcbuf_t *payload = ev->u.object_received.payload;
    uint64_t group = ev->u.object_received.group_id;
    uint64_t oid = ev->u.object_received.object_id;

    if (ev->u.object_received.datagram)
        sd->obs.dgram_objects++;
    if (payload == NULL) {
        /* only the scripted status datagram may arrive payload-less,
         * and its metadata must match the expectation exactly */
        if (ev->u.object_received.datagram && sd->expect_status_set &&
            group == sd->expect_status_group &&
            oid == sd->expect_status_object &&
            ev->u.object_received.status == sd->expect_status_value)
            sd->obs.status_objects++;
        else
            sd->obs.object_errors++;
        return;
    }
    size_t len = moq_rcbuf_len(payload);
    const uint8_t *data = moq_rcbuf_data(payload);

    sd->obs.object_bytes += len;
    if (len < 8) {
        sd->obs.object_errors++;
        return;
    }
    uint64_t idx = 0;
    for (size_t i = 0; i < 8; i++)
        idx |= (uint64_t)data[i] << (8 * i);
    if (idx != oid) {
        sd->obs.object_errors++; /* payload belongs to another object */
        return;
    }
    uint64_t expect = group == 1 ? sd->expect_size_g1 : sd->expect_size;
    if (expect != 0 && len != expect) {
        sd->obs.object_errors++;
        return;
    }
    for (uint64_t j = 8; j < len; j++)
        if (data[j] != (uint8_t)(idx * 131u + j * 7u + 47u)) {
            sd->obs.object_errors++;
            return;
        }
    if (group <= 1 && oid < 64) {
        uint64_t *seen = group == 1 ? &sd->obs.seen_g1 : &sd->obs.seen_g0;
        uint64_t bit = 1ull << oid;

        if (*seen & bit) {
            sd->obs.object_errors++; /* duplicate delivery */
            return;
        }
        *seen |= bit;
    }
}

/* --- script actions (on_lane_pump context) --------------------------------------- */

static void act_subscribe(msqc_side_t *sd, moq_session_t *s,
                          uint64_t now)
{
    static const moq_bytes_t ns[] = {
        { (const uint8_t *)"msqc", 4 },
    };
    moq_subscribe_cfg_t sc;
    moq_subscription_t sub;

    moq_subscribe_cfg_init(&sc);
    sc.track_namespace.parts = (moq_bytes_t *)ns;
    sc.track_namespace.count = 1;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    if (moq_session_subscribe(s, &sc, now, &sub) < 0)
        sd->obs.publish_errors++;
}

static void act_accept(msqc_side_t *sd, moq_session_t *s, uint64_t now)
{
    moq_accept_subscribe_cfg_t ac;

    if (!sd->have_sub) {
        sd->obs.publish_errors++;
        return;
    }
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(s, sd->sub, &ac, now) < 0)
        sd->obs.publish_errors++;
}

static void act_publish(msqc_side_t *sd, moq_session_t *s, uint64_t now,
                        uint64_t n, uint64_t size, bool reset,
                        uint64_t reset_code)
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
    if (moq_session_open_subgroup(s, sd->sub, &sgc, now, &sg) < 0) {
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
        msqc_payload_fill(bytes, size, i);
        int rc = moq_rcbuf_create(moq_alloc_default(), bytes, size, &buf);
        free(bytes);
        if (rc < 0) {
            sd->obs.publish_errors++;
            break;
        }
        if (moq_session_write_object(s, sg, i, buf, now) < 0)
            sd->obs.publish_errors++;
        moq_rcbuf_decref(buf);
    }
    if (reset) {
        if (moq_session_reset_subgroup(s, sg, reset_code, now) < 0)
            sd->obs.publish_errors++;
    } else {
        if (moq_session_close_subgroup(s, sg, now) < 0)
            sd->obs.publish_errors++;
    }
}

static void run_action(msqc_side_t *sd, moq_session_t *s, uint64_t now,
                       const msqc_step_t *st)
{
    switch (st->act) {
    case MSQC_DO_NOTHING:
        break;
    case MSQC_DO_SUBSCRIBE:
        act_subscribe(sd, s, now);
        break;
    case MSQC_DO_ACCEPT:
        act_accept(sd, s, now);
        break;
    case MSQC_DO_PUBLISH:
        act_publish(sd, s, now, st->a1, st->a2, false, 0);
        break;
    case MSQC_DO_PUBLISH_RESET:
        act_publish(sd, s, now, 1, st->a2, true, st->a1);
        break;
    case MSQC_DO_PUBLISH_DGRAM: {
        uint8_t *bytes = malloc(st->a2);
        moq_rcbuf_t *buf = NULL;

        if (!sd->have_sub || bytes == NULL) {
            sd->obs.publish_errors++;
            free(bytes);
            break;
        }
        msqc_payload_fill(bytes, st->a2, 0);
        if (moq_rcbuf_create(moq_alloc_default(), bytes, st->a2,
                             &buf) < 0) {
            sd->obs.publish_errors++;
            free(bytes);
            break;
        }
        free(bytes);
        if (moq_session_send_object_datagram(s, sd->sub, st->a1, 0, 200,
                                             false, buf, NULL, 0,
                                             now) < 0)
            sd->obs.publish_errors++;
        moq_rcbuf_decref(buf);
        break;
    }
    case MSQC_DO_PUBLISH_DGRAM_STATUS:
        if (!sd->have_sub) {
            sd->obs.publish_errors++;
            break;
        }
        if (moq_session_send_status_datagram(
                s, sd->sub, st->a1, 0, 200,
                MOQ_OBJECT_END_OF_GROUP, now) < 0)
            sd->obs.publish_errors++;
        break;
    case MSQC_DO_CLOSE:
        (void)moq_session_close(s, st->a1, NULL, now);
        break;
    case MSQC_DO_PROBE_DEAD: {
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
        sd->obs.probe_rc = (int)moq_session_subscribe(s, &sc, now, &sub);
        sd->obs.probe_ran = true;
        break;
    }
    }
}

static bool step_ready(const msqc_side_t *sd, const msqc_step_t *st)
{
    switch (st->when) {
    case MSQC_ON_SETUP:
        return sd->obs.setup > 0;
    case MSQC_ON_SUB_REQUEST:
        return sd->obs.sub_request > 0;
    case MSQC_ON_SUB_OK:
        return sd->obs.sub_ok > 0;
    case MSQC_ON_OBJECTS:
        return sd->obs.objects >= (int)st->when_arg;
    case MSQC_ON_OBJECTS_G1:
        return sd->obs.objects_g1 >= (int)st->when_arg;
    case MSQC_ON_CLOSED:
        return sd->obs.closed || sd->obs.fatal;
    }
    return false;
}

/* --- the managed pump -------------------------------------------------------- */

static int msqc_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now,
                     void *user)
{
    msqc_side_t *sd = user;
    /* each side runs exactly one connection: the client's single session
     * and the server's single accepted one both surface through their
     * lane's iteration — no managed_session() (which is client-only). */
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c)
                                 : NULL;
    moq_event_t ev;

    /* the whole batch runs under the side mutex: the main thread reads
     * observations from its wait predicates, and it only ever blocks
     * in a condition wait, so holding the lock across the script
     * actions is safe */
    pthread_mutex_lock(&sd->mu);
    while (s != NULL && moq_session_poll_events(s, &ev, 1) > 0) {
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

    /* terminal accessors are callback-context safe by contract */
    sd->obs.closed = moq_msquic_managed_is_closed(m);
    sd->obs.fatal = moq_msquic_managed_is_fatal(m);
    if (sd->obs.closed)
        sd->obs.close_code = moq_msquic_managed_close_code(m);
    if (sd->obs.fatal)
        sd->obs.fatal_code = moq_msquic_managed_fatal_code(m);
    sd->obs.pending_sends = moq_msquic_managed_pending_sends(m);
    sd->obs.pending_dgrams = moq_msquic_managed_pending_datagrams(m);

    /* advance the script: every satisfied step runs, in order */
    while (s != NULL && sd->cursor < sd->step_count &&
           step_ready(sd, &sd->steps[sd->cursor])) {
        const msqc_step_t *st = &sd->steps[sd->cursor];

        sd->cursor++;
        run_action(sd, s, now, st);
    }

    pthread_cond_broadcast(&sd->cv);
    pthread_mutex_unlock(&sd->mu);
    return 0;
}

/* --- waits ------------------------------------------------------------------- */

static bool wait_until(msqc_side_t *sd, bool (*pred)(const msqc_side_t *,
                                                     const void *, int),
                       const void *field, int least)
{
    struct timespec deadline;
    bool ok = true;
    bool hit;

    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += MSQC_WAIT_SECS;
    pthread_mutex_lock(&sd->mu);
    while (!pred(sd, field, least) && ok)
        ok = pthread_cond_timedwait(&sd->cv, &sd->mu, &deadline) == 0;
    hit = pred(sd, field, least);
    pthread_mutex_unlock(&sd->mu);
    return hit;
}

static bool pred_count(const msqc_side_t *sd, const void *field, int least)
{
    (void)sd;
    return *(const int *)field >= least;
}

static bool pred_flag(const msqc_side_t *sd, const void *field, int least)
{
    (void)sd;
    (void)least;
    return *(const bool *)field;
}

bool msqc_wait_count(msqc_side_t *sd, const int *field, int least)
{
    return wait_until(sd, pred_count, field, least);
}

bool msqc_wait_flag(msqc_side_t *sd, const bool *field)
{
    return wait_until(sd, pred_flag, field, 0);
}

/* --- pair lifecycle ----------------------------------------------------------- */

static void side_init(msqc_side_t *sd)
{
    pthread_mutex_init(&sd->mu, NULL);
    pthread_cond_init(&sd->cv, NULL);
    sd->sync_init = true;
}

int msqc_pair_setup(msqc_pair_t *p, const msqc_pair_cfg_t *cfg)
{
    /* Hold one library reference for the whole process: without it,
     * every pair teardown drops the last MsQuicClose reference and the
     * next setup re-initializes MsQuic's global state (registrations,
     * datapath) from scratch — churn that both slows soaks and has
     * shown rare transient connect failures mid-run. Intentionally
     * never released; the pointer keeps it reachable for leak
     * checkers and the OS reclaims at exit. */
    static const QUIC_API_TABLE *msqc_lib_pin;

    if (msqc_lib_pin == NULL &&
        QUIC_FAILED(MsQuicOpen2(&msqc_lib_pin)))
        return -1;

    side_init(&p->server);
    side_init(&p->client);

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0; /* ephemeral */
    scfg.cert_path = cfg->cert;
    scfg.key_path = cfg->key;
    scfg.on_lane_pump = msqc_pump;
    scfg.on_lane_pump_user = &p->server;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 256;
    scfg.version = cfg->server_version;
    if (moq_msquic_managed_create(&scfg, &p->server.m) != MOQ_OK)
        return -1;
    uint16_t port = moq_msquic_managed_port(p->server.m);
    if (port == 0)
        return -1;

    moq_msquic_managed_cfg_t ccfg;
    moq_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = msqc_pump;
    ccfg.on_lane_pump_user = &p->client;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    ccfg.max_events = 256;
    ccfg.version = cfg->client_version;
    if (moq_msquic_managed_create(&ccfg, &p->client.m) != MOQ_OK)
        return -1;
    return 0;
}

static void side_teardown(msqc_side_t *sd)
{
    if (sd->m != NULL) {
        /* stop is the quiescence barrier: after it returns, no pump
         * can run and the observations are stable */
        (void)moq_msquic_managed_stop(sd->m);
        moq_msquic_managed_destroy(sd->m);
        sd->m = NULL;
    }
    if (sd->sync_init) {
        pthread_mutex_destroy(&sd->mu);
        pthread_cond_destroy(&sd->cv);
        sd->sync_init = false;
    }
}

void msqc_pair_teardown(msqc_pair_t *p)
{
    side_teardown(&p->client);
    side_teardown(&p->server);
    /* handles are gone; observations stay readable for the asserts */
}
