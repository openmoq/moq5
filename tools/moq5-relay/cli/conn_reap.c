#include "conn_reap.h"

#include <moq/session.h>

bool moqr_relay_reap_pass(moqr_bind_t *bind, moq_msquic_managed_lane_t *lane,
                          moqr_reap_stats_t *st)
{
    if (bind == NULL || lane == NULL) {
        return false;   /* no binding or no lane to retire against: fail closed */
    }
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane,
                                                                     NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_msquic_managed_conn_session(conn);

        if (s == NULL) {
            continue;
        }
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == MOQR_CONN_OPENED && !moqr_bind_conn_is_open(bind, s)) {
            /* The binding detached; the connection stays visible until it is
             * acknowledged and reclaimed — never re-attach it. */
            moq_msquic_managed_conn_set_user(conn, MOQR_CONN_DEAD);
            tag = MOQR_CONN_DEAD;
        }
        if (tag != MOQR_CONN_DEAD) {
            continue;
        }
        /* Only now, with the binding no longer an owner, is draining this
         * session ours to do. Poll to exhaustion: the terminal is only
         * acknowledgeable once it has actually been transferred. */
        moq_event_t evs[16];
        size_t n;
        while ((n = moq_session_poll_events(s, evs, 16)) > 0) {
            for (size_t e = 0; e < n; e++) {
                moq_event_cleanup(&evs[e]);
            }
        }
        moq_result_t rc = moq_msquic_managed_conn_ack_terminal(conn);
        if (rc == MOQ_OK) {
            if (st != NULL) {
                st->acked++;
            }
        } else if (rc == MOQ_ERR_WRONG_STATE) {
            /* Not an error: the terminal has not arrived yet. The connection
             * is retained, and the next pump retries. */
            if (st != NULL) {
                st->retained++;
            }
        } else {
            return false;
        }
    }
    return true;
}

#ifdef MOQR_DUAL_LISTENER
bool moqr_relay_reap_pass_wt(moqr_bind_t *bind,
                             moq_wtquic_msquic_managed_lane_t *lane,
                             moqr_reap_stats_t *st)
{
    if (bind == NULL || lane == NULL) {
        return false;
    }
    for (moq_wtquic_msquic_managed_conn_t *conn =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         conn != NULL;
         conn = moq_wtquic_msquic_lane_next_conn(lane, conn)) {
        moq_session_t *s = moq_wtquic_msquic_managed_conn_session(conn);

        if (s == NULL) {
            continue;
        }
        void *tag = moq_wtquic_msquic_managed_conn_user(conn);
        if (tag == MOQR_CONN_OPENED && !moqr_bind_conn_is_open(bind, s)) {
            moq_wtquic_msquic_managed_conn_set_user(conn, MOQR_CONN_DEAD);
            tag = MOQR_CONN_DEAD;
        }
        if (tag != MOQR_CONN_DEAD) {
            continue;
        }
        moq_event_t evs[16];
        size_t n;
        while ((n = moq_session_poll_events(s, evs, 16)) > 0) {
            for (size_t e = 0; e < n; e++) {
                moq_event_cleanup(&evs[e]);
            }
        }
        moq_result_t rc = moq_wtquic_msquic_managed_conn_ack_terminal(conn);
        if (rc == MOQ_OK) {
            if (st != NULL) {
                st->acked++;
            }
        } else if (rc == MOQ_ERR_WRONG_STATE) {
            if (st != NULL) {
                st->retained++;
            }
        } else {
            return false;
        }
    }
    return true;
}
#endif /* MOQR_DUAL_LISTENER */
