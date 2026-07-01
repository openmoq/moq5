/*
 * ASPIRATIONAL — does not compile. This is the API we wish existed.
 *
 * Goal: show the complete polling loop that a QUIC adapter uses to
 * drive a moq_session_t. This is the glue between libmoq and any
 * QUIC stack (picoquic, quiche, msquic, etc.).
 *
 * Lifetime categories (advancing vs observing):
 *   Advancing: on_stream_bytes, on_stream_opened, on_stream_reset,
 *     on_datagram, on_timer, subscribe, subscribe_ok, subscribe_error,
 *     publish_object, unsubscribe, announce_namespace, bind_stream,
 *     reject_open, close, goaway.
 *   Observing: poll_events, poll_actions, next_deadline_us, session_state.
 *   Within a single poll batch, all pointers are stable until the next
 *   advancing call.
 */

#include <moq/moq.h>
#include <stdio.h>

/* ── Pseudocode QUIC interface ─────────────────────────────────────── */

typedef struct quic_conn quic_conn_t;

int      quic_stream_recv(quic_conn_t *qc, uint64_t sid, uint8_t *buf,
                          size_t cap, bool *fin);
int      quic_stream_send(quic_conn_t *qc, uint64_t sid, const uint8_t *buf,
                          size_t len, bool fin);
uint64_t quic_open_uni_stream(quic_conn_t *qc);
uint64_t quic_open_bidi_stream(quic_conn_t *qc);
void     quic_reset_stream(quic_conn_t *qc, uint64_t sid, uint64_t code);
void     quic_stop_sending(quic_conn_t *qc, uint64_t sid, uint64_t code);
void     quic_send_datagram(quic_conn_t *qc, const uint8_t *buf, size_t len);
void     quic_close(quic_conn_t *qc, uint64_t code);
uint64_t quic_next_readable_stream(quic_conn_t *qc);
uint64_t quic_timeout_us(quic_conn_t *qc);
uint64_t get_monotonic_us(void);
void     app_handle_event(const moq_event_t *ev);

#define QUIC_NO_STREAM UINT64_MAX

/* ── Adapter loop ──────────────────────────────────────────────────── */

void run_adapter(quic_conn_t *qc, moq_session_t *moq)
{
    uint8_t stream_buf[4096];
    moq_action_t acts[64];
    moq_event_t  evts[64];

    for (;;) {
        uint64_t now = get_monotonic_us();

        /* ── 1. Feed QUIC stream bytes into MoQ (advancing) ────── */

        uint64_t sid;
        while ((sid = quic_next_readable_stream(qc)) != QUIC_NO_STREAM) {
            bool fin = false;
            int r = quic_stream_recv(qc, sid, stream_buf, sizeof(stream_buf), &fin);
            if (r > 0 || fin) {
                moq_stream_id_t id = moq_stream_id_from_u64(sid);
                moq_session_on_stream_bytes(moq, id, stream_buf, (size_t)r,
                                            fin, now);
            }
        }

        /* ── 2. Fire timers (advancing) ────────────────────────── */

        moq_session_on_timer(moq, now);

        /* ── 3. Drain actions → execute on QUIC stack (observing) ─ */

        size_t n;
        while ((n = moq_session_poll_actions(moq, acts, 64)) > 0) {
            for (size_t i = 0; i < n; i++) {
                moq_action_t *a = &acts[i];
                switch (a->kind) {

                case MOQ_ACTION_SEND_CONTROL:
                    quic_stream_send(qc, a->stream_id._v,
                                    a->data, a->data_len, false);
                    break;

                case MOQ_ACTION_OPEN_UNI_STREAM: {
                    uint64_t real_sid = quic_open_uni_stream(qc);
                    if (real_sid != QUIC_NO_STREAM) {
                        moq_session_bind_stream(moq, a->stream_ref,
                                               moq_stream_id_from_u64(real_sid),
                                               now);
                    } else {
                        moq_session_reject_open(moq, a->stream_ref,
                                               0x1 /* stream limit */, now);
                    }
                    break;
                }

                case MOQ_ACTION_OPEN_BIDI_STREAM: {
                    uint64_t real_sid = quic_open_bidi_stream(qc);
                    if (real_sid != QUIC_NO_STREAM) {
                        moq_session_bind_stream(moq, a->stream_ref,
                                               moq_stream_id_from_u64(real_sid),
                                               now);
                    } else {
                        moq_session_reject_open(moq, a->stream_ref,
                                               0x1, now);
                    }
                    break;
                }

                case MOQ_ACTION_SEND_STREAM:
                    quic_stream_send(qc, a->stream_id._v,
                                    a->data, a->data_len, false);
                    break;

                case MOQ_ACTION_FIN_STREAM:
                    quic_stream_send(qc, a->stream_id._v, NULL, 0, true);
                    break;

                case MOQ_ACTION_RESET_STREAM:
                    quic_reset_stream(qc, a->stream_id._v, a->error_code);
                    break;

                case MOQ_ACTION_STOP_SENDING:
                    quic_stop_sending(qc, a->stream_id._v, a->error_code);
                    break;

                case MOQ_ACTION_SEND_DATAGRAM:
                    quic_send_datagram(qc, a->data, a->data_len);
                    break;

                case MOQ_ACTION_CLOSE_SESSION:
                    quic_close(qc, a->error_code);
                    return;

                default:
                    break;
                }
            }
        }

        /* ── 4. Drain events → deliver to application (observing) ─ */

        while ((n = moq_session_poll_events(moq, evts, 64)) > 0) {
            for (size_t i = 0; i < n; i++) {
                app_handle_event(&evts[i]);
            }
        }

        /* ── 5. Sleep until next deadline ──────────────────────── */

        uint64_t moq_deadline = moq_session_next_deadline_us(moq);
        uint64_t quic_deadline = quic_timeout_us(qc);
        uint64_t wake = moq_deadline < quic_deadline ? moq_deadline : quic_deadline;
        (void)wake; /* platform_sleep_until(wake); */
    }
}
