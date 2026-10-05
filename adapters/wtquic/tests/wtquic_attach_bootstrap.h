#ifndef WTQUIC_ATTACH_BOOTSTRAP_H
#define WTQUIC_ATTACH_BOOTSTRAP_H
#include <moq/wtquic.h>

/* Test-owned context outlives provider quiescence. No adapter exists on a
 * pre-establishment failure. Publish the adapter before forwarding any event. */
typedef struct {
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_t **out;
    moq_result_t result;
    void (*failed)(void *);
    bool failure_reported;
} wtq_test_attach_t;

/* The context remains owned by the test until provider quiescence, including
 * after failed() returns. Latch before calling out to survive reentrant close. */
static void attach_fail_once(wtq_test_attach_t *b, moq_result_t result)
{
    if (b->failure_reported) return;
    b->result = result;
    b->failure_reported = true;
    if (b->failed) b->failed(b->cfg.hook_user);
}

static void attach_established(wtq_session_t *s, wtq_str_t sub, void *user)
{
    wtq_test_attach_t *b = user;
    if (b->failure_reported || *b->out) return;
    b->cfg.wt_session = s;
    b->result = moq_wtquic_conn_create(&b->cfg, b->out);
    if (b->result != MOQ_OK) {
        attach_fail_once(b, b->result);
        (void)wtq_session_close(s, 0, NULL, 0);
        return;
    }
    moq_wtquic_conn_events()->on_established(s, sub, *b->out);
}

#define ATTACH_FORWARD(name, params, args) \
static void attach_##name params { \
    wtq_test_attach_t *b = user; \
    if (*b->out) moq_wtquic_conn_events()->on_##name args; \
}
ATTACH_FORWARD(stream_opened, (wtq_session_t *s, wtq_stream_t *st, bool bidi, void *user), (s, st, bidi, *b->out))
ATTACH_FORWARD(stream_data, (wtq_session_t *s, wtq_stream_t *st, const uint8_t *p, size_t n, bool fin, void *user), (s, st, p, n, fin, *b->out))
ATTACH_FORWARD(stream_reset, (wtq_session_t *s, wtq_stream_t *st, uint32_t code, void *user), (s, st, code, *b->out))
ATTACH_FORWARD(stream_stop, (wtq_session_t *s, wtq_stream_t *st, uint32_t code, void *user), (s, st, code, *b->out))
ATTACH_FORWARD(stream_closed, (wtq_session_t *s, wtq_stream_t *st, void *user), (s, st, *b->out))
ATTACH_FORWARD(stream_writable, (wtq_session_t *s, wtq_stream_t *st, void *user), (s, st, *b->out))
ATTACH_FORWARD(send_complete, (wtq_session_t *s, void *ctx, bool canceled, void *user), (s, ctx, canceled, *b->out))
ATTACH_FORWARD(datagram, (wtq_session_t *s, const uint8_t *p, size_t n, void *user), (s, p, n, *b->out))
#undef ATTACH_FORWARD

static void attach_refused(wtq_session_t *s, uint16_t status, void *user)
{
    wtq_test_attach_t *b = user;
    if (*b->out) moq_wtquic_conn_events()->on_refused(s, status, *b->out);
    else attach_fail_once(b, MOQ_ERR_CLOSED);
}
static void attach_failed(wtq_session_t *s, wtq_connect_failure_t why, void *user)
{
    wtq_test_attach_t *b = user;
    if (*b->out) moq_wtquic_conn_events()->on_failed(s, why, *b->out);
    else attach_fail_once(b, MOQ_ERR_CLOSED);
}
static void attach_closed(wtq_session_t *s, uint32_t code, const uint8_t *p,
                          size_t n, bool clean, void *user)
{
    wtq_test_attach_t *b = user;
    if (*b->out) moq_wtquic_conn_events()->on_closed(s, code, p, n, clean, *b->out);
    else attach_fail_once(b, MOQ_ERR_CLOSED);
}
static inline const wtq_session_events_t *wtq_test_attach_events(void)
{
static const wtq_session_events_t events = {
    .struct_size = sizeof(wtq_session_events_t),
    .on_established = attach_established, .on_refused = attach_refused,
    .on_failed = attach_failed, .on_closed = attach_closed,
    .on_stream_opened = attach_stream_opened, .on_stream_data = attach_stream_data,
    .on_stream_reset = attach_stream_reset, .on_stream_stop = attach_stream_stop,
    .on_stream_closed = attach_stream_closed, .on_stream_writable = attach_stream_writable,
    .on_send_complete = attach_send_complete, .on_datagram = attach_datagram,
};
return &events;
}
#endif
