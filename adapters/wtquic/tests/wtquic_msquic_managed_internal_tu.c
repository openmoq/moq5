/* Replace the separate managed TU in this internal-test target. The included
 * production source supplies both the actual event table and its forwarder. */
#ifndef MOQ_WTQ_MM_TESTING
#error "internal fixture requires MOQ_WTQ_MM_TESTING"
#endif
#include "../wtquic_msquic_managed.c"
#include "wtquic_managed_forwarding_test.h"

wtq_test_forward_table_t wtq_test_msquic_commit_dispatch(
    const wtq_session_events_t *forward, wtq_session_t *session,
    wtq_stream_t *stream, moq_wtquic_conn_t *adapter)
{
    moq_wtquic_msquic_managed_t m = {.fwd = forward};
    moq_wtquic_msquic_managed_conn_t c = {.m = &m, .adapter = adapter};
    wtq_test_forward_table_t result = {
        .table_size = g_mm_events.struct_size,
        .installed = g_mm_events.on_stream_send_committed != NULL,
    };
    if (result.installed) {
        g_mm_events.on_stream_send_committed(session, stream, &c);
        g_mm_events.on_stream_send_committed(session, stream, &c);
        c.adapter = NULL;
        g_mm_events.on_stream_send_committed(session, stream, &c);
    }
    return result;
}

wtq_test_forward_table_t wtq_test_msquic_abort_dispatch(
    const wtq_session_events_t *forward, wtq_session_t *session,
    wtq_stream_t *stream, const wtq_stream_abort_t *rec,
    moq_wtquic_conn_t *adapter)
{
    moq_wtquic_msquic_managed_t m = {.fwd = forward};
    moq_wtquic_msquic_managed_conn_t c = {.m = &m, .adapter = adapter};
    wtq_test_forward_table_t result = {
        .table_size = g_mm_events.struct_size,
        .installed = g_mm_events.on_stream_abort != NULL,
    };
    if (result.installed) {
        g_mm_events.on_stream_abort(session, stream, rec, &c);
        c.adapter = NULL;
        g_mm_events.on_stream_abort(session, stream, rec, &c);
    }
    return result;
}
