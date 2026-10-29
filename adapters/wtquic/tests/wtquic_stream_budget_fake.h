#ifndef MOQ_TEST_WTQUIC_STREAM_BUDGET_FAKE_H
#define MOQ_TEST_WTQUIC_STREAM_BUDGET_FAKE_H

/* Explicit fixture geometry, not a provider default: 15 peer + 1 local. */
enum { TEST_STREAM_CAPACITY = 16 };
static wtq_stream_budget_t test_budget = {
    .struct_size = sizeof(wtq_stream_budget_t),
    .peer_unidi_stream_count = 8,
    .peer_bidi_stream_count = 7,
    .max_local_streams = 1,
    .engine_capacity = TEST_STREAM_CAPACITY,
    .api_capacity = TEST_STREAM_CAPACITY,
    .native_capacity = TEST_STREAM_CAPACITY + 3,
};
static wtq_result_t test_budget_rc = WTQ_OK;
static unsigned test_budget_calls, test_budget_bad_size;

wtq_result_t wtq_session_stream_budget(const wtq_session_t *s,
                                      wtq_stream_budget_t *out)
{
    test_budget_calls++;
    if (!s || !out) return WTQ_ERR_INVALID_ARG;
    if (out->struct_size < WTQ_STREAM_BUDGET_V1_SIZE) {
        test_budget_bad_size++;
        return WTQ_ERR_INVALID_ARG;
    }
    if (test_budget_rc != WTQ_OK) return test_budget_rc;
    uint32_t caller_size = out->struct_size;
    size_t n = caller_size < sizeof(*out) ? caller_size : sizeof(*out);
    memcpy(out, &test_budget, n);
    /* A non-current size in test_budget deliberately models a broken query. */
    if (test_budget.struct_size == sizeof(test_budget))
        out->struct_size = caller_size;
    return WTQ_OK;
}

#endif
