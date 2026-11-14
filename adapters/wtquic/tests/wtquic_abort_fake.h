#ifndef MOQ_TEST_WTQUIC_ABORT_FAKE_H
#define MOQ_TEST_WTQUIC_ABORT_FAKE_H

/* The stream-abort observations the fake provider states for every session:
 * all three facts unless a row narrows them. */
static uint32_t test_abort_observability =
    WTQ_ABORT_OBSERVES_RESET_CODE | WTQ_ABORT_OBSERVES_STOP_EVENT |
    WTQ_ABORT_OBSERVES_STOP_CODE;
static unsigned test_abort_observability_calls;

uint32_t wtq_session_abort_observability(const wtq_session_t *s)
{
    (void)s;
    test_abort_observability_calls++;
    return test_abort_observability;
}

#endif
