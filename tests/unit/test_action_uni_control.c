/*
 * Focused white-box tests for unidirectional control-channel actions
 * (MOQ_ACTION_OPEN_UNI_CONTROL / MOQ_ACTION_SEND_UNI_CONTROL).
 *
 * These are a version-neutral action-model seam: profiles that carry control
 * messages on a local unidirectional channel, rather than on the shared
 * bidirectional control channel used by draft-16, emit them. This slice adds
 * the action kinds and payload storage only -- no profile emits them yet, and
 * no wire behavior is introduced.
 *
 * Coverage: payload round-trip through the action queue, FIFO ordering when
 * interleaved with existing actions, cleanup is a no-op (the payloads carry
 * only borrowed pointers, no owned refs), and the size-aware drain treats them
 * as non-owned (never ABI_MISMATCH).
 */
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

static moq_session_t *make_session(moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);
    return s;
}

int main(void)
{
    int failures = 0;

    static const uint8_t setup_bytes[] = { 0x01, 0x02, 0x03, 0x04 };
    static const uint8_t more_bytes[]  = { 0xAA, 0xBB };

    /* == A. Payload round-trip through the action queue =============== */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc);
        MOQ_TEST_CHECK(s != NULL);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x4242);

        moq_action_t open_act;
        memset(&open_act, 0, sizeof(open_act));
        open_act.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        open_act.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        open_act.borrow_epoch = s->borrow_epoch;
        open_act.u.open_uni_control.stream_ref = ref;
        open_act.u.open_uni_control.data = setup_bytes;
        open_act.u.open_uni_control.len  = sizeof(setup_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(s, &open_act), (int)MOQ_OK);

        moq_action_t send_act;
        memset(&send_act, 0, sizeof(send_act));
        send_act.kind = MOQ_ACTION_SEND_UNI_CONTROL;
        send_act.detail_size = (uint32_t)sizeof(moq_send_uni_control_action_t);
        send_act.borrow_epoch = s->borrow_epoch;
        send_act.u.send_uni_control.stream_ref = ref;
        send_act.u.send_uni_control.data = more_bytes;
        send_act.u.send_uni_control.len  = sizeof(more_bytes);
        send_act.u.send_uni_control.fin  = true;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(s, &send_act), (int)MOQ_OK);

        moq_action_t out[4];
        size_t n = moq_session_poll_actions(s, out, 4);
        MOQ_TEST_CHECK_EQ_SIZE(n, 2);

        MOQ_TEST_CHECK_EQ_U64(out[0].kind, MOQ_ACTION_OPEN_UNI_CONTROL);
        MOQ_TEST_CHECK_EQ_SIZE(out[0].detail_size,
            sizeof(moq_open_uni_control_action_t));
        MOQ_TEST_CHECK_EQ_U64(out[0].u.open_uni_control.stream_ref._v, 0x4242);
        MOQ_TEST_CHECK(out[0].u.open_uni_control.data == setup_bytes);
        MOQ_TEST_CHECK_EQ_SIZE(out[0].u.open_uni_control.len,
            sizeof(setup_bytes));

        MOQ_TEST_CHECK_EQ_U64(out[1].kind, MOQ_ACTION_SEND_UNI_CONTROL);
        MOQ_TEST_CHECK_EQ_SIZE(out[1].detail_size,
            sizeof(moq_send_uni_control_action_t));
        MOQ_TEST_CHECK_EQ_U64(out[1].u.send_uni_control.stream_ref._v, 0x4242);
        MOQ_TEST_CHECK(out[1].u.send_uni_control.data == more_bytes);
        MOQ_TEST_CHECK_EQ_SIZE(out[1].u.send_uni_control.len,
            sizeof(more_bytes));
        MOQ_TEST_CHECK(out[1].u.send_uni_control.fin);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == B. FIFO ordering preserved when interleaved with an existing ==
     *       action kind (SEND_CONTROL). */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc);

        moq_action_kind_t order[3] = {
            MOQ_ACTION_OPEN_UNI_CONTROL,
            MOQ_ACTION_SEND_CONTROL,
            MOQ_ACTION_SEND_UNI_CONTROL,
        };
        for (size_t i = 0; i < 3; i++) {
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = order[i];
            a.borrow_epoch = s->borrow_epoch;
            if (order[i] == MOQ_ACTION_SEND_CONTROL) {
                a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
                a.u.send_control.data = setup_bytes;
                a.u.send_control.len  = sizeof(setup_bytes);
            } else if (order[i] == MOQ_ACTION_OPEN_UNI_CONTROL) {
                a.detail_size =
                    (uint32_t)sizeof(moq_open_uni_control_action_t);
                a.u.open_uni_control.data = setup_bytes;
                a.u.open_uni_control.len  = sizeof(setup_bytes);
            } else {
                a.detail_size =
                    (uint32_t)sizeof(moq_send_uni_control_action_t);
                a.u.send_uni_control.data = more_bytes;
                a.u.send_uni_control.len  = sizeof(more_bytes);
            }
            MOQ_TEST_CHECK_EQ_INT((int)push_action(s, &a), (int)MOQ_OK);
        }

        moq_action_t out[3];
        size_t n = moq_session_poll_actions(s, out, 3);
        MOQ_TEST_CHECK_EQ_SIZE(n, 3);
        for (size_t i = 0; i < 3; i++)
            MOQ_TEST_CHECK_EQ_U64(out[i].kind, order[i]);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == C. cleanup is a no-op for the uni-control actions (no owned ===
     *       refs); idempotent and leaves borrowed payload untouched. */
    {
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_UNI_CONTROL;
        a.detail_size = (uint32_t)sizeof(moq_send_uni_control_action_t);
        a.u.send_uni_control.data = more_bytes;
        a.u.send_uni_control.len  = sizeof(more_bytes);

        moq_action_cleanup(&a);
        moq_action_cleanup(&a);  /* idempotent */
        MOQ_TEST_CHECK(a.u.send_uni_control.data == more_bytes);
        MOQ_TEST_CHECK_EQ_SIZE(a.u.send_uni_control.len, sizeof(more_bytes));
        MOQ_TEST_CHECK_EQ_U64(a.kind, MOQ_ACTION_SEND_UNI_CONTROL);
    }

    /* == D. Size-aware drain treats them as non-owned (never =========
     *       ABI_MISMATCH), even with a prefix-only element_size. */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc);

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.open_uni_control.data = setup_bytes;
        a.u.open_uni_control.len  = sizeof(setup_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(s, &a), (int)MOQ_OK);

        /* Full-size drain works. */
        moq_action_t full;
        size_t n = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_poll_actions_ex(s, &full, 1,
                sizeof(moq_action_t), &n), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(n, 1);
        MOQ_TEST_CHECK_EQ_U64(full.kind, MOQ_ACTION_OPEN_UNI_CONTROL);
        moq_action_cleanup(&full);

        /* Prefix-only element_size: a SEND_DATA-with-payload action would
         * return ABI_MISMATCH here; a non-owned uni-control action drains
         * fine. The 16-byte prefix carries the kind. */
        MOQ_TEST_CHECK_EQ_INT((int)push_action(s, &a), (int)MOQ_OK);
        unsigned char small[16];
        size_t n2 = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_poll_actions_ex(s, small, 1, sizeof(small), &n2),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(n2, 1);
        uint32_t kind;
        memcpy(&kind, small, sizeof(kind));
        MOQ_TEST_CHECK_EQ_U64(kind, MOQ_ACTION_OPEN_UNI_CONTROL);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    MOQ_TEST_PASS("action_uni_control");
    return failures != 0;
}
