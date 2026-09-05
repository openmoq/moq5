/*
 * Peer Request ID tracking (core/src/session/request_id_window.h): requests on
 * separate bidi streams can arrive out of order, and only a wrong parity or a
 * duplicate is an error (draft-18 section 10.1). Reproduces the red5-moq-relay
 * over mvfst case: the relay's SUBSCRIBEs 3 and 5 on streams 5 and 9, with
 * mvfst servicing stream 9 first.
 */
#include "../../core/src/session/request_id_window.h"
#include "test_support.h"

int main(void)
{
    int failures = 0;
    moq_request_id_window_t w;

    /* In order: 1, 3, 5 (a client tracking a server's odd ids). */
    moq_request_id_window_init(&w, 1);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 1) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, 1);
    MOQ_TEST_CHECK(w.next == 3 && w.gap_count == 0);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 3) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, 3);
    MOQ_TEST_CHECK(w.next == 5);

    /* Out of order: 5 arrives before 3 (streams reordered by the transport). */
    moq_request_id_window_init(&w, 1);
    moq_request_id_window_commit(&w, 1);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 5) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, 5);
    MOQ_TEST_CHECK(w.next == 7);               /* GOAWAY reports 7 as next */
    MOQ_TEST_CHECK(w.gap_count == 1 && w.gaps[0] == 3);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 3) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, 3);
    MOQ_TEST_CHECK(w.gap_count == 0 && w.next == 7);

    /* Duplicates: an id already committed, at or below the mark. */
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 3) == MOQ_REQUEST_ID_DUPLICATE);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 5) == MOQ_REQUEST_ID_DUPLICATE);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 1) == MOQ_REQUEST_ID_DUPLICATE);
    /* A gap that is still open is not a duplicate; once filled it is. */
    moq_request_id_window_commit(&w, 11);      /* gaps 7, 9 */
    MOQ_TEST_CHECK(w.gap_count == 2);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 9) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, 9);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 9) == MOQ_REQUEST_ID_DUPLICATE);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, 7) == MOQ_REQUEST_ID_NEW);

    /* Classification is pure: nothing moved until commit. */
    moq_request_id_window_t before = w;
    (void)moq_request_id_window_classify(&w, 13);
    MOQ_TEST_CHECK(before.next == w.next && before.gap_count == w.gap_count);

    /* Bounded gap set: running MOQ_REQUEST_ID_GAP_MAX ahead is allowed, one
     * more is refused, and the refusal does not change the window. */
    moq_request_id_window_init(&w, 0);
    uint64_t far = 2 * (uint64_t)MOQ_REQUEST_ID_GAP_MAX;   /* leaves exactly MAX gaps */
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, far) == MOQ_REQUEST_ID_NEW);
    moq_request_id_window_commit(&w, far);
    MOQ_TEST_CHECK(w.gap_count == (size_t)MOQ_REQUEST_ID_GAP_MAX);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, far + 2) == MOQ_REQUEST_ID_NEW);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, far + 4) == MOQ_REQUEST_ID_TOO_FAR_AHEAD);
    MOQ_TEST_CHECK(w.gap_count == (size_t)MOQ_REQUEST_ID_GAP_MAX && w.next == far + 2);
    /* Filling a gap frees room again. */
    moq_request_id_window_commit(&w, 0);
    MOQ_TEST_CHECK(moq_request_id_window_classify(&w, far + 4) == MOQ_REQUEST_ID_NEW);

    MOQ_TEST_PASS("test_request_id_window");
    return failures;
}
