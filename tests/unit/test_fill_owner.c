#include "../../core/src/session/fill_owner_internal.h"
#include "../../core/src/session/request_lifetime_internal.h"
#include "test_support.h"
#include <string.h>

static const moq_fill_parent_t parent = {
    .family = MOQ_FILL_PARENT_SUBSCRIBE, .slot = 0, .generation = 1};
static const moq_fill_delivery_t delivery = {
    .outcome = MOQ_FILL_OUTCOME_STREAM, .group_order = 1,
    .start_group = 1, .start_object = 0, .end_group = 2, .end_object = UINT64_MAX,
    .subscriber_priority = 7, .has_fill_timeout = true, .fill_timeout_ms = 0};

#define REQUIRE(expr) do { \
    bool ok_ = (expr); \
    MOQ_TEST_CHECK(ok_); \
    if (!ok_) return failures; \
} while (0)

/* Every refusal compares the entire caller-owned storage, including free slots,
 * generations and copied identities, plus table counters and pointers. */
#define UNCHANGED(expr, result) do { \
    moq_fill_entry_t saved_[sizeof(storage)/sizeof(storage[0])]; \
    moq_fill_table_t table_; \
    memcpy(saved_, storage, sizeof(storage)); \
    memcpy(&table_, &t, sizeof(t)); \
    MOQ_TEST_CHECK((expr) == (result)); \
    MOQ_TEST_CHECK(memcmp(saved_, storage, sizeof(storage)) == 0); \
    MOQ_TEST_CHECK(memcmp(&table_, &t, sizeof(t)) == 0); \
} while (0)

static moq_result_t admit(moq_fill_table_t *t, moq_fill_parent_t p,
    uint64_t id, uint32_t role, const moq_fill_delivery_t *d,
    moq_fill_ref_t *out)
{
    moq_fill_reservation_t r;
    moq_result_t rc = moq_fill_prepare(t, p, id, role, d, &r);
    if (rc != MOQ_OK) return rc;
    rc = moq_fill_commit(t, &r);
    if (rc == MOQ_OK) *out = r.ref;
    return rc;
}

static int work(const moq_fill_table_t *t, moq_fill_ref_t ref,
    uint32_t action, bool account, bool terminal, bool reclaim)
{
    int failures = 0;
    moq_fill_work_t w;
    REQUIRE(moq_fill_next(t, ref, &w) == MOQ_OK);
    MOQ_TEST_CHECK(w.action == action);
    MOQ_TEST_CHECK(w.account_stream == account);
    MOQ_TEST_CHECK(w.publish_terminal == terminal);
    MOQ_TEST_CHECK(w.reclaimable == reclaim);
    return failures;
}

static moq_result_t publish_retire(moq_fill_table_t *t, moq_fill_ref_t ref)
{
    moq_result_t rc = moq_fill_terminal_committed(t, ref);
    if (rc == MOQ_OK) rc = moq_fill_terminal_retired(t, ref);
    return rc;
}

static int test_reservation(void)
{
    int failures = 0;
    moq_fill_entry_t storage[1], saved[1];
    moq_fill_table_t t, before;
    MOQ_TEST_CHECK(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
    moq_fill_reservation_t ticket;
    memset(&ticket, 0xa5, sizeof(ticket));
    memcpy(saved, storage, sizeof(saved));
    memcpy(&before, &t, sizeof(t));
    moq_result_t rc = moq_fill_prepare(&t, parent, UINT64_MAX,
        MOQ_FILL_SUBSCRIBER, &delivery, &ticket);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(memcmp(storage, saved, sizeof(saved)) == 0);
    MOQ_TEST_CHECK(memcmp(&t, &before, sizeof(t)) == 0);
    if (rc != MOQ_OK) return failures;
    MOQ_TEST_CHECK(ticket.initiating_id == UINT64_MAX && ticket.ref.generation == 1);
    MOQ_TEST_CHECK(moq_fill_commit(&t, &ticket) == MOQ_OK);
    MOQ_TEST_CHECK(t.live == 1 && storage[0].active);
    MOQ_TEST_CHECK(storage[0].initiating_id == UINT64_MAX);
    moq_fill_reservation_t sentinel;
    memset(&ticket, 0xa5, sizeof(ticket));
    memcpy(&sentinel, &ticket, sizeof(ticket));
    UNCHANGED(moq_fill_prepare(&t, parent, 0, MOQ_FILL_SUBSCRIBER,
        &delivery, &ticket), MOQ_ERR_REQUEST_BLOCKED);
    MOQ_TEST_CHECK(memcmp(&sentinel, &ticket, sizeof(ticket)) == 0);
    return failures;
}

static int test_size_and_ticket_refusals(void)
{
    int failures = 0;
    moq_fill_entry_t storage[2];
    moq_fill_table_t t;
    memset(storage, 0x5a, sizeof(storage));
    memset(&t, 0x5a, sizeof(t));
    UNCHANGED(moq_fill_table_init(&t, storage,
        SIZE_MAX / sizeof(storage[0]) + 1), MOQ_ERR_INVAL);
    UNCHANGED(moq_fill_table_init(&t, NULL, 1), MOQ_ERR_INVAL);
    REQUIRE(moq_fill_table_init(&t, NULL, 0) == MOQ_OK);
    moq_fill_reservation_t r, r2, before;
    memset(&r, 0xa5, sizeof(r));
    memcpy(&before, &r, sizeof(r));
    UNCHANGED(moq_fill_prepare(&t, parent, 0, MOQ_FILL_SUBSCRIBER,
        &delivery, &r), MOQ_ERR_REQUEST_BLOCKED);
    MOQ_TEST_CHECK(memcmp(&r, &before, sizeof(r)) == 0);
    REQUIRE(moq_fill_table_init(&t, storage, 2) == MOQ_OK);
    moq_fill_parent_t bad = parent;
    bad.generation = 0;
    UNCHANGED(moq_fill_prepare(&t, bad, 0, MOQ_FILL_SUBSCRIBER,
        &delivery, &r), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(memcmp(&r, &before, sizeof(r)) == 0);
    UNCHANGED(moq_fill_prepare(&t, parent, 0, 3, &delivery, &r), MOQ_ERR_INVAL);
    moq_fill_delivery_t invalid = delivery;
    invalid.group_order = 0;
    UNCHANGED(moq_fill_prepare(&t, parent, 0, MOQ_FILL_PUBLISHER,
        &invalid, &r), MOQ_ERR_INVAL);
    REQUIRE(moq_fill_prepare(&t, parent, 0, MOQ_FILL_SUBSCRIBER,
        &delivery, &r) == MOQ_OK);
    REQUIRE(moq_fill_prepare(&t, parent, 2, MOQ_FILL_SUBSCRIBER,
        &delivery, &r2) == MOQ_OK);
    MOQ_TEST_CHECK(r.ref.slot == r2.ref.slot && t.live == 0);
    UNCHANGED(moq_fill_check_commit(&t, &r), MOQ_OK);
    REQUIRE(moq_fill_commit(&t, &r) == MOQ_OK);
    UNCHANGED(moq_fill_commit(&t, &r2), MOQ_ERR_STALE_HANDLE);
    UNCHANGED(moq_fill_commit(&t, &r), MOQ_ERR_STALE_HANDLE);
    UNCHANGED(moq_fill_prepare(&t, parent, 0, MOQ_FILL_SUBSCRIBER,
        &delivery, &r2), MOQ_ERR_WRONG_STATE);
    moq_fill_ref_t missing, old;
    memset(&missing, 0xa5, sizeof(missing));
    memcpy(&old, &missing, sizeof(old));
    UNCHANGED(moq_fill_find(&t, UINT64_MAX, &missing), MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(memcmp(&missing, &old, sizeof(old)) == 0);
    return failures;
}

static int test_overlap_and_frozen_identity(void)
{
    int failures = 0;
    moq_fill_entry_t storage[2];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 2) == MOQ_OK);
    moq_fill_ref_t a, b, found;
    moq_fill_delivery_t one = delivery, two = delivery;
    two.start_group = 10; two.end_group = 20; two.subscriber_priority = 1;
    two.group_order = 2; two.fill_timeout_ms = UINT64_MAX;
    REQUIRE(admit(&t, parent, 0, MOQ_FILL_SUBSCRIBER, &one, &a) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, a, 0, true) == MOQ_OK);
    /* ACK retires update credit elsewhere, not the child or frozen values. */
    UNCHANGED(moq_fill_reclaim(&t, a), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(admit(&t, parent, UINT64_MAX, MOQ_FILL_SUBSCRIBER, &two, &b) == MOQ_OK);
    memset(&one, 0, sizeof(one)); memset(&two, 0, sizeof(two));
    REQUIRE(moq_fill_find(&t, 0, &found) == MOQ_OK);
    MOQ_TEST_CHECK(found.slot == a.slot && found.generation == a.generation);
    REQUIRE(moq_fill_find(&t, UINT64_MAX, &found) == MOQ_OK);
    MOQ_TEST_CHECK(found.slot == b.slot && found.generation == b.generation);
    MOQ_TEST_CHECK(storage[a.slot].delivery.end_group == 2);
    MOQ_TEST_CHECK(storage[b.slot].delivery.end_group == 20);
    MOQ_TEST_CHECK(storage[b.slot].delivery.group_order == 2 &&
        storage[b.slot].delivery.fill_timeout_ms == UINT64_MAX);
    REQUIRE(moq_fill_control_response(&t, b, UINT64_MAX, true) == MOQ_OK);
    moq_stream_ref_t sa = {1}, sb = {UINT64_MAX};
    REQUIRE(moq_fill_bind_received(&t, a, sa) == MOQ_OK);
    UNCHANGED(moq_fill_bind_received(&t, a, sa), MOQ_OK);
    UNCHANGED(moq_fill_bind_received(&t, a, sb), MOQ_ERR_PROTO);
    UNCHANGED(moq_fill_bind_received(&t, b, sa), MOQ_ERR_PROTO);
    REQUIRE(moq_fill_bind_received(&t, b, sb) == MOQ_OK);
    moq_fill_entry_t a_before;
    memcpy(&a_before, &storage[a.slot], sizeof(a_before));
    REQUIRE(moq_fill_peer_fin(&t, b, sb) == MOQ_OK);
    REQUIRE(moq_fill_stream_accounted(&t, b) == MOQ_OK);
    REQUIRE(publish_retire(&t, b) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, b) == MOQ_OK);
    MOQ_TEST_CHECK(t.live == 1 && memcmp(&a_before, &storage[a.slot], sizeof(a_before)) == 0);
    UNCHANGED(moq_fill_find(&t, UINT64_MAX, &found), MOQ_ERR_STALE_HANDLE);
    REQUIRE(moq_fill_peer_reset(&t, a, sa) == MOQ_OK);
    REQUIRE(moq_fill_stream_accounted(&t, a) == MOQ_OK);
    REQUIRE(publish_retire(&t, a) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, a) == MOQ_OK);
    MOQ_TEST_CHECK(t.live == 0);
    return failures;
}

static int test_early_terminal_and_control(void)
{
    int failures = 0;
    for (unsigned reset = 0; reset < 2; ++reset) {
        moq_fill_entry_t storage[1];
        moq_fill_table_t t;
        REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
        moq_fill_ref_t ref, found;
        REQUIRE(admit(&t, parent, 42, MOQ_FILL_SUBSCRIBER, NULL, &ref) == MOQ_OK);
        moq_stream_ref_t stream = {77};
        REQUIRE(moq_fill_bind_received(&t, ref, stream) == MOQ_OK);
        REQUIRE((reset ? moq_fill_peer_reset(&t, ref, stream) :
            moq_fill_peer_fin(&t, ref, stream)) == MOQ_OK);
        failures += work(&t, ref, MOQ_FILL_ACTION_NONE, true, true, false);
        REQUIRE(moq_fill_stream_accounted(&t, ref) == MOQ_OK);
        UNCHANGED(moq_fill_stream_accounted(&t, ref), MOQ_ERR_WRONG_STATE);
        REQUIRE(publish_retire(&t, ref) == MOQ_OK);
        UNCHANGED(moq_fill_terminal_committed(&t, ref), MOQ_ERR_WRONG_STATE);
        UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
        REQUIRE(moq_fill_find(&t, 42, &found) == MOQ_OK);
        UNCHANGED(moq_fill_control_response(&t, ref, 44, true), MOQ_ERR_INVAL);
        REQUIRE(moq_fill_control_response(&t, ref, 42, true) == MOQ_OK);
        /* Accepted but unresolved snapshot is still a represented obligation. */
        UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
        REQUIRE(moq_fill_resolve_delivery(&t, ref, 42, &delivery) == MOQ_OK);
        UNCHANGED(moq_fill_resolve_delivery(&t, ref, 42, &delivery), MOQ_OK);
        moq_fill_delivery_t changed = delivery;
        ++changed.end_group;
        UNCHANGED(moq_fill_resolve_delivery(&t, ref, 42, &changed), MOQ_ERR_WRONG_STATE);
        UNCHANGED(moq_fill_control_response(&t, ref, 42, true), MOQ_ERR_WRONG_STATE);
        failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, true);
        REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
        MOQ_TEST_CHECK(t.live == 0);
    }
    return failures;
}

static int test_cancelled_unbound_retention(void)
{
    int failures = 0;
    moq_fill_entry_t storage[1];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
    moq_fill_ref_t ref, found;
    REQUIRE(admit(&t, parent, 10, MOQ_FILL_SUBSCRIBER, &delivery, &ref) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, ref, 10, true) == MOQ_OK);
    REQUIRE(moq_fill_cancel(&t, ref) == MOQ_OK);
    UNCHANGED(moq_fill_cancel(&t, ref), MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, true, false);
    REQUIRE(publish_retire(&t, ref) == MOQ_OK);
    for (unsigned i = 0; i < 8; ++i) {
        UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
        moq_fill_reservation_t r, saved;
        memset(&r, 0xa5, sizeof(r)); memcpy(&saved, &r, sizeof(r));
        UNCHANGED(moq_fill_prepare(&t, parent, 12, MOQ_FILL_SUBSCRIBER,
            &delivery, &r), MOQ_ERR_REQUEST_BLOCKED);
        MOQ_TEST_CHECK(memcmp(&r, &saved, sizeof(r)) == 0);
        UNCHANGED(moq_fill_find(&t, 10, &found), MOQ_OK);
        MOQ_TEST_CHECK(found.generation == ref.generation && found.slot == ref.slot);
    }
    /* A reused parent generation cannot consume the old child's late header. */
    moq_fill_ref_t wrong = ref;
    ++wrong.parent.generation;
    UNCHANGED(moq_fill_bind_received(&t, wrong, (moq_stream_ref_t){99}), MOQ_ERR_STALE_HANDLE);
    REQUIRE(moq_fill_bind_received(&t, ref, (moq_stream_ref_t){99}) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_STOP, false, false, false);
    UNCHANGED(moq_fill_stream_accounted(&t, ref), MOQ_ERR_WRONG_STATE);
    /* Queue refusal is represented by no commit, even after repeated queries. */
    moq_fill_work_t w;
    for (unsigned i = 0; i < 3; ++i) UNCHANGED(moq_fill_next(&t, ref, &w), MOQ_OK);
    REQUIRE(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_STOP) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, false);
    UNCHANGED(moq_fill_stream_accounted(&t, ref), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_fill_peer_reset(&t, ref, (moq_stream_ref_t){99}) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, true, false, false);
    REQUIRE(moq_fill_stream_accounted(&t, ref) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
    moq_fill_ref_t successor;
    REQUIRE(admit(&t, parent, 12, MOQ_FILL_SUBSCRIBER, &delivery, &successor) == MOQ_OK);
    MOQ_TEST_CHECK(successor.generation == ref.generation + 1 && t.live == 1);
    UNCHANGED(moq_fill_cancel(&t, ref), MOQ_ERR_STALE_HANDLE);
    return failures;
}

static int test_publisher_obligations(void)
{
    int failures = 0;
    moq_fill_entry_t storage[2];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 2) == MOQ_OK);
    moq_fill_ref_t ref, sibling;
    REQUIRE(admit(&t, parent, 0, MOQ_FILL_PUBLISHER, &delivery, &ref) == MOQ_OK);
    REQUIRE(admit(&t, parent, 2, MOQ_FILL_PUBLISHER, &delivery, &sibling) == MOQ_OK);
    REQUIRE(moq_fill_abort(&t, ref) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, false);
    UNCHANGED(moq_fill_header_committed(&t, ref, (moq_stream_ref_t){77}), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_terminal_committed(&t, ref), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_control_response(&t, ref, 0, true) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, sibling, 2, true) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_fill_publisher_clear(&t, parent));
    /* Parent half-FIN changes only its own lifetime, never a child fact. */
    moq_request_lifetime_t lifetime;
    REQUIRE(moq_request_lifetime_init(&lifetime, MOQ_REQUEST_FAMILY_SUBSCRIBE, false) == MOQ_OK);
    REQUIRE(moq_request_lifetime_initial_response_committed(&lifetime, true, true) == MOQ_OK);
    UNCHANGED(moq_request_lifetime_peer_fin(&lifetime), MOQ_OK);
    MOQ_TEST_CHECK(!moq_fill_publisher_clear(&t, parent));
    UNCHANGED(moq_request_lifetime_check_done(&lifetime, true,
        moq_fill_publisher_clear(&t, parent)), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_fill_abort(&t, ref) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_HEADER, false, false, false);
    UNCHANGED(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_RESET), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_terminal_committed(&t, ref), MOQ_ERR_WRONG_STATE);
    moq_fill_entry_t other;
    memcpy(&other, &storage[sibling.slot], sizeof(other));
    moq_fill_work_t w;
    for (unsigned i = 0; i < 3; ++i) UNCHANGED(moq_fill_next(&t, ref, &w), MOQ_OK);
    REQUIRE(moq_fill_header_committed(&t, ref, (moq_stream_ref_t){77}) == MOQ_OK);
    UNCHANGED(moq_fill_header_committed(&t, ref, (moq_stream_ref_t){77}), MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_RESET, false, false, false);
    for (unsigned i = 0; i < 3; ++i) UNCHANGED(moq_fill_next(&t, ref, &w), MOQ_OK);
    UNCHANGED(moq_fill_terminal_committed(&t, ref), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_RESET) == MOQ_OK);
    UNCHANGED(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_RESET), MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, true, true, false);
    REQUIRE(moq_fill_stream_accounted(&t, ref) == MOQ_OK);
    REQUIRE(publish_retire(&t, ref) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
    MOQ_TEST_CHECK(memcmp(&other, &storage[sibling.slot], sizeof(other)) == 0);
    MOQ_TEST_CHECK(!moq_fill_publisher_clear(&t, parent));
    /* Parent cancellation removes ONLY the still-unopened outgoing obligation. */
    REQUIRE(moq_fill_parent_cancel(&t, sibling) == MOQ_OK);
    failures += work(&t, sibling, MOQ_FILL_ACTION_NONE, false, true, false);
    UNCHANGED(moq_fill_header_committed(&t, sibling, (moq_stream_ref_t){78}), MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moq_fill_publisher_clear(&t, parent));
    REQUIRE(publish_retire(&t, sibling) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, sibling) == MOQ_OK);
    return failures;
}

static int test_no_stream_proof_and_terminal_cause(void)
{
    int failures = 0;
    const moq_fill_delivery_t empty = {.outcome = MOQ_FILL_OUTCOME_NO_STREAM};
    /* A tentative NO_STREAM snapshot must not win over a real terminal cause
     * while ACK is absent: test both creation-time and later resolution. */
    for (unsigned resolve_later = 0; resolve_later < 2; ++resolve_later) {
        for (unsigned arm = 0; arm < 4; ++arm) {
            moq_fill_entry_t storage[1];
            moq_fill_table_t t;
            REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
            moq_fill_ref_t ref;
            REQUIRE(admit(&t, parent, 0, MOQ_FILL_SUBSCRIBER,
                resolve_later ? NULL : &empty, &ref) == MOQ_OK);
            if (resolve_later) REQUIRE(moq_fill_resolve_delivery(&t, ref, 0, &empty) == MOQ_OK);
            MOQ_TEST_CHECK(storage[0].terminal_reason == MOQ_FILL_TERM_NONE);
            failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, false);
            uint32_t expected;
            if (arm == 0) {
                REQUIRE(moq_fill_cancel(&t, ref) == MOQ_OK);
                expected = MOQ_FILL_TERM_CANCELLED;
            } else if (arm == 1) {
                REQUIRE(moq_fill_control_response(&t, ref, 0, false) == MOQ_OK);
                expected = MOQ_FILL_TERM_CONTROL_REJECTED;
            } else if (arm == 2) {
                REQUIRE(moq_fill_parent_cancel(&t, ref) == MOQ_OK);
                expected = MOQ_FILL_TERM_PARENT_CANCELLED;
            } else {
                REQUIRE(moq_fill_control_response(&t, ref, 0, true) == MOQ_OK);
                expected = MOQ_FILL_TERM_NO_STREAM;
            }
            moq_fill_work_t w;
            REQUIRE(moq_fill_next(&t, ref, &w) == MOQ_OK);
            MOQ_TEST_CHECK(w.publish_terminal && w.terminal_reason == expected);
            REQUIRE(publish_retire(&t, ref) == MOQ_OK);
            if (arm == 3) REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
            else UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
            if (arm == 0 || arm == 2) {
                REQUIRE(moq_fill_control_response(&t, ref, 0, true) == MOQ_OK);
                REQUIRE(moq_fill_next(&t, ref, &w) == MOQ_OK);
                MOQ_TEST_CHECK(!w.publish_terminal && w.terminal_reason == expected);
                REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
            }
        }
    }
    moq_fill_entry_t storage[1];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
    moq_fill_ref_t ref;
    REQUIRE(admit(&t, parent, 2, MOQ_FILL_SUBSCRIBER, NULL, &ref) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, ref, 2, true) == MOQ_OK);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, false);
    UNCHANGED(moq_fill_terminal_committed(&t, ref), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_fill_bind_received(&t, ref, (moq_stream_ref_t){1}) == MOQ_OK);
    UNCHANGED(moq_fill_resolve_delivery(&t, ref, 2, &empty), MOQ_ERR_PROTO);
    MOQ_TEST_CHECK(!storage[0].delivery_known);
    return failures;
}

static int test_ordinary_reuse_stale_and_exhaustion(void)
{
    int failures = 0;
    moq_fill_entry_t storage[1];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
    moq_fill_ref_t old = {0};
    for (uint64_t i = 0; i < 128; ++i) {
        moq_fill_ref_t ref;
        REQUIRE(admit(&t, parent, i, MOQ_FILL_PUBLISHER, &delivery, &ref) == MOQ_OK);
        REQUIRE(moq_fill_control_response(&t, ref, i, true) == MOQ_OK);
        if (i) UNCHANGED(moq_fill_parent_cancel(&t, old), MOQ_ERR_STALE_HANDLE);
        moq_fill_ref_t wrong = ref;
        wrong.parent.family = MOQ_FILL_PARENT_PUBLISH;
        UNCHANGED(moq_fill_parent_cancel(&t, wrong), MOQ_ERR_STALE_HANDLE);
        wrong = ref; ++wrong.parent.slot;
        UNCHANGED(moq_fill_parent_cancel(&t, wrong), MOQ_ERR_STALE_HANDLE);
        wrong = ref; ++wrong.parent.generation;
        UNCHANGED(moq_fill_parent_cancel(&t, wrong), MOQ_ERR_STALE_HANDLE);
        REQUIRE(moq_fill_header_committed(&t, ref, (moq_stream_ref_t){i+1}) == MOQ_OK);
        REQUIRE(moq_fill_finish(&t, ref) == MOQ_OK);
        failures += work(&t, ref, MOQ_FILL_ACTION_FIN, false, false, false);
        REQUIRE(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_FIN) == MOQ_OK);
        REQUIRE(moq_fill_stream_accounted(&t, ref) == MOQ_OK);
        REQUIRE(moq_fill_terminal_committed(&t, ref) == MOQ_OK);
        /* Event publication is not application retirement. */
        UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
        REQUIRE(moq_fill_terminal_retired(&t, ref) == MOQ_OK);
        REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
        MOQ_TEST_CHECK(t.live == 0 && storage[0].generation == i+1);
        old = ref;
    }
    storage[0].generation = UINT64_MAX - 1; /* exact terminal-generation boundary */
    moq_fill_delivery_t empty = {.outcome = MOQ_FILL_OUTCOME_NO_STREAM};
    moq_fill_ref_t ref;
    REQUIRE(admit(&t, parent, UINT64_MAX, MOQ_FILL_PUBLISHER, &empty, &ref) == MOQ_OK);
    MOQ_TEST_CHECK(ref.generation == UINT64_MAX);
    REQUIRE(moq_fill_control_response(&t, ref, UINT64_MAX, true) == MOQ_OK);
    REQUIRE(publish_retire(&t, ref) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
    moq_fill_reservation_t r;
    UNCHANGED(moq_fill_prepare(&t, parent, 1, MOQ_FILL_PUBLISHER,
        &delivery, &r), MOQ_ERR_REQUEST_BLOCKED);
    MOQ_TEST_CHECK(t.live == 0 && storage[0].generation == UINT64_MAX);
    return failures;
}

static int test_role_and_control_reset_boundaries(void)
{
    int failures = 0;
    moq_fill_entry_t storage[2];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 2) == MOQ_OK);
    moq_fill_parent_t pub_parent = parent;
    pub_parent.family = MOQ_FILL_PARENT_PUBLISH;
    pub_parent.slot = UINT64_MAX;
    pub_parent.generation = UINT64_MAX;
    moq_fill_ref_t rx, tx;
    REQUIRE(admit(&t, pub_parent, 12, MOQ_FILL_SUBSCRIBER, &delivery, &rx) == MOQ_OK);
    REQUIRE(admit(&t, pub_parent, 14, MOQ_FILL_PUBLISHER, &delivery, &tx) == MOQ_OK);
    UNCHANGED(moq_fill_abort(&t, rx), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_finish(&t, rx), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_cancel(&t, tx), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_header_committed(&t, rx, (moq_stream_ref_t){1}), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_bind_received(&t, tx, (moq_stream_ref_t){1}), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_bind_received(&t, rx, (moq_stream_ref_t){0}), MOQ_ERR_INVAL);
    UNCHANGED(moq_fill_header_committed(&t, tx, (moq_stream_ref_t){1}), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_action_committed(&t, tx, MOQ_FILL_ACTION_HEADER), MOQ_ERR_INVAL);
    UNCHANGED(moq_fill_control_reset(&t, tx, 12), MOQ_ERR_INVAL);
    REQUIRE(moq_fill_control_reset(&t, tx, 14) == MOQ_OK);
    UNCHANGED(moq_fill_control_reset(&t, tx, 14), MOQ_OK);
    UNCHANGED(moq_fill_control_response(&t, tx, 14, true), MOQ_ERR_WRONG_STATE);
    failures += work(&t, tx, MOQ_FILL_ACTION_NONE, false, true, false);
    REQUIRE(publish_retire(&t, tx) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, tx) == MOQ_OK);
    /* A control reset settles control correlation, not an unbound incoming
     * data stream. Its eventual header is still owned and must be stopped. */
    REQUIRE(moq_fill_control_reset(&t, rx, 12) == MOQ_OK);
    REQUIRE(publish_retire(&t, rx) == MOQ_OK);
    UNCHANGED(moq_fill_reclaim(&t, rx), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_fill_bind_received(&t, rx, (moq_stream_ref_t){23}) == MOQ_OK);
    UNCHANGED(moq_fill_peer_fin(&t, rx, (moq_stream_ref_t){24}), MOQ_ERR_WRONG_STATE);
    UNCHANGED(moq_fill_peer_stop(&t, rx, (moq_stream_ref_t){23}), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_action_committed(&t, rx, MOQ_FILL_ACTION_STOP) == MOQ_OK);
    UNCHANGED(moq_fill_reclaim(&t, rx), MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_fill_peer_fin(&t, rx, (moq_stream_ref_t){23}) == MOQ_OK);
    UNCHANGED(moq_fill_peer_reset(&t, rx, (moq_stream_ref_t){23}), MOQ_OK);
    REQUIRE(moq_fill_stream_accounted(&t, rx) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, rx) == MOQ_OK);
    /* An opened outgoing child cannot use the parent-cancel unopened shortcut. */
    REQUIRE(admit(&t, pub_parent, 16, MOQ_FILL_PUBLISHER, &delivery, &tx) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, tx, 16, true) == MOQ_OK);
    REQUIRE(moq_fill_header_committed(&t, tx, (moq_stream_ref_t){25}) == MOQ_OK);
    REQUIRE(moq_fill_parent_cancel(&t, tx) == MOQ_OK);
    failures += work(&t, tx, MOQ_FILL_ACTION_RESET, false, false, false);
    MOQ_TEST_CHECK(!moq_fill_publisher_clear(&t, pub_parent));
    UNCHANGED(moq_fill_action_committed(&t, tx, MOQ_FILL_ACTION_FIN), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_action_committed(&t, tx, MOQ_FILL_ACTION_RESET) == MOQ_OK);
    REQUIRE(moq_fill_stream_accounted(&t, tx) == MOQ_OK);
    REQUIRE(publish_retire(&t, tx) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, tx) == MOQ_OK);
    REQUIRE(admit(&t, pub_parent, 18, MOQ_FILL_PUBLISHER, &delivery, &tx) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, tx, 18, true) == MOQ_OK);
    REQUIRE(moq_fill_header_committed(&t, tx, (moq_stream_ref_t){26}) == MOQ_OK);
    UNCHANGED(moq_fill_peer_reset(&t, tx, (moq_stream_ref_t){26}), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_peer_stop(&t, tx, (moq_stream_ref_t){26}) == MOQ_OK);
    failures += work(&t, tx, MOQ_FILL_ACTION_RESET, false, false, false);
    UNCHANGED(moq_fill_stream_accounted(&t, tx), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_action_committed(&t, tx, MOQ_FILL_ACTION_RESET) == MOQ_OK);
    REQUIRE(moq_fill_stream_accounted(&t, tx) == MOQ_OK);
    REQUIRE(publish_retire(&t, tx) == MOQ_OK);
    REQUIRE(moq_fill_reclaim(&t, tx) == MOQ_OK);
    /* Read/query refusal leaves the caller's output as well as storage intact. */
    moq_fill_entry_t out, saved;
    memset(&out, 0xa5, sizeof(out)); memcpy(&saved, &out, sizeof(out));
    UNCHANGED(moq_fill_read(&t, tx, &out), MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(memcmp(&out, &saved, sizeof(out)) == 0);
    moq_fill_work_t w, old;
    memset(&w, 0xa5, sizeof(w)); memcpy(&old, &w, sizeof(w));
    UNCHANGED(moq_fill_next(&t, tx, &w), MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(memcmp(&w, &old, sizeof(w)) == 0);
    return failures;
}

/* Releasing an unbound child: only once its terminal is published, never
 * while a stream is bound; the slot's generation survives for reuse. */
static int test_release_unbound(void)
{
    int failures = 0;
    moq_fill_entry_t storage[2];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 2) == MOQ_OK);
    moq_fill_ref_t a, b;
    REQUIRE(admit(&t, parent, 10, MOQ_FILL_SUBSCRIBER, NULL, &a) == MOQ_OK);
    REQUIRE(admit(&t, parent, 11, MOQ_FILL_SUBSCRIBER, NULL, &b) == MOQ_OK);
    /* Unpublished terminal: refused, nothing changes. */
    UNCHANGED(moq_fill_release_unbound(&t, a), MOQ_ERR_WRONG_STATE);
    /* A cancelled unbound child publishes its terminal and is then releasable. */
    REQUIRE(moq_fill_cancel(&t, a) == MOQ_OK);
    REQUIRE(publish_retire(&t, a) == MOQ_OK);
    failures += work(&t, a, MOQ_FILL_ACTION_NONE, false, false, false);
    REQUIRE(moq_fill_release_unbound(&t, a) == MOQ_OK);
    MOQ_TEST_CHECK(t.live == 1);
    MOQ_TEST_CHECK(!storage[a.slot].active && storage[a.slot].generation == a.generation);
    UNCHANGED(moq_fill_release_unbound(&t, a), MOQ_ERR_STALE_HANDLE);
    /* A bound child is never released this way, even once its terminal is out. */
    REQUIRE(moq_fill_bind_received(&t, b, (moq_stream_ref_t){ 7 }) == MOQ_OK);
    REQUIRE(moq_fill_cancel(&t, b) == MOQ_OK);
    REQUIRE(publish_retire(&t, b) == MOQ_OK);
    UNCHANGED(moq_fill_release_unbound(&t, b), MOQ_ERR_WRONG_STATE);
    /* The released slot admits a new child with the next generation. */
    moq_fill_ref_t c;
    REQUIRE(admit(&t, parent, 12, MOQ_FILL_SUBSCRIBER, NULL, &c) == MOQ_OK);
    MOQ_TEST_CHECK(c.slot == a.slot && c.generation == a.generation + 1);
    return failures;
}

/* A cancelled receiving child whose stopped stream the transport retired with
 * no peer FIN or RESET (local receive retirement): the stream is over and is
 * accounted once, the terminal keeps the cancellation's reason, and the child
 * becomes reclaimable. Refused without the committed STOP, for another stream
 * or for a stale child; a later peer terminal changes nothing. */
static int test_local_retired_stopped_stream(void)
{
    int failures = 0;
    moq_fill_entry_t storage[1];
    moq_fill_table_t t;
    REQUIRE(moq_fill_table_init(&t, storage, 1) == MOQ_OK);
    moq_fill_ref_t ref;
    REQUIRE(admit(&t, parent, 10, MOQ_FILL_SUBSCRIBER, &delivery, &ref) == MOQ_OK);
    REQUIRE(moq_fill_control_response(&t, ref, 10, true) == MOQ_OK);
    REQUIRE(moq_fill_bind_received(&t, ref, (moq_stream_ref_t){99}) == MOQ_OK);
    UNCHANGED(moq_fill_local_retired(&t, ref, (moq_stream_ref_t){99}), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_cancel(&t, ref) == MOQ_OK);
    UNCHANGED(moq_fill_local_retired(&t, ref, (moq_stream_ref_t){99}), MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_fill_action_committed(&t, ref, MOQ_FILL_ACTION_STOP) == MOQ_OK);
    UNCHANGED(moq_fill_reclaim(&t, ref), MOQ_ERR_WOULD_BLOCK);
    UNCHANGED(moq_fill_local_retired(&t, ref, (moq_stream_ref_t){98}), MOQ_ERR_WRONG_STATE);
    moq_fill_ref_t stale = ref;
    ++stale.generation;
    UNCHANGED(moq_fill_local_retired(&t, stale, (moq_stream_ref_t){99}), MOQ_ERR_STALE_HANDLE);
    REQUIRE(moq_fill_local_retired(&t, ref, (moq_stream_ref_t){99}) == MOQ_OK);
    UNCHANGED(moq_fill_local_retired(&t, ref, (moq_stream_ref_t){99}), MOQ_OK);
    MOQ_TEST_CHECK(!storage[ref.slot].data_fin && !storage[ref.slot].data_reset);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, true, true, false);
    moq_fill_work_t w;
    REQUIRE(moq_fill_next(&t, ref, &w) == MOQ_OK);
    MOQ_TEST_CHECK(w.terminal_reason == MOQ_FILL_TERM_CANCELLED);
    REQUIRE(moq_fill_stream_accounted(&t, ref) == MOQ_OK);
    UNCHANGED(moq_fill_stream_accounted(&t, ref), MOQ_ERR_WRONG_STATE);
    /* A peer terminal after the retirement neither recounts nor rewrites. */
    UNCHANGED(moq_fill_peer_fin(&t, ref, (moq_stream_ref_t){99}), MOQ_OK);
    UNCHANGED(moq_fill_peer_reset(&t, ref, (moq_stream_ref_t){99}), MOQ_OK);
    REQUIRE(publish_retire(&t, ref) == MOQ_OK);
    MOQ_TEST_CHECK(storage[ref.slot].terminal_reason == MOQ_FILL_TERM_CANCELLED);
    failures += work(&t, ref, MOQ_FILL_ACTION_NONE, false, false, true);
    REQUIRE(moq_fill_reclaim(&t, ref) == MOQ_OK);
    MOQ_TEST_CHECK(t.live == 0);
    return failures;
}

int main(void)
{
    int failures = test_reservation();
    failures += test_size_and_ticket_refusals();
    failures += test_overlap_and_frozen_identity();
    failures += test_early_terminal_and_control();
    failures += test_cancelled_unbound_retention();
    failures += test_publisher_obligations();
    failures += test_no_stream_proof_and_terminal_cause();
    failures += test_ordinary_reuse_stale_and_exhaustion();
    failures += test_role_and_control_reset_boundaries();
    failures += test_release_unbound();
    failures += test_local_retired_stopped_stream();
    printf("fill owner: %d failures\n", failures);
    return failures ? 1 : 0;
}
