#ifndef FIN_HANDOFF_TEST_CASES_H
#define FIN_HANDOFF_TEST_CASES_H

#include "fin_handoff_test_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void test_publish(void);

void check_image_fields(void);

void check_owner_metadata(void);

void test_fetch(void);

void test_pending_join(void);

void test_publish_namespace(void);

void test_subscribe_tracks(void);

void test_subscribe_namespace(void);

void test_track_status(void);

void test_subscribe_control(void);

void test_route_exclusions(void);

void test_precommit_rejects(void);

void test_ns_sub_local_teardown(void);

void test_no_owner_admission_matrix(void);

void test_axis3_fin_terminals(void);

void test_ns_sub_request_error(void);

void test_t5_receiver_teardown(void);

void test_axis4_announce_torn_down_retirement(void);

void test_axis4_announce_torn_down_blocked_retry(void);

void test_axis3_durable_terminals(void);

void test_harness(void);

void test_failed_update_closing_timer(void);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* FIN_HANDOFF_TEST_CASES_H */
