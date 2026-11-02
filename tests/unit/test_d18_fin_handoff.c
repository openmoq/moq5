#include "d18_fin_handoff/test_cases.h"

int main(void)
{
    test_harness();
    check_image_fields();
    check_owner_metadata();
    test_publish();
    test_fetch();
    test_pending_join();
    test_publish_namespace();
    test_subscribe_tracks();
    test_subscribe_namespace();
    test_track_status();
    test_subscribe_control();
    test_route_exclusions();
    test_precommit_rejects();
    test_axis3_fin_terminals();
    test_ns_sub_request_error();
    test_axis3_durable_terminals();
    test_t5_receiver_teardown();
    failures += txs_selfcheck_img_states();
    test_axis4_announce_torn_down_retirement();
    test_axis4_announce_torn_down_blocked_retry();
    test_ns_sub_local_teardown();
    test_no_owner_admission_matrix();
    test_failed_update_closing_timer();
    if (failures) { printf("FAILURES: %d\n", failures); return 1; }
    MOQ_TEST_PASS("d18_fin_handoff");
    return 0;
}
