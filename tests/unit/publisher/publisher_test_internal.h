#ifndef PUBLISHER_TEST_INTERNAL_H
#define PUBLISHER_TEST_INTERNAL_H

#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */
#include "../test_session_support.h"
#include "../publisher_track_old_layout.h"
#include "../../../core/src/spi/core_spi.h"
#include <stddef.h>
#include <stdio.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include <string.h>


#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

typedef struct {
    bool called;
    moq_subscribe_filter_t filter;
    uint8_t subscriber_priority;
    bool forward;
    size_t ns_count;
    size_t name_len;
    size_t token_count;
    uint64_t first_token_type;
    size_t first_token_value_len;
    uint8_t first_token_value[16];
} cb_capture_t;

/* ================================================================== */
/* Tick callback state                                                */
/* ================================================================== */

typedef struct {
    int joined;
    int left;
    int draining;
    int closed;
    int updated;
    int publish_ok;
    int publish_error;
    uint64_t close_code;
    moq_pub_track_t *joined_track;
    moq_pub_track_t *left_track;
    moq_pub_track_t *updated_track;
    moq_pub_subscribe_update_info_t last_update;
} tick_cb_state_t;

extern int failures;
extern moq_pub_deferred_t *last_deferred;
extern uint64_t last_deferred_id;
extern int g_wrap_releases;

void simpair_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                   moq_simpair_t **sp);
void drain_all(moq_simpair_t *sp);
moq_result_t end_track_virtual(moq_simpair_t *sp,
    moq_publisher_t *pub, moq_pub_track_t *track);
moq_result_t flush_virtual(moq_simpair_t *sp, moq_publisher_t *pub);
moq_pub_accept_decision_t cb_accept_all(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error);
moq_pub_accept_decision_t cb_capture(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error);
void tick_on_publish_ok(void *ctx, moq_pub_track_t *track, bool forward);
void tick_on_publish_error(void *ctx, moq_pub_track_t *track,
                           moq_request_error_t code);
void tick_on_joined(void *ctx, moq_pub_track_t *track);
void tick_on_left(void *ctx, moq_pub_track_t *track);
void tick_on_draining(void *ctx);
void tick_on_updated(void *ctx, moq_pub_track_t *track,
                     const moq_pub_subscribe_update_info_t *info);
void tick_on_closed(void *ctx, uint64_t code);
moq_pub_accept_decision_t cb_defer(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error);
bool client_accept_publish(moq_simpair_t *sp);
uint64_t client_publication_done_status(moq_simpair_t *sp, uint64_t streams,
                                        bool eot);
void fanout_setup_ver(test_alloc_state_t *as, moq_alloc_t *alloc,
                      moq_simpair_t **sp, moq_publisher_t **pub_out,
                      moq_pub_track_t **track_out, bool sub_first,
                      uint32_t max_actions, uint64_t version);
void fanout_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                  moq_simpair_t **sp, moq_publisher_t **pub_out,
                  moq_pub_track_t **track_out, bool sub_first,
                  uint32_t max_actions);
int fanout_count_objects(moq_simpair_t *sp, uint64_t g, uint64_t o,
                         const uint8_t *bytes, size_t len);
void count_release(void *ctx, const uint8_t *data, size_t len);
void manual_drain_actions(moq_session_t *s);
void manual_forward(moq_publisher_t *pub, moq_session_t *sv,
                    uint64_t now, moq_event_kind_t want,
                    int *seen, moq_pub_event_result_t *last_res);
int drain_count_resets(moq_session_t *s);
void windows_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                   moq_simpair_t **sp, moq_version_t ver,
                   moq_publisher_t **pub, moq_pub_track_t **track);
moq_subscription_t windows_subscribe(moq_simpair_t *sp,
    moq_publisher_t *pub, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg);
moq_result_t windows_write_dg(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag);
int windows_count(moq_simpair_t *sp, uint64_t g, uint64_t o, uint8_t tag);
void windows_inject_sub_filter(moq_simpair_t *sp, moq_version_t ver,
    int req_seq, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg);
int declare_client_dones(moq_simpair_t *sp, uint64_t *st, uint64_t *sc);
moq_result_t declare_write_stream(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag);
moq_result_t mono_write_stream_eog(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, bool eog, uint8_t tag);
int fo_seen(moq_simpair_t *sp, uint64_t g, uint64_t o, bool *first,
            uint64_t *sgid, int *ended);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

/* Counters provided by the core test-internals library. */
extern unsigned long moq_pub_test_commit_count;
extern unsigned long moq_pub_test_merge_count;
extern unsigned long moq_pub_test_release_count;

#endif /* PUBLISHER_TEST_INTERNAL_H */
