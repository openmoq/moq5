#ifndef MOQ_MEDIA_SENDER_PUMP_INTERNAL_H
#define MOQ_MEDIA_SENDER_PUMP_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void sender_fire_closed(moq_media_sender_t *s, bool is_fatal,
                        uint64_t fatal_code);

void sender_fire_ready(moq_media_sender_t *s);

void sender_set_fatal(moq_media_sender_t *s, uint64_t code);

void sender_resync_track_demand(moq_media_sender_t *s,
                                moq_media_track_t *t);

void sender_pub_on_publish_finished(void *ctx, moq_pub_track_t *pt);

void sender_hook(moq_endpoint_t *ep, moq_session_t *session,
                 uint64_t now_us, void *ctx);

moq_result_t sender_destroy_pub_task(moq_endpoint_t *ep,
                                     moq_session_t *session,
                                     uint64_t now_us, void *ctx);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_PUMP_INTERNAL_H */
