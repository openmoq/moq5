#ifndef MOQ_MEDIA_RECEIVER_INTERNAL_H
#define MOQ_MEDIA_RECEIVER_INTERNAL_H

/* Service-tier access to a receiver's internals for components built on
 * it. Never exported. */

#include <moq/media_receiver.h>
#include <moq/endpoint.h>

/* The endpoint the receiver runs on (its own, or the one it was attached
 * to); NULL for NULL. */
moq_endpoint_t *moq_media_receiver_endpoint_internal(const moq_media_receiver_t *r);

/* Whether a poll would return something now: what moq_media_receiver_wait
 * returns MOQ_OK for, without waiting or taking a wake. False for NULL. */
bool moq_media_receiver_has_input_internal(moq_media_receiver_t *r);

/* Objects and stream terminals of `track` the receiver still queues: input
 * it took in that the application has not polled. 0 for NULL. */
size_t moq_media_receiver_track_input_internal(moq_media_receiver_t *r,
                                              const moq_media_track_t *track);

#endif /* MOQ_MEDIA_RECEIVER_INTERNAL_H */
