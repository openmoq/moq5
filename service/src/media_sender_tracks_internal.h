#ifndef MOQ_MEDIA_SENDER_TRACKS_INTERNAL_H
#define MOQ_MEDIA_SENDER_TRACKS_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moq_bytes_t default_role(moq_media_type_t t);

void track_free(moq_media_sender_t *s, moq_media_track_t *t);

bool removed_pubtrack_with_name(moq_media_sender_t *s,
                                moq_media_track_t *const *tracks,
                                size_t n, moq_bytes_t name);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_TRACKS_INTERNAL_H */
