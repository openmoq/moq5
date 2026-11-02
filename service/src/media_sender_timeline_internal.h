#ifndef MOQ_MEDIA_SENDER_TIMELINE_INTERNAL_H
#define MOQ_MEDIA_SENDER_TIMELINE_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

uint64_t sap_ept_ms_from_us(uint64_t pts_us);

void sap_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                        uint64_t media_group, uint64_t media_object,
                        uint8_t sap_type, uint64_t ept_ms);

bool sender_track_demand(moq_media_sender_t *s, moq_media_track_t *t);

void sender_emit_sap_for(moq_media_sender_t *s, moq_media_track_t *tl,
                         uint64_t now_us);

void mt_history_append(moq_media_sender_t *s, moq_media_track_t *tl,
                       uint64_t media_group, uint64_t media_object,
                       uint64_t media_time_ms, uint64_t wallclock_ms);

void sender_emit_mt_for(moq_media_sender_t *s, moq_media_track_t *tl,
                        uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_TIMELINE_INTERNAL_H */
