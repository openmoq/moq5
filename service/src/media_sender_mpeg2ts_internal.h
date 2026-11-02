#ifndef MOQ_MEDIA_SENDER_MPEG2TS_INTERNAL_H
#define MOQ_MEDIA_SENDER_MPEG2TS_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

moq_result_t sender_check_extensions(const moq_media_track_cfg_t *cfg,
                                     const moq_msf_mpeg2ts_t **rec);

moq_result_t sender_copy_extensions(moq_media_sender_t *s,
                                    moq_media_track_t *t,
                                    const moq_msf_mpeg2ts_t *src);

moq_msf_mpeg2ts_t *sender_ts_record(moq_media_track_t *t);

#ifdef MOQ_SERVICE_HAVE_MPEG2TS

void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t);

#else

void sender_ts_free(moq_media_sender_t *s, moq_media_track_t *t);

#endif

moq_result_t sender_ts_prepare(moq_media_sender_t *s,
                               moq_media_track_t *t,
                               const moq_media_track_cfg_t *cfg);

#ifdef MOQ_SERVICE_HAVE_MPEG2TS

moq_result_t sender_ts_queue(moq_media_sender_t *s,
                             moq_media_track_t *t,
                             sender_ts_obj_t *list, size_t *n,
                             bool *queued_any);

moq_result_t sender_ts_queue_staged(moq_media_sender_t *s,
                                    moq_media_track_t *t,
                                    bool *queued_any);

bool sender_ts_complete_ready(const moq_media_track_t *t);

bool sender_ts_complete_step(moq_media_sender_t *s, moq_media_track_t *t);

#endif
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_MPEG2TS_INTERNAL_H */
