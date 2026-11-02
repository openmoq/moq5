#ifndef MOQ_MEDIA_SENDER_CATALOG_INTERNAL_H
#define MOQ_MEDIA_SENDER_CATALOG_INTERNAL_H

#include "media_sender_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void track_fill_msf(const moq_media_track_t *t, moq_msf_track_t *m);

bool track_in_catalog(const moq_media_track_t *t, bool only_registered);

bool sender_tracks_snapshot(moq_media_sender_t *s,
                            moq_media_track_t ***out_snap,
                            size_t *out_count);

void sender_tracks_release(moq_media_sender_t *s,
                           moq_media_track_t **snap, size_t n);

bool sender_init_by_reference(const moq_media_track_t *t);

uint64_t sender_group_base(const moq_media_sender_t *s);

bool sender_catalog_group_available(const moq_media_sender_t *s);

moq_result_t sender_build_catalog_fresh(moq_media_sender_t *s,
                                        moq_rcbuf_t **out,
                                        bool only_registered,
                                        uint64_t *out_stamp);

uint8_t sender_track_scale_of(moq_bytes_t track_properties);

bool sender_auto_d22_possible_locked(const moq_media_sender_t *s);

void sender_commit_baseline(moq_media_sender_t *s);

moq_result_t sender_catalog_semantically_unchanged(
    moq_media_sender_t *s, bool only_registered, bool *out_unchanged);

bool sender_stage_generation(moq_media_sender_t *s);

void sender_arm_refresh(moq_media_sender_t *s, uint64_t now_us);

void sender_recompute_wake_deadline(moq_media_sender_t *s);

bool sender_has_pending_output(void *ctx);

uint64_t sender_next_deadline_us(void *ctx);

void sender_republish_catalog(moq_media_sender_t *s, uint64_t now_us);

moq_result_t cp_validate_one(const moq_cmsf_content_protection_t *cp);

void free_root_cps(moq_media_sender_t *s);

moq_result_t sender_copy_root_cps(moq_media_sender_t *s,
    const moq_cmsf_content_protection_t *src, size_t count);

bool sender_has_cp_ref(const moq_media_sender_t *s, moq_bytes_t ref);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQ_MEDIA_SENDER_CATALOG_INTERNAL_H */
