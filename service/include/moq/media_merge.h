#ifndef MOQ_MEDIA_MERGE_H
#define MOQ_MEDIA_MERGE_H

/*
 * Delivery redundancy: one publisher sends identical Objects to two relays,
 * and the application runs one media receiver per relay. A merge takes the
 * Objects that the receivers poll for one MPEG-TS track and hands each
 * Location back once, in Location order. The first copy of a Location wins;
 * a later copy is counted and released. A Location that one path lost is
 * filled by the other path's copy if it arrives within the window.
 *
 * Every Object at one Location carries the same bytes whichever relay
 * delivers it (MOQT Object model), so the merge does not compare copies.
 *
 * One merge serves one track. The caller selects the same track on each
 * receiver (by its catalog namespace and name: the track handles differ per
 * receiver) and offers only that track's Objects; the merged Objects carry no
 * track handle.
 *
 * The merge is single-threaded: the caller serializes every call on one
 * merge. It owns no session and no receiver: the application connects,
 * reconnects and destroys each path's receiver, and keeps polling the live
 * ones. The receivers should keep their own reorder window: a receiver
 * refuses a Location it has already released, so a receiver without one
 * would drop a Group's tail that the next Group overtook.
 *
 * The merge neither inserts join PSI nor strips 192-octet prefixes. Turn both
 * off on each receiver, and apply them to the merged stream: the join PSI to
 * the first merged Object (moq_mpeg2ts_align_join_psi), and the prefix
 * stripping to every merged Object (moq_mpeg2ts_strip_prefix), both in
 * <moq/mpeg2ts.h>.
 *
 * A copy is told apart as a duplicate only while its Location is among the
 * last 2 x slot_count Locations released; an older copy is counted as late.
 */

#include <moq/media_receiver.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_MEDIA_MERGE_MAX_PATHS 4u

typedef struct moq_media_merge moq_media_merge_t;

typedef struct moq_media_merge_cfg {
    uint32_t struct_size;
    /* Number of paths, 2 to MOQ_MEDIA_MERGE_MAX_PATHS. Default 2. */
    uint32_t path_count;
    /* MPEG-TS packet size of the track's Objects: 188, or 192 in Timestamp
     * Mode. Default 188. */
    uint32_t packet_size;
    /* Program whose PMT gives the PCR PID for gap durations; 0 is the first
     * program of the PAT. Default 0. */
    uint16_t program_number;
    uint16_t _reserved;
    /* Objects held at once while a Location is missing. Default 128. */
    uint32_t slot_count;
    /* How long a missing Location is awaited before it is reported lost:
     * each receiver's own reorder window plus the delivery skew between the
     * paths. Default 600000 (600 ms). */
    uint64_t window_us;
} moq_media_merge_cfg_t;

/* Two paths, 188-octet packets, the first program, 128 slots, 600 ms. */
MOQ_API void moq_media_merge_cfg_init_sized(moq_media_merge_cfg_t *cfg,
                                            size_t cfg_size);
#define moq_media_merge_cfg_init(cfg) \
    moq_media_merge_cfg_init_sized((cfg), sizeof(*(cfg)))

/* Per-path counters, cumulative over the merge's life. */
typedef struct moq_media_merge_path_stats {
    uint32_t struct_size;
    uint64_t offered;     /* Objects offered on this path */
    uint64_t delivered;   /* first copies of a Location handed back */
    uint64_t duplicates;  /* copies of a Location already held or delivered */
    uint64_t late;        /* came after their Location was given up as lost */
    uint64_t invalid;     /* first copies whose payload is not valid MPEG-TS */
    uint64_t last_offer_us; /* now_us of the last offer; 0 before the first */
} moq_media_merge_path_stats_t;

/*
 * Returns MOQ_OK, MOQ_ERR_INVAL for a NULL out, a cfg struct_size too small
 * for window_us, or an invalid cfg, MOQ_ERR_NOMEM, and MOQ_ERR_UNSUPPORTED
 * when the library was built without MPEG-TS support. alloc NULL uses
 * moq_alloc_default().
 */
MOQ_API moq_result_t moq_media_merge_create(const moq_media_merge_cfg_t *cfg,
                                            const moq_alloc_t *alloc,
                                            moq_media_merge_t **out);

/* Releases every held Object. Safe on NULL. */
MOQ_API void moq_media_merge_destroy(moq_media_merge_t *m);

/*
 * Offer an Object that path `path` polled with moq_media_receiver_poll_object.
 * obj_size is sizeof(*obj) as the caller compiled it. The Object must carry
 * its identity (its stamped struct_size covers has_subgroup) and the MPEG2TS
 * packaging. An END_OF_GROUP status Object ends its Group, as
 * moq_media_merge_end_group_at would.
 *
 * MOQ_OK: the merge took the Object and *obj is zeroed. It is held, or, when
 * it is a duplicate or late, counted and released. MOQ_ERR_BUFFER: every slot
 * is in use; poll first. MOQ_ERR_INVAL: NULL arguments, a bad path, an
 * Object without identity or of another packaging. On any error *obj is
 * left untouched and stays the caller's.
 */
MOQ_API moq_result_t moq_media_merge_offer(moq_media_merge_t *m, uint32_t path,
                                           uint64_t now_us,
                                           moq_media_object_t *obj,
                                           size_t obj_size);

/*
 * The Group ended on path `path`: call it for a FIN_END_OF_GROUP stream
 * terminal (moq_media_receiver_poll_stream_terminal, with the receiver's
 * surface_stream_terminals on), after that path's Objects of the subgroup
 * were offered. The next Group is then released without waiting for the
 * window. MOQ_ERR_INVAL for NULL or a bad path.
 */
MOQ_API moq_result_t moq_media_merge_end_group(moq_media_merge_t *m,
                                               uint32_t path, uint64_t group);

/*
 * Hand back the next Location that is due: MOQ_OK moves the Object into
 * *obj (release it with moq_media_object_cleanup) and sets *path (optional)
 * to the path whose copy it is; MOQ_DONE when nothing is due yet. The
 * Object's track is NULL: the receiver it came from may be gone. When the
 * Object's stamped struct_size covers its loss report, the report describes
 * the merged stream, not one path. MOQ_ERR_BUFFER when obj_size is smaller
 * than the Object's stamped struct_size (nothing is moved); MOQ_ERR_INVAL
 * for NULL arguments.
 */
MOQ_API moq_result_t moq_media_merge_poll(moq_media_merge_t *m,
                                          uint64_t now_us,
                                          moq_media_object_t *obj,
                                          size_t obj_size, uint32_t *path);

/* The time from which poll makes progress for the window; UINT64_MAX when
 * nothing is held. An Object may become due earlier, after an offer or an
 * end_group. */
MOQ_API uint64_t moq_media_merge_next_deadline_us(const moq_media_merge_t *m);

/* End of the track: every held Object is due, in Location order. */
MOQ_API void moq_media_merge_finish(moq_media_merge_t *m);

/* MOQ_OK, or MOQ_ERR_INVAL for NULL arguments, a bad path, or a stats_size
 * below sizeof(uint32_t). Writes min(stats_size, sizeof) bytes and stamps
 * struct_size with them. */
MOQ_API moq_result_t moq_media_merge_get_path_stats(
    const moq_media_merge_t *m, uint32_t path,
    moq_media_merge_path_stats_t *stats, size_t stats_size);

#ifdef __cplusplus
}
#endif
#endif
