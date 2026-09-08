#ifndef MOQRELAY_LOG_INTERNAL_H
#define MOQRELAY_LOG_INTERNAL_H

/*
 * Internal layout shared by log.c and capacity.c (the checker validates
 * invariants white-box). Never installed, never included outside
 * relay/src/core/.
 */

#include <moq/relay/log.h>

#include <moq/rcbuf.h>

/* Resolved defaults (0-valued cfg fields become these). */
#define MOQR_LOG_DEF_MAX_GROUPS       8u
#define MOQR_LOG_DEF_MAX_BYTES        (8u * 1024u * 1024u)
#define MOQR_LOG_DEF_MAX_SUBGROUPS    16u
#define MOQR_LOG_DEF_MAX_OBJECTS      4096u
#define MOQR_LOG_DEF_MAX_RECORDS      4096u
#define MOQR_LOG_DEF_MAX_CURSORS      64u
/* Chunk-node pool: bounds the metadata for OPEN/chunked records (each retained
 * zero-copy rcbuf slice takes one node). Closed-form: max_chunk_nodes nodes. */
#define MOQR_LOG_DEF_MAX_CHUNK_NODES  8192u

/* Sentinel index for "no chunk" (empty list head / free-list tail). */
#define MOQR_CHUNK_NIL                UINT32_MAX

/* Record-vector growth starts here and doubles (alloc+copy+free — no
 * realloc dependency, so exact-size pool allocators recycle cleanly). */
#define MOQR_REC_VEC_INITIAL          8u

/* Journal slot value marking the datagram list (vs a subgroup index). */
#define MOQR_JSLOT_DATAGRAM           UINT32_MAX

/* Sentinel for "final object unknown". */
#define MOQR_FINAL_UNKNOWN            UINT64_MAX

/* One retained payload chunk for an OPEN/chunked record: a zero-copy rcbuf
 * slice (owned) + its length, threaded into the per-log chunk-node pool by
 * index (`next` == MOQR_CHUNK_NIL at the list tail / free-list end). */
typedef struct moqr_chunk_node {
    moq_rcbuf_t *buf;   /* owned; incref'd on append_chunk                  */
    uint64_t     len;
    uint32_t     next;  /* pool index of the next chunk, or MOQR_CHUNK_NIL  */
} moqr_chunk_node_t;

typedef struct moqr_rec {
    uint64_t          arrival_seq;
    uint64_t          arrival_us;    /* ingest time (append desc->now_us)  */
    uint64_t          group_id;
    uint64_t          subgroup_id;   /* undefined when datagram_pref       */
    uint64_t          object_id;
    uint64_t          bytes;         /* retained payload + properties bytes;
                                      * for OPEN records grows per chunk    */
    uint64_t          declared_len;  /* OPEN: declared payload length (wire);
                                      * 0 for whole-object records          */
    moq_rcbuf_t      *payload;       /* owned; NULL for status/OPEN objects */
    moq_rcbuf_t      *properties;    /* owned; NULL if none                */
    uint32_t          chunk_head;    /* pool index of first chunk, or
                                      * MOQR_CHUNK_NIL (whole-object)       */
    uint32_t          chunk_tail;    /* pool index of last chunk (O(1)
                                      * append), or MOQR_CHUNK_NIL          */
    uint32_t          chunk_count;   /* retained chunks (0 = whole-object)  */
    uint8_t           publisher_priority;
    moqr_obj_status_t status;
    moqr_obj_state_t  obj_state;
    moqr_reset_desc_t reset;          /* ABANDONED: the terminal reset cause,
                                       * tagged with the registry it was
                                       * written in (NONE otherwise)           */
    bool              datagram_pref;
    bool              end_of_group;
} moqr_rec_t;

typedef struct moqr_rec_vec {
    moqr_rec_t *recs;
    uint32_t    count;
    uint32_t    cap;
} moqr_rec_vec_t;

typedef struct moqr_subgroup {
    uint64_t       subgroup_id;
    uint64_t       last_object_id;   /* monotonicity guard                 */
    bool           has_records;
    bool           sealed;           /* terminal: no more objects will ever
                                      * be appended (clean FIN, or an
                                      * abnormal reset when sealed_reset)   */
    bool           sealed_reset;     /* the terminal was a RESET: downstream
                                      * closes abnormally with reset_code,
                                      * never as a clean FIN                */
    moqr_reset_desc_t reset;         /* sealed_reset: the upstream RESET
                                      * cause, tagged with its origin       */
    bool           end_of_group;     /* subgroup-header EOG bit: the group
                                      * ends at THIS subgroup's last object —
                                      * which one is knowable only at seal   */
    moqr_rec_vec_t v;
} moqr_subgroup_t;

typedef struct moqr_group {
    uint64_t         group_id;
    uint64_t         final_object_id;  /* MOQR_FINAL_UNKNOWN until known   */
    uint64_t         bytes;            /* retained payload+props bytes     */
    uint64_t         record_count;     /* live records incl. datagrams     */
    uint64_t         first_arrival_us;
    uint64_t         last_arrival_us;
    uint32_t         sg_count;
    moqr_subgroup_t *sgs;              /* fixed array [max_subgroups]      */
    moqr_rec_vec_t   dg;               /* datagram-preference records      */
    uint64_t        *id_set;           /* open-addressing set over Object
                                        * IDs: an object is uniquely
                                        * (track, group, object) and dupes
                                        * are malformed (-18 :795-798,
                                        * :1069-1077). Fixed array
                                        * [log->id_set_cap], UINT64_MAX =
                                        * empty. */
} moqr_group_t;

/* Journal slot: index-based so record-vector growth never dangles it.
 * Dead detection is by resolution failure (evicted group ids are never
 * reused — max_evicted_group_id guards). */
typedef struct moqr_jslot {
    uint64_t group_id;
    uint32_t sg_index;   /* MOQR_JSLOT_DATAGRAM for the datagram list      */
    uint32_t rec_index;
} moqr_jslot_t;

typedef struct moqr_cursor_slot {
    uint64_t seq;         /* next arrival_seq to read                      */
    uint64_t peeked_seq;  /* seq of the last peeked record; UINT64_MAX =
                           * no outstanding peek                           */
    uint64_t skips;       /* eviction-forced jumps observed                */
    uint32_t generation;  /* odd = live (handle discipline)                */
} moqr_cursor_slot_t;

struct moqr_log {
    moq_alloc_t   alloc;      /* copied vtable                             */
    moqr_trace_t *trace;      /* borrowed; may be NULL                     */
    uint64_t      trace_id;
    uint16_t      shard_tag;  /* nonzero handle tag                        */

    /* Resolved bounds. */
    uint32_t max_groups;
    uint64_t max_bytes;
    uint64_t max_age_us;      /* 0 = disabled                              */
    uint32_t max_subgroups;
    uint32_t max_objects_per_group;
    uint32_t max_records;
    uint32_t max_cursors;
    uint32_t max_chunk_nodes; /* chunk-node pool size (closed-form bound)   */
    uint32_t id_set_cap;      /* pow2 >= 2 x max_objects_per_group         */

    /* Groups, sorted ascending by group_id. */
    moqr_group_t *groups;     /* fixed array [max_groups]                  */
    uint32_t      group_count;
    uint64_t      max_evicted_group_id;  /* 0 = none evicted yet           */
    bool          any_evicted;

    /* End of track. */
    bool     end_of_track;
    uint64_t final_group_id;
    uint64_t final_track_object_id;

    /* Journal ring (arrival order). Window is [begin_seq, next_seq). */
    moqr_jslot_t *journal;    /* fixed array [max_records]                 */
    uint64_t      begin_seq;
    uint64_t      next_seq;
    uint64_t      live_records;

    /* Cursors. */
    moqr_cursor_slot_t *cursors;  /* fixed array [max_cursors]             */
    uint32_t            cursor_count;

    /* Chunk-node pool for OPEN/chunked record payload slices. Fixed array
     * [max_chunk_nodes]; free nodes are threaded through `next` from
     * chunk_free_head (MOQR_CHUNK_NIL when exhausted). */
    moqr_chunk_node_t *chunk_pool;
    uint32_t           chunk_free_head;
    uint32_t           chunk_used;

    /* Aggregates. */
    uint64_t retained_bytes;
    uint64_t appended_total;
    uint64_t evicted_groups_total;
    uint64_t evicted_records_total;
};

/* Prefix-safe resolved view of a caller config: every field read is gated
 * by struct_size containment; absent fields take library defaults. Shared
 * by moqr_log_create and moqr_log_capacity_describe so the two can never
 * disagree about what a config means. Defined in log.c. */
typedef struct moqr_log_cfg_resolved {
    const moq_alloc_t *alloc;   /* NULL when not contained in struct_size */
    moqr_log_budget_t  budget;  /* defaults applied                       */
    uint32_t           max_subgroups;
    uint32_t           max_objects_per_group;
    uint32_t           max_records;
    uint32_t           max_cursors;
    uint32_t           max_chunk_nodes;
    uint32_t           id_set_cap;
    moqr_trace_t      *trace;
    uint64_t           trace_id;
} moqr_log_cfg_resolved_t;

moqr_result_t moqr_log_cfg_resolve(const moqr_log_cfg_t *cfg,
                                   moqr_log_cfg_resolved_t *out);

/* Structure-byte ceiling from resolved limits (shared by describe and the
 * checker so the formula exists exactly once). */
uint64_t moqr_log_structure_ceiling(const moqr_log_cfg_resolved_t *r);

#endif /* MOQRELAY_LOG_INTERNAL_H */
