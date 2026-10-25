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
/* Chunk-node pool: bounds the metadata for chunked records, OPEN and COMPLETE
 * alike (each retained zero-copy rcbuf slice takes one node until its record is
 * abandoned or its group evicted). Closed-form: max_chunk_nodes nodes. */
#define MOQR_LOG_DEF_MAX_CHUNK_NODES  8192u

/* Subgroups remembered by moqr_log_note_gap before they have a list. */
#define MOQR_LOG_HOLES                8u

/* Sentinel index for "no chunk" (empty list head / free-list tail). */
#define MOQR_CHUNK_NIL                UINT32_MAX

/* Record-vector growth starts here and doubles (alloc+copy+free — no
 * realloc dependency, so exact-size pool allocators recycle cleanly). */
#define MOQR_REC_VEC_INITIAL          8u

/* Journal slot value marking the datagram list (vs a subgroup index). */
#define MOQR_JSLOT_DATAGRAM           UINT32_MAX

/* Historical reset value: an unbounded final. Not a presence test;
 * has_final distinguishes unknown from a known final at UINT64_MAX. */
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
    bool              first_object;  /* first object ever published in its
                                      * subgroup (upstream FIRST_OBJECT)    */
    bool              subgroup_properties; /* upstream header PROPERTIES     */
    uint8_t           properties_codec;    /* MOQR_PROPS_CODEC_* at ingest   */
    bool              follows_gap;   /* not known to continue the stream the
                                      * list's previous record was on: an
                                      * earlier object of the subgroup was
                                      * not retained (moqr_log_note_gap), or
                                      * the record opened a new upstream
                                      * stream of the subgroup after the
                                      * previous one ended                 */
    moqr_reset_desc_t gap_reset;     /* follows_gap after an upstream RESET:
                                      * the reset that ended the previous
                                      * stream (NONE otherwise)             */
    bool              has_opener;    /* OPEN: opener_* name its writer      */
    uint32_t          opener_conn;   /* the ingest source that opened it, as */
    uint64_t          opener_src;    /* the caller names it                 */
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
    bool           gap_pending;      /* the next record follows a refused one */
    bool           incomplete;       /* an object was refused: the graceful
                                      * terminal is recorded as a RESET      */
    moqr_rec_vec_t v;
} moqr_subgroup_t;

typedef struct moqr_group {
    uint64_t         group_id;
    uint64_t         final_object_id;  /* meaningful when has_final        */
    uint64_t         bytes;            /* retained payload+props bytes     */
    uint64_t         record_count;     /* live records incl. datagrams     */
    uint64_t         first_arrival_us;
    uint64_t         last_arrival_us;
    uint32_t         sg_count;
    bool             has_final;
    bool             has_max_object;   /* UINT64_MAX identity, outside set */
    moqr_subgroup_t *sgs;              /* fixed array [max_subgroups]      */
    moqr_rec_vec_t   dg;               /* datagram-preference records      */
    uint64_t        *id_set;           /* open-addressing set over Object
                                        * IDs: an object is uniquely
                                        * (track, group, object) and dupes
                                        * are malformed (-18 :795-798,
                                        * :1069-1077). Fixed array
                                        * [log->id_set_cap], UINT64_MAX =
                                        * empty; MAX itself is represented
                                        * by has_max_object. */
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

    /* Subgroups an object was refused for before the subgroup had a list:
     * applied when the list is created (moqr_log_note_gap). */
    struct {
        bool     used;
        uint64_t group_id;
        uint64_t subgroup_id;
    } holes[MOQR_LOG_HOLES];
    uint32_t holes_next;

    /* Aggregates. */
    uint64_t retained_bytes;
    uint64_t appended_total;
    uint64_t evicted_groups_total;
    uint64_t evicted_records_total;
};

/* The per-record marks a delivery needs but the public record view does not
 * carry: the upstream subgroup's PROPERTIES form, whether the record is not
 * known to continue the stream its list's previous record was on (it follows
 * a refused object, see moqr_log_note_gap, or it opens a new upstream stream
 * of the subgroup), and the terminal owed to that previous stream (the
 * upstream RESET that ended it, or none). All clear for the datagram list or
 * an absent record. Defined in log.c. */
void moqr_log_internal_rec_flags(const moqr_log_t *log, uint64_t group_id,
                                 uint32_t list_slot, uint32_t rec_idx,
                                 bool *subgroup_properties, bool *follows_gap,
                                 moqr_reset_desc_t *gap_reset);

/* The arrival time of the record at (group_id, object_id), whatever its
 * status or state. False when no record is retained there. Defined in
 * log.c. */
bool moqr_log_internal_object_arrival(const moqr_log_t *log, uint64_t group_id,
                                      uint64_t object_id, uint64_t *arrival_us);

/* An object of (group_id, subgroup_id) that arrived was not retained (its
 * append was refused): the subgroup's retained records are no longer the
 * whole subgroup. The next record appended to the subgroup is marked
 * follows_gap, and the subgroup's graceful terminal is recorded as a RESET
 * with the relay code MOQR_RESET_CODE_EVICTED instead of a seal, so no
 * downstream stream for it ends with a FIN (draft-22 §11.3.2: a sender that
 * closes a stream before delivering all objects MUST reset it). A subgroup
 * with no retained record yet is remembered (a bounded set of the most
 * recent ones) until its first record arrives. MOQR_OK when recorded,
 * MOQR_DONE when the group is not retained. Defined in log.c. */
moqr_result_t moqr_log_note_gap(moqr_log_t *log, uint64_t group_id,
                                uint64_t subgroup_id);

/* Classify a descriptor against the record retained at its Location
 * (draft-22 §7.1: identity is the Group and Object ID, whatever the list):
 * MOQR_LOG_DUP_* (retrieve_internal.h). Delivery Mode, Subgroup ID and
 * Priority must match. A Normal record and a status at the same Location
 * that says the object does not exist (End of Group / End of Track) is the
 * permitted existing-to-missing transition (UPDATE). A whole copy is
 * compared byte for byte; a copy streamed in chunks (an OPEN descriptor)
 * whose metadata and declared length match is PENDING: its chunks are
 * compared as they arrive (moqr_log_duplicate_range). Object Properties are
 * not compared: §7.1 lets them change. Defined in log.c. */
uint32_t moqr_log_duplicate(const moqr_log_t *log, const moqr_log_append_desc_t *desc);

/* Name the ingest source (`conn`, `src`) that opened the OPEN record at
 * (group, subgroup, object). MOQR_ERR_WRONG_STATE when no such record is
 * OPEN. Defined in log.c. */
moqr_result_t moqr_log_set_opener(moqr_log_t *log, uint64_t group_id,
                                  uint64_t subgroup_id, uint64_t object_id,
                                  uint32_t conn, uint64_t src);

/* True when the record at (group, subgroup, object) is OPEN and was opened by
 * a source other than (`conn`, `src`): that source's chunks are not this
 * record's and must not be appended to, complete or abandon it. False for a
 * record that is not OPEN or names no opener. Defined in log.c. */
bool moqr_log_open_by_other(const moqr_log_t *log, uint64_t group_id,
                            uint64_t subgroup_id, uint64_t object_id,
                            uint32_t conn, uint64_t src);

/* Compare `len` bytes at `offset` of a copy's payload with the payload of
 * the record retained at (group, subgroup, object): MOQR_LOG_RANGE_*. A
 * record still arriving answers UNKNOWN for bytes it does not hold yet.
 * Defined in log.c. */
uint32_t moqr_log_duplicate_range(const moqr_log_t *log, uint64_t group_id,
                                  uint64_t subgroup_id, uint64_t object_id,
                                  uint64_t offset, const uint8_t *data, size_t len);

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
