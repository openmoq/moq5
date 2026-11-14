#ifndef MOQ_ACTION_META_H
#define MOQ_ACTION_META_H

#include <stdbool.h>
#include <stdint.h>

/* Private value snapshot, owned beside the action through session and bridge
 * queues. No live request pointer or public action ABI is involved. */
#define MOQ_SCHED_MODE_NONE     0u
#define MOQ_SCHED_MODE_SUBGROUP 1u
#define MOQ_SCHED_MODE_DATAGRAM 2u
#define MOQ_SCHED_MODE_FILL     3u
#define MOQ_SCHED_MODE_FETCH    4u

#define MOQ_DATA_OPENED       1u
#define MOQ_DATA_SUPPRESSED   2u
#define MOQ_DATA_EXTERNAL     3u

typedef struct moq_action_meta {
    uint64_t terminal_owner;     /* owner of PUBLISH_DONE; 0 for other actions */
    bool subgroup_header;       /* the stream prefix, never an object header */
    bool opens_data;            /* fresh parent-counted stream, not a write */
    /* An empty data placeholder retains its scheduling identity until the
     * outgoing terminal event and its replacement RESET_DATA commit. */
    bool terminal_barrier;
    bool outgoing_terminal;
    /* The first action of a local data stream that is not parent-counted
     * (a FETCH response header): the tagged poll marks it FIRST_OF_STREAM. */
    bool stream_first;
    /* The one reset claimed through moq_session_claim_data_reset: it leaves
     * the ring only through the matching commit. */
    bool reset_claimed;
    /* The peer's STOP_SENDING of this RESET_DATA's stream is known: the
     * reset answers it (a FETCH teardown), or the STOP was observed through
     * moq_session_on_data_stop_ex while the reset was held, whatever its
     * first cause. The peer saw the stream, so its open is proven. */
    bool peer_stopped;
    uint64_t deadline_us;        /* UINT64_MAX = no object delivery deadline */
    uint64_t owner;              /* request handle; a fill uses its parent */
    /* On a FETCH response header (drafts 18 and 22): the FETCH's request
     * stream ref, carried with the action so the bridge can keep the pair
     * through the stream's completion even when the FETCH ended natively
     * before the header was handed over. 0 otherwise. */
    uint64_t paired_request;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint64_t object_id;
    uint8_t  subscriber_priority;
    uint8_t  publisher_priority;
    uint8_t  mode;
    uint8_t  group_order;        /* parent's resolved MOQ_GROUP_ORDER_* */
    uint8_t  fill_group_order;   /* fill's own resolved order */
    bool     has_location;
} moq_action_meta_t;

#endif
