#ifndef MOQ_PQ_LANE_GROUP_H
#define MOQ_PQ_LANE_GROUP_H

/*
 * A lane group: a server facade with cfg.lane_count > 1. PRIVATE to
 * adapters/picoquic.
 *
 * The group facade owns N lanes. Each lane is an internal single-lane facade
 * (its own picoquic context, connections and pump) whose loop is the group's,
 * on the lane's own thread: it polls the lane's sockets on the shared port,
 * its wake pipe and its inbox, and drives the facade's loop callback in
 * picoquic's callback order. A datagram is routed by its first connection
 * ID: a connection ID the group issued names its owner lane (an encrypted
 * lane tag), a client-chosen one hashes to its owner, so a connection is
 * served by one lane whatever socket the kernel delivers to. A datagram for
 * another lane is copied into that lane's inbox; one no lane can attribute is
 * offered to every lane as a stateless-reset candidate, with reset sending
 * disabled for that look, so only a connection's owner ever answers.
 */

#include "pq_threaded_internal.h"

#if defined(__GNUC__) || defined(__clang__)
/* The lane group's internals are not part of the adapter's ABI. */
#  pragma GCC visibility push(hidden)
#endif

struct pq_lane_group;

/* What the group hands the facade's create for each lane. */
typedef struct pq_lane_opts {
    moq_pq_threaded_t *group;        /* the group facade, the hooks' `t` */
    uint32_t           index;        /* this lane's index */
    int                wake_fd;      /* the loop's wake pipe, write end */
    picoquic_connection_id_cb_fn cnx_id_cb;
    void              *cnx_id_ctx;
    uint8_t            cid_len;      /* the group's connection ID length */
    const uint8_t     *ticket_key;   /* shared: tokens validate on any lane */
    size_t             ticket_key_len;
} pq_lane_opts_t;

/* moq_picoquic_threaded.c: create one lane facade. With opts, no packet
 * loop thread is started; the group runs the loop. */
moq_result_t moq_pq_threaded_create_lane(const moq_pq_threaded_cfg_t *cfg,
                                         const pq_lane_opts_t *opts,
                                         moq_pq_threaded_t **out);

/* The group's admission of a connection on a lane: the group total is
 * cfg.max_connections. Network thread of that lane. */
bool pq_lane_group_admit(moq_pq_threaded_t *lane);
void pq_lane_group_release(moq_pq_threaded_t *lane);

/* The group facade's create: cfg.lane_count lanes. */
moq_result_t pq_lane_group_create(const moq_pq_threaded_cfg_t *cfg,
                                  moq_pq_threaded_t **out);

uint32_t           pq_lane_group_count(const struct pq_lane_group *g);
moq_pq_threaded_t *pq_lane_group_lane(struct pq_lane_group *g, uint32_t i);
size_t             pq_lane_group_conn_count(const struct pq_lane_group *g);
int                pq_lane_group_drain_state(struct pq_lane_group *g);
bool               pq_lane_group_is_fatal(const struct pq_lane_group *g);
uint64_t           pq_lane_group_fatal_code(const struct pq_lane_group *g);
moq_result_t       pq_lane_group_wake_all(struct pq_lane_group *g);
moq_result_t       pq_lane_group_stop(struct pq_lane_group *g);
void               pq_lane_group_destroy(struct pq_lane_group *g);

/* A lane facade's stop, from moq_pq_threaded_stop on that lane: wake its loop
 * so it observes the stop, and wait (bounded) for the loop to exit. */
void pq_lane_group_lane_stop(moq_pq_threaded_t *lane);

#if defined(__GNUC__) || defined(__clang__)
#  pragma GCC visibility pop
#endif
#endif /* MOQ_PQ_LANE_GROUP_H */
