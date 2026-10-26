#ifndef MOQ_PQ_RECV_BUDGET_H
#define MOQ_PQ_RECV_BUDGET_H

/*
 * Fair division of a lane's per-turn receive budget among its ready sockets.
 * A lane serves one socket per address family (IPv4 and IPv6) on the shared
 * port; spending the whole budget on the first ready socket lets sustained
 * traffic on one family starve the other turn after turn. Each ready socket
 * instead gets an equal share (at least one), so a second ready socket is
 * always served within the turn, and the shares never exceed the budget.
 * Pure, no allocation. PRIVATE to adapters/picoquic.
 */

static inline int pq_recv_share(int ready, int budget)
{
    if (ready <= 1) return budget;      /* one family ready: the whole budget */
    int share = budget / ready;
    return share < 1 ? 1 : share;       /* every ready socket gets served */
}

#endif /* MOQ_PQ_RECV_BUDGET_H */
