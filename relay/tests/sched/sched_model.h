#ifndef MOQR_SCHED_MODEL_H
#define MOQR_SCHED_MODEL_H

/*
 * The independent reference model for the seeded scheduler explorer, and the
 * operation grammar it enables. PURE: no production headers, no clocks, no
 * allocation — the same sources build in the rig-free trace-vector unit and
 * in the full explorer, so the generated trace bytes are a function of
 * (seed, grammar version, configuration constants) alone.
 *
 * The model is updated only from the grammar; capacities come from the
 * configuration constants below, never from production snapshots. Grammar
 * constraints that exist to keep the credit arithmetic exact (rather than to
 * mirror a production restriction) are marked GRAMMAR-CONSTRAINT.
 */

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "../seedx/seedx_ledger.h"
#include "../seedx/seedx_trace.h"

/* -- configuration constants (folded into config_hash) ----------------- */

#define SCHED_LANES        2u
#define SCHED_MAX_LIVE     4u   /* == the facade max_connections          */
#define SCHED_MAX_CHILDREN 16u  /* trace child ids (never reused; reaped
                                 * slots admit fresh accepts up to LIVE)   */
#define SCHED_NS_COUNT     2u   /* ns id i is owned by shard i (asserted
                                 * against production placement at rig
                                 * init; the byte strings are rig config) */
#define SCHED_CREDIT_CAP   2u   /* demand_channel_entries                 */
#define SCHED_OPS_PER_SEED 256u
#define SCHED_GRAMMAR_VERSION 3u

/* -- operation codes (the trace vocabulary) ----------------------------- */

#define SCHED_OP_ACCEPT    0x01u /* lane                                  */
#define SCHED_OP_STEP      0x02u /* lane                                  */
#define SCHED_OP_WAKE      0x03u /* lane                                  */
#define SCHED_OP_SHUTTLE   0x04u /* a=child, b=budget class 0..4          */
#define SCHED_OP_ESTABLISH 0x05u /* a=child                               */
#define SCHED_OP_ANNOUNCE  0x06u /* a=child, b=ns                         */
#define SCHED_OP_WITHDRAW  0x07u /* a=child, b=ns (publisher-side)        */
#define SCHED_OP_REVOKE    0x08u /* a=child, b=ns (relay-side)            */
#define SCHED_OP_SUBSCRIBE 0x09u /* a=child, b=ns                         */
#define SCHED_OP_CANCEL    0x0Au /* a=child                               */
#define SCHED_OP_PUSH      0x0Bu /* a=child (publisher)                   */
#define SCHED_OP_CONSUME   0x0Cu /* lane (requester drain step)           */
#define SCHED_OP_RETRY     0x0Du /* a=src lane, b=dst lane                */
#define SCHED_OP_CLOSE_FEED_FAULT 0x0Eu /* an admission whose FIRST
                                 * StreamStart is refused: the bridge
                                 * latches its first fatal (0x1) while the
                                 * session stays open, then the transport
                                 * terminal arrives — the close-feed path
                                 * must still deliver exactly one
                                 * SESSION_CLOSED carrying that first
                                 * fatal code                             */
#define SCHED_OP_TERMINAL  0x0Fu /* a=child                               */
#define SCHED_OP_STOP      0x10u /* terminates the trace                  */
#define SCHED_OP_MAX       0x10u

/* -- model state --------------------------------------------------------- */

typedef struct sched_child {
    bool    used;
    uint8_t lane;
    bool    established;
    bool    terminal;      /* TERMINAL issued                              */
    bool    reap_expected; /* awaiting reclamation                         */
    int8_t  announced_ns;  /* -1 or the ns id this child publishes         */
    int8_t  sub_ns;        /* -1 or the ns id this child subscribes to     */
    bool    sub_accepted;  /* SUBSCRIBE_OK observed (set by the executor's
                            * deterministic driver policy; the generator
                            * treats subscribe->accept as one scripted
                            * exchange the way ESTABLISH is)               */
} sched_child_t;

typedef struct sched_model {
    sched_child_t child[SCHED_MAX_CHILDREN];
    uint32_t      children_ever;              /* next accept's child id    */
    uint32_t      rr_next_lane;               /* mirror of the facade RR   */
    int8_t        ns_pub[SCHED_NS_COUNT];     /* announcement winner or -1 */
    int8_t        src_child[SCHED_NS_COUNT];  /* the ESTABLISHED track
                                               * source — set when a
                                               * subscribe establishes
                                               * against the then-current
                                               * winner, cleared only by a
                                               * real track-source terminal
                                               * transition (last cancel,
                                               * or revocation of the
                                               * source). Announcement
                                               * state never implies it. */
    sx_pair_t     pair[SCHED_LANES][SCHED_LANES]; /* [src][dst], src!=dst  */
    bool          owed_pump[SCHED_LANES];
    bool          stopped;
    uint32_t      live_children;
} sched_model_t;

static inline void sched_model_init(sched_model_t *m)
{
    memset(m, 0, sizeof(*m));
    for (uint32_t i = 0; i < SCHED_NS_COUNT; i++) {
        m->ns_pub[i] = -1;
        m->src_child[i] = -1;
    }
    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        for (uint32_t d = 0; d < SCHED_LANES; d++) {
            m->pair[s][d].capacity = SCHED_CREDIT_CAP;
        }
    }
}

/* ns id -> owning shard: by construction of the rig's ns byte strings. */
static inline uint8_t sched_ns_owner(uint8_t ns)
{
    return ns; /* ns i owned by shard i — asserted against production */
}

/* -- enabled-set computation --------------------------------------------- *
 * One entry per legal (op, args) tuple. The generator draws uniformly over
 * this list, so a seeded trace never contains a precondition-failing record.
 */

typedef struct sched_op {
    uint8_t  op;
    uint8_t  lane;
    uint16_t a, b, c;
} sched_op_t;

#define SCHED_ENABLED_MAX 128

static inline int
sched_enabled(const sched_model_t *m, uint32_t op_index, sched_op_t *out,
              int cap)
{
    int n = 0;
#define EMIT(o, l, aa, bb, cc)                                             \
    do {                                                                   \
        if (n < cap) {                                                     \
            out[n].op = (o); out[n].lane = (uint8_t)(l);                   \
            out[n].a = (uint16_t)(aa); out[n].b = (uint16_t)(bb);          \
            out[n].c = (uint16_t)(cc);                                     \
        }                                                                  \
        n++;                                                               \
    } while (0)

    if (m->stopped) {
        /* STOP terminates the trace: nothing is enabled after it. */
        return 0;
    }

    /* STEP / WAKE: always. STEP carries a double weight so the schedule
     * interleaves generously without drowning the flow operations
     * (documented weight, part of the grammar version). */
    for (uint32_t l = 0; l < SCHED_LANES; l++) {
        EMIT(SCHED_OP_STEP, l, 0, 0, 0);
        EMIT(SCHED_OP_STEP, l, 0, 0, 0);
        EMIT(SCHED_OP_WAKE, l, 0, 0, 0);
    }

    if (m->children_ever < SCHED_MAX_CHILDREN &&
        m->live_children < SCHED_MAX_LIVE) {
        /* Child ids are never reused in a trace; the facade cap gates the
         * LIVE population. The lane recorded is the model's own
         * round-robin expectation — I1's oracle. */
        EMIT(SCHED_OP_ACCEPT, m->rr_next_lane % SCHED_LANES,
             m->children_ever, 0, 0);
        EMIT(SCHED_OP_CLOSE_FEED_FAULT, m->rr_next_lane % SCHED_LANES,
             m->children_ever, 0, 0);
    }

    for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
        const sched_child_t *ch = &m->child[c];

        if (!ch->used || ch->terminal) {
            continue;
        }
        /* SHUTTLE: enabled for any live child; budget class scans the
         * fragmentation schedule. A shuttle with nothing pending is a
         * recorded NOOP disposition, not a skip. */
        EMIT(SCHED_OP_SHUTTLE, ch->lane, c, (op_index % 5u), 0);
        EMIT(SCHED_OP_SHUTTLE, ch->lane, c, 0, 0);
        if (!ch->established) {
            EMIT(SCHED_OP_ESTABLISH, ch->lane, c, 0, 0);
            continue;
        }
        if (ch->announced_ns < 0 && ch->sub_ns < 0) {
            /* GRAMMAR-CONSTRAINT: announce only the ns owned by this
             * child's lane, so announce routing never crosses shards and
             * the pair arithmetic stays a pure function of (pub lane,
             * sub lane). */
            uint8_t ns = ch->lane; /* sched_ns_owner inverse */
            if (m->ns_pub[ns] < 0) {
                EMIT(SCHED_OP_ANNOUNCE, ch->lane, c, ns, 0);
            }
            for (uint32_t ns2 = 0; ns2 < SCHED_NS_COUNT; ns2++) {
                if (m->ns_pub[ns2] >= 0 && m->ns_pub[ns2] != (int8_t)c &&
                    (m->src_child[ns2] < 0 ||
                     m->src_child[ns2] == m->ns_pub[ns2])) {
                    EMIT(SCHED_OP_SUBSCRIBE, ch->lane, c, ns2, 0);
                }
            }
        }
        if (ch->announced_ns >= 0) {
            bool pipe_empty = true;
            uint8_t src = sched_ns_owner((uint8_t)ch->announced_ns);
            for (uint32_t d = 0; d < SCHED_LANES; d++) {
                if (d != src && (m->pair[src][d].occupancy > 0 ||
                                 m->pair[src][d].held_current > 0)) {
                    pipe_empty = false;
                }
            }
            /* GRAMMAR-CONSTRAINT: withdraw/revoke only once the directed
             * channel is drained, so no in-flight entry needs a modelled
             * disposal rule. */
            if (pipe_empty) {
                EMIT(SCHED_OP_WITHDRAW, ch->lane, c, ch->announced_ns, 0);
                EMIT(SCHED_OP_REVOKE, ch->lane, c, ch->announced_ns, 0);
            }
            /* PUSH: only the ESTABLISHED source may push — a fresh
             * announcement alone proves nothing about an upstream
             * SUBSCRIBE having reached this publisher — and at least one
             * accepted subscriber must exist. */
            for (uint32_t s = 0; s < SCHED_MAX_CHILDREN; s++) {
                if (m->src_child[ch->announced_ns] != (int8_t)c) {
                    break;
                }
                if (m->child[s].used && !m->child[s].terminal &&
                    m->child[s].sub_accepted &&
                    m->child[s].sub_ns == ch->announced_ns) {
                    /* heavy weight: only consecutive pushes can fill the
                     * directed channel to its held/credit boundary */
                    for (int w = 0; w < 10; w++) {
                        EMIT(SCHED_OP_PUSH, ch->lane, c, 0, 0);
                    }
                    break;
                }
            }
        }
        if (ch->sub_ns >= 0 && ch->sub_accepted) {
            EMIT(SCHED_OP_CANCEL, ch->lane, c, 0, 0);
        }
        /* GRAMMAR-CONSTRAINT: terminate only an idle child (no announce,
         * no subscription, its lane's pairs empty), so teardown disposal
         * of in-flight channel content never enters the model. */
        if (ch->announced_ns < 0 && ch->sub_ns < 0) {
            bool clean = true;
            /* a STANDING source may terminate (grammar v3): the failover
             * contract retargets its demand to the current winner or
             * terminates the subscribers explicitly */
            for (uint32_t d = 0; d < SCHED_LANES; d++) {
                if (d == ch->lane) continue;
                if (m->pair[ch->lane][d].occupancy > 0 ||
                    m->pair[ch->lane][d].held_current > 0 ||
                    m->pair[d][ch->lane].occupancy > 0 ||
                    m->pair[d][ch->lane].held_current > 0) {
                    clean = false;
                }
            }
            if (clean) {
                EMIT(SCHED_OP_TERMINAL, ch->lane, c, 0, 0);
            }
        }
    }

    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        for (uint32_t d = 0; d < SCHED_LANES; d++) {
            if (s == d) continue;
            if (m->pair[s][d].occupancy > 0) {
                EMIT(SCHED_OP_CONSUME, d, 0, 0, 0);
                EMIT(SCHED_OP_CONSUME, d, 0, 0, 0);
                EMIT(SCHED_OP_CONSUME, d, 0, 0, 0);
                EMIT(SCHED_OP_CONSUME, d, 0, 0, 0);
            }
            if (m->pair[s][d].held_current > 0 &&
                m->pair[s][d].occupancy < m->pair[s][d].capacity) {
                /* heavy weight: the held->retry->credit-wake arm */
                for (int w = 0; w < 8; w++) {
                    EMIT(SCHED_OP_RETRY, s, s, d, 0);
                }
            }
        }
    }

    /* STOP: explored, not appended — eligible at sparse indices from the
     * trace midpoint (one entry against the many above), so its position
     * varies across seeds while most seeds still run their full budget. */
    if (op_index >= SCHED_OPS_PER_SEED / 2u && (op_index % 32u) == 0u) {
        EMIT(SCHED_OP_STOP, 0, 0, 0, 0);
    }
#undef EMIT
    return n;
}


/* Every executed production step on lane L drains the channels directed at
 * L (the push wake guarantees the pump sees the content). Each operation's
 * execution recipe steps a FIXED set of lanes, so the drain is part of the
 * pure transition — generation and execution share it. */
static inline void sched_drain_into(sched_model_t *m, uint8_t lane)
{
    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        if (s == lane) {
            continue;
        }
        while (sx_pair_consume(&m->pair[s][lane])) {
        }
    }
}

/* A settle that steps BOTH lanes repeatedly flushes every held entry into
 * the freed credit and consumes it: full flush-and-drain. */
static inline void sched_drain_all(sched_model_t *m)
{
    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        for (uint32_t d = 0; d < SCHED_LANES; d++) {
            if (s == d) {
                continue;
            }
            sx_pair_t *p = &m->pair[s][d];

            while (p->occupancy > 0 || p->held_current > 0) {
                while (sx_pair_consume(p)) {
                }
                if (!sx_pair_retry(p)) {
                    break;
                }
            }
        }
    }
}

/* -- model transition ----------------------------------------------------- *
 * Applies one op the generator (or replayer) chose. Returns false when the
 * op is not currently enabled — generation never trips this; replay uses it
 * as the precondition validity check.
 */

static inline bool
sched_model_apply(sched_model_t *m, const sched_op_t *op)
{
    if (m->stopped) {
        return false;
    }
    switch (op->op) {
    case SCHED_OP_STEP:
        if (op->lane >= SCHED_LANES) return false;
        m->owed_pump[op->lane] = false; /* a step consumes what is owed */
        sched_drain_into(m, op->lane);
        return true;
    case SCHED_OP_WAKE:
        if (op->lane >= SCHED_LANES) return false;
        m->owed_pump[op->lane] = true;
        return true;
    case SCHED_OP_ACCEPT: {
        if (m->children_ever >= SCHED_MAX_CHILDREN ||
            m->live_children >= SCHED_MAX_LIVE) {
            return false;
        }
        if (op->a != m->children_ever) return false;
        uint8_t lane = (uint8_t)(m->rr_next_lane % SCHED_LANES);
        if (op->lane != lane) return false;
        sched_child_t *ch = &m->child[op->a];
        ch->used = true;
        ch->lane = lane;
        ch->established = false;
        ch->terminal = false;
        ch->reap_expected = false;
        ch->announced_ns = -1;
        ch->sub_ns = -1;
        ch->sub_accepted = false;
        m->children_ever++;
        m->rr_next_lane++;
        m->live_children++;
        m->owed_pump[lane] = true; /* the accept batch arms the pump */
        sched_drain_all(m); /* the reclaim recipe settles both lanes */
        return true;
    }
    case SCHED_OP_SHUTTLE: {
        if (op->a >= SCHED_MAX_CHILDREN) return false;
        const sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal) return false;
        if (op->b > 4u) return false;
        return true; /* byte movement is production-side; no model change */
    }
    case SCHED_OP_ESTABLISH: {
        if (op->a >= SCHED_MAX_CHILDREN) return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal || ch->established) return false;
        ch->established = true;
        m->owed_pump[ch->lane] = true;
        sched_drain_all(m); /* the scripted exchange settles both lanes */
        return true;
    }
    case SCHED_OP_ANNOUNCE: {
        if (op->a >= SCHED_MAX_CHILDREN || op->b >= SCHED_NS_COUNT)
            return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal || !ch->established) return false;
        if (ch->announced_ns >= 0 || ch->sub_ns >= 0) return false;
        if (m->ns_pub[op->b] >= 0) return false;
        if (sched_ns_owner((uint8_t)op->b) != ch->lane) return false;
        ch->announced_ns = (int8_t)op->b;
        m->ns_pub[op->b] = (int8_t)op->a;
        m->owed_pump[ch->lane] = true;
        sched_drain_all(m);
        return true;
    }
    case SCHED_OP_WITHDRAW:
    case SCHED_OP_REVOKE: {
        if (op->a >= SCHED_MAX_CHILDREN || op->b >= SCHED_NS_COUNT)
            return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal) return false;
        if (ch->announced_ns != (int8_t)op->b) return false;
        uint8_t src = sched_ns_owner((uint8_t)op->b);
        for (uint32_t d = 0; d < SCHED_LANES; d++) {
            if (d != src && (m->pair[src][d].occupancy > 0 ||
                             m->pair[src][d].held_current > 0)) {
                return false;
            }
        }
        if (op->op == SCHED_OP_REVOKE &&
            m->src_child[op->b] == (int8_t)op->a) {
            /* revoking the ESTABLISHED source terminates its downstream
             * subscriptions (the reset-flavored seal) and retires the
             * source; a WITHDRAWAL removes only the advertisement — the
             * established track survives it untouched */
            for (uint32_t s2 = 0; s2 < SCHED_MAX_CHILDREN; s2++) {
                if (m->child[s2].used &&
                    m->child[s2].sub_ns == (int8_t)op->b) {
                    m->child[s2].sub_ns = -1;
                    m->child[s2].sub_accepted = false;
                    m->owed_pump[m->child[s2].lane] = true;
                }
            }
            m->src_child[op->b] = -1;
        }
        ch->announced_ns = -1;
        m->ns_pub[op->b] = -1;
        m->owed_pump[ch->lane] = true;
        sched_drain_all(m);
        return true;
    }
    case SCHED_OP_SUBSCRIBE: {
        if (op->a >= SCHED_MAX_CHILDREN || op->b >= SCHED_NS_COUNT)
            return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal || !ch->established) return false;
        if (ch->announced_ns >= 0 || ch->sub_ns >= 0) return false;
        if (m->ns_pub[op->b] < 0 || m->ns_pub[op->b] == (int8_t)op->a)
            return false;
        if (m->src_child[op->b] >= 0 &&
            m->src_child[op->b] != m->ns_pub[op->b]) {
            return false; /* standing source differs from the winner */
        }
        ch->sub_ns = (int8_t)op->b;
        ch->sub_accepted = true; /* the executor's scripted exchange */
        if (m->src_child[op->b] < 0) {
            /* establishment selects the then-current winner as the source */
            m->src_child[op->b] = m->ns_pub[op->b];
        }
        m->owed_pump[ch->lane] = true;
        m->owed_pump[sched_ns_owner((uint8_t)op->b)] = true;
        sched_drain_all(m);
        return true;
    }
    case SCHED_OP_CANCEL: {
        if (op->a >= SCHED_MAX_CHILDREN) return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal) return false;
        if (ch->sub_ns < 0 || !ch->sub_accepted) return false;
        int8_t cns = ch->sub_ns;

        ch->sub_ns = -1;
        ch->sub_accepted = false;
        bool remaining = false;

        for (uint32_t s2 = 0; s2 < SCHED_MAX_CHILDREN; s2++) {
            if (m->child[s2].used && m->child[s2].sub_ns == cns) {
                remaining = true;
            }
        }
        if (!remaining) {
            m->src_child[cns] = -1; /* upstream unsubscribed: source retires */
        }
        m->owed_pump[ch->lane] = true;
        sched_drain_all(m);
        return true;
    }
    case SCHED_OP_PUSH: {
        if (op->a >= SCHED_MAX_CHILDREN) return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal || ch->announced_ns < 0) return false;
        if (m->src_child[ch->announced_ns] != (int8_t)op->a) {
            return false; /* only the ESTABLISHED source may push */
        }
        sched_drain_into(m, ch->lane); /* the recipe steps the owner lane */
        for (uint32_t d2 = 0; d2 < SCHED_LANES; d2++) {
            if (d2 != ch->lane) {
                /* the owner step flushes existing held entries into any
                 * free credit before the new object is offered */
                while (sx_pair_retry(&m->pair[ch->lane][d2])) {
                }
            }
        }
        /* the demand channel carries ONE entry per destination shard —
         * the requester side fans out to its local subscribers */
        bool any = false;
        bool dst[SCHED_LANES] = { false };
        for (uint32_t s = 0; s < SCHED_MAX_CHILDREN; s++) {
            const sched_child_t *sub = &m->child[s];
            if (!sub->used || sub->terminal || !sub->sub_accepted ||
                sub->sub_ns != ch->announced_ns) {
                continue;
            }
            any = true;
            if (sub->lane != ch->lane) {
                dst[sub->lane] = true;
            }
        }
        if (!any) return false;
        for (uint32_t d2 = 0; d2 < SCHED_LANES; d2++) {
            if (dst[d2]) {
                (void)sx_pair_offer(&m->pair[ch->lane][d2]);
                m->owed_pump[d2] = true;
            }
        }
        m->owed_pump[ch->lane] = true;
        return true;
    }
    case SCHED_OP_CONSUME: {
        if (op->lane >= SCHED_LANES) return false;
        bool moved = false;
        for (uint32_t s = 0; s < SCHED_LANES; s++) {
            if (s == op->lane) continue;
            while (sx_pair_consume(&m->pair[s][op->lane])) {
                moved = true;
            }
        }
        if (!moved) return false;
        m->owed_pump[op->lane] = false; /* the consuming step ran */
        return true;
    }
    case SCHED_OP_RETRY: {
        if (op->a >= SCHED_LANES || op->b >= SCHED_LANES || op->a == op->b)
            return false;
        sched_drain_into(m, op->a); /* the recipe steps the src lane */
        sx_pair_t *p = &m->pair[op->a][op->b];
        if (p->held_current == 0 || p->occupancy >= p->capacity)
            return false;
        /* the producer-lane flush retries every held entry the freed
         * credit admits — the production wake contract, not one-at-a-time */
        while (sx_pair_retry(p)) {
        }
        m->owed_pump[op->a] = false; /* executed as a producer-lane step */
        m->owed_pump[op->b] = true;
        return true;
    }
    case SCHED_OP_TERMINAL: {
        if (op->a >= SCHED_MAX_CHILDREN) return false;
        sched_child_t *ch = &m->child[op->a];
        if (!ch->used || ch->terminal) return false;
        if (ch->announced_ns >= 0 || ch->sub_ns >= 0) return false;
        /* the failover contract: a dying STANDING source hands its demand
         * to the current announce winner, or its subscribers are
         * explicitly terminated (in-grammar a single announce exists per
         * namespace, so a retarget needs a NEW winner to have announced
         * after the source withdrew) */
        for (uint32_t ns2 = 0; ns2 < SCHED_NS_COUNT; ns2++) {
            if (m->src_child[ns2] != (int8_t)op->a) {
                continue;
            }
            if (m->ns_pub[ns2] >= 0 &&
                m->ns_pub[ns2] != (int8_t)op->a) {
                m->src_child[ns2] = m->ns_pub[ns2]; /* retarget */
            } else {
                for (uint32_t s2 = 0; s2 < SCHED_MAX_CHILDREN; s2++) {
                    if (m->child[s2].used &&
                        m->child[s2].sub_ns == (int8_t)ns2) {
                        m->child[s2].sub_ns = -1;
                        m->child[s2].sub_accepted = false;
                        m->owed_pump[m->child[s2].lane] = true;
                    }
                }
                m->src_child[ns2] = -1;
            }
        }
        ch->terminal = true;
        ch->reap_expected = true;
        m->live_children--;
        m->owed_pump[ch->lane] = true;
        sched_drain_all(m); /* the recipe settles both lanes */
        return true;
    }
    case SCHED_OP_CLOSE_FEED_FAULT: {
        if (m->children_ever >= SCHED_MAX_CHILDREN ||
            m->live_children >= SCHED_MAX_LIVE) {
            return false;
        }
        if (op->a != m->children_ever) return false;
        uint8_t lane = (uint8_t)(m->rr_next_lane % SCHED_LANES);
        if (op->lane != lane) return false;
        sched_child_t *ch = &m->child[op->a];
        ch->used = true;
        ch->lane = lane;
        ch->established = false;
        ch->terminal = true;    /* admitted, faulted, transport-terminal */
        ch->reap_expected = true;
        ch->announced_ns = -1;
        ch->sub_ns = -1;
        ch->sub_accepted = false;
        m->children_ever++;
        m->rr_next_lane++;
        m->owed_pump[lane] = true;
        sched_drain_all(m); /* the recipe settles both lanes */
        return true;
    }
    case SCHED_OP_STOP:
        m->stopped = true;
        return true;
    default:
        return false;
    }
}

/* Trace-field validity for the strict decoder (vocabulary callback). */
static inline int sched_field_ok(const seedx_rec_t *r)
{
    if (r->op == 0 || r->op > SCHED_OP_MAX) return 0;
    if (r->lane >= SCHED_LANES) return 0;
    if (r->a >= 256u || r->b >= 256u || r->c != 0) return 0;
    return 1;
}

#endif /* MOQR_SCHED_MODEL_H */
