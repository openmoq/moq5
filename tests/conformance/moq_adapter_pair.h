#ifndef MOQ_ADAPTER_PAIR_H
#define MOQ_ADAPTER_PAIR_H

/*
 * Adapter conformance pair interface.
 *
 * A transport-agnostic vtable that any MoQ adapter can implement
 * to run the shared conformance test suite. Each adapter provides
 * its own deterministic pair binding (e.g. mvfst BridgePair,
 * picoquic fake pair). Scenarios are written in C against this
 * interface and run identically across all adapters.
 */

#include <moq/session.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Capability flags — adapters declare what the pair supports.
 * Scenarios check capabilities before using optional features. */
enum {
    MOQ_ADAPTER_PAIR_CAP_BLOCK_WRITES      = 1 << 0,
    MOQ_ADAPTER_PAIR_CAP_DROP_DATAGRAMS    = 1 << 1,
    MOQ_ADAPTER_PAIR_CAP_STREAM_COUNTS     = 1 << 2,
    MOQ_ADAPTER_PAIR_CAP_TOMBSTONES        = 1 << 3,
    MOQ_ADAPTER_PAIR_CAP_REAL_QUIC_IDS     = 1 << 4,
    MOQ_ADAPTER_PAIR_CAP_DATAGRAMS         = 1 << 5,
    MOQ_ADAPTER_PAIR_CAP_BIDI_STREAMS      = 1 << 6,
    MOQ_ADAPTER_PAIR_CAP_VIRTUAL_TIME      = 1 << 7,
    MOQ_ADAPTER_PAIR_CAP_OPENED_BIDI_COUNT = 1 << 8,
    MOQ_ADAPTER_PAIR_CAP_INJECT_BIDI_FIN   = 1 << 9,
};

typedef enum moq_adapter_pair_side {
    MOQ_ADAPTER_PAIR_CLIENT = 0,
    MOQ_ADAPTER_PAIR_SERVER = 1,
} moq_adapter_pair_side_t;

typedef enum moq_adapter_pair_pump_result {
    MOQ_ADAPTER_PAIR_QUIESCENT  =  0,
    MOQ_ADAPTER_PAIR_PROGRESS   =  1,
    MOQ_ADAPTER_PAIR_MAX_STEPS  =  2,
    MOQ_ADAPTER_PAIR_ERROR      = -1,
} moq_adapter_pair_pump_result_t;

typedef struct moq_adapter_pair_ops {
    /* Session access */
    moq_session_t *(*client_session)(void *ctx);
    moq_session_t *(*server_session)(void *ctx);

    /* Time control */
    uint64_t (*now_us)(void *ctx);
    void     (*advance_to)(void *ctx, uint64_t now_us);
    uint64_t (*next_deadline_us)(void *ctx);

    /* Pump transport — uses current now_us() only; does NOT
     * auto-advance time to deadlines. Timer scenarios must
     * explicitly advance_to(deadline) then pump. */
    moq_adapter_pair_pump_result_t (*pump_once)(void *ctx,
                                                 uint64_t now_us);
    moq_adapter_pair_pump_result_t (*pump_until_quiescent)(void *ctx,
                                                            int max_steps);

    /* Diagnostics — side-aware closed/fatal distinction */
    bool       (*has_error)(void *ctx);
    bool       (*is_closed)(void *ctx, moq_adapter_pair_side_t side);
    uint64_t   (*close_code)(void *ctx, moq_adapter_pair_side_t side);
    bool       (*has_fatal)(void *ctx, moq_adapter_pair_side_t side);
    uint64_t   (*fatal_code)(void *ctx, moq_adapter_pair_side_t side);
    const char *(*last_error)(void *ctx);

    /* Fault injection + counters — side-aware (check capabilities) */
    void   (*block_writes)(void *ctx, moq_adapter_pair_side_t side,
                            bool block);
    void   (*drop_datagrams)(void *ctx, moq_adapter_pair_side_t side,
                              bool drop);
    size_t (*stream_count)(void *ctx, moq_adapter_pair_side_t side);
    size_t (*tombstone_count)(void *ctx, moq_adapter_pair_side_t side);
    size_t (*opened_bidi_count)(void *ctx, moq_adapter_pair_side_t side);

    /* Bidi FIN injection — inject an empty FIN on the last opened
     * namespace bidi stream from the given side. Used by half-close
     * scenarios to force peer FIN before server response. */
    int (*inject_bidi_fin)(void *ctx, moq_adapter_pair_side_t from_side);

    /* Cleanup */
    void (*destroy)(void *ctx);
} moq_adapter_pair_ops_t;

typedef struct moq_adapter_pair {
    const moq_adapter_pair_ops_t *ops;
    uint32_t capabilities;
    void *ctx;
} moq_adapter_pair_t;

/* Convenience helpers */

static inline moq_session_t *
moq_pair_client(moq_adapter_pair_t *p) {
    return p->ops->client_session(p->ctx);
}

static inline moq_session_t *
moq_pair_server(moq_adapter_pair_t *p) {
    return p->ops->server_session(p->ctx);
}

static inline moq_adapter_pair_pump_result_t
moq_pair_pump(moq_adapter_pair_t *p) {
    return p->ops->pump_until_quiescent(p->ctx, 100);
}

static inline void
moq_pair_destroy(moq_adapter_pair_t *p) {
    if (p->ops->destroy) p->ops->destroy(p->ctx);
}

static inline bool
moq_pair_has_cap(const moq_adapter_pair_t *p, uint32_t cap) {
    return (p->capabilities & cap) != 0;
}

#ifdef __cplusplus
}
#endif

#endif /* MOQ_ADAPTER_PAIR_H */
