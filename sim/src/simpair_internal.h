#ifndef SIMPAIR_INTERNAL_H
#define SIMPAIR_INTERNAL_H

#include "moq/sim.h"
#include "moq/wire.h"

#include <stddef.h>
#include <string.h>

#define MOQ_SIM_MAX_DATA_STREAMS 16
/* Bidi slots are reclaimed only when both halves close (or on RESET/STOP), which
 * models real QUIC: a half-closed request bidi (one side FIN'd, the peer's half
 * still open) legitimately persists until both ends close. Real QUIC has a vast
 * stream-ID space; this pool is sized generously so realistic half-open
 * accumulation in interleaved scenarios does not artificially exhaust the harness
 * (clean terminal exchanges still retire their slot promptly). */
#define MOQ_SIM_MAX_BIDI_STREAMS 64

typedef struct moq_sim_stream_map {
    uint64_t            sender_ref;
    uint64_t            receiver_ref;
    moq_perspective_t   sender;
    bool                active;
    bool                terminal_pending;
    uint64_t            generation;
    uint64_t            last_due_us;
} moq_sim_stream_map_t;

typedef struct moq_sim_bidi_map {
    uint64_t            opener_ref;
    uint64_t            responder_ref;
    moq_perspective_t   opener;
    bool                active;
    /* Per-direction FIN tracking: a bidi is a pair of independent send halves. A
     * FIN closes only the sender's half; the slot retires once BOTH halves are
     * closed (or a RESET/STOP tears it down at once). A responder FIN (e.g. a
     * REQUEST_ERROR closing the response half) thus leaves the slot live so the
     * opener can still close its half afterwards, matching real QUIC. */
    bool                opener_fin;
    bool                responder_fin;
    uint64_t            generation;
    uint64_t            last_due_us;
} moq_sim_bidi_map_t;

typedef enum sim_delay_kind {
    SIM_DELAY_CONTROL_BYTES,
    SIM_DELAY_BIDI_BYTES,
    SIM_DELAY_BIDI_RESET,
    SIM_DELAY_DATA_BYTES,
    SIM_DELAY_DATA_RESET,
    SIM_DELAY_DATA_STOP,
    SIM_DELAY_BIDI_STOP,
} sim_delay_kind_t;

typedef struct sim_delay_entry {
    struct sim_delay_entry *next;
    uint64_t          due_us;
    uint64_t          seq;
    sim_delay_kind_t  kind;
    moq_perspective_t from;
    moq_perspective_t to;
    moq_stream_ref_t  ref;
    uint64_t          error_code;
    bool              fin;
    int               bidi_slot;
    uint64_t          bidi_generation;
    int               data_slot;
    uint64_t          data_generation;
    size_t            alloc_size;
    size_t            len;
    uint8_t           bytes[];
} sim_delay_entry_t;

struct moq_simpair {
    moq_alloc_t      alloc;
    moq_session_t   *client;
    moq_session_t   *server;
    uint64_t         seed;
    uint64_t         now_us;
    uint64_t         step;
    moq_sim_trace_fn trace_fn;
    void            *trace_ctx;
    moq_sim_stream_map_t stream_map[MOQ_SIM_MAX_DATA_STREAMS];
    moq_sim_bidi_map_t   bidi_map[MOQ_SIM_MAX_BIDI_STREAMS];
    uint64_t         next_rx_ref;
    uint64_t         next_bidi_ref;
    uint32_t         fault_per_mille;
    uint32_t         fault_flags;
    bool             fault_enabled;
    sim_delay_entry_t *delay_head;
    uint64_t          delay_seq;
    /* Separate from delay_seq: increments on every delay decision
     * (fire or not), not just successful enqueues. Split actions
     * produce multiple chunks with independent delay decisions,
     * so each chunk needs a unique deterministic decision index. */
    uint64_t          delay_decision_seq;
    uint64_t          last_control_due_us[2];
    moq_version_t     version;   /* both sessions; 0 = session default */
};

/* -- simpair_fault.c ------------------------------------------------- */

uint64_t sim_mix64(uint64_t z);

bool sim_fault_fires(const moq_simpair_t *sp,
                     size_t action_index,
                     moq_action_kind_t action_kind);

bool sim_split_fires(const moq_simpair_t *sp,
                     size_t action_index);

bool sim_split_control_fires(const moq_simpair_t *sp,
                             size_t action_index);

bool sim_split_bidi_fires(const moq_simpair_t *sp,
                          size_t action_index);

bool sim_delay_fires(const moq_simpair_t *sp,
                     size_t action_index);

uint64_t sim_delay_compute_due(const moq_simpair_t *sp,
                               size_t action_index);

bool sim_mutate_fires(const moq_simpair_t *sp,
                      size_t action_index);

bool sim_mutate_data_fires(const moq_simpair_t *sp,
                           size_t action_index);

bool sim_truncate_control_fires(const moq_simpair_t *sp,
                                size_t action_index);

bool sim_truncate_data_fires(const moq_simpair_t *sp,
                             size_t action_index);

bool sim_reorder_eligible(moq_action_kind_t kind);

bool sim_reorder_fires(const moq_simpair_t *sp,
                       size_t batch_index,
                       moq_action_kind_t kind_i,
                       moq_action_kind_t kind_j);

void trace_record(moq_simpair_t *sp,
                  const moq_sim_trace_record_t *record);

void trace_input(moq_simpair_t *sp,
                 moq_sim_trace_input_kind_t input_kind,
                 moq_perspective_t from,
                 moq_perspective_t to,
                 moq_bytes_t bytes,
                 moq_result_t result);

void trace_action(moq_simpair_t *sp,
                  moq_perspective_t from,
                  moq_perspective_t to,
                  const moq_action_t *action);

void trace_quiescent(moq_simpair_t *sp, size_t count);

void trace_fault_drop(moq_simpair_t *sp,
                      moq_perspective_t from,
                      moq_perspective_t to,
                      const moq_action_t *action);

void trace_fault_mutate(moq_simpair_t *sp,
                        moq_perspective_t from,
                        moq_perspective_t to,
                        moq_action_kind_t action_kind,
                        const uint8_t *mutated_data,
                        size_t mutated_len,
                        size_t byte_idx,
                        unsigned bit_idx);

void trace_fault_reorder(moq_simpair_t *sp,
                         moq_perspective_t from,
                         moq_perspective_t to,
                         moq_action_kind_t displaced_kind,
                         size_t orig_i, size_t orig_j);

void trace_fault_truncate(moq_simpair_t *sp,
                          moq_perspective_t from,
                          moq_perspective_t to,
                          moq_action_kind_t action_kind,
                          size_t prefix_len,
                          size_t original_len);

/* -- simpair_delay.c ------------------------------------------------- */

sim_delay_entry_t *sim_delay_alloc(moq_simpair_t *sp,
                                   sim_delay_kind_t kind,
                                   const uint8_t *data, size_t len,
                                   uint64_t due_us);

void sim_delay_enqueue(moq_simpair_t *sp, sim_delay_entry_t *e);

void sim_delay_free_entry(moq_simpair_t *sp, sim_delay_entry_t *e);

void sim_delay_clear(moq_simpair_t *sp);

void trace_delay_enqueue(moq_simpair_t *sp,
                         moq_perspective_t from,
                         moq_perspective_t to,
                         sim_delay_kind_t kind,
                         size_t len, uint64_t code,
                         uint64_t due_us, uint64_t seq,
                         bool fifo_forced);

void trace_delay_stale(moq_simpair_t *sp,
                       const sim_delay_entry_t *e);

moq_result_t deliver_or_delay_control_chunk(
    moq_simpair_t *sp, moq_session_t *to,
    const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_control(
    moq_simpair_t *sp, moq_session_t *to,
    const uint8_t *data, size_t len, size_t action_index,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_bidi_chunk(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int bslot, moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_bidi(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    size_t action_index, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_bidi_reset(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_bidi_stop(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_data_chunk(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int dslot, moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_data(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *hdr, size_t hlen,
    const uint8_t *payload, size_t plen,
    bool fin, size_t action_index, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_data_reset(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t deliver_or_delay_data_stop(
    moq_simpair_t *sp, moq_session_t *sender_session,
    moq_stream_ref_t sender_ref, uint64_t error_code,
    int dslot, moq_perspective_t from, moq_perspective_t sender_persp);

moq_result_t sim_delay_deliver_matured(moq_simpair_t *sp,
                                       size_t *delivered);

/* -- simpair_pump.c -------------------------------------------------- */

int sim_stream_map_find(moq_simpair_t *sp, uint64_t sender_ref,
                        moq_perspective_t sender);

moq_result_t sim_stream_map_get_or_create(moq_simpair_t *sp,
                                          uint64_t sender_ref,
                                          moq_perspective_t sender,
                                          uint64_t *out_rx_ref,
                                          int *out_slot,
                                          bool *out_created);

void sim_stream_map_remove(moq_simpair_t *sp, uint64_t sender_ref,
                           moq_perspective_t sender);

int sim_bidi_find_by_opener(moq_simpair_t *sp, uint64_t opener_ref,
                            moq_perspective_t opener);

int sim_bidi_find_by_responder(moq_simpair_t *sp, uint64_t responder_ref,
                               moq_perspective_t opener);

moq_result_t pump_direction(moq_simpair_t *sp,
                            moq_session_t *from_session,
                            moq_session_t *to_session,
                            moq_perspective_t from,
                            moq_perspective_t to,
                            size_t *delivered);

#endif /* SIMPAIR_INTERNAL_H */
