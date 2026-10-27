#ifndef SIMPAIR_INTERNAL_H
#define SIMPAIR_INTERNAL_H

#include "moq/sim.h"
#include "moq/wire.h"
#include "../../core/src/spi/core_spi.h"

#include <stddef.h>
#include <string.h>

#define MOQ_SIM_MAX_DATA_STREAMS 16
#define MOQ_SIM_ACTION_BATCH 16
/* Bidi slots are reclaimed only when both halves close (or on RESET/STOP), which
 * models real QUIC: a half-closed request bidi (one side FIN'd, the peer's half
 * still open) legitimately persists until both ends close. Real QUIC has a vast
 * stream-ID space; this pool is sized generously so realistic half-open
 * accumulation in interleaved scenarios does not artificially exhaust the harness
 * (clean terminal exchanges still retire their slot promptly). */
#define MOQ_SIM_MAX_BIDI_STREAMS 64

struct sim_delay_entry;

/* What a receiving session's WOULD_BLOCK left owed on one ordered input
 * (a data stream, one half of a request bidi, or the control channel). */
typedef enum sim_hold {
    SIM_HOLD_NONE = 0,
    /* The session retained the refused bytes (and FIN, hold_fin): a stream
     * resumes with an empty re-feed, the control channel with
     * moq_session_process_pending, once the session's queue has room. */
    SIM_HOLD_RETAINED,
    /* The session did not admit the data stream and retained nothing: the
     * refused chunk heads the held queue and is delivered again as it was. */
    SIM_HOLD_REDELIVER,
    /* The session recorded the RESET and owes its closure: the same reset is
     * delivered again with hold_code. */
    SIM_HOLD_RESET,
} sim_hold_t;

/* Receive-side backpressure for one ordered input. While a refusal is owed
 * (hold), every later chunk waits in head..tail in order, as a transport
 * pauses a stream's reads: nothing written is dropped, and the step loop
 * resumes the input once the session accepts again. A STOP_SENDING the
 * session refused is owed separately (stop_owed); it is not ordered with
 * the bytes. */
typedef struct sim_inbound {
    sim_hold_t              hold;
    bool                    hold_fin;
    bool                    stop_owed;
    uint64_t                hold_code;
    uint64_t                stop_code;
    struct sim_delay_entry *head;
    struct sim_delay_entry *tail;
} sim_inbound_t;

typedef struct moq_sim_stream_map {
    uint64_t            sender_ref;
    uint64_t            receiver_ref;
    moq_perspective_t   sender;
    bool                active;
    bool                terminal_pending;
    uint64_t            generation;
    uint64_t            last_due_us;
    /* The receiver's backpressure on this stream; stop_owed is a STOP
     * refused by the sender's session. */
    sim_inbound_t       in;
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
    /* Backpressure per receiving session, indexed by sim_side(). */
    sim_inbound_t       in[2];
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
    bool             trace_hash_enabled;
    uint64_t         trace_hash;
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
    /* Control-channel backpressure per receiving session (sim_side()), and
     * whether that session's peer drives control over a unidirectional
     * stream pair -- where, as in the transport bridge, a blocked control
     * channel also pauses the session's other unidirectional inputs. */
    sim_inbound_t     ctrl_in[2];
    bool              ctrl_uni_pair[2];
    /* At most one accepted FIN per action in the current source batch.
     * Delay source-advancing commitment feedback until all its borrows die. */
    moq_session_t    *borrowed_sender;
    moq_stream_ref_t committed[MOQ_SIM_ACTION_BATCH];
    size_t           committed_count;
    bool             committed_overflow;
    moq_version_t     version;   /* both sessions; 0 = session default */
};

moq_result_t sim_finish_batch(moq_simpair_t *sp, bool abort_batch);

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

/*
 * The appended per-input detail (trace record v1). Every trace_input() call
 * site supplies one EXPLICITLY -- kinds that carry no stream pass
 * SIM_INPUT_DETAIL_NONE rather than relying on a default, so a new call site
 * cannot inherit zeros by omission.
 *
 * ref is the ref the delivery itself used, i.e. the ref valid at the SESSION
 * being fed after SimPair's mapping -- never the far-end ref.
 */
typedef struct sim_input_detail {
    moq_stream_ref_t ref;
    uint64_t         error_code;
    bool             fin;
} sim_input_detail_t;

#define SIM_INPUT_DETAIL_NONE ((sim_input_detail_t){ { 0 }, 0, false })

void trace_input(moq_simpair_t *sp,
                 moq_sim_trace_input_kind_t input_kind,
                 moq_perspective_t from,
                 moq_perspective_t to,
                 moq_bytes_t bytes,
                 moq_result_t result,
                 sim_input_detail_t detail);

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

/* to_server (nullable): additionally counts the matured inputs whose target
 * was the SERVER session -- the directional receive signal event-paced
 * fixtures key their doorbell model on. */
moq_result_t sim_delay_deliver_matured(moq_simpair_t *sp,
                                       size_t *delivered,
                                       size_t *to_server);

/* -- simpair_hold.c ------------------------------------------------- *
 * Receive-side backpressure: every session input that returns
 * MOQ_ERR_WOULD_BLOCK is held, never dropped, and resumed by
 * sim_resume_held at the start of each step. Each *_input function feeds
 * the session now or queues behind an owed refusal; a WOULD_BLOCK is
 * recorded and MOQ_OK returned. Any other negative result is returned. */

static inline int sim_side(moq_perspective_t p)
{
    return p == MOQ_PERSPECTIVE_CLIENT ? 0 : 1;
}

moq_result_t sim_control_input(
    moq_simpair_t *sp, moq_session_t *to, const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t sim_data_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int dslot, moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t sim_data_reset_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int dslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t sim_data_stop_input(
    moq_simpair_t *sp, moq_session_t *sender_session,
    moq_stream_ref_t sender_ref, uint64_t error_code,
    int dslot, moq_perspective_t from, moq_perspective_t sender_persp);

moq_result_t sim_bidi_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    const uint8_t *data, size_t len, bool fin,
    int bslot, moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t sim_bidi_reset_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp);

moq_result_t sim_bidi_stop_input(
    moq_simpair_t *sp, moq_session_t *to, moq_stream_ref_t ref,
    uint64_t error_code, int bslot,
    moq_perspective_t from, moq_perspective_t to_persp);

/* A datagram the session refuses is dropped, as the bridge drops it. */
moq_result_t sim_datagram_input(
    moq_simpair_t *sp, moq_session_t *to, const uint8_t *data, size_t len,
    moq_perspective_t from, moq_perspective_t to_persp);

/* Resume every held input: the control channels first, then each stream
 * (owed refusal, then held chunks in order, then an owed STOP). Counts each
 * input that progressed toward its receiving direction. */
moq_result_t sim_resume_held(moq_simpair_t *sp,
                             size_t *to_server, size_t *to_client);

/* Record a FIN on the sender's half of a bidi; retire once both are closed. */
void sim_bidi_mark_fin(moq_simpair_t *sp, int bslot, moq_perspective_t from);

/* Retire a data-stream slot: releases its held inputs and owed refusals and
 * bumps its generation, so delayed inputs for it become stale. */
void sim_stream_slot_retire(moq_simpair_t *sp, int slot);

/* Mark a slot inactive and release its held inputs, keeping its generation
 * (delayed inputs for it stay deliverable). */
void sim_stream_slot_deactivate(moq_simpair_t *sp, int slot);

/* Retire a bidi slot, releasing both halves' held inputs. */
void sim_bidi_slot_retire(moq_simpair_t *sp, int bslot);

/* Release every held input (pair teardown). */
void sim_held_clear(moq_simpair_t *sp);

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

/* delivered: work handled toward `to`. delivered_reverse (nullable): work
 * this pump handles toward `from` -- the fault-injected STOP is delivered
 * back into from_session, so it is counted there. */
moq_result_t pump_direction(moq_simpair_t *sp,
                            moq_session_t *from_session,
                            moq_session_t *to_session,
                            moq_perspective_t from,
                            moq_perspective_t to,
                            size_t *delivered,
                            size_t *delivered_reverse);

#endif /* SIMPAIR_INTERNAL_H */
