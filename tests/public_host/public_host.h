#ifndef MOQ_TEST_PUBLIC_HOST_H
#define MOQ_TEST_PUBLIC_HOST_H

/*
 * A transport host built on the public headers alone.
 *
 * Two sessions are joined by an in-memory transport the test controls. Each
 * side drives its session with nothing but the public inputs, polls and
 * transport host operations: it routes peer unidirectional streams with
 * moq_session_route_peer_uni(), keeps refused input for redelivery, holds
 * output the transport cannot take yet, and reports transport retirement and
 * the end of a control stream with the calls made for them.
 *
 * By default output is written in the order it was polled, across all
 * streams: nothing polled later reaches the transport before everything
 * polled earlier has. That order keeps a stream's bytes, FIN and reset in
 * sequence, and keeps a terminal control message behind the stream ends
 * polled before it. It also means one blocked stream holds back all later
 * output. A side with per_stream_order keeps each stream's own order
 * instead: a held record holds back only the later output of its stream,
 * whose bytes, FIN and reset stay in sequence, while other streams' output
 * may overtake it; output on no stream (the transport close) still waits
 * for everything polled before it. In both orders three things stand apart.
 * Datagrams are held on their own: a refused one holds back no stream. In
 * the tagged mode a data stream's reset (a claimed one, a polled one, or one
 * this side owes after a NO_RESET outcome) is written once that stream's
 * earlier bytes are. And an owner's final message (tag final_for) is handed
 * to the transport only after every data stream of that owner has its FIN
 * or reset accepted, no reset claim of it is outstanding, and no datagram of
 * it is held; in per-stream order that fence alone keeps it back.
 *
 * In the tagged mode a FETCH response stream tagged RESET_FINAL_ON_STOP
 * keeps that policy on its stream record until its FIN or reset is
 * accepted, past the FETCH's native end. A STOP of it observed before then
 * ends the request with a reset. The owner's final message, held already or
 * polled later, is never written, FIN included: it becomes the request
 * stream's reset with CANCELLED, once. That does not happen when the STOP's
 * outcome is REQUEST_ENDED, where the session resets the request itself. A
 * reset the session queued for the request stands as it is.
 *
 * In the tagged mode a reset of a data stream the peer stopped is claimed
 * WAIVED once the session took that STOP, whatever the reset's first cause,
 * and delivers only the bytes the transport took: nothing held is written
 * after the STOP. While this side has seen a STOP the session has not taken
 * yet, the claim waits. A refused WAIVED claim is a fault.
 *
 * A STOP of a request stream resets this side's half once: with the
 * session's own reset or abort of the stream when one is polled or held,
 * otherwise with the STOP's code once a poll returns no action. A reset or
 * abort the session polls for a request stream makes what is still held
 * for it obsolete: none of it is written, and the reset does not wait for
 * it. An ordinary FIN still follows its bytes.
 *
 * In the tagged mode each held record keeps its tag deadline. Before any
 * byte of a subgroup is written or retried the deadline is checked, and no
 * byte past it is written. Held subgroup bytes past it are reported with
 * moq_session_on_held_data_expired() when nothing has decided the stream's
 * end yet; when a reset already owns that end, they are dropped past the
 * prefix that reset still delivers, with no second report or reset. A
 * datagram past its deadline is dropped.
 *
 * The host is deterministic: no threads, no clock, no sockets. Time is the
 * pair's counter. A side is driven in wakes: each wake has one time, runs
 * drain passes at it, and ticks the session at most once (only when its
 * deadline has come). Progress is a transition: bytes or a datagram the
 * transport took, an action or input taken, a report, a claim, a commit, an
 * accepted reset, a dropped expired record. A refused write or reset, a
 * WOULD_BLOCK, and a tick that returned WOULD_BLOCK or left the deadline
 * where it was are not. A tick that returned WOULD_BLOCK is not repeated
 * at the same time while the session's deadline stays where that tick left
 * it; a later wake whose deadline has come ticks again, once, so a hard
 * timeout (idle, GOAWAY) behind blocked terminal work is still serviced
 * without any progress of the application.
 */

#include <moq/moq.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Transport streams per pair over its whole lifetime: the double never
 * reuses a stream slot, so this bounds how many streams a scenario may open
 * in all, not how many may be open at once. */
#define PH_MAX_STREAMS  32
#define PH_PIPE_CAP     4096    /* bytes in flight per direction */
#define PH_HOLD_CAP     2048    /* undelivered input kept per stream */
#define PH_OUT_SLOTS    32      /* output records held per side */
#define PH_MAX_REFUSED  4       /* refused peer streams kept per side */
#define PH_DGRAM_SLOTS  8
#define PH_DGRAM_CAP    1500

/* Why a side stopped driving its session. A side that ended its session
 * over the fault still closes the transport, so its peer learns. */
#define PH_FAULT_NONE         0
#define PH_FAULT_UNSUPPORTED  1  /* an action kind or route this host does not
                                  * know, or a record stamped too short */
#define PH_FAULT_POLL         2  /* an action poll failed */
#define PH_FAULT_NOMEM        3  /* an output could not be copied */
#define PH_FAULT_STREAMS      4  /* the transport double ran out of streams */
#define PH_FAULT_INPUT        5  /* an input failed in a way this host ends on */
#define PH_FAULT_ORDER        6  /* output for a stream this side does not know */
#define PH_FAULT_OUTCOME      7  /* a session call returned a result its contract
                                  * does not list for that input */

/* What the transport refuses from a side (ph_set_write_refusal()). */
#define PH_REFUSE_STREAM_BYTES  0x1u   /* every stream byte; a FIN with no byte
                                        * and resets are still taken */
#define PH_REFUSE_DATAGRAMS     0x2u

/* How a stream injected with ph_peer_opens_uni() ends. */
#define PH_END_NONE   0
#define PH_END_FIN    1
#define PH_END_RESET  2

typedef struct ph_pair ph_pair_t;

typedef struct ph_options {
    uint32_t max_events[2];     /* 0 keeps the session's default */
    uint32_t max_actions[2];
    uint32_t max_data_streams[2];
    size_t   write_limit;       /* most bytes one write takes; 0 = no limit */
    size_t   read_limit;        /* most bytes one read returns; 0 = no limit */
    /* A receive side this endpoint stopped is dropped by the transport: the
     * peer's bytes, FIN and RESET never arrive, and the transport reports
     * the stream finished instead. */
    bool     drop_after_stop;
    /* Sides that run the tagged host mode: enabled before start; actions
     * come from the tagged poll; counted opens are reported once their
     * first bytes are written; resets go through the reset transaction. */
    bool     tagged[2];
    /* The sessions' own idle and GOAWAY timeouts (moq_session_cfg_t);
     * 0 keeps each disabled, as by default. */
    uint64_t idle_timeout_us[2];
    uint64_t goaway_timeout_us[2];
    /* Sides that keep each stream's output order alone (see above) instead
     * of the order output was polled in across all streams. */
    bool     per_stream_order[2];
    /* The sessions' subgroup pools (moq_session_cfg_t.max_open_subgroups);
     * 0 keeps the default. */
    uint32_t max_open_subgroups[2];
} ph_options_t;

/* What a side did, for tests to assert on. */
typedef struct ph_counters {
    unsigned data_retired;       /* moq_session_on_data_stream_retired == OK */
    unsigned bidi_retired;       /* moq_session_on_bidi_stream_retired == OK */
    unsigned retire_refused;     /* either returned MOQ_ERR_WRONG_STATE */
    unsigned retire_stale;       /* either returned MOQ_ERR_STALE_HANDLE */
    unsigned request_stops;      /* request STOPs the session took (MOQ_OK) */
    unsigned ended_unrouted;     /* peer streams that ended before their type
                                  * was complete, without a session input */
    unsigned control_ended;      /* moq_session_on_control_stream_ended calls */
    unsigned need_more;          /* routes answered MOQ_UNI_ROUTE_NEED_MORE */
    unsigned routed_control, routed_data, routed_padding;
    unsigned refused_inputs;     /* deliveries the session did not admit */
    unsigned redelivered;        /* refused deliveries offered again and taken */
    unsigned owned_retries;      /* empty retries of a retained delivery */
    unsigned partial_writes;     /* writes the transport took only part of */
    unsigned poll_waits;         /* action polls that waited for events */
    unsigned peer_fins, peer_resets, peer_stops;  /* delivered to the session */
    unsigned stop_entries;       /* STOPs that reached this host, repeats included */
    unsigned stop_answers;       /* request STOPs this side answered with their
                                  * own code (the session reset nothing) */
    unsigned datagrams_dropped;
    unsigned violations;         /* closes this host asked for */
    /* Tagged mode only. */
    unsigned opens_reported;     /* moq_session_report_data_open == OK */
    unsigned fins_reported;      /* moq_session_report_data_fin == OK */
    unsigned claims;             /* moq_session_claim_data_reset == OK */
    unsigned claims_settled;     /* ... of which UNOPENED, settled */
    unsigned claim_waits;        /* offer or claim WOULD_BLOCK */
    unsigned claim_stale;        /* claim STALE_HANDLE */
    unsigned commits;            /* moq_session_commit_data_reset == OK */
    unsigned resets_refused;     /* claimed or owed resets the transport refused */
    unsigned host_resets;        /* NO_RESET answers this host reset itself */
    unsigned held_expired;       /* moq_session_on_held_data_expired == OK */
    unsigned held_trimmed;       /* expired held bytes dropped past the prefix of
                                  * a reset that already owned the stream's end */
    unsigned final_waits;        /* passes an owner's final message waited */
    unsigned final_policies;     /* data streams opened RESET_FINAL_ON_STOP */
    unsigned finals_cancelled;   /* request final messages a STOP of their
                                  * response stream made CANCELLED resets */
    /* Both modes. */
    unsigned ticks;              /* moq_session_tick calls */
    unsigned ticks_blocked;      /* ... that returned MOQ_ERR_WOULD_BLOCK */
    unsigned writes_refused;     /* stream writes and datagrams the transport
                                  * took nothing of */
    unsigned datagrams_sent;     /* datagrams the transport took */
    unsigned datagrams_expired;  /* held datagrams dropped past their deadline */
    unsigned resets_unknown;     /* data resets polled for a stream this side
                                  * never opened (nothing to end) */
} ph_counters_t;

/* Both sessions created, bound where the profile needs it, and started.
 * Side 0 is the client. NULL when a session cannot be created or started. */
ph_pair_t *ph_pair_create(moq_version_t version, const ph_options_t *options);

/* Destroys both sessions and the pair. Returns the number of blocks the
 * sessions' allocator and the host's output copies still held: 0 when both
 * the native and the host state were cleaned up. */
size_t ph_pair_destroy(ph_pair_t *pair);

moq_session_t *ph_session(ph_pair_t *pair, int side);
const ph_counters_t *ph_counters(const ph_pair_t *pair, int side);
int ph_fault(const ph_pair_t *pair, int side);
uint64_t ph_now(const ph_pair_t *pair);

/* Drive both sides until a whole pass moves nothing, or max_passes passes
 * have run. Each pass is a wake of its own, one microsecond after the last.
 * Returns true when it stopped because nothing moved. A pass that only
 * retried something still blocked does not count as movement.
 *
 * True means quiescence, not completion: work can still wait on the
 * application (a full event queue it has not drained), on a deadline, or
 * on a retry that keeps being refused. A test asserts what completed. */
bool ph_run(ph_pair_t *pair, unsigned max_passes);

/* One wake of both sides at virtual time at_us (or at the current time, if
 * that is later): drain passes at that one time until a pass moves nothing,
 * at most max_passes, with at most one tick per side in the whole wake.
 * Returns true when it stopped because nothing moved. */
bool ph_wake(ph_pair_t *pair, uint64_t at_us, unsigned max_passes);

/* Output records a side still holds for the transport, datagrams included. */
size_t ph_held_outputs(const ph_pair_t *pair, int side);

/* The earliest tag deadline of the bytes and datagrams a side holds and has
 * not written: the time its host must wake for them. UINT64_MAX for none. */
uint64_t ph_next_held_deadline(const ph_pair_t *pair, int side);

/* The transport refuses what `what` names (PH_REFUSE_* bits) from `side`
 * until it is changed; 0 refuses nothing. A refused write is retried on a
 * later pass and is not progress. */
void ph_set_write_refusal(ph_pair_t *pair, int side, unsigned what);

/* True when the side closed the transport, with the code it used. */
bool ph_transport_closed(const ph_pair_t *pair, int side, uint64_t *out_code);

/* Change the transport's limits between runs. */
void ph_set_write_limit(ph_pair_t *pair, size_t limit);
void ph_set_read_limit(ph_pair_t *pair, size_t limit);

/* While held, the transport delivers the bytes `side` writes but keeps
 * back every FIN it writes. Releasing delivers the FINs it kept, except on
 * a direction its reader has since stopped and dropped. */
void ph_set_fin_hold(ph_pair_t *pair, int side, bool held);

/* The profile the pair follows, from the version its sessions speak. */
bool ph_bidi_control(const ph_pair_t *pair);
bool ph_uses_request_streams(const ph_pair_t *pair);

/* The counting allocator both sessions use. Buffers a test creates with it
 * (object payloads, for instance) are counted by ph_pair_destroy() too. */
const moq_alloc_t *ph_allocator(const ph_pair_t *pair);

/* The stream ref the given side uses for the only peer data stream it has
 * not finished with. Returns false when there is none or more than one. */
bool ph_only_open_peer_data_stream(const ph_pair_t *pair, int side,
                                   moq_stream_ref_t *out_ref);

/* The stream ref of the only request stream the side opened and has not
 * finished with. Returns false when there is none or more than one. */
bool ph_only_open_local_request(const ph_pair_t *pair, int side,
                                moq_stream_ref_t *out_ref);

/* Transport events a peer could cause, injected by the test: the side's
 * peer finishes, or resets, the control stream it sends on. Returns false
 * when that stream does not exist yet. */
bool ph_peer_finishes_control(ph_pair_t *pair, int side);
bool ph_peer_resets_control(ph_pair_t *pair, int side, uint64_t code);

/* The side's peer transport opens a unidirectional stream with these raw
 * bytes, which no session wrote, and ends it as `end` says (PH_END_*; code
 * is the reset's). Returns false when the double has no stream left or the
 * bytes do not fit. */
bool ph_peer_opens_uni(ph_pair_t *pair, int side, const uint8_t *data,
                       size_t len, int end, uint64_t code);

/* The peer of `side` stops the only request stream `side` serves (one its
 * peer opened). False when there is not exactly one. */
bool ph_peer_stops_served_request(ph_pair_t *pair, int side, uint64_t code);

/* The stream ref of the only local data stream the side opened and has not
 * ended. False when there is none or more than one. */
bool ph_only_open_local_data(const ph_pair_t *pair, int side,
                             moq_stream_ref_t *out_ref);

/* The refs of the local data streams the side opened, in the order it
 * opened them; returns how many (at most cap are written). */
size_t ph_local_data_refs(const ph_pair_t *pair, int side, moq_stream_ref_t *refs,
                          size_t cap);

/* The code of the last reset the transport took from `side`. */
uint64_t ph_last_reset_code(const ph_pair_t *pair, int side);

/* Resets the transport took from `side` (each stream half once), and the
 * reset requests it ignored because that half was already reset. */
unsigned ph_transport_resets(const ph_pair_t *pair, int side, unsigned *repeats);

/* Tagged mode: the transport refuses the next `count` data stream resets
 * `side` attempts (claimed, polled, or owed after a NO_RESET outcome); each
 * stays held, is retried on a later pass, and is not progress. */
void ph_refuse_resets(ph_pair_t *pair, int side, unsigned count);

/* The side's peer sends STOP_SENDING on the data stream `ref` that the side
 * opened, without its session asking. False when there is no such stream. */
bool ph_peer_stops_data(ph_pair_t *pair, int side, moq_stream_ref_t ref,
                        uint64_t code);

/* The side's peer sends STOP_SENDING on the request stream `ref` that the
 * side opened, without its session asking. Returns false when the side has
 * no such request stream. */
bool ph_peer_stops_request(ph_pair_t *pair, int side, moq_stream_ref_t ref,
                           uint64_t code);

/* The transport refuses every byte and the FIN on one local data stream of
 * `side`, until ph_lift_data_refusal(): the stream with session ref `ref`,
 * or, with ref 0, the next data stream the side opens. That stream's reset
 * is still taken, and the side's other streams are not affected. A refused
 * write is retried on a later pass and is not progress. One stream at a
 * time: a call replaces the refusal before it. False, refusing nothing,
 * when `ref` names no local data stream of the side. */
bool ph_refuse_data_stream(ph_pair_t *pair, int side, moq_stream_ref_t ref);
void ph_lift_data_refusal(ph_pair_t *pair, int side);

/* The side's host polls at most `actions` actions in a pass (0, the
 * default, polls until the session has none): what the session queued
 * later is taken in later passes, after the reset transaction ran again. */
void ph_set_poll_budget(ph_pair_t *pair, int side, unsigned actions);

/* The refused stream takes its first `bytes` bytes before the refusal
 * applies (0, the default, refuses from the first byte), until
 * ph_lift_data_refusal(). Its FIN is refused either way. */
void ph_set_refusal_allowance(ph_pair_t *pair, int side, uint64_t bytes);

/* The same refusal on a request stream of `side`, one it opened or serves
 * (by a ref from ph_served_request_refs()); it replaces any refusal before
 * it and is lifted by ph_lift_data_refusal(). False when there is no such
 * stream. */
bool ph_refuse_request_stream(ph_pair_t *pair, int side, moq_stream_ref_t ref);

/* The peer's STOP of `side`'s local data stream or request stream `ref`
 * reaches `side`'s host again, as from a transport that reports a STOP more
 * than once: past the double's own once-only STOP, whether or not the
 * first was taken. Counted in stop_entries. False when there is no such
 * stream. */
bool ph_redeliver_stop(ph_pair_t *pair, int side, moq_stream_ref_t ref, uint64_t code);

/* What the transport took from one side's host on one of its streams. The
 * writes that took bytes, the FINs and the resets the transport takes, on
 * any stream from either side, number the pair's transport steps in the
 * order it took them, from 1; a step that never happened is 0. Steps
 * compare order across streams. */
typedef struct ph_stream_trace {
    unsigned writes;            /* writes that took bytes */
    uint64_t bytes;
    uint64_t first_write;       /* the step of the first such write */
    uint64_t last_write;
    uint64_t fin;               /* the step of the FIN */
    uint64_t reset;             /* the step of the reset (the first one) */
    uint64_t reset_code;
    uint64_t reset_reliable;    /* the leading bytes that reset still delivers */
} ph_stream_trace_t;

/* The trace of what `side` wrote on its local data stream `ref` (its
 * session's ref), or on the request stream `ref` (one it opened, by its
 * session's ref, or one it serves, by a ref from ph_served_request_refs()).
 * False when the side has no such stream. */
bool ph_data_trace(const ph_pair_t *pair, int side, moq_stream_ref_t ref,
                   ph_stream_trace_t *out);
bool ph_request_trace(const ph_pair_t *pair, int side, moq_stream_ref_t ref,
                      ph_stream_trace_t *out);

/* The refs of the request streams `side` serves (its peer opened them), in
 * the order they arrived; returns how many (at most cap are written). */
size_t ph_served_request_refs(const ph_pair_t *pair, int side,
                              moq_stream_ref_t *refs, size_t cap);

/* While held, `side` reads nothing from the transport: no byte, FIN,
 * reset, STOP or datagram reaches its session, and nothing it kept is
 * offered again. Its output is still written. Releasing resumes reading
 * where it stopped. A test holds a peer's reactions back with it. */
void ph_set_input_hold(ph_pair_t *pair, int side, bool held);

/* The side's peer transport writes these raw bytes, which no session wrote,
 * on the only request stream `side` serves, after what its peer wrote
 * there. False when there is not exactly one, its peer already finished or
 * reset it, or the bytes do not fit. */
bool ph_peer_writes_served_request(ph_pair_t *pair, int side,
                                   const uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_TEST_PUBLIC_HOST_H */
