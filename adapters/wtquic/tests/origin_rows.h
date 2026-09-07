/*
 * The Origin authorization gate's row table and verdict rules.
 *
 * This is the orchestration the native fixture runs and the offline tests
 * drive: the same row table, the same observation recorders, the same verdict
 * function. It deliberately includes NO transport header, so the rules can be
 * exercised with no provider, no socket and no certificate -- and so the
 * offline tests cannot drift into a parallel re-implementation of them.
 *
 * What the rules encode, and why each exists:
 *
 *   A refusal is only authorization evidence when its REASON is pinned. The
 *   server answers an unknown path with 404 and a malformed or profile-invalid
 *   request with 400, both BEFORE the Origin decision, and answers an Origin
 *   denial with one generic 403. A TLS chain or name failure never becomes an
 *   HTTP status at all -- it ends the connection before establishment through
 *   the ordinary failure event. So status 403 and nothing else is the oracle.
 *
 *   Establishment is NOT terminal. The transport contract fires exactly one
 *   terminal event per session: on_closed when an established session ends,
 *   on_refused/on_failed when establishment never completed. A positive row is
 *   therefore only provisionally good at establishment and is finished when its
 *   deliberate close completes.
 */
#ifndef MOQR_ORIGIN_ROWS_H
#define MOQR_ORIGIN_ROWS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The server policy a row configures. Values mirror the public closed set. */
typedef enum {
    ORIGIN_POLICY_UNSET = 0,
    ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE = 1,
    ORIGIN_POLICY_ALLOWLIST = 2,
    ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL = 3
} origin_policy_t;

typedef enum {
    ROW_EXPECT_ESTABLISH = 0,
    ROW_EXPECT_REFUSED_403 = 1
} row_expect_t;

/* The only status that proves an Origin denial. */
#define ORIGIN_REFUSAL_STATUS 403u

/* One frozen row. `client_origin == NULL` means the client sends none. */
typedef struct {
    const char        *name;
    origin_policy_t    policy;
    const char *const *allow;      /* ALLOWLIST entries, else NULL */
    size_t             allow_count;
    const char        *client_origin;
    row_expect_t       expect;
} origin_row_t;

/* The frozen first slice: ten CURRENT rows, in order. */
const origin_row_t *origin_rows(size_t *count_out);

/*
 * What one row observed.
 *
 * Callbacks may run before the connect call returns and on a thread the driver
 * does not own, so every field here is written under `mu` by the recorders
 * below and read only through origin_obs_snapshot.
 */
typedef struct origin_obs origin_obs_t;

origin_obs_t *origin_obs_new(void);
void          origin_obs_free(origin_obs_t *o);

/* Recorders. The native fixture calls these from its callbacks with the
 * session argument the callback was given; the offline tests call the same
 * functions directly. */
void origin_obs_established(origin_obs_t *o, const char *subprotocol,
                            size_t subprotocol_len);
/* A construction step completed, and later its matching finalization. Every
 * object the row builds must be finalized before a verdict is taken. */
void origin_obs_constructed(origin_obs_t *o);
void origin_obs_finalized(origin_obs_t *o);
/* The app-owned reference a successful connect returned was captured. */
void origin_obs_ref_acquired(origin_obs_t *o);
/* The environment's completion barrier returned; only after this is a held
 * reference safe to inspect or release off the callback domain. */
void origin_obs_barrier_done(origin_obs_t *o);
/* Teardown finished within its grace. */
void origin_obs_teardown_ok(origin_obs_t *o);
/* The profile observed INSIDE on_established: `ok` is the query's success,
 * `profile` its output. A failed query must leave `profile` untouched, which
 * the caller passes through unchanged. */
void origin_obs_profile(origin_obs_t *o, bool ok, uint32_t profile);
void origin_obs_refused(origin_obs_t *o, uint16_t http_status);
void origin_obs_failed(origin_obs_t *o);
void origin_obs_closed(origin_obs_t *o);
/*
 * The sealed transport-error record, read at a documented point.
 *
 * `query_ok` is the READ's own result. A failed read must be recorded as a
 * failure, never replaced by a present-looking sentinel: "we could not read the
 * record" and "the record says nothing happened" are different facts, and only
 * the second is legal evidence.
 */
/*
 * The COMPLETE sealed record, not just its kind.
 *
 * For every row of this matrix the connection ends without a transport cause:
 * an HTTP refusal, or a clean locally initiated close. The library seals an
 * explicit NONE in that case, so the legal shape is fully specified and a
 * successful query carrying some other kind, a nonzero code, a foreign domain
 * or dirty reserved bytes is NOT the shape this matrix expects. Rows that
 * deliberately test transport or TLS failure would have a different expected
 * shape; this rule is for these ten rows only.
 */
typedef struct {
    bool     query_ok;      /* the read itself succeeded */
    /*
     * The whole output object matched the expected image.
     *
     * The API restores the caller's struct_size, so comparing that field with
     * itself proves nothing. And every expected value here is zero, so a
     * zeroed buffer looks correct even when a shorter producer never wrote the
     * trailing fields. The fixture therefore POISONS the whole object first
     * and requires that the caller's size survived and every other byte was
     * overwritten to the exact shape -- a measured result, not a tautology.
     */
    bool     image_exact;
    uint32_t kind;
    uint32_t reserved0;
    uint64_t quic_code;
    uint32_t native_domain;
    uint32_t reserved1;
    long long native_code;
} origin_sealed_t;

/* The no-transport-cause shape these rows require. */
#define ORIGIN_SEALED_KIND_NONE   0u
#define ORIGIN_SEALED_DOMAIN_NONE 0u

void origin_obs_sealed(origin_obs_t *o, const origin_sealed_t *rec);
/*
 * The profile query at a legal point on a session that never selected one.
 *
 * The caller seeds its output word with a nonzero poison first. The query must
 * report the unselected state AND leave that poison intact, which is the only
 * way to tell "not selected" from the zero-valued current profile.
 */
void origin_obs_profile_unselected(origin_obs_t *o, bool reported_state,
                                   bool poison_intact);
/*
 * The deliberate close, in two parts.
 *
 * The library delivers on_closed BEFORE wtq_session_close returns, so the
 * ATTEMPT must be recorded before entering the call -- otherwise the reentrant
 * on_closed sees no attempt and cannot tell a local close from a peer one. The
 * RESULT is recorded after the call returns, and the verdict still requires it.
 */
void origin_obs_close_attempt(origin_obs_t *o);
void origin_obs_close_result(origin_obs_t *o, bool call_ok);
/* True when a legal local close attempt was already recorded. Callable from a
 * reentrant on_closed. */
bool origin_obs_close_attempted(origin_obs_t *o);

/* A server-side pump failure for this row: a poll error or a refused terminal
 * acknowledgment. Named, and folded into the row's verdict. */
void origin_obs_pump_failed(origin_obs_t *o, const char *why);
/* The app-owned client reference was released, once, after the environment's
 * completion barrier. */
void origin_obs_ref_released(origin_obs_t *o);
/* Setup could not complete; the row never reached a connect attempt. */
void origin_obs_setup_failed(origin_obs_t *o, const char *why);
/* The row budget elapsed. Never a pass, whatever else was observed. */
void origin_obs_timed_out(origin_obs_t *o);

/* A stable copy of the record, safe to inspect off the callback thread. */
typedef struct {
    unsigned n_established, n_refused, n_failed, n_closed;
    uint16_t status;
    char     subprotocol[64];
    size_t   subprotocol_len;
    bool     profile_ok;
    uint32_t profile;
    bool     profile_before_state, profile_before_untouched, profile_before_seen;
    bool     sealed_seen;
    origin_sealed_t sealed;
    bool     close_attempted;
    bool     close_seen_attempt_in_callback;
    bool     close_issued;
    bool     close_ok;
    bool     pump_failed;
    unsigned n_ref_acquired;
    unsigned n_ref_released;
    unsigned n_released_after_barrier;
    bool     barrier_done;
    bool     teardown_ok;
    unsigned n_constructed;
    unsigned n_finalized;
    bool     setup_failed;
    bool     timed_out;
    bool     contradiction;
    char     note[128];
} origin_snapshot_t;

void origin_obs_snapshot(origin_obs_t *o, origin_snapshot_t *out);

/*
 * The verdict.
 *
 * `reason_out` (optional, at least ORIGIN_REASON_CAP bytes) receives a short
 * explanation on failure. A row passes only when every rule for its expectation
 * holds; anything unexplained fails, so a new event kind cannot be silently
 * absorbed into a pass.
 */
#define ORIGIN_REASON_CAP 160
bool origin_row_passed(const origin_row_t *row, const origin_snapshot_t *s,
                       char *reason_out, size_t reason_cap);

/*
 * The bounded child-to-parent evidence record.
 *
 * A row runs in a supervised child so a blocked operation can be stopped
 * without unwinding the parent onto live callback objects. The child's verdict
 * therefore has to survive a pipe, and a pipe can deliver less than was
 * written. A short, corrupt or mis-declared record must be REFUSED: a partial
 * record that happened to decode is how a row that never finished becomes a
 * silent pass.
 */
#define ORIGIN_REPORT_MAGIC 0x4f524731u  /* "ORG1" */

typedef struct {
    uint32_t          magic;
    uint32_t          bytes;      /* the whole record's size, as written */
    uint32_t          row_index;
    uint32_t          passed;
    uint32_t          checksum;
    origin_snapshot_t snap;
} origin_report_t;

/* Fill a report from a finished row. */
void origin_report_init(origin_report_t *r, uint32_t row_index, bool passed,
                        const origin_snapshot_t *snap);
/* Encode into `buf`; returns the byte count, or 0 if it does not fit. */
/* Recompute the checksum after a deliberate field edit (tests only). */
void   origin_report_reseal(origin_report_t *r);
size_t origin_report_encode(const origin_report_t *r, void *buf, size_t cap);
/* Decode exactly `len` bytes. Returns false, with a reason, for a short,
 * over-long, mis-declared, wrong-magic or corrupt record. */
bool origin_report_decode(const void *buf, size_t len, origin_report_t *out,
                          char *why, size_t why_cap);

/*
 * The child's phase transition.
 *
 * The parent owns a work deadline and a teardown grace, and it cannot tell one
 * phase from the other by watching a pipe that stays silent. So the child emits
 * exactly one integrity-checked ENTER_TEARDOWN frame immediately before unwind
 * begins, and the parent starts the teardown clock on THAT. Extending a single
 * combined timer instead would let a blocked setup call spend the teardown
 * budget too.
 *
 * The frame carries the CLOCK_MONOTONIC instant of the transition. Parent and
 * child read the same system-wide clock, so the grace runs from when teardown
 * actually began rather than from whenever the parent happened to read the
 * pipe -- a parent that is slow to read must not thereby grant extra grace.
 *
 * The checksum is FNV-1a: it detects corruption and truncation. It is not a
 * cryptographic authentication primitive and nothing here depends on it being
 * one; the pipe has exactly one writer, which is the child this parent forked.
 */
#define ORIGIN_PHASE_MAGIC 0x4f504831u  /* "OPH1" */
#define ORIGIN_PHASE_ENTER_TEARDOWN 1u

typedef struct {
    uint32_t magic;
    uint32_t kind;
    uint32_t bytes;
    uint32_t checksum;
    uint64_t at_ms;         /* CLOCK_MONOTONIC instant of the transition */
} origin_phase_t;

void   origin_phase_init(origin_phase_t *p, uint32_t kind, uint64_t at_ms);
bool   origin_phase_decode(const void *buf, size_t len, origin_phase_t *out);
/* Recompute the checksum over the current fields. Tests only: it is what lets
 * a negative break exactly ONE field instead of breaking the checksum too. */
void   origin_phase_reseal(origin_phase_t *p);

/* The expected negotiated subprotocol for every row in this slice. */
#define ORIGIN_EXPECT_SUBPROTOCOL "moqt-18"
/* The profile every row of this slice runs at. */
#define ORIGIN_EXPECT_PROFILE 0u

#ifdef __cplusplus
}
#endif

#endif /* MOQR_ORIGIN_ROWS_H */
