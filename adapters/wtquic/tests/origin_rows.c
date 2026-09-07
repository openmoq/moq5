#include "origin_rows.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- the frozen row table -------------------------------------------------- */

static const char *const k_allow_a[]    = { "https://a.example" };
static const char *const k_allow_null[] = { "null" };

static const origin_row_t k_rows[] = {
    { "unset/absent", ORIGIN_POLICY_UNSET, NULL, 0,
      NULL, ROW_EXPECT_ESTABLISH },
    { "any-non-opaque/tuple", ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE, NULL, 0,
      "https://a.example", ROW_EXPECT_ESTABLISH },
    { "any-non-opaque/null", ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE, NULL, 0,
      "null", ROW_EXPECT_REFUSED_403 },
    { "any-non-opaque/absent", ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE, NULL, 0,
      NULL, ROW_EXPECT_REFUSED_403 },
    { "any-including-null/null", ORIGIN_POLICY_ALLOW_ANY_INCLUDING_NULL, NULL, 0,
      "null", ROW_EXPECT_ESTABLISH },
    { "allowlist/exact", ORIGIN_POLICY_ALLOWLIST, k_allow_a, 1,
      "https://a.example", ROW_EXPECT_ESTABLISH },
    { "allowlist/other", ORIGIN_POLICY_ALLOWLIST, k_allow_a, 1,
      "https://b.example", ROW_EXPECT_REFUSED_403 },
    { "allowlist/case", ORIGIN_POLICY_ALLOWLIST, k_allow_a, 1,
      "https://A.example", ROW_EXPECT_REFUSED_403 },
    { "allowlist/null-entry", ORIGIN_POLICY_ALLOWLIST, k_allow_null, 1,
      "null", ROW_EXPECT_ESTABLISH },
    /* One valid HTTP field value carrying two origins. The client emits it as
     * a single origin field, so it reaches the server, which classifies it as
     * a multi-origin list and denies it. */
    { "any-non-opaque/multi", ORIGIN_POLICY_ALLOW_ANY_NON_OPAQUE, NULL, 0,
      "https://a.example https://b.example", ROW_EXPECT_REFUSED_403 },
};

const origin_row_t *
origin_rows(size_t *count_out)
{
    if (count_out != NULL) {
        *count_out = sizeof(k_rows) / sizeof(k_rows[0]);
    }
    return k_rows;
}

/* -- the observation record ------------------------------------------------ */

struct origin_obs {
    pthread_mutex_t   mu;
    origin_snapshot_t s;
};

origin_obs_t *
origin_obs_new(void)
{
    origin_obs_t *o = calloc(1, sizeof(*o));
    if (o == NULL) {
        return NULL;
    }
    if (pthread_mutex_init(&o->mu, NULL) != 0) {
        free(o);
        return NULL;
    }
    return o;
}

void
origin_obs_free(origin_obs_t *o)
{
    if (o == NULL) {
        return;
    }
    pthread_mutex_destroy(&o->mu);
    free(o);
}

/* Record a contradiction once, keeping the FIRST one: the earliest
 * inconsistency explains the row, and a later cascade must not overwrite it. */
static void
note_contradiction(origin_obs_t *o, const char *why)
{
    if (!o->s.contradiction) {
        o->s.contradiction = true;
        snprintf(o->s.note, sizeof(o->s.note), "%s", why);
    }
}

/* Has any terminal already been recorded? */
static bool
has_terminal(const origin_obs_t *o)
{
    return o->s.n_refused > 0 || o->s.n_failed > 0 || o->s.n_closed > 0;
}

void
origin_obs_established(origin_obs_t *o, const char *subprotocol,
                       size_t subprotocol_len)
{
    pthread_mutex_lock(&o->mu);
    /* Establishment after a terminal is impossible; a refusal followed by an
     * establishment is exactly the contradiction the gate must catch. */
    if (has_terminal(o)) {
        note_contradiction(o, "established after a terminal event");
    }
    o->s.n_established++;
    if (o->s.n_established > 1) {
        note_contradiction(o, "established more than once");
    }
    /* the argument is borrowed for the callback only: copy it here */
    if (subprotocol != NULL && subprotocol_len < sizeof(o->s.subprotocol)) {
        memcpy(o->s.subprotocol, subprotocol, subprotocol_len);
        o->s.subprotocol[subprotocol_len] = '\0';
        o->s.subprotocol_len = subprotocol_len;
    } else if (subprotocol != NULL) {
        note_contradiction(o, "negotiated subprotocol longer than the record");
    }
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_profile(origin_obs_t *o, bool ok, uint32_t profile)
{
    pthread_mutex_lock(&o->mu);
    o->s.profile_ok = ok;
    o->s.profile = profile;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_refused(origin_obs_t *o, uint16_t http_status)
{
    pthread_mutex_lock(&o->mu);
    if (has_terminal(o)) {
        note_contradiction(o, "a second terminal event after the first");
    }
    if (o->s.n_established > 0) {
        note_contradiction(o, "refused after establishing");
    }
    o->s.n_refused++;
    o->s.status = http_status;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_failed(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    if (has_terminal(o)) {
        note_contradiction(o, "a second terminal event after the first");
    }
    o->s.n_failed++;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_closed(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    if (has_terminal(o)) {
        note_contradiction(o, "a second terminal event after the first");
    }
    if (o->s.n_established == 0) {
        note_contradiction(o, "closed without establishing");
    }
    o->s.n_closed++;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_sealed(origin_obs_t *o, const origin_sealed_t *rec)
{
    pthread_mutex_lock(&o->mu);
    o->s.sealed_seen = true;
    if (rec != NULL) {
        o->s.sealed = *rec;
    }
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_pump_failed(origin_obs_t *o, const char *why)
{
    pthread_mutex_lock(&o->mu);
    o->s.pump_failed = true;
    if (why != NULL && o->s.note[0] == '\0') {
        snprintf(o->s.note, sizeof(o->s.note), "%s", why);
    }
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_profile_unselected(origin_obs_t *o, bool reported_state,
                              bool poison_intact)
{
    pthread_mutex_lock(&o->mu);
    o->s.profile_before_seen = true;
    o->s.profile_before_state = reported_state;
    o->s.profile_before_untouched = poison_intact;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_close_attempt(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.close_attempted = true;
    pthread_mutex_unlock(&o->mu);
}

bool
origin_obs_close_attempted(origin_obs_t *o)
{
    bool v;
    pthread_mutex_lock(&o->mu);
    v = o->s.close_attempted;
    /* the reentrant on_closed asked, and its answer is itself evidence */
    o->s.close_seen_attempt_in_callback = v;
    pthread_mutex_unlock(&o->mu);
    return v;
}

void
origin_obs_close_result(origin_obs_t *o, bool call_ok)
{
    pthread_mutex_lock(&o->mu);
    o->s.close_issued = true;
    o->s.close_ok = call_ok;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_constructed(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.n_constructed++;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_finalized(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.n_finalized++;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_ref_acquired(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.n_ref_acquired++;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_barrier_done(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.barrier_done = true;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_teardown_ok(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.teardown_ok = true;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_ref_released(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.n_ref_released++;
    /* A release is legal only once the completion barrier has returned; one
     * recorded before it touches a handle the backend may still be using. */
    if (o->s.barrier_done) {
        o->s.n_released_after_barrier++;
    }
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_setup_failed(origin_obs_t *o, const char *why)
{
    pthread_mutex_lock(&o->mu);
    o->s.setup_failed = true;
    if (why != NULL && o->s.note[0] == '\0') {
        snprintf(o->s.note, sizeof(o->s.note), "%s", why);
    }
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_timed_out(origin_obs_t *o)
{
    pthread_mutex_lock(&o->mu);
    o->s.timed_out = true;
    pthread_mutex_unlock(&o->mu);
}

void
origin_obs_snapshot(origin_obs_t *o, origin_snapshot_t *out)
{
    pthread_mutex_lock(&o->mu);
    *out = o->s;
    pthread_mutex_unlock(&o->mu);
}

/* -- the verdict ----------------------------------------------------------- */

static bool
fail(char *out, size_t cap, const char *why)
{
    if (out != NULL && cap > 0) {
        snprintf(out, cap, "%s", why);
    }
    return false;
}

bool
origin_row_passed(const origin_row_t *row, const origin_snapshot_t *s,
                  char *reason_out, size_t reason_cap)
{
    if (reason_out != NULL && reason_cap > 0) {
        reason_out[0] = '\0';
    }
    if (row == NULL || s == NULL) {
        return fail(reason_out, reason_cap, "no row or no observations");
    }
    /* Rules that hold for every row, whatever it expected. */
    if (s->setup_failed) {
        return fail(reason_out, reason_cap,
                    s->note[0] != '\0' ? s->note : "row setup failed");
    }
    if (s->timed_out) {
        return fail(reason_out, reason_cap,
                    "the row budget elapsed; an elapsed budget is never a pass");
    }
    if (s->contradiction) {
        return fail(reason_out, reason_cap,
                    s->note[0] != '\0' ? s->note : "contradictory events");
    }
    /*
     * Resource facts, required rather than merely not-contradicted. A row that
     * "looks" successful but never acquired a reference, never released it, or
     * released it before the completion barrier has not finished safely -- and
     * counting only the pathological case (more than one release) let all three
     * of those pass.
     */
    if (s->n_ref_acquired != 1) {
        return fail(reason_out, reason_cap,
                    "a connect must yield exactly one app-owned reference");
    }
    if (!s->barrier_done) {
        return fail(reason_out, reason_cap,
                    "the environment completion barrier never returned");
    }
    if (s->n_ref_released != 1) {
        return fail(reason_out, reason_cap,
                    "the app-owned reference must be released exactly once");
    }
    if (s->n_released_after_barrier != 1) {
        return fail(reason_out, reason_cap,
                    "the reference was released before the completion "
                    "barrier, while the backend could still touch it");
    }
    if (s->n_constructed == 0 || s->n_finalized != s->n_constructed) {
        return fail(reason_out, reason_cap,
                    "not every constructed object was finalized");
    }
    if (!s->teardown_ok) {
        return fail(reason_out, reason_cap,
                    "teardown did not complete within its grace");
    }
    if (s->pump_failed) {
        return fail(reason_out, reason_cap,
                    s->note[0] != '\0' ? s->note
                                       : "the owning server pump failed");
    }
    /*
     * The sealed record must have been READ successfully, filled completely,
     * and carry the no-transport-cause shape these rows expect. Checking only
     * its kind let a nonzero code, a foreign domain or dirty reserved bytes
     * through.
     */
    if (!s->sealed_seen || !s->sealed.query_ok) {
        return fail(reason_out, reason_cap,
                    "the sealed transport-error record could not be read");
    }
    /*
     * Each field is checked on its own so a refusal says WHICH part of the
     * shape was wrong, and the whole-object comparison follows to catch a
     * producer that simply never wrote a trailing field.
     */
    if (s->sealed.kind != ORIGIN_SEALED_KIND_NONE) {
        return fail(reason_out, reason_cap,
                    "the sealed record reports a transport error kind");
    }
    if (s->sealed.quic_code != 0) {
        return fail(reason_out, reason_cap,
                    "the sealed record carries a nonzero QUIC code");
    }
    if (s->sealed.native_domain != ORIGIN_SEALED_DOMAIN_NONE) {
        return fail(reason_out, reason_cap,
                    "the sealed record names a native error domain");
    }
    if (s->sealed.native_code != 0) {
        return fail(reason_out, reason_cap,
                    "the sealed record carries a nonzero native code");
    }
    if (s->sealed.reserved0 != 0) {
        return fail(reason_out, reason_cap,
                    "the sealed record has a dirty first reserved field");
    }
    if (s->sealed.reserved1 != 0) {
        return fail(reason_out, reason_cap,
                    "the sealed record has a dirty second reserved field");
    }
    if (!s->sealed.image_exact) {
        return fail(reason_out, reason_cap,
                    "the sealed record was not written whole: some field kept "
                    "the caller's poison");
    }

    if (row->expect == ROW_EXPECT_REFUSED_403) {
        if (s->n_refused != 1) {
            return fail(reason_out, reason_cap,
                        "an authorization denial needs exactly one refusal");
        }
        if (s->status != ORIGIN_REFUSAL_STATUS) {
            /* 404 is an unknown path and 400 a malformed or profile-invalid
             * request; both are decided before the Origin check and neither is
             * authorization evidence. */
            return fail(reason_out, reason_cap,
                        "refused with a status other than 403, which is a "
                        "different decision, not an Origin denial");
        }
        if (s->n_established != 0) {
            return fail(reason_out, reason_cap,
                        "a denied row must never establish");
        }
        if (s->n_failed != 0) {
            return fail(reason_out, reason_cap,
                        "a transport failure is not an Origin denial");
        }
        if (s->n_closed != 0) {
            return fail(reason_out, reason_cap,
                        "a session that never established cannot close");
        }
        /*
         * A session that never established never selected a profile. THIS is
         * where the poisoned query belongs: it proves the query reports the
         * unselected state rather than a zero that could be read as CURRENT.
         */
        if (!s->profile_before_seen) {
            return fail(reason_out, reason_cap,
                        "no profile query was made at the legal unselected "
                        "point");
        }
        if (!s->profile_before_state || !s->profile_before_untouched) {
            return fail(reason_out, reason_cap,
                        "the unselected-profile query did not report that "
                        "state with its poisoned output left intact");
        }
        return true;
    }

    /* ROW_EXPECT_ESTABLISH */
    if (s->n_established != 1) {
        return fail(reason_out, reason_cap,
                    "an admitted row must establish exactly once");
    }
    if (s->n_refused != 0 || s->n_failed != 0) {
        return fail(reason_out, reason_cap,
                    "an admitted row must not be refused or fail");
    }
    if (s->subprotocol_len != strlen(ORIGIN_EXPECT_SUBPROTOCOL) ||
        memcmp(s->subprotocol, ORIGIN_EXPECT_SUBPROTOCOL,
               s->subprotocol_len) != 0) {
        return fail(reason_out, reason_cap,
                    "the negotiated subprotocol was not the offered one");
    }
    /*
     * An established session HAS selected a profile, so the query succeeds from
     * inside on_established onward. Demanding the unselected shape here was a
     * rule no honest positive row could satisfy.
     */
    if (!s->profile_ok || s->profile != ORIGIN_EXPECT_PROFILE) {
        return fail(reason_out, reason_cap,
                    "the profile observed at establishment was not the "
                    "configured one");
    }
    if (s->profile_before_seen) {
        return fail(reason_out, reason_cap,
                    "an established row must not claim an unselected-profile "
                    "observation");
    }
    /* Establishment is provisional: the row is finished only when its own
     * deliberate close has completed. */
    if (!s->close_issued) {
        return fail(reason_out, reason_cap,
                    "an established row must close deliberately");
    }
    /* The close CALL's own result matters: a failed close followed by plausible
     * peer events is not this row closing itself. */
    if (!s->close_attempted) {
        return fail(reason_out, reason_cap,
                    "no local close attempt was recorded before the call");
    }
    if (!s->close_seen_attempt_in_callback) {
        return fail(reason_out, reason_cap,
                    "the close callback did not observe the local attempt, so "
                    "this close cannot be told from a peer-initiated one");
    }
    if (!s->close_ok) {
        return fail(reason_out, reason_cap,
                    "the deliberate close call itself failed");
    }
    if (s->n_closed != 1) {
        return fail(reason_out, reason_cap,
                    "an established row completes at exactly one close");
    }
    return true;
}

/* -- the bounded child-to-parent report ------------------------------------ */

/* A deliberately simple additive checksum over every byte but the field that
 * holds it: enough to refuse a truncated or scrambled record, and not a
 * security claim. */
static uint32_t
report_checksum(const origin_report_t *r)
{
    origin_report_t tmp = *r;
    const unsigned char *p;
    uint32_t sum = 2166136261u;
    size_t i;

    tmp.checksum = 0;
    p = (const unsigned char *)&tmp;
    for (i = 0; i < sizeof(tmp); i++) {
        sum = (sum ^ p[i]) * 16777619u;
    }
    return sum;
}

void
origin_report_init(origin_report_t *r, uint32_t row_index, bool passed,
                   const origin_snapshot_t *snap)
{
    if (r == NULL) {
        return;
    }
    /* zeroed whole, so padding never varies between writer and reader */
    memset(r, 0, sizeof(*r));
    r->magic = ORIGIN_REPORT_MAGIC;
    r->bytes = (uint32_t)sizeof(*r);
    r->row_index = row_index;
    r->passed = passed ? 1u : 0u;
    if (snap != NULL) {
        r->snap = *snap;
    }
    r->checksum = report_checksum(r);
}

void
origin_report_reseal(origin_report_t *r)
{
    if (r != NULL) {
        r->checksum = report_checksum(r);
    }
}

size_t
origin_report_encode(const origin_report_t *r, void *buf, size_t cap)
{
    if (r == NULL || buf == NULL || cap < sizeof(*r)) {
        return 0;
    }
    memcpy(buf, r, sizeof(*r));
    return sizeof(*r);
}

bool
origin_report_decode(const void *buf, size_t len, origin_report_t *out,
                     char *why, size_t why_cap)
{
    origin_report_t r;

    if (why != NULL && why_cap > 0) {
        why[0] = '\0';
    }
    if (buf == NULL || out == NULL) {
        return fail(why, why_cap, "no report buffer");
    }
    if (len < sizeof(r)) {
        /* the child was cut short: fewer bytes than one whole record */
        return fail(why, why_cap, "the report is shorter than one record");
    }
    if (len > sizeof(r)) {
        return fail(why, why_cap, "the report is longer than one record");
    }
    memcpy(&r, buf, sizeof(r));
    if (r.magic != ORIGIN_REPORT_MAGIC) {
        return fail(why, why_cap, "the report does not begin a record");
    }
    if (r.bytes != (uint32_t)sizeof(r)) {
        return fail(why, why_cap,
                    "the record declares a size this reader cannot verify");
    }
    if (r.checksum != report_checksum(&r)) {
        return fail(why, why_cap, "the record's contents do not check out");
    }
    *out = r;
    return true;
}

/* -- the child's phase transition ------------------------------------------ */

static uint32_t
phase_checksum(const origin_phase_t *p)
{
    origin_phase_t tmp = *p;
    const unsigned char *b;
    uint32_t sum = 2166136261u;
    size_t i;

    tmp.checksum = 0;
    b = (const unsigned char *)&tmp;
    for (i = 0; i < sizeof(tmp); i++) {
        sum = (sum ^ b[i]) * 16777619u;
    }
    return sum;
}

void
origin_phase_init(origin_phase_t *p, uint32_t kind, uint64_t at_ms)
{
    if (p == NULL) {
        return;
    }
    memset(p, 0, sizeof(*p));
    p->magic = ORIGIN_PHASE_MAGIC;
    p->kind = kind;
    p->bytes = (uint32_t)sizeof(*p);
    p->at_ms = at_ms;
    p->checksum = phase_checksum(p);
}

void
origin_phase_reseal(origin_phase_t *p)
{
    if (p != NULL) {
        p->checksum = phase_checksum(p);
    }
}

bool
origin_phase_decode(const void *buf, size_t len, origin_phase_t *out)
{
    origin_phase_t p;

    if (buf == NULL || out == NULL || len != sizeof(p)) {
        return false;
    }
    memcpy(&p, buf, sizeof(p));
    if (p.magic != ORIGIN_PHASE_MAGIC || p.bytes != (uint32_t)sizeof(p)) {
        return false;
    }
    if (p.checksum != phase_checksum(&p)) {
        return false;
    }
    *out = p;
    return true;
}
