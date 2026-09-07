/*
 * The Origin gate's orchestration, driven with no provider.
 *
 * These cases call the SAME recorders and the SAME verdict function the native
 * fixture uses, so a rule proved here is the rule that runs there. Nothing in
 * this file starts an environment, a listener, a client, a socket or a
 * certificate: every event is supplied directly, which is what makes the
 * negative cases -- a 404 standing in for a 403, a callback arriving before the
 * connect call returned, a double release -- reachable at all.
 */
#include "origin_rows.h"

#include <stdio.h>
#include <string.h>

/* The no-transport-cause shape every row of this matrix ends with. */
static void
seal(origin_obs_t *o, bool query_ok, bool size_ok, uint32_t kind)
{
    origin_sealed_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.query_ok = query_ok;
    rec.image_exact = size_ok;
    rec.kind = kind;
    origin_obs_sealed(o, &rec);
}
#define SEAL_OK(o)      seal((o), true, true, ORIGIN_SEALED_KIND_NONE)
#define SEAL_KIND(o, k) seal((o), true, true, (uint32_t)(k))
#define SEAL_FAILED(o)  seal((o), false, false, 0u)

/* A deliberate close, recorded the way the real API sequences it: the attempt
 * before the call, the reentrant callback observing it, the result after. */
static void
close_ok(origin_obs_t *o)
{
    origin_obs_close_attempt(o);
    (void)origin_obs_close_attempted(o);   /* the reentrant on_closed asks */
    origin_obs_close_result(o, true);
}
#define CLOSE_OK(o) close_ok((o))

static int failures;
static int checks;

#define CHECK(cond)                                                          \
    do {                                                                     \
        checks++;                                                            \
        if (!(cond)) {                                                       \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond);          \
            failures++;                                                      \
        }                                                                    \
    } while (0)

/* Assert a row's verdict, and on an expected failure that the reason is
 * actually populated -- a silent refusal explains nothing to an operator. */
static void
expect_verdict(const char *what, const origin_row_t *row,
               const origin_snapshot_t *s, bool want)
{
    char reason[ORIGIN_REASON_CAP];
    bool got = origin_row_passed(row, s, reason, sizeof(reason));
    checks++;
    if (got != want) {
        printf("FAIL: %s: verdict %s, wanted %s (reason: %s)\n", what,
               got ? "pass" : "fail", want ? "pass" : "fail",
               reason[0] != '\0' ? reason : "(none)");
        failures++;
        return;
    }
    if (!want && reason[0] == '\0') {
        printf("FAIL: %s: refused without a reason\n", what);
        failures++;
    }
}

/* A complete, correct positive run, recorded through the real recorders. */
static void
drive_good_positive(origin_obs_t *o)
{
    origin_obs_constructed(o);                 /* server */
    origin_obs_constructed(o);                 /* client env */
    origin_obs_ref_acquired(o);                /* connect returned a handle */
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    CLOSE_OK(o);
    origin_obs_closed(o);
    SEAL_OK(o);
    origin_obs_barrier_done(o);                /* env_close returned */
    origin_obs_ref_released(o);                /* only now is this legal */
    origin_obs_finalized(o); origin_obs_finalized(o);
    origin_obs_teardown_ok(o);
}

/* A complete, correct authorization denial. */
static void
drive_good_denial(origin_obs_t *o)
{
    origin_obs_constructed(o);
    origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    /* queried at the legal point inside on_refused, on a session that never
     * selected a profile, with the output word poisoned first */
    origin_obs_profile_unselected(o, true, true);
    SEAL_OK(o);
    origin_obs_barrier_done(o);
    origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o);
    origin_obs_teardown_ok(o);
}

int
main(void)
{
    size_t n = 0;
    const origin_row_t *rows = origin_rows(&n);
    origin_snapshot_t s;
    origin_obs_t *o;

    /* -- the frozen table ------------------------------------------------- */
    CHECK(n == 10);
    if (n == 10) {
        /* order and expectation are frozen: a reordering or a flipped
         * expectation silently changes what the gate proves */
        CHECK(rows[0].policy == ORIGIN_POLICY_UNSET &&
              rows[0].client_origin == NULL &&
              rows[0].expect == ROW_EXPECT_ESTABLISH);
        CHECK(rows[2].expect == ROW_EXPECT_REFUSED_403 &&
              strcmp(rows[2].client_origin, "null") == 0);
        CHECK(rows[3].expect == ROW_EXPECT_REFUSED_403 &&
              rows[3].client_origin == NULL);
        CHECK(rows[7].expect == ROW_EXPECT_REFUSED_403 &&
              strcmp(rows[7].client_origin, "https://A.example") == 0);
        CHECK(rows[8].policy == ORIGIN_POLICY_ALLOWLIST &&
              rows[8].allow_count == 1 &&
              strcmp(rows[8].allow[0], "null") == 0);
        /* the multi-origin row is one field value carrying two origins */
        CHECK(rows[9].expect == ROW_EXPECT_REFUSED_403 &&
              strchr(rows[9].client_origin, ' ') != NULL);
        /* five admitted and five denied; only the denials are authorization
         * evidence, and the count is pinned so a row cannot quietly change
         * side */
        {
            int est = 0, ref = 0;
            for (size_t i = 0; i < n; i++) {
                if (rows[i].expect == ROW_EXPECT_ESTABLISH) est++; else ref++;
            }
            CHECK(est == 5 && ref == 5);
        }
    }

    /* -- controls: a correct run of each kind passes ---------------------- */
    o = origin_obs_new(); CHECK(o != NULL);
    drive_good_positive(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a correct positive", &rows[0], &s, true);
    origin_obs_free(o);

    o = origin_obs_new();
    drive_good_denial(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a correct 403 denial", &rows[2], &s, true);
    origin_obs_free(o);

    /* -- the four false passes this gate used to have --------------------- */
    /* A row that never acquired a reference is not a completed row, even
     * though nothing pathological was recorded. */
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    CLOSE_OK(o); origin_obs_closed(o);
    SEAL_OK(o); origin_obs_barrier_done(o);
    origin_obs_finalized(o); origin_obs_finalized(o); origin_obs_teardown_ok(o);
    origin_obs_snapshot(o, &s);
    CHECK(s.n_ref_acquired == 0 && s.n_ref_released == 0);
    expect_verdict("a row that never acquired or released a reference",
                   &rows[0], &s, false);
    origin_obs_free(o);

    /* A sealed record whose READ failed is not evidence, however present the
     * recorded sentinel looks. */
    o = origin_obs_new();
    drive_good_denial(o);
    SEAL_FAILED(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a denial whose sealed read failed", &rows[2], &s, false);
    origin_obs_free(o);

    /* A denial that never queried the unselected profile at its legal point. */
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    SEAL_OK(o);
    origin_obs_barrier_done(o); origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o); origin_obs_teardown_ok(o);
    origin_obs_snapshot(o, &s);
    CHECK(!s.profile_before_seen);
    expect_verdict("a denial with no unselected-profile query", &rows[2], &s,
                   false);
    origin_obs_free(o);

    /* A release recorded BEFORE the completion barrier: the count is one, but
     * it happened while the backend could still touch the handle. */
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_profile_unselected(o, true, true);
    SEAL_OK(o);
    origin_obs_ref_released(o);            /* before the barrier */
    origin_obs_barrier_done(o);
    origin_obs_finalized(o); origin_obs_finalized(o); origin_obs_teardown_ok(o);
    origin_obs_snapshot(o, &s);
    CHECK(s.n_ref_released == 1 && s.n_released_after_barrier == 0);
    expect_verdict("a release before the completion barrier", &rows[2], &s,
                   false);
    origin_obs_free(o);

    /* A deliberate close whose CALL failed is not this row closing itself,
     * even when the peer events that follow look plausible. */
    o = origin_obs_new();
    drive_good_positive(o);
    origin_obs_close_result(o, false);
    origin_obs_snapshot(o, &s);
    expect_verdict("a deliberate close whose call failed", &rows[0], &s, false);
    origin_obs_free(o);

    /* An unfinalized object is an unfinished row. */
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_profile_unselected(o, true, true);
    SEAL_OK(o);
    origin_obs_barrier_done(o); origin_obs_ref_released(o);
    origin_obs_finalized(o);               /* one of two */
    origin_obs_teardown_ok(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a row with an unfinalized object", &rows[2], &s, false);
    origin_obs_free(o);

    /* Teardown that did not complete in its grace fails the row. */
    o = origin_obs_new();
    drive_good_denial(o);
    { origin_snapshot_t t; origin_obs_snapshot(o, &t); (void)t; }
    origin_obs_free(o);
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    origin_obs_ref_acquired(o);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_profile_unselected(o, true, true);
    SEAL_OK(o);
    origin_obs_barrier_done(o); origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o);
    /* teardown_ok deliberately not recorded */
    origin_obs_snapshot(o, &s);
    expect_verdict("a row whose teardown never completed", &rows[2], &s, false);
    origin_obs_free(o);

    /* -- the wrong refusal causes must not score as authorization --------- */
    {
        static const uint16_t kWrong[] = { 400, 404, 401, 500, 200, 0 };
        for (size_t i = 0; i < sizeof(kWrong) / sizeof(kWrong[0]); i++) {
            char what[64];
            o = origin_obs_new();
            drive_good_denial(o);
            origin_obs_refused(o, kWrong[i]);   /* perturb only the status */
            origin_obs_snapshot(o, &s);
            snprintf(what, sizeof(what), "a refusal with status %u",
                     (unsigned)kWrong[i]);
            expect_verdict(what, &rows[2], &s, false);
            origin_obs_free(o);
        }
    }
    /* a TLS/transport failure is not a denial */
    o = origin_obs_new();
    origin_obs_failed(o);
    SEAL_KIND(o, 3);
    origin_obs_snapshot(o, &s);
    expect_verdict("a transport failure on a denial row", &rows[2], &s, false);
    origin_obs_free(o);

    /* nothing at all is not a denial */
    o = origin_obs_new();
    origin_obs_snapshot(o, &s);
    expect_verdict("no events at all on a denial row", &rows[2], &s, false);
    origin_obs_free(o);

    /* a denial with no sealed observation is incomplete */
    o = origin_obs_new();
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_snapshot(o, &s);
    expect_verdict("a 403 with no sealed record", &rows[2], &s, false);
    origin_obs_free(o);

    /* -- contradictions --------------------------------------------------- */
    /* establishment after a refusal is the contradiction that must never pass */
    o = origin_obs_new();
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    SEAL_OK(o);
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_snapshot(o, &s);
    CHECK(s.contradiction);
    expect_verdict("establishment after a refusal", &rows[2], &s, false);
    origin_obs_free(o);

    /* two terminals */
    o = origin_obs_new();
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    origin_obs_refused(o, ORIGIN_REFUSAL_STATUS);
    SEAL_OK(o);
    origin_obs_snapshot(o, &s);
    CHECK(s.contradiction);
    expect_verdict("two refusals", &rows[2], &s, false);
    origin_obs_free(o);

    /* a close with no establishment */
    o = origin_obs_new();
    origin_obs_closed(o);
    origin_obs_snapshot(o, &s);
    CHECK(s.contradiction);
    expect_verdict("closed without establishing", &rows[0], &s, false);
    origin_obs_free(o);

    /* -- callback before the connect call returned ------------------------ */
    /* The recorders take no handle, so a callback that runs before the driver
     * has published one still records correctly; this pins that ordering. */
    o = origin_obs_new();
    origin_obs_constructed(o); origin_obs_constructed(o);
    /* every callback lands BEFORE the connect call returns and before the
     * driver holds any handle */
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    CLOSE_OK(o);
    origin_obs_closed(o);
    SEAL_OK(o);
    /* only now does connect return, yielding the app reference; it is released
     * after the barrier, exactly as in an ordinary row */
    origin_obs_ref_acquired(o);
    origin_obs_barrier_done(o);
    origin_obs_ref_released(o);
    origin_obs_finalized(o); origin_obs_finalized(o); origin_obs_teardown_ok(o);
    origin_obs_snapshot(o, &s);
    CHECK(!s.contradiction);
    expect_verdict("callbacks before connect returned", &rows[0], &s, true);
    origin_obs_free(o);

    /* -- exactly-once release --------------------------------------------- */
    o = origin_obs_new();
    drive_good_positive(o);
    origin_obs_ref_released(o);            /* a second release */
    origin_obs_snapshot(o, &s);
    expect_verdict("a doubly released reference", &rows[0], &s, false);
    origin_obs_free(o);

    /* -- positive-row observation rules ----------------------------------- */
    o = origin_obs_new();
    origin_obs_established(o, "moqt-16", strlen("moqt-16"));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    CLOSE_OK(o); origin_obs_closed(o); SEAL_OK(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("the wrong negotiated subprotocol", &rows[0], &s, false);
    origin_obs_free(o);

    o = origin_obs_new();
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, false, ORIGIN_EXPECT_PROFILE); /* query failed */
    CLOSE_OK(o); origin_obs_closed(o); SEAL_OK(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a failed profile query at establishment", &rows[0], &s,
                   false);
    origin_obs_free(o);

    o = origin_obs_new();
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, 1u);       /* a different profile */
    CLOSE_OK(o); origin_obs_closed(o); SEAL_OK(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a profile other than the configured one", &rows[0], &s,
                   false);
    origin_obs_free(o);

    /* establishment alone is provisional: no close, no pass */
    o = origin_obs_new();
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    SEAL_OK(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("established but never closed", &rows[0], &s, false);
    origin_obs_free(o);

    /* A peer-initiated close is not our deliberate close. Every count here is
     * exactly what a good row shows -- established once, closed once, right
     * subprotocol and profile -- so only the deliberate-close rule separates
     * "we finished the row" from "the server ended it for us". */
    o = origin_obs_new();
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    origin_obs_closed(o);                  /* closed, but we never issued it */
    SEAL_OK(o);
    origin_obs_ref_released(o);
    origin_obs_snapshot(o, &s);
    CHECK(!s.contradiction);
    CHECK(s.n_established == 1 && s.n_closed == 1);
    expect_verdict("a close we did not issue", &rows[0], &s, false);
    origin_obs_free(o);

    /* An impossible ORDER whose counts are all correct: the close is recorded
     * before the establishment. Only the contradiction record carries ordering,
     * so this is what makes that record load-bearing rather than decorative. */
    o = origin_obs_new();
    origin_obs_closed(o);                  /* before anything established */
    origin_obs_established(o, ORIGIN_EXPECT_SUBPROTOCOL,
                           strlen(ORIGIN_EXPECT_SUBPROTOCOL));
    origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
    CLOSE_OK(o);
    SEAL_OK(o);
    origin_obs_ref_released(o);
    origin_obs_snapshot(o, &s);
    CHECK(s.n_established == 1 && s.n_closed == 1 && s.close_issued);
    CHECK(s.contradiction);
    expect_verdict("a close recorded before the establishment", &rows[0], &s,
                   false);
    origin_obs_free(o);

    /* -- the pre-establishment query must preserve its output ------------- */
    o = origin_obs_new();
    drive_good_positive(o);
    origin_obs_profile_unselected(o, true, false);   /* output was written */
    origin_obs_snapshot(o, &s);
    expect_verdict("a pre-establishment query that wrote its output",
                   &rows[0], &s, false);
    origin_obs_free(o);

    o = origin_obs_new();
    drive_good_positive(o);
    origin_obs_profile_unselected(o, false, true);   /* wrong result kind */
    origin_obs_snapshot(o, &s);
    expect_verdict("a pre-establishment query that did not report state",
                   &rows[0], &s, false);
    origin_obs_free(o);

    /* -- partial construction and blocked operations ---------------------- */
    o = origin_obs_new();
    origin_obs_setup_failed(o, "listener could not bind");
    origin_obs_snapshot(o, &s);
    expect_verdict("a row whose setup never completed", &rows[0], &s, false);
    origin_obs_free(o);

    /* a setup failure cannot be rescued by later good events */
    o = origin_obs_new();
    origin_obs_setup_failed(o, "client env could not open");
    drive_good_positive(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a setup failure followed by a good run", &rows[0], &s,
                   false);
    origin_obs_free(o);

    /* an elapsed budget is never a pass, whatever else was seen */
    o = origin_obs_new();
    drive_good_positive(o);
    origin_obs_timed_out(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a complete run whose budget elapsed", &rows[0], &s, false);
    origin_obs_free(o);

    o = origin_obs_new();
    drive_good_denial(o);
    origin_obs_timed_out(o);
    origin_obs_snapshot(o, &s);
    expect_verdict("a denial whose budget elapsed", &rows[2], &s, false);
    origin_obs_free(o);

    /* -- a truncated record is not a pass --------------------------------- */
    /* An oversized borrowed subprotocol must be refused, not silently cut. */
    {
        char big[128];
        memset(big, 'x', sizeof(big));
        o = origin_obs_new();
        origin_obs_established(o, big, sizeof(big));
        origin_obs_profile(o, true, ORIGIN_EXPECT_PROFILE);
        CLOSE_OK(o); origin_obs_closed(o); SEAL_OK(o);
        origin_obs_snapshot(o, &s);
        CHECK(s.contradiction);
        expect_verdict("an oversized negotiated subprotocol", &rows[0], &s,
                       false);
        origin_obs_free(o);
    }

    /* -- first-failure stop over the whole table -------------------------- */
    /* The driver stops at the first failing row; nothing after it runs. */
    {
        size_t executed = 0;
        bool stopped = false;
        for (size_t i = 0; i < n && !stopped; i++) {
            origin_obs_t *r = origin_obs_new();
            executed++;
            if (rows[i].expect == ROW_EXPECT_ESTABLISH) {
                drive_good_positive(r);
            } else if (i == 3) {
                origin_obs_refused(r, 404);  /* the injected wrong cause */
                SEAL_OK(r);
            } else {
                drive_good_denial(r);
            }
            origin_obs_snapshot(r, &s);
            if (!origin_row_passed(&rows[i], &s, NULL, 0)) {
                stopped = true;
            }
            origin_obs_free(r);
        }
        CHECK(stopped);
        CHECK(executed == 4);   /* rows 1..4, stopping at the injected row */
    }
    /* and a clean table runs to the end */
    {
        size_t executed = 0;
        bool stopped = false;
        for (size_t i = 0; i < n && !stopped; i++) {
            origin_obs_t *r = origin_obs_new();
            executed++;
            if (rows[i].expect == ROW_EXPECT_ESTABLISH) {
                drive_good_positive(r);
            } else {
                drive_good_denial(r);
            }
            origin_obs_snapshot(r, &s);
            if (!origin_row_passed(&rows[i], &s, NULL, 0)) {
                stopped = true;
            }
            origin_obs_free(r);
        }
        CHECK(!stopped);
        CHECK(executed == n);
    }

    /* -- the child-to-parent evidence record ------------------------------ */
    {
        origin_report_t r, back;
        unsigned char buf[sizeof(origin_report_t) + 8];
        char why[ORIGIN_REASON_CAP];
        size_t wrote;

        o = origin_obs_new();
        drive_good_denial(o);
        origin_obs_snapshot(o, &s);
        origin_report_init(&r, 2u, origin_row_passed(&rows[2], &s, NULL, 0), &s);
        wrote = origin_report_encode(&r, buf, sizeof(buf));
        CHECK(wrote == sizeof(origin_report_t));

        /* a whole record round-trips, verdict and observations intact */
        CHECK(origin_report_decode(buf, wrote, &back, why, sizeof(why)));
        CHECK(back.row_index == 2u && back.passed == 1u);
        CHECK(back.snap.status == ORIGIN_REFUSAL_STATUS);

        /* TRUNCATION: every short prefix must be refused, not decoded. A
         * partial record that parsed would turn a row that never finished
         * into a silent pass. */
        for (size_t cut = 0; cut < wrote; cut++) {
            checks++;
            if (origin_report_decode(buf, cut, &back, why, sizeof(why))) {
                printf("FAIL: a %zu-byte truncated report decoded\n", cut);
                failures++;
                break;
            }
        }
        /* an over-long record is refused too */
        CHECK(!origin_report_decode(buf, wrote + 1, &back, why, sizeof(why)));
        /* a corrupted byte is refused */
        {
            unsigned char bad[sizeof(origin_report_t)];
            memcpy(bad, buf, sizeof(bad));
            bad[sizeof(bad) / 2] = (unsigned char)(bad[sizeof(bad) / 2] ^ 0xffu);
            CHECK(!origin_report_decode(bad, sizeof(bad), &back, why,
                                        sizeof(why)));
        }
        /* a record that is not one at all is refused */
        {
            unsigned char junk[sizeof(origin_report_t)];
            memset(junk, 0, sizeof(junk));
            CHECK(!origin_report_decode(junk, sizeof(junk), &back, why,
                                        sizeof(why)));
            CHECK(why[0] != '\0');
        }
        /* a truncated report is not a verdict: the parent must not fall back
         * to the child's exit status alone */
        CHECK(!origin_report_decode(buf, wrote - 1, &back, why, sizeof(why)));
        CHECK(why[0] != '\0');
        origin_obs_free(o);
    }

    /*
     * -- the PHASE FRAME's codec ------------------------------------------
     *
     * The parent starts a whole clock on this frame, so every field it
     * accepts needs a decisive negative. Each negative below breaks exactly
     * ONE field and reseals, so a broken magic cannot be refused by the
     * checksum rule standing in for the magic rule.
     */
    {
        origin_phase_t ph, back;
        unsigned char raw[sizeof(origin_phase_t) + 8];

        origin_phase_init(&ph, ORIGIN_PHASE_ENTER_TEARDOWN, 1234567u);
        memcpy(raw, &ph, sizeof(ph));

        /* the positive: every field survives the round trip exactly */
        CHECK(origin_phase_decode(raw, sizeof(ph), &back));
        CHECK(back.magic == ORIGIN_PHASE_MAGIC);
        CHECK(back.kind == ORIGIN_PHASE_ENTER_TEARDOWN);
        CHECK(back.bytes == (uint32_t)sizeof(ph));
        CHECK(back.at_ms == 1234567u);

        /* a length that is not the frame's is not a frame */
        CHECK(!origin_phase_decode(raw, sizeof(ph) - 1u, &back));
        CHECK(!origin_phase_decode(raw, sizeof(ph) + 1u, &back));
        CHECK(!origin_phase_decode(NULL, sizeof(ph), &back));

        /* wrong magic, correctly sealed */
        {
            origin_phase_t bad = ph;
            bad.magic = ORIGIN_PHASE_MAGIC ^ 1u;
            origin_phase_reseal(&bad);
            CHECK(!origin_phase_decode(&bad, sizeof(bad), &back));
        }
        /* a declared size that disagrees with the frame, correctly sealed */
        {
            origin_phase_t bad = ph;
            bad.bytes = (uint32_t)sizeof(bad) - 4u;
            origin_phase_reseal(&bad);
            CHECK(!origin_phase_decode(&bad, sizeof(bad), &back));
        }
        /* a checksum that does not hold over the fields it covers */
        {
            origin_phase_t bad = ph;
            bad.checksum ^= 0x5a5a5a5au;
            CHECK(!origin_phase_decode(&bad, sizeof(bad), &back));
        }
        /* the checksum covers the timestamp too: a frame whose instant was
         * edited in flight is corruption, not a later transition */
        {
            origin_phase_t bad = ph;
            bad.at_ms += 5000u;
            CHECK(!origin_phase_decode(&bad, sizeof(bad), &back));
        }
        /*
         * The KIND is carried faithfully and is not the codec's business to
         * judge -- the parent decides which kind it will act on, which is why
         * a wrong-kind frame has its own supervisor case.
         */
        {
            origin_phase_t other;
            origin_phase_init(&other, 7u, 99u);
            CHECK(origin_phase_decode(&other, sizeof(other), &back));
            CHECK(back.kind == 7u);
        }
    }

    if (failures != 0) {
        printf("FAILED: %d of %d origin-row checks\n", failures, checks);
        return 1;
    }
    printf("PASS: %d origin-row checks\n", checks);
    return 0;
}
