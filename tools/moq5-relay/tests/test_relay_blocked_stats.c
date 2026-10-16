/*
 * Strict grammar + completeness + per-mode acceptance for the
 * RELAY_BLOCKED_V0 record (cli/blockedstats). Pure logic, no transport — the
 * relay owns this schema; the gauntlet parses records the relay emits. The
 * live LIVE-only aggregate lifecycle / overflow behaviour is proven against a
 * real bind in test_relay_session_binding.c; here we prove only the wire
 * grammar and the acceptance rules over the plain row struct.
 */

#include "../cli/blockedstats.h"

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

static moqr_cli_blocked_row_t
mk(uint32_t lane, uint32_t live, uint32_t cac, uint32_t csg, uint32_t cbs,
   uint64_t tac, uint64_t tsg, uint64_t tbs, uint32_t pac, uint32_t psg)
{
    moqr_cli_blocked_row_t r;
    memset(&r, 0, sizeof(r));
    r.epoch = 7;
    r.lane = lane;
    r.live_conns = live;
    r.conns_action_cap = cac;
    r.conns_session_sg = csg;
    r.conns_bind_sg = cbs;
    r.action_cap_total = tac;
    r.session_sg_total = tsg;
    r.bind_sg_total = tbs;
    r.parked_action_cap = pac;
    r.parked_session_sg = psg;
    return r;
}

static int
test_format_parse_roundtrip(void)
{
    int failures = 0;
    moqr_cli_blocked_row_t r = mk(3, 5, 1, 2, 0, 10, 20, 0, 1, 1);
    r.epoch = 0x1122334455667788ull;
    char buf[512];
    int w = moqr_cli_blocked_format(buf, sizeof(buf), &r);
    MOQ_TEST_CHECK(w > 0 && (size_t)w < sizeof(buf));
    moqr_cli_blocked_row_t got;
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(buf, &got) == MOQR_BLOCKED_ROW);
    MOQ_TEST_CHECK(memcmp(&r, &got, sizeof(r)) == 0);

    /* exact field names/order: the emitted line is byte-for-byte the schema */
    moqr_cli_blocked_row_t s = mk(1, 2, 0, 0, 1, 0, 0, 4, 0, 0);
    s.epoch = 9;
    char sb[512];
    moqr_cli_blocked_format(sb, sizeof(sb), &s);
    MOQ_TEST_CHECK(strcmp(sb,
        "RELAY_BLOCKED_V0,epoch=9,lane=1,live_conns=2,conns_action_cap=0,"
        "conns_session_sg=0,conns_bind_sg=1,action_cap_total=0,"
        "session_sg_total=0,bind_sg_total=4,parked_action_cap=0,"
        "parked_session_sg=0,eor=1") == 0);

    /* u32/u64 maxima round-trip losslessly AND fit the shared row buffer:
     * every field maxed is the worst case the coordinator must serialize, so
     * MOQR_BLOCKED_ROW_MAX must hold it (the coordinator sizes line[] and its
     * per-lane allocation off this constant). */
    moqr_cli_blocked_row_t hi =
        mk(0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu,
           0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull,
           0xFFFFFFFFu, 0xFFFFFFFFu);   /* every field at its type maximum */
    hi.epoch = 0xFFFFFFFFFFFFFFFFull;
    char hb[MOQR_BLOCKED_ROW_MAX];
    int hw = moqr_cli_blocked_format(hb, sizeof(hb), &hi);
    MOQ_TEST_CHECK(hw > 0 && (size_t)hw < MOQR_BLOCKED_ROW_MAX);
    MOQ_TEST_CHECK(hw <= 337);   /* the documented all-max width */
    moqr_cli_blocked_row_t hg;
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(hb, &hg) == MOQR_BLOCKED_ROW);
    MOQ_TEST_CHECK(memcmp(&hi, &hg, sizeof(hi)) == 0);

    /* refusal round-trips as REFUSED with epoch+lane */
    char rb[128];
    MOQ_TEST_CHECK(moqr_cli_blocked_format_refused(rb, sizeof(rb), 9, 2) > 0);
    moqr_cli_blocked_row_t rr;
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(rb, &rr) == MOQR_BLOCKED_REFUSED);
    MOQ_TEST_CHECK(rr.epoch == 9 && rr.lane == 2);

    /* format into a too-small buffer refuses (truncation detectable) */
    char tiny[16];
    MOQ_TEST_CHECK(moqr_cli_blocked_format(tiny, sizeof(tiny), &r) == -1);
    MOQ_TEST_CHECK(moqr_cli_blocked_format_refused(tiny, sizeof(tiny), 1, 0)
                   == -1);
    MOQ_TEST_PASS("format_parse_roundtrip");
    return failures;
}

static int
test_parse_malformed(void)
{
    int failures = 0;
    moqr_cli_blocked_row_t g;
    /* wrong prefix / version */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse("RELAY_BLOCKED_V1,epoch=1,eor=1",
                                          &g) == MOQR_BLOCKED_MALFORMED);
    /* prefix present but not comma-delimited */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse("RELAY_BLOCKED_V0X", &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* missing eor */
    const char *no_eor =
        "RELAY_BLOCKED_V0,epoch=7,lane=0,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(no_eor, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* wrong key ORDER (lane before epoch) */
    const char *reorder =
        "RELAY_BLOCKED_V0,lane=0,epoch=7,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,eor=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(reorder, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* non-digit value */
    const char *junk =
        "RELAY_BLOCKED_V0,epoch=7,lane=0,live_conns=1x,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,eor=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(junk, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* empty value */
    const char *empty =
        "RELAY_BLOCKED_V0,epoch=,lane=0,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,eor=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(empty, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* u32 field max+one (lane = 2^32) */
    const char *u32ovf =
        "RELAY_BLOCKED_V0,epoch=7,lane=4294967296,live_conns=1,"
        "conns_action_cap=1,conns_session_sg=0,conns_bind_sg=0,"
        "action_cap_total=5,session_sg_total=0,bind_sg_total=0,"
        "parked_action_cap=1,parked_session_sg=0,eor=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(u32ovf, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* u64 field max+one (action_cap_total = 2^64) */
    const char *u64ovf =
        "RELAY_BLOCKED_V0,epoch=7,lane=0,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,"
        "action_cap_total=18446744073709551616,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,eor=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(u64ovf, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* truncated mid-line (cut a valid row short) */
    char buf[512];
    moqr_cli_blocked_row_t r = mk(0, 1, 1, 0, 0, 5, 0, 0, 1, 0);
    moqr_cli_blocked_format(buf, sizeof(buf), &r);
    buf[40] = '\0';
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(buf, &g) == MOQR_BLOCKED_MALFORMED);
    /* extra trailing token (field count wrong) */
    moqr_cli_blocked_format(buf, sizeof(buf), &r);
    strcat(buf, ",extra=1");
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(buf, &g) == MOQR_BLOCKED_MALFORMED);
    /* too few tokens */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(
        "RELAY_BLOCKED_V0,epoch=7,lane=0,eor=1", &g) ==
        MOQR_BLOCKED_MALFORMED);
    /* correct token COUNT but the 12th terminator is not eor=1 */
    const char *bad_eor =
        "RELAY_BLOCKED_V0,epoch=7,lane=0,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,zzz=1";
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(bad_eor, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* doubled delimiter right after the prefix (empty leading token) */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(
        "RELAY_BLOCKED_V0,,epoch=7,lane=0,live_conns=1,conns_action_cap=1,"
        "conns_session_sg=0,conns_bind_sg=0,action_cap_total=5,"
        "session_sg_total=0,bind_sg_total=0,parked_action_cap=1,"
        "parked_session_sg=0,eor=1", &g) == MOQR_BLOCKED_MALFORMED);
    /* trailing delimiter after a valid row (empty final token) */
    {
        char tc[MOQR_BLOCKED_ROW_MAX];
        moqr_cli_blocked_row_t vr = mk(0, 1, 1, 0, 0, 5, 0, 0, 1, 0);
        moqr_cli_blocked_format(tc, sizeof(tc), &vr);
        strcat(tc, ",");
        MOQ_TEST_CHECK(moqr_cli_blocked_parse(tc, &g) ==
                       MOQR_BLOCKED_MALFORMED);
    }
    /* trailing delimiter on a refusal record */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(
        "RELAY_BLOCKED_V0,epoch=9,lane=2,refused=1,eor=1,", &g) ==
        MOQR_BLOCKED_MALFORMED);
    /* NULL arguments fail closed (no crash) */
    MOQ_TEST_CHECK(moqr_cli_blocked_parse(NULL, &g) == MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_blocked_parse("RELAY_BLOCKED_V0,epoch=1,eor=1",
                                          NULL) == MOQR_BLOCKED_MALFORMED);
    {
        moqr_cli_blocked_row_t vr = mk(0, 1, 1, 0, 0, 5, 0, 0, 1, 0);
        char b[MOQR_BLOCKED_ROW_MAX];
        MOQ_TEST_CHECK(moqr_cli_blocked_format(NULL, sizeof(b), &vr) == -1);
        MOQ_TEST_CHECK(moqr_cli_blocked_format(b, sizeof(b), NULL) == -1);
        MOQ_TEST_CHECK(moqr_cli_blocked_format_refused(NULL, 16, 1, 0) == -1);
    }
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(NULL));
    MOQ_TEST_PASS("parse_malformed");
    return failures;
}

static int
test_row_consistency(void)
{
    int failures = 0;
    /* healthy: 1 action-cap conn, currently parked */
    moqr_cli_blocked_row_t ok = mk(0, 3, 1, 0, 0, 5, 0, 0, 1, 0);
    MOQ_TEST_CHECK(moqr_cli_blocked_row_consistent(&ok));
    /* conns > live */
    moqr_cli_blocked_row_t a = mk(0, 1, 2, 0, 0, 5, 0, 0, 0, 0);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&a));
    /* parked > live */
    moqr_cli_blocked_row_t b = mk(0, 1, 1, 0, 0, 5, 0, 0, 2, 0);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&b));
    /* parked > conns of that reason */
    moqr_cli_blocked_row_t c = mk(0, 3, 1, 0, 0, 5, 0, 0, 2, 0);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&c));
    /* parked_action_cap + parked_session_sg > live_conns (disjoint bound) */
    moqr_cli_blocked_row_t p = mk(0, 1, 1, 1, 0, 5, 5, 0, 1, 1);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&p));
    /* ...but the disjoint sum exactly equal to live is fine */
    moqr_cli_blocked_row_t pe = mk(0, 2, 1, 1, 0, 5, 5, 0, 1, 1);
    MOQ_TEST_CHECK(moqr_cli_blocked_row_consistent(&pe));
    /* total>0 but conns==0 */
    moqr_cli_blocked_row_t d = mk(0, 3, 0, 0, 0, 5, 0, 0, 0, 0);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&d));
    /* conns>0 but total==0 */
    moqr_cli_blocked_row_t e = mk(0, 3, 1, 0, 0, 0, 0, 0, 1, 0);
    MOQ_TEST_CHECK(!moqr_cli_blocked_row_consistent(&e));
    MOQ_TEST_PASS("row_consistency");
    return failures;
}

/* Build a healthy K=2 record for `mode` (blocked binding on lane 0). */
static void
healthy(moqr_cli_blocked_row_t rows[2], moqr_blocked_mode_t mode)
{
    /* lane 1: an unrelated conn, no blockage */
    rows[1] = mk(1, 1, 0, 0, 0, 0, 0, 0, 0, 0);
    switch (mode) {
    case MOQR_BLK_ACTION_CAP:
        rows[0] = mk(0, 2, 1, 0, 0, 4, 0, 0, 1, 0);
        break;
    case MOQR_BLK_SESSION_SG:
        rows[0] = mk(0, 2, 0, 1, 0, 0, 4, 0, 0, 1);
        break;
    case MOQR_BLK_BIND_SG:
        rows[0] = mk(0, 2, 0, 0, 1, 0, 0, 4, 0, 0);
        break;
    }
}

static int
test_accept(void)
{
    int failures = 0;
    const char *why;
    /* all three accepted shapes */
    for (int m = 0; m < 3; m++) {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, (moqr_blocked_mode_t)m);
        why = NULL;
        bool ok = moqr_cli_blocked_accept(rows, 2, 2, 7,
                                          (moqr_blocked_mode_t)m, &why);
        MOQ_TEST_CHECK(ok);
        if (!ok) {
            fprintf(stderr, "  mode %d refused: %s\n", m, why ? why : "?");
        }
    }
    /* NULL rows */
    MOQ_TEST_CHECK(!moqr_cli_blocked_accept(NULL, 2, 2, 7,
                                            MOQR_BLK_BIND_SG, &why));
    /* wrong count */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 1, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* out-of-order (descending) lanes */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        moqr_cli_blocked_row_t tmp = rows[0];
        rows[0] = rows[1];
        rows[0].lane = 1;
        rows[1] = tmp;
        rows[1].lane = 0;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* duplicate lane (both lane 0) */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[1].lane = 0;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* out-of-range / gap in lanes ({0,2} for K=2) */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[1].lane = 2;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* mixed epoch */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[1].epoch = 8;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* stale: rows all carry epoch 6 but the caller requested 7 */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[0].epoch = rows[1].epoch = 6;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* zero epoch requested */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[0].epoch = rows[1].epoch = 0;
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 0,
                                                MOQR_BLK_BIND_SG, &why));
    }
    /* an inconsistent row rejects the whole record */
    {
        moqr_cli_blocked_row_t rows[2];
        healthy(rows, MOQR_BLK_BIND_SG);
        rows[1].conns_action_cap = 5;   /* > live_conns=1 */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(rows, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    MOQ_TEST_PASS("accept");
    return failures;
}

/* Each per-mode rejection, exercised individually. */
static int
test_accept_per_mode_rejects(void)
{
    int failures = 0;
    const char *why;
    /* --- ACTION_CAP --- */
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_ACTION_CAP);
        r[0].parked_action_cap = 0;   /* consistent, but not currently parked */
        MOQ_TEST_CHECK(moqr_cli_blocked_row_consistent(&r[0]));
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_ACTION_CAP, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_ACTION_CAP);
        r[1] = mk(1, 2, 1, 0, 0, 3, 0, 0, 1, 0);   /* lane1 also action-cap */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_ACTION_CAP, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_ACTION_CAP);
        /* a session-sg park anywhere disqualifies an action-cap scenario */
        r[1] = mk(1, 1, 0, 1, 0, 0, 2, 0, 0, 1);
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_ACTION_CAP, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_ACTION_CAP);
        r[0] = mk(0, 2, 0, 0, 0, 0, 0, 0, 0, 0);   /* lane0 not blocked */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_ACTION_CAP, &why));
    }
    /* --- SESSION_SG --- */
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_SESSION_SG);
        r[0].parked_session_sg = 0;   /* consistent, not currently parked */
        MOQ_TEST_CHECK(moqr_cli_blocked_row_consistent(&r[0]));
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_SESSION_SG, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_SESSION_SG);
        r[1] = mk(1, 2, 0, 1, 0, 0, 3, 0, 0, 1);   /* lane1 also session-sg */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_SESSION_SG, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_SESSION_SG);
        /* an action-cap park anywhere disqualifies a session-sg scenario */
        r[1] = mk(1, 1, 1, 0, 0, 2, 0, 0, 1, 0);
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_SESSION_SG, &why));
    }
    /* --- BIND_SG --- */
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_BIND_SG);
        r[1] = mk(1, 2, 0, 0, 1, 0, 0, 3, 0, 0);   /* lane1 also bind-sg */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_BIND_SG);
        /* bind-sg never parks: any parked population disqualifies */
        r[0].conns_action_cap = 1;
        r[0].action_cap_total = 1;
        r[0].parked_action_cap = 1;
        MOQ_TEST_CHECK(moqr_cli_blocked_row_consistent(&r[0]));
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    {
        moqr_cli_blocked_row_t r[2];
        healthy(r, MOQR_BLK_BIND_SG);
        r[0] = mk(0, 2, 0, 0, 0, 0, 0, 0, 0, 0);   /* no blockage */
        MOQ_TEST_CHECK(!moqr_cli_blocked_accept(r, 2, 2, 7,
                                                MOQR_BLK_BIND_SG, &why));
    }
    MOQ_TEST_PASS("accept_per_mode_rejects");
    return failures;
}

/* The record-level contract the gauntlet implements: parse each of the K
 * lines and REQUIRE every one to be a ROW — a single REFUSED or MALFORMED
 * line invalidates the whole record (it is never filtered out and the
 * survivors accepted). Only an all-ROW set reaches moqr_cli_blocked_accept. */
static bool
accept_record(const char *const *lines, size_t k, uint32_t lanes,
              uint64_t epoch, moqr_blocked_mode_t mode, const char **why)
{
    moqr_cli_blocked_row_t rows[8];
    if (k > 8) {
        return false;
    }
    for (size_t i = 0; i < k; i++) {
        if (moqr_cli_blocked_parse(lines[i], &rows[i]) != MOQR_BLOCKED_ROW) {
            if (why != NULL) {
                *why = "a non-ROW line invalidates the whole record";
            }
            return false;
        }
    }
    return moqr_cli_blocked_accept(rows, k, lanes, epoch, mode, why);
}

static int
test_record_level(void)
{
    int failures = 0;
    const char *why = NULL;
    moqr_cli_blocked_row_t rows[2];
    healthy(rows, MOQR_BLK_BIND_SG);
    char l0[MOQR_BLOCKED_ROW_MAX], l1[MOQR_BLOCKED_ROW_MAX];
    MOQ_TEST_CHECK(moqr_cli_blocked_format(l0, sizeof(l0), &rows[0]) > 0);
    MOQ_TEST_CHECK(moqr_cli_blocked_format(l1, sizeof(l1), &rows[1]) > 0);

    /* all-ROW: accepted */
    const char *ok_lines[2] = { l0, l1 };
    MOQ_TEST_CHECK(accept_record(ok_lines, 2, 2, 7, MOQR_BLK_BIND_SG, &why));

    /* one refusal line among K → whole record invalid */
    char lref[MOQR_BLOCKED_ROW_MAX];
    MOQ_TEST_CHECK(
        moqr_cli_blocked_format_refused(lref, sizeof(lref), 7, 1) > 0);
    const char *ref_lines[2] = { l0, lref };
    MOQ_TEST_CHECK(!accept_record(ref_lines, 2, 2, 7, MOQR_BLK_BIND_SG, &why));

    /* one malformed line among K → whole record invalid */
    const char *mal_lines[2] = { l0, "not-a-record" };
    MOQ_TEST_CHECK(!accept_record(mal_lines, 2, 2, 7, MOQR_BLK_BIND_SG, &why));
    MOQ_TEST_PASS("record_level");
    return failures;
}

/* RELAY_SGDIAG: per-conn stranding diagnostics — grammar + the exactly-one-
 * blocked-target acceptance (ambiguity fails closed). */
static moqr_cli_sgdiag_row_t
dgmk(uint32_t slot, uint32_t occ, uint32_t ready, uint32_t parked,
     uint32_t reason, uint64_t bsg)
{
    moqr_cli_sgdiag_row_t r;
    memset(&r, 0, sizeof(r));
    r.epoch = 7;
    r.lane = 0;
    r.slot = slot;
    r.occ = occ;
    r.ready = ready;
    r.parked = parked;
    r.reason = reason;
    r.bind_sg_total = bsg;
    return r;
}

static int
test_sgdiag(void)
{
    int failures = 0;
    /* round-trip */
    moqr_cli_sgdiag_row_t r = dgmk(3, 4, 1, 0, 0, 9);
    r.action_cap_total = 1;
    r.session_sg_total = 2;
    r.bind_sg_demand = 42;
    char b[MOQR_BLOCKED_ROW_MAX];
    MOQ_TEST_CHECK(moqr_cli_sgdiag_format(b, sizeof(b), &r) > 0);
    moqr_cli_sgdiag_row_t g;
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse(b, &g) == MOQR_BLOCKED_ROW);
    MOQ_TEST_CHECK(memcmp(&r, &g, sizeof(r)) == 0);
    /* malformed: junk value, missing new fields, trailing comma, missing
     * eor, wrong prefix */
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse("RELAY_SGDIAG,epoch=x,lane=0,"
        "slot=0,occ=0,ready=0,parked=0,reason=0,action_cap_total=0,"
        "session_sg_total=0,bind_sg_total=0,bind_sg_demand=0,"
        "bind_sg_demand_ambiguous=0,eor=1", &g) ==
        MOQR_BLOCKED_MALFORMED);
    /* the OLD 11-token grammar (no demand fields) is now malformed — the
     * demand join is part of the row, never optional */
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse("RELAY_SGDIAG,epoch=7,lane=0,"
        "slot=0,occ=0,ready=0,parked=0,reason=0,action_cap_total=0,"
        "session_sg_total=0,bind_sg_total=0,eor=1", &g) ==
        MOQR_BLOCKED_MALFORMED);
    {
        char t[MOQR_BLOCKED_ROW_MAX + 2];
        moqr_cli_sgdiag_format(b, sizeof(b), &r);
        snprintf(t, sizeof(t), "%s,", b);
        MOQ_TEST_CHECK(moqr_cli_sgdiag_parse(t, &g) ==
                       MOQR_BLOCKED_MALFORMED);
    }
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse("RELAY_SGDIAG,epoch=1,eor=1", &g) ==
                   MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse("RELAY_SGDIAGX,epoch=1", &g) ==
                   MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_sgdiag_parse(NULL, &g) ==
                   MOQR_BLOCKED_MALFORMED);
    /* consistency: flags bounded, park names a reason, attribution implies
     * the refusal was reached */
    moqr_cli_sgdiag_row_t c = dgmk(0, 0, 2, 0, 0, 0);
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_row_consistent(&c));
    c = dgmk(0, 0, 0, 1, 0, 0);
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_row_consistent(&c));
    c = dgmk(0, 0, 0, 1, 2, 0);
    MOQ_TEST_CHECK(moqr_cli_sgdiag_row_consistent(&c));
    c = dgmk(0, 0, 0, 0, 0, 0);
    c.bind_sg_demand = 9;            /* demand without any refusal */
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_row_consistent(&c));
    c = dgmk(0, 0, 0, 0, 0, 0);
    c.bind_sg_demand_ambiguous = 1;  /* ambiguity without any refusal */
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_row_consistent(&c));
    c = dgmk(0, 0, 0, 0, 0, 1);
    c.bind_sg_demand_ambiguous = 2;  /* flag out of range */
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_row_consistent(&c));
    /* acceptance: exactly one blocked target ON THE EXPECTED LANE (0),
     * carrying an UNAMBIGUOUS demand attribution */
    const char *why = NULL;
    uint32_t slot = 99;
    uint64_t dem = 0;
    moqr_cli_sgdiag_row_t rows[3] = { dgmk(0, 4, 0, 0, 0, 2),
                                      dgmk(1, 0, 0, 0, 0, 0),
                                      dgmk(2, 1, 0, 0, 0, 0) };
    rows[0].bind_sg_demand = 42;
    MOQ_TEST_CHECK(moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    MOQ_TEST_CHECK(slot == 0);
    MOQ_TEST_CHECK(dem == 42);   /* the join hands back the exact demand */
    /* NO demand attribution on the blocked row: the join cannot be made —
     * fail closed (the caller must never guess the demand) */
    rows[0].bind_sg_demand = 0;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    rows[0].bind_sg_demand = 42;
    /* AMBIGUOUS attribution (two demands refused on the same conn): the
     * exact join is impossible — fail closed */
    rows[0].bind_sg_demand_ambiguous = 1;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    rows[0].bind_sg_demand_ambiguous = 0;
    /* WRONG LANE: a row from another lane is someone else's evidence */
    rows[1].lane = 1;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    rows[1].lane = 0;
    /* the whole record pinned to the wrong expected lane also fails */
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 1, &slot, &dem, &why));
    /* ambiguous: two blocked rows */
    rows[2].bind_sg_total = 1;
    rows[2].bind_sg_demand = 43;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    /* missing: zero blocked rows */
    rows[0].bind_sg_total = 0;
    rows[0].bind_sg_demand = 0;
    rows[2].bind_sg_total = 0;
    rows[2].bind_sg_demand = 0;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 3, 7, 0, &slot, &dem, &why));
    /* duplicate slot */
    rows[0] = dgmk(1, 4, 0, 0, 0, 2);
    rows[0].bind_sg_demand = 42;
    rows[1] = dgmk(1, 0, 0, 0, 0, 0);
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 2, 7, 0, &slot, &dem, &why));
    /* wrong epoch */
    rows[0] = dgmk(0, 4, 0, 0, 0, 2);
    rows[0].bind_sg_demand = 42;
    rows[1] = dgmk(1, 0, 0, 0, 0, 0);
    rows[1].epoch = 6;
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 2, 7, 0, &slot, &dem, &why));
    /* zero epoch / NULL rows */
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(rows, 2, 0, 0, &slot, &dem, &why));
    MOQ_TEST_CHECK(!moqr_cli_sgdiag_accept(NULL, 2, 7, 0, &slot, &dem, &why));
    MOQ_TEST_PASS("sgdiag");
    return failures;
}

/* RELAY_SEALLOG: destination seal-ingest evidence — grammar + strictly
 * increasing ingest order. */
static moqr_cli_seallog_row_t
slmk(uint64_t seq, uint64_t total, uint64_t demand, uint64_t g, uint64_t sg)
{
    moqr_cli_seallog_row_t r;
    memset(&r, 0, sizeof(r));
    r.epoch = 7;
    r.lane = 0;
    r.seq = seq;
    r.total = total;
    r.src = 1;
    r.demand = demand;
    r.group_id = g;
    r.subgroup_id = sg;
    return r;
}

static int
test_seallog(void)
{
    int failures = 0;
    /* round-trip with full attribution */
    moqr_cli_seallog_row_t r = slmk(5, 6, 42, 0, 4);
    char b[MOQR_BLOCKED_ROW_MAX];
    MOQ_TEST_CHECK(moqr_cli_seallog_format(b, sizeof(b), &r) > 0);
    moqr_cli_seallog_row_t g;
    MOQ_TEST_CHECK(moqr_cli_seallog_parse(b, &g) == MOQR_BLOCKED_ROW);
    MOQ_TEST_CHECK(memcmp(&r, &g, sizeof(r)) == 0);
    /* malformed: junk value, missing field, NULL */
    MOQ_TEST_CHECK(moqr_cli_seallog_parse("RELAY_SEALLOG,epoch=7,lane=0,"
        "seq=1,total=2,src=1,demand=3,group=0,subgroup=x,eor=1", &g) ==
        MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_seallog_parse("RELAY_SEALLOG,epoch=7,lane=0,"
        "seq=1,total=2,src=1,group=0,subgroup=1,eor=1", &g) ==
        MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_seallog_parse(NULL, &g) ==
                   MOQR_BLOCKED_MALFORMED);

    /* COLLISION: a background seal for the SAME (group, subgroup) from a
     * DIFFERENT demand is visibly distinct — attribution separates what
     * group/subgroup alone cannot. (Acceptance itself lives in the interval
     * validator — the sole canonical contract; the removed lifetime
     * count==total form went permanently invalid once the ring wrapped.) */
    moqr_cli_seallog_row_t rows[4] = { slmk(0, 4, 9, 0, 4), slmk(1, 4, 77, 0, 4),
                                       slmk(2, 4, 9, 0, 1), slmk(3, 4, 9, 0, 2) };
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, 4, 9, 0) == 3);
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, 4, 77, 0) == 1);
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, 4, 5, 0) == 0);
    /* the cursor bounds the count: only rows AT/AFTER since_seq exist for
     * ownership purposes */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, 4, 9, 2) == 2);
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, 4, 9, 4) == 0);
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(NULL, 4, 9, 0) == 0);
    MOQ_TEST_PASS("seallog");
    return failures;
}

/* RELAY_SEALMETA + interval-complete acceptance: the seal ring keeps only the
 * newest 32, so lifetime completeness (count==total) is impossible after wrap.
 * Evidence is INTERVAL-complete: a header pins [first_retained_seq,
 * lifetime_total) and a cursor established at baseline. */

/* Fill `rows` with the contiguous ring window [first, total) (seq ascending),
 * all lane 0 / epoch 7 / same lifetime total; returns the count. */
static size_t
fill_window(moqr_cli_seallog_row_t *rows, uint64_t first, uint64_t total)
{
    size_t k = 0;
    for (uint64_t s = first; s < total; s++) {
        rows[k++] = slmk(s, total, 9, 0, 4);
    }
    return k;
}

static moqr_cli_seallog_meta_t
mkmeta(uint64_t first, uint64_t total, uint64_t count)
{
    moqr_cli_seallog_meta_t m;
    memset(&m, 0, sizeof(m));
    m.epoch = 7;
    m.lane = 0;
    m.first_retained_seq = first;
    m.lifetime_total = total;
    m.retained_count = count;
    return m;
}

static int
test_seallog_interval(void)
{
    int failures = 0;
    const char *why = NULL;
    char b[MOQR_BLOCKED_ROW_MAX];
    moqr_cli_seallog_row_t win[32];
    size_t c;

    /* meta round-trip */
    moqr_cli_seallog_meta_t m = mkmeta(8, 40, 32);
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_format(b, sizeof(b), &m) > 0);
    moqr_cli_seallog_meta_t mg;
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_parse(b, &mg) == MOQR_BLOCKED_ROW);
    MOQ_TEST_CHECK(memcmp(&m, &mg, sizeof(m)) == 0);
    /* meta malformed: junk value, missing field, wrong prefix, NULL */
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_parse("RELAY_SEALMETA,epoch=7,lane=0,"
        "first_retained_seq=8,lifetime_total=x,retained_count=32,eor=1", &mg)
        == MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_parse("RELAY_SEALMETA,epoch=7,lane=0,"
        "first_retained_seq=8,lifetime_total=40,eor=1", &mg)
        == MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_parse("RELAY_SEALMETAX,epoch=1", &mg)
        == MOQR_BLOCKED_MALFORMED);
    MOQ_TEST_CHECK(moqr_cli_seallog_meta_parse(NULL, &mg) ==
                   MOQR_BLOCKED_MALFORMED);

    /* BASELINE after 40 background seals: window is the newest 32 = [8,40). */
    c = fill_window(win, 8, 40);
    MOQ_TEST_CHECK(c == 32);
    moqr_cli_seallog_meta_t base = mkmeta(8, 40, 32);
    uint64_t cursor = 0;
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 0,
                                                    true, &cursor, &why));
    MOQ_TEST_CHECK(cursor == 40);   /* baseline cursor = 40 */

    /* +6 target seals: total 46, window [14,46). Interval 40..45 must accept
     * even though lifetime history (seq 8..13) has now wrapped out. */
    c = fill_window(win, 14, 46);
    MOQ_TEST_CHECK(c == 32);
    moqr_cli_seallog_meta_t after = mkmeta(14, 46, 32);
    uint64_t cursor2 = 0;
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&after, win, c, 7, 0,
                                                    cursor, false, &cursor2,
                                                    &why));
    MOQ_TEST_CHECK(cursor2 == 46);

    /* 33 events after the cursor OVERWRITE it: baseline cursor=40, +33 →
     * total 73, window [41,73), first_retained_seq 41 > cursor 40 → reject. */
    c = fill_window(win, 41, 73);
    MOQ_TEST_CHECK(c == 32);
    moqr_cli_seallog_meta_t over = mkmeta(41, 73, 32);
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&over, win, c, 7, 0, 40,
                                                     false, NULL, &why));

    /* boundary: exactly 32 after the cursor still covers it (first==cursor). */
    c = fill_window(win, 40, 72);
    moqr_cli_seallog_meta_t edge = mkmeta(40, 72, 32);
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&edge, win, c, 7, 0, 40,
                                                    false, NULL, &why));

    /* ZERO-EVENT snapshot: header still REQUIRED and still accepts (interval
     * since the cursor is empty; total unchanged). */
    moqr_cli_seallog_meta_t zero = mkmeta(46, 46, 0);
    uint64_t zc = 0;
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&zero, NULL, 0, 7, 0, 46,
                                                    false, &zc, &why));
    MOQ_TEST_CHECK(zc == 46);
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&zero, NULL, 0, 7, 0, 0,
                                                    true, NULL, &why));

    /* meta REQUIRED (NULL header proves nothing) */
    c = fill_window(win, 8, 40);
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(NULL, win, c, 7, 0, 0,
                                                     true, NULL, &why));

    /* GAP inside the window breaks contiguity → reject */
    c = fill_window(win, 8, 40);
    win[10].seq += 1;
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 0,
                                                     true, NULL, &why));
    /* REORDER */
    c = fill_window(win, 8, 40);
    {
        moqr_cli_seallog_row_t tmp = win[3];
        win[3] = win[4];
        win[4] = tmp;
    }
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 0,
                                                     true, NULL, &why));
    /* WRONG LANE (meta pin) and WRONG LANE (a single row) */
    c = fill_window(win, 8, 40);
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 1, 0,
                                                     true, NULL, &why));
    c = fill_window(win, 8, 40);
    win[5].lane = 1;
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 0,
                                                     true, NULL, &why));
    /* MIXED EPOCH (a row) */
    c = fill_window(win, 8, 40);
    win[5].epoch = 6;
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 0,
                                                     true, NULL, &why));
    /* INCONSISTENT META: retained_count disagrees with the row count */
    c = fill_window(win, 8, 40);
    {
        moqr_cli_seallog_meta_t bad = mkmeta(8, 40, 31);
        MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&bad, win, c, 7, 0, 0,
                                                         true, NULL, &why));
    }
    /* INCONSISTENT META: the [first,total) span disagrees with the rows */
    c = fill_window(win, 8, 40);
    {
        moqr_cli_seallog_meta_t bad = mkmeta(9, 40, 32);   /* span 31 != 32 */
        MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&bad, win, c, 7, 0, 0,
                                                         true, NULL, &why));
    }
    c = fill_window(win, 8, 40);
    {
        moqr_cli_seallog_meta_t bad = mkmeta(8, 41, 32);   /* span 33 != 32 */
        MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&bad, win, c, 7, 0, 0,
                                                         true, NULL, &why));
    }
    /* cursor ahead of the lifetime total (impossible) → reject */
    c = fill_window(win, 8, 40);
    MOQ_TEST_CHECK(!moqr_cli_seallog_accept_interval(&base, win, c, 7, 0, 41,
                                                     false, NULL, &why));
    MOQ_TEST_PASS("seallog_interval");
    return failures;
}

/* The SGDIAG→SEALLOG demand join, adversarial cases: (1) two demands publish
 * seals with the SAME (group, subgroup) identity, so identity alone cannot
 * attribute them — the join must run on the SGDIAG-mapped demand and the
 * WRONG demand must never satisfy the target's ownership; (2) the retained
 * ring holds mapped-demand seals from BEFORE the interval cursor — only
 * post-cursor seals may count or establish seal presence, else stale
 * history satisfies the target. */
static int
test_demand_join(void)
{
    int failures = 0;
    const char *why = NULL;
    /* SGDIAG: the blocked conn slot 2 identifies demand 42, unambiguous. */
    moqr_cli_sgdiag_row_t drows[2] = { dgmk(2, 4, 0, 0, 0, 3),
                                       dgmk(5, 1, 1, 0, 0, 0) };
    drows[0].bind_sg_demand = 42;
    uint32_t slot = 0;
    uint64_t mapped = 0;
    MOQ_TEST_CHECK(moqr_cli_sgdiag_accept(drows, 2, 7, 0, &slot, &mapped,
                                          &why));
    MOQ_TEST_CHECK(slot == 2 && mapped == 42);
    /* SEALLOG: retained window [14,46) (the 32-ring after 46 lifetime
     * seals), interval cursor previous_total = 40. Demand 42 owns seals on
     * BOTH sides of the cursor: pre-cursor at seq 20/25/30 (stale history
     * still in the ring) and post-cursor at seq 41/43/45 (the interval's
     * true ownership). Demand 43 owns post-cursor 40/44 — ALL 42/43 seals
     * carry the identical (group=1, subgroup=0) identity. Background rows
     * are demand 9. */
    moqr_cli_seallog_row_t rows[32];
    size_t c = fill_window(rows, 14, 46);
    MOQ_TEST_CHECK(c == 32);
    for (size_t i = 0; i < c; i++) {
        rows[i].demand = 9;
        rows[i].group_id = 0;
        rows[i].subgroup_id = 9;
    }
    static const uint64_t d42_seqs[6] = { 20, 25, 30, 41, 43, 45 };
    static const uint64_t d43_seqs[2] = { 40, 44 };
    for (size_t i = 0; i < 6; i++) {
        rows[d42_seqs[i] - 14].demand = 42;
        rows[d42_seqs[i] - 14].group_id = 1;
        rows[d42_seqs[i] - 14].subgroup_id = 0;
    }
    for (size_t i = 0; i < 2; i++) {
        rows[d43_seqs[i] - 14].demand = 43;
        rows[d43_seqs[i] - 14].group_id = 1;
        rows[d43_seqs[i] - 14].subgroup_id = 0;
    }
    moqr_cli_seallog_meta_t m = mkmeta(14, 46, 32);
    uint64_t cursor = 40;
    MOQ_TEST_CHECK(moqr_cli_seallog_accept_interval(&m, rows, c, 7, 0, cursor,
                                                    false, NULL, &why));
    /* The MAPPED demand owns its expected POST-CURSOR seals (3)... */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, mapped, cursor) ==
                   3);
    /* ...and the unbounded ring count would be INFLATED by stale pre-cursor
     * history (6 != 3) — this is exactly what cursor-bounding excludes. */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, mapped, 0) == 6);
    /* seal PRESENCE is cursor-bounded too: a demand whose only seals are
     * pre-cursor is ABSENT from the interval. */
    rows[20 - 14].demand = 55;   /* demand 55: one pre-cursor seal only */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, 55, cursor) == 0);
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, 55, 0) == 1);
    rows[20 - 14].demand = 42;
    /* The WRONG demand NEVER satisfies the target: not by count... */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, 43, cursor) != 3);
    /* ...and not by identity — (group=1, subgroup=0) matching without the
     * demand claims all 5 post-cursor identity rows, equal to neither
     * owner's true count. */
    size_t by_identity = 0;
    for (size_t i = 0; i < c; i++) {
        if (rows[i].group_id == 1 && rows[i].subgroup_id == 0 &&
            rows[i].seq >= cursor) {
            by_identity++;
        }
    }
    MOQ_TEST_CHECK(by_identity == 5);
    MOQ_TEST_CHECK(by_identity !=
                   moqr_cli_seallog_count_demand(rows, c, mapped, cursor));
    /* If SGDIAG had mapped the OTHER demand, the same evidence yields a
     * DIFFERENT ownership — the join direction matters, never symmetric. */
    MOQ_TEST_CHECK(moqr_cli_seallog_count_demand(rows, c, 43, cursor) == 2);
    MOQ_TEST_PASS("demand_join");
    return failures;
}

/* The pre-registered BIND_SG stranding decision table: every observation
 * maps to exactly its committed diagnosis, and invalid observations fail
 * closed — the interpretation is fixed BEFORE the real-transport reading,
 * never fitted to it. */
static int
test_sg_decision_table(void)
{
    int failures = 0;
    /* the five registered rows (cap 4 as in the K=2 scenario) */
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(false, 4, 4, 0) ==
                   MOQR_SG_DIAG_NOT_APPLIED);
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(false, 0, 4, 1) ==
                   MOQR_SG_DIAG_NOT_APPLIED);   /* seal absent dominates */
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 4, 4, 0) ==
                   MOQR_SG_DIAG_NO_RELEASE);
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 4, 4, 1) ==
                   MOQR_SG_DIAG_PASS_INCOMPLETE);
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 3, 4, 0) ==
                   MOQR_SG_DIAG_NOT_REARMED);
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 3, 4, 1) ==
                   MOQR_SG_DIAG_DOORBELL_UNCONSUMED);
    /* occ == 0 sits in the released half, not a special case */
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 0, 4, 0) ==
                   MOQR_SG_DIAG_NOT_REARMED);
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 0, 4, 1) ==
                   MOQR_SG_DIAG_DOORBELL_UNCONSUMED);
    /* invalid observations fail closed */
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 5, 4, 0) ==
                   MOQR_SG_DIAG_INVALID);   /* occ > cap impossible */
    MOQ_TEST_CHECK(moqr_cli_sg_diagnose(true, 4, 4, 2) ==
                   MOQR_SG_DIAG_INVALID);   /* flag out of range */
    /* every diagnosis names its registered wording; no NULLs */
    for (int d = 0; d <= 5; d++) {
        MOQ_TEST_CHECK(moqr_cli_sg_diagnosis_str((moqr_sg_diagnosis_t)d) !=
                       NULL);
    }
    /* distinct diagnoses carry distinct wordings */
    MOQ_TEST_CHECK(strcmp(
        moqr_cli_sg_diagnosis_str(MOQR_SG_DIAG_NO_RELEASE),
        moqr_cli_sg_diagnosis_str(MOQR_SG_DIAG_NOT_REARMED)) != 0);
    MOQ_TEST_PASS("sg_decision_table");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_format_parse_roundtrip();
    failures += test_parse_malformed();
    failures += test_row_consistency();
    failures += test_accept();
    failures += test_accept_per_mode_rejects();
    failures += test_record_level();
    failures += test_sgdiag();
    failures += test_seallog();
    failures += test_seallog_interval();
    failures += test_demand_join();
    failures += test_sg_decision_table();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
