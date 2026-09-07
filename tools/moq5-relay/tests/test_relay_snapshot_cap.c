/*
 * The snapshot's capability tri-state, and the broker that decides which
 * generation is collected and who receives it.
 *
 * Pure: no transport, no socket, no clock, no threads beyond the ones a test
 * starts itself.
 */

#include "../cli/broker.h"
#include "../cli/snapshot.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* -- capability tri-state -------------------------------------------------- */

static uint64_t
epoch_of(void *ctx)
{
    return *(const uint64_t *)ctx;
}

typedef struct {
    char     doc[1u << 18];
    size_t   len;
    uint64_t epoch;
    unsigned calls;
} cap_sink_t;

static void
cap_emit(void *ctx, const char *doc, size_t len, uint64_t epoch)
{
    cap_sink_t *s = ctx;
    s->calls++;
    s->epoch = epoch;
    s->len = len < sizeof(s->doc) - 1u ? len : sizeof(s->doc) - 1u;
    memcpy(s->doc, doc, s->len);
    s->doc[s->len] = '\0';
}

/* One lane publishing a row with the given capability at epoch 1, then one
 * coordinator dump. Returns the dump's result. */
static moqr_result_t
dump_with_cap(moqr_cli_cap_t cap, cap_sink_t *sink, uint64_t *out_epoch)
{
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    if (moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()) != MOQR_OK) {
        return MOQR_ERR_NOMEM;
    }

    moqr_cli_snapshot_stats_t st;
    memset(&st, 0, sizeof(st));
    st.core.ingested_total = 11;
    st.core.delivered_total = 22;
    st.core.bindings = 5;
    st.bind.conns = 3;
    st.shard.pump_turns = 99;
    st.shard.journal_epoch = 7;
    st.shard_cap = cap;
    st.lane_wakes = 4;
    moqr_cli_snapshot_publish(&snap, 0, &st, 1u);

    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };
    uint64_t requested = 1u;
    memset(sink, 0, sizeof(*sink));
    moqr_result_t rc = moqr_cli_coord_dump(&snap, &labels, epoch_of,
                                           &requested, cap_emit, sink,
                                           out_epoch);
    moqr_cli_snapshot_destroy(&snap);
    return rc;
}

/* VALID: the shard plane is rendered. */
static int
test_cap_valid_renders_shard_plane(void)
{
    int failures = 0;
    static cap_sink_t sink;
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(dump_with_cap(MOQR_CLI_CAP_VALID, &sink, &ep),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 1u);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_pump_turns_total") != NULL);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_journal_epoch") != NULL);
    /* Core families are present either way. */
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_objects_ingested_total") != NULL);
    return failures;
}

/*
 * ABSENT: there is no shard runtime, so shard-plane families are OMITTED --
 * never fabricated from a zeroed struct. A zeroed shard snapshot would render
 * `pump_turns 0`, which is a lie, not a measurement.
 */
static int
test_cap_absent_omits_shard_plane(void)
{
    int failures = 0;
    static cap_sink_t sink;
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(dump_with_cap(MOQR_CLI_CAP_ABSENT, &sink, &ep),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 1u);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_objects_ingested_total") != NULL);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_connections") != NULL);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_pump_turns_total") == NULL);
    MOQ_TEST_CHECK(strstr(sink.doc, "moqrelay_journal_epoch") == NULL);
    return failures;
}

/* REFUSED poisons the epoch: nothing is emitted at all. */
static int
test_cap_refused_poisons(void)
{
    int failures = 0;
    static cap_sink_t sink;
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(dump_with_cap(MOQR_CLI_CAP_REFUSED, &sink, &ep),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 0u);
    return failures;
}

/*
 * Zero is REFUSED by construction, so a row that was never populated -- or
 * only partly populated -- poisons rather than presenting as a legitimate
 * K=1 ABSENT composition.
 */
static int
test_zero_initialized_row_poisons(void)
{
    int failures = 0;
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_snapshot_init(&snap, 2u, moq_alloc_default()), MOQR_OK);

    moqr_cli_snapshot_stats_t good;
    memset(&good, 0, sizeof(good));
    good.shard_cap = MOQR_CLI_CAP_VALID;
    moqr_cli_snapshot_publish(&snap, 0, &good, 1u);

    /* Lane 1 publishes a row it never filled in. */
    moqr_cli_snapshot_stats_t zeroed;
    memset(&zeroed, 0, sizeof(zeroed));
    moqr_cli_snapshot_publish(&snap, 1, &zeroed, 1u);

    moqr_obs_labels_t labels[2] = { { 0, "msquic", "moqt-18" },
                                    { 1, "msquic", "moqt-18" } };
    uint64_t requested = 1u;
    static cap_sink_t sink;
    memset(&sink, 0, sizeof(sink));
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_coord_dump(&snap, labels, epoch_of,
                                              &requested, cap_emit, &sink,
                                              &ep),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 0u);
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}

/* An unknown capability value is rejected, never treated as a default. */
static int
test_unknown_cap_rejected(void)
{
    int failures = 0;
    static cap_sink_t sink;
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(dump_with_cap(MOQR_CLI_CAP__COUNT, &sink, &ep),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 0u);
    MOQ_TEST_CHECK_EQ_INT(dump_with_cap(99u, &sink, &ep), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64(sink.calls, 0u);
    return failures;
}

/* Mixed capabilities in one epoch: a VALID lane still renders its shard
 * plane while an ABSENT lane omits its own, and neither poisons. */
static int
test_mixed_caps_render_per_lane(void)
{
    int failures = 0;
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_snapshot_init(&snap, 2u, moq_alloc_default()), MOQR_OK);

    moqr_cli_snapshot_stats_t a;
    memset(&a, 0, sizeof(a));
    a.shard_cap = MOQR_CLI_CAP_VALID;
    a.shard.pump_turns = 1234;
    moqr_cli_snapshot_publish(&snap, 0, &a, 1u);

    moqr_cli_snapshot_stats_t b;
    memset(&b, 0, sizeof(b));
    b.shard_cap = MOQR_CLI_CAP_ABSENT;
    b.shard.pump_turns = 5678;   /* must NOT be rendered */
    moqr_cli_snapshot_publish(&snap, 1, &b, 1u);

    moqr_obs_labels_t labels[2] = { { 0, "msquic", "moqt-18" },
                                    { 1, "wtquic-msquic", "moqt-18" } };
    uint64_t requested = 1u;
    static cap_sink_t sink;
    memset(&sink, 0, sizeof(sink));
    uint64_t ep = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_cli_coord_dump(&snap, labels, epoch_of,
                                              &requested, cap_emit, &sink,
                                              &ep),
                          MOQR_OK);
    MOQ_TEST_CHECK(strstr(sink.doc, "1234") != NULL);
    MOQ_TEST_CHECK(strstr(sink.doc, "5678") == NULL);
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}

/* -- broker ---------------------------------------------------------------- */

/* Slot-aware helpers: the broker describes one generation per bank, so
 * "is anything collecting" and "which serial" are read together. */
static bool
broker_is_collecting(const moqr_broker_t *b)
{
    uint64_t s = 0;
    uint32_t d = 0;
    return moqr_broker_current(b, &s, &d);
}

static uint64_t
broker_collecting_serial(const moqr_broker_t *b)
{
    uint64_t s = 0;
    uint32_t d = 0;
    return moqr_broker_current(b, &s, &d) ? s : 0u;
}

/* First demand opens a generation and asks for a wake; further demand joins
 * it without advancing the serial. */
static int
test_broker_coalesces_without_advancing(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    MOQ_TEST_CHECK(!moqr_broker_busy(&b));

    uint64_t s1 = 0;
    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &wake),
        MOQR_OK);
    MOQ_TEST_CHECK(wake);
    MOQ_TEST_CHECK(s1 != 0);
    MOQ_TEST_CHECK(broker_is_collecting(&b));

    /* Both further demands join: same serial, no second wake storm. */
    for (int i = 0; i < 32; i++) {
        uint64_t s = 0;
        bool w = true;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&b,
                                (i % 2) ? MOQR_BROKER_DEMAND_HTTP
                                        : MOQR_BROKER_DEMAND_SIGNAL,
                                &s, &w),
            MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(s, s1);
        MOQ_TEST_CHECK(!w);
    }
    MOQ_TEST_CHECK_EQ_U64(broker_collecting_serial(&b), s1);

    /* Demand bits accumulate onto the generation. */
    moqr_broker_on_complete(&b, s1);
    MOQ_TEST_CHECK(!broker_is_collecting(&b) && moqr_broker_busy(&b));
    uint64_t got = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &demand, &bank));
    MOQ_TEST_CHECK_EQ_U64(got, s1);
    MOQ_TEST_CHECK_EQ_U64(demand,
                          MOQR_BROKER_DEMAND_SIGNAL | MOQR_BROKER_DEMAND_HTTP);
    return failures;
}

/* Only the sinks that asked receive the generation. */
static int
test_broker_demand_bits_are_per_generation(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t s = 0;
    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s, &wake), MOQR_OK);
    moqr_broker_on_complete(&b, s);

    uint64_t got = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &demand, &bank));
    /* An HTTP-only generation must NOT carry signal demand: that is what
     * would otherwise produce an unsolicited stderr dump per scrape. */
    MOQ_TEST_CHECK_EQ_U64(demand, MOQR_BROKER_DEMAND_HTTP);
    MOQ_TEST_CHECK((demand & MOQR_BROKER_DEMAND_SIGNAL) == 0u);
    return failures;
}

/* A stale completion (for an older serial) must not promote the current
 * generation to READY. */
static int
test_broker_ignores_stale_completion(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    uint64_t s = 0;
    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s, &wake),
        MOQR_OK);
    moqr_broker_on_complete(&b, s - 1u);
    MOQ_TEST_CHECK(broker_is_collecting(&b));
    uint64_t got = 0;
    uint32_t d = 0;
    uint32_t bank = 0;
    MOQ_TEST_CHECK(!moqr_broker_take_ready(&b, &got, &d, &bank));
    return failures;
}

/*
 * Bank accounting: a new generation may start only when a bank is free.
 * With both banks pinned, demand is remembered but no serial is issued --
 * otherwise sustained scrape load would keep re-targeting and never let a
 * generation finish.
 */
/*
 * Bank accounting, and what survives a refusal.
 *
 * With every bank pinned a further demand is refused with no token. Only
 * SIGNAL demand is remembered: nothing re-raises a signal, so dropping it
 * loses the operator's request outright. A refused scrape has already been
 * answered (Step 3 returns 503 immediately), so retaining its bit would open
 * a generation for a client that is no longer there.
 */
static int
test_broker_bank_exhaustion_defers_only_signal(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    /* Pin every bank. */
    uint64_t pinned[MOQR_BROKER_BANKS];
    uint32_t banks[MOQR_BROKER_BANKS];
    for (uint32_t i = 0; i < MOQR_BROKER_BANKS; i++) {
        uint64_t s = 0;
        bool     w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s, &w),
            MOQR_OK);
        MOQ_TEST_CHECK(w);
        moqr_broker_on_complete(&b, s);
        uint64_t got = 0;
        uint32_t d = 0;
        MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &banks[i]));
        pinned[i] = got;
    }

    /* A refused HTTP request leaves NO deferred work behind. */
    uint64_t refused = 12345;
    bool     w2 = true;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &refused, &w2),
        MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(refused, 0u);
    MOQ_TEST_CHECK(!w2);

    bool wake = true;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_release(&b, pinned[0], banks[0], &wake), MOQR_OK);
    /* Nothing was owed, so releasing a bank starts no work. */
    MOQ_TEST_CHECK(!wake);
    MOQ_TEST_CHECK(!broker_is_collecting(&b));

    /* A refused SIGNAL request IS remembered, and the next release honours it. */
    uint64_t refused2 = 999;
    bool     w3 = true;
    /* Re-pin the free bank so the broker is exhausted again. */
    {
        uint64_t s = 0;
        bool     w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s, &w),
            MOQR_OK);
        moqr_broker_on_complete(&b, s);
        uint64_t got = 0;
        uint32_t d = 0;
        MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &banks[0]));
        pinned[0] = got;
    }
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &refused2, &w3),
        MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(refused2, 0u);
    MOQ_TEST_CHECK(!w3);

    bool wake2 = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_release(&b, pinned[1], banks[1], &wake2), MOQR_OK);
    MOQ_TEST_CHECK(wake2);
    MOQ_TEST_CHECK(broker_is_collecting(&b));
    /* And the generation it opened carries only the signal demand. */
    uint64_t cs = 0;
    uint32_t cd = 0;
    MOQ_TEST_CHECK(moqr_broker_current(&b, &cs, &cd));
    MOQ_TEST_CHECK_EQ_U64(cd, MOQR_BROKER_DEMAND_SIGNAL);
    moqr_broker_destroy(&b);
    return failures;
}

/* Sustained demand must not starve completion: the serial advances only when
 * a generation actually finishes, never once per arriving request. */
static int
test_broker_load_does_not_starve_completion(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t first = 0;
    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &first, &wake),
        MOQR_OK);
    for (int i = 0; i < 10000; i++) {
        uint64_t s = 0;
        bool w = true;
        (void)moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s, &w);
    }
    /* Ten thousand arrivals, still the same generation. */
    MOQ_TEST_CHECK_EQ_U64(broker_collecting_serial(&b), first);
    moqr_broker_on_complete(&b, first);
    MOQ_TEST_CHECK(!broker_is_collecting(&b) && moqr_broker_busy(&b));
    return failures;
}

static int
test_broker_rejects_bad_args(void)
{
    int failures = 0;
    moqr_broker_t b;
    uint64_t s = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(NULL, 2u), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, 0u), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_request(&b, 0u, &s, &w),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_request(&b, 0xffu, &s, &w),
                          MOQR_ERR_INVAL);
    return failures;
}


/* -- F1: a READY generation must survive a later request ------------------ */

static int
test_ready_generation_is_not_overwritten(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t s1 = 0, s2 = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s1);

    /* A second request may open a new generation on the free bank, but it
     * must NOT destroy the one already waiting to be delivered. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    MOQ_TEST_CHECK(s2 != s1);

    uint64_t got = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &demand, &bank));
    MOQ_TEST_CHECK_EQ_U64(got, s1);
    MOQ_TEST_CHECK_EQ_U64(demand, MOQR_BROKER_DEMAND_SIGNAL);
    moqr_broker_destroy(&b);
    return failures;
}

/* -- F2: bank ownership is authenticated ---------------------------------- */

static int
test_release_is_authenticated(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t s1 = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s1);
    uint64_t got = 0;
    uint32_t d = 0, bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &bank));

    /* An unknown serial must not free anyone's bank. */
    bool wake = true;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_release(&b, 0xdeadbeefu, bank, &wake), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(!wake);

    /* Nor a mismatched bank index. */
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank + 7u, &wake),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(!wake);

    /* F7: the wake result is required -- it is the only notice a deferred
     * generation's lanes would ever get. */
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank, NULL),
                          MOQR_ERR_INVAL);
    /* ...and refusing it mutated nothing, so the rightful release still works. */
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank, &wake), MOQR_OK);

    /* A duplicate release must fail closed. */
    bool wake2 = true;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank, &wake2),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(!wake2);
    moqr_broker_destroy(&b);
    return failures;
}

/* Releasing a generation that is still COLLECTING is not a release at all. */
static int
test_release_rejects_collecting_generation(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    uint64_t s = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    bool wake = true;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s, 0u, &wake),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(!wake);
    /* Still collectable afterwards. */
    moqr_broker_on_complete(&b, s);
    uint64_t got = 0;
    uint32_t d = 0, bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &bank));
    MOQ_TEST_CHECK_EQ_U64(got, s);
    moqr_broker_destroy(&b);
    return failures;
}

/* -- F3: dispatch uses the frozen token, including a late join ------------ */

static int
test_late_join_is_served_by_the_token(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t s = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);

    /* An early observation of the demand -- the kind a renderer would make
     * before producing. */
    uint64_t obs_serial = 0;
    uint32_t obs_demand = 0;
    MOQ_TEST_CHECK(moqr_broker_current(&b, &obs_serial, &obs_demand));
    MOQ_TEST_CHECK_EQ_U64(obs_serial, s);
    MOQ_TEST_CHECK_EQ_U64(obs_demand, MOQR_BROKER_DEMAND_SIGNAL);

    /* A scrape joins while the generation is still collecting. */
    uint64_t s2 = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(s2, s);

    moqr_broker_on_complete(&b, s);

    /* The delivery token carries BOTH demands: dispatching from the early
     * observation would have silently dropped the joiner. */
    uint64_t got = 0;
    uint32_t demand = 0, bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &demand, &bank));
    MOQ_TEST_CHECK_EQ_U64(got, s);
    MOQ_TEST_CHECK_EQ_U64(demand,
                          MOQR_BROKER_DEMAND_SIGNAL | MOQR_BROKER_DEMAND_HTTP);
    MOQ_TEST_CHECK(demand != obs_demand);
    moqr_broker_destroy(&b);
    return failures;
}

/* A join arriving after the generation is frozen gets its own generation. */
static int
test_join_after_ready_opens_a_new_generation(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    uint64_t s1 = 0, s2 = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s1);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    MOQ_TEST_CHECK(s2 != s1);

    uint64_t got = 0;
    uint32_t d = 0, bank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &bank));
    MOQ_TEST_CHECK_EQ_U64(got, s1);
    MOQ_TEST_CHECK_EQ_U64(d, MOQR_BROKER_DEMAND_SIGNAL);

    /* The HTTP generation stays isolated. */
    moqr_broker_on_complete(&b, s2);
    uint64_t got2 = 0;
    uint32_t d2 = 0, bank2 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got2, &d2, &bank2));
    MOQ_TEST_CHECK_EQ_U64(got2, s2);
    MOQ_TEST_CHECK_EQ_U64(d2, MOQR_BROKER_DEMAND_HTTP);
    MOQ_TEST_CHECK(bank2 != bank);
    moqr_broker_destroy(&b);
    return failures;
}

/* -- F6: serials stay distinct across the 32-bit boundary ----------------- */

static int
test_serials_survive_the_32bit_boundary(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    moqr_broker_test_seed_serial(&b, (uint64_t)UINT32_MAX);

    uint64_t seen[4];
    for (int i = 0; i < 4; i++) {
        uint64_t s = 0;
        bool w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w),
            MOQR_OK);
        seen[i] = s;
        moqr_broker_on_complete(&b, s);
        uint64_t got = 0;
        uint32_t d = 0, bank = 0;
        MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &bank));
        MOQ_TEST_CHECK_EQ_U64(got, s);
        bool wake = false;
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s, bank, &wake),
                              MOQR_OK);
    }
    for (int i = 0; i < 4; i++) {
        for (int j = i + 1; j < 4; j++) {
            MOQ_TEST_CHECK(seen[i] != seen[j]);
        }
    }
    /* The sequence really did cross the boundary, and the values above it are
     * carried whole. Truncating any of them to 32 bits would alias onto the
     * small serials a fresh broker hands out, which is what a `unsigned`
     * rendered-generation variable would then confuse. */
    MOQ_TEST_CHECK(seen[3] > (uint64_t)UINT32_MAX);
    MOQ_TEST_CHECK((uint32_t)seen[1] == 0u);
    MOQ_TEST_CHECK(seen[1] != (uint64_t)(uint32_t)seen[1]);
    moqr_broker_destroy(&b);
    return failures;
}



/*
 * `take_ready` is the DRAIN api: it hands back the oldest frozen generation,
 * never the lowest bank index. Banks are reused, so a newer generation can
 * land on a lower index than an older one still awaiting delivery; picking by
 * index would deliver the newer document and strand the older slot.
 */
static int
test_take_ready_is_serial_ordered(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    uint64_t s1 = 0, s2 = 0, s3 = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s1);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s2);
    MOQ_TEST_CHECK(s2 > s1);

    uint64_t got = 0;
    uint32_t d = 0, bank1 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got, &d, &bank1));
    MOQ_TEST_CHECK_EQ_U64(got, s1);
    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank1, &wake), MOQR_OK);

    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s3, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s3);
    MOQ_TEST_CHECK(s3 > s2);

    uint64_t got2 = 0;
    uint32_t d2 = 0, bank2 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got2, &d2, &bank2));
    MOQ_TEST_CHECK_EQ_U64(got2, s2);
    MOQ_TEST_CHECK_EQ_U64(d2, MOQR_BROKER_DEMAND_HTTP);

    bool wake2 = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, got2, bank2, &wake2),
                          MOQR_OK);
    uint64_t got3 = 0;
    uint32_t d3 = 0, bank3 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_ready(&b, &got3, &d3, &bank3));
    MOQ_TEST_CHECK_EQ_U64(got3, s3);
    moqr_broker_destroy(&b);
    return failures;
}

/* take_serial takes ONLY the named generation, and refuses anything else. */
static int
test_take_serial_is_exact(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    uint64_t s1 = 0, s2 = 0, s3 = 0;
    bool w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s1);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    moqr_broker_on_complete(&b, s2);

    uint32_t d = 0, bank = 0;
    /* The NEWER one, while an older is also ready. */
    MOQ_TEST_CHECK(moqr_broker_take_serial(&b, s2, &d, &bank));
    MOQ_TEST_CHECK_EQ_U64(d, MOQR_BROKER_DEMAND_HTTP);
    MOQ_TEST_CHECK(!moqr_broker_take_serial(&b, 0xdeadbeefu, &d, &bank));
    MOQ_TEST_CHECK(!moqr_broker_take_serial(&b, s2, &d, &bank));

    bool wake = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s2, bank, &wake), MOQR_OK);
    uint32_t d1 = 0, bank1 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_serial(&b, s1, &d1, &bank1));
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s1, bank1, &wake), MOQR_OK);
    /* A still-collecting generation is not takeable. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&b, MOQR_BROKER_DEMAND_SIGNAL, &s3, &w), MOQR_OK);
    uint32_t d3 = 0, bank3 = 0;
    MOQ_TEST_CHECK(!moqr_broker_take_serial(&b, s3, &d3, &bank3));
    moqr_broker_destroy(&b);
    return failures;
}

/* -- F4: a live pump-shaped reader must never see a half-built broker -----
 *
 * No cooperative wait, and therefore no way to hang. The coordinator spawns
 * the readers for each generation and JOINS them, and a join always returns —
 * including for a worker that returned before executing anything. A worker
 * that does no work is then a ledger failure, not a stall, so no correctness
 * verdict here depends on elapsed time.
 *
 * Overlap is still real: all four readers run concurrently against the live
 * broker for every generation, which is the property TSan exercises.
 */

#define PUMP_THREADS 4
#define PUMP_GENS    16

/*
 * Emitted only after the concurrency test has finished every generation, every
 * per-worker ledger check and its cleanup. Where a statement is written proves
 * nothing about whether execution reaches it -- an ordinary `return 0;` above
 * the call, or at the top of the function, leaves the source canonical and the
 * work undone. The receipt is the evidence that the body actually ran.
 */
#define PUMP_RECEIPT "RECEIPT concurrent_pumps completed"

typedef struct {
    moqr_broker_t       *b;
    moqr_cli_snapshot_t *snap;
    uint32_t             lane;
    uint64_t             serial;      /* the generation to publish for   */
    /* Ledger, accumulated across generations. */
    unsigned             reads;
    unsigned             publishes;
    unsigned             gens_seen;
    uint64_t             last_serial;
    unsigned             rounds_entered;
} pump_arg_t;

/* Exactly what a lane pump does per turn. */
static void
pump_body(pump_arg_t *a)
{
    uint64_t observed = moqr_broker_epoch_fn(a->b);
    a->reads++;
    if (observed != 0u && observed != a->last_serial) {
        /* Serials only advance for a reader; the final value is compared
         * against the last generation issued, which a stale or repeated
         * observation fails. */
        a->last_serial = observed;
        a->gens_seen++;
    }
    if (observed == a->serial) {
        moqr_cli_snapshot_stats_t st;
        memset(&st, 0, sizeof(st));
        st.shard_cap = MOQR_CLI_CAP_ABSENT;
        moqr_cli_snapshot_publish(a->snap, a->lane, &st, a->serial);
        a->publishes++;
    }
}

static void *
pump_worker(void *v)
{
    pump_arg_t *a = v;
    a->rounds_entered++;
    pump_body(a);
    return NULL;
}

/*
 * The cohort is created and joined in two dedicated helpers, each holding the
 * ONLY raw pthread call of its kind in this file. check_pump_overlap.sh pins
 * that call graph as well as the calls: `join_cohort` is defined once and
 * called once, from the phase after `spawn_cohort` returns. Joining a reader
 * before the next is created -- directly or through a helper -- is a queue,
 * not a cohort, and nothing at runtime tells the two apart without making
 * elapsed time the verdict.
 *
 * `spawn_cohort` reports how many threads it actually created. A failed
 * create leaves its `pthread_t` slot uninitialised, so joining it would be
 * undefined; the caller joins exactly the created prefix.
 */
static int
spawn_cohort(pthread_t *th, pump_arg_t *args, int *out_created)
{
    int failures = 0;
    int created = 0;
    for (int i = 0; i < PUMP_THREADS; i++) {
        int rc = pthread_create(&th[i], NULL, pump_worker, &args[i]);
        MOQ_TEST_CHECK_EQ_INT(rc, 0);
        if (rc != 0) {
            break;
        }
        created++;
    }
    *out_created = created;
    return failures;
}

/* A join always returns, so a worker that did nothing cannot stall the run;
 * it shows up in the ledger instead. */
static int
join_cohort(pthread_t *th, int created)
{
    int failures = 0;
    for (int i = 0; i < created; i++) {
        MOQ_TEST_CHECK_EQ_INT(pthread_join(th[i], NULL), 0);
    }
    return failures;
}

static int
test_broker_survives_concurrent_pumps(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, PUMP_THREADS, moq_alloc_default()),
        MOQR_OK);

    pump_arg_t args[PUMP_THREADS];
    for (int i = 0; i < PUMP_THREADS; i++) {
        memset(&args[i], 0, sizeof(args[i]));
        args[i].b = &b;
        args[i].snap = &snap;
        args[i].lane = (uint32_t)i;
    }

    unsigned delivered = 0;
    uint64_t last_issued = 0;
    for (unsigned g = 0; g < PUMP_GENS; g++) {
        uint64_t s = 0;
        bool     w = false;
        uint32_t want = (g % 2u) ? MOQR_BROKER_DEMAND_HTTP
                                 : MOQR_BROKER_DEMAND_SIGNAL;
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_request(&b, want, &s, &w), MOQR_OK);
        MOQ_TEST_CHECK(w);
        last_issued = s;

        for (int i = 0; i < PUMP_THREADS; i++) {
            args[i].serial = s;
        }
        /* Whole cohort in flight, then the whole cohort joined. */
        pthread_t th[PUMP_THREADS];
        int       created = 0;
        failures += spawn_cohort(th, args, &created);
        MOQ_TEST_CHECK_EQ_INT(created, PUMP_THREADS);
        failures += join_cohort(th, created);

        moqr_broker_on_complete(&b, s);
        uint32_t d = 0, bank = 0;
        if (moqr_broker_take_serial(&b, s, &d, &bank)) {
            MOQ_TEST_CHECK_EQ_U64(d, want);
            bool wake = false;
            MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s, bank, &wake),
                                  MOQR_OK);
            /* Exactly once: a second release of the same token fails closed. */
            MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&b, s, bank, &wake),
                                  MOQR_ERR_INVAL);
            delivered++;
        }
    }

    /* Every generation collected and delivered... */
    MOQ_TEST_CHECK_EQ_U64(delivered, PUMP_GENS);
    /* ...and each worker was live for every generation, read the broker,
     * published its row, saw each generation exactly once, and ended on the
     * last serial issued. Each is an independent failure. */
    for (int i = 0; i < PUMP_THREADS; i++) {
        MOQ_TEST_CHECK_EQ_U64(args[i].rounds_entered, PUMP_GENS);
        MOQ_TEST_CHECK_EQ_U64(args[i].reads, PUMP_GENS);
        MOQ_TEST_CHECK_EQ_U64(args[i].publishes, PUMP_GENS);
        MOQ_TEST_CHECK_EQ_U64(args[i].gens_seen, PUMP_GENS);
        MOQ_TEST_CHECK_EQ_U64(args[i].last_serial, last_issued);
    }

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(&b);
    printf("%s gens=%u threads=%u\n", PUMP_RECEIPT, (unsigned)PUMP_GENS,
           (unsigned)PUMP_THREADS);
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_cap_valid_renders_shard_plane();
    failures += test_cap_absent_omits_shard_plane();
    failures += test_cap_refused_poisons();
    failures += test_zero_initialized_row_poisons();
    failures += test_unknown_cap_rejected();
    failures += test_mixed_caps_render_per_lane();
    failures += test_broker_coalesces_without_advancing();
    failures += test_broker_demand_bits_are_per_generation();
    failures += test_broker_ignores_stale_completion();
    failures += test_broker_bank_exhaustion_defers_only_signal();
    failures += test_broker_load_does_not_starve_completion();
    failures += test_broker_rejects_bad_args();
    failures += test_ready_generation_is_not_overwritten();
    failures += test_take_ready_is_serial_ordered();
    failures += test_take_serial_is_exact();
    failures += test_release_is_authenticated();
    failures += test_release_rejects_collecting_generation();
    failures += test_late_join_is_served_by_the_token();
    failures += test_join_after_ready_opens_a_new_generation();
    failures += test_serials_survive_the_32bit_boundary();
    failures += test_broker_survives_concurrent_pumps();
    if (failures != 0) {
        fprintf(stderr, "test_relay_snapshot_cap: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_relay_snapshot_cap: OK\n");
    return 0;
}
