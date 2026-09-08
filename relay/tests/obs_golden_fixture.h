#ifndef MOQR_OBS_GOLDEN_FIXTURE_H
#define MOQR_OBS_GOLDEN_FIXTURE_H

/*
 * The snapshot behind the frozen legacy goldens.
 *
 * This header is compiled twice against two different trees: once against the
 * accepted base to MINT the golden bytes, and once against the working tree to
 * CHECK them. It therefore may only touch stat fields that exist in both, and
 * every value here must stay literal — a value derived from anything outside
 * this header would let the two builds disagree silently and turn the golden
 * into a tautology.
 */

#include <stdint.h>
#include <string.h>

static void
moqr_golden_core(moqr_core_stats_t *cs)
{
    memset(cs, 0, sizeof(*cs));
    cs->ingested_total = 1234567890123456789ull;
    cs->delivered_total = 987654321098765432ull;
    cs->evicted_total = 4242;
    cs->bindings = 300;
    cs->tracks = 55;
    cs->subs = 88;
    cs->subs_parked = 20;
    cs->subs_active = 60;
    cs->ns_nodes = 41;
    cs->ns_subs = 27;
    cs->retained_bytes = 8388608;
    cs->intent_highwater = 91;
    cs->route_epoch = 424242;
    cs->refusals[MOQR_REFUSE_SUBS] = 31;
    cs->refusals[MOQR_REFUSE_NAME_BYTES] = 17;
    cs->auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW] = 120;
    cs->auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] = 33;
    cs->auth_decisions[MOQR_AUTH_PUBLISH][MOQR_AUTH_DEFER] = 11;
    cs->auth_denials[MOQR_AUTH_REASON_UNSCOPED] = 33;
    cs->auth_denials[MOQR_AUTH_REASON_EXPIRED] = 5;
}

static void
moqr_golden_bind(moqr_bind_stats_t *bs)
{
    memset(bs, 0, sizeof(*bs));
    bs->conns = 30;
    bs->events_translated = 500000;
    bs->deliveries_written = 250000;
    bs->ingest_refusals = 44;
    bs->session_errors = 12;
}

static void
moqr_golden_shard(moqr_shards_stats_t *sh)
{
    memset(sh, 0, sizeof(*sh));
    sh->wake_requests_push = 700;
    sh->wake_requests_credit = 800;
    sh->wake_requests_local = 900;
    sh->pump_turns = 10000;
    sh->journal_epoch = 777;
    sh->channel_entries_hwm = 64;
    sh->channel_bytes_hwm = 65536;
    /* Internals stay strictly below their core counterparts so the
     * exclusion invariant holds and the exposition is not suppressed. */
    sh->pump_subs_parked = 5;
    sh->pump_subs_active = 15;
    sh->internal_bindings = 7;
    sh->internal_ns_subs = 3;
}

/* Two views with distinct labels, matching the dual-listener shape. */
static void
moqr_golden_views(moqr_snapshot_view_t *vs, const moqr_core_stats_t *cs,
                  const moqr_bind_stats_t *bs, const moqr_shards_stats_t *sh)
{
    memset(vs, 0, sizeof(*vs) * 2u);
    vs[0].core = cs;
    vs[0].bind = bs;
    vs[0].shard = sh;
    vs[0].lane_wakes = 1500;
    vs[0].labels.shard = 0;
    vs[0].labels.transport = "msquic";
    vs[0].labels.version = "moqt-18+moqt-16";
    vs[1].core = cs;
    vs[1].bind = bs;
    vs[1].shard = sh;
    vs[1].lane_wakes = 2500;
    vs[1].labels.shard = 1;
    vs[1].labels.transport = "wtquic-msquic";
    vs[1].labels.version = "moqt-18+moqt-16";
}

#endif /* MOQR_OBS_GOLDEN_FIXTURE_H */
