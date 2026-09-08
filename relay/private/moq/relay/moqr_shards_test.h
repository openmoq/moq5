#ifndef MOQR_SHARDS_TEST_H
#define MOQR_SHARDS_TEST_H
#include <moq/relay/moqr_shards.h>
#ifdef __cplusplus
extern "C" {
#endif
/* -- test/debug introspection of the cross-shard control plane -------------- */

typedef struct moqr_shards_jinfo {
    bool     present;      /* the shard's journal has an entry for this ns */
    uint64_t candidates;   /* live origin bitset (bit i = shard i announces it) */
    int32_t  winner;       /* HRW winner over the candidates, or -1 */
    int32_t  mirror;       /* installed PB-mirror origin, or -1 (none) */
    bool     holdout;      /* a local loser whose force-withdrawal is deferred
                            * (WOULD_BLOCK); clears once the withdrawal lands */
} moqr_shards_jinfo_t;

/* Read a shard manager's candidate-set journal entry for a namespace. `present`
 * is false at K == 1 (no manager) or when the namespace is absent. */
void moqr_shards_debug_journal(moqr_shards_t *s, uint16_t shard,
                               const moq_bytes_t *parts, uint32_t count,
                               moqr_shards_jinfo_t *out);

/* The HRW winner over a candidate bitset for a namespace (pure) — lets a test
 * pick a namespace a particular shard owns. -1 when no candidates. */
int32_t moqr_shards_debug_hrw_winner(uint64_t candidates,
                                     const moq_bytes_t *parts, uint32_t count);

/* Count of self-echoes a shard's manager still expects from its own mirror ops
 * (0 at K == 1). At a converged fixed point this is 0 — every mirror
 * announce/unannounce echo has been recognised and consumed, so a nonzero
 * steady-state value means a mirror echo was misclassified as a real event (or
 * vice-versa). */
uint32_t moqr_shards_debug_pending_tokens(moqr_shards_t *s, uint16_t shard);

/* Occupied slots in the directed src→dst control mailbox (0 at K == 1). A
 * quiesced runtime has none; a value that never drains signals a stuck mailbox. */
uint32_t moqr_shards_debug_mailbox_pending(moqr_shards_t *s, uint16_t src,
                                           uint16_t dst);

/* Count of remote-owner subscribe demands resolved as refused (0 at K == 1).
 * Counted once per attempt, WHEN THE REQUESTER RESOLVES THE OWNER'S ANSWER —
 * a demand refused locally (no live mirror) or cancelled in flight does not
 * count, so a nonzero value proves the round-trip completed. */
uint64_t moqr_shards_debug_remote_demand_refused(moqr_shards_t *s, uint16_t shard);

/* Remote-owner demands recorded but not yet resolved (any state: recorded,
 * forwarded, acknowledged-active, or awaiting the cancel notice). 0 at
 * K == 1. An acknowledged demand stays counted for as long as its track is
 * ACTIVE — the entry is the cancel notice's durable home. */
uint32_t moqr_shards_debug_pending_demand(moqr_shards_t *s, uint16_t shard);

/* Count of owner DONE answers this shard resolved into its own core as the
 * REQUESTER (pre-ACK refusals and post-ACK terminals both), with the last
 * resolution's error code (verbatim from the owner) and whether it was
 * pre-ACK. 0 at K == 1. */
uint64_t moqr_shards_debug_remote_demand_resolved(moqr_shards_t *s,
                                                  uint16_t shard,
                                                  uint64_t *last_code,
                                                  bool *last_pre_ack);

/* The complete tagged PUBLISH_DONE descriptor of the last owner DONE this
 * shard resolved as the REQUESTER — tag, 64-bit value and origin profile, not
 * just the wire number that `..._resolved` reports. Returns false, leaving
 * *out untouched, until this shard has resolved at least one DONE, and for
 * invalid inputs. A pre-ACK refusal resolves with NONE: that path is
 * REQUEST_ERROR-domain and states no terminal. */
bool moqr_shards_debug_remote_demand_last_pd(moqr_shards_t *s, uint16_t shard,
                                             moqr_pd_desc_t *out);

/* The tagged terminal carried by the head message of the directed src->dst
 * demand channel, read WITHOUT consuming it — what a re-peek sees. false when
 * the head is absent or is not a DONE. */
bool moqr_shards_debug_demand_channel_head_pd(moqr_shards_t *s, uint16_t src,
                                              uint16_t dst,
                                              moqr_pd_desc_t *out);

/* Admitted remote demands live on this shard as the OWNER (pump-subs whose
 * lifecycle is still open). 0 at K == 1, with admission off, and at any
 * quiesced fixed point. */
uint32_t moqr_shards_debug_owner_pump_subs(moqr_shards_t *s, uint16_t shard);

/* Queued LOGICAL bytes in the directed src→dst demand channel (payload +
 * properties + control canonical keys; 0 at K == 1). A quiesced runtime has
 * none. */
uint64_t moqr_shards_debug_demand_channel_bytes(moqr_shards_t *s,
                                                uint16_t src, uint16_t dst);

/* Data-pump counters for one shard as the OWNER: turns (data phases that
 * attempted extraction), messages pushed, and logical bytes pushed. All 0 at
 * K == 1 and with admission off. */
void moqr_shards_debug_pump_counters(moqr_shards_t *s, uint16_t shard,
                                     uint64_t *turns, uint64_t *messages,
                                     uint64_t *bytes);

/* Data records this shard as the REQUESTER refused at ingest for its own
 * retention horizon (TOO_OLD): counted, never a hidden drop. */
uint64_t moqr_shards_debug_remote_data_rejected(moqr_shards_t *s,
                                                uint16_t shard);

/* Remote demands this shard terminated for a local capacity edge (the
 * demand-terminal path; wire code INTERNAL 0x0). */
uint64_t moqr_shards_debug_remote_demand_term_capacity(moqr_shards_t *s,
                                                       uint16_t shard);

/* Remote demands this shard terminated because a subgroup-progress table
 * filled (distinct live subgroups beyond pump_subgroup_slots; INTERNAL 0x0,
 * distinguished from ingest-capacity terminals by this metric only). */
uint64_t moqr_shards_debug_remote_demand_term_overrun(moqr_shards_t *s,
                                                      uint16_t shard);

/* Live subgroup-progress slots on this shard: as the OWNER (chunk-resume
 * cursors across its admitted demands) and as the REQUESTER (open-object
 * bookkeeping across its pending demands). Both 0 at K == 1, with admission
 * off, and at any quiesced fixed point with no live streams. */
uint32_t moqr_shards_debug_owner_progress_slots(moqr_shards_t *s,
                                                uint16_t shard);
uint32_t moqr_shards_debug_requester_open_objects(moqr_shards_t *s,
                                                  uint16_t shard);

/* Occupied slots in the directed src→dst DEMAND channel (0 at K == 1). A
 * quiesced runtime has none. */
uint32_t moqr_shards_debug_demand_channel_pending(moqr_shards_t *s,
                                                  uint16_t src, uint16_t dst);

/* The {track, *track_gen} of pending-demand entry `idx` on this shard's manager
 * (idx < pending_demand). For tests that drive the recorded track out of PENDING
 * to exercise the refuse drain's stale / wrong-state moot handling. Returns
 * MOQR_TRACK_INVALID when out of range. */
moqr_track_t moqr_shards_debug_pending_demand_track(moqr_shards_t *s,
                                                    uint16_t shard, uint32_t idx,
                                                    uint64_t *track_gen);

/* Test alias for moqr_shards_step_shard (same contract, incl. the sticky
 * fail-stop → MOQR_ERR_NOMEM). Kept so existing tests that compose hand-rolled
 * passes with debug_round_advance keep reading clearly as test scaffolding. */
moqr_result_t moqr_shards_debug_step_shard(moqr_shards_t *s, uint16_t shard,
                                           uint64_t now_us,
                                           uint64_t *pushed_dst_mask);

/* Advance the round barrier exactly once, after every shard has stepped:
 * pushes stamped this round become visible next round. */
void moqr_shards_debug_round_advance(moqr_shards_t *s);

/* Inbound visibility policy. false (default): the deterministic round barrier
 * (pushed in round r ⇒ visible at r+1 under moqr_shards_step). true: a push is
 * visible as soon as the mailbox mutex publishes it — for tests free-running
 * per-shard steppers on their own threads, where no shared round exists (the
 * round counter is then never advanced, so it is never written concurrently).
 * Toggle only while quiesced: no stepper may be running. */
void moqr_shards_debug_set_live_visibility(moqr_shards_t *s, bool live);

#ifdef MOQR_BIND_TESTING
/* Test-only: latch this shard's manager fail-stop exactly as an unrecordable
 * borrowed observation does — sticky, never cleared — so a test can activate
 * the step-failure branch deterministically, with no traffic and no capacity
 * squeeze. No-op when the shard has no manager (K == 1). */
void moqr_shards_debug_fail_stop(moqr_shards_t *s, uint16_t shard);

/* Verify-only SG_SEAL ingest evidence (see moqr_shards.c seal_log): one
 * entry per seal this REQUESTER shard applied to its core, in ingest order. */
typedef struct moqr_shards_seal_ev {
    uint64_t seq;        /* lifetime ingest order on this shard */
    uint16_t src;        /* owner lane the seal crossed from    */
    uint64_t demand_id;  /* exact demand attribution            */
    uint64_t group_id;
    uint64_t subgroup_id;
} moqr_shards_seal_ev_t;

/* Copies the newest window, oldest first; *out_total (nullable) receives the
 * LIFETIME ingest count — total > returned means the ring overwrote evidence
 * (the fail-closed acceptance rejects such a record). */
uint32_t moqr_shards_debug_seal_log(const moqr_shards_t *s, uint16_t shard,
                                    moqr_shards_seal_ev_t *out, uint32_t cap,
                                    uint64_t *out_total);

/* Verify-only: resolve a REQUESTER-side track (raw handle + generation, as
 * captured by moqr_bind_debug_conn_bind_sg_track at a BIND_SG refusal) to
 * the demand id feeding it, by the shard's own pending-demand table — the
 * table is keyed by track, which is why the track is the join key between
 * the blocked conn and the SEALLOG's demand attribution. Returns 0 when the
 * shard is bad or no live demand matches (a purely LOCAL track has no
 * demand — the cross-shard acceptance fails closed on 0). */
uint64_t moqr_shards_debug_track_demand(const moqr_shards_t *s,
                                        uint16_t shard, uint64_t track_raw,
                                        uint64_t track_gen);

/* Verify-only invariant-breaker pair for the duplicate-resolution pin:
 * _pend_dup clones the pending-demand entry for `demand_id` under
 * `new_demand_id` (own canon copy, no shared progress row — false on
 * missing source / id collision / full table / OOM); _pend_drop retires the
 * entry for `demand_id` again. Together they prove the resolver above
 * returns 0 on a duplicate {track, gen} instead of silently choosing. */
bool moqr_shards_debug_pend_dup(moqr_shards_t *s, uint16_t shard,
                                uint64_t demand_id, uint64_t new_demand_id);
bool moqr_shards_debug_pend_drop(moqr_shards_t *s, uint16_t shard,
                                 uint64_t demand_id);
#endif


#ifdef __cplusplus
}
#endif
#endif
