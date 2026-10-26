/* The handle registry's behavioral oracle.
 *
 * It compiles against the PRODUCTION registry source, never a model: a model
 * would prove only that the model is self-consistent. Everything below is a
 * statement about lifetime behaviour that must hold before any pointer is
 * exposed to Kotlin.
 *
 * Concurrency is driven by explicit barriers, never by sleeps: a sleep makes a
 * race likely rather than certain, and a test that is only usually right is not
 * evidence.
 */
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "../moq5_registry.h"
#include "../moq5_registry_testing.h"

static int checks;
static int failures;

/* Every failure is FLUSHED immediately.
 *
 * A mutant that deadlocks later would otherwise take the evidence with it: the
 * process is killed with the buffer unwritten, and a named failure that had
 * already been recorded is lost, leaving only "HUNG". */
static void ck(int cond, const char *what)
{
    checks++;
    if (!cond) {
        failures++;
        printf("FAIL[%s]\n", what);
        fflush(stdout);
    }
}

/* Full-width unsigned comparison, so a token above INT64_MAX is diagnosed
 * without being squeezed through a signed type first. */
static void ck_u64_eq(uint64_t got, uint64_t want, const char *what)
{
    checks++;
    if (got != want) {
        failures++;
        printf("FAIL[%s]: got 0x%016llx, want 0x%016llx\n", what,
               (unsigned long long)got, (unsigned long long)want);
        fflush(stdout);
    }
}

static void ck_eq(long long got, long long want, const char *what)
{
    checks++;
    if (got != want) {
        failures++;
        printf("FAIL[%s]: got %lld, want %lld\n", what, got, want);
        fflush(stdout);
    }
}


/* An expected REFUSAL.
 *
 * If the call unexpectedly succeeds it has taken a pin, and leaving that pin
 * held would make the next retirement block forever -- turning a mutant into a
 * hang instead of an attributable failure. So the pin is released here and the
 * mismatch is reported normally. */
static void ck_refused(moq5_reg_result_t got, moq5_reg_result_t want,
                       moq5_handle_t h, const char *what)
{
    checks++;
    if (got != want) {
        failures++;
        printf("FAIL[%s]: got %d, want %d\n", what, (int)got, (int)want);
        fflush(stdout);
        if (got == MOQ5_REG_OK) {
            moq5_reg_release(h);
        }
    }
}

/* Every pthread result is CHECKED and fails closed.
 *
 * Discarding them meant a fixture could continue through invalid
 * synchronization state and then hang or read garbage, which is exactly what a
 * mutant must never be able to hide behind (chats/libmoq/1236 F1). A failure
 * here is named, flushed and terminal: there is no safe way to continue once a
 * barrier or thread could not be created. */
static void pt_must(int rc, const char *what)
{
    if (rc != 0) {
        failures++;
        printf("FAIL[pthread.%s]: rc=%d\n", what, rc);
        fflush(stdout);
        printf("\nregistry oracle: %d check(s), %d failure(s)\n", checks, failures);
        fflush(stdout);
        _exit(1);
    }
}

/* -- a barrier, because macOS has no pthread_barrier_t ---------------------- */
typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    unsigned needed;
    unsigned seen;
    unsigned round;
} barrier_t;

static void barrier_init(barrier_t *b, unsigned needed)
{
    pt_must(pthread_mutex_init(&b->mu, NULL), "barrier.mutex_init");
    pt_must(pthread_cond_init(&b->cv, NULL), "barrier.cond_init");
    b->needed = needed;
    b->seen = 0;
    b->round = 0;
}

static void barrier_destroy(barrier_t *b)
{
    pt_must(pthread_mutex_destroy(&b->mu), "barrier.mutex_destroy");
    pt_must(pthread_cond_destroy(&b->cv), "barrier.cond_destroy");
}

static void barrier_wait(barrier_t *b)
{
    pt_must(pthread_mutex_lock(&b->mu), "barrier.lock");
    unsigned r = b->round;
    if (++b->seen == b->needed) {
        b->seen = 0;
        b->round++;
        pt_must(pthread_cond_broadcast(&b->cv), "barrier.broadcast");
    } else {
        while (r == b->round) {
            pt_must(pthread_cond_wait(&b->cv, &b->mu), "barrier.wait");
        }
    }
    pt_must(pthread_mutex_unlock(&b->mu), "barrier.unlock");
}

/* -- fake resources, so the generic rule is proven without guessing the
 *    eventual endpoint/receiver/sender mapping ------------------------------ */
typedef struct {
    int id;
    int cleaned;
} fake_res_t;

static int cleanup_order[64];
static int cleanup_count;

/* Lock observation is STICKY and counted.
 *
 * A single flag was overwritten by every callback, so a violation on an early
 * child could be erased by a later well-behaved one (chats/libmoq/1234 F2).
 * `cleanup_lock_free_count` records how many callbacks observed a free lock and
 * `cleanup_lock_violations` never decreases. */
static int cleanup_lock_free_count;
static int cleanup_lock_violations;

static void observe_lock_free(void)
{
    if (moq5_reg_test_lock_is_free()) {
        cleanup_lock_free_count++;
    } else {
        cleanup_lock_violations++;
    }
}

static void fake_cleanup(void *p)
{
    fake_res_t *r = (fake_res_t *)p;
    r->cleaned++;
    if (cleanup_count < (int)(sizeof cleanup_order / sizeof cleanup_order[0])) {
        cleanup_order[cleanup_count] = r->id;
    }
    cleanup_count++;

    /* The lock must NOT be held across this callback. A trylock answers that
     * immediately and cannot hang; a re-entrant call alone would deadlock
     * rather than report. */
    observe_lock_free();
}

/* -- token codecs ----------------------------------------------------------- */
static void test_token_codec(void)
{
    moq5_handle_t h = moq5_handle_make(MOQ5_KIND_ROOT, 5u, MOQ5_HANDLE_GEN_FIRST);

    ck_eq(moq5_handle_kind(h), MOQ5_KIND_ROOT, "codec.kind");
    ck_eq((long long)moq5_handle_slot(h), 5, "codec.slot");
    ck_eq((long long)moq5_handle_generation(h), (long long)MOQ5_HANDLE_GEN_FIRST, "codec.gen");

    /* The first generation sets bit 31, so the token sets bit 63 and is
     * negative as a signed 64-bit value. A narrowing or signed-comparison
     * implementation fails here rather than subtly later. */
    ck((h & ((uint64_t)1u << 63)) != 0, "codec.high-bit-set");
    {
        int64_t probe;
        memcpy(&probe, &h, sizeof probe);
        ck(probe < 0, "codec.negative-as-signed");
    }

    /* Full width survives a signed round trip -- carried as BITS. An
     * out-of-range uint64_t -> int64_t conversion is implementation-defined,
     * and this test is specifically about not relying on that
     * (chats/libmoq/1234 F6). */
    int64_t as_signed;
    uint64_t back;
    memcpy(&as_signed, &h, sizeof as_signed);
    memcpy(&back, &as_signed, sizeof back);
    ck_u64_eq(back, h, "codec.roundtrip-bits");

    ck_eq((long long)moq5_handle_kind(MOQ5_HANDLE_INVALID), 0, "codec.zero-kind");
}

/* -- identity refusals ------------------------------------------------------ */
static void test_identity(void)
{
    fake_res_t root_res = { 1, 0 };
    fake_res_t child_res = { 2, 0 };
    moq5_handle_t root = MOQ5_HANDLE_INVALID;
    moq5_handle_t child = MOQ5_HANDLE_INVALID;
    void *p = NULL;

    ck_eq(moq5_reg_insert_root(&root_res, fake_cleanup, &root), MOQ5_REG_OK, "id.insert-root");
    ck(root != MOQ5_HANDLE_INVALID, "id.root-nonzero");
    ck_eq(moq5_reg_insert_child(root, &child_res, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &child),
          MOQ5_REG_OK, "id.insert-child");

    /* zero */
    ck_refused(moq5_reg_acquire(MOQ5_HANDLE_INVALID, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_INVALID, MOQ5_HANDLE_INVALID, "id.zero-refused");

    /* a token whose bits are a plausible ADDRESS, not a token */
    moq5_handle_t as_ptr = (moq5_handle_t)(uintptr_t)&root_res;
    ck(moq5_reg_acquire(as_ptr, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p) != MOQ5_REG_OK,
       "id.pointer-shaped-refused");

    /* forged: a real slot, a generation never issued */
    moq5_handle_t forged = moq5_handle_make(MOQ5_KIND_ROOT, moq5_handle_slot(root),
                                            moq5_handle_generation(root) + 7u);
    ck_refused(moq5_reg_acquire(forged, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_STALE, forged, "id.forged-generation-refused");

    /* wrong kind on a live token */
    ck_refused(moq5_reg_acquire(root, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_KIND, root, "id.wrong-kind-refused");

    /* slot beyond the runtime limit */
    moq5_handle_t oob = moq5_handle_make(MOQ5_KIND_ROOT, MOQ5_REGISTRY_MAX_ENTRIES + 1u,
                                         MOQ5_HANDLE_GEN_FIRST);
    ck_refused(moq5_reg_acquire(oob, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_INVALID, oob, "id.out-of-range-refused");

    /* the happy path still works, and pins are balanced */
    ck_eq(moq5_reg_acquire(child, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_OK, "id.acquire-ok");
    ck(p == &child_res, "id.acquire-pointer");
    ck_eq(moq5_reg_release(child), MOQ5_REG_OK, "id.release-ok");

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "id.retire");
    ck_eq((long long)moq5_reg_live_count(), 0, "id.live-zero");
}

/* -- foreign owner, with coincident slots ----------------------------------- */
static void test_foreign_owner(void)
{
    fake_res_t ra = { 10, 0 }, rb = { 11, 0 }, ca = { 12, 0 }, cb = { 13, 0 };
    moq5_handle_t root_a = 0, root_b = 0, child_a = 0, child_b = 0;
    void *p = NULL;

    ck_eq(moq5_reg_insert_root(&ra, fake_cleanup, &root_a), MOQ5_REG_OK, "fo.root-a");
    ck_eq(moq5_reg_insert_root(&rb, fake_cleanup, &root_b), MOQ5_REG_OK, "fo.root-b");
    ck_eq(moq5_reg_insert_child(root_a, &ca, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &child_a),
          MOQ5_REG_OK, "fo.child-a");
    ck_eq(moq5_reg_insert_child(root_b, &cb, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &child_b),
          MOQ5_REG_OK, "fo.child-b");

    /* A's child is FOREIGN when B is the expected owner, and vice versa. */
    ck_refused(moq5_reg_acquire(child_a, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root_b, &p),
               MOQ5_REG_ERR_OWNER, child_a, "fo.a-child-under-b");
    ck_refused(moq5_reg_acquire(child_b, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root_a, &p),
               MOQ5_REG_ERR_OWNER, child_b, "fo.b-child-under-a");

    /* Retire and rebuild both, so each has independently reused slots, then
     * repeat: the refusal must not have depended on slot coincidence. */
    ck_eq(moq5_reg_retire_root(root_a), MOQ5_REG_OK, "fo.retire-a");
    ck_eq(moq5_reg_retire_root(root_b), MOQ5_REG_OK, "fo.retire-b");

    moq5_handle_t root_a2 = 0, root_b2 = 0, child_a2 = 0, child_b2 = 0;
    ck_eq(moq5_reg_insert_root(&ra, NULL, &root_a2), MOQ5_REG_OK, "fo.root-a2");
    ck_eq(moq5_reg_insert_root(&rb, NULL, &root_b2), MOQ5_REG_OK, "fo.root-b2");
    ck_eq(moq5_reg_insert_child(root_a2, &ca, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &child_a2),
          MOQ5_REG_OK, "fo.child-a2");
    ck_eq(moq5_reg_insert_child(root_b2, &cb, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &child_b2),
          MOQ5_REG_OK, "fo.child-b2");

    ck_refused(moq5_reg_acquire(child_a2, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root_b2, &p),
               MOQ5_REG_ERR_OWNER, child_a2, "fo.a2-child-under-b2");

    /* the retired tokens stay stale after reuse */
    ck_refused(moq5_reg_acquire(child_a, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root_a, &p),
               MOQ5_REG_ERR_STALE, child_a, "fo.old-child-stale");

    ck_eq(moq5_reg_retire_root(root_a2), MOQ5_REG_OK, "fo.retire-a2");
    ck_eq(moq5_reg_retire_root(root_b2), MOQ5_REG_OK, "fo.retire-b2");
    ck_eq((long long)moq5_reg_live_count(), 0, "fo.live-zero");
}

/* -- reuse, staleness and the wrap policy ----------------------------------- */
static void test_reuse_and_wrap(void)
{
    fake_res_t r1 = { 20, 0 };
    moq5_handle_t a = 0, b = 0;
    void *p = NULL;

    ck_eq(moq5_reg_insert_root(&r1, NULL, &a), MOQ5_REG_OK, "reuse.first");
    uint32_t slot_a = moq5_handle_slot(a);
    ck_eq(moq5_reg_retire_root(a), MOQ5_REG_OK, "reuse.retire");

    /* the same address is reused deliberately: generation, not address, decides */
    ck_eq(moq5_reg_insert_root(&r1, NULL, &b), MOQ5_REG_OK, "reuse.second");
    ck_eq((long long)moq5_handle_slot(b), (long long)slot_a, "reuse.same-slot");
    ck(a != b, "reuse.different-token");
    ck(moq5_handle_generation(b) != moq5_handle_generation(a), "reuse.generation-advanced");
    ck_refused(moq5_reg_acquire(a, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_STALE, a, "reuse.old-token-stale-after-same-address-reuse");
    ck_eq(moq5_reg_retire_root(b), MOQ5_REG_OK, "reuse.retire2");

    /* Near-wrap. The boundary is reached by priming a FREE slot's NEXT
     * generation, never by rewriting a live tree: doing that would leave the
     * tokens a root's descendants recorded pointing at an identity that no
     * longer exists, a state no real execution can reach. */
    moq5_handle_t probe = 0;
    ck_eq(moq5_reg_insert_root(&r1, NULL, &probe), MOQ5_REG_OK, "wrap.probe");
    uint32_t nw_slot = moq5_handle_slot(probe);
    ck_eq(moq5_reg_retire_root(probe), MOQ5_REG_OK, "wrap.probe-retire");
    ck(moq5_reg_test_prime_free_slot_generation(nw_slot, MOQ5_HANDLE_GEN_LAST),
       "wrap.prime-free-slot");

    moq5_handle_t at_last = 0;
    ck_eq(moq5_reg_insert_root(&r1, NULL, &at_last), MOQ5_REG_OK, "wrap.insert-at-last");
    ck_eq((long long)moq5_handle_slot(at_last), (long long)nw_slot, "wrap.took-primed-slot");
    ck_eq((long long)moq5_handle_generation(at_last), (long long)MOQ5_HANDLE_GEN_LAST,
          "wrap.at-last-generation");
    ck_eq(moq5_reg_retire_root(at_last), MOQ5_REG_OK, "wrap.retire-at-last");

    moq5_handle_t after = 0;
    ck_eq(moq5_reg_insert_root(&r1, NULL, &after), MOQ5_REG_OK, "wrap.insert-after");
    ck((long long)moq5_handle_slot(after) != (long long)nw_slot, "wrap.slot-retired-not-reused");
    ck_refused(moq5_reg_acquire(at_last, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_STALE, at_last, "wrap.oldest-never-valid-again");
    ck_eq(moq5_reg_test_slot_retired(nw_slot), 1, "wrap.slot-marked-retired");
    ck_eq(moq5_reg_retire_root(after), MOQ5_REG_OK, "wrap.cleanup");
    ck_eq((long long)moq5_reg_live_count(), 0, "wrap.live-zero");
}

/* -- teardown ordering and exactly-once cleanup ----------------------------- */
static void test_teardown(void)
{
    fake_res_t root_r = { 100, 0 };
    fake_res_t owned_a = { 101, 0 };
    fake_res_t owned_b = { 102, 0 };
    fake_res_t borrowed = { 103, 0 };
    moq5_handle_t root = 0, ca = 0, cb = 0, cborrow = 0;
    void *p = NULL;

    cleanup_count = 0;
    cleanup_lock_free_count = 0;
    cleanup_lock_violations = 0;
    memset(cleanup_order, 0, sizeof cleanup_order);

    ck_eq(moq5_reg_insert_root(&root_r, fake_cleanup, &root), MOQ5_REG_OK, "td.root");
    ck_eq(moq5_reg_insert_child(root, &owned_a, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &ca),
          MOQ5_REG_OK, "td.child-a");
    ck_eq(moq5_reg_insert_child(root, &owned_b, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &cb),
          MOQ5_REG_OK, "td.child-b");
    /* A cleanup IS recorded for the borrowed child on purpose: the rule is that
     * a borrowed entry is invalidated but never cleaned, and with a NULL cleanup
     * that rule would hold trivially and prove nothing. */
    ck_eq(moq5_reg_insert_child(root, &borrowed, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, fake_cleanup, &cborrow),
          MOQ5_REG_OK, "td.child-borrowed");
    ck_eq((long long)moq5_reg_live_count(), 4, "td.live-four");

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "td.retire");

    /* every descendant token is invalidated */
    ck_refused(moq5_reg_acquire(ca, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_ERR_STALE, ca, "td.a-stale");
    ck_refused(moq5_reg_acquire(cb, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_ERR_STALE, cb, "td.b-stale");
    ck_refused(moq5_reg_acquire(cborrow, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, root, &p),
               MOQ5_REG_ERR_STALE, cborrow, "td.borrowed-stale");
    ck_refused(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_STALE, root, "td.root-stale");

    /* owned cleaned exactly once each; borrowed never cleaned */
    ck_eq(owned_a.cleaned, 1, "td.a-cleaned-once");
    ck_eq(owned_b.cleaned, 1, "td.b-cleaned-once");
    ck_eq(root_r.cleaned, 1, "td.root-cleaned-once");
    ck_eq(borrowed.cleaned, 0, "td.borrowed-never-cleaned");

    /* children before the root */
    ck_eq(cleanup_count, 3, "td.cleanup-count");
    ck(cleanup_order[cleanup_count - 1] == 100, "td.root-cleaned-last");
    ck(cleanup_order[0] != 100 && cleanup_order[1] != 100, "td.children-first");

    /* the lock was not held across ANY callback, and the count is exact so a
     * violation on one child cannot be hidden by a later well-behaved one */
    ck_eq(cleanup_lock_violations, 0, "td.lock-free-during-every-cleanup");
    ck_eq(cleanup_lock_free_count, 3, "td.lock-free-observed-exactly-three-times");

    /* a second retirement is stale, not a double free */
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_ERR_STALE, "td.second-retire-stale");
    ck_eq(owned_a.cleaned, 1, "td.no-double-cleanup");
    ck_eq((long long)moq5_reg_live_count(), 0, "td.live-zero");
}

/* -- transactional failure --------------------------------------------------- */
static void test_transactional_failure(void)
{
    fake_res_t r = { 200, 0 };
    moq5_handle_t h = 0;

    size_t live_before = moq5_reg_live_count();

    /* Fill the table exactly to capacity, so the next insertion MUST grow and
     * the forced allocation failure is genuinely on the growth path rather than
     * on a path that happened not to allocate. */
    moq5_handle_t filler[MOQ5_REGISTRY_MAX_ENTRIES];
    uint32_t filled = 0;
    uint32_t cap = moq5_reg_test_capacity();
    while (moq5_reg_test_next_free_slot() < cap) {
        ck_eq(moq5_reg_insert_root(&r, NULL, &filler[filled]), MOQ5_REG_OK, "tx.fill");
        filled++;
        if (filled >= MOQ5_REGISTRY_MAX_ENTRIES) {
            break;
        }
    }
    ck((long long)moq5_reg_test_next_free_slot() == (long long)cap, "tx.at-capacity-boundary");

    uint32_t next_before = moq5_reg_test_next_free_slot();
    uint32_t cap_before = moq5_reg_test_capacity();
    uint32_t gen_before = moq5_reg_test_slot_generation(next_before);
    long balance_before = moq5_reg_test_alloc_balance();
    size_t live_at_boundary = moq5_reg_live_count();

    moq5_reg_test_fail_next_alloc(1);
    ck_eq(moq5_reg_insert_root(&r, fake_cleanup, &h), MOQ5_REG_ERR_NOMEM, "tx.growth-failure");
    moq5_reg_test_fail_next_alloc(0);

    ck_eq(h, MOQ5_HANDLE_INVALID, "tx.no-token-on-failure");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_at_boundary, "tx.live-unchanged");
    ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)next_before,
          "tx.candidate-slot-unchanged");
    /* Capacity is pinned EXPLICITLY. A failed grow could install a larger table
     * and still leave the next-free slot and the balance untouched, so
     * next-free-slot is not a proof about capacity (chats/libmoq/1236 F4). */
    ck_eq((long long)moq5_reg_test_capacity(), (long long)cap_before, "tx.capacity-unchanged");
    ck_eq((long long)moq5_reg_test_slot_generation(next_before), (long long)gen_before,
          "tx.generation-unchanged");
    ck_eq(moq5_reg_test_alloc_balance(), balance_before, "tx.allocator-unchanged");
    ck_eq(r.cleaned, 0, "tx.no-callback-on-failure");

    /* The retry must take the ORIGINAL candidate slot with the FIRST generation
     * it would have had: proof that the failed attempt consumed neither. */
    ck_eq(moq5_reg_insert_root(&r, NULL, &h), MOQ5_REG_OK, "tx.recovers-after-failure");
    ck_eq((long long)moq5_handle_slot(h), (long long)next_before, "tx.retry-takes-original-slot");
    ck_eq((long long)moq5_handle_generation(h),
          (long long)(gen_before == 0 ? MOQ5_HANDLE_GEN_FIRST : gen_before),
          "tx.retry-takes-original-generation");
    ck_eq(moq5_reg_retire_root(h), MOQ5_REG_OK, "tx.retire-recovered");

    for (uint32_t i = 0; i < filled; i++) {
        ck_eq(moq5_reg_retire_root(filler[i]), MOQ5_REG_OK, "tx.unfill");
    }
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "tx.live-after-unfill");

    /* a child of a dead root is refused, transactionally */
    moq5_handle_t root = 0, child = 0;
    ck_eq(moq5_reg_insert_root(&r, NULL, &root), MOQ5_REG_OK, "tx.root");
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "tx.retire");
    ck_eq(moq5_reg_insert_child(root, &r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &child),
          MOQ5_REG_ERR_STALE, "tx.child-of-dead-root");
    ck_eq(child, MOQ5_HANDLE_INVALID, "tx.child-no-token");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "tx.live-restored");
}

/* -- thread confinement and the any-thread lease ---------------------------- */
typedef struct {
    moq5_handle_t handle;
    moq5_handle_t owner;
    barrier_t *barrier;
    moq5_reg_result_t rc;
    void *ptr;
} thread_arg_t;

/* An engine-confined acquire attempted from another thread must be refused
 * BEFORE any pointer is exposed. */
/* A foreign thread attempting to retire one child: refused by THREAD, and it
 * must clean nothing and invalidate nothing. */
static void *wrong_thread_commit_child(void *v)
{
    thread_arg_t *a = (thread_arg_t *)v;
    static fake_res_t off_thread_commit = { 998, 0 };
    a->rc = moq5_reg_commit_child((moq5_handle_t)(uintptr_t)a->ptr, a->handle,
                                  MOQ5_TYPE_SENDER_TRACK, &off_thread_commit, NULL);
    return NULL;
}

static void *wrong_thread_retire_child(void *v)
{
    thread_arg_t *a = (thread_arg_t *)v;
    a->rc = moq5_reg_retire_child((moq5_handle_t)(uintptr_t)a->ptr, a->handle, MOQ5_TYPE_RECEIVER);
    return NULL;
}

static void *wrong_thread_acquire(void *v)
{
    thread_arg_t *a = (thread_arg_t *)v;
    a->ptr = (void *)(uintptr_t)0xDEADBEEF; /* poisoned: must be left untouched */
    a->rc = moq5_reg_acquire(a->handle, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &a->ptr);
    if (a->rc == MOQ5_REG_OK) {
        /* An unexpected success took a pin. Release it, so a mutant that drops
         * the thread check fails by name instead of deadlocking the retirement
         * that follows. */
        moq5_reg_release(a->handle);
    }
    return NULL;
}

/* -- the reservation state machine ------------------------------------------
 *
 * Reserve/commit/abort is the primitive the JNI construction path depends on to
 * have no window in which an object exists that the registry does not own.
 * Integration evidence (a slot being reused) is not coverage of it, so every
 * rule it promises is pinned directly here. */
typedef struct {
    moq5_handle_t handle;
    void *ptr;
    moq5_reg_result_t commit_rc;
    moq5_reg_result_t abort_rc;
} resv_thread_arg_t;

static void *foreign_commit_abort(void *v)
{
    resv_thread_arg_t *a = (resv_thread_arg_t *)v;
    a->commit_rc = moq5_reg_commit_root(a->handle, a->ptr, NULL);
    a->abort_rc = moq5_reg_abort_root(a->handle);
    return NULL;
}

static void test_reservation(void)
{
    fake_res_t r = { 900, 0 };
    fake_res_t r2 = { 901, 0 };
    moq5_handle_t h = 0;
    void *p = (void *)(uintptr_t)0xDEADBEEF;
    size_t live0 = moq5_reg_live_count();

    /* A reservation is claimed, and it is NOT live. */
    ck_eq(moq5_reg_reserve_root(&h), MOQ5_REG_OK, "resv.reserve");
    ck(h != MOQ5_HANDLE_INVALID, "resv.token-issued");
    ck_eq(moq5_handle_kind(h), MOQ5_KIND_ROOT, "resv.token-kind");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.not-live");

    /* Its token resolves to nothing: neither acquire nor lease may see it, and
     * neither may expose a pointer. */
    ck_eq(moq5_reg_acquire(h, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
          MOQ5_REG_ERR_STALE, "resv.acquire-refused");
    ck(p == (void *)(uintptr_t)0xDEADBEEF, "resv.acquire-exposed-nothing");
    p = (void *)(uintptr_t)0xDEADBEEF;
    ck_eq(moq5_reg_lease_root_any_thread(h, &p), MOQ5_REG_ERR_STALE,
          "resv.lease-refused");
    ck(p == (void *)(uintptr_t)0xDEADBEEF, "resv.lease-exposed-nothing");
    ck_eq(moq5_reg_retire_root(h), MOQ5_REG_ERR_STALE, "resv.retire-refused");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.still-not-live");

    /* Only the exact reservation is accepted. */
    ck_eq(moq5_reg_commit_root(h, NULL, NULL), MOQ5_REG_ERR_INVALID,
          "resv.commit-null-ptr");
    ck_eq(moq5_reg_commit_root(moq5_handle_make(MOQ5_KIND_CHILD_OWNED,
                                                moq5_handle_slot(h),
                                                moq5_handle_generation(h)),
                               &r, NULL),
          MOQ5_REG_ERR_KIND, "resv.commit-wrong-kind");
    ck_eq(moq5_reg_commit_root(moq5_handle_make(MOQ5_KIND_ROOT,
                                                moq5_handle_slot(h),
                                                moq5_handle_generation(h) ^ 1u),
                               &r, NULL),
          MOQ5_REG_ERR_STALE, "resv.commit-wrong-generation");
    ck_eq(moq5_reg_abort_root(moq5_handle_make(MOQ5_KIND_ROOT,
                                               moq5_handle_slot(h),
                                               moq5_handle_generation(h) ^ 1u)),
          MOQ5_REG_ERR_STALE, "resv.abort-wrong-generation");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0,
          "resv.refusals-changed-nothing");

    /* A foreign thread may neither commit nor abort it. */
    {
        pthread_t t;
        resv_thread_arg_t a;
        memset(&a, 0, sizeof a);
        a.handle = h;
        a.ptr = &r;
        pt_must(pthread_create(&t, NULL, foreign_commit_abort, &a), "resv.thread-create");
        pt_must(pthread_join(t, NULL), "resv.thread-join");
        ck_eq(a.commit_rc, MOQ5_REG_ERR_THREAD, "resv.foreign-commit-refused");
        ck_eq(a.abort_rc, MOQ5_REG_ERR_THREAD, "resv.foreign-abort-refused");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0,
              "resv.foreign-refusals-changed-nothing");
    }

    /* The reservation still commits on its own thread, exactly once. */
    ck_eq(moq5_reg_commit_root(h, &r, fake_cleanup), MOQ5_REG_OK, "resv.commit");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 1, "resv.live-after-commit");
    ck_eq(moq5_reg_commit_root(h, &r, fake_cleanup), MOQ5_REG_ERR_STATE,
          "resv.double-commit-refused");
    ck_eq(moq5_reg_abort_root(h), MOQ5_REG_ERR_STATE, "resv.abort-after-commit-refused");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 1,
          "resv.double-commit-changed-nothing");

    /* A committed reservation is an ordinary root: it resolves, and retirement
     * cleans it exactly once. */
    p = NULL;
    ck_eq(moq5_reg_acquire(h, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK,
          "resv.committed-acquires");
    ck(p == &r, "resv.committed-pointer");
    ck_eq(moq5_reg_release(h), MOQ5_REG_OK, "resv.release");
    ck_eq(moq5_reg_retire_root(h), MOQ5_REG_OK, "resv.retire");
    ck_eq(r.cleaned, 1, "resv.cleaned-exactly-once");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.live-after-retire");

    /* ABORT CONSUMES NOTHING: the next issued token for that slot carries the
     * same generation an insertion would have received had the reservation
     * never happened. */
    {
        moq5_handle_t a1 = 0, a2 = 0;
        ck_eq(moq5_reg_reserve_root(&a1), MOQ5_REG_OK, "resv.abort-reserve");
        ck_eq(moq5_reg_abort_root(a1), MOQ5_REG_OK, "resv.abort");
        ck_eq(moq5_reg_abort_root(a1), MOQ5_REG_ERR_STATE, "resv.double-abort-refused");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.abort-not-live");

        ck_eq(moq5_reg_reserve_root(&a2), MOQ5_REG_OK, "resv.reserve-again");
        ck_eq(a2, a1, "resv.abort-consumed-no-token");
        ck_eq(moq5_reg_commit_root(a2, &r2, fake_cleanup), MOQ5_REG_OK, "resv.commit-again");
        ck_eq(moq5_reg_retire_root(a2), MOQ5_REG_OK, "resv.retire-again");
        ck_eq(r2.cleaned, 1, "resv.second-cleaned-once");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.final-live");
    }

    /* And an insertion after an aborted reservation receives that same token,
     * so the two paths agree about what the reservation cost. */
    {
        moq5_handle_t a3 = 0, ins = 0;
        fake_res_t r3 = { 902, 0 };
        ck_eq(moq5_reg_reserve_root(&a3), MOQ5_REG_OK, "resv.mixed-reserve");
        ck_eq(moq5_reg_abort_root(a3), MOQ5_REG_OK, "resv.mixed-abort");
        ck_eq(moq5_reg_insert_root(&r3, fake_cleanup, &ins), MOQ5_REG_OK,
              "resv.mixed-insert");
        ck_eq(ins, a3, "resv.insert-receives-the-same-token");
        ck_eq(moq5_reg_retire_root(ins), MOQ5_REG_OK, "resv.mixed-retire");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0, "resv.mixed-final-live");
    }
}

/* -- retirement readiness ---------------------------------------------------
 *
 * Cleanup eligibility is the registry's, not the caller's: a root is cleaned
 * only after whoever owns it has said it is quiesced. These rows pin that the
 * gate refuses an unready root, that identity is checked before state, and that
 * marking is monotonic and confined to the authority thread. */
typedef struct {
    moq5_handle_t handle;
    moq5_reg_result_t mark_rc;
    moq5_reg_result_t retire_rc;
} ready_thread_arg_t;

static void *foreign_mark_retire(void *v)
{
    ready_thread_arg_t *a = (ready_thread_arg_t *)v;
    a->mark_rc = moq5_reg_mark_ready(a->handle);
    a->retire_rc = moq5_reg_retire_root_ready(a->handle);
    return NULL;
}

static void test_ready(void)
{
    fake_res_t r = { 700, 0 };
    moq5_handle_t root = 0;
    size_t live0 = moq5_reg_live_count();

    ck_eq(moq5_reg_insert_root(&r, fake_cleanup, &root), MOQ5_REG_OK, "rdy.insert");

    /* An unready live root is refused, stays valid, and is NOT cleaned. */
    ck_eq(moq5_reg_retire_root_ready(root), MOQ5_REG_ERR_STATE, "rdy.unready-refused");
    ck_eq(r.cleaned, 0, "rdy.unready-not-cleaned");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 1, "rdy.unready-still-live");
    {
        void *p = NULL;
        ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK,
              "rdy.unready-still-acquires");
        ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "rdy.unready-release");
    }

    /* Neither marking nor the ready-checked retirement is available to a
     * foreign thread, and IDENTITY is reported before state. */
    {
        pthread_t t;
        ready_thread_arg_t a;
        memset(&a, 0, sizeof a);
        a.handle = root;
        pt_must(pthread_create(&t, NULL, foreign_mark_retire, &a), "rdy.thread-create");
        pt_must(pthread_join(t, NULL), "rdy.thread-join");
        ck_eq(a.mark_rc, MOQ5_REG_ERR_THREAD, "rdy.foreign-mark-refused");
        ck_eq(a.retire_rc, MOQ5_REG_ERR_THREAD, "rdy.foreign-retire-identity-first");
        ck_eq(r.cleaned, 0, "rdy.foreign-refusals-cleaned-nothing");
    }

    /* Marking is monotonic, and only then does the gate open. */
    ck_eq(moq5_reg_mark_ready(root), MOQ5_REG_OK, "rdy.mark");
    ck_eq(moq5_reg_mark_ready(root), MOQ5_REG_OK, "rdy.mark-again-idempotent");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 1, "rdy.mark-changed-no-count");
    ck_eq(r.cleaned, 0, "rdy.mark-cleaned-nothing");

    ck_eq(moq5_reg_retire_root_ready(root), MOQ5_REG_OK, "rdy.retire");
    ck_eq(r.cleaned, 1, "rdy.cleaned-exactly-once");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "rdy.live-after");

    /* A retired token is refused by identity, and cleans nothing a second
     * time -- repeated retirement has exactly one owner. */
    ck_eq(moq5_reg_retire_root_ready(root), MOQ5_REG_ERR_STALE, "rdy.retired-refused");
    ck_eq(moq5_reg_mark_ready(root), MOQ5_REG_ERR_STALE, "rdy.retired-mark-refused");
    ck_eq(r.cleaned, 1, "rdy.still-cleaned-once");

    /* Readiness does not survive a slot's reuse. */
    {
        fake_res_t r2 = { 701, 0 };
        moq5_handle_t again = 0;
        ck_eq(moq5_reg_insert_root(&r2, fake_cleanup, &again), MOQ5_REG_OK, "rdy.reuse-insert");
        ck_eq(moq5_reg_retire_root_ready(again), MOQ5_REG_ERR_STATE,
              "rdy.reuse-starts-unready");
        ck_eq(moq5_reg_mark_ready(again), MOQ5_REG_OK, "rdy.reuse-mark");
        ck_eq(moq5_reg_retire_root_ready(again), MOQ5_REG_OK, "rdy.reuse-retire");
        ck_eq(r2.cleaned, 1, "rdy.reuse-cleaned-once");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0, "rdy.final-live");
    }
}

/* -- retiring one owned child leaves its owner live -------------------------- */
static void test_child_retirement(void)
{
    fake_res_t root_r = { 700, 0 }, child_r = { 701, 0 }, grand_r = { 702, 0 },
               borrow_r = { 703, 0 }, sib_r = { 704, 0 }, other_r = { 705, 0 },
               sender_r = { 707, 0 };
    moq5_handle_t root = 0, child = 0, grand = 0, borrowed = 0, sibling = 0, other = 0,
                  sender = 0;
    void *p = NULL;
    size_t live0 = moq5_reg_live_count();

    cleanup_count = 0;
    memset(cleanup_order, 0, sizeof cleanup_order);
    cleanup_lock_free_count = 0;
    cleanup_lock_violations = 0;

    ck_eq(moq5_reg_insert_root(&root_r, fake_cleanup, &root), MOQ5_REG_OK, "rc.root");
    ck_eq(moq5_reg_insert_root(&other_r, fake_cleanup, &other), MOQ5_REG_OK, "rc.other-root");
    ck_eq(moq5_reg_insert_child(root, &child_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &child),
          MOQ5_REG_OK, "rc.child");
    ck_eq(moq5_reg_insert_child(child, &grand_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &grand),
          MOQ5_REG_OK, "rc.grandchild");
    ck_eq(moq5_reg_insert_child(child, &borrow_r, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, fake_cleanup, &borrowed),
          MOQ5_REG_OK, "rc.borrowed-under-child");
    ck_eq(moq5_reg_insert_child(root, &sib_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &sibling),
          MOQ5_REG_OK, "rc.sibling");
    /* A second family under the SAME owner with the SAME kind: exactly the pair
     * that kind and owner cannot tell apart. */
    ck_eq(moq5_reg_insert_child(root, &sender_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, fake_cleanup, &sender),
          MOQ5_REG_OK, "rc.sender-child");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 7, "rc.live-seven");

    /* Refusals, each named, each changing nothing. */
    ck_eq(moq5_reg_retire_child(root, root, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_KIND, "rc.root-through-child-path-refused");
    ck_eq(moq5_reg_retire_child(child, borrowed, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_KIND, "rc.borrowed-child-refused");
    ck_eq(moq5_reg_retire_child(other, child, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_OWNER, "rc.foreign-owner-refused");
    ck_eq(moq5_reg_retire_child(root, grand, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_OWNER, "rc.ancestor-is-not-the-owner");
    {
        moq5_handle_t forged = moq5_handle_make(MOQ5_KIND_CHILD_OWNED, moq5_handle_slot(child),
                                                moq5_handle_generation(child) + 5u);
        ck_eq(moq5_reg_retire_child(root, forged, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_STALE, "rc.stale-child-refused");
    }
    ck_eq(moq5_reg_retire_child(root, MOQ5_HANDLE_INVALID, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_INVALID, "rc.zero-child-refused");
    /* Right owner, right kind, WRONG family, in both directions: neither child
     * may be destroyed through the other's row. */
    ck_eq(moq5_reg_retire_child(root, sender, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_TYPE, "rc.sender-through-receiver-row");
    ck_eq(moq5_reg_retire_child(root, sibling, MOQ5_TYPE_SENDER), MOQ5_REG_ERR_TYPE, "rc.receiver-through-sender-row");
    /* And the family is checked BEFORE the owner, so a cross-family token is
     * never even compared against an owner it does not belong to. */
    ck_eq(moq5_reg_retire_child(other, sender, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_TYPE, "rc.type-outranks-owner");
    {
        pthread_t t;
        thread_arg_t a;
        memset(&a, 0, sizeof a);
        a.handle = child;
        a.ptr = (void *)(uintptr_t)root;
        pt_must(pthread_create(&t, NULL, wrong_thread_retire_child, &a), "rc.thread-create");
        pt_must(pthread_join(t, NULL), "rc.thread-join");
        ck_eq(a.rc, MOQ5_REG_ERR_THREAD, "rc.wrong-thread-refused");
    }
    ck_eq(cleanup_count, 0, "rc.refusals-cleaned-nothing");
    ck_eq(sender_r.cleaned, 0, "rc.cross-type-refusal-destroyed-nothing");
    ck_eq(sib_r.cleaned, 0, "rc.cross-type-refusal-spared-the-receiver");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 7, "rc.refusals-changed-no-count");
    ck_eq(moq5_reg_acquire(grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, child, &p), MOQ5_REG_OK, "rc.grand-still-live");
    ck_eq(moq5_reg_release(grand), MOQ5_REG_OK, "rc.grand-release");

    /* The retirement: the subtree, deepest first, lock free, borrowed not cleaned. */
    ck_eq(moq5_reg_retire_child(root, child, MOQ5_TYPE_RECEIVER), MOQ5_REG_OK, "rc.retire");
    ck_eq(cleanup_count, 2, "rc.two-cleanups");
    ck_eq(cleanup_order[0], 702, "rc.grandchild-first");
    ck_eq(cleanup_order[1], 701, "rc.child-second");
    ck_eq(grand_r.cleaned, 1, "rc.grandchild-cleaned-once");
    ck_eq(child_r.cleaned, 1, "rc.child-cleaned-once");
    ck_eq(borrow_r.cleaned, 0, "rc.borrowed-never-cleaned");
    ck_eq(cleanup_lock_violations, 0, "rc.lock-free-during-cleanup");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 4, "rc.subtree-freed");

    /* Each family still retires through its OWN row, exactly once. */
    ck_eq(moq5_reg_retire_child(root, sender, MOQ5_TYPE_SENDER), MOQ5_REG_OK, "rc.sender-retires-by-its-own-type");
    ck_eq(sender_r.cleaned, 1, "rc.sender-cleaned-once");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 3, "rc.sender-freed");

    /* The subtree's tokens are gone; the owner and the sibling are untouched. */
    ck_refused(moq5_reg_acquire(child, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_ERR_STALE, child, "rc.child-stale");
    ck_refused(moq5_reg_acquire(grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, child, &p), MOQ5_REG_ERR_STALE, grand, "rc.grand-stale");
    ck_refused(moq5_reg_acquire(borrowed, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, child, &p), MOQ5_REG_ERR_STALE, borrowed,
               "rc.borrowed-stale");
    ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK, "rc.root-still-live");
    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "rc.root-release");
    ck_eq(moq5_reg_acquire(sibling, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_OK, "rc.sibling-still-live");
    ck_eq(moq5_reg_release(sibling), MOQ5_REG_OK, "rc.sibling-release");
    ck_refused(moq5_reg_acquire(sender, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p),
               MOQ5_REG_ERR_STALE, sender, "rc.retired-sender-stale");
    ck_eq(moq5_reg_retire_child(root, child, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_STALE, "rc.retire-again-stale");

    /* The owner accepts a new child in the freed capacity, and root retirement
     * later cleans only what is still live -- the retired child never again. */
    {
        fake_res_t again_r = { 706, 0 };
        moq5_handle_t again = 0;
        ck_eq(moq5_reg_insert_child(root, &again_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &again),
              MOQ5_REG_OK, "rc.owner-accepts-new-child");
        ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "rc.retire-root");
        ck_eq(again_r.cleaned, 1, "rc.new-child-cleaned-once");
        ck_eq(sib_r.cleaned, 1, "rc.sibling-cleaned-once");
        ck_eq(root_r.cleaned, 1, "rc.root-cleaned-once");
        ck_eq(child_r.cleaned, 1, "rc.retired-child-not-cleaned-again");
        ck_eq(grand_r.cleaned, 1, "rc.retired-grandchild-not-cleaned-again");
    }
    ck_eq(moq5_reg_retire_root(other), MOQ5_REG_OK, "rc.retire-other");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "rc.live-restored");
}

/* -- the concrete type behind an entry --------------------------------------
 *
 * A kind is a LIFETIME role. Two attached objects of one endpoint share it, and
 * share the owner too, so kind and owner together cannot say which family a
 * token names -- yet every consumer casts the pointer it acquires to a concrete
 * wrapper. This is the check that stands between those two facts.
 */
static void test_concrete_type(void)
{
    fake_res_t root_r = { 800, 0 }, rx_r = { 801, 0 }, tx_r = { 802, 0 },
               other_r = { 803, 0 }, reused_r = { 804, 0 };
    moq5_handle_t root = 0, rx = 0, tx = 0, rx_track = 0, tx_track = 0, other = 0;
    int rx_slot_marker = 0, tx_slot_marker = 0;
    /* An address this test stores in NO entry. A sentinel that aliased a stored
     * pointer would let "nothing was published" be satisfied by publishing that
     * very pointer, which is the one outcome these rows exist to exclude. */
    int unpublished = 0;
    void *sentinel = &unpublished;
    void *p = NULL;
    size_t live0 = moq5_reg_live_count();

    cleanup_count = 0;
    memset(cleanup_order, 0, sizeof cleanup_order);

    ck_eq(moq5_reg_insert_root(&root_r, fake_cleanup, &root), MOQ5_REG_OK, "ty.root");
    ck_eq(moq5_reg_insert_root(&other_r, fake_cleanup, &other), MOQ5_REG_OK, "ty.other-root");
    ck_eq(moq5_reg_insert_child(root, &rx_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &rx),
          MOQ5_REG_OK, "ty.receiver-child");
    ck_eq(moq5_reg_insert_child(root, &tx_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, fake_cleanup, &tx),
          MOQ5_REG_OK, "ty.sender-child");
    ck_eq(moq5_handle_kind(rx), moq5_handle_kind(tx), "ty.same-kind");
    ck_eq(moq5_reg_insert_child(rx, &rx_slot_marker, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK,
                                NULL, &rx_track), MOQ5_REG_OK, "ty.receiver-track");
    ck_eq(moq5_reg_insert_child(tx, &tx_slot_marker, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK,
                                NULL, &tx_track), MOQ5_REG_OK, "ty.sender-track");

    /* -- 1. same owner, same kind, different family ------------------------- */
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p),
               MOQ5_REG_ERR_TYPE, rx, "ty.receiver-as-sender-refused");
    ck(p == sentinel, "ty.receiver-as-sender-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(tx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p),
               MOQ5_REG_ERR_TYPE, tx, "ty.sender-as-receiver-refused");
    ck(p == sentinel, "ty.sender-as-receiver-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, rx, &p),
               MOQ5_REG_ERR_TYPE, rx_track, "ty.receiver-track-as-sender-track-refused");
    ck(p == sentinel, "ty.receiver-track-refusal-published-no-pointer");
    /* And the converse. A one-directional comparison would satisfy the row
     * above and still let a receiver row through with a sender's track. */
    p = sentinel;
    ck_refused(moq5_reg_acquire(tx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, tx, &p),
               MOQ5_REG_ERR_TYPE, tx_track, "ty.sender-track-as-receiver-track-refused");
    ck(p == sentinel, "ty.sender-track-refusal-published-no-pointer");

    /* The right family reaches the right object. */
    p = sentinel;
    ck_eq(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_OK,
          "ty.receiver-as-receiver-ok");
    ck(p == &rx_r, "ty.receiver-pointer-identity");
    ck_eq(moq5_reg_release(rx), MOQ5_REG_OK, "ty.receiver-release");
    p = sentinel;
    ck_eq(moq5_reg_acquire(tx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p), MOQ5_REG_OK,
          "ty.sender-as-sender-ok");
    ck(p == &tx_r, "ty.sender-pointer-identity");
    ck_eq(moq5_reg_release(tx), MOQ5_REG_OK, "ty.sender-release");
    p = sentinel;
    ck_eq(moq5_reg_acquire(rx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, rx, &p),
          MOQ5_REG_OK, "ty.receiver-track-ok");
    ck(p == &rx_slot_marker, "ty.receiver-track-pointer-identity");
    ck_eq(moq5_reg_release(rx_track), MOQ5_REG_OK, "ty.receiver-track-release");
    p = sentinel;
    ck_eq(moq5_reg_acquire(tx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, tx, &p),
          MOQ5_REG_OK, "ty.sender-track-ok");
    ck(p == &tx_slot_marker, "ty.sender-track-pointer-identity");
    ck_eq(moq5_reg_release(tx_track), MOQ5_REG_OK, "ty.sender-track-release");

    /* The track pair keeps its KIND and OWNER controls: the family check is an
     * addition to them, not a replacement. */
    p = sentinel;
    ck_refused(moq5_reg_acquire(tx_track, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, tx, &p),
               MOQ5_REG_ERR_KIND, tx_track, "ty.track-kind-still-checked");
    ck(p == sentinel, "ty.track-kind-refusal-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(tx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, rx, &p),
               MOQ5_REG_ERR_OWNER, tx_track, "ty.track-owner-still-checked");
    ck(p == sentinel, "ty.track-owner-refusal-published-no-pointer");

    /* -- 2. a refusal changes nothing --------------------------------------- */
    ck_eq(cleanup_count, 0, "ty.refusals-cleaned-nothing");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 6, "ty.refusals-changed-no-count");
    /* Pins are conserved: primed one below the limit, a refused cross-family
     * acquire must leave room for exactly one more real pin. If it had taken
     * one, the acquire below would be refused by PINS instead of succeeding. */
    ck(moq5_reg_test_set_pins(rx, MOQ5_REGISTRY_MAX_PINS - 1u), "ty.prime-pins");
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p),
               MOQ5_REG_ERR_TYPE, rx, "ty.primed-cross-family-refused");
    ck(p == sentinel, "ty.primed-cross-family-published-no-pointer");
    ck_eq(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_OK,
          "ty.refusal-left-the-last-pin-free");
    ck(p == &rx_r, "ty.refusal-left-the-right-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p),
               MOQ5_REG_ERR_PINS, rx, "ty.now-at-the-pin-limit");
    ck(p == sentinel, "ty.pin-limit-published-no-pointer");
    ck(moq5_reg_test_set_pins(rx, 0), "ty.restore-pins");

    /* -- 3. precedence: kind first, then type, then owner ------------------- */
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx_track, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, rx, &p),
               MOQ5_REG_ERR_KIND, rx_track, "ty.kind-outranks-type");
    ck(p == sentinel, "ty.kind-refusal-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, other, &p),
               MOQ5_REG_ERR_TYPE, rx, "ty.type-outranks-owner");
    ck(p == sentinel, "ty.type-outranks-owner-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, other, &p),
               MOQ5_REG_ERR_OWNER, rx, "ty.owner-still-checked-for-the-right-family");
    ck(p == sentinel, "ty.owner-refusal-published-no-pointer");

    /* -- 4. declarations no entry could ever satisfy ------------------------ */
    {
        moq5_handle_t bad = 0;
        uint32_t free_before = moq5_reg_test_next_free_slot();
        ck_eq(moq5_reg_insert_child(root, &reused_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_NONE, NULL, &bad),
              MOQ5_REG_ERR_TYPE, "ty.insert-none-refused");
        ck_eq(moq5_reg_insert_child(root, &reused_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE__COUNT, NULL, &bad),
              MOQ5_REG_ERR_TYPE, "ty.insert-unknown-refused");
        ck_eq(moq5_reg_insert_child(root, &reused_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_ENDPOINT, NULL, &bad),
              MOQ5_REG_ERR_TYPE, "ty.insert-endpoint-under-owned-refused");
        ck_eq(moq5_reg_insert_child(root, &reused_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER_TRACK, NULL, &bad),
              MOQ5_REG_ERR_TYPE, "ty.insert-track-as-owned-refused");
        ck_eq(moq5_reg_insert_child(rx, &reused_r, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER, NULL, &bad),
              MOQ5_REG_ERR_TYPE, "ty.insert-receiver-as-borrowed-refused");
        ck_eq(bad, MOQ5_HANDLE_INVALID, "ty.refused-declaration-published-no-token");
        ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)free_before,
              "ty.refused-declaration-consumed-no-slot");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 6, "ty.refused-declaration-changed-no-count");

        p = sentinel;
        ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_NONE, root, &p),
                   MOQ5_REG_ERR_TYPE, rx, "ty.acquire-none-refused");
        ck(p == sentinel, "ty.acquire-none-published-no-pointer");
        p = sentinel;
        ck_refused(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE__COUNT, root, &p),
                   MOQ5_REG_ERR_TYPE, rx, "ty.acquire-unknown-refused");
        ck(p == sentinel, "ty.acquire-unknown-published-no-pointer");
        p = sentinel;
        ck_refused(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_RECEIVER, MOQ5_HANDLE_INVALID, &p),
                   MOQ5_REG_ERR_TYPE, root, "ty.acquire-root-as-receiver-refused");
        ck(p == sentinel, "ty.acquire-root-as-receiver-published-no-pointer");
        ck_eq(moq5_reg_retire_child(root, rx, MOQ5_TYPE_ENDPOINT), MOQ5_REG_ERR_TYPE,
              "ty.retire-endpoint-declaration-refused");
        ck_eq(moq5_reg_retire_child(root, rx, MOQ5_TYPE_RECEIVER_TRACK), MOQ5_REG_ERR_TYPE,
              "ty.retire-track-declaration-refused");
        ck_eq(rx_r.cleaned, 0, "ty.refused-declaration-destroyed-nothing");
    }

    /* -- 5. the root is ENDPOINT by invariant, and the lease says so -------- */
    p = sentinel;
    ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
          MOQ5_REG_OK, "ty.root-is-endpoint");
    ck(p == &root_r, "ty.root-pointer-identity");
    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "ty.root-release");
    ck_eq(moq5_reg_lease_root_any_thread(root, &p), MOQ5_REG_OK, "ty.lease-endpoint-ok");
    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "ty.lease-release");
    /* The lease takes no family from its caller, and no child can be leased as
     * one: the restriction stays structural. */
    p = sentinel;
    ck_refused(moq5_reg_lease_root_any_thread(rx, &p), MOQ5_REG_ERR_KIND, rx, "ty.lease-child-refused");
    ck(p == sentinel, "ty.lease-child-published-no-pointer");
    p = sentinel;
    ck_refused(moq5_reg_lease_root_any_thread(tx, &p), MOQ5_REG_ERR_KIND, tx, "ty.lease-sender-refused");
    ck(p == sentinel, "ty.lease-sender-published-no-pointer");

    /* -- 6. the family does not outlive its slot ---------------------------- */
    {
        uint32_t slot = moq5_handle_slot(rx);
        moq5_handle_t reused = 0;

        ck_eq(moq5_reg_retire_child(root, rx, MOQ5_TYPE_RECEIVER), MOQ5_REG_OK, "ty.retire-receiver");
        ck_eq(rx_r.cleaned, 1, "ty.receiver-cleaned-once");
        /* Its borrowed leaf went with it and is never cleaned. */
        p = sentinel;
        ck_refused(moq5_reg_acquire(rx_track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, rx, &p),
                   MOQ5_REG_ERR_STALE, rx_track, "ty.receiver-track-went-with-it");
        ck(p == sentinel, "ty.stale-track-published-no-pointer");
        ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)slot,
              "ty.the-freed-slot-is-the-next-one-taken");

        ck_eq(moq5_reg_insert_child(root, &reused_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER,
                                    fake_cleanup, &reused), MOQ5_REG_OK, "ty.reuse-as-sender");
        ck_eq((long long)moq5_handle_slot(reused), (long long)slot, "ty.same-slot-reused");
        /* The new tenant is a SENDER. The receiver that used to live in this
         * slot cannot be reached through it, by token or by family. */
        p = sentinel;
        ck_refused(moq5_reg_acquire(reused, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p),
                   MOQ5_REG_ERR_TYPE, reused, "ty.reused-slot-is-not-the-old-family");
        ck(p == sentinel, "ty.reused-slot-refusal-published-no-pointer");
        p = sentinel;
        ck_eq(moq5_reg_acquire(reused, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p),
              MOQ5_REG_OK, "ty.reused-slot-is-the-new-family");
        ck(p == &reused_r, "ty.reused-pointer-identity");
        ck_eq(moq5_reg_release(reused), MOQ5_REG_OK, "ty.reused-release");
        ck_eq(moq5_reg_retire_child(root, reused, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_TYPE,
              "ty.reused-slot-not-retirable-as-the-old-family");
        ck_eq(reused_r.cleaned, 0, "ty.reused-slot-refusal-destroyed-nothing");
    }

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "ty.retire-root");
    ck_eq(moq5_reg_retire_root(other), MOQ5_REG_OK, "ty.retire-other");
    ck_eq(tx_r.cleaned, 1, "ty.sender-cleaned-once");
    ck_eq(reused_r.cleaned, 1, "ty.reused-cleaned-once");
    ck_eq(root_r.cleaned, 1, "ty.root-cleaned-once");
    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "ty.live-restored");
}

/* -- the private child transaction ------------------------------------------
 *
 * Reserve before the object exists, abort if creating it fails, commit if it
 * succeeds. What is proved here is that a refusal costs nothing, that a
 * settlement is judged on what the ENTRY stores rather than on what the token
 * claims, and that an outstanding reservation blocks exactly the retirements
 * that would strand it and no others.
 */
static void test_child_transaction(void)
{
    fake_res_t root_r = { 900, 0 }, other_r = { 901, 0 }, rx_r = { 902, 0 },
               tx_r = { 903, 0 }, made_r = { 904, 0 }, deep_r = { 905, 0 };
    moq5_handle_t root = 0, other = 0, rx = 0, tx = 0, res = 0, made = 0, deep = 0;
    int unpublished = 0;
    void *sentinel = &unpublished;
    void *p = NULL;
    size_t live0 = moq5_reg_live_count();

    cleanup_count = 0;
    memset(cleanup_order, 0, sizeof cleanup_order);

    ck_eq(moq5_reg_insert_root(&root_r, fake_cleanup, &root), MOQ5_REG_OK, "tx.root");
    ck_eq(moq5_reg_insert_root(&other_r, fake_cleanup, &other), MOQ5_REG_OK, "tx.other-root");
    ck_eq(moq5_reg_insert_child(root, &rx_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &rx),
          MOQ5_REG_OK, "tx.receiver");
    ck_eq(moq5_reg_insert_child(root, &tx_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, fake_cleanup, &tx),
          MOQ5_REG_OK, "tx.sender");

    /* -- 1. declarations no reservation could satisfy, before any slot ------ */
    {
        uint32_t free_before = moq5_reg_test_next_free_slot();
        moq5_handle_t bad = 0;
        ck_eq(moq5_reg_reserve_child(tx, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, &bad),
              MOQ5_REG_ERR_KIND, "tx.reserve-root-kind-refused");
        ck_eq(moq5_reg_reserve_child(tx, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_NONE, &bad),
              MOQ5_REG_ERR_TYPE, "tx.reserve-none-type-refused");
        ck_eq(moq5_reg_reserve_child(tx, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER, &bad),
              MOQ5_REG_ERR_TYPE, "tx.reserve-incompatible-type-refused");
        ck_eq(moq5_reg_reserve_child(rx, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, &bad),
              MOQ5_REG_OK, "tx.reserve-under-the-receiver");
        ck_eq(moq5_reg_abort_child(rx, bad, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_OK, "tx.abort-that-one");
        ck_eq(moq5_reg_reserve_child(MOQ5_HANDLE_INVALID, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, &bad),
              MOQ5_REG_ERR_INVALID, "tx.reserve-no-owner-refused");
        ck_eq(bad, MOQ5_HANDLE_INVALID, "tx.refused-reserve-published-no-token");
        ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)free_before,
              "tx.refused-reserve-consumed-no-slot");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 4, "tx.refused-reserve-changed-no-count");
    }

    /* -- 2. a reservation is not reachable --------------------------------- */
    {
        uint32_t slot;
        uint32_t gen_before;
        ck_eq(moq5_reg_reserve_child(tx, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, &res),
              MOQ5_REG_OK, "tx.reserve");
        slot = moq5_handle_slot(res);
        gen_before = moq5_reg_test_slot_generation(slot);
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 4, "tx.a-reservation-is-not-live");
        p = sentinel;
        ck_refused(moq5_reg_acquire(res, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, tx, &p),
                   MOQ5_REG_ERR_STALE, res, "tx.reserved-cannot-be-acquired");
        ck(p == sentinel, "tx.reserved-acquire-published-no-pointer");
        p = sentinel;
        ck_refused(moq5_reg_lease_root_any_thread(res, &p), MOQ5_REG_ERR_STALE, res, "tx.reserved-cannot-be-leased");
        ck(p == sentinel, "tx.reserved-lease-published-no-pointer");
        /* Declared as an owned child, so the row reaches the resolve rather
         * than stopping at the declaration: a reservation resolves to nothing. */
        ck_eq(moq5_reg_retire_child(tx, res, MOQ5_TYPE_SENDER), MOQ5_REG_ERR_STALE,
              "tx.reserved-cannot-be-retired");

        /* -- 3. its owner, its ancestor and its root refuse to retire ------- */
        ck_eq(moq5_reg_retire_child(root, tx, MOQ5_TYPE_SENDER), MOQ5_REG_ERR_STATE,
              "tx.owner-retirement-refused-while-reserved");
        ck_eq(moq5_reg_retire_root(root), MOQ5_REG_ERR_STATE, "tx.root-retirement-refused-while-reserved");
        ck_eq(cleanup_count, 0, "tx.refused-retirement-cleaned-nothing");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 4, "tx.refused-retirement-changed-no-count");
        ck_eq(moq5_reg_acquire(tx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, root, &p), MOQ5_REG_OK,
              "tx.owner-still-live-after-a-refused-retirement");
        ck_eq(moq5_reg_release(tx), MOQ5_REG_OK, "tx.owner-release");
        ck_eq(moq5_reg_acquire(rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p), MOQ5_REG_OK,
              "tx.sibling-still-live");
        ck_eq(moq5_reg_release(rx), MOQ5_REG_OK, "tx.sibling-release");
        /* An unrelated subtree is untouched by any of it. */
        ck_eq(moq5_reg_retire_child(root, rx, MOQ5_TYPE_RECEIVER), MOQ5_REG_OK,
              "tx.unrelated-subtree-still-retires");
        ck_eq(rx_r.cleaned, 1, "tx.unrelated-subtree-cleaned-once");

        /* -- 4. settlement is judged on what the ENTRY stores --------------- */
        ck_eq(moq5_reg_commit_child(tx, res, MOQ5_TYPE_RECEIVER_TRACK, &made_r, fake_cleanup),
              MOQ5_REG_ERR_TYPE, "tx.commit-wrong-type-refused");
        ck_eq(moq5_reg_commit_child(root, res, MOQ5_TYPE_SENDER_TRACK, &made_r, fake_cleanup),
              MOQ5_REG_ERR_OWNER, "tx.commit-wrong-owner-refused");
        ck_eq(moq5_reg_abort_child(root, res, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_ERR_OWNER,
              "tx.abort-wrong-owner-refused");
        {
            /* The forged cross-transaction attempts, both ways. */
            moq5_handle_t as_root = moq5_handle_make((uint8_t)MOQ5_KIND_ROOT, slot, moq5_handle_generation(res));
            moq5_handle_t as_owned = moq5_handle_make((uint8_t)MOQ5_KIND_CHILD_OWNED, slot, moq5_handle_generation(res));
            ck_eq(moq5_reg_commit_root(as_root, &made_r, fake_cleanup), MOQ5_REG_ERR_KIND,
                  "tx.child-reservation-repacked-as-root-refused-by-commit");
            ck_eq(moq5_reg_abort_root(as_root), MOQ5_REG_ERR_KIND,
                  "tx.child-reservation-repacked-as-root-refused-by-abort");
            ck_eq(moq5_reg_commit_child(tx, as_owned, MOQ5_TYPE_SENDER, &made_r, fake_cleanup),
                  MOQ5_REG_ERR_KIND, "tx.child-reservation-repacked-as-owned-refused");
        }
        {
            moq5_handle_t stale = moq5_handle_make((uint8_t)MOQ5_KIND_CHILD_BORROWED, slot,
                                                   moq5_handle_generation(res) + 7u);
            ck_eq(moq5_reg_commit_child(tx, stale, MOQ5_TYPE_SENDER_TRACK, &made_r, fake_cleanup),
                  MOQ5_REG_ERR_STALE, "tx.commit-stale-generation-refused");
        }
        {
            pthread_t t;
            thread_arg_t a;
            memset(&a, 0, sizeof a);
            a.handle = res;
            a.ptr = (void *)(uintptr_t)tx;
            pt_must(pthread_create(&t, NULL, wrong_thread_commit_child, &a), "tx.thread-create");
            pt_must(pthread_join(t, NULL), "tx.thread-join");
            ck_eq(a.rc, MOQ5_REG_ERR_THREAD, "tx.commit-wrong-thread-refused");
        }
        ck_eq(moq5_reg_commit_child(tx, res, MOQ5_TYPE_SENDER_TRACK, NULL, NULL),
              MOQ5_REG_ERR_INVALID, "tx.commit-null-pointer-refused");
        ck_eq((long long)moq5_reg_test_slot_generation(slot), (long long)gen_before,
              "tx.refused-settlement-moved-no-generation");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 3, "tx.refused-settlement-changed-no-count");

        /* -- 5. the commit itself ------------------------------------------ */
        ck_eq(moq5_reg_commit_child(tx, res, MOQ5_TYPE_SENDER_TRACK, &made_r, fake_cleanup),
              MOQ5_REG_OK, "tx.commit");
        made = res;
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 4, "tx.commit-made-it-live");
        p = sentinel;
        ck_eq(moq5_reg_acquire(made, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, tx, &p),
              MOQ5_REG_OK, "tx.committed-acquires");
        ck(p == &made_r, "tx.committed-pointer-identity");
        ck_eq(moq5_reg_release(made), MOQ5_REG_OK, "tx.committed-release");
        /* Settling it again is refused by state: linear use, once. */
        ck_eq(moq5_reg_commit_child(tx, made, MOQ5_TYPE_SENDER_TRACK, &made_r, fake_cleanup),
              MOQ5_REG_ERR_STATE, "tx.repeat-commit-refused");
        ck_eq(moq5_reg_abort_child(tx, made, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_ERR_STATE,
              "tx.settled-abort-refused");
        /* And with the reservation settled, the owner retires again. */
        ck_eq(moq5_reg_retire_child(root, tx, MOQ5_TYPE_SENDER), MOQ5_REG_OK,
              "tx.owner-retires-once-the-reservation-has-settled");
        ck_eq(tx_r.cleaned, 1, "tx.owner-cleaned-once");
        /* A BORROWED entry is invalidated with its owner and never cleaned --
         * which is exactly why a track's C removal can never be delegated to
         * registry retirement. */
        ck_eq(made_r.cleaned, 0, "tx.committed-borrowed-child-is-never-cleaned");
        p = sentinel;
        ck_refused(moq5_reg_acquire(made, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, tx, &p),
                   MOQ5_REG_ERR_STALE, made, "tx.committed-child-went-with-its-owner");
        ck(p == sentinel, "tx.stale-committed-published-no-pointer");
    }

    /* -- 6. abort conserves the slot AND the physical generation ------------ */
    {
        moq5_handle_t sender2 = 0;
        fake_res_t s2 = { 906, 0 };
        ck_eq(moq5_reg_insert_child(root, &s2, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_SENDER, fake_cleanup, &sender2),
              MOQ5_REG_OK, "tx.second-sender");

        /* A VIRGIN slot, reached by filling the table so the next index is one
         * GROWTH would expose: its physical generation is zero, and an abort
         * must leave it zero rather than the first issued value. Filling it
         * with a live owner present is also the growth case: the reservation
         * below is what makes the table grow. */
        static moq5_handle_t filler[MOQ5_REGISTRY_MAX_ENTRIES];
        static fake_res_t filler_res[MOQ5_REGISTRY_MAX_ENTRIES];
        uint32_t filled = 0;
        while (moq5_reg_test_next_free_slot() != moq5_reg_test_capacity() &&
               filled < MOQ5_REGISTRY_MAX_ENTRIES) {
            if (moq5_reg_insert_child(sender2, &filler_res[filled], MOQ5_KIND_CHILD_OWNED,
                                      MOQ5_TYPE_RECEIVER, NULL, &filler[filled]) != MOQ5_REG_OK) {
                break;
            }
            filled++;
        }
        uint32_t virgin = moq5_reg_test_next_free_slot();
        uint32_t capacity_before = moq5_reg_test_capacity();
        ck_eq((long long)virgin, (long long)capacity_before, "tx.the-next-slot-is-one-growth-would-expose");
        ck_eq((long long)moq5_reg_test_slot_generation(virgin), 0, "tx.that-slot-is-virgin");
        ck_eq(moq5_reg_reserve_child(sender2, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, &res),
              MOQ5_REG_OK, "tx.reserve-virgin");
        ck(moq5_reg_test_capacity() > capacity_before, "tx.the-reservation-grew-the-table");
        ck_eq((long long)moq5_handle_slot(res), (long long)virgin, "tx.reserve-took-the-virgin-slot");
        ck_eq(moq5_reg_abort_child(sender2, res, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_OK, "tx.abort-virgin");
        /* The child paths compare stored identity BEFORE state, so a freed slot
         * no longer names that kind and answers KIND rather than STATE. The
         * refusal is what matters; its category is pinned here so it cannot
         * drift silently. */
        ck_eq(moq5_reg_abort_child(sender2, res, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_ERR_KIND,
              "tx.double-abort-refused");
        ck_eq((long long)moq5_reg_test_slot_generation(virgin), 0, "tx.abort-restored-the-virgin-generation");
        ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)virgin, "tx.abort-freed-the-slot");
        ck_eq((long long)moq5_reg_live_count(), (long long)live0 + 3 + (long long)filled,
              "tx.abort-changed-no-count");
        /* rx and tx are the only cleanups so far; the borrowed commit had none. */
        ck_eq(cleanup_count, 2, "tx.abort-cleaned-nothing");
        /* The filler goes away again so the rest of this row reads the ordinary
         * geometry; its entries carry no cleanup, so nothing is counted. */
        for (uint32_t f = 0; f < filled; f++) {
            ck_eq(moq5_reg_retire_child(sender2, filler[f], MOQ5_TYPE_RECEIVER), MOQ5_REG_OK, "tx.filler-retires");
        }

        /* A USED slot: retire a child to put a real generation on it, then
         * reserve and abort, and prove that generation is exactly restored. */
        moq5_handle_t doomed = 0;
        fake_res_t d = { 907, 0 };
        ck_eq(moq5_reg_insert_child(sender2, &d, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &doomed),
              MOQ5_REG_OK, "tx.doomed");
        uint32_t used = moq5_handle_slot(doomed);
        ck_eq(moq5_reg_retire_child(sender2, doomed, MOQ5_TYPE_RECEIVER), MOQ5_REG_OK, "tx.retire-doomed");
        uint32_t used_gen = moq5_reg_test_slot_generation(used);
        ck(used_gen != 0u, "tx.the-used-slot-carries-a-real-generation");
        ck_eq(moq5_reg_reserve_child(sender2, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_SENDER_TRACK, &res),
              MOQ5_REG_OK, "tx.reserve-used");
        ck_eq((long long)moq5_handle_slot(res), (long long)used, "tx.reserve-took-the-used-slot");
        ck_eq((long long)moq5_handle_generation(res), (long long)used_gen, "tx.reserve-read-the-generation");
        ck_eq(moq5_reg_abort_child(sender2, res, MOQ5_TYPE_SENDER_TRACK), MOQ5_REG_OK, "tx.abort-used");
        ck_eq((long long)moq5_reg_test_slot_generation(used), (long long)used_gen,
              "tx.abort-restored-the-used-generation");

        /* -- 7. a deep reservation blocks its ANCESTOR, exactly ------------- */
        ck_eq(moq5_reg_insert_child(sender2, &deep_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, fake_cleanup, &deep),
              MOQ5_REG_OK, "tx.deep-child");
        ck_eq(moq5_reg_reserve_child(deep, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, &res),
              MOQ5_REG_OK, "tx.reserve-deep");
        ck_eq(moq5_reg_retire_child(sender2, deep, MOQ5_TYPE_RECEIVER), MOQ5_REG_ERR_STATE,
              "tx.direct-owner-refused");
        ck_eq(moq5_reg_retire_child(root, sender2, MOQ5_TYPE_SENDER), MOQ5_REG_ERR_STATE,
              "tx.ancestor-refused");
        ck_eq(moq5_reg_retire_root(root), MOQ5_REG_ERR_STATE, "tx.root-refused");
        ck_eq(moq5_reg_retire_root(other), MOQ5_REG_OK, "tx.an-unrelated-root-still-retires");
        /* Exactly the four legitimate cleanups so far -- the receiver, the
         * first sender, the doomed child and the unrelated root -- and not one
         * from any refused retirement. */
        ck_eq(cleanup_count, 4, "tx.refused-ancestor-retirement-cleaned-nothing-more");
        ck_eq(moq5_reg_abort_child(deep, res, MOQ5_TYPE_RECEIVER_TRACK), MOQ5_REG_OK, "tx.abort-deep");
        ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "tx.root-retires-once-it-has-settled");
    }

    ck_eq((long long)moq5_reg_live_count(), (long long)live0, "tx.live-restored");
}

static void test_thread_confinement(void)
{
    fake_res_t r = { 300, 0 };
    moq5_handle_t root = 0;
    pthread_t t;
    thread_arg_t arg;

    ck_eq(moq5_reg_insert_root(&r, NULL, &root), MOQ5_REG_OK, "tc.root");

    memset(&arg, 0, sizeof arg);
    arg.handle = root;
    pt_must(pthread_create(&t, NULL, wrong_thread_acquire, &arg), "confine.create");
    pt_must(pthread_join(t, NULL), "confine.join");

    ck_eq(arg.rc, MOQ5_REG_ERR_THREAD, "tc.wrong-thread-refused");
    ck(arg.ptr == (void *)(uintptr_t)0xDEADBEEF, "tc.no-pointer-exposed-before-checks");

    /* the authority thread still succeeds */
    void *p = NULL;
    ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK,
          "tc.authority-thread-ok");
    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "tc.release");

    /* The any-thread lease is the deliberate exception: it is NOT thread-checked,
     * and the lease test below proves it succeeds from another thread. Here the
     * point is only that the confined path and the lease path differ. */
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "tc.retire");
}

/* -- a live lease blocks destruction until it drains ------------------------ */
typedef struct {
    moq5_handle_t handle;
    barrier_t *pinned;
    barrier_t *may_unpin;
} lease_arg_t;

static volatile int lease_cleanup_ran;
static volatile int lease_still_pinned;

static void lease_cleanup(void *p)
{
    ((fake_res_t *)p)->cleaned++;
    observe_lock_free();
    /* If retirement destroyed while a lease was live, this runs with the pin
     * still held -- which is the use-after-free this protocol exists to stop. */
    if (lease_still_pinned) {
        lease_cleanup_ran = -1;
    } else {
        lease_cleanup_ran = 1;
    }
}

/* An exactly typed adapter. Invoking barrier_wait(barrier_t *) through a
 * void (*)(void *) is an incompatible function-pointer call, which is undefined
 * regardless of how the ABI happens to behave (chats/libmoq/1236 F1). */
static void barrier_wait_hook(void *arg)
{
    barrier_wait((barrier_t *)arg);
}

static void *lease_holder(void *v)
{
    lease_arg_t *a = (lease_arg_t *)v;
    void *p = NULL;

    int leased = (moq5_reg_lease_root_any_thread(a->handle, &p) == MOQ5_REG_OK);

    /* BOTH rendezvous happen whether or not the lease was granted. A refused
     * lease used to return early, leaving the retiring thread's hook waiting
     * forever on a holder that had already exited -- so a product mutant that
     * refuses leases would hang instead of failing by name. */
    lease_still_pinned = leased ? 1 : -1;
    barrier_wait(a->pinned);      /* the retiring thread may now start */
    barrier_wait(a->may_unpin);   /* it has started and is waiting for the pin */
    if (leased) {
        lease_still_pinned = 0;
        moq5_reg_release(a->handle);
    }
    return NULL;
}

static void test_lease_blocks_destruction(void)
{
    /* A deterministic MULTI-ROUND replay. One round proves the interleaving
     * once; repeating it over reused slots and advancing generations is what
     * shows the protocol holds as the table churns. Every round is barrier-
     * driven, every pthread call is checked, and the round's pin, cleanup and
     * live state must balance before the next begins. */
    enum { ROUNDS = 32 };
    fake_res_t r = { 400, 0 };
    moq5_handle_t seen[ROUNDS];
    int completed = 0;

    for (int round = 0; round < ROUNDS; round++) {
        moq5_handle_t root = 0;
        barrier_t pinned, may_unpin;
        pthread_t t;
        lease_arg_t arg;

        lease_cleanup_ran = 0;
        lease_still_pinned = 0;
        barrier_init(&pinned, 2);
        barrier_init(&may_unpin, 2);

        if (moq5_reg_insert_root(&r, lease_cleanup, &root) != MOQ5_REG_OK) {
            /* Nothing was created, so the round's own primitives are the only
             * state to release before reporting. */
            barrier_destroy(&pinned);
            barrier_destroy(&may_unpin);
            ck(0, "lease.round-insert");
            break;
        }
        seen[round] = root;

        memset(&arg, 0, sizeof arg);
        arg.handle = root;
        arg.pinned = &pinned;
        arg.may_unpin = &may_unpin;
        /* A thread that cannot be created leaves no safe way to continue: the
         * rendezvous below would wait for a holder that does not exist. pt_must
         * therefore reports the failure, flushes the ledger and terminates the
         * process immediately -- it does NOT retire this round's root or destroy
         * its primitives, because the run ends here and there is nothing left to
         * observe them. */
        pt_must(pthread_create(&t, NULL, lease_holder, &arg), "lease.create");

        barrier_wait(&pinned);
        if (lease_still_pinned != 1) {
            ck(0, "lease.round-held");
        }

        moq5_reg_test_on_retire_wait(&may_unpin, barrier_wait_hook);
        moq5_reg_result_t rc = moq5_reg_retire_root(root);
        moq5_reg_test_on_retire_wait(NULL, NULL);

        pt_must(pthread_join(t, NULL), "lease.join");

        if (rc != MOQ5_REG_OK || lease_cleanup_ran != 1
            || moq5_reg_live_count() != 0
            || moq5_reg_test_last_cleanup_was_retiring_thread() != 1) {
            ck(0, "lease.round-balanced");
        }
        barrier_destroy(&pinned);
        barrier_destroy(&may_unpin);
        completed++;
    }

    /* Every round must have completed; the comparisons below read ONLY that
     * many entries, so a short run can never scan uninitialized tokens. */
    ck_eq(completed, ROUNDS, "lease.all-rounds-completed");
    ck_eq(r.cleaned, completed, "lease.cleaned-once-per-round");
    ck_eq((long long)moq5_reg_live_count(), 0, "lease.live-zero");

    /* PAIRWISE unique, not merely different from the previous round, and at
     * least one slot reused between SOME pair, not only against round zero. */
    int all_unique = 1;
    int reused_slot = 0;
    for (int i = 0; i < completed; i++) {
        for (int j = i + 1; j < completed; j++) {
            if (seen[i] == seen[j]) {
                all_unique = 0;
            }
            if (moq5_handle_slot(seen[i]) == moq5_handle_slot(seen[j])) {
                reused_slot = 1;
            }
        }
    }
    ck(all_unique, "lease.every-token-pairwise-unique");
    ck(reused_slot, "lease.slots-were-reused-across-rounds");
}

/* -- the registry lock is never held across a caller's operation ------------- */
static void test_no_lock_across_operation(void)
{
    fake_res_t r = { 500, 0 };
    moq5_handle_t root = 0;
    void *p = NULL;

    ck_eq(moq5_reg_insert_root(&r, NULL, &root), MOQ5_REG_OK, "nolock.root");
    ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK,
          "nolock.acquire");

    /* With the pointer in hand and a pin held, the registry must still answer:
     * if acquire returned while holding the lock, this would deadlock. */
    ck((long long)moq5_reg_live_count() >= 1, "nolock.registry-answers-while-pinned");
    moq5_handle_t other = 0;
    fake_res_t r2 = { 501, 0 };
    ck_eq(moq5_reg_insert_root(&r2, NULL, &other), MOQ5_REG_OK, "nolock.insert-while-pinned");

    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "nolock.release");
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "nolock.retire");
    ck_eq(moq5_reg_retire_root(other), MOQ5_REG_OK, "nolock.retire2");
}


/* -- a real tree: root -> owned child -> grandchild -------------------------- */
/* Exact descriptors, not bare tokens.
 *
 * The old probe leased every token through the ROOT-ONLY any-thread API, so a
 * fully LIVE child was refused by KIND and counted as "already dead" -- a false
 * pass that let a root-only-invalidation mutant through (chats/libmoq/1234 F1).
 * A descriptor carries the kind and the exact direct owner, so the engine-
 * confined acquire can be asked the real question. */
typedef struct {
    moq5_handle_t handle;
    moq5_kind_t kind;
    moq5_type_t type;
    moq5_handle_t owner;
} tree_desc_t;

static int invalidated_before_first_cleanup;
static tree_desc_t tree_descs[4];
static int tree_desc_count;

static void tree_cleanup(void *p)
{
    fake_res_t *r = (fake_res_t *)p;

    /* On the FIRST cleanup, every token in the tree must already be dead.
     * Checking staleness only after retirement returns would not distinguish
     * "invalidated first" from "invalidated as we went". */
    if (cleanup_count == 0) {
        void *q = NULL;
        int all_dead = 1;

        /* Probing takes the registry lock, which would DEADLOCK under a mutant
         * that holds it across this callback. So the lock is observed first and
         * the probe is skipped when it is held: the violation is recorded by
         * observe_lock_free() and the mutant terminates instead of hanging
         * (chats/libmoq/1234 F2). */
        if (moq5_reg_test_lock_is_free()) {
            for (int i = 0; i < tree_desc_count; i++) {
                moq5_reg_result_t rc = moq5_reg_acquire(tree_descs[i].handle,
                                                        tree_descs[i].kind,
                                                        tree_descs[i].type,
                                                        tree_descs[i].owner, &q);
                if (rc != MOQ5_REG_ERR_STATE) {
                    /* Anything else means this entry was not RETIRING yet. */
                    all_dead = 0;
                    if (rc == MOQ5_REG_OK) {
                        moq5_reg_release(tree_descs[i].handle);
                    }
                }
            }
            invalidated_before_first_cleanup = all_dead;
        }
    }
    observe_lock_free();
    r->cleaned++;
    if (cleanup_count < (int)(sizeof cleanup_order / sizeof cleanup_order[0])) {
        cleanup_order[cleanup_count] = r->id;
    }
    cleanup_count++;
}

static void test_tree(void)
{
    fake_res_t root_r = { 600, 0 }, child_r = { 601, 0 }, grand_r = { 602, 0 },
               borrow_r = { 603, 0 };
    moq5_handle_t root = 0, child = 0, grand = 0, borrowed = 0;
    void *p = NULL;

    cleanup_count = 0;
    invalidated_before_first_cleanup = 0;
    cleanup_lock_free_count = 0;
    cleanup_lock_violations = 0;
    memset(cleanup_order, 0, sizeof cleanup_order);

    ck_eq(moq5_reg_insert_root(&root_r, tree_cleanup, &root), MOQ5_REG_OK, "tree.root");
    ck_eq(moq5_reg_insert_child(root, &child_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, tree_cleanup, &child),
          MOQ5_REG_OK, "tree.child");
    ck_eq(moq5_reg_insert_child(child, &grand_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, tree_cleanup, &grand),
          MOQ5_REG_OK, "tree.grandchild-under-owned-child");
    ck_eq(moq5_reg_insert_child(child, &borrow_r, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, tree_cleanup, &borrowed),
          MOQ5_REG_OK, "tree.borrowed-grandchild");

    /* a borrowed entry is a LEAF: nothing may hang off it */
    moq5_handle_t nope = 0;
    ck_eq(moq5_reg_insert_child(borrowed, &grand_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &nope),
          MOQ5_REG_ERR_KIND, "tree.borrowed-cannot-own");
    ck_eq(nope, MOQ5_HANDLE_INVALID, "tree.borrowed-owner-no-token");

    /* EXACT direct-owner rejection: the grandchild's owner is the child, not
     * the root, even though both are its ancestors. */
    ck_refused(moq5_reg_acquire(grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p),
               MOQ5_REG_ERR_OWNER, grand, "tree.grandchild-under-root-refused");
    ck_eq(moq5_reg_acquire(grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, child, &p), MOQ5_REG_OK,
          "tree.grandchild-under-child-ok");
    ck_eq(moq5_reg_release(grand), MOQ5_REG_OK, "tree.release");

    tree_descs[0] = (tree_desc_t){ root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID };
    tree_descs[1] = (tree_desc_t){ child, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root };
    tree_descs[2] = (tree_desc_t){ grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, child };
    tree_descs[3] = (tree_desc_t){ borrowed, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK, child };
    tree_desc_count = 4;

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "tree.retire");

    ck_eq(invalidated_before_first_cleanup, 1, "tree.whole-tree-retiring-before-first-cleanup");
    ck_eq(cleanup_lock_violations, 0, "tree.lock-free-during-every-cleanup");
    ck_eq(grand_r.cleaned, 1, "tree.grandchild-cleaned-once");
    ck_eq(child_r.cleaned, 1, "tree.child-cleaned-once");
    ck_eq(root_r.cleaned, 1, "tree.root-cleaned-once");
    ck_eq(borrow_r.cleaned, 0, "tree.borrowed-never-cleaned");
    ck_eq(cleanup_count, 3, "tree.cleanup-count");
    ck_eq(cleanup_order[0], 602, "tree.deepest-first");
    ck_eq(cleanup_order[1], 601, "tree.then-child");
    ck_eq(cleanup_order[2], 600, "tree.root-last");
    ck_eq((long long)moq5_reg_live_count(), 0, "tree.live-zero");
}

/* -- mutation is engine-confined too ---------------------------------------- */
typedef struct {
    moq5_handle_t owner;
    moq5_reg_result_t rc;
    moq5_handle_t out;
} insert_arg_t;

static fake_res_t off_thread_res = { 700, 0 };

static void *wrong_thread_insert(void *v)
{
    insert_arg_t *a = (insert_arg_t *)v;
    a->out = (moq5_handle_t)0xAAAAAAAAAAAAAAAAull; /* poisoned */
    a->rc = moq5_reg_insert_child(a->owner, &off_thread_res, MOQ5_KIND_CHILD_OWNED,
                                  MOQ5_TYPE_RECEIVER, fake_cleanup, &a->out);
    return NULL;
}

static void *wrong_thread_retire(void *v)
{
    insert_arg_t *a = (insert_arg_t *)v;
    a->rc = moq5_reg_retire_root(a->owner);
    return NULL;
}

static void test_mutation_confinement(void)
{
    fake_res_t r = { 701, 0 };
    moq5_handle_t root = 0;
    pthread_t t;
    insert_arg_t arg;

    ck_eq(moq5_reg_insert_root(&r, fake_cleanup, &root), MOQ5_REG_OK, "mc.root");
    size_t live_before = moq5_reg_live_count();
    uint32_t next_before = moq5_reg_test_next_free_slot();
    uint32_t cap_before = moq5_reg_test_capacity();
    uint32_t gen_before = moq5_reg_test_slot_generation(next_before);

    memset(&arg, 0, sizeof arg);
    arg.owner = root;
    pt_must(pthread_create(&t, NULL, wrong_thread_insert, &arg), "mc.insert-create");
    pt_must(pthread_join(t, NULL), "mc.insert-join");

    ck_eq(arg.rc, MOQ5_REG_ERR_THREAD, "mc.wrong-thread-insert-refused");
    ck_eq(arg.out, MOQ5_HANDLE_INVALID, "mc.no-token-published");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "mc.live-unchanged");
    ck_eq((long long)moq5_reg_test_next_free_slot(), (long long)next_before,
          "mc.no-slot-consumed");
    /* Capacity and the candidate slot's generation are observed SEPARATELY:
     * the next-free slot alone is not a synonym for either
     * (chats/libmoq/1234 F6). */
    ck_eq((long long)moq5_reg_test_capacity(), (long long)cap_before, "mc.capacity-unchanged");
    ck_eq((long long)moq5_reg_test_slot_generation(next_before), (long long)gen_before,
          "mc.generation-unchanged");
    ck_eq(off_thread_res.cleaned, 0, "mc.no-cleanup-invoked");

    memset(&arg, 0, sizeof arg);
    arg.owner = root;
    pt_must(pthread_create(&t, NULL, wrong_thread_retire, &arg), "mc.retire-create");
    pt_must(pthread_join(t, NULL), "mc.retire-join");
    ck_eq(arg.rc, MOQ5_REG_ERR_THREAD, "mc.wrong-thread-retire-refused");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "mc.still-live");

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "mc.retire-on-authority");
}

/* -- boundaries: pins, capacity, NULL --------------------------------------- */
static void test_boundaries(void)
{
    fake_res_t r = { 800, 0 };
    moq5_handle_t root = 0;
    void *p = NULL;

    /* NULL resources are refused transactionally: a handle that resolves to
     * NULL would make a later dereference the first effective validation. */
    size_t live_before = moq5_reg_live_count();
    moq5_handle_t nullh = (moq5_handle_t)0x1234;
    ck_eq(moq5_reg_insert_root(NULL, fake_cleanup, &nullh), MOQ5_REG_ERR_INVALID,
          "bd.null-root-refused");
    ck_eq(nullh, MOQ5_HANDLE_INVALID, "bd.null-root-no-token");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "bd.null-live-unchanged");

    ck_eq(moq5_reg_insert_root(&r, NULL, &root), MOQ5_REG_OK, "bd.root");
    moq5_handle_t nullc = (moq5_handle_t)0x1234;
    ck_eq(moq5_reg_insert_child(root, NULL, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, NULL, &nullc),
          MOQ5_REG_ERR_INVALID, "bd.null-child-refused");
    ck_eq(nullc, MOQ5_HANDLE_INVALID, "bd.null-child-no-token");

    /* A pin at the representable limit is REFUSED, not wrapped: a wrapped count
     * would make a pinned entry look free to a retirement about to destroy it. */
    ck(moq5_reg_test_set_pins(root, MOQ5_REGISTRY_MAX_PINS), "bd.prime-pins");
    ck_refused(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_PINS, root, "bd.pin-limit-refused");
    ck(moq5_reg_test_set_pins(root, 0), "bd.restore-pins");
    ck_eq(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p), MOQ5_REG_OK,
          "bd.pin-ok-after-restore");
    ck_eq(moq5_reg_release(root), MOQ5_REG_OK, "bd.release");
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "bd.retire");

    /* The explicit capacity boundary is refused by name. */
    static moq5_handle_t all[MOQ5_REGISTRY_MAX_ENTRIES];
    uint32_t n = 0;
    while (n < MOQ5_REGISTRY_MAX_ENTRIES) {
        if (moq5_reg_insert_root(&r, NULL, &all[n]) != MOQ5_REG_OK) {
            break;
        }
        n++;
    }
    /* The reachable limit is the declared maximum MINUS the slots the wrap
     * policy has permanently retired. Losing a slot after 2^32 reuses is the
     * documented cost of never resurrecting a token, so the expectation is
     * computed rather than asserted as a bare 4096. */
    uint32_t retired = 0;
    for (uint32_t i = 0; i < MOQ5_REGISTRY_MAX_ENTRIES; i++) {
        if (moq5_reg_test_slot_retired(i)) {
            retired++;
        }
    }
    ck_eq((long long)(n + retired), (long long)MOQ5_REGISTRY_MAX_ENTRIES,
          "bd.fills-to-the-declared-limit-less-retired");
    ck(retired >= 1, "bd.wrap-policy-really-retired-a-slot");

    moq5_handle_t over = (moq5_handle_t)0x1234;
    ck_eq(moq5_reg_insert_root(&r, NULL, &over), MOQ5_REG_ERR_CAPACITY, "bd.capacity-refused");
    ck_eq(over, MOQ5_HANDLE_INVALID, "bd.capacity-no-token");

    /* A RESERVATION refuses on the same boundary, and leaves nothing behind:
     * no token, no live entry, and a table that still admits exactly what it
     * did before -- so a refused reservation cannot consume a slot the caller
     * would never learn about. */
    {
        moq5_handle_t rsv = (moq5_handle_t)0x5678;
        size_t live_at_cap = moq5_reg_live_count();
        ck_eq(moq5_reg_reserve_root(&rsv), MOQ5_REG_ERR_CAPACITY, "bd.reserve-refused");
        ck_eq(rsv, MOQ5_HANDLE_INVALID, "bd.reserve-no-token");
        ck_eq((long long)moq5_reg_live_count(), (long long)live_at_cap,
              "bd.reserve-refusal-changed-nothing");
        ck_eq(moq5_reg_reserve_root(&rsv), MOQ5_REG_ERR_CAPACITY, "bd.reserve-refused-again");
    }

    /* Drained as ONE assertion: 4095 individual rows would drown the ledger. */
    uint32_t drained_ok = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (moq5_reg_retire_root(all[i]) == MOQ5_REG_OK) {
            drained_ok++;
        }
    }
    ck_eq((long long)drained_ok, (long long)n, "bd.drain-all");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "bd.live-restored");
}

/* -- a new pin is refused once the tree is RETIRING -------------------------- */
static volatile moq5_reg_result_t pin_during_retire_rc;
static moq5_handle_t pin_probe_handle;

static void pin_probe(void *arg)
{
    void *p = NULL;
    (void)arg;
    /* Called by the retiring thread once the tree is RETIRING and before the
     * pins are awaited: a new pin must be refused by STATE, not granted. */
    pin_during_retire_rc =
        moq5_reg_lease_root_any_thread(pin_probe_handle, &p);
    if (pin_during_retire_rc == MOQ5_REG_OK) {
        moq5_reg_release(pin_probe_handle);
    }
}

static void test_no_new_pin_while_retiring(void)
{
    fake_res_t r = { 900, 0 };
    moq5_handle_t root = 0;

    ck_eq(moq5_reg_insert_root(&r, fake_cleanup, &root), MOQ5_REG_OK, "retiring.root");
    pin_probe_handle = root;
    pin_during_retire_rc = MOQ5_REG_OK;
    moq5_reg_test_on_retire_wait(NULL, pin_probe);
    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "retiring.retire");
    moq5_reg_test_on_retire_wait(NULL, NULL);
    ck_eq(pin_during_retire_rc, MOQ5_REG_ERR_STATE, "retiring.new-pin-refused");
    ck_eq((long long)moq5_reg_live_count(), 0, "retiring.live-zero");
}


/* -- a cleanup that makes the table GROW while its tree is retiring ---------- */
static moq5_handle_t grow_roots[64];
static uint32_t grow_count;
static uint32_t grow_capacity_before;
static uint32_t grow_capacity_during;
static fake_res_t grow_res = { 1000, 0 };
static int grow_order[8];
static int grow_order_count;

static void growing_cleanup(void *p)
{
    fake_res_t *r = (fake_res_t *)p;

    if (grow_order_count < (int)(sizeof grow_order / sizeof grow_order[0])) {
        grow_order[grow_order_count] = r->id;
    }
    grow_order_count++;
    r->cleaned++;

    /* Re-entrant work needs the registry lock, so it is attempted only when the
     * lock is observably free. Under a mutant that holds it across callbacks
     * this records the violation and skips, so the mutant TERMINATES with named
     * diagnostics instead of deadlocking here (chats/libmoq/1236 F1/F2). */
    observe_lock_free();
    if (grow_order_count == 1 && moq5_reg_test_lock_is_free()) {
        grow_capacity_before = moq5_reg_test_capacity();
        while (grow_count < (uint32_t)(sizeof grow_roots / sizeof grow_roots[0])) {
            if (moq5_reg_insert_root(&grow_res, NULL, &grow_roots[grow_count]) != MOQ5_REG_OK) {
                break;
            }
            grow_count++;
            if (moq5_reg_test_capacity() > grow_capacity_before) {
                break;
            }
        }
        grow_capacity_during = moq5_reg_test_capacity();
    }
}

static void test_growth_during_cleanup(void)
{
    fake_res_t root_r = { 1001, 0 }, child_r = { 1002, 0 }, grand_r = { 1003, 0 };
    moq5_handle_t root = 0, child = 0, grand = 0;
    void *p = NULL;

    grow_count = 0;
    grow_order_count = 0;
    grow_capacity_before = 0;
    grow_capacity_during = 0;
    memset(grow_order, 0, sizeof grow_order);

    size_t live_before = moq5_reg_live_count();
    long balance_before = moq5_reg_test_alloc_balance();

    ck_eq(moq5_reg_insert_root(&root_r, growing_cleanup, &root), MOQ5_REG_OK, "grow.root");
    ck_eq(moq5_reg_insert_child(root, &child_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, growing_cleanup, &child),
          MOQ5_REG_OK, "grow.child");
    ck_eq(moq5_reg_insert_child(child, &grand_r, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, growing_cleanup, &grand),
          MOQ5_REG_OK, "grow.grandchild");

    ck_eq(moq5_reg_retire_root(root), MOQ5_REG_OK, "grow.retire");

    ck(grow_capacity_during > grow_capacity_before, "grow.capacity-really-increased");
    ck(grow_count > 0, "grow.reentrant-roots-inserted");

    /* The ORIGINAL tree still finished correctly across that reallocation. */
    ck_eq(grow_order_count, 3, "grow.original-cleanups-ran");
    ck_eq(grow_order[0], 1003, "grow.deepest-first-across-growth");
    ck_eq(grow_order[1], 1002, "grow.then-child");
    ck_eq(grow_order[2], 1001, "grow.root-last");
    ck_eq(grand_r.cleaned, 1, "grow.grandchild-once");
    ck_eq(child_r.cleaned, 1, "grow.child-once");
    ck_eq(root_r.cleaned, 1, "grow.root-once");

    /* Every original token is dead. */
    ck_refused(moq5_reg_acquire(grand, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, child, &p),
               MOQ5_REG_ERR_STALE, grand, "grow.grandchild-stale");
    ck_refused(moq5_reg_acquire(child, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER, root, &p),
               MOQ5_REG_ERR_STALE, child, "grow.child-stale");
    ck_refused(moq5_reg_acquire(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p),
               MOQ5_REG_ERR_STALE, root, "grow.root-stale");

    /* The reentrant roots are ordinary live entries and retire normally. */
    uint32_t retired_ok = 0;
    for (uint32_t i = 0; i < grow_count; i++) {
        if (moq5_reg_retire_root(grow_roots[i]) == MOQ5_REG_OK) {
            retired_ok++;
        }
    }
    ck_eq((long long)retired_ok, (long long)grow_count, "grow.reentrant-roots-retire");
    ck_eq((long long)moq5_reg_live_count(), (long long)live_before, "grow.live-restored");
    /* Growth legitimately frees the old table and installs a new one, so the
     * balance returns to where it started rather than growing. */
    ck_eq(moq5_reg_test_alloc_balance(), balance_before, "grow.allocator-balance-restored");
}

int main(void)
{
    test_token_codec();
    test_identity();
    test_foreign_owner();
    test_reuse_and_wrap();
    test_teardown();
    test_transactional_failure();
    test_reservation();
    test_ready();
    test_thread_confinement();
    test_child_retirement();
    test_concrete_type();
    test_child_transaction();
    test_lease_blocks_destruction();
    test_no_lock_across_operation();
    test_tree();
    test_mutation_confinement();
    test_no_new_pin_while_retiring();
    /* Growth must run BEFORE the capacity test: once the table has been filled
     * to its maximum it can never grow again, and the reentrant-growth case
     * would pass vacuously. */
    test_growth_during_cleanup();
    test_boundaries();

    ck_eq((long long)moq5_reg_live_count(), 0, "final.live-zero");
    /* The registry's own table is a process-wide allocation that legitimately
     * outlives every entry, and growth frees the old table as it installs the
     * new one. So the balance settles at exactly ONE live allocation -- the
     * table -- not zero, and that boundary is named rather than assumed. */
    ck_eq(moq5_reg_test_alloc_balance(), 1, "final.allocator-balance-one-table");

    printf("\nregistry oracle: %d check(s), %d failure(s)\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
