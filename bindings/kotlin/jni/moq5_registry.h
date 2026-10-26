/* The private native handle registry.
 *
 * Kotlin never sees a pointer. It sees an opaque 64-bit token that this
 * registry mints, and every crossing is validated here before any pointer is
 * exposed. The registry is lifetime metadata ONLY: it is not a command queue,
 * not a callback dispatcher, not a status poller, and it owns no
 * application-visible state.
 *
 * ONE registry, process-wide, private to this DSO. That is a deliberate
 * exception to the general "no process-global mutable state" rule and it is
 * what makes opaque handles bootstrappable at all: a per-owner table would
 * still have to be named across JNI, which needs either a raw pointer in a
 * `Long` (forbidden) or an outer registry anyway. "Foreign" therefore means a
 * token presented with the wrong EXPECTED OWNER in this one registry --
 * compared against the owner's exact token including its generation, never
 * inferred from a probabilistic salt (chats/libmoq/1230 F1).
 *
 * TOKEN LAYOUT
 *
 *     bits 63..32  generation (32)
 *     bits 31..8   slot       (24)
 *     bits  7..0   kind       (8)
 *
 * Token 0 is invalid and kind 0 is invalid, so a zeroed `Long` can never
 * resolve. The FIRST generation issued for every slot has bit 31 set, so an
 * ordinary valid token has bit 63 set and crosses JNI as a NEGATIVE `jlong`.
 * That is deliberate: the full-width contract is then exercised by every normal
 * allocation rather than by a contrived fixture, and any narrowing or signed
 * comparison fails immediately instead of subtly.
 *
 * A slot's generation advances on every retirement. A slot whose generation
 * would wrap is PERMANENTLY RETIRED rather than reissued, so a previously
 * issued token can never become live again.
 *
 * THREADING
 *
 * Stateful work stays engine-confined: `moq5_reg_acquire` verifies that the
 * caller is the root's recorded authority thread before exposing a pointer.
 * The one deliberate exception is `moq5_reg_lease_root_any_thread`, for operations
 * whose C contract is itself any-thread-safe: the endpoint wake, the interrupt
 * latch, and the application-thread drain. Both PIN the entry: the registry lock is taken to validate and pin,
 * released for the call, and retaken to unpin. The lock is never held across a
 * caller's operation or a cleanup callback, and an unpinned pointer is never
 * returned.
 *
 * Retirement invalidates the token FIRST, then waits for outstanding pins to
 * drain, and only then runs cleanups -- on the retiring (authority) thread, so
 * no destructor ever runs on whichever thread happened to unpin last.
 */
#ifndef MOQ5_REGISTRY_H
#define MOQ5_REGISTRY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* The opaque token. Zero is never valid. */
typedef uint64_t moq5_handle_t;

#define MOQ5_HANDLE_INVALID ((moq5_handle_t)0)

/* Field widths and shifts, stated once so the encoders and every test read the
 * same authority. */
#define MOQ5_HANDLE_KIND_BITS 8u
#define MOQ5_HANDLE_SLOT_BITS 24u
#define MOQ5_HANDLE_GEN_BITS  32u

#define MOQ5_HANDLE_KIND_SHIFT 0u
#define MOQ5_HANDLE_SLOT_SHIFT (MOQ5_HANDLE_KIND_BITS)
#define MOQ5_HANDLE_GEN_SHIFT  (MOQ5_HANDLE_KIND_BITS + MOQ5_HANDLE_SLOT_BITS)

#define MOQ5_HANDLE_KIND_MASK ((uint64_t)0xFFu)
#define MOQ5_HANDLE_SLOT_MASK ((uint64_t)0xFFFFFFu)
#define MOQ5_HANDLE_GEN_MASK  ((uint64_t)0xFFFFFFFFu)

/* The first generation ever issued for a slot. Bit 31 is set so the token's
 * bit 63 is set: valid tokens are negative when carried as `jlong`. */
#define MOQ5_HANDLE_GEN_FIRST ((uint32_t)0x80000001u)

/* The last generation a slot may be issued. A slot that reaches it is retired
 * permanently rather than wrapped. */
#define MOQ5_HANDLE_GEN_LAST ((uint32_t)0xFFFFFFFFu)

/* The runtime limit on live entries. It is deliberately lower than the 24-bit
 * slot space is capable of representing, is checked, and does not depend on
 * pointer width. */
#define MOQ5_REGISTRY_MAX_ENTRIES 4096u

/* Entry kinds. Zero is invalid so a zeroed token cannot name one.
 *
 * These are the GENERIC lifetime roles this tranche proves. They deliberately
 * do not name endpoint, receiver or sender: the eventual mapping is not guessed
 * here (chats/libmoq/1230 F4). */
typedef enum moq5_kind {
    MOQ5_KIND_NONE = 0,
    MOQ5_KIND_ROOT = 1,           /* an owning root; carries the authority thread */
    MOQ5_KIND_CHILD_OWNED = 2,    /* a child this registry cleans up */
    MOQ5_KIND_CHILD_BORROWED = 3, /* a child it invalidates but never cleans */
    MOQ5_KIND__COUNT
} moq5_kind_t;

/* The CONCRETE type behind an entry, closed and private to this DSO.
 *
 * A kind is a LIFETIME role, and two objects of different families share one:
 * a receiver child and a sender child of the same endpoint are both
 * MOQ5_KIND_CHILD_OWNED with the same owner, so kind and owner together cannot
 * tell them apart. Every consumer casts the pointer it acquires to a concrete
 * wrapper, so the family is part of the identity a crossing must validate --
 * otherwise a sender token satisfies a receiver row's checks and is used as a
 * receiver.
 *
 * Zero is not a type, so a zeroed field can never name one, and no wildcard
 * exists: a caller states exactly what it expects. This lives in entry
 * metadata; the token layout is untouched. */
typedef enum moq5_type {
    MOQ5_TYPE_NONE = 0,
    MOQ5_TYPE_ENDPOINT = 1,       /* the root's object */
    MOQ5_TYPE_RECEIVER = 2,       /* an owned media-receiver child */
    MOQ5_TYPE_SENDER = 3,         /* an owned media-sender child */
    MOQ5_TYPE_RECEIVER_TRACK = 4, /* a borrowed track leaf under a receiver */
    MOQ5_TYPE_SENDER_TRACK = 5,   /* a borrowed track leaf under a sender */
    MOQ5_TYPE__COUNT
} moq5_type_t;

/* Outcomes. Each failure is distinct enough to attribute which check refused,
 * which is what makes the discriminators independently load-bearing. */
typedef enum moq5_reg_result {
    MOQ5_REG_OK = 0,
    MOQ5_REG_ERR_INVALID = -1,  /* zero, malformed, or out-of-range token */
    MOQ5_REG_ERR_STALE = -2,    /* the slot lives, but under a later generation */
    MOQ5_REG_ERR_KIND = -3,     /* live token, wrong kind */
    MOQ5_REG_ERR_OWNER = -4,    /* live token, wrong expected owner */
    MOQ5_REG_ERR_THREAD = -5,   /* engine-confined access from another thread */
    MOQ5_REG_ERR_STATE = -6,    /* the entry is retiring */
    MOQ5_REG_ERR_CAPACITY = -7, /* the explicit live-entry limit is reached */
    MOQ5_REG_ERR_NOMEM = -8,    /* table growth failed */
    MOQ5_REG_ERR_PINS = -9,     /* the pin count is at its representable limit */
    MOQ5_REG_ERR_TYPE = -10     /* right lifetime kind, wrong concrete type */
} moq5_reg_result_t;

/* The largest pin count an entry may hold. A pin is refused at the limit rather
 * than allowed to wrap, because a wrapped count would make a pinned object look
 * free to a retirement that is about to destroy it. */
#define MOQ5_REGISTRY_MAX_PINS ((uint32_t)0xFFFFFFFFu)

/* A cleanup for an owned entry. Run exactly once, on the retiring thread, with
 * the registry lock NOT held. */
typedef void (*moq5_cleanup_fn)(void *ptr);

/* Pure token codecs. No registry state is consulted, so a test can build and
 * dissect tokens without one. */
moq5_handle_t moq5_handle_make(uint8_t kind, uint32_t slot, uint32_t generation);
uint8_t       moq5_handle_kind(moq5_handle_t h);
uint32_t      moq5_handle_slot(moq5_handle_t h);
uint32_t      moq5_handle_generation(moq5_handle_t h);

/* Insert a root. The CALLING thread becomes the root's recorded authority
 * thread. `cleanup` may be NULL. Transactional: on any failure no token is
 * issued and no slot or generation is consumed.
 *
 * A root is MOQ5_TYPE_ENDPOINT by invariant, assigned here: the registry roots
 * one object family, so there is nothing for a caller to choose or to get
 * wrong. */
moq5_reg_result_t moq5_reg_insert_root(void *ptr,
                                       moq5_cleanup_fn cleanup,
                                       moq5_handle_t *out_handle);

/* THREE-STEP root construction, for an owner whose creation is fallible.
 *
 * The reserved slot is MOQ5_TYPE_ENDPOINT from the moment it is claimed, and an
 * abort returns it to MOQ5_TYPE_NONE with everything else it found.
 *
 * reserve -> (construct) -> commit, or reserve -> (fail) -> abort. Reserving
 * performs the only fallible registry step BEFORE the object exists, so no
 * caller can ever be left holding an object the registry refused to own. A
 * reservation resolves to nothing, is not live, and consumes no generation;
 * aborting leaves the slot exactly as it was found, restoring the physical
 * generation including a virgin zero. Commit and abort also compare what the
 * entry STORES -- ROOT, MOQ5_TYPE_ENDPOINT and no owner -- so a child
 * reservation repacked with root kind bits cannot reach these paths. */
moq5_reg_result_t moq5_reg_reserve_root(moq5_handle_t *out_handle);
moq5_reg_result_t moq5_reg_commit_root(moq5_handle_t handle, void *ptr,
                                       moq5_cleanup_fn cleanup);
moq5_reg_result_t moq5_reg_abort_root(moq5_handle_t handle);

/* Insert a child under a LIVE OWNING ANCESTOR -- a root, or an owned child at
 * any depth. A BORROWED entry is a LEAF and may never own anything. `kind` must be
 * MOQ5_KIND_CHILD_OWNED or MOQ5_KIND_CHILD_BORROWED. A borrowed child is
 * invalidated on retirement but never cleaned. Transactional as above.
 *
 * `type` is the entry's concrete family and is recorded for every later
 * crossing. It must be a live object type the kind admits -- OWNED takes
 * RECEIVER or SENDER, BORROWED takes their track types -- and an unknown, NONE
 * or incompatible declaration is MOQ5_REG_ERR_TYPE, refused before a slot or a
 * token is touched. */
moq5_reg_result_t moq5_reg_insert_child(moq5_handle_t owner,
                                        void *ptr,
                                        moq5_kind_t kind,
                                        moq5_type_t type,
                                        moq5_cleanup_fn cleanup,
                                        moq5_handle_t *out_handle);

/* THREE-STEP CHILD construction, for a child whose creation is fallible.
 *
 * Same shape as the root transaction and for the same reason: the only fallible
 * registry step -- claiming a slot, and any table growth -- runs BEFORE the
 * object exists, so no caller is ever left holding an object the registry
 * refused to own, and no failed creation leaves an entry behind.
 *
 * `reserve_child` validates the kind, the concrete type, and the owner's
 * resolution, kind, liveness and authority thread, then claims a slot whose
 * generation is READ, not advanced. The reservation is NOT live: it resolves to
 * nothing, cannot be acquired, leased or retired, and counts toward no live
 * total.
 *
 * IDENTITY IS AN INPUT. Commit and abort take the owner and the concrete type
 * the caller believes it reserved, and compare them against what the entry
 * STORES -- stored kind against the token's admitted child kind, stored type
 * against `expect_type`, stored owner against the exact `owner` token including
 * its generation -- before the reservation's own state, generation and
 * authority. A token repacked as another kind therefore cannot reach the wrong
 * transaction.
 *
 * The reservation is LINEAR and authority-thread-confined: the caller settles
 * it exactly once and then discards it. Settling it twice is refused by state.
 * That is a caller precondition, NOT an ABA guarantee: an aborted slot restores
 * its physical generation, so a later reservation of that slot can mint the
 * same private token. No LIVE token ever regains validity after retirement,
 * which is the property the generation is there for.
 *
 * While a reservation is outstanding, retiring its owner -- or any ancestor
 * subtree containing it -- is refused with MOQ5_REG_ERR_STATE before any entry
 * is marked and before any cleanup runs. Unrelated subtrees stay retireable.
 * Nothing waits: the reserving thread is the retiring thread. */
moq5_reg_result_t moq5_reg_reserve_child(moq5_handle_t owner,
                                         moq5_kind_t kind,
                                         moq5_type_t type,
                                         moq5_handle_t *out_handle);
moq5_reg_result_t moq5_reg_commit_child(moq5_handle_t owner,
                                        moq5_handle_t reservation,
                                        moq5_type_t expect_type,
                                        void *ptr,
                                        moq5_cleanup_fn cleanup);
moq5_reg_result_t moq5_reg_abort_child(moq5_handle_t owner,
                                       moq5_handle_t reservation,
                                       moq5_type_t expect_type);

/* ENGINE-CONFINED acquisition: validates token, kind, CONCRETE TYPE, expected
 * owner AND that the caller is the root's authority thread, then PINS and
 * returns the pointer. `expect_owner` is MOQ5_HANDLE_INVALID when acquiring a
 * root itself, whose type is MOQ5_TYPE_ENDPOINT.
 *
 * The type is what makes the returned pointer safe to cast: it is compared
 * under the same lock that pins, after the kind and before owner, state, thread
 * and pin work, and a mismatch is MOQ5_REG_ERR_TYPE with `*out_ptr` untouched.
 * Every successful call must be matched by exactly one moq5_reg_release. */
moq5_reg_result_t moq5_reg_acquire(moq5_handle_t handle,
                                   moq5_kind_t expect_kind,
                                   moq5_type_t expect_type,
                                   moq5_handle_t expect_owner,
                                   void **out_ptr);

/* ANY-THREAD lease of a ROOT, for operations whose own C contract is
 * any-thread-safe: the endpoint wake, the interrupt latch, and the
 * application-thread drain (which holds the lease for a bounded blocking call).
 *
 * It takes neither kind nor type: the restriction is STRUCTURAL, not a comment
 * plus caller-supplied identity that could quietly widen into a generic child
 * lease. It validates ROOT and MOQ5_TYPE_ENDPOINT itself. It
 * skips the authority-thread check deliberately and still PINS, so the entry
 * cannot be destroyed while the lease is live. Matched by exactly one
 * moq5_reg_release, which stays valid from the leasing thread even while the
 * entry is RETIRING. */
moq5_reg_result_t moq5_reg_lease_root_any_thread(moq5_handle_t root, void **out_ptr);

/* Drop one pin taken by acquire or lease. */
moq5_reg_result_t moq5_reg_release(moq5_handle_t handle);

/* RETIREMENT READINESS, owned by the registry rather than by the caller.
 *
 * `mark_ready` records that the owner has been quiesced -- for an endpoint,
 * that `moq_endpoint_stop()` returned MOQ_OK. It is monotonic and confined to
 * the authority thread. `retire_root_ready` refuses an unready live root with
 * MOQ5_REG_ERR_STATE, leaving it valid and uncleaned, and otherwise performs
 * the ordinary retirement below -- the algorithm is gated, never duplicated. */
moq5_reg_result_t moq5_reg_mark_ready(moq5_handle_t root);
moq5_reg_result_t moq5_reg_retire_root_ready(moq5_handle_t root);

/* Retire a root and its whole descendant set.
 *
 * Order is fixed and observable: every descendant token is invalidated FIRST,
 * then outstanding pins are awaited, then owned cleanups run child-before-parent
 * exactly once with the lock released. Must be called on the root's authority
 * thread. */
moq5_reg_result_t moq5_reg_retire_root(moq5_handle_t root);

/* Retire ONE owned child and its descendant subtree, leaving its owner live.
 *
 * The same algorithm as root retirement, scoped to the subtree: the child and
 * every descendant are invalidated FIRST, their outstanding pins awaited, then
 * owned cleanups run deepest-first exactly once with the lock released, and
 * only then are the slots freed (borrowed descendants are invalidated, never
 * cleaned). `owner` must be the child's DIRECT owner, exactly (a foreign or
 * ancestor token is MOQ5_REG_ERR_OWNER); `child` must be MOQ5_KIND_CHILD_OWNED
 * (a root or a borrowed child is MOQ5_REG_ERR_KIND) AND of the concrete
 * `expect_type` the caller names (MOQ5_REG_ERR_TYPE otherwise, checked under
 * the same lock, before the owner comparison and before anything is
 * invalidated); confined to the root's authority thread. A later retirement of
 * the owner never sees the child again: its slot is free and its generation
 * advanced. */
moq5_reg_result_t moq5_reg_retire_child(moq5_handle_t owner, moq5_handle_t child,
                                        moq5_type_t expect_type);

/* Live-entry count, for conservation assertions. */
size_t moq5_reg_live_count(void);

#endif /* MOQ5_REGISTRY_H */
