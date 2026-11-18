/* The private native handle registry. Contract and rationale: moq5_registry.h.
 *
 * One process-wide table, private to this DSO, holding lifetime metadata only.
 * Nothing here dispatches callbacks into Kotlin, polls status, or owns
 * application-visible state.
 *
 * LOCK DISCIPLINE, which is most of the correctness:
 *
 *   - the lock is taken to VALIDATE and PIN, and released before the caller
 *     uses the pointer, so no caller operation and no cleanup callback ever
 *     runs while the registry is locked;
 *   - an unpinned pointer is never returned, so a concurrent retirement cannot
 *     destroy an entry a caller is about to touch;
 *   - retirement marks the subtree RETIRING first (which refuses new pins),
 *     then waits for outstanding pins to drain, then runs cleanups with the
 *     lock released, on the RETIRING thread -- never on whichever thread
 *     happened to unpin last.
 */
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "moq5_registry.h"
#ifdef MOQ5_REGISTRY_TESTING
#include "moq5_registry_testing.h"
#endif

enum entry_state {
    ENTRY_FREE = 0,
    ENTRY_LIVE = 1,
    ENTRY_RETIRING = 2,
    /* A slot claimed for a construction that has not committed yet. It is NOT
     * live: it resolves to nothing, counts toward nothing, and consumes no
     * generation. Its whole purpose is to move the only fallible registry step
     * BEFORE the endpoint exists, so no branch can ever hold an endpoint the
     * registry does not own. */
    ENTRY_RESERVED = 3
};

typedef struct entry {
    void *ptr;
    moq5_cleanup_fn cleanup;
    moq5_handle_t owner; /* the owner's EXACT token, generation included; 0 for a root */
    moq5_handle_t root;  /* the EXACT root token, generation included; its own for a root */
    uint32_t depth;      /* 0 for a root; deeper entries are cleaned first */
    uint32_t generation; /* 0 until the slot has ever been used */
    uint32_t gen_before_reserve; /* physical generation an abort must restore */
    uint32_t pins;
    pthread_t authority; /* the root's authority thread; children inherit it */
    uint8_t kind;
    uint8_t type;        /* the concrete family; MOQ5_TYPE_NONE when the slot holds nothing */
    uint8_t state;
    uint8_t retired;     /* permanently withdrawn by the wrap policy */
    uint8_t ready;       /* the owner has been quiesced; cleanup may run */
} entry_t;

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_unpinned = PTHREAD_COND_INITIALIZER;
static entry_t *g_entries;
static uint32_t g_capacity;
static size_t g_live;

#ifdef MOQ5_REGISTRY_TESTING
static int g_fail_next_alloc;
static long g_alloc_balance;
static void *g_retire_hook_arg;
static void (*g_retire_hook)(void *);
static pthread_t g_last_cleanup_thread;
static pthread_t g_last_retire_thread;
static int g_last_cleanup_recorded;
#endif

/* Which concrete types a lifetime kind admits.
 *
 * Stated once, so insertion and every acquisition read the same authority and
 * a declaration that could never describe a real entry is refused at the
 * boundary rather than silently never matching. A root has exactly one type;
 * an owned child is one of the two attached objects; a borrowed leaf is one of
 * their tracks. */
static int reg_type_admits(moq5_kind_t kind, moq5_type_t type)
{
    switch (kind) {
    case MOQ5_KIND_ROOT:
        return type == MOQ5_TYPE_ENDPOINT;
    case MOQ5_KIND_CHILD_OWNED:
        return type == MOQ5_TYPE_RECEIVER || type == MOQ5_TYPE_SENDER;
    case MOQ5_KIND_CHILD_BORROWED:
        return type == MOQ5_TYPE_RECEIVER_TRACK || type == MOQ5_TYPE_SENDER_TRACK;
    case MOQ5_KIND_NONE:
    case MOQ5_KIND__COUNT:
    default:
        return 0;
    }
}

/* -- token codecs ----------------------------------------------------------- */

moq5_handle_t moq5_handle_make(uint8_t kind, uint32_t slot, uint32_t generation)
{
    return ((uint64_t)generation & MOQ5_HANDLE_GEN_MASK) << MOQ5_HANDLE_GEN_SHIFT
         | ((uint64_t)slot & MOQ5_HANDLE_SLOT_MASK) << MOQ5_HANDLE_SLOT_SHIFT
         | ((uint64_t)kind & MOQ5_HANDLE_KIND_MASK) << MOQ5_HANDLE_KIND_SHIFT;
}

uint8_t moq5_handle_kind(moq5_handle_t h)
{
    return (uint8_t)((h >> MOQ5_HANDLE_KIND_SHIFT) & MOQ5_HANDLE_KIND_MASK);
}

uint32_t moq5_handle_slot(moq5_handle_t h)
{
    return (uint32_t)((h >> MOQ5_HANDLE_SLOT_SHIFT) & MOQ5_HANDLE_SLOT_MASK);
}

uint32_t moq5_handle_generation(moq5_handle_t h)
{
    return (uint32_t)((h >> MOQ5_HANDLE_GEN_SHIFT) & MOQ5_HANDLE_GEN_MASK);
}

/* -- internal allocation, counted so a test can prove conservation ---------- */

static void *reg_alloc(size_t n)
{
#ifdef MOQ5_REGISTRY_TESTING
    if (g_fail_next_alloc) {
        g_fail_next_alloc = 0;
        return NULL;
    }
#endif
    void *p = calloc(1, n);
#ifdef MOQ5_REGISTRY_TESTING
    if (p != NULL) {
        g_alloc_balance++;
    }
#endif
    return p;
}

static void reg_free(void *p)
{
    if (p == NULL) {
        return;
    }
#ifdef MOQ5_REGISTRY_TESTING
    g_alloc_balance--;
#endif
    free(p);
}

/* Grow the table. On failure NOTHING changes: the old table is still installed
 * and no slot or generation has been consumed. */
static int reg_grow_locked(void)
{
    uint32_t want = (g_capacity == 0) ? 16u : g_capacity * 2u;
    if (want > MOQ5_REGISTRY_MAX_ENTRIES) {
        want = MOQ5_REGISTRY_MAX_ENTRIES;
    }
    if (want <= g_capacity) {
        return 0; /* already at the explicit runtime limit */
    }

    entry_t *bigger = (entry_t *)reg_alloc((size_t)want * sizeof *bigger);
    if (bigger == NULL) {
        return 0;
    }
    if (g_entries != NULL) {
        memcpy(bigger, g_entries, (size_t)g_capacity * sizeof *bigger);
        reg_free(g_entries);
    }
    g_entries = bigger;
    g_capacity = want;
    return 1;
}

/* The slot the next insertion would take, or UINT32_MAX if none is available.
 * A permanently retired slot is skipped forever. */
static uint32_t reg_next_free_locked(void)
{
    for (uint32_t i = 0; i < g_capacity; i++) {
        if (g_entries[i].state == ENTRY_FREE && !g_entries[i].retired) {
            return i;
        }
    }
    if (g_capacity < MOQ5_REGISTRY_MAX_ENTRIES) {
        return g_capacity; /* a grow would expose this index */
    }
    return UINT32_MAX;
}

/* -- resolution -------------------------------------------------------------
 *
 * Metadata only: `ptr` is never read, and never written to the caller, until
 * every identity check has passed. */
static moq5_reg_result_t reg_resolve_locked(moq5_handle_t h, entry_t **out)
{
    if (h == MOQ5_HANDLE_INVALID) {
        return MOQ5_REG_ERR_INVALID;
    }

    uint8_t kind = moq5_handle_kind(h);
    if (kind == MOQ5_KIND_NONE || kind >= MOQ5_KIND__COUNT) {
        return MOQ5_REG_ERR_INVALID;
    }

    uint32_t slot = moq5_handle_slot(h);
    if (slot >= MOQ5_REGISTRY_MAX_ENTRIES || slot >= g_capacity) {
        return MOQ5_REG_ERR_INVALID;
    }

    entry_t *e = &g_entries[slot];
    if (e->state == ENTRY_FREE) {
        return MOQ5_REG_ERR_STALE;
    }
    if (e->state == ENTRY_RESERVED) {
        /* A reservation's token is never handed out, so any token that lands
         * here is forged or guessed: refuse before the generation comparison
         * could make it look merely stale. */
        return MOQ5_REG_ERR_STALE;
    }
    if (e->generation != moq5_handle_generation(h)) {
        return MOQ5_REG_ERR_STALE;
    }
    if (e->kind != kind) {
        return MOQ5_REG_ERR_STALE;
    }

    *out = e;
    return MOQ5_REG_OK;
}

static moq5_reg_result_t reg_pin_locked(moq5_handle_t handle,
                                        moq5_kind_t expect_kind,
                                        moq5_type_t expect_type,
                                        moq5_handle_t expect_owner,
                                        int check_thread,
                                        void **out_ptr)
{
    entry_t *e = NULL;
    moq5_reg_result_t rc = reg_resolve_locked(handle, &e);
    if (rc != MOQ5_REG_OK) {
        return rc;
    }
    if (e->kind != (uint8_t)expect_kind) {
        return MOQ5_REG_ERR_KIND;
    }
    /* CONCRETE TYPE, under the pinning lock and before anything else is
     * consulted or changed: two families share one kind and one owner, so this
     * is the check that decides whether the pointer may be cast at all. */
    if (e->type != (uint8_t)expect_type) {
        return MOQ5_REG_ERR_TYPE;
    }
    if (e->owner != expect_owner) {
        return MOQ5_REG_ERR_OWNER;
    }
    if (e->state != ENTRY_LIVE) {
        return MOQ5_REG_ERR_STATE;
    }
    if (check_thread && !pthread_equal(e->authority, pthread_self())) {
        return MOQ5_REG_ERR_THREAD;
    }

    if (e->pins == MOQ5_REGISTRY_MAX_PINS) {
        /* Refuse rather than wrap: a wrapped count would make a pinned entry
         * look free to a retirement about to destroy it. */
        return MOQ5_REG_ERR_PINS;
    }
    e->pins++;
    *out_ptr = e->ptr; /* only now, with every check passed and a pin held */
    return MOQ5_REG_OK;
}

/* -- insertion --------------------------------------------------------------- */

static moq5_reg_result_t reg_insert_locked(void *ptr,
                                           moq5_kind_t kind,
                                           moq5_type_t type,
                                           moq5_cleanup_fn cleanup,
                                           moq5_handle_t owner,
                                           moq5_handle_t root,
                                           uint32_t depth,
                                           int is_root,
                                           pthread_t authority,
                                           moq5_handle_t *out_handle)
{
    uint32_t slot = reg_next_free_locked();
    if (slot == UINT32_MAX) {
        return MOQ5_REG_ERR_CAPACITY;
    }
    if (slot >= g_capacity && !reg_grow_locked()) {
        return MOQ5_REG_ERR_NOMEM;
    }
    if (slot >= g_capacity) {
        return MOQ5_REG_ERR_CAPACITY;
    }

    entry_t *e = &g_entries[slot];
    uint32_t generation = (e->generation == 0) ? MOQ5_HANDLE_GEN_FIRST : e->generation;

    e->ptr = ptr;
    e->cleanup = cleanup;
    e->owner = owner;
    e->generation = generation;
    e->pins = 0;
    e->authority = authority;
    e->kind = (uint8_t)kind;
    /* Written unconditionally: a reused slot must describe its NEW tenant, not
     * whatever last lived here. */
    e->type = (uint8_t)type;
    e->ready = 0;
    e->state = ENTRY_LIVE;

    moq5_handle_t h = moq5_handle_make((uint8_t)kind, slot, generation);
    e->root = is_root ? h : root;
    e->depth = is_root ? 0u : depth;
    *out_handle = h;
    g_live++;
    return MOQ5_REG_OK;
}

/* -- reservation, commit, abort --------------------------------------------- *
 *
 * Three-step construction for an owner whose creation is fallible. Reserving
 * claims a slot and fixes the token; committing publishes a pointer into it and
 * cannot fail; aborting returns the slot exactly as it was found.
 *
 * Why it exists: inserting after a construction leaves a window in which a
 * refused insert holds a live object the registry does not own, and the caller
 * has nowhere durable to put it. Reserving first removes the window instead of
 * handling it. */
moq5_reg_result_t moq5_reg_reserve_root(moq5_handle_t *out_handle)
{
    if (out_handle == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    *out_handle = MOQ5_HANDLE_INVALID;

    pthread_mutex_lock(&g_mu);

    uint32_t slot = reg_next_free_locked();
    moq5_reg_result_t rc = MOQ5_REG_OK;
    if (slot == UINT32_MAX) {
        rc = MOQ5_REG_ERR_CAPACITY;
    } else if (slot >= g_capacity && !reg_grow_locked()) {
        rc = MOQ5_REG_ERR_NOMEM;
    } else if (slot >= g_capacity) {
        rc = MOQ5_REG_ERR_CAPACITY;
    }
    if (rc == MOQ5_REG_OK) {
        entry_t *e = &g_entries[slot];
        /* The generation is READ, not advanced: an aborted reservation must
         * leave the slot indistinguishable from one that was never claimed. */
        uint32_t generation = (e->generation == 0) ? MOQ5_HANDLE_GEN_FIRST : e->generation;
        /* The PHYSICAL prior value is remembered so an abort can restore it. A
         * virgin grown slot holds 0 and would otherwise keep FIRST after an
         * aborted reservation -- observably different from a slot never
         * claimed, even though no ISSUED generation was consumed. */
        e->gen_before_reserve = e->generation;
        e->ptr = NULL;
        e->cleanup = NULL;
        e->owner = MOQ5_HANDLE_INVALID;
        e->generation = generation;
        e->pins = 0;
        e->authority = pthread_self();
        e->kind = (uint8_t)MOQ5_KIND_ROOT;
        e->type = (uint8_t)MOQ5_TYPE_ENDPOINT; /* a root has one family, by invariant */
        e->depth = 0u;
        e->ready = 0;
        e->state = ENTRY_RESERVED;
        *out_handle = moq5_handle_make((uint8_t)MOQ5_KIND_ROOT, slot, generation);
        e->root = *out_handle;
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Publish a pointer into a reservation. It fails only for a token that is not
 * this thread's own live reservation, so a caller holding one can always
 * complete the transaction. */
moq5_reg_result_t moq5_reg_commit_root(moq5_handle_t handle, void *ptr,
                                       moq5_cleanup_fn cleanup)
{
    if (handle == MOQ5_HANDLE_INVALID || ptr == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    if (moq5_handle_kind(handle) != (uint8_t)MOQ5_KIND_ROOT) {
        return MOQ5_REG_ERR_KIND;
    }

    uint32_t slot = moq5_handle_slot(handle);
    moq5_reg_result_t rc;

    pthread_mutex_lock(&g_mu);
    if (slot >= g_capacity) {
        rc = MOQ5_REG_ERR_INVALID;
    } else {
        entry_t *e = &g_entries[slot];
        /* The established root precedence is STATE first -- a slot that holds
         * no reservation is answered as such. The identity checks that follow
         * are on what the ENTRY stores, not what the token claims: once
         * reserved children exist, one repacked with root kind bits would
         * otherwise reach this path. */
        if (e->state != ENTRY_RESERVED) {
            rc = MOQ5_REG_ERR_STATE;
        } else if (e->kind != (uint8_t)MOQ5_KIND_ROOT) {
            rc = MOQ5_REG_ERR_KIND;
        } else if (e->type != (uint8_t)MOQ5_TYPE_ENDPOINT) {
            rc = MOQ5_REG_ERR_TYPE;
        } else if (e->owner != MOQ5_HANDLE_INVALID) {
            rc = MOQ5_REG_ERR_OWNER;
        } else if (e->generation != moq5_handle_generation(handle)) {
            rc = MOQ5_REG_ERR_STALE;
        } else if (!pthread_equal(e->authority, pthread_self())) {
            rc = MOQ5_REG_ERR_THREAD;
        } else {
            e->ptr = ptr;
            e->cleanup = cleanup;
            e->ready = 0;
            e->state = ENTRY_LIVE;
            g_live++;
            rc = MOQ5_REG_OK;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Return a reservation unused. The slot becomes free again with its generation
 * untouched, so an aborted construction consumes nothing at all. */
moq5_reg_result_t moq5_reg_abort_root(moq5_handle_t handle)
{
    if (handle == MOQ5_HANDLE_INVALID) {
        return MOQ5_REG_ERR_INVALID;
    }
    if (moq5_handle_kind(handle) != (uint8_t)MOQ5_KIND_ROOT) {
        return MOQ5_REG_ERR_KIND;
    }

    uint32_t slot = moq5_handle_slot(handle);
    moq5_reg_result_t rc;

    pthread_mutex_lock(&g_mu);
    if (slot >= g_capacity) {
        rc = MOQ5_REG_ERR_INVALID;
    } else {
        entry_t *e = &g_entries[slot];
        if (e->state != ENTRY_RESERVED) {
            rc = MOQ5_REG_ERR_STATE;
        } else if (e->kind != (uint8_t)MOQ5_KIND_ROOT) {
            rc = MOQ5_REG_ERR_KIND;
        } else if (e->type != (uint8_t)MOQ5_TYPE_ENDPOINT) {
            rc = MOQ5_REG_ERR_TYPE;
        } else if (e->owner != MOQ5_HANDLE_INVALID) {
            rc = MOQ5_REG_ERR_OWNER;
        } else if (e->generation != moq5_handle_generation(handle)) {
            rc = MOQ5_REG_ERR_STALE;
        } else if (!pthread_equal(e->authority, pthread_self())) {
            rc = MOQ5_REG_ERR_THREAD;
        } else {
            e->ptr = NULL;
            e->cleanup = NULL;
            e->owner = MOQ5_HANDLE_INVALID;
            e->root = MOQ5_HANDLE_INVALID;
            e->kind = (uint8_t)MOQ5_KIND_NONE;
            e->type = (uint8_t)MOQ5_TYPE_NONE;
            e->generation = e->gen_before_reserve;
            e->gen_before_reserve = 0u;
            e->state = ENTRY_FREE;
            rc = MOQ5_REG_OK;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return rc;
}

moq5_reg_result_t moq5_reg_insert_root(void *ptr,
                                       moq5_cleanup_fn cleanup,
                                       moq5_handle_t *out_handle)
{
    if (out_handle == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    *out_handle = MOQ5_HANDLE_INVALID;
    if (ptr == NULL) {
        /* A handle that resolves to NULL has no valid use, and accepting one
         * would make a later JNI dereference the first effective validation. */
        return MOQ5_REG_ERR_INVALID;
    }

    pthread_mutex_lock(&g_mu);
    moq5_reg_result_t rc = reg_insert_locked(ptr, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, cleanup,
                                             MOQ5_HANDLE_INVALID, MOQ5_HANDLE_INVALID, 0u, 1,
                                             pthread_self(), out_handle);
    pthread_mutex_unlock(&g_mu);
    return rc;
}

moq5_reg_result_t moq5_reg_insert_child(moq5_handle_t owner,
                                        void *ptr,
                                        moq5_kind_t kind,
                                        moq5_type_t type,
                                        moq5_cleanup_fn cleanup,
                                        moq5_handle_t *out_handle)
{
    if (out_handle == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    *out_handle = MOQ5_HANDLE_INVALID;
    if (ptr == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    if (kind != MOQ5_KIND_CHILD_OWNED && kind != MOQ5_KIND_CHILD_BORROWED) {
        return MOQ5_REG_ERR_KIND;
    }
    /* An unknown, NONE, or kind-incompatible family is refused HERE: before the
     * lock, before a slot is chosen, and before any token could be published. */
    if (!reg_type_admits(kind, type)) {
        return MOQ5_REG_ERR_TYPE;
    }

    pthread_mutex_lock(&g_mu);

    entry_t *owner_entry = NULL;
    moq5_reg_result_t rc = reg_resolve_locked(owner, &owner_entry);
    if (rc == MOQ5_REG_OK) {
        if (owner_entry->kind != MOQ5_KIND_ROOT && owner_entry->kind != MOQ5_KIND_CHILD_OWNED) {
            /* A borrowed entry is a LEAF: it owns nothing, so nothing may hang
             * off it. Construction order therefore makes cycles impossible --
             * an owner must already be live, so an entry can never become its
             * own ancestor. */
            rc = MOQ5_REG_ERR_KIND;
        } else if (owner_entry->state != ENTRY_LIVE) {
            rc = MOQ5_REG_ERR_STATE;
        } else if (!pthread_equal(owner_entry->authority, pthread_self())) {
            /* Stateful mutation is engine-confined exactly like acquisition and
             * retirement. Refused BEFORE anything is published. */
            rc = MOQ5_REG_ERR_THREAD;
        } else {
            rc = reg_insert_locked(ptr, kind, type, cleanup, owner,
                                   owner_entry->root, owner_entry->depth + 1u, 0,
                                   owner_entry->authority, out_handle);
        }
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* -- the child transaction --------------------------------------------------- *
 *
 * The same three steps as the root's, and for the same reason: the only
 * fallible registry work happens BEFORE the object exists. What differs is
 * that a child has an OWNER, so identity is an input to every step and is
 * compared against what the entry stores.
 *
 * A reserved child is not live and is not reachable: reg_resolve_locked
 * refuses ENTRY_RESERVED, so acquisition, leasing and retirement all decline
 * it, and it counts toward no live total. */

/* The stored-identity gate shared by commit and abort. Caller holds the lock.
 *
 * Stored kind, then stored type, then stored owner, then the reservation's own
 * state, generation and authority -- the same order the child retirement path
 * uses, so one token can never be judged by two different precedences. */
static moq5_reg_result_t reg_reservation_locked(moq5_handle_t owner,
                                                moq5_handle_t reservation,
                                                moq5_type_t expect_type,
                                                entry_t **out)
{
    uint8_t kind;
    uint32_t slot;
    entry_t *e;

    if (reservation == MOQ5_HANDLE_INVALID) {
        return MOQ5_REG_ERR_INVALID;
    }
    kind = moq5_handle_kind(reservation);
    if (kind != (uint8_t)MOQ5_KIND_CHILD_OWNED && kind != (uint8_t)MOQ5_KIND_CHILD_BORROWED) {
        return MOQ5_REG_ERR_KIND;
    }
    if (!reg_type_admits((moq5_kind_t)kind, expect_type)) {
        return MOQ5_REG_ERR_TYPE;
    }
    slot = moq5_handle_slot(reservation);
    if (slot >= MOQ5_REGISTRY_MAX_ENTRIES || slot >= g_capacity) {
        return MOQ5_REG_ERR_INVALID;
    }
    e = &g_entries[slot];
    if (e->kind != kind) {
        return MOQ5_REG_ERR_KIND;
    }
    if (e->type != (uint8_t)expect_type) {
        return MOQ5_REG_ERR_TYPE;
    }
    if (e->owner != owner) {
        return MOQ5_REG_ERR_OWNER;
    }
    if (e->state != ENTRY_RESERVED) {
        return MOQ5_REG_ERR_STATE;
    }
    if (e->generation != moq5_handle_generation(reservation)) {
        return MOQ5_REG_ERR_STALE;
    }
    if (!pthread_equal(e->authority, pthread_self())) {
        return MOQ5_REG_ERR_THREAD;
    }
    *out = e;
    return MOQ5_REG_OK;
}

moq5_reg_result_t moq5_reg_reserve_child(moq5_handle_t owner,
                                         moq5_kind_t kind,
                                         moq5_type_t type,
                                         moq5_handle_t *out_handle)
{
    if (out_handle == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    *out_handle = MOQ5_HANDLE_INVALID;
    if (kind != MOQ5_KIND_CHILD_OWNED && kind != MOQ5_KIND_CHILD_BORROWED) {
        return MOQ5_REG_ERR_KIND;
    }
    if (!reg_type_admits(kind, type)) {
        return MOQ5_REG_ERR_TYPE;
    }

    pthread_mutex_lock(&g_mu);

    entry_t *owner_entry = NULL;
    moq5_reg_result_t rc = reg_resolve_locked(owner, &owner_entry);
    if (rc == MOQ5_REG_OK) {
        if (owner_entry->kind != MOQ5_KIND_ROOT && owner_entry->kind != MOQ5_KIND_CHILD_OWNED) {
            rc = MOQ5_REG_ERR_KIND;
        } else if (owner_entry->state != ENTRY_LIVE) {
            rc = MOQ5_REG_ERR_STATE;
        } else if (!pthread_equal(owner_entry->authority, pthread_self())) {
            rc = MOQ5_REG_ERR_THREAD;
        }
    }
    if (rc == MOQ5_REG_OK) {
        /* COPIED BEFORE ANY GROWTH: reg_grow_locked reallocates the table, so
         * owner_entry must not be read after the slot search below. */
        const moq5_handle_t owner_root = owner_entry->root;
        const uint32_t child_depth = owner_entry->depth + 1u;
        const pthread_t authority = owner_entry->authority;

        uint32_t slot = reg_next_free_locked();
        if (slot == UINT32_MAX) {
            rc = MOQ5_REG_ERR_CAPACITY;
        } else if (slot >= g_capacity && !reg_grow_locked()) {
            rc = MOQ5_REG_ERR_NOMEM;
        } else if (slot >= g_capacity) {
            rc = MOQ5_REG_ERR_CAPACITY;
        } else {
            entry_t *e = &g_entries[slot];
            /* The generation is READ, not advanced, and its physical prior
             * value is remembered so an abort restores it exactly -- including
             * a virgin zero, which FIRST would otherwise replace. */
            uint32_t generation = (e->generation == 0) ? MOQ5_HANDLE_GEN_FIRST : e->generation;
            e->gen_before_reserve = e->generation;
            e->ptr = NULL;
            e->cleanup = NULL;
            e->owner = owner;
            e->root = owner_root;
            e->depth = child_depth;
            e->generation = generation;
            e->pins = 0;
            e->authority = authority;
            e->kind = (uint8_t)kind;
            e->type = (uint8_t)type;
            e->ready = 0;
            e->state = ENTRY_RESERVED;
            *out_handle = moq5_handle_make((uint8_t)kind, slot, generation);
        }
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Publish a pointer into a child reservation. Nonallocating: for this thread's
 * own live reservation whose owner is still live, there is nothing left that
 * can fail. */
moq5_reg_result_t moq5_reg_commit_child(moq5_handle_t owner,
                                        moq5_handle_t reservation,
                                        moq5_type_t expect_type,
                                        void *ptr,
                                        moq5_cleanup_fn cleanup)
{
    if (ptr == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }

    pthread_mutex_lock(&g_mu);

    entry_t *e = NULL;
    moq5_reg_result_t rc = reg_reservation_locked(owner, reservation, expect_type, &e);
    if (rc == MOQ5_REG_OK) {
        /* The owner must still be there to own it. Checked, never pinned: a
         * pin would have to be dropped again, and this thread is the only one
         * that could retire the owner anyway. */
        entry_t *oe = NULL;
        moq5_reg_result_t orc = reg_resolve_locked(owner, &oe);
        if (orc != MOQ5_REG_OK) {
            rc = orc;
        } else if (oe->state != ENTRY_LIVE) {
            rc = MOQ5_REG_ERR_STATE;
        } else {
            e->ptr = ptr;
            e->cleanup = cleanup;
            e->ready = 0;
            e->state = ENTRY_LIVE;
            g_live++;
        }
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Return a child reservation unused. The slot becomes free again with its
 * physical generation restored, so an aborted creation consumes nothing. */
moq5_reg_result_t moq5_reg_abort_child(moq5_handle_t owner,
                                       moq5_handle_t reservation,
                                       moq5_type_t expect_type)
{
    pthread_mutex_lock(&g_mu);

    entry_t *e = NULL;
    moq5_reg_result_t rc = reg_reservation_locked(owner, reservation, expect_type, &e);
    if (rc == MOQ5_REG_OK) {
        e->ptr = NULL;
        e->cleanup = NULL;
        e->owner = MOQ5_HANDLE_INVALID;
        e->root = MOQ5_HANDLE_INVALID;
        e->depth = 0u;
        e->kind = (uint8_t)MOQ5_KIND_NONE;
        e->type = (uint8_t)MOQ5_TYPE_NONE;
        e->generation = e->gen_before_reserve;
        e->gen_before_reserve = 0u;
        e->pins = 0;
        e->state = ENTRY_FREE;
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* -- acquisition ------------------------------------------------------------- */

moq5_reg_result_t moq5_reg_acquire(moq5_handle_t handle,
                                   moq5_kind_t expect_kind,
                                   moq5_type_t expect_type,
                                   moq5_handle_t expect_owner,
                                   void **out_ptr)
{
    if (out_ptr == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    /* A declaration no entry could ever satisfy is refused as such, rather
     * than left to fail as a mismatch against whatever happens to be there. */
    if (!reg_type_admits(expect_kind, expect_type)) {
        return MOQ5_REG_ERR_TYPE;
    }
    pthread_mutex_lock(&g_mu);
    moq5_reg_result_t rc = reg_pin_locked(handle, expect_kind, expect_type, expect_owner, 1, out_ptr);
    pthread_mutex_unlock(&g_mu);
    return rc;
}

moq5_reg_result_t moq5_reg_lease_root_any_thread(moq5_handle_t root, void **out_ptr)
{
    if (out_ptr == NULL) {
        return MOQ5_REG_ERR_INVALID;
    }
    pthread_mutex_lock(&g_mu);
    /* Deliberately not thread-checked: this exists for operations whose own C
     * contract is any-thread-safe. It is structurally ROOT-ONLY -- there is no
     * kind or type parameter to widen -- and it still pins. */
    moq5_reg_result_t rc = reg_pin_locked(root, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT,
                                          MOQ5_HANDLE_INVALID, 0, out_ptr);
    pthread_mutex_unlock(&g_mu);
    return rc;
}

moq5_reg_result_t moq5_reg_release(moq5_handle_t handle)
{
    pthread_mutex_lock(&g_mu);

    entry_t *e = NULL;
    moq5_reg_result_t rc = reg_resolve_locked(handle, &e);
    if (rc == MOQ5_REG_OK) {
        if (e->pins == 0) {
            rc = MOQ5_REG_ERR_STATE;
        } else {
            e->pins--;
            if (e->pins == 0) {
                pthread_cond_broadcast(&g_unpinned);
            }
        }
    }

    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Whether any outstanding reservation belongs to the tree rooted at `root`.
 *
 * A reservation is not live, so the retirement walks never see it. Retiring
 * out from under one would leave a slot reserved for an owner that no longer
 * exists, so retirement refuses instead -- and refuses BEFORE it marks
 * anything. One pass, no allocation. */
static int reg_root_has_reservation_locked(moq5_handle_t root)
{
    for (uint32_t i = 0; i < g_capacity; i++) {
        if (g_entries[i].state == ENTRY_RESERVED && g_entries[i].root == root) {
            return 1;
        }
    }
    return 0;
}

/* Whether `reserved` hangs under `child`, by walking UP its recorded owner
 * chain with EXACT tokens. Bounded by the entry's own depth, so the whole scan
 * is O(capacity * depth) with no allocation and no inventory. */
static int reg_reserved_under_locked(const entry_t *reserved, moq5_handle_t child)
{
    moq5_handle_t cur = reserved->owner;
    for (uint32_t steps = 0; steps <= reserved->depth; steps++) {
        if (cur == MOQ5_HANDLE_INVALID) {
            return 0;
        }
        if (cur == child) {
            return 1;
        }
        uint32_t slot = moq5_handle_slot(cur);
        if (slot >= g_capacity) {
            return 0;
        }
        const entry_t *o = &g_entries[slot];
        if (o->generation != moq5_handle_generation(cur) || o->kind != moq5_handle_kind(cur)) {
            return 0;
        }
        cur = o->owner;
    }
    return 0;
}

/* -- retirement -------------------------------------------------------------- */

/* -- retirement readiness --------------------------------------------------- *
 *
 * Cleanup eligibility is REGISTRY-OWNED, so the two halves of a teardown cannot
 * disagree about whether the owner was quiesced. The caller performs its own
 * quiescing operation -- for an endpoint that is `moq_endpoint_stop()` -- and
 * marks the root ready only when that succeeded; the ready-checked retirement
 * then refuses to clean a root nobody has quiesced.
 *
 * Why this rather than "destroy stops it itself": the engine contract calls
 * stop and destroy as two operations, and a destroy that quietly stopped again
 * would make the second stop invisible and a stop-refusal oracle ambiguous. */
moq5_reg_result_t moq5_reg_mark_ready(moq5_handle_t root)
{
    uint32_t slot;
    moq5_reg_result_t rc;
    entry_t *e = NULL;

    if (root == MOQ5_HANDLE_INVALID) return MOQ5_REG_ERR_INVALID;
    if (moq5_handle_kind(root) != (uint8_t)MOQ5_KIND_ROOT) return MOQ5_REG_ERR_KIND;
    slot = moq5_handle_slot(root);

    pthread_mutex_lock(&g_mu);
    rc = reg_resolve_locked(root, &e);
    if (rc == MOQ5_REG_OK) {
        if (e->state != ENTRY_LIVE) {
            rc = MOQ5_REG_ERR_STATE;
        } else if (!pthread_equal(e->authority, pthread_self())) {
            rc = MOQ5_REG_ERR_THREAD;
        } else {
            e->ready = 1;      /* monotonic: readiness is never withdrawn */
        }
    }
    (void)slot;
    pthread_mutex_unlock(&g_mu);
    return rc;
}

/* Retire a root ONLY if it has been marked ready. An unready live root is
 * refused by STATE, stays valid, and is not cleaned. The retirement algorithm
 * itself is not duplicated: this is the eligibility gate in front of it. */
moq5_reg_result_t moq5_reg_retire_root_ready(moq5_handle_t root)
{
    moq5_reg_result_t rc;
    entry_t *e = NULL;

    pthread_mutex_lock(&g_mu);
    rc = reg_resolve_locked(root, &e);
    if (rc == MOQ5_REG_OK) {
        /* IDENTITY BEFORE STATE: a caller on the wrong thread is told so, and
         * is not handed a readiness answer about a root it may not touch. */
        if (e->state != ENTRY_LIVE)                          rc = MOQ5_REG_ERR_STATE;
        else if (!pthread_equal(e->authority, pthread_self())) rc = MOQ5_REG_ERR_THREAD;
        else if (!e->ready)                                  rc = MOQ5_REG_ERR_STATE;
    }
    pthread_mutex_unlock(&g_mu);
    if (rc != MOQ5_REG_OK) return rc;
    return moq5_reg_retire_root(root);
}

moq5_reg_result_t moq5_reg_retire_root(moq5_handle_t root)
{
    pthread_mutex_lock(&g_mu);

    entry_t *re = NULL;
    moq5_reg_result_t rc = reg_resolve_locked(root, &re);
    if (rc != MOQ5_REG_OK) {
        pthread_mutex_unlock(&g_mu);
        return rc;
    }
    if (re->kind != MOQ5_KIND_ROOT) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_KIND;
    }
    if (re->state != ENTRY_LIVE) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_STATE;
    }
    if (!pthread_equal(re->authority, pthread_self())) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_THREAD;
    }
    /* An outstanding reservation must settle first, and this refusal happens
     * BEFORE anything is marked or cleaned. Nothing waits: the reserving
     * thread is this thread. */
    if (reg_root_has_reservation_locked(root)) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_STATE;
    }

    /* INVALIDATE FIRST: the exact tree stops accepting new pins before any
     * destruction is contemplated. Membership is the EXACT root token,
     * generation included -- a slot alone would also match a later tenant. */
    for (uint32_t i = 0; i < g_capacity; i++) {
        entry_t *e = &g_entries[i];
        if (e->state == ENTRY_LIVE && e->root == root) {
            e->state = ENTRY_RETIRING;
        }
    }

#ifdef MOQ5_REGISTRY_TESTING
    g_last_retire_thread = pthread_self();
    g_last_cleanup_recorded = 0;
    if (g_retire_hook != NULL) {
        void (*hook)(void *) = g_retire_hook;
        void *arg = g_retire_hook_arg;
        pthread_mutex_unlock(&g_mu);
        hook(arg); /* released deliberately: the hook may block on a pin holder */
        pthread_mutex_lock(&g_mu);
    }
#endif

    /* Wait for pins. ANY pinned entry blocks: summing counts could wrap and
     * make a pinned tree look free. */
    for (;;) {
        int any_pinned = 0;
        for (uint32_t i = 0; i < g_capacity && !any_pinned; i++) {
            entry_t *e = &g_entries[i];
            if (e->state == ENTRY_RETIRING && e->root == root && e->pins > 0) {
                any_pinned = 1;
            }
        }
        if (!any_pinned) {
            break;
        }
        pthread_cond_wait(&g_unpinned, &g_mu);
    }

    /* Cleanups, deepest FIRST and the root LAST, one at a time.
     *
     * No inventory is built: a fixed-capacity array here cost a ~98 KiB frame,
     * which a routine close must not reserve merely because the registry's
     * maximum is 4096. Each pass re-scans under the lock, copies ONE eligible
     * cleanup, clears it so it can never run twice, releases the lock to run it,
     * and re-scans afterwards. No pointer into g_entries is retained across the
     * unlocked call, so a cleanup that re-enters the registry -- even one that
     * makes the table grow -- stays safe. Every slot stays RETIRING, and so
     * unavailable for reuse, until the last callback has finished. */
    for (;;) {
        void *victim_ptr = NULL;
        moq5_cleanup_fn victim_fn = NULL;
        uint32_t best_depth = 0;
        uint32_t best = UINT32_MAX;

        for (uint32_t i = 0; i < g_capacity; i++) {
            entry_t *e = &g_entries[i];
            if (e->state != ENTRY_RETIRING || e->root != root || e->cleanup == NULL) {
                continue;
            }
            if (e->kind == MOQ5_KIND_CHILD_BORROWED) {
                continue; /* invalidated, never cleaned */
            }
            if (best == UINT32_MAX || e->depth > best_depth) {
                best = i;
                best_depth = e->depth;
            }
        }
        if (best == UINT32_MAX) {
            break;
        }

        victim_ptr = g_entries[best].ptr;
        victim_fn = g_entries[best].cleanup;
        g_entries[best].cleanup = NULL; /* exactly once, even if it re-enters */

        pthread_mutex_unlock(&g_mu);
        victim_fn(victim_ptr);
#ifdef MOQ5_REGISTRY_TESTING
        g_last_cleanup_thread = pthread_self();
        g_last_cleanup_recorded = 1;
#endif
        pthread_mutex_lock(&g_mu);
    }

    /* Only now are the slots freed and their generations advanced. */
    for (uint32_t i = 0; i < g_capacity; i++) {
        entry_t *e = &g_entries[i];
        if (e->state != ENTRY_RETIRING || e->root != root) {
            continue;
        }
        if (e->generation >= MOQ5_HANDLE_GEN_LAST) {
            e->retired = 1; /* never reissue: wrapping would resurrect a token */
        } else {
            e->generation++;
        }
        e->ptr = NULL;
        e->cleanup = NULL;
        e->owner = MOQ5_HANDLE_INVALID;
        e->root = MOQ5_HANDLE_INVALID;
        e->depth = 0;
        e->kind = MOQ5_KIND_NONE;
        e->type = MOQ5_TYPE_NONE;
        e->state = ENTRY_FREE;
        e->pins = 0;
        g_live--;
    }

    pthread_mutex_unlock(&g_mu);
    return MOQ5_REG_OK;
}

/* True when `e` (LIVE, same root) hangs directly off an entry already marked
 * RETIRING in this pass. Membership is the owner's EXACT token: a slot alone
 * would also match a later tenant. */
static int reg_owner_retiring_locked(const entry_t *e)
{
    uint32_t oslot;
    const entry_t *o;
    if (e->owner == MOQ5_HANDLE_INVALID) {
        return 0;
    }
    oslot = moq5_handle_slot(e->owner);
    if (oslot >= g_capacity) {
        return 0;
    }
    o = &g_entries[oslot];
    return o->state == ENTRY_RETIRING &&
           o->generation == moq5_handle_generation(e->owner) &&
           o->kind == moq5_handle_kind(e->owner);
}

moq5_reg_result_t moq5_reg_retire_child(moq5_handle_t owner, moq5_handle_t child,
                                        moq5_type_t expect_type)
{
    entry_t *ce = NULL;
    entry_t *oe = NULL;
    moq5_reg_result_t rc;
    moq5_handle_t root;

    if (!reg_type_admits(MOQ5_KIND_CHILD_OWNED, expect_type)) {
        return MOQ5_REG_ERR_TYPE;
    }

    pthread_mutex_lock(&g_mu);

    rc = reg_resolve_locked(child, &ce);
    if (rc != MOQ5_REG_OK) {
        pthread_mutex_unlock(&g_mu);
        return rc;
    }
    /* KIND before TYPE before OWNER before STATE before THREAD: a root or a
     * borrowed leaf is refused by kind whatever owner is claimed; a child of
     * the wrong family is refused before its owner is even compared, so a row
     * can never destroy the other family's object under a matching owner; a
     * wrong owner is refused before the caller learns anything about state. */
    if (ce->kind != MOQ5_KIND_CHILD_OWNED) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_KIND;
    }
    if (ce->type != (uint8_t)expect_type) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_TYPE;
    }
    if (ce->owner != owner) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_OWNER;
    }
    if (ce->state != ENTRY_LIVE) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_STATE;
    }
    if (!pthread_equal(ce->authority, pthread_self())) {
        pthread_mutex_unlock(&g_mu);
        return MOQ5_REG_ERR_THREAD;
    }
    rc = reg_resolve_locked(owner, &oe);
    if (rc != MOQ5_REG_OK || oe->state != ENTRY_LIVE) {
        pthread_mutex_unlock(&g_mu);
        return rc != MOQ5_REG_OK ? rc : MOQ5_REG_ERR_STATE;
    }
    root = ce->root;
    /* The same rule, scoped to this subtree: an outstanding reservation under
     * `child` refuses the retirement before any entry is marked or cleaned.
     * A reservation elsewhere under the root does not block it. */
    for (uint32_t i = 0; i < g_capacity; i++) {
        const entry_t *e = &g_entries[i];
        if (e->state != ENTRY_RESERVED || e->root != root) {
            continue;
        }
        if (e->owner == child || reg_reserved_under_locked(e, child)) {
            pthread_mutex_unlock(&g_mu);
            return MOQ5_REG_ERR_STATE;
        }
    }

    /* INVALIDATE the subtree first: the child, then -- until nothing changes
     * -- every live entry under this root whose owner is already retiring. A
     * retirement is confined to the authority thread, so the only RETIRING
     * entries under this root during this pass are this subtree's. */
    ce->state = ENTRY_RETIRING;
    for (;;) {
        int changed = 0;
        for (uint32_t i = 0; i < g_capacity; i++) {
            entry_t *e = &g_entries[i];
            if (e->state == ENTRY_LIVE && e->root == root && reg_owner_retiring_locked(e)) {
                e->state = ENTRY_RETIRING;
                changed = 1;
            }
        }
        if (!changed) {
            break;
        }
    }

    /* Wait for pins on the subtree. */
    for (;;) {
        int any_pinned = 0;
        for (uint32_t i = 0; i < g_capacity && !any_pinned; i++) {
            entry_t *e = &g_entries[i];
            if (e->state == ENTRY_RETIRING && e->root == root && e->pins > 0) {
                any_pinned = 1;
            }
        }
        if (!any_pinned) {
            break;
        }
        pthread_cond_wait(&g_unpinned, &g_mu);
    }

    /* Cleanups, deepest first, one at a time, lock released around each --
     * the root-retirement discipline, scoped to what is RETIRING here. */
    for (;;) {
        void *victim_ptr = NULL;
        moq5_cleanup_fn victim_fn = NULL;
        uint32_t best_depth = 0;
        uint32_t best = UINT32_MAX;

        for (uint32_t i = 0; i < g_capacity; i++) {
            entry_t *e = &g_entries[i];
            if (e->state != ENTRY_RETIRING || e->root != root || e->cleanup == NULL) {
                continue;
            }
            if (e->kind == MOQ5_KIND_CHILD_BORROWED) {
                continue; /* invalidated, never cleaned */
            }
            if (best == UINT32_MAX || e->depth > best_depth) {
                best = i;
                best_depth = e->depth;
            }
        }
        if (best == UINT32_MAX) {
            break;
        }
        victim_ptr = g_entries[best].ptr;
        victim_fn = g_entries[best].cleanup;
        g_entries[best].cleanup = NULL; /* exactly once, even if it re-enters */

        pthread_mutex_unlock(&g_mu);
        victim_fn(victim_ptr);
        pthread_mutex_lock(&g_mu);
    }

    /* Free the subtree's slots; the owner and the rest of the tree stay LIVE. */
    for (uint32_t i = 0; i < g_capacity; i++) {
        entry_t *e = &g_entries[i];
        if (e->state != ENTRY_RETIRING || e->root != root) {
            continue;
        }
        if (e->generation >= MOQ5_HANDLE_GEN_LAST) {
            e->retired = 1;
        } else {
            e->generation++;
        }
        e->ptr = NULL;
        e->cleanup = NULL;
        e->owner = MOQ5_HANDLE_INVALID;
        e->root = MOQ5_HANDLE_INVALID;
        e->depth = 0;
        e->kind = MOQ5_KIND_NONE;
        e->type = MOQ5_TYPE_NONE;
        e->state = ENTRY_FREE;
        e->pins = 0;
        g_live--;
    }

    pthread_mutex_unlock(&g_mu);
    return MOQ5_REG_OK;
}

size_t moq5_reg_live_count(void)
{
    pthread_mutex_lock(&g_mu);
    size_t n = g_live;
    pthread_mutex_unlock(&g_mu);
    return n;
}

/* -- test-only seams --------------------------------------------------------- */
#ifdef MOQ5_REGISTRY_TESTING

void moq5_reg_test_fail_next_alloc(int on)
{
    pthread_mutex_lock(&g_mu);
    g_fail_next_alloc = on;
    pthread_mutex_unlock(&g_mu);
}

int moq5_reg_test_prime_free_slot_generation(uint32_t slot, uint32_t generation)
{
    int ok = 0;
    pthread_mutex_lock(&g_mu);
    if (slot < g_capacity && g_entries[slot].state == ENTRY_FREE && !g_entries[slot].retired) {
        g_entries[slot].generation = generation;
        ok = 1;
    }
    pthread_mutex_unlock(&g_mu);
    return ok;
}

int moq5_reg_test_slot_retired(uint32_t slot)
{
    pthread_mutex_lock(&g_mu);
    int r = (slot < g_capacity) ? (int)g_entries[slot].retired : 0;
    pthread_mutex_unlock(&g_mu);
    return r;
}

uint32_t moq5_reg_test_next_free_slot(void)
{
    pthread_mutex_lock(&g_mu);
    uint32_t s = reg_next_free_locked();
    pthread_mutex_unlock(&g_mu);
    return s;
}

uint32_t moq5_reg_test_capacity(void)
{
    pthread_mutex_lock(&g_mu);
    uint32_t c = g_capacity;
    pthread_mutex_unlock(&g_mu);
    return c;
}

uint32_t moq5_reg_test_slot_generation(uint32_t slot)
{
    pthread_mutex_lock(&g_mu);
    uint32_t g = (slot < g_capacity) ? g_entries[slot].generation : 0u;
    pthread_mutex_unlock(&g_mu);
    return g;
}

long moq5_reg_test_alloc_balance(void)
{
    pthread_mutex_lock(&g_mu);
    long b = g_alloc_balance;
    pthread_mutex_unlock(&g_mu);
    return b;
}

void moq5_reg_test_on_retire_wait(void *arg, void (*fn)(void *))
{
    pthread_mutex_lock(&g_mu);
    g_retire_hook_arg = arg;
    g_retire_hook = fn;
    pthread_mutex_unlock(&g_mu);
}

int moq5_reg_test_lock_is_free(void)
{
    if (pthread_mutex_trylock(&g_mu) == 0) {
        pthread_mutex_unlock(&g_mu);
        return 1;
    }
    return 0;
}

int moq5_reg_test_set_pins(moq5_handle_t handle, uint32_t pins)
{
    int ok = 0;
    pthread_mutex_lock(&g_mu);
    entry_t *e = NULL;
    if (reg_resolve_locked(handle, &e) == MOQ5_REG_OK) {
        e->pins = pins;
        ok = 1;
    }
    pthread_mutex_unlock(&g_mu);
    return ok;
}

int moq5_reg_test_last_cleanup_was_retiring_thread(void)
{
    pthread_mutex_lock(&g_mu);
    int r = g_last_cleanup_recorded && pthread_equal(g_last_cleanup_thread, g_last_retire_thread);
    pthread_mutex_unlock(&g_mu);
    return r;
}

#endif /* MOQ5_REGISTRY_TESTING */
