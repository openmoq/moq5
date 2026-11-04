#include "test_session_support.h"
#include "test_raw_pair_support.h"

/* The in-place receive parse against its staged reference.
 *
 * session_rx_stage_reference makes a session append every delivery to its
 * staging and parse the staged copy. Each differential case builds two
 * identical client/server pairs, generates the server's data streams on both
 * (checked byte-equal), and feeds the same bytes, cut the same way, to both
 * clients: the reference with the switch set, the other parsing in place.
 *
 * After every delivery, retry and drain it compares, semantically and with
 * handles normalized to each side's own subscriptions, its fetch, and the
 * fill child its FILL_CREATED announced (with that child's parent and
 * initiating request):
 * - the return code;
 * - every polled event, by kind: identity, flags, gaps, status, terminal and
 *   close causes, payload/properties/chunk bytes and reference counts; a kind
 *   this oracle does not model fails;
 * - every polled action, likewise; an unmodelled kind fails;
 * - session state, receive charges, allocator calls, refusals and the size
 *   refused, live objects and live bytes;
 * - each receive entry: parse state, current object and its metadata,
 *   initialized payload bytes, extensions, pending chunk and its flags, reset
 *   obligation, header buffer, staged bytes, staged length and capacity; its
 *   subscription, publication, fetch and fill bindings; and for a stream on a
 *   shared alias, every recipient record (subscription, pending, current,
 *   owed, closed, closure) and the ambiguity flag.
 *
 * Storage nothing may read is poisoned: fresh allocations, each entry's
 * staging beyond its logical length (a different byte per arm, after every
 * call), and caller buffers after each call (volatile stores; an input rcbuf
 * only when the session kept no reference to it).
 *
 * A WOULD_BLOCK is settled as the input contract prescribes for the stream's
 * actual ownership in each arm: an owned stream retained the bytes and is
 * retried empty (a WOULD_BLOCK that retries cannot clear fails the case); a
 * stream the session did not admit retained nothing, and the same bytes and
 * FIN are replayed -- a stream still unowned once a stream can be admitted,
 * a stream admitted meanwhile at once. Subgroup cases also keep
 * each arm's own inventory of delivered objects, bytes and terminals, checked
 * against the generator.
 *
 * Every block the session allocators hand out is tracked, with an
 * allocation id, so what survives both sessions is known block by block, not
 * only as totals. An allocator refusal that never fired is an ordinary run;
 * one that fired must leave nothing behind, except one known observation,
 * counted by name: a refused whole-object payload allocation leaves that
 * object's properties block -- the very allocation the stream held when the
 * refusal fired, identified then by its id, never another block of the same
 * size or bytes. The --known-alloc-failure mode runs the refusal cases alone
 * and fails while that observation remains.
 *
 * Known-answer cases pin what both arms share and a differential cannot see:
 * staging growth, clamp, charge and refusal; a retained allocation failure
 * recovered by retry; a refused whole-object payload allocation; an emitted
 * object outliving its session. */

static int failures = 0;
static uint64_t g_mismatches = 0;
static unsigned g_reported = 0;
/* Allocator-refusal outcomes. */
static uint64_t g_refusal_unexercised = 0;  /* the index was never reached */
static uint64_t g_refusal_clean = 0;        /* fired; nothing left behind */
static uint64_t g_refusal_known = 0;        /* fired; the known observation */
static uint64_t g_known_targeted = 0;       /* the targeted case showed it */
static unsigned g_known_reported = 0;
/* Unowned WOULD_BLOCKs parked, parked deliveries replayed, and inventory
 * checks that failed. */
static uint64_t g_parked = 0;
static uint64_t g_replayed = 0;
static uint64_t g_inventory_failures = 0;

#define RXP_POISON      0xCC
#define RXP_POISON_MAX  65536u   /* staging and payload blocks; session blocks
                                  * above this are zeroed by the session */
#define RXP_REPORT_MAX  24u
#define RXP_STREAM_CAP  2048u
#define RXP_UNITS       16u
#define RXP_ROLES       3
#define RXP_NO_FLIP     ((size_t)-1)
#define RXP_RETRIES     64
#define RXP_NOW         100u
#define RXP_PARK_MAX    8u
#define RXP_INV_OBJECTS 4

static void rxp_note(const char *what, const char *where)
{
    g_mismatches++;
    if (g_reported < RXP_REPORT_MAX) {
        fprintf(stderr, "MISMATCH: %s (%s)\n", what, where);
        g_reported++;
    }
}

static void rxp_known_report(const char *where)
{
    if (g_known_reported < RXP_REPORT_MAX) {
        fprintf(stderr, "KNOWN FAILING: refused whole-object payload allocation left "
                        "the object's properties block (%s)\n", where);
        g_known_reported++;
    }
}

static void rxp_scrub(uint8_t *p, size_t n, uint8_t v)
{
    volatile uint8_t *q = p;
    for (size_t i = 0; i < n; i++) q[i] = v;
}

/* -- Poisoning, counting, failing, tracking allocator --------------- */

/* Every block carries this header in front of the caller's bytes, linking it
 * into its allocator's live list, with an allocation id unique within that
 * allocator (kept across a realloc of the block). */
typedef struct rxp_blk {
    struct rxp_blk *prev;
    struct rxp_blk *next;
    size_t          size;   /* the size the caller asked for */
    size_t          id;     /* 1, 2, ... in allocation order */
} rxp_blk_t;
_Static_assert(sizeof(rxp_blk_t) % 16 == 0, "block header keeps the caller's alignment");

typedef struct rxp_alloc {
    int64_t    balance;           /* live allocations */
    int64_t    live;              /* live bytes */
    uint64_t   calls;             /* alloc + realloc calls, refused ones included */
    uint64_t   fail_at;           /* refuse exactly this call number; 0 = never */
    size_t     fail_size;         /* refuse the next request of exactly this size */
    uint64_t   fail_hits;         /* refusals actually made */
    size_t     last_refused;      /* the size of the last refused request */
    uint64_t   size_errors;       /* a free or realloc naming the wrong size */
    size_t     last_realloc_old;  /* the last growth of an existing block */
    size_t     last_realloc_new;
    rxp_blk_t *head;              /* every live block */
    size_t     last_id;           /* the last allocation id given out */
} rxp_alloc_t;

static void rxp_link(rxp_alloc_t *a, rxp_blk_t *b)
{
    b->prev = NULL;
    b->next = a->head;
    if (a->head) a->head->prev = b;
    a->head = b;
}

static void rxp_unlink(rxp_alloc_t *a, rxp_blk_t *b)
{
    if (b->prev) b->prev->next = b->next;
    else a->head = b->next;
    if (b->next) b->next->prev = b->prev;
}

static bool rxp_refuse(rxp_alloc_t *a, size_t size)
{
    if (a->fail_at && a->calls == a->fail_at) {
        a->fail_hits++;
        a->last_refused = size;
        return true;
    }
    if (a->fail_size && size == a->fail_size) {
        a->fail_size = 0;
        a->fail_hits++;
        a->last_refused = size;
        return true;
    }
    return false;
}

static void *rxp_alloc_fn(size_t size, void *ctx)
{
    rxp_alloc_t *a = (rxp_alloc_t *)ctx;
    a->calls++;
    if (rxp_refuse(a, size)) return NULL;
    rxp_blk_t *b = (rxp_blk_t *)malloc(sizeof(*b) + size);
    if (!b) return NULL;
    b->size = size;
    b->id = ++a->last_id;
    rxp_link(a, b);
    uint8_t *p = (uint8_t *)(b + 1);
    if (size <= RXP_POISON_MAX) memset(p, RXP_POISON, size);
    a->balance++;
    a->live += (int64_t)size;
    return p;
}

static void *rxp_realloc_fn(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    rxp_alloc_t *a = (rxp_alloc_t *)ctx;
    const bool had_ptr = ptr != NULL;
    a->calls++;
    if (rxp_refuse(a, new_sz)) return NULL;
    rxp_blk_t *b = NULL;
    if (had_ptr) {
        b = (rxp_blk_t *)ptr - 1;
        if (b->size != old_sz) a->size_errors++;
        rxp_unlink(a, b);
    }
    rxp_blk_t *nb = (rxp_blk_t *)realloc(b, sizeof(*nb) + new_sz);
    if (!nb) {
        if (b) rxp_link(a, b);
        return NULL;
    }
    nb->size = new_sz;
    if (!had_ptr) nb->id = ++a->last_id;
    rxp_link(a, nb);
    if (!had_ptr) {
        a->balance++;
        old_sz = 0;
    } else {
        a->last_realloc_old = old_sz;
        a->last_realloc_new = new_sz;
    }
    a->live += (int64_t)new_sz - (int64_t)old_sz;
    uint8_t *p = (uint8_t *)(nb + 1);
    if (new_sz > old_sz && new_sz <= RXP_POISON_MAX)
        memset(p + old_sz, RXP_POISON, new_sz - old_sz);
    return p;
}

static void rxp_free_fn(void *ptr, size_t size, void *ctx)
{
    rxp_alloc_t *a = (rxp_alloc_t *)ctx;
    if (!ptr) return;
    rxp_blk_t *b = (rxp_blk_t *)ptr - 1;
    if (b->size != size) a->size_errors++;
    rxp_unlink(a, b);
    a->balance--;
    a->live -= (int64_t)size;
    free(b);
}

/* Free whatever is still live in `a` once both its sessions are gone and no
 * event holds a block: nothing can reach it, and the test process stays
 * clean. */
static void rxp_reclaim(rxp_alloc_t *a)
{
    while (a->head) {
        rxp_blk_t *b = a->head;
        a->head = b->next;
        free(b);
    }
    a->balance = 0;
    a->live = 0;
}

/* The allocation id of the live block of `a` whose caller pointer is `p`;
 * 0 when `p` is no live block of `a`. */
static size_t rxp_block_id(const rxp_alloc_t *a, const void *p)
{
    for (const rxp_blk_t *b = a->head; b; b = b->next)
        if ((const void *)(b + 1) == p) return b->id;
    return 0;
}

/* What is still live in `a`: 0 = nothing; 1 = exactly one block, and it is
 * allocation `id` itself, still of the properties rcbuf's size and ending
 * in `props`; -1 = anything else. Any other block fails, however equal its
 * size and bytes; an `id` of 0 names no block, so then only nothing passes. */
static int rxp_residue(const rxp_alloc_t *a, size_t id, const uint8_t *props,
                       size_t props_len)
{
    if (!a->head) return a->balance == 0 && a->live == 0 ? 0 : -1;
    size_t want = 0;
    if (id == 0 || moq_rcbuf_allocation_size(props_len, &want) != MOQ_OK) return -1;
    const rxp_blk_t *b = a->head;
    if (b->next || b->id != id || a->balance != 1 || a->live != (int64_t)want ||
        b->size != want)
        return -1;
    const uint8_t *bytes = (const uint8_t *)(b + 1);
    return memcmp(bytes + want - props_len, props, props_len) == 0 ? 1 : -1;
}

/* Plain allocator for the test's own payload and input rcbufs, so they never
 * count toward or fail inside the compared sessions' allocators. */
static test_alloc_state_t g_plain_state;
static moq_alloc_t g_plain;

/* -- Cases, sides, streams -------------------------------------------- */

enum { RXP_BYTES = 0, RXP_RCBUF = 1 };

/* What each pair carries before the deliveries. Roles 0 and 1 are the
 * subgroup streams of group 1 (object properties; payloads 5, 300, 0, 70) and
 * group 2 (no properties; 40, 1, 200), each generated and captured on its
 * own, so a role always names the same group whatever order the publisher
 * schedules. Role 2 is a fetch or fill stream. */
enum {
    RXP_SC_SUBGROUPS = 0,  /* one subscription, both subgroup streams */
    RXP_SC_SUPPRESS,       /* as SUBGROUPS, with the subscription at Forward 0 */
    RXP_SC_FETCH,          /* a standalone fetch (role 2) */
    RXP_SC_FILL,           /* draft-22: a subscription's fill (role 2) */
    RXP_SC_SHARED,         /* draft-22: a second subscription on the alias */
    RXP_SC_HELD            /* draft-22: a second subscription still pending,
                            * answered after the deliveries */
};

typedef struct rxp_case {
    moq_version_t version;     /* 0: the draft-16 default */
    int           scenario;
    bool          streaming;
    int           route;
    uint32_t      budget;      /* max_receive_buffer_bytes; 0 = default */
    uint32_t      max_events;  /* 0 = default */
    uint32_t      max_streams; /* max_data_streams; 0 = default */
    size_t        poll_cap;    /* events polled after each delivery */
    uint64_t      fail_at;     /* refuse this allocator call after setup */
    bool          flip;        /* change role-0 byte `flip_at` on the in-place side */
    size_t        flip_at;
    bool          slot_reuse;  /* role 1 must take role 0's freed slot */
    bool          empty_unowned; /* settle an unowned WOULD_BLOCK with empty
                                  * retries, as the contract forbids (the
                                  * negative control only) */
} rxp_case_t;

typedef struct rxp_stream {
    uint8_t bytes[RXP_STREAM_CAP];
    size_t  len;
    bool    fin;
    bool    seen;
    size_t  units;                 /* SEND_DATA actions captured */
    size_t  unit_hdr[RXP_UNITS];
    size_t  unit_len[RXP_UNITS];
} rxp_stream_t;

/* What one side's client delivered for the two subgroup streams (role 0:
 * group 1, role 1: group 2), recorded from every event it polled and checked
 * against the generator, independently of the other arm. */
typedef struct rxp_inv {
    size_t   got[2][RXP_INV_OBJECTS];   /* payload bytes matched, in order */
    uint8_t  done[2][RXP_INV_OBJECTS];  /* completions of each object */
    uint8_t  finished[2];               /* SUBGROUP_FINISHED events */
    uint8_t  reset[2];                  /* SUBGROUP_RESET events */
    uint64_t last[2];                   /* the last FINISHED's last object */
    bool     bad;                       /* a byte, id, group or order wrong */
} rxp_inv_t;

typedef struct rxp_side {
    rxp_alloc_t        as;
    moq_alloc_t        alloc;
    moq_session_t     *c;
    moq_session_t     *sv;
    bool               reference;
    uint8_t            poison;     /* written beyond each staged extent */
    moq_version_t      version;
    moq_subscription_t sub[2], ssub[2];
    uint64_t           alias[2];
    moq_fetch_t        fetch, sfetch;
    /* The fill child as FILL_CREATED announced it. */
    moq_fill_t         fill;
    moq_subscription_t fill_parent;
    uint64_t           fill_request;
    bool               fill_known;
    rxp_stream_t       st[RXP_ROLES];
    rxp_inv_t          inv;
    /* The allocation id of the properties block the stream held at the call
     * during which this arm's refusal fired, when that refusal was its
     * object's payload allocation (see rxp_note_refusal_site); 0 otherwise. */
    size_t             known_props_id;
    /* UNSUBSCRIBED events this side polled, by their own fields: those for
     * role 0 with the malformed-track cause and stream error code 0, and
     * every other one. */
    uint32_t           unsub_role0_malformed;
    uint32_t           unsub_other;
} rxp_side_t;

typedef struct rxp_piece {
    int      role;
    size_t   off;
    size_t   len;
    bool     fin;
    bool     reset;     /* a peer RESET of the role's stream, with `code` */
    uint64_t code;
    bool     hold;      /* leave a WOULD_BLOCK owed: no drain, no retry */
} rxp_piece_t;

static uint64_t rxp_ref(int role) { return 100u + (uint64_t)role; }

static moq_session_t *rxp_session(rxp_side_t *p, moq_perspective_t persp,
                                  const rxp_case_t *k)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &p->alloc, persp);
    cfg.version = k->version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    if (persp == MOQ_PERSPECTIVE_CLIENT) {
        cfg.streaming_objects = k->streaming;
        cfg.max_receive_buffer_bytes = k->budget;
        cfg.max_events = k->max_events;
        cfg.max_data_streams = k->max_streams;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (test_bind_if_required(s) < 0) { moq_session_destroy(s); return NULL; }
    if (moq_session_start(s, 0) < 0 && persp == MOQ_PERSPECTIVE_CLIENT) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static bool rxp_open(rxp_side_t *p, const rxp_case_t *k, bool reference)
{
    memset(p, 0, sizeof(*p));
    p->reference = reference;
    p->poison = reference ? 0x11 : 0x22;
    p->version = k->version;
    p->alloc = (moq_alloc_t){ &p->as, rxp_alloc_fn, rxp_realloc_fn, rxp_free_fn };
    p->c = rxp_session(p, MOQ_PERSPECTIVE_CLIENT, k);
    p->sv = rxp_session(p, MOQ_PERSPECTIVE_SERVER, k);
    if (!p->c || !p->sv) return false;
    run(p->c, p->sv, 1);
    drain(p->c);
    drain(p->sv);
    return true;
}

/* Destroy both sessions. Then everything this side's allocator handed out is
 * back (anything left is reported, then reclaimed), unless the caller still
 * owns or will classify what is left (`kept` nonzero). */
static void rxp_close(rxp_side_t *p, int64_t kept, const char *label)
{
    p->as.fail_at = 0;
    p->as.fail_size = 0;
    if (p->c) moq_session_destroy(p->c);
    if (p->sv) moq_session_destroy(p->sv);
    p->c = p->sv = NULL;
    if (p->as.size_errors) rxp_note("a free or realloc named the wrong size", label);
    if (kept == 0) {
        if (p->as.balance != 0 || p->as.live != 0 || p->as.head)
            rxp_note("allocator not empty after destroy", label);
        rxp_reclaim(&p->as);
    }
}

static bool rxp_accept(rxp_side_t *p, int role, bool explicit_alias, uint64_t alias,
                       bool take_ok)
{
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_track_alias = explicit_alias;
    ac.track_alias = alias;
    if (p->version == MOQ_VERSION_DRAFT_22) {
        ac.has_largest = true;
        ac.largest_group = 0;
        ac.largest_object = 0;
    }
    if (moq_session_accept_subscribe(p->sv, p->ssub[role], &ac, RXP_NOW) != MOQ_OK)
        return false;
    run(p->c, p->sv, RXP_NOW);
    p->alias[role] = alias;
    if (!take_ok) return true;
    moq_event_t ev;
    if (!take(p->c, MOQ_EVENT_SUBSCRIBE_OK, &ev)) return false;
    p->alias[role] = ev.u.subscribe_ok.track_alias;
    moq_event_cleanup(&ev);
    return true;
}

static bool rxp_subscribe(rxp_side_t *p, int role, const moq_fill_cfg_t *fill)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("video");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    cfg.start_group = fill ? 0 : 1;
    cfg.start_object = 0;
    cfg.fill = fill;
    if (moq_session_subscribe(p->c, &cfg, RXP_NOW, &p->sub[role]) != MOQ_OK) return false;
    run(p->c, p->sv, RXP_NOW);
    moq_event_t ev;
    if (!take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    p->ssub[role] = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    return true;
}

/* Collect one server data stream into `st`, delivering every other server
 * action to the client; then let the client answer and drop setup events. */
static bool rxp_capture(rxp_side_t *p, rxp_stream_t *st)
{
    memset(st, 0, sizeof(*st));
    bool ok = true;
    uint64_t ref = 0;
    moq_action_t a;
    while (moq_session_poll_actions(p->sv, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            const moq_send_data_action_t *d = &a.u.send_data;
            size_t hl = d->header_len;
            size_t pl = d->payload ? moq_rcbuf_len(d->payload) : 0;
            if (!st->seen) {
                st->seen = true;
                ref = d->stream_ref._v;
            }
            if (d->stream_ref._v != ref || st->len + hl + pl > RXP_STREAM_CAP ||
                st->units >= RXP_UNITS) {
                ok = false;
            } else {
                if (hl) memcpy(st->bytes + st->len, d->header, hl);
                if (pl) memcpy(st->bytes + st->len + hl, moq_rcbuf_data(d->payload), pl);
                st->unit_hdr[st->units] = hl;
                st->unit_len[st->units] = hl + pl;
                st->units++;
                st->len += hl + pl;
                if (d->fin) st->fin = true;
            }
        } else {
            deliver(p->c, &a, true, RXP_NOW);
        }
        moq_action_cleanup(&a);
    }
    pump(p->c, p->sv, false, RXP_NOW, 0);
    drain(p->c);
    drain(p->sv);
    return ok && st->seen && st->fin;
}

static void rxp_payload(uint8_t *body, uint64_t object_id, size_t n)
{
    for (size_t i = 0; i < n; i++)
        body[i] = (uint8_t)(object_id * 31u + i * 7u + 1u);
}

static const uint8_t *rxp_props_bytes(moq_version_t version, size_t *len)
{
    static const uint8_t kvp_d22[] = { 0x40, 0x01 };  /* type 0x40, value 1 */
    static const uint8_t kvp[] = { 0x02, 0x05 };      /* type 2, value 5 */
    if (version == MOQ_VERSION_DRAFT_22) {
        *len = sizeof(kvp_d22);
        return kvp_d22;
    }
    *len = sizeof(kvp);
    return kvp;
}

static bool rxp_props(const rxp_side_t *p, moq_rcbuf_t **out)
{
    size_t len = 0;
    const uint8_t *kvp = rxp_props_bytes(p->version, &len);
    return moq_rcbuf_create(&g_plain, kvp, len, out) == MOQ_OK;
}

static bool rxp_gen_subgroup(rxp_side_t *p, int sub_role, uint64_t group,
                             const size_t *sizes, const bool *props, size_t n,
                             rxp_stream_t *st)
{
    bool any_props = false;
    for (size_t i = 0; i < n; i++) any_props = any_props || props[i];
    moq_subgroup_cfg_t g;
    moq_subgroup_cfg_init(&g);
    g.group_id = group;
    g.subgroup_id = 0;
    g.publisher_priority = 128;
    g.object_properties = any_props;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(p->sv, p->ssub[sub_role], &g, RXP_NOW, &sg) != MOQ_OK)
        return false;
    for (size_t i = 0; i < n; i++) {
        uint8_t body[512];
        if (sizes[i] > sizeof(body)) return false;
        rxp_payload(body, i, sizes[i]);
        moq_rcbuf_t *pl = NULL, *pr = NULL;
        if (moq_rcbuf_create(&g_plain, sizes[i] ? body : NULL, sizes[i], &pl) != MOQ_OK)
            return false;
        if (props[i] && !rxp_props(p, &pr)) {
            moq_rcbuf_decref(pl);
            return false;
        }
        moq_object_cfg_t oc;
        moq_object_cfg_init(&oc);
        oc.object_id = i;
        oc.payload = pl;
        oc.properties = pr;
        moq_result_t rc = moq_session_write_object_ex(p->sv, sg, &oc, RXP_NOW);
        moq_rcbuf_decref(pl);
        if (pr) moq_rcbuf_decref(pr);
        if (rc != MOQ_OK) return false;
    }
    if (moq_session_close_subgroup(p->sv, sg, RXP_NOW) != MOQ_OK) return false;
    return rxp_capture(p, st);
}

static const size_t g_a_sizes[] = { 5, 300, 0, 70 };
static const bool   g_a_props[] = { true, false, false, true };
static const size_t g_b_sizes[] = { 40, 1, 200 };
static const bool   g_b_props[] = { false, false, false };

static bool rxp_gen_fetch(rxp_side_t *p, rxp_stream_t *st)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t f;
    moq_fetch_cfg_init(&f);
    f.track_namespace = (moq_namespace_t){ parts, 1 };
    f.track_name = MOQ_BYTES_LITERAL("video");
    f.end_group = 3;
    if (moq_session_fetch(p->c, &f, RXP_NOW, &p->fetch) != MOQ_OK) return false;
    run(p->c, p->sv, RXP_NOW);
    moq_event_t ev;
    if (!take(p->sv, MOQ_EVENT_FETCH_REQUEST, &ev)) return false;
    p->sfetch = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 3;
    if (moq_session_accept_fetch(p->sv, p->sfetch, &ac, RXP_NOW) != MOQ_OK) return false;
    static const struct { uint64_t g, o; size_t n; } objs[] = {
        { 0, 0, 5 }, { 2, 0, 300 }, { 2, 1, 70 },
    };
    for (size_t i = 0; i < 3; i++) {
        if (i == 1 && moq_session_write_fetch_range(p->sv, p->sfetch,
                MOQ_FETCH_RANGE_NON_EXISTENT, 1, 0, RXP_NOW) != MOQ_OK)
            return false;
        uint8_t body[512];
        rxp_payload(body, objs[i].g + objs[i].o, objs[i].n);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = objs[i].g;
        oc.subgroup_id = 0;
        oc.object_id = objs[i].o;
        oc.publisher_priority = 128;
        if (moq_rcbuf_create(&g_plain, body, objs[i].n, &oc.payload) != MOQ_OK) return false;
        moq_result_t rc = moq_session_write_fetch_object(p->sv, p->sfetch, &oc, RXP_NOW);
        moq_rcbuf_decref(oc.payload);
        if (rc != MOQ_OK) return false;
    }
    if (moq_session_end_fetch(p->sv, p->sfetch, RXP_NOW) != MOQ_OK) return false;
    return rxp_capture(p, st);
}

static bool rxp_gen_fill(rxp_side_t *p, rxp_stream_t *st)
{
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    if (!rxp_subscribe(p, 0, &fill)) return false;
    /* The subscribe call announced the child: exactly one FILL_CREATED, whose
     * fill, parent and initiating request are what every later fill event on
     * this side must carry. */
    size_t created = 0;
    bool well_formed = true;
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_CREATED) {
            const moq_fill_created_event_t *fc = &ev.u.fill_created;
            created++;
            p->fill = fc->fill;
            p->fill_parent = fc->sub;
            p->fill_request = fc->initiating_request_id;
            well_formed = well_formed && moq_fill_is_valid(fc->fill) &&
                          !moq_publication_is_valid(fc->pub) &&
                          moq_subscription_is_valid(fc->sub) &&
                          fc->sub._opaque == p->sub[0]._opaque;
        }
        moq_event_cleanup(&ev);
    }
    if (created != 1 || !well_formed) return false;
    p->fill_known = true;
    if (!rxp_accept(p, 0, false, 0, true)) return false;
    moq_fill_t sf = { 0 };
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_REQUEST) sf = ev.u.fill_request.fill;
        moq_event_cleanup(&ev);
    }
    if (!moq_fill_is_valid(sf)) return false;
    if (moq_session_open_fill(p->sv, sf, RXP_NOW) != MOQ_OK) return false;
    uint8_t body[300];
    rxp_payload(body, 7, sizeof(body));
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 3;
    if (moq_rcbuf_create(&g_plain, body, sizeof(body), &oc.payload) != MOQ_OK) return false;
    moq_result_t rc = moq_session_write_fill_object(p->sv, sf, &oc, RXP_NOW);
    moq_rcbuf_decref(oc.payload);
    if (rc != MOQ_OK) return false;
    if (moq_session_end_fill(p->sv, sf, RXP_NOW) != MOQ_OK) return false;
    return rxp_capture(p, st);
}

/* The scenario's setup and streams on one side. */
static bool rxp_setup(rxp_side_t *p, const rxp_case_t *k)
{
    switch (k->scenario) {
    case RXP_SC_FETCH:
        return rxp_gen_fetch(p, &p->st[2]);
    case RXP_SC_FILL:
        return rxp_gen_fill(p, &p->st[2]);
    case RXP_SC_SHARED:
    case RXP_SC_HELD:
        if (!rxp_subscribe(p, 0, NULL) || !rxp_accept(p, 0, false, 0, true)) return false;
        if (!rxp_subscribe(p, 1, NULL)) return false;
        if (k->scenario == RXP_SC_SHARED && !rxp_accept(p, 1, true, p->alias[0], true))
            return false;
        return rxp_gen_subgroup(p, 0, 1, g_a_sizes, g_a_props, 4, &p->st[0]);
    default:
        if (!rxp_subscribe(p, 0, NULL) || !rxp_accept(p, 0, false, 0, true)) return false;
        if (!rxp_gen_subgroup(p, 0, 1, g_a_sizes, g_a_props, 4, &p->st[0]) ||
            !rxp_gen_subgroup(p, 0, 2, g_b_sizes, g_b_props, 3, &p->st[1]))
            return false;
        if (k->scenario == RXP_SC_SUPPRESS) {
            int slot = test_sub_resolve_handle(p->c, p->sub[0]);
            if (slot < 0) return false;
            p->c->subs[slot].forward = false;
        }
        return true;
    }
}

/* -- Delivered inventory ----------------------------------------------- */

static void rxp_inv_note(rxp_side_t *p, const moq_event_t *e)
{
    rxp_inv_t *v = &p->inv;
    uint64_t group = 0, id = 0;
    const moq_rcbuf_t *bytes = NULL;
    bool begin = true, end = true, normal = true;
    switch (e->kind) {
    case MOQ_EVENT_OBJECT_RECEIVED:
        group = e->u.object_received.group_id;
        id = e->u.object_received.object_id;
        bytes = e->u.object_received.payload;
        break;
    case MOQ_EVENT_OBJECT_CHUNK:
        group = e->u.object_chunk.group_id;
        id = e->u.object_chunk.object_id;
        bytes = e->u.object_chunk.chunk;
        begin = e->u.object_chunk.begin;
        end = e->u.object_chunk.end;
        normal = e->u.object_chunk.terminal == MOQ_OBJECT_TERMINAL_NORMAL;
        break;
    case MOQ_EVENT_SUBGROUP_FINISHED:
    case MOQ_EVENT_SUBGROUP_RESET: {
        bool fin = e->kind == MOQ_EVENT_SUBGROUP_FINISHED;
        uint64_t g = fin ? e->u.subgroup_finished.group_id : e->u.subgroup_reset.group_id;
        int role = g == 1 ? 0 : g == 2 ? 1 : -1;
        if (role < 0) {
            v->bad = true;
        } else if (fin) {
            v->finished[role]++;
            v->last[role] = e->u.subgroup_finished.has_last_object
                ? e->u.subgroup_finished.last_object_id : UINT64_MAX;
        } else {
            v->reset[role]++;
        }
        return;
    }
    default:
        return;
    }
    int role = group == 1 ? 0 : group == 2 ? 1 : -1;
    size_t count = role == 0 ? 4 : 3;
    if (role < 0 || id >= count) {
        v->bad = true;
        return;
    }
    const size_t *sizes = role == 0 ? g_a_sizes : g_b_sizes;
    size_t *got = &v->got[role][id];
    size_t len = moq_rcbuf_len(bytes);
    if ((begin && *got != 0) || *got + len > sizes[id]) {
        v->bad = true;
        return;
    }
    uint8_t want[512];
    rxp_payload(want, id, sizes[id]);
    if (len && memcmp(moq_rcbuf_data(bytes), want + *got, len) != 0) {
        v->bad = true;
        return;
    }
    *got += len;
    if (end && normal) {
        if (*got == sizes[id]) v->done[role][id]++;
        else v->bad = true;
    }
}

/* Every object of the role's stream exactly once with generator-exact bytes,
 * then exactly one SUBGROUP_FINISHED naming its last object, and no reset. */
static bool rxp_inv_complete(const rxp_inv_t *v, int role)
{
    size_t count = role == 0 ? 4 : 3;
    if (v->bad || v->finished[role] != 1 || v->reset[role] != 0 ||
        v->last[role] != count - 1)
        return false;
    for (size_t i = 0; i < count; i++)
        if (v->done[role][i] != 1) return false;
    return true;
}

static bool rxp_inv_empty(const rxp_inv_t *v, int role)
{
    if (v->bad || v->finished[role] || v->reset[role]) return false;
    for (size_t i = 0; i < RXP_INV_OBJECTS; i++)
        if (v->got[role][i] || v->done[role][i]) return false;
    return true;
}

/* Subgroup cases with nothing refused, changed or stopped by a budget: in
 * each arm on its own, a stream fed through its FIN (and never reset) must
 * have delivered exactly its inventory, and a stream never fed nothing. */
static void rxp_inv_expect(const rxp_side_t *r, const rxp_side_t *n, const rxp_case_t *k,
                           const rxp_piece_t *pcs, size_t npcs, const char *label)
{
    if (k->scenario != RXP_SC_SUBGROUPS || k->budget || k->fail_at || k->flip) return;
    for (int role = 0; role < 2; role++) {
        bool fed = false, fin = false, reset = false;
        for (size_t i = 0; i < npcs; i++) {
            if (pcs[i].role != role) continue;
            fed = true;
            if (pcs[i].reset) reset = true;
            else if (pcs[i].fin) fin = true;
        }
        bool complete = fin && !reset;
        if (!complete && fed) continue;
        bool okr = complete ? rxp_inv_complete(&r->inv, role) : rxp_inv_empty(&r->inv, role);
        bool okn = complete ? rxp_inv_complete(&n->inv, role) : rxp_inv_empty(&n->inv, role);
        if (!okr || !okn) {
            g_inventory_failures++;
            rxp_note(complete ? "inventory incomplete" : "inventory of an unfed stream", label);
        }
    }
}

/* -- Semantic comparison ---------------------------------------------- */

/* Each side's handles map to roles: 0/1 its subscriptions (0 its fetch or
 * fill), -1 invalid; an unknown handle is -2, which never matches. */
static int rxp_sub_role(const rxp_side_t *p, moq_subscription_t h)
{
    if (!moq_subscription_is_valid(h)) return -1;
    for (int i = 0; i < 2; i++)
        if (moq_subscription_is_valid(p->sub[i]) && h._opaque == p->sub[i]._opaque)
            return i;
    return -2;
}

static int rxp_pub_role(moq_publication_t h)
{
    return moq_publication_is_valid(h) ? -2 : -1;
}

static int rxp_fetch_role(const rxp_side_t *p, moq_fetch_t h)
{
    if (!moq_fetch_is_valid(h)) return -1;
    return h._opaque == p->fetch._opaque ? 0 : -2;
}

/* Role 0 is only the child this side's FILL_CREATED announced; any other
 * handle, however well-formed, never matches. */
static int rxp_fill_role(const rxp_side_t *p, moq_fill_t h)
{
    if (!moq_fill_is_valid(h)) return -1;
    return p->fill_known && h._opaque == p->fill._opaque ? 0 : -2;
}

/* A fill event's parent and initiating request are the announced child's. */
static bool rxp_fill_origin(const rxp_side_t *p, moq_subscription_t sub,
                            moq_publication_t pub, uint64_t request)
{
    return p->fill_known && moq_subscription_is_valid(sub) &&
           sub._opaque == p->fill_parent._opaque && !moq_publication_is_valid(pub) &&
           request == p->fill_request;
}

static bool rxp_same_role(int a, int b) { return a == b && a != -2; }

static int rxp_cmp_bytes(moq_bytes_t a, moq_bytes_t b)
{
    return a.len != b.len || (a.len && memcmp(a.data, b.data, a.len) != 0);
}

static int rxp_cmp_buf(const moq_rcbuf_t *x, const moq_rcbuf_t *y)
{
    if (!x != !y) return 1;
    if (!x) return 0;
    size_t n = moq_rcbuf_len(x);
    if (n != moq_rcbuf_len(y)) return 1;
    if (moq_rcbuf_refcount(x) != moq_rcbuf_refcount(y)) return 1;
    return n && memcmp(moq_rcbuf_data(x), moq_rcbuf_data(y), n) != 0;
}

#define RXP_GAPS_DIFFER(x, y) \
    ((x)->has_prior_group_gap != (y)->has_prior_group_gap || \
     (x)->has_prior_object_gap != (y)->has_prior_object_gap || \
     ((x)->has_prior_group_gap && (x)->prior_group_gap != (y)->prior_group_gap) || \
     ((x)->has_prior_object_gap && (x)->prior_object_gap != (y)->prior_object_gap))

#define RXP_SUBPUB_DIFFER(x, y) \
    (!rxp_same_role(rxp_sub_role(r, (x)->sub), rxp_sub_role(n, (y)->sub)) || \
     !rxp_same_role(rxp_pub_role((x)->pub), rxp_pub_role((y)->pub)))

/* A fill event names its own side's announced child (role 0: an invalid or
 * other handle never matches, even when both arms agree) and that child's
 * parent and request. */
#define RXP_FILL_DIFFER(x, y) \
    (rxp_fill_role(r, (x)->fill) != 0 || rxp_fill_role(n, (y)->fill) != 0 || \
     !rxp_fill_origin(r, (x)->sub, (x)->pub, (x)->initiating_request_id) || \
     !rxp_fill_origin(n, (y)->sub, (y)->pub, (y)->initiating_request_id))

static int rxp_cmp_event(const rxp_side_t *r, const moq_event_t *a,
                         const rxp_side_t *n, const moq_event_t *b)
{
    if (a->kind != b->kind) return 1;
    switch (a->kind) {
    case MOQ_EVENT_OBJECT_RECEIVED: {
        const moq_object_received_event_t *x = &a->u.object_received;
        const moq_object_received_event_t *y = &b->u.object_received;
        if (RXP_SUBPUB_DIFFER(x, y) ||
            x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
            x->object_id != y->object_id || x->status != y->status ||
            x->publisher_priority != y->publisher_priority ||
            x->end_of_group != y->end_of_group || x->datagram != y->datagram ||
            x->first_object != y->first_object ||
            x->subgroup_properties != y->subgroup_properties ||
            RXP_GAPS_DIFFER(x, y))
            return 1;
        return rxp_cmp_buf(x->payload, y->payload) ||
               rxp_cmp_buf(x->properties, y->properties);
    }
    case MOQ_EVENT_OBJECT_CHUNK: {
        const moq_object_chunk_event_t *x = &a->u.object_chunk;
        const moq_object_chunk_event_t *y = &b->u.object_chunk;
        if (RXP_SUBPUB_DIFFER(x, y) ||
            x->begin != y->begin || x->end != y->end || x->terminal != y->terminal ||
            x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
            x->object_id != y->object_id)
            return 1;
        if (x->end && x->terminal == MOQ_OBJECT_TERMINAL_RESET &&
            x->error_code != y->error_code)
            return 1;
        if (x->begin &&
            (x->publisher_priority != y->publisher_priority ||
             x->status != y->status || x->end_of_group != y->end_of_group ||
             x->first_object != y->first_object ||
             x->payload_length != y->payload_length ||
             x->subgroup_properties != y->subgroup_properties ||
             RXP_GAPS_DIFFER(x, y)))
            return 1;
        return rxp_cmp_buf(x->chunk, y->chunk) ||
               rxp_cmp_buf(x->properties, y->properties);
    }
    case MOQ_EVENT_SUBGROUP_FINISHED: {
        const moq_subgroup_finished_event_t *x = &a->u.subgroup_finished;
        const moq_subgroup_finished_event_t *y = &b->u.subgroup_finished;
        return RXP_SUBPUB_DIFFER(x, y) ||
               x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
               x->end_of_group != y->end_of_group ||
               x->has_last_object != y->has_last_object ||
               (x->has_last_object && x->last_object_id != y->last_object_id);
    }
    case MOQ_EVENT_SUBGROUP_RESET: {
        const moq_subgroup_reset_event_t *x = &a->u.subgroup_reset;
        const moq_subgroup_reset_event_t *y = &b->u.subgroup_reset;
        return RXP_SUBPUB_DIFFER(x, y) ||
               x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
               x->error_code != y->error_code || x->end_of_group != y->end_of_group ||
               x->has_last_object != y->has_last_object ||
               (x->has_last_object && x->last_object_id != y->last_object_id);
    }
    case MOQ_EVENT_SESSION_CLOSED:
        return a->u.closed.code != b->u.closed.code ||
               rxp_cmp_bytes(a->u.closed.reason, b->u.closed.reason);
    case MOQ_EVENT_SUBSCRIBE_OK:
        return !rxp_same_role(rxp_sub_role(r, a->u.subscribe_ok.sub),
                              rxp_sub_role(n, b->u.subscribe_ok.sub)) ||
               a->u.subscribe_ok.track_alias != b->u.subscribe_ok.track_alias;
    case MOQ_EVENT_FETCH_OBJECT: {
        const moq_fetch_object_event_t *x = &a->u.fetch_object;
        const moq_fetch_object_event_t *y = &b->u.fetch_object;
        if (!rxp_same_role(rxp_fetch_role(r, x->fetch), rxp_fetch_role(n, y->fetch)) ||
            x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
            x->object_id != y->object_id ||
            x->publisher_priority != y->publisher_priority ||
            x->datagram != y->datagram || RXP_GAPS_DIFFER(x, y))
            return 1;
        return rxp_cmp_buf(x->payload, y->payload) ||
               rxp_cmp_buf(x->properties, y->properties);
    }
    case MOQ_EVENT_FETCH_GAP: {
        const moq_fetch_gap_event_t *x = &a->u.fetch_gap;
        const moq_fetch_gap_event_t *y = &b->u.fetch_gap;
        return !rxp_same_role(rxp_fetch_role(r, x->fetch), rxp_fetch_role(n, y->fetch)) ||
               x->range_kind != y->range_kind || x->group_id != y->group_id ||
               x->object_id != y->object_id;
    }
    case MOQ_EVENT_FETCH_COMPLETE:
        return !rxp_same_role(rxp_fetch_role(r, a->u.fetch_complete.fetch),
                              rxp_fetch_role(n, b->u.fetch_complete.fetch));
    case MOQ_EVENT_FILL_OBJECT: {
        const moq_fill_object_event_t *x = &a->u.fill_object;
        const moq_fill_object_event_t *y = &b->u.fill_object;
        if (RXP_FILL_DIFFER(x, y) || RXP_SUBPUB_DIFFER(x, y) ||
            x->initiating_request_id != y->initiating_request_id ||
            x->group_id != y->group_id || x->subgroup_id != y->subgroup_id ||
            x->object_id != y->object_id ||
            x->publisher_priority != y->publisher_priority ||
            x->datagram != y->datagram || RXP_GAPS_DIFFER(x, y))
            return 1;
        return rxp_cmp_buf(x->payload, y->payload) ||
               rxp_cmp_buf(x->properties, y->properties);
    }
    case MOQ_EVENT_FILL_GAP: {
        const moq_fill_gap_event_t *x = &a->u.fill_gap;
        const moq_fill_gap_event_t *y = &b->u.fill_gap;
        return RXP_FILL_DIFFER(x, y) || RXP_SUBPUB_DIFFER(x, y) ||
               x->initiating_request_id != y->initiating_request_id ||
               x->group_id != y->group_id || x->object_id != y->object_id ||
               x->kind != y->kind;
    }
    case MOQ_EVENT_FILL_TERMINAL: {
        const moq_fill_terminal_event_t *x = &a->u.fill_terminal;
        const moq_fill_terminal_event_t *y = &b->u.fill_terminal;
        return RXP_FILL_DIFFER(x, y) || RXP_SUBPUB_DIFFER(x, y) ||
               x->initiating_request_id != y->initiating_request_id ||
               x->reason != y->reason || x->error_domain != y->error_domain ||
               x->error_code != y->error_code;
    }
    case MOQ_EVENT_UNSUBSCRIBED: {
        /* It must name a subscription this fixture holds, on both sides: an
         * invalid or unknown handle refuses, whatever the other side says. */
        int ra = rxp_sub_role(r, a->u.unsubscribed.sub);
        int rb = rxp_sub_role(n, b->u.unsubscribed.sub);
        return ra < 0 || rb < 0 || ra != rb ||
               a->u.unsubscribed.cause != b->u.unsubscribed.cause ||
               a->u.unsubscribed.stream_error_code != b->u.unsubscribed.stream_error_code;
    }
    default:
        return 1;  /* an output kind this oracle does not model */
    }
}

static int rxp_cmp_action(const moq_action_t *a, const moq_action_t *b)
{
    if (a->kind != b->kind) return 1;
    switch (a->kind) {
    case MOQ_ACTION_STOP_DATA:
        return a->u.stop_data.stream_ref._v != b->u.stop_data.stream_ref._v ||
               a->u.stop_data.error_code != b->u.stop_data.error_code;
    case MOQ_ACTION_CLOSE_SESSION:
        return a->u.close_session.code != b->u.close_session.code ||
               rxp_cmp_bytes(a->u.close_session.reason, b->u.close_session.reason);
    case MOQ_ACTION_SEND_CONTROL:
        return rxp_cmp_bytes(
            (moq_bytes_t){ a->u.send_control.data, a->u.send_control.len },
            (moq_bytes_t){ b->u.send_control.data, b->u.send_control.len });
    case MOQ_ACTION_SEND_UNI_CONTROL:
        return a->u.send_uni_control.stream_ref._v != b->u.send_uni_control.stream_ref._v ||
               a->u.send_uni_control.fin != b->u.send_uni_control.fin ||
               rxp_cmp_bytes(
                   (moq_bytes_t){ a->u.send_uni_control.data, a->u.send_uni_control.len },
                   (moq_bytes_t){ b->u.send_uni_control.data, b->u.send_uni_control.len });
    case MOQ_ACTION_SEND_BIDI_STREAM:
        return a->u.send_bidi_stream.stream_ref._v != b->u.send_bidi_stream.stream_ref._v ||
               a->u.send_bidi_stream.fin != b->u.send_bidi_stream.fin ||
               rxp_cmp_bytes(
                   (moq_bytes_t){ a->u.send_bidi_stream.data, a->u.send_bidi_stream.len },
                   (moq_bytes_t){ b->u.send_bidi_stream.data, b->u.send_bidi_stream.len });
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return a->u.close_bidi_stream.stream_ref._v != b->u.close_bidi_stream.stream_ref._v;
    case MOQ_ACTION_ABORT_BIDI_STREAM:
        return a->u.abort_bidi_stream.stream_ref._v != b->u.abort_bidi_stream.stream_ref._v ||
               a->u.abort_bidi_stream.error_code != b->u.abort_bidi_stream.error_code;
    default:
        return 1;  /* an output kind this oracle does not model */
    }
}

static const moq_rx_stream_t *rxp_entry(const moq_session_t *s, uint64_t ref_v, int *slot)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (s->rx_streams[i].active && s->rx_streams[i].stream_ref._v == ref_v) {
            if (slot) *slot = (int)i;
            return &s->rx_streams[i];
        }
    return NULL;
}

/* An entry's bindings, by role: subscription, publication, fetch. The fill
 * binding is an index pair inside each session rather than a handle; two
 * sessions driven by the same calls hold the same pair. */
static int rxp_cmp_binding(const rxp_side_t *r, const moq_rx_stream_t *x,
                           const rxp_side_t *n, const moq_rx_stream_t *y)
{
    if (!rxp_same_role(rxp_sub_role(r, x->sub), rxp_sub_role(n, y->sub)) ||
        !rxp_same_role(rxp_pub_role(x->pub_handle), rxp_pub_role(y->pub_handle)) ||
        !rxp_same_role(rxp_fetch_role(r, x->fetch), rxp_fetch_role(n, y->fetch)) ||
        x->fill_bound != y->fill_bound || x->binding_ended != y->binding_ended ||
        x->stop_cancels != y->stop_cancels)
        return 1;
    if (x->fill_bound &&
        (x->fill_ref.slot != y->fill_ref.slot ||
         x->fill_ref.generation != y->fill_ref.generation ||
         x->fill_ref.parent.family != y->fill_ref.parent.family ||
         x->fill_ref.parent.slot != y->fill_ref.parent.slot ||
         x->fill_ref.parent.generation != y->fill_ref.parent.generation))
        return 1;
    return 0;
}

/* A shared-alias stream's recipients: each initialized record by role and
 * flag, in order. */
static int rxp_cmp_fan(const rxp_side_t *r, const moq_rx_stream_t *x,
                       const rxp_side_t *n, const moq_rx_stream_t *y)
{
    if (x->fan_shared != y->fan_shared || x->fan_ambiguous != y->fan_ambiguous ||
        x->fan_count != y->fan_count)
        return 1;
    if (x->fan_count > x->fan_cap || y->fan_count > y->fan_cap) return 1;
    if (x->fan_count && (!x->fan || !y->fan)) return 1;
    for (uint32_t i = 0; i < x->fan_count; i++) {
        const moq_rx_fan_t *a = &x->fan[i];
        const moq_rx_fan_t *b = &y->fan[i];
        if (!rxp_same_role(rxp_sub_role(r, a->sub), rxp_sub_role(n, b->sub)) ||
            !rxp_same_role(rxp_pub_role(a->pub), rxp_pub_role(b->pub)) ||
            a->pending != b->pending || a->cur != b->cur || a->owed != b->owed ||
            a->closed != b->closed || a->closure != b->closure)
            return 1;
    }
    return 0;
}

static int rxp_cmp_entry(const rxp_side_t *r, const rxp_side_t *n, uint64_t ref_v)
{
    const moq_rx_stream_t *x = rxp_entry(r->c, ref_v, NULL);
    const moq_rx_stream_t *y = rxp_entry(n->c, ref_v, NULL);
    if (!x != !y) return 1;
    if (!x) return 0;
    if (rxp_cmp_binding(r, x, n, y) || rxp_cmp_fan(r, x, n, y)) return 1;
    if (x->default_priority != y->default_priority ||
        x->publisher_priority != y->publisher_priority ||
        x->has_extensions != y->has_extensions || x->end_of_group != y->end_of_group ||
        x->first_object != y->first_object || x->cur_first_object != y->cur_first_object ||
        x->has_prev_object != y->has_prev_object ||
        (x->has_prev_object && x->prev_object_id != y->prev_object_id) ||
        x->subgroup_id_resolved != y->subgroup_id_resolved ||
        x->resumed_deferred != y->resumed_deferred ||
        x->fetch_order_known != y->fetch_order_known ||
        (x->fetch_order_known &&
         x->effective_fetch_group_order != y->effective_fetch_group_order))
        return 1;
    if (x->parse_state != y->parse_state || x->stream_kind != y->stream_kind ||
        x->track_alias != y->track_alias || x->group_id != y->group_id ||
        x->subgroup_id != y->subgroup_id || x->cur_object_id != y->cur_object_id ||
        x->cur_status != y->cur_status ||
        x->payload_expected != y->payload_expected ||
        x->payload_written != y->payload_written ||
        x->input_len != y->input_len || x->input_cap != y->input_cap ||
        (x->input_buf == NULL) != (y->input_buf == NULL) ||
        x->hdr_len != y->hdr_len || x->pending_fin != y->pending_fin ||
        x->pending_begin != y->pending_begin || x->pending_end != y->pending_end ||
        x->pending_terminal != y->pending_terminal ||
        x->pending_data_len != y->pending_data_len ||
        x->pending_from_input != y->pending_from_input ||
        x->suppress_cur_object != y->suppress_cur_object ||
        x->reset_owed != y->reset_owed ||
        (x->reset_owed && x->reset_error_code != y->reset_error_code) ||
        x->has_last_complete != y->has_last_complete ||
        (x->has_last_complete && x->last_complete_object_id != y->last_complete_object_id))
        return 1;
    if (x->pending_terminal == MOQ_OBJECT_TERMINAL_RESET &&
        x->pending_reset_code != y->pending_reset_code)
        return 1;
    if (x->hdr_len && memcmp(x->hdr_buf, y->hdr_buf, x->hdr_len) != 0) return 1;
    if (x->input_len && x->input_buf &&
        memcmp(x->input_buf, y->input_buf, x->input_len) != 0)
        return 1;
    if (rxp_cmp_buf(x->cur_extensions, y->cur_extensions)) return 1;
    if (rxp_cmp_buf(x->pending_chunk, y->pending_chunk)) return 1;
    if (!x->payload_rcbuf != !y->payload_rcbuf) return 1;
    if (x->payload_rcbuf) {
        if (moq_rcbuf_len(x->payload_rcbuf) != moq_rcbuf_len(y->payload_rcbuf) ||
            moq_rcbuf_refcount(x->payload_rcbuf) != moq_rcbuf_refcount(y->payload_rcbuf) ||
            (!x->payload_buf) != (!y->payload_buf))
            return 1;
        /* Only the bytes written so far are initialized. */
        if (x->payload_buf && x->payload_written &&
            memcmp(x->payload_buf, y->payload_buf, x->payload_written) != 0)
            return 1;
    }
    return 0;
}

static int rxp_cmp_state(const rxp_side_t *r, const rxp_side_t *n)
{
    if (moq_session_state(r->c) != moq_session_state(n->c)) return 1;
    if (r->c->recv_input_bytes != n->c->recv_input_bytes) return 1;
    if (r->c->recv_payload_bytes != n->c->recv_payload_bytes) return 1;
    if (r->as.calls != n->as.calls || r->as.balance != n->as.balance ||
        r->as.live != n->as.live || r->as.fail_hits != n->as.fail_hits ||
        r->as.last_refused != n->as.last_refused)
        return 1;
    for (int i = 0; i < RXP_ROLES; i++)
        if (rxp_cmp_entry(r, n, rxp_ref(i))) return 1;
    return 0;
}

/* Overwrite every entry's staging beyond its logical length with this arm's
 * own byte: nothing may read there, and the two arms now differ if one does. */
static void rxp_poison_tails(rxp_side_t *p)
{
    for (size_t i = 0; i < p->c->rx_cap; i++) {
        moq_rx_stream_t *x = &p->c->rx_streams[i];
        if (x->active && x->input_buf && x->input_cap > x->input_len)
            memset(x->input_buf + x->input_len, p->poison, x->input_cap - x->input_len);
    }
}

/* Count an UNSUBSCRIBED by its own fields (see rxp_side_t). */
static void rxp_count_unsubscribed(rxp_side_t *p, const moq_event_t *e)
{
    if (e->kind != MOQ_EVENT_UNSUBSCRIBED) return;
    const moq_unsubscribed_event_t *u = &e->u.unsubscribed;
    if (rxp_sub_role(p, u->sub) == 0 &&
        u->cause == MOQ_REQUEST_TERMINAL_MALFORMED_TRACK && u->stream_error_code == 0)
        p->unsub_role0_malformed++;
    else
        p->unsub_other++;
}

/* The targeted d16 known answer for one side, independent of the other
 * arm: exactly one UNSUBSCRIBED, for role 0, with the malformed-track cause
 * and stream error code 0, and no other UNSUBSCRIBED. */
static bool rxp_unsub_known_answer(const rxp_side_t *p)
{
    return p->unsub_role0_malformed == 1 && p->unsub_other == 0;
}

static void rxp_drain(rxp_side_t *r, rxp_side_t *n, size_t poll_cap, const char *where)
{
    for (size_t polled = 0; polled < poll_cap; polled++) {
        moq_event_t er, en;
        size_t cr = moq_session_poll_events(r->c, &er, 1);
        size_t cn = moq_session_poll_events(n->c, &en, 1);
        if (cr != cn) rxp_note("event count", where);
        if (cr && cn && rxp_cmp_event(r, &er, n, &en)) rxp_note("event", where);
        if (cr) {
            rxp_count_unsubscribed(r, &er);
            rxp_inv_note(r, &er);
            moq_event_cleanup(&er);
        }
        if (cn) {
            rxp_count_unsubscribed(n, &en);
            rxp_inv_note(n, &en);
            moq_event_cleanup(&en);
        }
        if (!cr || !cn) break;
    }
    for (;;) {
        moq_action_t ar, an;
        size_t cr = moq_session_poll_actions(r->c, &ar, 1);
        size_t cn = moq_session_poll_actions(n->c, &an, 1);
        if (cr != cn) rxp_note("action count", where);
        if (cr && cn && rxp_cmp_action(&ar, &an)) rxp_note("action", where);
        if (cr) moq_action_cleanup(&ar);
        if (cn) moq_action_cleanup(&an);
        if (!cr || !cn) break;
    }
}

/* One delivery from a fresh caller buffer. The buffer is overwritten with
 * volatile stores once the call returns; an input rcbuf is overwritten only
 * when the session kept no reference to it (a kept slice is legitimate). */
static moq_result_t rxp_call(rxp_side_t *p, uint64_t ref_v, const uint8_t *src,
                             size_t n, bool fin, int route, size_t flip)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(ref_v);
    moq_result_t rc;
    session_rx_stage_reference = p->reference;
    if (route == RXP_RCBUF && n > 0) {
        moq_rcbuf_t *rb = NULL;
        if (moq_rcbuf_create(&g_plain, src, n, &rb) != MOQ_OK) abort();
        uint8_t *own = (uint8_t *)(uintptr_t)moq_rcbuf_data(rb);
        if (flip < n) own[flip] ^= 0x5A;
        rc = moq_session_on_data_rcbuf(p->c, ref, rb, fin, RXP_NOW);
        if (moq_rcbuf_refcount(rb) == 1) rxp_scrub(own, n, 0xA5);
        moq_rcbuf_decref(rb);
    } else {
        uint8_t *cb = NULL;
        if (n > 0) {
            cb = (uint8_t *)malloc(n);
            if (!cb) abort();
            memcpy(cb, src, n);
            if (flip < n) cb[flip] ^= 0x5A;
        }
        rc = moq_session_on_data_bytes(p->c, ref, cb, n, fin, RXP_NOW);
        if (cb) {
            rxp_scrub(cb, n, 0xA5);
            free(cb);
        }
    }
    session_rx_stage_reference = false;
    return rc;
}

static void rxp_after_call(rxp_side_t *r, rxp_side_t *n, moq_result_t rr,
                           moq_result_t rn, const char *what, const char *label)
{
    if (rr != rn) rxp_note(what, label);
    if (rxp_cmp_state(r, n)) rxp_note("state", label);
    rxp_poison_tails(r);
    rxp_poison_tails(n);
}

static moq_result_t rxp_reset(rxp_side_t *p, uint64_t ref_v, uint64_t code)
{
    return moq_session_on_data_reset(p->c, moq_stream_ref_from_u64(ref_v), code, RXP_NOW);
}

enum { RXP_DELIVERED = 0, RXP_HELD, RXP_PARKED };

/* Called after each delivery call with the arm's refusal count from before
 * it. When this call is the one in which the arm's single refusal fired, and
 * it left role 0's stream (whole-object, subgroup case) at an object whose
 * payload allocation was refused -- AWAITING_OBJECT, no payload buffer, the
 * refused size that object's own payload allocation size -- while holding
 * that object's properties, the allocation id of that very properties block
 * is recorded. Only that block can later be the known observation. */
static void rxp_note_refusal_site(rxp_side_t *p, const rxp_case_t *k, int role,
                                  uint64_t hits_before)
{
    if (hits_before != 0 || p->as.fail_hits != 1) return;
    if (k->streaming || k->scenario != RXP_SC_SUBGROUPS || role != 0) return;
    const moq_rx_stream_t *x = rxp_entry(p->c, rxp_ref(role), NULL);
    if (!x || x->parse_state != MOQ_RX_AWAITING_OBJECT || x->payload_rcbuf ||
        !x->cur_extensions || x->cur_object_id >= 4)
        return;
    size_t id = (size_t)x->cur_object_id, pay = 0;
    if (!g_a_props[id] || g_a_sizes[id] == 0 ||
        moq_rcbuf_allocation_size(g_a_sizes[id], &pay) != MOQ_OK ||
        pay != p->as.last_refused)
        return;
    p->known_props_id = rxp_block_id(&p->as, x->cur_extensions);
}

/* One data piece to both arms. A WOULD_BLOCK is settled by each arm's actual
 * ownership of the stream (the arms must agree): owned, the bytes and FIN
 * were retained, so the full queue is drained and an empty retry resumes
 * them, unless the piece is to be left owed (`hold`); not owned, nothing was
 * retained, and the piece is returned as PARKED for replay of the same bytes
 * and FIN. */
static int rxp_deliver(rxp_side_t *r, rxp_side_t *n, const rxp_case_t *k,
                       const rxp_piece_t *pc, const char *label)
{
    uint64_t ref_v = rxp_ref(pc->role);
    const rxp_stream_t *t = &r->st[pc->role];
    size_t flip = RXP_NO_FLIP;
    if (pc->role == 0 && k->flip &&
        k->flip_at >= pc->off && k->flip_at < pc->off + pc->len)
        flip = k->flip_at - pc->off;
    uint64_t hr = r->as.fail_hits, hn = n->as.fail_hits;
    moq_result_t rr = rxp_call(r, ref_v, t->bytes + pc->off, pc->len, pc->fin, k->route,
                               RXP_NO_FLIP);
    moq_result_t rn = rxp_call(n, ref_v, t->bytes + pc->off, pc->len, pc->fin, k->route,
                               flip);
    rxp_after_call(r, n, rr, rn, "return code", label);
    rxp_note_refusal_site(r, k, pc->role, hr);
    rxp_note_refusal_site(n, k, pc->role, hn);
    if (rr != MOQ_ERR_WOULD_BLOCK || rn != MOQ_ERR_WOULD_BLOCK) return RXP_DELIVERED;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(ref_v);
    bool owned_r = moq_session_has_transport_stream(r->c, ref);
    bool owned_n = moq_session_has_transport_stream(n->c, ref);
    if (owned_r != owned_n) {
        rxp_note("stream ownership differs between arms", label);
        return RXP_DELIVERED;
    }
    if (!owned_r && !k->empty_unowned) {
        g_parked++;
        return RXP_PARKED;
    }
    if (pc->hold) return RXP_HELD;
    for (int g = 0; g < RXP_RETRIES && rr == MOQ_ERR_WOULD_BLOCK &&
                    rn == MOQ_ERR_WOULD_BLOCK; g++) {
        rxp_drain(r, n, k->poll_cap ? k->poll_cap : 1, label);
        hr = r->as.fail_hits;
        hn = n->as.fail_hits;
        rr = rxp_call(r, ref_v, NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
        rn = rxp_call(n, ref_v, NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
        rxp_after_call(r, n, rr, rn, "retry return code", label);
        rxp_note_refusal_site(r, k, pc->role, hr);
        rxp_note_refusal_site(n, k, pc->role, hn);
    }
    if (rr == MOQ_ERR_WOULD_BLOCK || rn == MOQ_ERR_WOULD_BLOCK)
        rxp_note("retries exhausted", label);
    return RXP_DELIVERED;
}

/* Replay parked pieces, oldest first. A piece of a stream the session now
 * owns (in both arms: its parked head was admitted) is delivered as it is; a
 * piece of a stream still unowned waits until both arms can admit a new
 * stream. A replay refused again (another stream took the entry) stays
 * parked, with everything behind it. */
static void rxp_replay(rxp_side_t *r, rxp_side_t *n, const rxp_case_t *k,
                       rxp_piece_t *parked, size_t *np, const char *label)
{
    while (*np > 0) {
        rxp_piece_t pc = parked[0];
        moq_stream_ref_t ref = moq_stream_ref_from_u64(rxp_ref(pc.role));
        bool owned_r = moq_session_has_transport_stream(r->c, ref);
        bool owned_n = moq_session_has_transport_stream(n->c, ref);
        if (owned_r != owned_n) {
            rxp_note("stream ownership differs between arms", label);
            return;
        }
        if (!owned_r) {
            bool ar = moq_session_can_admit_data_stream(r->c);
            bool an = moq_session_can_admit_data_stream(n->c);
            if (ar != an) {
                rxp_note("admission differs between arms", label);
                return;
            }
            if (!ar) return;
        }
        if (rxp_deliver(r, n, k, &pc, label) == RXP_PARKED) return;
        g_replayed++;
        memmove(parked, parked + 1, (*np - 1) * sizeof(*parked));
        (*np)--;
        rxp_drain(r, n, k->poll_cap, label);
        if (rxp_cmp_state(r, n)) rxp_note("state after replay", label);
    }
}

/* The slot-reuse layout (rxp_reuse): role 0 in two pieces, then role 1 in
 * two. With one receive slot, after piece `i`: 0, role 0 holds the slot in
 * both arms; 1, role 0's FIN released it (no entry, no ownership, a stream
 * can be admitted); 2, role 1 holds that same slot. Each condition is
 * required on its own; the final inventories prove what both streams
 * delivered. */
static void rxp_reuse_check(const rxp_side_t *r, const rxp_side_t *n, size_t i,
                            int *slot0, const char *label)
{
    moq_stream_ref_t ref0 = moq_stream_ref_from_u64(rxp_ref(0));
    int sr = -1, sn = -1;
    if (r->c->rx_cap != 1 || n->c->rx_cap != 1)
        rxp_note("more than one receive slot", label);
    switch (i) {
    case 0:
        if (!rxp_entry(r->c, rxp_ref(0), &sr) || !rxp_entry(n->c, rxp_ref(0), &sn) ||
            sr != sn)
            rxp_note("first stream not admitted", label);
        else
            *slot0 = sr;
        break;
    case 1:
        if (rxp_entry(r->c, rxp_ref(0), NULL) || rxp_entry(n->c, rxp_ref(0), NULL) ||
            moq_session_has_transport_stream(r->c, ref0) ||
            moq_session_has_transport_stream(n->c, ref0))
            rxp_note("first stream not released", label);
        if (!moq_session_can_admit_data_stream(r->c) ||
            !moq_session_can_admit_data_stream(n->c))
            rxp_note("released slot not admissible", label);
        break;
    case 2:
        if (*slot0 < 0 || !rxp_entry(r->c, rxp_ref(1), &sr) ||
            !rxp_entry(n->c, rxp_ref(1), &sn) || sr != *slot0 || sn != *slot0)
            rxp_note("second stream not admitted into the released slot", label);
        break;
    default:
        break;
    }
}

static void rxp_feed(rxp_side_t *r, rxp_side_t *n, const rxp_case_t *k,
                     const rxp_piece_t *pcs, size_t npcs, const char *label)
{
    rxp_piece_t parked[RXP_PARK_MAX];
    size_t nparked = 0;
    int slot0 = -1;
    if (k->slot_reuse && npcs != 4) rxp_note("slot reuse layout", label);
    for (size_t i = 0; i < npcs; i++) {
        const rxp_piece_t *pc = &pcs[i];
        uint64_t ref_v = rxp_ref(pc->role);
        moq_result_t rr, rn;
        if (pc->reset) {
            rr = rxp_reset(r, ref_v, pc->code);
            rn = rxp_reset(n, ref_v, pc->code);
            rxp_after_call(r, n, rr, rn, "reset return code", label);
            for (int g = 0; g < RXP_RETRIES && rr == MOQ_ERR_WOULD_BLOCK &&
                            rn == MOQ_ERR_WOULD_BLOCK; g++) {
                rxp_drain(r, n, 1, label);
                rr = rxp_reset(r, ref_v, pc->code);
                rn = rxp_reset(n, ref_v, pc->code);
                rxp_after_call(r, n, rr, rn, "reset retry return code", label);
            }
            if (rr == MOQ_ERR_WOULD_BLOCK || rn == MOQ_ERR_WOULD_BLOCK)
                rxp_note("reset retries exhausted", label);
        } else {
            if (pc->off + pc->len > r->st[pc->role].len) {
                rxp_note("piece bounds", label);
                return;
            }
            /* A stream with a parked piece is not admitted: later pieces of
             * it queue behind, in order. */
            bool behind = false;
            for (size_t j = 0; j < nparked; j++)
                behind = behind || parked[j].role == pc->role;
            int got = behind ? RXP_PARKED : rxp_deliver(r, n, k, pc, label);
            if (got == RXP_HELD) continue;
            if (got == RXP_PARKED) {
                if (nparked == RXP_PARK_MAX) {
                    rxp_note("too many parked deliveries", label);
                    return;
                }
                parked[nparked++] = *pc;
            }
        }
        rxp_drain(r, n, k->poll_cap, label);
        if (rxp_cmp_state(r, n)) rxp_note("state after drain", label);
        rxp_replay(r, n, k, parked, &nparked, label);
        if (k->slot_reuse) rxp_reuse_check(r, n, i, &slot0, label);
    }
    rxp_replay(r, n, k, parked, &nparked, label);
    if (nparked) rxp_note("parked delivery never admitted", label);
}

/* Settle an allocator-refusal case after its sessions. An index the case
 * never reached is an ordinary run and must leave nothing. A refusal that
 * fired must have fired once in each arm, at the same size, and leave
 * nothing, with one exception: the known observation, where each arm on its
 * own still holds exactly the properties block recorded at its refusal
 * (rxp_note_refusal_site), by allocation id. It is counted by name, never as
 * a clean result. Any other residue fails, whatever its size or bytes. */
static void rxp_refusal_settle(rxp_side_t *r, rxp_side_t *n, const rxp_case_t *k,
                               const char *label)
{
    if (r->as.fail_hits == 0 && n->as.fail_hits == 0) {
        g_refusal_unexercised++;
        rxp_close(r, 0, label);
        rxp_close(n, 0, label);
        return;
    }
    rxp_close(r, 1, label);
    rxp_close(n, 1, label);
    size_t plen = 0;
    const uint8_t *props = rxp_props_bytes(k->version, &plen);
    int cr = rxp_residue(&r->as, r->known_props_id, props, plen);
    int cn = rxp_residue(&n->as, n->known_props_id, props, plen);
    if (r->as.fail_hits != 1 || n->as.fail_hits != 1 ||
        r->as.last_refused != n->as.last_refused) {
        rxp_note("refusal did not fire exactly once in both arms", label);
    } else if (cr == 0 && cn == 0) {
        g_refusal_clean++;
    } else if (cr == 1 && cn == 1) {
        g_refusal_known++;
        rxp_known_report(label);
    } else {
        rxp_note("blocks left after a refused allocation", label);
    }
    rxp_reclaim(&r->as);
    rxp_reclaim(&n->as);
}

static uint64_t rxp_run(const rxp_case_t *k, const rxp_piece_t *pcs, size_t npcs,
                        const char *label)
{
    uint64_t before = g_mismatches;
    static rxp_side_t r, n;
    memset(&r, 0, sizeof(r));
    memset(&n, 0, sizeof(n));
    bool ready = rxp_open(&r, k, true) && rxp_open(&n, k, false) &&
                 rxp_setup(&r, k) && rxp_setup(&n, k);
    if (!ready) {
        rxp_note("setup", label);
    } else {
        for (int j = 0; j < RXP_ROLES; j++)
            if (r.st[j].len != n.st[j].len ||
                memcmp(r.st[j].bytes, n.st[j].bytes, r.st[j].len) != 0)
                rxp_note("generated streams differ", label);
        if (k->fail_at) {
            r.as.fail_at = r.as.calls + k->fail_at;
            n.as.fail_at = n.as.calls + k->fail_at;
        }
        rxp_feed(&r, &n, k, pcs, npcs, label);
        r.as.fail_at = n.as.fail_at = 0;
        if (k->scenario == RXP_SC_HELD) {
            /* The pending subscription is answered on the shared alias: the
             * objects held for it surface now, through the oracle. */
            bool ar = rxp_accept(&r, 1, true, r.alias[0], false);
            bool an = rxp_accept(&n, 1, true, n.alias[0], false);
            if (!ar || !an) rxp_note("held accept", label);
        }
        rxp_drain(&r, &n, (size_t)-1, label);
        if (rxp_cmp_state(&r, &n)) rxp_note("final state", label);
        rxp_inv_expect(&r, &n, k, pcs, npcs, label);
    }
    if (k->fail_at) {
        rxp_refusal_settle(&r, &n, k, label);
    } else {
        rxp_close(&r, 0, label);
        rxp_close(&n, 0, label);
    }
    return g_mismatches - before;
}

/* -- Cut patterns ------------------------------------------------------ */

#define RXP_MAX_PIECES (2u * RXP_STREAM_CAP + 8u)
static rxp_piece_t g_pieces[RXP_MAX_PIECES];

static size_t rxp_cut_one(int role, size_t len, size_t cut, bool fin_alone)
{
    size_t n = 0;
    if (cut > 0 && cut < len) {
        g_pieces[n++] = (rxp_piece_t){ .role = role, .off = 0, .len = cut };
        g_pieces[n++] = (rxp_piece_t){ .role = role, .off = cut, .len = len - cut,
                                       .fin = !fin_alone };
    } else {
        g_pieces[n++] = (rxp_piece_t){ .role = role, .off = 0, .len = len,
                                       .fin = !fin_alone };
    }
    if (fin_alone) g_pieces[n++] = (rxp_piece_t){ .role = role, .off = len, .fin = true };
    return n;
}

static size_t rxp_bytewise(int role, size_t len)
{
    size_t n = 0;
    for (size_t i = 0; i < len; i++)
        g_pieces[n++] = (rxp_piece_t){ .role = role, .off = i, .len = 1,
                                       .fin = i + 1 == len };
    return n;
}

static size_t rxp_interleave(size_t len_a, size_t len_b, size_t cut)
{
    size_t ca = cut < len_a ? cut : len_a - 1;
    size_t cb = cut < len_b ? cut : len_b - 1;
    size_t n = 0;
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .off = 0, .len = ca };
    g_pieces[n++] = (rxp_piece_t){ .role = 1, .off = 0, .len = cb };
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .off = ca, .len = len_a - ca, .fin = true };
    g_pieces[n++] = (rxp_piece_t){ .role = 1, .off = cb, .len = len_b - cb, .fin = true };
    return n;
}

/* Role 0 up to `cut` without FIN, left owed when blocked, then its RESET. */
static size_t rxp_reset_at(size_t len, size_t cut, uint64_t code)
{
    size_t n = 0;
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .off = 0, .len = cut < len ? cut : len,
                                   .hold = true };
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .reset = true, .code = code };
    return n;
}

/* Role 0 in two pieces (the second with the FIN), then role 1 cut at `cut`
 * (1 <= cut < len_b): with one data-stream slot, role 1 must take the slot
 * role 0 released. Each stream's first piece leaves its entry observable. */
static size_t rxp_reuse(size_t len_a, size_t len_b, size_t cut)
{
    size_t n = 0, ha = len_a / 2;
    size_t cb = cut >= 1 && cut < len_b ? cut : 1;
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .off = 0, .len = ha };
    g_pieces[n++] = (rxp_piece_t){ .role = 0, .off = ha, .len = len_a - ha, .fin = true };
    g_pieces[n++] = (rxp_piece_t){ .role = 1, .off = 0, .len = cb };
    g_pieces[n++] = (rxp_piece_t){ .role = 1, .off = cb, .len = len_b - cb, .fin = true };
    return n;
}

/* Every cut (at `stride`), with the FIN on the data and alone, then bytewise. */
static void rxp_sweep_role(const rxp_case_t *k, int role, size_t len, size_t stride,
                           const char *tag)
{
    char label[160];
    for (size_t cut = 0; cut < len; cut += stride)
        for (int fa = 0; fa < 2; fa++) {
            snprintf(label, sizeof(label), "%s role %d cut %zu%s", tag, role, cut,
                     fa ? " fin alone" : "");
            rxp_run(k, g_pieces, rxp_cut_one(role, len, cut, fa != 0), label);
        }
    snprintf(label, sizeof(label), "%s role %d bytewise", tag, role);
    rxp_run(k, g_pieces, rxp_bytewise(role, len), label);
}

static bool rxp_lengths(const rxp_case_t *k, size_t len[RXP_ROLES])
{
    static rxp_side_t p;
    bool ok = rxp_open(&p, k, true) && rxp_setup(&p, k);
    for (int j = 0; j < RXP_ROLES; j++) len[j] = p.st[j].len;
    rxp_close(&p, 0, "lengths");
    return ok;
}

/* -- Controls ----------------------------------------------------------- */

/* The comparison must see one changed payload byte. */
static void rxp_control_detects(size_t len_a)
{
    rxp_case_t k = { .poll_cap = (size_t)-1, .flip = true, .flip_at = len_a / 2 };
    uint64_t saved = g_mismatches;
    unsigned saved_reported = g_reported;
    g_reported = RXP_REPORT_MAX;  /* expected mismatches: keep quiet */
    uint64_t seen = rxp_run(&k, g_pieces, rxp_cut_one(0, len_a, 0, false),
                            "control: changed byte");
    g_mismatches = saved;
    g_reported = saved_reported;
    MOQ_TEST_CHECK(seen > 0);
}

/* After a whole role-0 delivery without FIN, the reference's staging holds
 * the delivered bytes and the in-place side's reserved staging still holds
 * the allocator's poison: the arms took different staging paths. */
static void rxp_control_paths_differ(void)
{
    rxp_case_t k = { .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t r, n;
    bool ok = rxp_open(&r, &k, true) && rxp_open(&n, &k, false) &&
              rxp_setup(&r, &k) && rxp_setup(&n, &k);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        size_t len = r.st[0].len;
        MOQ_TEST_CHECK(rxp_call(&r, rxp_ref(0), r.st[0].bytes, len, false,
                                RXP_BYTES, RXP_NO_FLIP) == MOQ_OK);
        MOQ_TEST_CHECK(rxp_call(&n, rxp_ref(0), r.st[0].bytes, len, false,
                                RXP_BYTES, RXP_NO_FLIP) == MOQ_OK);
        MOQ_TEST_CHECK(rxp_cmp_state(&r, &n) == 0);
        const moq_rx_stream_t *x = rxp_entry(r.c, rxp_ref(0), NULL);
        const moq_rx_stream_t *y = rxp_entry(n.c, rxp_ref(0), NULL);
        MOQ_TEST_CHECK(x && y);
        if (x && y) {
            MOQ_TEST_CHECK_EQ_SIZE(x->input_len, 0);
            MOQ_TEST_CHECK(x->input_cap >= len && y->input_cap >= len);
            MOQ_TEST_CHECK(x->input_buf && memcmp(x->input_buf, r.st[0].bytes, len) == 0);
            bool untouched = y->input_buf != NULL;
            for (size_t i = 0; untouched && i < len; i++)
                untouched = y->input_buf[i] == RXP_POISON;
            MOQ_TEST_CHECK(untouched);
        }
        rxp_drain(&r, &n, (size_t)-1, "control: paths differ");
    }
    rxp_close(&r, 0, "control: paths differ");
    rxp_close(&n, 0, "control: paths differ");
}

/* The residue classifier on allocators it fills itself, each shape against
 * an expected allocation id. Nothing left is 0. The expected properties
 * rcbuf alone is 1. Each of these is -1:
 * - that rcbuf beside another block;
 * - an rcbuf of the same size holding other bytes;
 * - a plain block of the same size whose last bytes are the properties,
 *   expected id that of an earlier, released properties rcbuf;
 * - a second properties rcbuf, the same size and bytes, after the expected
 *   one was released (it may even reuse its memory);
 * - any block at all against id 0, which names no block. */
static void rxp_control_residue(void)
{
    size_t plen = 0, want = 0;
    const uint8_t *props = rxp_props_bytes(0, &plen);
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(plen, &want) == MOQ_OK);
    static const uint8_t other[] = { 0x7E, 0x7F };
    MOQ_TEST_CHECK(plen == sizeof(other) && memcmp(props, other, plen) != 0);
    for (int shape = 0; shape < 7; shape++) {
        rxp_alloc_t a;
        memset(&a, 0, sizeof(a));
        moq_alloc_t al = { &a, rxp_alloc_fn, rxp_realloc_fn, rxp_free_fn };
        moq_rcbuf_t *b = NULL, *gone = NULL;
        uint8_t *extra = NULL;
        size_t extra_size = 0, expect_id = 0;
        int expect = -1;
        switch (shape) {
        case 0:                                     /* nothing */
            expect = 0;
            break;
        case 1:                                     /* the expected block alone */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &b) == MOQ_OK);
            expect_id = rxp_block_id(&a, b);
            expect = 1;
            break;
        case 2:                                     /* it, beside another block */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &b) == MOQ_OK);
            expect_id = rxp_block_id(&a, b);
            extra_size = 1;
            extra = (uint8_t *)rxp_alloc_fn(extra_size, &a);
            break;
        case 3:                                     /* the right id, other bytes */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, other, sizeof(other), &b) == MOQ_OK);
            expect_id = rxp_block_id(&a, b);
            break;
        case 4:                                     /* same size and bytes, not an rcbuf */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &gone) == MOQ_OK);
            expect_id = rxp_block_id(&a, gone);
            moq_rcbuf_decref(gone);
            extra_size = want;
            extra = (uint8_t *)rxp_alloc_fn(extra_size, &a);
            if (extra) memcpy(extra + want - plen, props, plen);
            break;
        case 5:                                     /* another matching rcbuf */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &gone) == MOQ_OK);
            expect_id = rxp_block_id(&a, gone);
            moq_rcbuf_decref(gone);
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &b) == MOQ_OK);
            break;
        default:                                    /* a matching rcbuf against id 0 */
            MOQ_TEST_CHECK(moq_rcbuf_create(&al, props, plen, &b) == MOQ_OK);
            break;
        }
        if (shape >= 1 && shape <= 5) MOQ_TEST_CHECK(expect_id != 0);
        if (shape == 4) MOQ_TEST_CHECK(extra != NULL);
        MOQ_TEST_CHECK(rxp_residue(&a, expect_id, props, plen) == expect);
        if (b) moq_rcbuf_decref(b);
        if (extra) rxp_free_fn(extra, extra_size, &a);
        MOQ_TEST_CHECK(a.balance == 0 && a.live == 0 && !a.head && !a.size_errors);
    }
}

static moq_fill_t *rxp_event_fill(moq_event_t *e)
{
    switch (e->kind) {
    case MOQ_EVENT_FILL_OBJECT:   return &e->u.fill_object.fill;
    case MOQ_EVENT_FILL_GAP:      return &e->u.fill_gap.fill;
    case MOQ_EVENT_FILL_TERMINAL: return &e->u.fill_terminal.fill;
    default:                      return NULL;
    }
}

static uint64_t *rxp_event_request(moq_event_t *e)
{
    switch (e->kind) {
    case MOQ_EVENT_FILL_OBJECT:   return &e->u.fill_object.initiating_request_id;
    case MOQ_EVENT_FILL_GAP:      return &e->u.fill_gap.initiating_request_id;
    case MOQ_EVENT_FILL_TERMINAL: return &e->u.fill_terminal.initiating_request_id;
    default:                      return NULL;
    }
}

/* Fill events must carry the child FILL_CREATED announced. For every fill
 * event of a real fill stream, the comparator must reject: one arm's copy
 * naming a different, well-formed fill handle; both arms' copies each naming
 * a different handle of their own; both arms' copies naming the same wrong
 * initiating request; and both arms' copies naming no child at all (an
 * invalid fill handle on each side). The unchanged events must compare
 * equal. The changed copies are compared before the unchanged ones, so a
 * comparator that adopted the first handle it saw would accept them. */
static void rxp_control_fill_identity(void)
{
    rxp_case_t k = { .version = MOQ_VERSION_DRAFT_22, .scenario = RXP_SC_FILL,
                     .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t r, n;
    bool ok = rxp_open(&r, &k, true) && rxp_open(&n, &k, false) &&
              rxp_setup(&r, &k) && rxp_setup(&n, &k);
    MOQ_TEST_CHECK(ok && r.fill_known && n.fill_known);
    if (ok) {
        const rxp_stream_t *t = &r.st[2];
        MOQ_TEST_CHECK(rxp_call(&r, rxp_ref(2), t->bytes, t->len, true,
                                RXP_BYTES, RXP_NO_FLIP) == MOQ_OK);
        MOQ_TEST_CHECK(rxp_call(&n, rxp_ref(2), t->bytes, t->len, true,
                                RXP_BYTES, RXP_NO_FLIP) == MOQ_OK);
        size_t fills = 0;
        for (;;) {
            moq_event_t er, en;
            size_t cr = moq_session_poll_events(r.c, &er, 1);
            size_t cn = moq_session_poll_events(n.c, &en, 1);
            MOQ_TEST_CHECK(cr == cn);
            if (!cr || !cn) {
                if (cr) moq_event_cleanup(&er);
                if (cn) moq_event_cleanup(&en);
                break;
            }
            if (rxp_event_fill(&er) && rxp_event_fill(&en)) {
                fills++;
                moq_event_t a = en;
                moq_fill_t *fa = rxp_event_fill(&a);
                fa->_opaque ^= 1u;
                MOQ_TEST_CHECK(moq_fill_is_valid(*fa));
                MOQ_TEST_CHECK(rxp_cmp_event(&r, &er, &n, &a) != 0);
                moq_event_t b = er, c = en;
                rxp_event_fill(&b)->_opaque ^= 1u;
                rxp_event_fill(&c)->_opaque ^= 1u;
                MOQ_TEST_CHECK(rxp_cmp_event(&r, &b, &n, &c) != 0);
                b = er;
                c = en;
                *rxp_event_request(&b) += 1;
                *rxp_event_request(&c) += 1;
                MOQ_TEST_CHECK(rxp_cmp_event(&r, &b, &n, &c) != 0);
                b = er;
                c = en;
                *rxp_event_fill(&b) = MOQ_FILL_INVALID;
                *rxp_event_fill(&c) = MOQ_FILL_INVALID;
                MOQ_TEST_CHECK(!moq_fill_is_valid(*rxp_event_fill(&b)));
                MOQ_TEST_CHECK(rxp_cmp_event(&r, &b, &n, &c) != 0);
            }
            MOQ_TEST_CHECK(rxp_cmp_event(&r, &er, &n, &en) == 0);
            moq_event_cleanup(&er);
            moq_event_cleanup(&en);
        }
        MOQ_TEST_CHECK(fills >= 1);
        drain(r.sv);
        drain(n.sv);
    }
    rxp_close(&r, 0, "control: fill identity");
    rxp_close(&n, 0, "control: fill identity");
}

/* A handle that is structurally valid and names no subscription the
 * fixture holds: `h`'s slot, pool and session, with another live (odd)
 * generation. (The slot is the handle's low 16 bits; changing those would
 * name the subscription in the neighbouring slot.) */
static moq_subscription_t rxp_other_generation(moq_subscription_t h)
{
    uint64_t v = h._opaque;
    return (moq_subscription_t){ moq_handle_pack(moq_handle_pool_tag(v),
                                                 moq_handle_session_tag(v),
                                                 moq_handle_generation(v) ^ 2u,
                                                 moq_handle_slot(v)) };
}

/* UNSUBSCRIBED, on sides holding two subscriptions (draft-22, the second
 * still pending). The comparator accepts a matching pair for role 0, and
 * rejects: the other arm naming role 1; an invalid handle on both sides; an
 * unknown, well-formed handle on both sides; a changed cause; a changed
 * stream error code; and an output kind it does not model. The targeted
 * known answer accepts one correct event, and refuses, on each side by
 * itself, an event both arms agree on that names role 1 or carries another
 * stream error code, and a second UNSUBSCRIBED: agreement cannot hide a
 * shared error. */
static void rxp_control_unsubscribed(void)
{
    rxp_case_t k = { .version = MOQ_VERSION_DRAFT_22, .scenario = RXP_SC_HELD,
                     .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t r, n;
    bool ok = rxp_open(&r, &k, true) && rxp_open(&n, &k, false) &&
              rxp_setup(&r, &k) && rxp_setup(&n, &k);
    MOQ_TEST_CHECK(ok && moq_subscription_is_valid(r.sub[1]) &&
                   moq_subscription_is_valid(n.sub[1]));
    if (ok) {
        moq_event_t a, b, c, d;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_EVENT_UNSUBSCRIBED;
        a.detail_size = (uint32_t)sizeof(moq_unsubscribed_event_t);
        a.u.unsubscribed.sub = r.sub[0];
        a.u.unsubscribed.cause = MOQ_REQUEST_TERMINAL_MALFORMED_TRACK;
        a.u.unsubscribed.stream_error_code = 0;
        b = a;
        b.u.unsubscribed.sub = n.sub[0];
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &a, &n, &b) == 0);
        c = b;
        c.u.unsubscribed.sub = n.sub[1];
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &a, &n, &c) != 0);
        c = b;
        d = a;
        c.u.unsubscribed.sub = MOQ_SUBSCRIPTION_INVALID;
        d.u.unsubscribed.sub = MOQ_SUBSCRIPTION_INVALID;
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &d, &n, &c) != 0);
        c = b;
        d = a;
        c.u.unsubscribed.sub = rxp_other_generation(n.sub[0]);
        d.u.unsubscribed.sub = rxp_other_generation(r.sub[0]);
        MOQ_TEST_CHECK(moq_subscription_is_valid(c.u.unsubscribed.sub) &&
                       moq_subscription_is_valid(d.u.unsubscribed.sub));
        MOQ_TEST_CHECK(rxp_sub_role(&n, c.u.unsubscribed.sub) == -2 &&
                       rxp_sub_role(&r, d.u.unsubscribed.sub) == -2);
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &d, &n, &c) != 0);
        c = b;
        c.u.unsubscribed.cause = MOQ_REQUEST_TERMINAL_LOCAL_CANCEL;
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &a, &n, &c) != 0);
        c = b;
        c.u.unsubscribed.stream_error_code = 1;
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &a, &n, &c) != 0);
        c = b;
        d = a;
        c.kind = MOQ_EVENT_TRACK_STATUS_TERMINATED;
        d.kind = MOQ_EVENT_TRACK_STATUS_TERMINATED;
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &d, &n, &c) != 0);

        /* The known answer, side by side. */
        r.unsub_role0_malformed = r.unsub_other = 0;
        n.unsub_role0_malformed = n.unsub_other = 0;
        rxp_count_unsubscribed(&r, &a);
        rxp_count_unsubscribed(&n, &b);
        MOQ_TEST_CHECK(rxp_unsub_known_answer(&r) && rxp_unsub_known_answer(&n));
        rxp_count_unsubscribed(&r, &a);   /* a second one */
        MOQ_TEST_CHECK(!rxp_unsub_known_answer(&r));
        d = a;
        c = b;
        d.u.unsubscribed.sub = r.sub[1];
        c.u.unsubscribed.sub = n.sub[1];
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &d, &n, &c) == 0);   /* the arms agree */
        r.unsub_role0_malformed = r.unsub_other = 0;
        n.unsub_role0_malformed = n.unsub_other = 0;
        rxp_count_unsubscribed(&r, &d);
        rxp_count_unsubscribed(&n, &c);
        MOQ_TEST_CHECK(!rxp_unsub_known_answer(&r) && !rxp_unsub_known_answer(&n));
        d = a;
        c = b;
        d.u.unsubscribed.stream_error_code = 5;
        c.u.unsubscribed.stream_error_code = 5;
        MOQ_TEST_CHECK(rxp_cmp_event(&r, &d, &n, &c) == 0);   /* the arms agree */
        r.unsub_role0_malformed = r.unsub_other = 0;
        n.unsub_role0_malformed = n.unsub_other = 0;
        rxp_count_unsubscribed(&r, &d);
        rxp_count_unsubscribed(&n, &c);
        MOQ_TEST_CHECK(!rxp_unsub_known_answer(&r) && !rxp_unsub_known_answer(&n));
        drain(r.sv);
        drain(n.sv);
    }
    rxp_close(&r, 0, "control: unsubscribed");
    rxp_close(&n, 0, "control: unsubscribed");
}

/* An unowned refusal must be replayed with the same bytes and FIN. With one
 * data-stream slot, role 0's first half holds it, so role 1 (whole, with its
 * FIN) is refused without being retained; role 0's rest and FIN release the
 * slot and the parked delivery is replayed: one refusal parked, one replay,
 * both inventories exact, no mismatch. The same pieces settled with empty
 * retries, as the contract forbids, lose role 1, and the inventory check
 * must fail. Then the refused delivery is split: role 1's first byte is
 * refused, its tail and FIN queue behind it, and role 0's release lets the
 * head be admitted into the only slot; the tail belongs to a stream the
 * session now owns, so it is replayed without a free slot: one refusal
 * parked, two replays, both inventories exact, nothing left parked. */
static void rxp_control_unowned(size_t len_a, size_t len_b)
{
    rxp_case_t k = { .max_streams = 1, .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    size_t h = len_a / 2;
    const rxp_piece_t pcs[3] = {
        { .role = 0, .off = 0, .len = h },
        { .role = 1, .off = 0, .len = len_b, .fin = true },
        { .role = 0, .off = h, .len = len_a - h, .fin = true },
    };
    uint64_t parked = g_parked, replayed = g_replayed, inv = g_inventory_failures;
    uint64_t seen = rxp_run(&k, pcs, 3, "control: unowned refusal replayed");
    MOQ_TEST_CHECK(seen == 0);
    MOQ_TEST_CHECK(g_parked - parked == 1 && g_replayed - replayed == 1);
    MOQ_TEST_CHECK(g_inventory_failures == inv);

    k.empty_unowned = true;
    uint64_t saved = g_mismatches;
    unsigned saved_reported = g_reported;
    g_reported = RXP_REPORT_MAX;  /* expected mismatches: keep quiet */
    parked = g_parked;
    seen = rxp_run(&k, pcs, 3, "control: unowned refusal retried empty");
    uint64_t lost = g_inventory_failures - inv;
    g_mismatches = saved;
    g_reported = saved_reported;
    g_inventory_failures = inv;
    MOQ_TEST_CHECK(seen > 0 && lost > 0);
    MOQ_TEST_CHECK(g_parked == parked);

    k.empty_unowned = false;
    const rxp_piece_t split[4] = {
        { .role = 0, .off = 0, .len = h },
        { .role = 1, .off = 0, .len = 1 },
        { .role = 1, .off = 1, .len = len_b - 1, .fin = true },
        { .role = 0, .off = h, .len = len_a - h, .fin = true },
    };
    parked = g_parked;
    replayed = g_replayed;
    seen = rxp_run(&k, split, 4, "control: unowned head refused, tail queued");
    MOQ_TEST_CHECK(seen == 0);
    MOQ_TEST_CHECK(g_parked - parked == 1 && g_replayed - replayed == 2);
    MOQ_TEST_CHECK(g_inventory_failures == inv);
}

/* -- Known answers ------------------------------------------------------ */

/* Staging growth, clamp, charge and refusal in absolute numbers, per arm.
 * The first delivery is exactly 64 bytes, ending at an object boundary
 * (subgroup header plus object 0), so the stream keeps 64 bytes of capacity
 * with nothing staged. The second delivers 65 bytes holding the header of a
 * 75-byte object. Growth doubles 64 to 128 unless the budget clamps it; the
 * object is admitted only if 75 fits beside that capacity: budget 202
 * refuses, 203 admits, and a budget one above the first object's needs clamps
 * the growth to itself and refuses. */
static void rxp_known_capacity(void)
{
    static rxp_side_t p;
    static const bool none[] = { false, false };
    rxp_case_t probe = { .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    size_t pre = 0;
    {
        static const size_t sizes[] = { 40, 75 };
        bool ok = rxp_open(&p, &probe, true) && rxp_subscribe(&p, 0, NULL) &&
                  rxp_accept(&p, 0, false, 0, true) &&
                  rxp_gen_subgroup(&p, 0, 1, sizes, none, 2, &p.st[0]);
        ok = ok && p.st[0].units >= 3;
        if (ok) pre = p.st[0].unit_len[0] + p.st[0].unit_hdr[1];
        rxp_close(&p, 0, "capacity probe");
        MOQ_TEST_CHECK(ok && pre > 0 && pre < 64);
        if (!ok || pre == 0 || pre >= 64) return;
    }
    size_t p0 = 64 - pre;
    uint32_t clamp = (uint32_t)(64 + p0 + 1);
    MOQ_TEST_CHECK(clamp < 128);
    if (clamp >= 128) return;
    const uint32_t budgets[] = { 202, 203, clamp };
    for (int arm = 0; arm < 2; arm++)
    for (size_t b = 0; b < 3; b++) {
        char label[96];
        snprintf(label, sizeof(label), "capacity %s budget %u",
                 arm == 0 ? "reference" : "in place", (unsigned)budgets[b]);
        rxp_case_t k = { .budget = budgets[b], .poll_cap = (size_t)-1,
                         .flip_at = RXP_NO_FLIP };
        size_t sizes[2] = { p0, 75 };
        bool ok = rxp_open(&p, &k, arm == 0) && rxp_subscribe(&p, 0, NULL) &&
                  rxp_accept(&p, 0, false, 0, true) &&
                  rxp_gen_subgroup(&p, 0, 1, sizes, none, 2, &p.st[0]);
        if (!ok || p.st[0].units < 3 ||
            p.st[0].unit_len[0] + p.st[0].unit_len[1] != 64 || p.st[0].len < 64 + 65) {
            rxp_note("capacity setup", label);
            rxp_close(&p, 0, label);
            continue;
        }
        uint64_t ref = rxp_ref(0);
        if (rxp_call(&p, ref, p.st[0].bytes, 64, false, RXP_BYTES, RXP_NO_FLIP) != MOQ_OK)
            rxp_note("first delivery", label);
        drain(p.c);
        const moq_rx_stream_t *x = rxp_entry(p.c, ref, NULL);
        if (!x || x->input_len != 0 || x->input_cap != 64 ||
            p.c->recv_input_bytes != 64 || p.c->recv_payload_bytes != 0)
            rxp_note("64 bytes of retained capacity", label);
        moq_result_t rc = rxp_call(&p, ref, p.st[0].bytes + 64, 65, false,
                                   RXP_BYTES, RXP_NO_FLIP);
        size_t want_cap = budgets[b] == clamp ? clamp : 128;  /* clamp < 128, checked */
        if (rc != MOQ_OK) rxp_note("second delivery", label);
        if (p.as.last_realloc_old != 64 || p.as.last_realloc_new != want_cap)
            rxp_note("staging growth", label);
        bool admit = budgets[b] == 203;
        bool stopped = false, object = false;
        moq_action_t act;
        while (moq_session_poll_actions(p.c, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_STOP_DATA && act.u.stop_data.stream_ref._v == ref)
                stopped = true;
            moq_action_cleanup(&act);
        }
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) object = true;
            moq_event_cleanup(&ev);
        }
        if (stopped == admit || object) rxp_note("admission decision", label);
        x = rxp_entry(p.c, ref, NULL);
        if (admit) {
            if (!x || x->parse_state != MOQ_RX_READING_PAYLOAD || x->input_cap != 128 ||
                x->payload_written != 65 - p.st[0].unit_hdr[2] ||
                p.c->recv_input_bytes != 128 || p.c->recv_payload_bytes != 75)
                rxp_note("admitted charges", label);
        } else if (p.c->recv_input_bytes != 0 || p.c->recv_payload_bytes != 0) {
            rxp_note("refused stream kept charges", label);
        }
        rxp_close(&p, 0, label);
    }
}

/* A streaming chunk whose copy fails is retained and re-created by the next
 * empty drive: the failure must actually fire, and the retry must complete
 * every object and the subgroup terminal with the right bytes, in both arms. */
static void rxp_known_nomem_retry(void)
{
    rxp_case_t k = { .streaming = true, .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t r, n;
    size_t chunk_size = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(300, &chunk_size) == MOQ_OK);
    bool ok = rxp_open(&r, &k, true) && rxp_open(&n, &k, false) &&
              rxp_setup(&r, &k) && rxp_setup(&n, &k);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        r.as.fail_size = n.as.fail_size = chunk_size;
        const rxp_stream_t *t = &r.st[0];
        moq_result_t rr = rxp_call(&r, rxp_ref(0), t->bytes, t->len, true,
                                   RXP_BYTES, RXP_NO_FLIP);
        moq_result_t rn = rxp_call(&n, rxp_ref(0), t->bytes, t->len, true,
                                   RXP_BYTES, RXP_NO_FLIP);
        MOQ_TEST_CHECK(rr == MOQ_ERR_NOMEM && rn == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(r.as.fail_hits == 1 && n.as.fail_hits == 1);
        MOQ_TEST_CHECK(rxp_cmp_state(&r, &n) == 0);
        const moq_rx_stream_t *x = rxp_entry(n.c, rxp_ref(0), NULL);
        MOQ_TEST_CHECK(x && x->parse_state == MOQ_RX_PENDING_CHUNK &&
                       x->pending_chunk == NULL && x->pending_data_len == 300 &&
                       x->pending_from_input && x->input_len >= 300);
        rr = rxp_call(&r, rxp_ref(0), NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
        rn = rxp_call(&n, rxp_ref(0), NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
        MOQ_TEST_CHECK(rr == MOQ_OK && rn == MOQ_OK);
        /* In order: the four objects as begin+end chunks, then the FIN. */
        size_t objects = 0, finished = 0;
        for (;;) {
            moq_event_t er, en;
            size_t cr = moq_session_poll_events(r.c, &er, 1);
            size_t cn = moq_session_poll_events(n.c, &en, 1);
            MOQ_TEST_CHECK(cr == cn);
            if (!cr || !cn) {
                if (cr) moq_event_cleanup(&er);
                if (cn) moq_event_cleanup(&en);
                break;
            }
            MOQ_TEST_CHECK(rxp_cmp_event(&r, &er, &n, &en) == 0);
            if (er.kind == MOQ_EVENT_OBJECT_CHUNK) {
                const moq_object_chunk_event_t *ch = &er.u.object_chunk;
                uint64_t id = ch->object_id;
                MOQ_TEST_CHECK(id == objects && ch->begin && ch->end &&
                               ch->terminal == MOQ_OBJECT_TERMINAL_NORMAL);
                if (id < 4) {
                    uint8_t want[512];
                    rxp_payload(want, id, g_a_sizes[id]);
                    MOQ_TEST_CHECK(moq_rcbuf_len(ch->chunk) == g_a_sizes[id]);
                    MOQ_TEST_CHECK(g_a_sizes[id] == 0 ||
                                   memcmp(moq_rcbuf_data(ch->chunk), want,
                                          g_a_sizes[id]) == 0);
                }
                objects++;
            } else if (er.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                MOQ_TEST_CHECK(objects == 4);
                finished++;
            } else {
                MOQ_TEST_CHECK(false);
            }
            moq_event_cleanup(&er);
            moq_event_cleanup(&en);
        }
        MOQ_TEST_CHECK(objects == 4 && finished == 1);
        MOQ_TEST_CHECK(rxp_entry(r.c, rxp_ref(0), NULL) == NULL &&
                       rxp_entry(n.c, rxp_ref(0), NULL) == NULL);
        MOQ_TEST_CHECK(rxp_cmp_state(&r, &n) == 0);
    }
    rxp_close(&r, 0, "nomem retry");
    rxp_close(&n, 0, "nomem retry");
}

/* Whole-object mode: role 0's object 0 (properties, 5-byte payload) has its
 * payload allocation refused, exactly once in each arm, after its header and
 * properties were parsed. Both arms must agree throughout. Then one of two
 * outcomes is accepted:
 * - the parser stays at that object with nothing of it held, and an empty
 *   drive delivers the stream's whole inventory, leaving nothing behind;
 * - the known observation: the object's header consumed and its properties
 *   held. It is counted by name, with its two consequences for this stream,
 *   each required of each side on its own:
 *   - the held properties block: its allocation id is recorded at once, and
 *     after an empty drive and destroy each arm may hold nothing or exactly
 *     that block (by id), and nothing else;
 *   - the empty drive re-parses object 0's payload (01 08 0F ...) as an
 *     object header whose properties are malformed, so the Track is failed:
 *     the known answer of rxp_unsub_known_answer (one UNSUBSCRIBED for role
 *     0, malformed-track cause, stream error code 0, no other), and nothing
 *     of the stream delivered.
 *   Without the known observation neither consequence may appear: no
 *   UNSUBSCRIBED at all, and the whole inventory. */
static void rxp_known_payload_refusal(void)
{
    static const char *label = "payload refusal";
    rxp_case_t k = { .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t r, n;
    size_t pay = 0, plen = 0;
    const uint8_t *props = rxp_props_bytes(0, &plen);
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(g_a_sizes[0], &pay) == MOQ_OK);
    bool ok = g_a_props[0] && rxp_open(&r, &k, true) && rxp_open(&n, &k, false) &&
              rxp_setup(&r, &k) && rxp_setup(&n, &k);
    MOQ_TEST_CHECK(ok);
    if (!ok) {
        rxp_close(&r, 0, label);
        rxp_close(&n, 0, label);
        return;
    }
    const rxp_stream_t *t = &r.st[0];
    uint64_t ref = rxp_ref(0);
    r.as.fail_size = n.as.fail_size = pay;
    moq_result_t rr = rxp_call(&r, ref, t->bytes, t->len, true, RXP_BYTES, RXP_NO_FLIP);
    moq_result_t rn = rxp_call(&n, ref, t->bytes, t->len, true, RXP_BYTES, RXP_NO_FLIP);
    MOQ_TEST_CHECK(rr == MOQ_ERR_NOMEM && rn == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(r.as.fail_hits == 1 && n.as.fail_hits == 1);
    MOQ_TEST_CHECK(r.as.last_refused == pay && n.as.last_refused == pay);
    MOQ_TEST_CHECK(rxp_cmp_state(&r, &n) == 0);
    const moq_rx_stream_t *x = rxp_entry(n.c, ref, NULL);
    const moq_rx_stream_t *xr = rxp_entry(r.c, ref, NULL);
    MOQ_TEST_CHECK(x && x->parse_state == MOQ_RX_AWAITING_OBJECT && !x->payload_rcbuf);
    bool held = x && x->cur_extensions;
    size_t id_r = 0, id_n = 0;
    if (held) {
        MOQ_TEST_CHECK(x->has_prev_object && x->cur_object_id == 0);
        id_r = xr && xr->cur_extensions ? rxp_block_id(&r.as, xr->cur_extensions) : 0;
        id_n = rxp_block_id(&n.as, x->cur_extensions);
        MOQ_TEST_CHECK(id_r != 0 && id_n != 0);
    }
    rxp_drain(&r, &n, (size_t)-1, label);
    rr = rxp_call(&r, ref, NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
    rn = rxp_call(&n, ref, NULL, 0, false, RXP_BYTES, RXP_NO_FLIP);
    rxp_after_call(&r, &n, rr, rn, "advance return code", label);
    rxp_drain(&r, &n, (size_t)-1, label);
    if (held) {
        g_known_targeted++;
        rxp_known_report(label);
        rxp_close(&r, 1, label);
        rxp_close(&n, 1, label);
        int cr = rxp_residue(&r.as, id_r, props, plen);
        int cn = rxp_residue(&n.as, id_n, props, plen);
        if (cr < 0 || cn < 0 || cr != cn)
            rxp_note("residue beyond the known observation", label);
        rxp_reclaim(&r.as);
        rxp_reclaim(&n.as);
        MOQ_TEST_CHECK(rxp_unsub_known_answer(&r));
        MOQ_TEST_CHECK(rxp_unsub_known_answer(&n));
        MOQ_TEST_CHECK(rxp_inv_empty(&r.inv, 0) && rxp_inv_empty(&n.inv, 0));
    } else {
        MOQ_TEST_CHECK(rr == MOQ_OK && rn == MOQ_OK);
        MOQ_TEST_CHECK(r.unsub_role0_malformed + r.unsub_other == 0);
        MOQ_TEST_CHECK(n.unsub_role0_malformed + n.unsub_other == 0);
        MOQ_TEST_CHECK(rxp_inv_complete(&r.inv, 0) && rxp_inv_complete(&n.inv, 0));
        MOQ_TEST_CHECK(!rxp_entry(r.c, ref, NULL) && !rxp_entry(n.c, ref, NULL));
        rxp_close(&r, 0, label);
        rxp_close(&n, 0, label);
    }
}

/* An emitted object outlives its session: its payload and properties stay
 * intact and owned by the event, the allocator holds exactly those blocks,
 * and cleanup returns every byte, identically in both arms. */
static void rxp_known_lifetime(void)
{
    rxp_case_t k = { .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    static rxp_side_t sides[2];
    moq_event_t kept[2];
    bool have[2] = { false, false };
    for (int a = 0; a < 2; a++) {
        rxp_side_t *p = &sides[a];
        bool ok = rxp_open(p, &k, a == 0) && rxp_setup(p, &k);
        MOQ_TEST_CHECK(ok);
        if (!ok) {
            rxp_close(p, 0, "lifetime");
            continue;
        }
        MOQ_TEST_CHECK(rxp_call(p, rxp_ref(0), p->st[0].bytes, p->st[0].len, true,
                                RXP_BYTES, RXP_NO_FLIP) == MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(p->c, &ev, 1) > 0) {
            if (!have[a] && ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.object_id == 0) {
                kept[a] = ev;
                have[a] = true;
                continue;
            }
            moq_event_cleanup(&ev);
        }
        drain(p->sv);
        rxp_close(p, have[a] ? 2 : 0, "lifetime");
    }
    MOQ_TEST_CHECK(have[0] && have[1]);
    if (!have[0] || !have[1]) return;
    size_t want_live = 0, sz = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(g_a_sizes[0], &sz) == MOQ_OK);
    want_live += sz;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(
        moq_rcbuf_len(kept[0].u.object_received.properties), &sz) == MOQ_OK);
    want_live += sz;
    uint8_t want[8];
    rxp_payload(want, 0, g_a_sizes[0]);
    for (int a = 0; a < 2; a++) {
        const moq_object_received_event_t *o = &kept[a].u.object_received;
        MOQ_TEST_CHECK(o->payload && moq_rcbuf_refcount(o->payload) == 1 &&
                       moq_rcbuf_len(o->payload) == g_a_sizes[0] &&
                       memcmp(moq_rcbuf_data(o->payload), want, g_a_sizes[0]) == 0);
        MOQ_TEST_CHECK(o->properties && moq_rcbuf_refcount(o->properties) == 1);
        MOQ_TEST_CHECK(sides[a].as.balance == 2);
        MOQ_TEST_CHECK(sides[a].as.live == (int64_t)want_live);
        moq_event_cleanup(&kept[a]);
        MOQ_TEST_CHECK(sides[a].as.balance == 0 && sides[a].as.live == 0);
    }
}

/* -- Main -------------------------------------------------------------- */

/* With --known-alloc-failure only the controls, the known answers and the
 * allocator-refusal cases run, and any instance of the known observation
 * fails the run. */
int main(int argc, char **argv)
{
    bool known_mode = argc == 2 && strcmp(argv[1], "--known-alloc-failure") == 0;
    if (argc > 1 && !known_mode) {
        fprintf(stderr, "usage: %s [--known-alloc-failure]\n", argv[0]);
        return 2;
    }
    bool full = !known_mode;
    const char *name = known_mode ? "session_rx_in_place_known_alloc_failure"
                                  : "session_rx_in_place";
    g_plain = test_allocator(&g_plain_state);

    rxp_case_t base = { .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP };
    size_t len[RXP_ROLES];
    MOQ_TEST_CHECK(rxp_lengths(&base, len));
    MOQ_TEST_CHECK(len[0] > 2 && len[1] > 2);
    if (failures) return 1;

    rxp_control_detects(len[0]);
    rxp_control_paths_differ();
    rxp_control_residue();
    rxp_control_fill_identity();
    rxp_control_unsubscribed();
    rxp_control_unowned(len[0], len[1]);
    rxp_known_capacity();
    rxp_known_nomem_retry();
    rxp_known_payload_refusal();
    rxp_known_lifetime();
    if (failures || g_mismatches) {
        fprintf(stderr, "controls failed: %llu mismatches\n",
                (unsigned long long)g_mismatches);
        return 1;
    }

    static const struct { uint32_t max_events; size_t poll_cap; } pressure[] = {
        { 0, (size_t)-1 },  /* default queue, drained after every delivery */
        { 2, 1 },           /* two-slot queue, one event polled per step */
    };
    char tag[96];
    char label[160];

    /* Draft-16 subgroup streams: both streams, both modes and routes, three
     * budgets, two queue pressures; every cut (stride 4 off the default
     * budget), separate FIN, bytewise, allocator refusals, and both streams
     * interleaved under one budget. */
    static const uint32_t budgets[] = { 0, 200, 420 };
    for (int streaming = 0; streaming < 2; streaming++)
    for (int route = 0; route < 2; route++)
    for (size_t b = 0; b < sizeof(budgets) / sizeof(budgets[0]); b++)
    for (size_t q = 0; q < 2; q++) {
        rxp_case_t k = { .streaming = streaming != 0, .route = route,
                         .budget = budgets[b], .max_events = pressure[q].max_events,
                         .poll_cap = pressure[q].poll_cap, .flip_at = RXP_NO_FLIP };
        snprintf(tag, sizeof(tag), "d16 s%d r%d b%u q%zu", streaming, route,
                 (unsigned)budgets[b], q);
        if (full) {
            size_t stride = budgets[b] == 0 ? 1 : 4;
            rxp_sweep_role(&k, 0, len[0], stride, tag);
            rxp_sweep_role(&k, 1, len[1], stride, tag);
            for (size_t cut = 1; cut < len[0]; cut += 7) {
                snprintf(label, sizeof(label), "%s interleave %zu", tag, cut);
                rxp_run(&k, g_pieces, rxp_interleave(len[0], len[1], cut), label);
            }
        }
        for (uint64_t fail = 1; fail <= 12; fail++)
        for (int role = 0; role < 2; role++) {
            static const size_t cuts[] = { 0, 3, 40 };
            for (size_t c = 0; c < sizeof(cuts) / sizeof(cuts[0]); c++) {
                rxp_case_t kf = k;
                kf.fail_at = fail;
                snprintf(label, sizeof(label), "%s fail %llu role %d cut %zu", tag,
                         (unsigned long long)fail, role, cuts[c]);
                rxp_run(&kf, g_pieces, rxp_cut_one(role, len[role], cuts[c], false), label);
            }
        }
    }

    /* Draft-18 and draft-22 subgroup streams. */
    static const moq_version_t later[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; full && v < 2; v++)
    for (int streaming = 0; streaming < 2; streaming++)
    for (size_t q = 0; q < 2; q++) {
        rxp_case_t k = { .version = later[v], .streaming = streaming != 0,
                         .max_events = pressure[q].max_events,
                         .poll_cap = pressure[q].poll_cap, .flip_at = RXP_NO_FLIP };
        size_t vl[RXP_ROLES];
        snprintf(tag, sizeof(tag), "v%u s%d q%zu", (unsigned)later[v], streaming, q);
        if (!rxp_lengths(&k, vl)) {
            rxp_note("setup", tag);
            continue;
        }
        rxp_sweep_role(&k, 0, vl[0], 2, tag);
        rxp_sweep_role(&k, 1, vl[1], 2, tag);
    }

    /* Fetch (draft-16) and fill (draft-22) streams. */
    for (int which = 0; which < 2; which++)
    for (int streaming = 0; streaming < 2; streaming++)
    for (int route = 0; route < 2; route++)
    for (size_t q = 0; q < 2; q++) {
        rxp_case_t k = { .version = which ? MOQ_VERSION_DRAFT_22 : 0,
                         .scenario = which ? RXP_SC_FILL : RXP_SC_FETCH,
                         .streaming = streaming != 0, .route = route,
                         .max_events = pressure[q].max_events,
                         .poll_cap = pressure[q].poll_cap, .flip_at = RXP_NO_FLIP };
        size_t fl[RXP_ROLES];
        snprintf(tag, sizeof(tag), "%s s%d r%d q%zu", which ? "fill" : "fetch",
                 streaming, route, q);
        if (!rxp_lengths(&k, fl)) {
            rxp_note("setup", tag);
            continue;
        }
        if (full) rxp_sweep_role(&k, 2, fl[2], 2, tag);
        for (uint64_t fail = 1; fail <= 8; fail++) {
            rxp_case_t kf = k;
            kf.fail_at = fail;
            snprintf(label, sizeof(label), "%s fail %llu", tag, (unsigned long long)fail);
            rxp_run(&kf, g_pieces, rxp_cut_one(2, fl[2], 0, false), label);
        }
    }

    /* Draft-22 shared alias and held output; draft-16 Forward 0. The held row
     * holds output for a pending subscription on an alias already known; it
     * does not reach a deferred unknown alias or a deferred STOP. */
    static const int scenarios[] = { RXP_SC_SHARED, RXP_SC_HELD, RXP_SC_SUPPRESS };
    for (size_t s = 0; full && s < 3; s++)
    for (int streaming = 0; streaming < 2; streaming++)
    for (size_t q = 0; q < 2; q++) {
        rxp_case_t k = { .version = scenarios[s] == RXP_SC_SUPPRESS ? 0 : MOQ_VERSION_DRAFT_22,
                         .scenario = scenarios[s], .streaming = streaming != 0,
                         .max_events = pressure[q].max_events,
                         .poll_cap = pressure[q].poll_cap, .flip_at = RXP_NO_FLIP };
        size_t sl[RXP_ROLES];
        snprintf(tag, sizeof(tag), "scenario %d s%d q%zu", scenarios[s], streaming, q);
        if (!rxp_lengths(&k, sl)) {
            rxp_note("setup", tag);
            continue;
        }
        rxp_sweep_role(&k, 0, sl[0], 5, tag);
    }

    /* RESET while output is parked (one-slot queue, nothing polled) and with
     * room; cut anywhere, whole-object and streaming. */
    for (int streaming = 0; full && streaming < 2; streaming++)
    for (int parked = 0; parked < 2; parked++) {
        rxp_case_t k = { .streaming = streaming != 0,
                         .max_events = parked ? 1u : 0u,
                         .poll_cap = parked ? 0u : (size_t)-1, .flip_at = RXP_NO_FLIP };
        for (size_t cut = 1; cut <= len[0]; cut += 5) {
            snprintf(label, sizeof(label), "reset s%d parked%d cut %zu", streaming,
                     parked, cut);
            rxp_run(&k, g_pieces, rxp_reset_at(len[0], cut, 9), label);
        }
    }

    /* A released slot taken by the next stream. */
    for (int streaming = 0; full && streaming < 2; streaming++) {
        rxp_case_t k = { .streaming = streaming != 0, .max_streams = 1,
                         .poll_cap = (size_t)-1, .flip_at = RXP_NO_FLIP,
                         .slot_reuse = true };
        for (size_t cut = 1; cut < len[1]; cut += 5) {
            snprintf(label, sizeof(label), "reuse s%d cut %zu", streaming, cut);
            rxp_run(&k, g_pieces, rxp_reuse(len[0], len[1], cut), label);
        }
    }

    fprintf(stderr, "refusals: %llu fired and left nothing, %llu known observation, "
                    "%llu never reached; unowned refusals parked %llu, replayed %llu\n",
            (unsigned long long)g_refusal_clean, (unsigned long long)g_refusal_known,
            (unsigned long long)g_refusal_unexercised, (unsigned long long)g_parked,
            (unsigned long long)g_replayed);
    /* The refusal sweeps must actually refuse something. */
    MOQ_TEST_CHECK(g_refusal_clean + g_refusal_known > 0);
    uint64_t known = g_refusal_known + g_known_targeted;
    if (known) {
        fprintf(stderr, "KNOWN FAILING: %llu cases (%llu targeted) of a refused whole-object "
                        "payload allocation leaving the object's properties block\n",
                (unsigned long long)known, (unsigned long long)g_known_targeted);
        if (known_mode) failures++;
    }
    if (g_mismatches) {
        fprintf(stderr, "%llu mismatches\n", (unsigned long long)g_mismatches);
        failures++;
    }
    MOQ_TEST_CHECK(g_plain_state.balance == 0);
    MOQ_TEST_PASS(name);
    return failures ? 1 : 0;
}
