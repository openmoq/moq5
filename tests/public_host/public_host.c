/*
 * A transport host built on the public headers alone. See public_host.h.
 *
 * The file has three parts: the transport double (ph_wire_*), one side's
 * host loop (host_*), and the pair that joins two sides (ph_*).
 */
#include "public_host.h"

#include <stdlib.h>
#include <string.h>

/* Session termination codes the drafts share (draft-16 section 3.4,
 * draft-18 section 3.5, draft-22 section 12.2). */
#define PH_CLOSE_INTERNAL_ERROR      UINT64_C(0x1)
#define PH_CLOSE_PROTOCOL_VIOLATION  UINT64_C(0x3)

/* Refs this host assigns to peer-opened streams. This is a convention of
 * this fixture, not a rule the library publishes: the session's own refs
 * are observed to start at 1 and count up, and the high bit is assumed to
 * keep the two apart. No public header states how the two namespaces stay
 * disjoint, what happens when either runs out, or when a ref may be used
 * again. */
#define PH_PEER_REF_BASE (UINT64_C(1) << 63)

/* What each draft's profile does that this host must follow. Keyed by the
 * version the session reports speaking, never inferred from a result. */
typedef struct ph_profile {
    moq_version_t version;
    bool          bidi_control;     /* one bidirectional control stream */
    bool          request_streams;  /* each request on its own bidi */
} ph_profile_t;

static const ph_profile_t ph_profiles[] = {
    { MOQ_VERSION_DRAFT_16, true,  false },
    { MOQ_VERSION_DRAFT_18, false, true  },
    { MOQ_VERSION_DRAFT_22, false, true  },
};

static const ph_profile_t *ph_profile_for(moq_version_t version)
{
    for (size_t i = 0; i < sizeof(ph_profiles) / sizeof(ph_profiles[0]); i++)
        if (ph_profiles[i].version == version) return &ph_profiles[i];
    return NULL;
}

/* -- The transport double -------------------------------------------- */

typedef struct ph_pipe {            /* one direction of one stream */
    uint8_t  buf[PH_PIPE_CAP];
    size_t   len;                   /* written, not yet read */
    uint64_t read_off;              /* bytes the reader has taken so far */
    bool     fin;                   /* the writer finished */
    bool     fin_held;              /* ... but the transport holds it back */
    bool     fin_read;              /* the reader has seen that FIN */
    bool     reset;                 /* the writer reset */
    bool     reset_read;
    uint64_t reset_code;
    bool     stop;                  /* the reader told the writer to stop */
    bool     stop_read;             /* the writer has seen that stop */
    uint64_t stop_code;
    bool     dropped;               /* the reader's transport dropped this
                                     * direction after its stop */
    bool     retire_read;           /* the reader has seen that report */
    ph_stream_trace_t trace;        /* what the transport took of the writer */
} ph_pipe_t;

typedef struct ph_stream {
    bool      used;
    bool      bidi;
    int       opener;
    ph_pipe_t dir[2];               /* dir[x]: what side x writes */
} ph_stream_t;

typedef struct ph_dgram {
    uint8_t bytes[PH_DGRAM_CAP];
    size_t  len;
} ph_dgram_t;

typedef struct ph_wire {
    ph_stream_t streams[PH_MAX_STREAMS];
    size_t      write_limit;
    size_t      read_limit;
    bool        drop_after_stop;
    bool        fin_hold[2];        /* FINs side x writes are held back */
    unsigned    refuse[2];          /* PH_REFUSE_* bits for what side x writes */
    bool        closed[2];          /* side x closed the transport */
    unsigned    resets[2];          /* resets side x's halves took */
    unsigned    reset_repeats[2];   /* resets asked of an already reset half */
    uint64_t    last_reset_code[2];
    uint64_t    close_code[2];
    ph_dgram_t  dgram[2][PH_DGRAM_SLOTS];   /* dgram[x]: sent by side x */
    size_t      dgram_count[2];
    uint64_t    step;               /* the last transport step taken */
} ph_wire_t;

static int wire_open(ph_wire_t *w, int opener, bool bidi)
{
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        if (w->streams[i].used) continue;
        memset(&w->streams[i], 0, sizeof(w->streams[i]));
        w->streams[i].used = true;
        w->streams[i].bidi = bidi;
        w->streams[i].opener = opener;
        return i;
    }
    return -1;
}

/* A write of `took` bytes the transport took, as the writer's trace. */
static void wire_trace_write(ph_wire_t *w, ph_pipe_t *p, size_t took)
{
    if (!took) return;
    p->trace.last_write = ++w->step;
    if (!p->trace.first_write) p->trace.first_write = p->trace.last_write;
    p->trace.writes++;
    p->trace.bytes += took;
}

/* How many of len bytes the transport takes now. */
static size_t wire_write(ph_wire_t *w, int index, int side,
                         const uint8_t *data, size_t len)
{
    ph_pipe_t *p = &w->streams[index].dir[side];
    if (p->fin || p->reset) return 0;
    if (w->refuse[side] & PH_REFUSE_STREAM_BYTES) return 0;
    size_t take = len;
    if (w->write_limit && take > w->write_limit) take = w->write_limit;
    if (p->dropped) {
        wire_trace_write(w, p, take);
        return take;                        /* accepted, never delivered */
    }
    size_t room = PH_PIPE_CAP - p->len;
    if (take > room) take = room;
    if (take) memcpy(p->buf + p->len, data, take);
    p->len += take;
    wire_trace_write(w, p, take);
    return take;
}

static void wire_fin(ph_wire_t *w, int index, int side)
{
    ph_pipe_t *p = &w->streams[index].dir[side];
    if (p->reset) return;
    if (!p->trace.fin) p->trace.fin = ++w->step;
    if (w->fin_hold[side]) p->fin_held = true;
    else p->fin = true;
}

/* A reset abandons what the reader has not read yet, except the leading
 * `reliable` bytes of the stream: those are still delivered, and the reset
 * follows them. */
static void wire_reset(ph_wire_t *w, int index, int side, uint64_t code,
                       uint64_t reliable)
{
    ph_pipe_t *p = &w->streams[index].dir[side];
    if (p->fin && p->len == 0 && p->fin_read) return;   /* already ended */
    if (p->reset) { w->reset_repeats[side]++; return; }
    w->resets[side]++;
    w->last_reset_code[side] = code;
    p->trace.reset = ++w->step;
    p->trace.reset_code = code;
    p->trace.reset_reliable = reliable;
    p->reset = true;
    p->reset_code = code;
    p->fin = false;
    p->fin_held = false;
    uint64_t keep = reliable > p->read_off ? reliable - p->read_off : 0;
    if (keep < p->len) p->len = (size_t)keep;
}

/* The reader of a direction tells its writer to stop. */
static void wire_stop(ph_wire_t *w, int index, int reader, uint64_t code)
{
    ph_pipe_t *p = &w->streams[index].dir[1 - reader];
    if (p->stop) return;
    p->stop = true;
    p->stop_code = code;
    if (w->drop_after_stop) {
        p->dropped = true;
        p->len = 0;
    }
}

static size_t wire_read(ph_wire_t *w, int index, int reader,
                        uint8_t *out, size_t cap)
{
    ph_pipe_t *p = &w->streams[index].dir[1 - reader];
    if (p->dropped) return 0;
    size_t take = p->len < cap ? p->len : cap;
    if (w->read_limit && take > w->read_limit) take = w->read_limit;
    if (!take) return 0;
    memcpy(out, p->buf, take);
    memmove(p->buf, p->buf + take, p->len - take);
    p->len -= take;
    p->read_off += take;
    return take;
}

/* -- One side -------------------------------------------------------- */

typedef enum ph_role {
    PH_ROLE_UNROUTED = 0,       /* a peer unidirectional stream, type unknown */
    PH_ROLE_CONTROL_BIDI,       /* the one bidirectional control stream */
    PH_ROLE_CONTROL_OUT,        /* this side's unidirectional control stream */
    PH_ROLE_CONTROL_IN,         /* the peer's unidirectional control stream */
    PH_ROLE_DATA_OUT,
    PH_ROLE_DATA_IN,
    PH_ROLE_REQUEST,
    PH_ROLE_PADDING
} ph_role_t;

typedef struct ph_local {       /* a transport stream as this side knows it */
    bool             used;
    int              wire;
    moq_stream_ref_t ref;
    ph_role_t        role;
    bool             peer_opened;

    /* Input. hold is what this side read from the transport and the
     * session has not taken: a stream type still incomplete, or a delivery
     * the session did not admit, kept byte for byte with its FIN. */
    uint8_t          hold[PH_HOLD_CAP];
    size_t           hold_len;
    bool             hold_fin;
    bool             refused;       /* counted in the side's refused streams */
    bool             owned_retry;   /* the session kept a delivery; retry empty */
    bool             owned_fin;     /* ... and that delivery carried the FIN */
    bool             in_done;       /* nothing more will be fed for this stream */
    bool             stop_sent;     /* the transport took this side's stop */

    /* Output. */
    bool             out_done;      /* this side's FIN or reset was taken */
    bool             drop_output;   /* the peer stopped this side: write no more */
    bool             peer_stop_done;/* ... and the session knows */
    /* A request stream the peer stopped, not answered yet: this side resets
     * its half once, with the session's own reset or abort of the stream
     * when one is polled or held, otherwise with the STOP's code once a
     * poll shows the session has nothing more to hand over. */
    bool             stop_debt;
    uint64_t         stop_code;
    /* The session reset or aborted this request stream: the bytes and FIN
     * still held for it are obsolete and are not written. */
    bool             torn;
    bool             committed;     /* "all data committed" was reported */

    /* Tagged mode. */
    bool             open_pending;  /* a counted open to report */
    moq_output_owner_t open_owner;
    uint64_t         written;       /* bytes of completed records the
                                     * transport took */
    uint64_t         owner;         /* the owner of its FIRST_OF_STREAM action */
    uint64_t         prefix;        /* that action's header: the bytes every
                                     * reset of the stream still delivers */
    bool             end_known;     /* an expiry, a STOP, a reset or a claim
                                     * decided how the stream ends; expired
                                     * held bytes are then trimmed, not
                                     * reported */
    bool             reset_owed;    /* this side writes the stream's reset */
    bool             reset_answer;  /* ... answering a NO_RESET outcome */
    uint64_t         reset_code;
    uint64_t         reset_reliable;
    /* A FETCH response stream tagged RESET_FINAL_ON_STOP, and what a STOP
     * of it, observed while it was still open, made of its owner's
     * request: the request's final message becomes a CANCELLED reset of
     * its stream (once), unless the session resets the request itself. */
    bool             reset_final_on_stop;
    bool             final_cancel;  /* that STOP was observed */
    bool             final_native;  /* ... with REQUEST_ENDED: the session's reset */
    bool             final_replaced;/* ... and the final message was replaced */
} ph_local_t;

typedef enum ph_out_kind {
    PH_OUT_WRITE = 1,           /* bytes, then a FIN when fin is set */
    PH_OUT_RESET,
    PH_OUT_STOP,
    PH_OUT_ABORT,               /* reset this side's half and stop the peer's */
    PH_OUT_CLOSE,               /* close the transport */
    PH_OUT_DATAGRAM             /* held apart, in the side's datagram list */
} ph_out_kind_t;

typedef struct ph_out {         /* one output this side owns until written */
    ph_out_kind_t kind;
    int           local;        /* stream, or -1 */
    uint8_t      *bytes;
    size_t        len;
    size_t        off;          /* bytes the transport has taken */
    bool          fin;
    uint64_t      code;
    uint64_t      reliable;     /* PH_OUT_RESET: leading bytes still delivered */
    /* The tag of the action it came from (tagged mode; otherwise no
     * deadline, owner 0 and final_for 0). */
    uint64_t      deadline;
    uint64_t      owner;
    uint64_t      final_for;
} ph_out_t;

typedef struct ph_host {
    struct ph_pair *pair;
    int             side;
    moq_session_t  *s;

    ph_local_t      local[PH_MAX_STREAMS];
    int             control_bidi;   /* index into local, or -1 */
    int             control_out;
    int             control_in;
    uint64_t        next_peer_ref;
    unsigned        refused_streams;

    ph_out_t        out[PH_OUT_SLOTS];
    size_t          out_head;
    size_t          out_count;
    bool            per_stream;     /* keep each stream's output order alone */
    bool            input_held;     /* ph_set_input_hold(): read nothing */

    /* The one local stream whose bytes and FIN the transport refuses
     * (index into local, or -1), or the next data stream this side opens,
     * past the first refuse_allow bytes it takes of that stream. */
    int             refuse_local;
    bool            refuse_next;
    uint64_t        refuse_allow;
    bool            polled_empty;   /* this pass's poll returned no action */
    unsigned        poll_budget;    /* most actions polled per pass; 0 = no limit */

    bool            control_pending;    /* the session kept control input */
    bool            wire_closed;        /* this side closed the transport */
    bool            peer_close_told;    /* the session knows the peer closed */
    int             fault;
    ph_counters_t   n;

    /* Tagged mode: the one claim this host holds, as the session offered
     * it, with its stream and the bytes its reset still delivers, and the
     * transport resets it refuses next. */
    bool            tagged;
    bool            claim_active;
    moq_data_reset_offer_t claim;
    int             claim_local;
    uint64_t        claim_reliable;
    unsigned        refuse_resets;

    /* Datagrams, held apart from the ordered output, oldest first. */
    ph_out_t        dg[PH_DGRAM_SLOTS];
    size_t          dg_count;

    /* The tag of the action being taken, stamped on the records it makes. */
    uint64_t        cur_deadline;
    uint64_t        cur_owner;
    uint64_t        cur_final_for;

    /* Wakes. */
    bool            ticked;         /* this wake's tick has run */
    bool            tick_blocked;   /* the last tick returned WOULD_BLOCK ... */
    uint64_t        blocked_at;     /* ... at this time ... */
    uint64_t        blocked_due;    /* ... leaving the deadline here */
} ph_host_t;

typedef struct ph_balance {
    size_t blocks;
} ph_balance_t;

struct ph_pair {
    ph_wire_t     wire;
    ph_host_t     host[2];
    uint64_t      now_us;
    const ph_profile_t *profile;
    ph_balance_t  native;           /* the sessions' allocations */
    size_t        copies;           /* output copies this host holds */
    moq_alloc_t   alloc;
};

static void *ph_alloc(size_t n, void *ctx)
{
    ph_balance_t *b = (ph_balance_t *)ctx;
    void *p = malloc(n ? n : 1);
    if (p) b->blocks++;
    return p;
}

static void ph_free(void *p, size_t n, void *ctx)
{
    ph_balance_t *b = (ph_balance_t *)ctx;
    (void)n;
    if (!p) return;
    b->blocks--;
    free(p);
}

static void *ph_realloc(void *p, size_t old, size_t n, void *ctx)
{
    if (!p) return ph_alloc(n, ctx);
    if (!n) { ph_free(p, old, ctx); return NULL; }
    return realloc(p, n);
}

static ph_wire_t *host_wire(ph_host_t *h) { return &h->pair->wire; }
static uint64_t host_now(const ph_host_t *h) { return h->pair->now_us; }

static void host_fail(ph_host_t *h, int fault)
{
    if (!h->fault) h->fault = fault;
}

/* End the session for a reason this host found: a protocol violation by
 * the peer, or a limit of its own. The session queues the close action,
 * which closes the transport when it is written. */
static void host_close(ph_host_t *h, uint64_t code, const char *reason)
{
    if (code == PH_CLOSE_PROTOCOL_VIOLATION) h->n.violations++;
    (void)moq_session_close(h->s, code, reason, host_now(h));
}

/* An input came back with a result that is neither success nor a capacity
 * refusal. When the session closed itself over it (a protocol error of the
 * peer's), its close action is queued and this side adds nothing. Otherwise
 * the result does not say whether the input was kept, so this side can
 * neither redeliver nor retry it: it ends the session for its own limit. */
static void host_input_failed(ph_host_t *h, const char *reason)
{
    if (moq_session_state(h->s) == MOQ_SESS_CLOSED) return;
    host_fail(h, PH_FAULT_INPUT);
    host_close(h, PH_CLOSE_INTERNAL_ERROR, reason);
}

static int host_alloc_local(ph_host_t *h, int wire, bool peer_opened,
                            ph_role_t role, moq_stream_ref_t ref)
{
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        if (h->local[i].used) continue;
        memset(&h->local[i], 0, sizeof(h->local[i]));
        h->local[i].used = true;
        h->local[i].wire = wire;
        h->local[i].peer_opened = peer_opened;
        h->local[i].role = role;
        h->local[i].ref = ref;
        return i;
    }
    return -1;
}

static int host_find_wire(const ph_host_t *h, int wire)
{
    for (int i = 0; i < PH_MAX_STREAMS; i++)
        if (h->local[i].used && h->local[i].wire == wire) return i;
    return -1;
}

static int host_find_ref(const ph_host_t *h, moq_stream_ref_t ref,
                         ph_role_t a, ph_role_t b)
{
    for (int i = 0; i < PH_MAX_STREAMS; i++)
        if (h->local[i].used && h->local[i].ref._v == ref._v &&
            (h->local[i].role == a || h->local[i].role == b))
            return i;
    return -1;
}

/* True when the caller's record covers a field, by the record's stamp. */
static bool action_covers(const moq_action_t *a, size_t end)
{
    return (size_t)a->detail_size >= end;
}

/* How much of an action's detail this host reads, by kind: through the last
 * field it uses. SIZE_MAX for a kind it does not know. */
static size_t action_detail_needed(moq_action_kind_t kind)
{
#define PH_THROUGH(type, field) \
    (offsetof(type, field) + sizeof(((type *)0)->field))
    switch (kind) {
    case MOQ_ACTION_SEND_CONTROL:
        return PH_THROUGH(moq_send_control_action_t, len);
    case MOQ_ACTION_CLOSE_SESSION:
        return PH_THROUGH(moq_close_session_action_t, code);
    case MOQ_ACTION_SEND_DATA:
        return PH_THROUGH(moq_send_data_action_t, fin);
    case MOQ_ACTION_RESET_DATA:
        return PH_THROUGH(moq_reset_data_action_t, error_code);
    case MOQ_ACTION_STOP_DATA:
        return PH_THROUGH(moq_stop_data_action_t, error_code);
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        return PH_THROUGH(moq_open_bidi_stream_action_t, fin);
    case MOQ_ACTION_SEND_BIDI_STREAM:
        return PH_THROUGH(moq_send_bidi_stream_action_t, fin);
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return PH_THROUGH(moq_close_bidi_stream_action_t, stream_ref);
    case MOQ_ACTION_SEND_DATAGRAM:
        return PH_THROUGH(moq_send_datagram_action_t, len);
    case MOQ_ACTION_OPEN_UNI_CONTROL:
        return PH_THROUGH(moq_open_uni_control_action_t, len);
    case MOQ_ACTION_SEND_UNI_CONTROL:
        return PH_THROUGH(moq_send_uni_control_action_t, fin);
    case MOQ_ACTION_RESET_BIDI_STREAM:
        return PH_THROUGH(moq_reset_bidi_stream_action_t, error_code);
    case MOQ_ACTION_STOP_BIDI_STREAM:
        return PH_THROUGH(moq_stop_bidi_stream_action_t, error_code);
    case MOQ_ACTION_ABORT_BIDI_STREAM:
        return PH_THROUGH(moq_abort_bidi_stream_action_t, error_code);
    case MOQ_ACTION_SET_DATA_PRIORITY:
        return 0;                /* nothing of it is read */
    default:
        return SIZE_MAX;
    }
#undef PH_THROUGH
}

/* -- Output ---------------------------------------------------------- */

static bool host_out_push(ph_host_t *h, ph_out_kind_t kind, int local,
                          const uint8_t *a, size_t a_len,
                          const uint8_t *b, size_t b_len,
                          bool fin, uint64_t code)
{
    if (h->out_count == PH_OUT_SLOTS) {
        host_fail(h, PH_FAULT_ORDER);
        return false;
    }
    ph_out_t o;
    memset(&o, 0, sizeof(o));
    o.kind = kind;
    o.local = local;
    o.fin = fin;
    o.code = code;
    o.deadline = h->cur_deadline;
    o.owner = h->cur_owner;
    o.final_for = h->cur_final_for;
    o.len = a_len + b_len;
    if (o.len) {
        o.bytes = (uint8_t *)malloc(o.len);
        if (!o.bytes) {
            /* The output cannot be kept and was not written: it is lost, so
             * this side ends the session. */
            host_fail(h, PH_FAULT_NOMEM);
            host_close(h, PH_CLOSE_INTERNAL_ERROR, "output copy failed");
            return false;
        }
        h->pair->copies++;
        if (a_len) memcpy(o.bytes, a, a_len);
        if (b_len) memcpy(o.bytes + a_len, b, b_len);
    }
    h->out[(h->out_head + h->out_count) % PH_OUT_SLOTS] = o;
    h->out_count++;
    return true;
}

static void host_out_pop(ph_host_t *h)
{
    ph_out_t *o = &h->out[h->out_head];
    if (o->bytes) {
        free(o->bytes);
        h->pair->copies--;
    }
    memset(o, 0, sizeof(*o));
    h->out_head = (h->out_head + 1) % PH_OUT_SLOTS;
    h->out_count--;
}

/* Drop held datagram k; the later ones move up, keeping their order. */
static void host_dgram_drop(ph_host_t *h, size_t k)
{
    if (h->dg[k].bytes) {
        free(h->dg[k].bytes);
        h->pair->copies--;
    }
    for (size_t j = k + 1; j < h->dg_count; j++) h->dg[j - 1] = h->dg[j];
    h->dg_count--;
    memset(&h->dg[h->dg_count], 0, sizeof(h->dg[0]));
}

/* Hold a copy of a datagram with the tag of its action. A datagram that
 * finds no room is dropped: the transport never promised it. */
static void host_dgram_hold(ph_host_t *h, const uint8_t *data, size_t len)
{
    if (h->dg_count == PH_DGRAM_SLOTS || len > PH_DGRAM_CAP || (len && !data)) {
        h->n.datagrams_dropped++;
        return;
    }
    ph_out_t *d = &h->dg[h->dg_count];
    memset(d, 0, sizeof(*d));
    d->kind = PH_OUT_DATAGRAM;
    d->local = -1;
    d->len = len;
    d->deadline = h->cur_deadline;
    d->owner = h->cur_owner;
    d->final_for = h->cur_final_for;
    if (len) {
        d->bytes = (uint8_t *)malloc(len);
        if (!d->bytes) {
            h->n.datagrams_dropped++;
            return;
        }
        h->pair->copies++;
        memcpy(d->bytes, data, len);
    }
    h->dg_count++;
}

static void host_out_drop_all(ph_host_t *h)
{
    while (h->out_count) host_out_pop(h);
    while (h->dg_count) host_dgram_drop(h, h->dg_count - 1);
}

static bool host_holds_output_for(const ph_host_t *h, int local)
{
    for (size_t k = 0; k < h->out_count; k++)
        if (h->out[(h->out_head + k) % PH_OUT_SLOTS].local == local)
            return true;
    return false;
}

/* A request's final message (tag final_for) on its request stream, when a
 * STOP of the owner's FETCH response stream, observed while that stream was
 * open, ended the request: the message is never written, FIN included, and
 * becomes the request stream's reset with CANCELLED. Once per obligation;
 * not where the session resets the request itself. */
static void host_cancel_final(ph_host_t *h, ph_out_t *o)
{
    if (!o->final_for || o->kind != PH_OUT_WRITE || o->local < 0 ||
        h->local[o->local].role != PH_ROLE_REQUEST)
        return;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        ph_local_t *d = &h->local[i];
        if (!d->used || d->role != PH_ROLE_DATA_OUT || d->owner != o->final_for ||
            !d->final_cancel || d->final_native || d->final_replaced)
            continue;
        d->final_replaced = true;
        if (o->bytes) {
            free(o->bytes);
            h->pair->copies--;
        }
        o->bytes = NULL;
        o->len = o->off = 0;
        o->fin = false;
        o->kind = PH_OUT_RESET;
        o->code = MOQ_STREAM_ERROR_CANCELLED;
        o->reliable = 0;
        o->final_for = 0;
        h->n.finals_cancelled++;
        return;
    }
}

/* A STOP of local data stream `local` observed while the stream is still
 * open (neither its FIN nor its reset accepted). For a stream tagged
 * RESET_FINAL_ON_STOP the obligation is recorded once: the owner's final
 * message, held now or polled later, becomes the request's CANCELLED reset.
 * With `native` (REQUEST_ENDED) the session resets the request itself and
 * nothing is replaced. */
static void host_stop_final(ph_host_t *h, int local, bool native)
{
    ph_local_t *l = &h->local[local];
    if (!l->reset_final_on_stop || l->final_cancel) return;
    l->final_cancel = true;
    l->final_native = native;
    if (native) return;
    for (size_t k = 0; k < h->out_count; k++)
        host_cancel_final(h, &h->out[(h->out_head + k) % PH_OUT_SLOTS]);
}

/* The stream control output goes on. With one bidirectional control stream
 * the client opens it with its first control bytes, and the server answers
 * on the stream the client's bytes arrived on. */
static int host_control_bidi(ph_host_t *h)
{
    if (h->control_bidi >= 0) return h->control_bidi;
    if (h->side != 0) return -1;
    int wire = wire_open(host_wire(h), h->side, true);
    if (wire < 0) return -1;
    moq_stream_ref_t none = moq_stream_ref_from_u64(0);
    h->control_bidi = host_alloc_local(h, wire, false, PH_ROLE_CONTROL_BIDI, none);
    return h->control_bidi;
}

static int host_open_local(ph_host_t *h, bool bidi, ph_role_t role,
                           moq_stream_ref_t ref)
{
    int wire = wire_open(host_wire(h), h->side, bidi);
    if (wire < 0) return -1;
    int l = host_alloc_local(h, wire, false, role, ref);
    if (l >= 0 && role == PH_ROLE_DATA_OUT && h->refuse_next) {
        h->refuse_local = l;
        h->refuse_next = false;
    }
    return l;
}

/* The session resets or aborts request stream `local`: what is still held
 * for it was meant for a stream that now ends, so none of it is written and
 * the reset goes without waiting for it (an ordinary FIN still waits behind
 * its bytes). The session's reset answers a STOP the peer sent too. */
static void host_tear_request(ph_host_t *h, int local)
{
    h->local[local].torn = true;
    h->local[local].stop_debt = false;
}

/* Turn one polled action into an output record this side owns. Every
 * borrowed span is copied here, before the next advancing call. */
static void host_take_action(ph_host_t *h, const moq_action_t *a)
{
    int l;
    /* An action kind this host does not know cannot be skipped: it may be
     * the output a later one depends on. A record stamped shorter than the
     * fields this host reads does not carry them, and none is read. */
    size_t needed = action_detail_needed(a->kind);
    if (needed == SIZE_MAX || !action_covers(a, needed)) {
        host_fail(h, PH_FAULT_UNSUPPORTED);
        return;
    }
    switch (a->kind) {
    case MOQ_ACTION_SEND_CONTROL:
        /* Control bytes on this side's control channel: the one
         * bidirectional control stream or, where each side sends control
         * on a unidirectional stream of its own, the stream its
         * OPEN_UNI_CONTROL opened. */
        l = h->pair->profile->bidi_control ? host_control_bidi(h) : h->control_out;
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.send_control.data,
                            a->u.send_control.len, NULL, 0, false, 0);
        return;

    case MOQ_ACTION_OPEN_UNI_CONTROL:
        l = host_open_local(h, false, PH_ROLE_CONTROL_OUT,
                            a->u.open_uni_control.stream_ref);
        if (l < 0) { host_fail(h, PH_FAULT_STREAMS); return; }
        h->control_out = l;
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.open_uni_control.data,
                            a->u.open_uni_control.len, NULL, 0, false, 0);
        return;

    case MOQ_ACTION_SEND_UNI_CONTROL:
        l = host_find_ref(h, a->u.send_uni_control.stream_ref,
                          PH_ROLE_CONTROL_OUT, PH_ROLE_CONTROL_OUT);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.send_uni_control.data,
                            a->u.send_uni_control.len, NULL, 0,
                            a->u.send_uni_control.fin, 0);
        return;

    case MOQ_ACTION_OPEN_BIDI_STREAM:
        l = host_open_local(h, true, PH_ROLE_REQUEST,
                            a->u.open_bidi_stream.stream_ref);
        if (l < 0) { host_fail(h, PH_FAULT_STREAMS); return; }
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.open_bidi_stream.data,
                            a->u.open_bidi_stream.len, NULL, 0,
                            a->u.open_bidi_stream.fin, 0);
        return;

    case MOQ_ACTION_SEND_BIDI_STREAM:
        l = host_find_ref(h, a->u.send_bidi_stream.stream_ref,
                          PH_ROLE_REQUEST, PH_ROLE_REQUEST);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.send_bidi_stream.data,
                            a->u.send_bidi_stream.len, NULL, 0,
                            a->u.send_bidi_stream.fin, 0);
        return;

    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        l = host_find_ref(h, a->u.close_bidi_stream.stream_ref,
                          PH_ROLE_REQUEST, PH_ROLE_REQUEST);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        (void)host_out_push(h, PH_OUT_WRITE, l, NULL, 0, NULL, 0, true, 0);
        return;

    case MOQ_ACTION_RESET_BIDI_STREAM:
        l = host_find_ref(h, a->u.reset_bidi_stream.stream_ref,
                          PH_ROLE_REQUEST, PH_ROLE_REQUEST);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        host_tear_request(h, l);
        (void)host_out_push(h, PH_OUT_RESET, l, NULL, 0, NULL, 0, false,
                            a->u.reset_bidi_stream.error_code);
        return;

    case MOQ_ACTION_STOP_BIDI_STREAM:
        l = host_find_ref(h, a->u.stop_bidi_stream.stream_ref,
                          PH_ROLE_REQUEST, PH_ROLE_REQUEST);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        (void)host_out_push(h, PH_OUT_STOP, l, NULL, 0, NULL, 0, false,
                            a->u.stop_bidi_stream.error_code);
        return;

    case MOQ_ACTION_ABORT_BIDI_STREAM:
        l = host_find_ref(h, a->u.abort_bidi_stream.stream_ref,
                          PH_ROLE_REQUEST, PH_ROLE_REQUEST);
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); return; }
        host_tear_request(h, l);
        (void)host_out_push(h, PH_OUT_ABORT, l, NULL, 0, NULL, 0, false,
                            a->u.abort_bidi_stream.error_code);
        return;

    case MOQ_ACTION_SEND_DATA: {
        /* The first SEND_DATA for a ref opens its stream. */
        l = host_find_ref(h, a->u.send_data.stream_ref,
                          PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
        if (l < 0)
            l = host_open_local(h, false, PH_ROLE_DATA_OUT,
                                a->u.send_data.stream_ref);
        if (l < 0) { host_fail(h, PH_FAULT_STREAMS); return; }
        const uint8_t *payload = NULL;
        size_t payload_len = 0;
        if (a->u.send_data.payload) {
            payload = moq_rcbuf_data(a->u.send_data.payload);
            payload_len = moq_rcbuf_len(a->u.send_data.payload);
        }
        (void)host_out_push(h, PH_OUT_WRITE, l, a->u.send_data.header,
                            a->u.send_data.header_len, payload, payload_len,
                            a->u.send_data.fin, 0);
        return;
    }

    case MOQ_ACTION_RESET_DATA:
        /* A reset for a stream this side never opened has nothing to end.
         * Everything polled before it for the stream is written first, so
         * the bytes it must still deliver are already ahead of it. */
        l = host_find_ref(h, a->u.reset_data.stream_ref,
                          PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
        if (l < 0) {
            h->n.resets_unknown++;
            return;
        }
        if (h->tagged) {
            /* Written once the stream's earlier bytes are, apart from the
             * poll order, so an owner's final message held for its fence
             * never stands in front of it. A reset this side already holds
             * keeps its own code. */
            ph_local_t *x = &h->local[l];
            x->end_known = true;
            if (x->out_done || x->reset_owed) return;
            x->reset_owed = true;
            x->reset_answer = false;
            x->reset_code = a->u.reset_data.error_code;
            x->reset_reliable =
                action_covers(a, offsetof(moq_reset_data_action_t, reliable_size) +
                                 sizeof(uint64_t))
                ? a->u.reset_data.reliable_size : 0;
            return;
        }
        if (host_out_push(h, PH_OUT_RESET, l, NULL, 0, NULL, 0, false,
                          a->u.reset_data.error_code) &&
            action_covers(a, offsetof(moq_reset_data_action_t, reliable_size) +
                             sizeof(uint64_t))) {
            /* The stream header must still reach the peer, so it can tell
             * what the reset ends. */
            size_t last = (h->out_head + h->out_count - 1) % PH_OUT_SLOTS;
            h->out[last].reliable = a->u.reset_data.reliable_size;
        }
        return;

    case MOQ_ACTION_STOP_DATA:
        l = host_find_ref(h, a->u.stop_data.stream_ref,
                          PH_ROLE_DATA_IN, PH_ROLE_UNROUTED);
        if (l < 0) return;       /* the stream already ended here */
        (void)host_out_push(h, PH_OUT_STOP, l, NULL, 0, NULL, 0, false,
                            a->u.stop_data.error_code);
        return;

    case MOQ_ACTION_SEND_DATAGRAM:
        /* Held apart from the ordered output: a refused datagram holds back
         * no stream, one past its deadline is dropped, and the owner's final
         * message waits while one is held. */
        host_dgram_hold(h, a->u.send_datagram.data, a->u.send_datagram.len);
        return;

    case MOQ_ACTION_SET_DATA_PRIORITY:
        return;                  /* the double has no scheduler */

    case MOQ_ACTION_CLOSE_SESSION:
        /* The session is closed: what it queued before no longer has a
         * session behind it, and the close must not wait behind it. */
        host_out_drop_all(h);
        (void)host_out_push(h, PH_OUT_CLOSE, -1, NULL, 0, NULL, 0, false,
                            a->u.close_session.code);
        return;

    default:
        host_fail(h, PH_FAULT_UNSUPPORTED);
        return;
    }
}

/* Poll actions while this side can still own another one. A poll that
 * returns no action is "empty" only when it also returns MOQ_OK. */
static bool host_actions(ph_host_t *h)
{
    bool progress = false;
    h->polled_empty = false;
    for (int guard = 0; guard < 4 * PH_OUT_SLOTS; guard++) {
        if (h->out_count == PH_OUT_SLOTS) break;
        if (h->poll_budget && (unsigned)guard >= h->poll_budget) break;
        if (h->fault && moq_session_state(h->s) != MOQ_SESS_CLOSED) break;
        moq_action_t a;
        moq_action_tag_t tag;
        size_t count = 0;
        memset(&a, 0, sizeof(a));
        memset(&tag, 0, sizeof(tag));
        moq_result_t rc = h->tagged
            ? moq_session_poll_actions_tagged(h->s, &a, sizeof(a), &tag,
                                              sizeof(tag), 1, &count)
            : moq_session_poll_actions_ex(h->s, &a, 1, sizeof(a), &count);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            /* Event capacity, or (tagged) a reset only the claim takes:
             * not empty either way. */
            h->n.poll_waits++;
            break;
        }
        if (rc != MOQ_OK) { host_fail(h, PH_FAULT_POLL); break; }
        if (count == 0) {
            h->polled_empty = true;
            break;
        }
        progress = true;
        if (h->tagged &&
            (tag.struct_size < MOQ_ACTION_TAG_V0_SIZE ||
             (tag.flags & ~(MOQ_ACTION_TAG_FIRST_OF_STREAM |
                            MOQ_ACTION_TAG_OPEN_COUNTED |
                            MOQ_ACTION_TAG_RESET_FINAL_ON_STOP)) != 0 ||
             ((tag.flags & MOQ_ACTION_TAG_RESET_FINAL_ON_STOP) &&
              (a.kind != MOQ_ACTION_SEND_DATA || tag.owner._v == 0 ||
               !(tag.flags & MOQ_ACTION_TAG_FIRST_OF_STREAM))))) {
            moq_action_cleanup(&a);
            host_fail(h, PH_FAULT_UNSUPPORTED);
            break;
        }
        if (h->tagged) {
            h->cur_deadline = tag.deadline_us;
            h->cur_owner = tag.owner._v;
            h->cur_final_for = tag.final_for._v;
        }
        const size_t held_before = h->out_count;
        host_take_action(h, &a);
        if (h->cur_final_for && h->out_count > held_before)
            host_cancel_final(h, &h->out[(h->out_head + h->out_count - 1) % PH_OUT_SLOTS]);
        h->cur_deadline = UINT64_MAX;
        h->cur_owner = 0;
        h->cur_final_for = 0;
        if (h->tagged && a.kind == MOQ_ACTION_SEND_DATA &&
            (tag.flags & (MOQ_ACTION_TAG_FIRST_OF_STREAM |
                          MOQ_ACTION_TAG_OPEN_COUNTED))) {
            int l = host_find_ref(h, a.u.send_data.stream_ref,
                                  PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
            if (l >= 0 && (tag.flags & MOQ_ACTION_TAG_FIRST_OF_STREAM)) {
                /* The stream counts toward its owner's final-message fence,
                 * and its header is the prefix its reset still delivers. */
                h->local[l].owner = tag.owner._v;
                h->local[l].prefix = a.u.send_data.header_len;
            }
            if (l >= 0 && (tag.flags & MOQ_ACTION_TAG_RESET_FINAL_ON_STOP)) {
                h->local[l].reset_final_on_stop = true;
                h->n.final_policies++;
            }
            if (l >= 0 && (tag.flags & MOQ_ACTION_TAG_OPEN_COUNTED)) {
                h->local[l].open_pending = true;
                h->local[l].open_owner = tag.owner;
            }
        }
        moq_action_cleanup(&a);
    }
    return progress;
}

/* Stop writing a stream past its first `keep` bytes: held writes of it
 * shrink to what the transport took or to `keep`, and lose their FIN. */
static void host_trim_stream(ph_host_t *h, int local, uint64_t keep)
{
    uint64_t at = h->local[local].written;
    for (size_t k = 0; k < h->out_count; k++) {
        ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (o->local != local || o->kind != PH_OUT_WRITE) continue;
        uint64_t room = keep > at ? keep - at : 0;
        size_t len = o->off + (size_t)(room < o->len - o->off ? room : o->len - o->off);
        at += len;
        o->len = len;
        o->fin = false;
    }
}

/* Bytes of a stream the transport took: its completed records and the
 * taken part of a record still in progress. */
static uint64_t host_accepted(const ph_host_t *h, int local)
{
    uint64_t n = h->local[local].written;
    for (size_t k = 0; k < h->out_count; k++) {
        const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (o->local == local && o->kind == PH_OUT_WRITE) n += o->off;
    }
    return n;
}

/* Whether held writes of a stream still have bytes the transport has not
 * taken. */
static bool host_unwritten(const ph_host_t *h, int local)
{
    for (size_t k = 0; k < h->out_count; k++) {
        const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (o->local == local && o->kind == PH_OUT_WRITE && o->off < o->len)
            return true;
    }
    return false;
}

/* Whether this side holds, owes or claimed a reset of the stream it has not
 * written yet: that reset keeps its own code, and no second one follows. */
static bool host_holds_reset(const ph_host_t *h, int local)
{
    if (h->local[local].reset_owed) return true;
    if (h->claim_active && h->claim_local == local) return true;
    for (size_t k = 0; k < h->out_count; k++) {
        const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (o->local == local && o->kind == PH_OUT_RESET) return true;
    }
    return false;
}

/* After a NO_RESET outcome: what this side holds of the stream is dropped,
 * and this side resets the stream with `code`, unless its FIN or a reset
 * was already accepted, or this side already holds, owes or claimed a reset
 * of it (which keeps its own code). */
static void host_owe_reset(ph_host_t *h, int local, uint64_t code)
{
    ph_local_t *l = &h->local[local];
    l->end_known = true;
    host_trim_stream(h, local, 0);
    if (l->out_done || host_holds_reset(h, local)) return;
    l->reset_owed = true;
    l->reset_answer = true;
    l->reset_code = code;
    l->reset_reliable = 0;
}

/* The prefix fact for an offered reset: what this side holds and wrote.
 * A STOP the session took waives the unsent prefix, whatever the reset's
 * cause. 0 while this side has seen a STOP the session has not taken yet:
 * the claim waits for it. */
static moq_data_prefix_state_t host_prefix_fact(const ph_host_t *h,
                                                const moq_data_reset_offer_t *o,
                                                int local)
{
    if (o->state == MOQ_DATA_RESET_UNOPENED || local < 0)
        return MOQ_DATA_PREFIX_UNOPENED;
    const ph_local_t *l = &h->local[local];
    if (l->drop_output)
        return l->peer_stop_done ? MOQ_DATA_PREFIX_WAIVED : 0;
    uint64_t taken = host_accepted(h, local);
    if (taken >= o->reliable_size) return MOQ_DATA_PREFIX_HANDED;
    /* Nothing of the stream reached the transport: no stream exists. */
    if (taken == 0) return MOQ_DATA_PREFIX_UNOPENED;
    return MOQ_DATA_PREFIX_HELD;
}

/* The reset transaction: one claim at a time. host_claim_write writes the
 * claimed reset once the prefix it still delivers is written, and commits
 * it once the transport takes it. */
static bool host_resets(ph_host_t *h)
{
    bool progress = false;
    for (int guard = 0; guard < 8 && !h->claim_active; guard++) {
        moq_data_reset_offer_t off;
        size_t count = 0;
        moq_result_t rc = moq_session_offer_data_reset(h->s, &off, sizeof(off), &count);
        if (rc == MOQ_ERR_WOULD_BLOCK) { h->n.claim_waits++; break; }
        if (rc == MOQ_ERR_CLOSED) break;
        if (rc != MOQ_OK) { host_fail(h, PH_FAULT_POLL); break; }
        if (count == 0) break;
        if (off.struct_size < MOQ_DATA_RESET_OFFER_V0_SIZE ||
            off.state == MOQ_DATA_RESET_CLAIMED) {
            host_fail(h, PH_FAULT_OUTCOME);        /* this host holds no claim */
            break;
        }
        int l = host_find_ref(h, off.stream_ref, PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
        moq_data_prefix_state_t prefix = host_prefix_fact(h, &off, l);
        if (prefix == 0) { h->n.claim_waits++; break; }
        moq_data_reset_claim_t cl;
        rc = moq_session_claim_data_reset(h->s, &off, sizeof(off), prefix,
                                          &cl, sizeof(cl));
        if (rc == MOQ_ERR_WRONG_STATE && prefix == MOQ_DATA_PREFIX_UNOPENED && l >= 0) {
            /* A FETCH or fill prefix cannot be waived: write it first. */
            prefix = MOQ_DATA_PREFIX_HELD;
            rc = moq_session_claim_data_reset(h->s, &off, sizeof(off), prefix,
                                              &cl, sizeof(cl));
        }
        if (rc == MOQ_ERR_STALE_HANDLE) { h->n.claim_stale++; continue; }
        if (rc == MOQ_ERR_WOULD_BLOCK) { h->n.claim_waits++; break; }
        if (rc != MOQ_OK || cl.struct_size < MOQ_DATA_RESET_CLAIM_V0_SIZE ||
            (cl.flags & ~(MOQ_DATA_RESET_CLAIM_EVENT_COMMITTED |
                          MOQ_DATA_RESET_CLAIM_SETTLED |
                          MOQ_DATA_RESET_CLAIM_OPEN_SUPPRESSED)) != 0) {
            host_fail(h, PH_FAULT_OUTCOME);
            break;
        }
        h->n.claims++;
        progress = true;
        if (cl.flags & MOQ_DATA_RESET_CLAIM_SETTLED) {
            /* No transport stream exists: drop what is held of it. */
            h->n.claims_settled++;
            if (l >= 0) {
                host_trim_stream(h, l, 0);
                h->local[l].open_pending = false;
                h->local[l].out_done = true;
                h->local[l].end_known = true;
            }
            continue;
        }
        if (l < 0) { host_fail(h, PH_FAULT_ORDER); break; }
        host_trim_stream(h, l, prefix == MOQ_DATA_PREFIX_HELD ? off.reliable_size : 0);
        h->local[l].end_known = true;
        h->claim = off;
        h->claim_local = l;
        h->claim_reliable = prefix == MOQ_DATA_PREFIX_WAIVED ? host_accepted(h, l)
                                                            : off.reliable_size;
        h->claim_active = true;
    }
    return progress;
}

/* The claimed reset, once every prefix byte it still delivers is written
 * (or waived by the peer's STOP). A refusal keeps the claim and is not
 * progress; once the transport takes the reset, the claim is committed. */
static bool host_claim_write(ph_host_t *h)
{
    if (!h->claim_active) return false;
    if (h->wire_closed) {
        h->claim_active = false;     /* the close dropped the claim */
        return false;
    }
    ph_local_t *l = &h->local[h->claim_local];
    if (!l->drop_output && host_unwritten(h, h->claim_local)) return false;
    if (!l->out_done) {
        if (h->refuse_resets) {
            h->refuse_resets--;
            h->n.resets_refused++;
            return false;
        }
        wire_reset(host_wire(h), l->wire, h->side, h->claim.error_code,
                   h->claim_reliable);
        l->out_done = true;
    }
    moq_result_t rc = moq_session_commit_data_reset(h->s, &h->claim, sizeof(h->claim));
    h->claim_active = false;
    if (rc == MOQ_OK) h->n.commits++;
    else if (rc != MOQ_ERR_CLOSED) host_fail(h, PH_FAULT_OUTCOME);
    return true;
}

/* The data stream resets this side writes on its own timing (one it polled,
 * or one answering NO_RESET): each once its stream's earlier bytes are
 * written. A refused one stays owed and is not progress. */
static bool host_owed_resets(ph_host_t *h)
{
    bool progress = false;
    for (int i = 0; i < PH_MAX_STREAMS && !h->wire_closed; i++) {
        ph_local_t *l = &h->local[i];
        if (!l->used || !l->reset_owed) continue;
        if (l->out_done) {
            l->reset_owed = false;
            continue;
        }
        if (!l->drop_output && host_unwritten(h, i)) continue;
        if (h->refuse_resets) {
            h->refuse_resets--;
            h->n.resets_refused++;
            break;
        }
        wire_reset(host_wire(h), l->wire, h->side, l->reset_code, l->reset_reliable);
        l->out_done = true;
        l->reset_owed = false;
        if (l->reset_answer) h->n.host_resets++;
        progress = true;
    }
    return progress;
}

/* Whether an owner's final message must still wait: a data stream of that
 * owner has neither its FIN nor its reset accepted, a reset claim of it is
 * outstanding, or a datagram of it is held. */
static bool host_final_waits(const ph_host_t *h, uint64_t owner)
{
    if (h->claim_active && h->claim.owner._v == owner) return true;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        const ph_local_t *l = &h->local[i];
        if (l->used && l->role == PH_ROLE_DATA_OUT && l->owner == owner &&
            !l->out_done)
            return true;
    }
    for (size_t k = 0; k < h->dg_count; k++)
        if (h->dg[k].owner == owner) return true;
    return false;
}

/* Whether a held record has subgroup bytes not yet written that its
 * deadline still governs: tagged, on a local data stream this side still
 * writes. A reset that already owns the stream's end changes what expiry
 * does (host_expire_stream), never whether expired bytes may be written:
 * they may not. */
static bool host_out_can_expire(const ph_host_t *h, const ph_out_t *o)
{
    if (!h->tagged || o->kind != PH_OUT_WRITE || o->off >= o->len || o->local < 0)
        return false;
    const ph_local_t *l = &h->local[o->local];
    return l->role == PH_ROLE_DATA_OUT && !l->drop_output && !l->out_done;
}

static bool host_out_expired(const ph_host_t *h, const ph_out_t *o)
{
    return o->deadline <= host_now(h) && host_out_can_expire(h, o);
}

/* The bytes of a stream that still go before the end already decided for
 * it: the reliable size of the reset this side owes or claimed, else its
 * header (a reset the session will offer delivers that). */
static uint64_t host_kept_prefix(const ph_host_t *h, int local)
{
    const ph_local_t *l = &h->local[local];
    if (l->reset_owed) return l->reset_reliable;
    if (h->claim_active && h->claim_local == local) return h->claim_reliable;
    return l->prefix;
}

/* Held bytes of a subgroup stream passed their tag deadline. When nothing
 * has decided the stream's end yet, the session says how it ends:
 * RESET_FOLLOWS (nothing past its prefix is written; the reset comes
 * through the reset offer) or NO_RESET (what is held is dropped, and this
 * side resets the stream itself). When the end is already decided (a reset
 * this side polled, owes or claimed, or an earlier expiry or STOP), the
 * expired bytes are only dropped past the prefix that end still delivers:
 * that reset keeps its code, and no second report, reset or claim follows.
 * Returns false when this side faulted. */
static bool host_expire_stream(ph_host_t *h, int local)
{
    ph_local_t *l = &h->local[local];
    if (l->end_known) {
        host_trim_stream(h, local, host_kept_prefix(h, local));
        h->n.held_trimmed++;
        return true;
    }
    moq_data_end_result_t end;
    memset(&end, 0, sizeof(end));
    moq_result_t rc = moq_session_on_held_data_expired(h->s, l->ref, host_now(h),
                                                       &end, sizeof(end));
    l->end_known = true;
    if (rc == MOQ_ERR_CLOSED) {
        /* Nothing more is written; the session's close output tells. */
        host_trim_stream(h, local, 0);
        return true;
    }
    if (rc != MOQ_OK || end.struct_size < MOQ_DATA_END_RESULT_V0_SIZE ||
        (end.outcome != MOQ_DATA_END_RESET_FOLLOWS &&
         end.outcome != MOQ_DATA_END_NO_RESET) || end.flags != 0) {
        host_fail(h, PH_FAULT_OUTCOME);
        return false;
    }
    h->n.held_expired++;
    if (end.outcome == MOQ_DATA_END_RESET_FOLLOWS)
        host_trim_stream(h, local, l->prefix);
    else
        host_owe_reset(h, local, end.host_reset_code);
    return true;
}

/* Held datagrams past their deadline are dropped; nothing is reported. */
static bool host_dgram_expire(ph_host_t *h)
{
    bool progress = false;
    size_t k = 0;
    for (int guard = 0; guard < 2 * PH_DGRAM_SLOTS && k < h->dg_count; guard++) {
        if (h->dg[k].deadline > host_now(h)) {
            k++;
            continue;
        }
        host_dgram_drop(h, k);
        h->n.datagrams_expired++;
        progress = true;
    }
    return progress;
}

/* Held output past its tag deadline, before anything this pass writes. */
static bool host_expire(ph_host_t *h)
{
    bool progress = host_dgram_expire(h);
    for (size_t k = 0; k < h->out_count && !h->fault; k++) {
        const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (!host_out_expired(h, o)) continue;
        /* The report trims records in place: the indexes still hold. */
        if (!host_expire_stream(h, o->local)) break;
        progress = true;
    }
    return progress;
}

/* Held datagrams, oldest first, each checked against its deadline before
 * the transport is offered it. A refused one waits and is not progress; one
 * the transport's datagram queue has no room for is dropped. */
static bool host_dgram_flush(ph_host_t *h)
{
    bool progress = host_dgram_expire(h);
    ph_wire_t *w = host_wire(h);
    for (int guard = 0; guard < PH_DGRAM_SLOTS && h->dg_count && !h->wire_closed;
         guard++) {
        if (w->refuse[h->side] & PH_REFUSE_DATAGRAMS) {
            h->n.writes_refused++;
            break;
        }
        const ph_out_t *d = &h->dg[0];
        if (w->dgram_count[h->side] == PH_DGRAM_SLOTS) {
            h->n.datagrams_dropped++;
        } else {
            ph_dgram_t *x = &w->dgram[h->side][w->dgram_count[h->side]++];
            if (d->len) memcpy(x->bytes, d->bytes, d->len);
            x->len = d->len;
            h->n.datagrams_sent++;
        }
        host_dgram_drop(h, 0);
        progress = true;
    }
    return progress;
}

/* What one attempt to write a held record did. */
typedef enum ph_step {
    PH_STEP_DONE,               /* the record is finished: remove it */
    PH_STEP_HELD,               /* it stays: refused, partly taken or fenced */
    PH_STEP_AGAIN,              /* its stream's expiry trimmed it: try again */
    PH_STEP_STOP                /* this side faulted: write nothing more */
} ph_step_t;

/* Try one held record. Before any byte of a subgroup is written or retried
 * its deadline is checked, and an owner's final message waits for its
 * fence (host_final_waits). */
static ph_step_t host_out_step(ph_host_t *h, ph_out_t *o, bool *progress)
{
    ph_wire_t *w = host_wire(h);
    ph_local_t *l = o->local >= 0 ? &h->local[o->local] : NULL;

    if (o->final_for && o->off == 0 && host_final_waits(h, o->final_for)) {
        h->n.final_waits++;
        return PH_STEP_HELD;
    }

    switch (o->kind) {
    case PH_OUT_WRITE:
        /* Bytes the peer asked this side not to send, or that follow this
         * side's own reset, are not delivered. */
        if (l->drop_output || l->out_done || l->torn) return PH_STEP_DONE;
        if (host_out_expired(h, o)) {
            if (!host_expire_stream(h, o->local)) return PH_STEP_STOP;
            *progress = true;
            return PH_STEP_AGAIN;            /* the trimmed record again */
        }
        size_t room = o->len - o->off;
        const bool refused = o->local == h->refuse_local;
        if (refused && (o->off < o->len || o->fin)) {
            /* ph_refuse_data_stream(): nothing past the allowance, no FIN */
            uint64_t taken = host_accepted(h, o->local);
            if (o->off == o->len || taken >= h->refuse_allow) {
                h->n.writes_refused++;
                return PH_STEP_HELD;
            }
            if (room > h->refuse_allow - taken) room = (size_t)(h->refuse_allow - taken);
        }
        if (o->off < o->len) {
            size_t took = wire_write(w, l->wire, h->side, o->bytes + o->off, room);
            if (took == 0) {
                h->n.writes_refused++;
                return PH_STEP_HELD;         /* blocked */
            }
            o->off += took;
            *progress = true;
            if (o->off < o->len) {
                h->n.partial_writes++;
                return PH_STEP_HELD;         /* the rest is still owed */
            }
            if (refused && o->fin) return PH_STEP_HELD;   /* its FIN is refused */
        }
        l->written += o->off;                /* the record is complete */
        if (h->tagged && l->open_pending) {
            /* The transport accepted the stream's first bytes: its open is a
             * fact, reported once. */
            moq_result_t rc = moq_session_report_data_open(
                h->s, l->ref, l->open_owner, MOQ_DATA_OPEN_OPENED);
            l->open_pending = false;
            if (rc == MOQ_OK) h->n.opens_reported++;
            else if (rc != MOQ_ERR_STALE_HANDLE) host_fail(h, PH_FAULT_OUTCOME);
        }
        if (o->fin) {
            wire_fin(w, l->wire, h->side);   /* after the last byte */
            l->out_done = true;
            if (h->tagged && l->role == PH_ROLE_DATA_OUT) {
                /* The transport accepted the FIN after every earlier byte of
                 * the stream: reported once, whatever the stream carries. */
                moq_result_t rc = moq_session_report_data_fin(h->s, l->ref);
                if (rc == MOQ_OK) h->n.fins_reported++;
                else if (rc != MOQ_ERR_CLOSED) host_fail(h, PH_FAULT_OUTCOME);
            }
        }
        return PH_STEP_DONE;

    case PH_OUT_RESET:
        if (!l->out_done) {
            wire_reset(w, l->wire, h->side, o->code, o->reliable);
            l->out_done = true;
        }
        return PH_STEP_DONE;

    case PH_OUT_STOP:
        wire_stop(w, l->wire, h->side, o->code);
        l->stop_sent = true;
        return PH_STEP_DONE;

    case PH_OUT_ABORT:
        if (!l->out_done) {
            wire_reset(w, l->wire, h->side, o->code, 0);
            l->out_done = true;
        }
        wire_stop(w, l->wire, h->side, o->code);
        l->stop_sent = true;
        return PH_STEP_DONE;

    case PH_OUT_CLOSE:
        w->closed[h->side] = true;
        w->close_code[h->side] = o->code;
        h->wire_closed = true;
        return PH_STEP_DONE;

    default:
        host_fail(h, PH_FAULT_UNSUPPORTED);
        return PH_STEP_STOP;
    }
}

/* Remove held record k (0 is the oldest); the later ones keep their order. */
static void host_out_remove(ph_host_t *h, size_t k)
{
    if (k == 0) {
        host_out_pop(h);
        return;
    }
    ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
    if (o->bytes) {
        free(o->bytes);
        h->pair->copies--;
    }
    for (size_t j = k; j + 1 < h->out_count; j++)
        h->out[(h->out_head + j) % PH_OUT_SLOTS] =
            h->out[(h->out_head + j + 1) % PH_OUT_SLOTS];
    memset(&h->out[(h->out_head + h->out_count - 1) % PH_OUT_SLOTS], 0,
           sizeof(h->out[0]));
    h->out_count--;
}

/* Write held output in the order it was polled: the oldest record first,
 * and nothing after one that stays held. */
static bool host_flush_polled(ph_host_t *h)
{
    bool progress = false;
    /* Each round removes a record, or reports one stream's expiry (which
     * decides that stream's end, so no stream is reported twice). */
    for (int guard = 0; guard < PH_OUT_SLOTS + PH_MAX_STREAMS &&
                        h->out_count && !h->wire_closed; guard++) {
        ph_step_t step = host_out_step(h, &h->out[h->out_head], &progress);
        if (step == PH_STEP_HELD || step == PH_STEP_STOP) return progress;
        if (step == PH_STEP_AGAIN) continue;
        host_out_pop(h);
        progress = true;
    }
    if (h->wire_closed) host_out_drop_all(h);
    return progress;
}

/* Write held output in each stream's own order: oldest first, past the
 * records that stay held, except that a held record holds back the later
 * records of its stream. A record on no stream (the transport close) is
 * written only once everything before it is, and holds back everything
 * after it. */
static bool host_flush_streams(ph_host_t *h)
{
    bool progress = false;
    bool held[PH_MAX_STREAMS];
    memset(held, 0, sizeof(held));
    size_t k = 0;
    /* Each round removes a record, passes one, or reports one stream's
     * expiry. */
    for (int guard = 0; guard < 2 * PH_OUT_SLOTS + PH_MAX_STREAMS &&
                        k < h->out_count && !h->wire_closed; guard++) {
        ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (o->local < 0 ? k != 0 : held[o->local]) {
            if (o->local < 0) break;
            k++;
            continue;
        }
        ph_step_t step = host_out_step(h, o, &progress);
        if (step == PH_STEP_STOP) return progress;
        if (step == PH_STEP_AGAIN) continue;
        if (step == PH_STEP_HELD) {
            if (o->local < 0) break;
            held[o->local] = true;
            k++;
            continue;
        }
        host_out_remove(h, k);
        progress = true;
    }
    if (h->wire_closed) host_out_drop_all(h);
    return progress;
}

static bool host_flush(ph_host_t *h)
{
    return h->per_stream ? host_flush_streams(h) : host_flush_polled(h);
}

/* -- Input ----------------------------------------------------------- */

/* Offer what this side holds for a stream to the feed its role names.
 * Returns true when the session took it (consumed or retained). */
static bool host_feed(ph_host_t *h, int index)
{
    ph_local_t *l = &h->local[index];
    moq_result_t rc;

    switch (l->role) {
    case PH_ROLE_CONTROL_BIDI:
    case PH_ROLE_CONTROL_IN:
        /* Nothing new is fed while the session still holds a blocked
         * control message. */
        if (h->control_pending) return false;
        if (l->hold_len) {
            rc = moq_session_on_control_bytes(h->s, l->hold, l->hold_len,
                                              host_now(h));
            if (rc == MOQ_ERR_WOULD_BLOCK) {
                /* Every byte was kept. A FIN read with them is reported
                 * only after the session has worked through them. */
                h->control_pending = true;
                l->hold_len = 0;
                return true;
            } else if (rc != MOQ_OK) {
                l->hold_len = 0;
                host_input_failed(h, "control input refused");
                return false;
            }
            l->hold_len = 0;
        }
        if (l->hold_fin) {
            l->hold_fin = false;
            l->in_done = true;
            h->n.peer_fins++;
            h->n.control_ended++;
            (void)moq_session_on_control_stream_ended(h->s, host_now(h));
        }
        return true;

    case PH_ROLE_PADDING:
        l->hold_len = 0;
        if (l->hold_fin) { l->hold_fin = false; l->in_done = true; }
        return true;

    case PH_ROLE_DATA_IN:
        rc = moq_session_on_data_bytes(h->s, l->ref, l->hold, l->hold_len,
                                       l->hold_fin, host_now(h));
        break;

    case PH_ROLE_REQUEST:
        rc = moq_session_on_bidi_stream_bytes(h->s, l->ref, l->hold,
                                              l->hold_len, l->hold_fin,
                                              host_now(h));
        break;

    default:
        return false;
    }

    if (rc == MOQ_ERR_WOULD_BLOCK) {
        if (moq_session_has_transport_stream(h->s, l->ref)) {
            /* The session owns the stream and kept what it needs: this side
             * lets go of the bytes and later retries with an empty call. */
            l->owned_retry = true;
            l->owned_fin = l->hold_fin;
            l->hold_len = 0;
            l->hold_fin = false;
            if (l->refused) { l->refused = false; h->refused_streams--; }
            return true;
        }
        /* Not admitted: nothing was kept. This side keeps the exact bytes
         * and FIN, stops reading the stream, and offers them again. */
        if (!l->refused) {
            l->refused = true;
            h->refused_streams++;
            h->n.refused_inputs++;
        }
        return false;
    }
    /* Closed, or setup not complete: nothing was taken; the bytes stay. */
    if (rc == MOQ_ERR_CLOSED) return false;
    if (rc != MOQ_OK) {
        l->hold_len = 0;
        l->hold_fin = false;
        host_input_failed(h, "stream input failed");
        return false;
    }
    if (l->refused) {
        l->refused = false;
        h->refused_streams--;
        h->n.redelivered++;
    }
    if (l->hold_fin) { l->in_done = true; h->n.peer_fins++; }
    l->hold_len = 0;
    l->hold_fin = false;
    return true;
}

/* Find the route of a peer unidirectional stream from the bytes held.
 * *waiting is set when the type is still incomplete and the stream stays
 * unrouted with its bytes held. */
static bool host_route(ph_host_t *h, int index, bool ended, bool *waiting)
{
    ph_local_t *l = &h->local[index];
    moq_uni_route_t route = MOQ_UNI_ROUTE_NONE;
    *waiting = false;
    moq_result_t rc = moq_session_route_peer_uni(h->s, l->hold, l->hold_len,
                                                 &route);
    if (rc == MOQ_ERR_CLOSED) return false;
    if (rc != MOQ_OK) { host_fail(h, PH_FAULT_INPUT); return false; }

    switch (route) {
    case MOQ_UNI_ROUTE_NEED_MORE:
        h->n.need_more++;
        if (ended && l->hold_len == 0) {
            /* A FIN with no byte: no type, nothing owed, no session input. */
            l->hold_fin = false;
            l->in_done = true;
            h->n.ended_unrouted++;
        } else if (ended || l->hold_len == PH_HOLD_CAP) {
            /* A FIN inside the type: the stream type is truncated, which
             * the session's own data path treats as a protocol violation. */
            l->hold_len = 0;
            l->hold_fin = false;
            l->in_done = true;
            host_close(h, PH_CLOSE_PROTOCOL_VIOLATION, "truncated stream type");
        } else {
            *waiting = true;
        }
        return false;
    case MOQ_UNI_ROUTE_CONTROL:
        if (h->control_in >= 0) {
            l->in_done = true;
            host_close(h, PH_CLOSE_PROTOCOL_VIOLATION, "second control stream");
            return false;
        }
        h->control_in = index;
        l->role = PH_ROLE_CONTROL_IN;
        h->n.routed_control++;
        return true;
    case MOQ_UNI_ROUTE_DATA:
        l->role = PH_ROLE_DATA_IN;
        h->n.routed_data++;
        return true;
    case MOQ_UNI_ROUTE_PADDING:
        l->role = PH_ROLE_PADDING;
        h->n.routed_padding++;
        return true;
    case MOQ_UNI_ROUTE_UNKNOWN_TYPE:
        l->in_done = true;
        host_close(h, PH_CLOSE_PROTOCOL_VIOLATION, "unknown stream type");
        return false;
    default:
        /* A route this host does not know: the bytes go nowhere. */
        host_fail(h, PH_FAULT_UNSUPPORTED);
        return false;
    }
}

/* The peer reset the direction it writes. */
static bool host_peer_reset(ph_host_t *h, int index, uint64_t code)
{
    ph_local_t *l = &h->local[index];
    moq_result_t rc;
    /* What this side still held of the stream ends with it. */
    l->hold_len = 0;
    l->hold_fin = false;
    if (l->refused) { l->refused = false; h->refused_streams--; }

    switch (l->role) {
    case PH_ROLE_CONTROL_BIDI:
    case PH_ROLE_CONTROL_IN:
        h->n.control_ended++;
        rc = moq_session_on_control_stream_ended(h->s, host_now(h));
        break;
    case PH_ROLE_UNROUTED:
        /* Reset before its type was complete: no route, no session input,
         * and no identity is inferred for it (it is not the control
         * channel, whatever its first bytes were). Nothing is owed. */
        l->in_done = true;
        h->n.ended_unrouted++;
        return true;
    case PH_ROLE_DATA_IN:
        rc = moq_session_on_data_reset(h->s, l->ref, code, host_now(h));
        break;
    case PH_ROLE_REQUEST:
        rc = moq_session_on_bidi_stream_reset(h->s, l->ref, code, host_now(h));
        break;
    case PH_ROLE_PADDING:
        rc = MOQ_OK;             /* nothing of it reached the session */
        break;
    default:
        host_fail(h, PH_FAULT_ORDER);
        return false;
    }
    switch (rc) {
    case MOQ_OK:
    case MOQ_ERR_CLOSED:         /* the session is closed: nothing is owed */
        break;
    case MOQ_ERR_WOULD_BLOCK:
        return false;            /* retained: the same signal again */
    default:
        host_fail(h, PH_FAULT_OUTCOME);
        return false;
    }
    l->in_done = true;
    l->owned_retry = false;
    h->n.peer_resets++;
    return true;
}

/* The peer told this side to stop sending on a stream. */
static bool host_peer_stop(ph_host_t *h, int index, uint64_t code)
{
    ph_local_t *l = &h->local[index];
    moq_result_t rc = MOQ_OK;
    h->n.stop_entries++;
    l->drop_output = true;       /* write nothing more, whatever follows */

    switch (l->role) {
    case PH_ROLE_CONTROL_BIDI:
    case PH_ROLE_CONTROL_OUT:
        h->n.control_ended++;
        rc = moq_session_on_control_stream_ended(h->s, host_now(h));
        break;
    case PH_ROLE_DATA_OUT:
        if (h->tagged) {
            /* Whether the STOP finds the stream still open: only then does
             * it end a FETCH's request too (RESET_FINAL_ON_STOP). */
            const bool open = !l->out_done;
            /* A held claim of this stream turns WAIVED with no session call:
             * its reset answers the STOP, delivering only what was taken. */
            if (h->claim_active && h->claim_local == index) {
                h->claim_reliable = host_accepted(h, index);
                if (open) host_stop_final(h, index, false);
                rc = MOQ_OK;
                break;
            }
            moq_data_end_result_t end;
            memset(&end, 0, sizeof(end));
            rc = moq_session_on_data_stop_ex(h->s, l->ref, code, host_now(h),
                                             &end, sizeof(end));
            if (rc == MOQ_OK) {
                if (end.struct_size < MOQ_DATA_END_RESULT_V0_SIZE ||
                    (end.outcome != MOQ_DATA_END_RESET_FOLLOWS &&
                     end.outcome != MOQ_DATA_END_NO_RESET) ||
                    (end.flags & ~MOQ_DATA_END_REQUEST_ENDED) != 0) {
                    host_fail(h, PH_FAULT_OUTCOME);
                    return false;
                }
                l->end_known = true;
                /* No native reset follows: this side answers the STOP,
                 * unless it already holds, owes or had accepted one. */
                if (end.outcome == MOQ_DATA_END_NO_RESET)
                    host_owe_reset(h, index, end.host_reset_code);
                if (open)
                    host_stop_final(h, index,
                                    (end.flags & MOQ_DATA_END_REQUEST_ENDED) != 0);
            }
            break;
        }
        /* MOQ_ERR_WOULD_BLOCK: deliver the same stop again. */
        rc = moq_session_on_data_stop(h->s, l->ref, code, host_now(h));
        break;
    case PH_ROLE_REQUEST:
        /* Nothing more is written on this side's half, and it is reset
         * once: with the session's own reset or abort of the stream when
         * one comes, else with this STOP's code once a poll shows the
         * session has nothing more for it (host_stop_debts). The session is
         * told whichever side opened the stream: a STOP ends the request
         * it carries either way. */
        if (!l->out_done && !l->stop_debt && !l->torn) {
            l->stop_debt = true;
            l->stop_code = code;
        }
        rc = moq_session_on_bidi_stream_stop(h->s, l->ref, code, host_now(h));
        if (rc == MOQ_OK) h->n.request_stops++;
        break;
    case PH_ROLE_PADDING:
    case PH_ROLE_DATA_IN:
    case PH_ROLE_CONTROL_IN:
    case PH_ROLE_UNROUTED:
        break;                   /* this side writes nothing on these */
    default:
        host_fail(h, PH_FAULT_ORDER);
        return false;
    }
    switch (rc) {
    case MOQ_OK:
    case MOQ_ERR_CLOSED:
        break;
    case MOQ_ERR_WOULD_BLOCK:
        return false;            /* retained: the same signal again */
    default:
        host_fail(h, PH_FAULT_OUTCOME);
        return false;
    }
    l->peer_stop_done = true;
    h->n.peer_stops++;
    return true;
}

/* The transport retired a stream this side stopped: no FIN or RESET from
 * the peer will arrive. Report it once this side holds nothing of it. */
static bool host_retire(ph_host_t *h, int index)
{
    ph_local_t *l = &h->local[index];
    ph_stream_t *st = &host_wire(h)->streams[l->wire];
    moq_result_t rc;

    if (!l->stop_sent || l->hold_len || l->hold_fin) return false;
    if (st->bidi) {
        if (l->role != PH_ROLE_REQUEST) { l->in_done = true; return true; }
        if (host_holds_output_for(h, index)) return false;
        rc = moq_session_on_bidi_stream_retired(h->s, l->ref, host_now(h));
        if (rc == MOQ_OK) h->n.bidi_retired++;
    } else {
        if (l->role != PH_ROLE_DATA_IN) { l->in_done = true; return true; }
        rc = moq_session_on_data_stream_retired(h->s, l->ref, host_now(h));
        if (rc == MOQ_OK) h->n.data_retired++;
    }
    switch (rc) {
    case MOQ_OK:
        break;
    case MOQ_ERR_STALE_HANDLE:
        h->n.retire_stale++;         /* the session held nothing for it */
        break;
    case MOQ_ERR_CLOSED:
        break;
    case MOQ_ERR_WRONG_STATE:
        h->n.retire_refused++;       /* the session has not let go yet */
        return false;
    default:
        host_fail(h, PH_FAULT_OUTCOME);
        return false;
    }
    l->in_done = true;
    l->owned_retry = false;
    st->dir[1 - h->side].retire_read = true;
    return true;
}

static bool host_stream_input(ph_host_t *h, int index)
{
    ph_local_t *l = &h->local[index];
    ph_wire_t *w = host_wire(h);
    ph_stream_t *st = &w->streams[l->wire];
    ph_pipe_t *in = &st->dir[1 - h->side];       /* what the peer writes */
    ph_pipe_t *out = &st->dir[h->side];          /* what this side writes */
    bool progress = false;
    bool has_in = st->bidi || l->peer_opened;
    bool has_out = st->bidi || !l->peer_opened;

    /* The peer's stop of this side's direction. */
    if (has_out && out->stop && !l->peer_stop_done) {
        if (host_peer_stop(h, index, out->stop_code)) {
            out->stop_read = true;
            progress = true;
        }
    }

    /* "All data committed": the peer has read every byte and the FIN. */
    if (has_out && l->role == PH_ROLE_DATA_OUT && !l->committed &&
        out->fin && out->fin_read && out->len == 0) {
        l->committed = true;
        (void)moq_session_on_data_committed(h->s, l->ref, host_now(h));
        progress = true;
    }

    if (!has_in || l->in_done) return progress;

    /* A delivery the session kept: an empty call resumes it. */
    if (l->owned_retry) {
        moq_result_t rc;
        if (l->role == PH_ROLE_REQUEST)
            rc = moq_session_on_bidi_stream_bytes(h->s, l->ref, NULL, 0, false,
                                                  host_now(h));
        else
            rc = moq_session_on_data_bytes(h->s, l->ref, NULL, 0, false,
                                           host_now(h));
        h->n.owned_retries++;
        if (rc == MOQ_ERR_WOULD_BLOCK) return progress;
        if (rc != MOQ_OK) {
            l->owned_retry = false;
            host_input_failed(h, "stream retry failed");
            return progress;
        }
        l->owned_retry = false;
        if (l->owned_fin) { l->owned_fin = false; l->in_done = true; h->n.peer_fins++; }
        progress = true;
        if (l->in_done) return progress;
    }

    /* A delivery the session did not admit: offer the same bytes again once
     * it could take a new stream. Nothing more is read meanwhile. */
    if (l->refused) {
        if (l->role == PH_ROLE_DATA_IN &&
            !moq_session_can_admit_data_stream(h->s))
            return progress;
        if (host_feed(h, index)) progress = true;
        if (l->refused || l->in_done) return progress;
    }

    /* Read: as much as the hold takes, and the FIN once the bytes are in. */
    if (!in->dropped) {
        size_t room = PH_HOLD_CAP - l->hold_len;
        size_t got = room ? wire_read(w, l->wire, h->side,
                                      l->hold + l->hold_len, room) : 0;
        if (got) { l->hold_len += got; progress = true; }
        if (in->fin && !in->fin_read && in->len == 0) {
            in->fin_read = true;
            l->hold_fin = true;
            progress = true;
        }
    }

    /* A peer unidirectional stream is routed before any byte is fed. When
     * the type is still incomplete, every reliable byte has been read and
     * the peer reset the stream, no more bytes will come: the reset below
     * settles the unrouted stream (its held bytes are discarded, it is
     * counted once, and nothing reaches the session). */
    if (l->role == PH_ROLE_UNROUTED && (l->hold_len || l->hold_fin)) {
        bool waiting = false;
        if (host_route(h, index, l->hold_fin, &waiting)) {
            progress = true;
        } else if (!(waiting && in->reset && !in->reset_read &&
                     !in->dropped && in->len == 0)) {
            return progress;
        }
    }

    if (l->role != PH_ROLE_UNROUTED && (l->hold_len || l->hold_fin)) {
        if (host_feed(h, index)) progress = true;
    }

    /* The peer's reset, once nothing of the stream is left to read. */
    if (in->reset && !in->reset_read && !in->dropped && !l->in_done &&
        in->len == 0) {
        if (host_peer_reset(h, index, in->reset_code)) {
            in->reset_read = true;
            progress = true;
        }
    }

    /* Retirement: the transport dropped the direction after this side's
     * stop and says nothing more will come. */
    if (in->dropped && !in->retire_read && !l->in_done) {
        if (host_retire(h, index)) progress = true;
    }
    return progress;
}

static bool host_inputs(ph_host_t *h)
{
    bool progress = false;
    ph_wire_t *w = host_wire(h);
    int peer = 1 - h->side;

    /* Control input the session kept: resume it before feeding more. */
    if (h->control_pending) {
        moq_result_t rc = moq_session_process_pending(h->s, host_now(h));
        if (rc == MOQ_OK) {
            h->control_pending = false;
            progress = true;
        } else if (rc != MOQ_ERR_WOULD_BLOCK) {
            h->control_pending = false;
            host_input_failed(h, "control resume failed");
        }
    }

    for (int i = 0; i < PH_MAX_STREAMS && !h->fault; i++) {
        ph_stream_t *st = &w->streams[i];
        if (!st->used) continue;
        int index = host_find_wire(h, i);
        if (index < 0) {
            if (st->opener == h->side) continue;
            /* A new peer stream. At the limit of refused streams this side
             * accepts no more: the transport's stream limit holds the peer. */
            if (h->refused_streams >= PH_MAX_REFUSED) continue;
            ph_role_t role = PH_ROLE_UNROUTED;
            if (st->bidi) {
                role = PH_ROLE_REQUEST;
                if (h->pair->profile->bidi_control && h->control_bidi < 0)
                    role = PH_ROLE_CONTROL_BIDI;
            }
            moq_stream_ref_t ref =
                moq_stream_ref_from_u64(PH_PEER_REF_BASE | h->next_peer_ref++);
            index = host_alloc_local(h, i, true, role, ref);
            if (index < 0) { host_fail(h, PH_FAULT_STREAMS); break; }
            if (role == PH_ROLE_CONTROL_BIDI) h->control_bidi = index;
            progress = true;
        }
        if (host_stream_input(h, index)) progress = true;
    }

    /* Datagrams are never retained: taken or dropped. */
    for (size_t k = 0; k < w->dgram_count[peer]; k++) {
        ph_dgram_t *d = &w->dgram[peer][k];
        moq_result_t rc = moq_session_on_datagram(h->s, d->bytes, d->len,
                                                  host_now(h));
        if (rc != MOQ_OK) h->n.datagrams_dropped++;
        progress = true;
    }
    w->dgram_count[peer] = 0;
    return progress;
}

/* After a poll that returned no action: each request stream the peer
 * stopped and the session neither reset nor aborted is reset with the
 * STOP's code, once. A reset or abort of it already held stands instead. */
static bool host_stop_debts(ph_host_t *h)
{
    bool progress = false;
    for (int i = 0; i < PH_MAX_STREAMS && !h->wire_closed; i++) {
        ph_local_t *l = &h->local[i];
        if (!l->used || !l->stop_debt) continue;
        l->stop_debt = false;
        bool held = false;
        for (size_t k = 0; k < h->out_count; k++) {
            const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
            if (o->local == i && (o->kind == PH_OUT_RESET || o->kind == PH_OUT_ABORT))
                held = true;
        }
        if (held || l->out_done) continue;
        wire_reset(host_wire(h), l->wire, h->side, l->stop_code, 0);
        l->out_done = true;
        h->n.stop_answers++;
        progress = true;
    }
    return progress;
}

/* The session's own deadline, after the input that arrived before it and
 * before this pass takes actions, so what the tick queues is taken (and
 * counted) in the same pass. At most one tick per wake. An OK tick is
 * progress only when it moved the deadline; a tick that returned
 * MOQ_ERR_WOULD_BLOCK is not. Such a tick is not repeated at the same time
 * while the deadline stays where it left it (that tick would find the same
 * work blocked); at any later time a due deadline ticks again, since the
 * deadline the session reports is the earliest of its timers and a blocked
 * retry can stand in front of a later hard timeout (idle, GOAWAY), which
 * the tick serves before any terminal work. */
static bool host_timer(ph_host_t *h)
{
    if (h->ticked) return false;
    uint64_t due = moq_session_next_deadline_us(h->s);
    if (due > host_now(h)) return false;
    if (h->tick_blocked && h->blocked_at == host_now(h) && h->blocked_due == due)
        return false;
    h->ticked = true;
    h->n.ticks++;
    moq_result_t rc = moq_session_tick(h->s, host_now(h));
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        h->tick_blocked = true;
        h->blocked_at = host_now(h);
        h->blocked_due = moq_session_next_deadline_us(h->s);
        h->n.ticks_blocked++;
        return false;
    }
    h->tick_blocked = false;
    if (rc != MOQ_OK) return false;
    return moq_session_next_deadline_us(h->s) != due;
}

static bool host_pass(ph_host_t *h)
{
    bool progress = false;
    ph_wire_t *w = host_wire(h);
    int peer = 1 - h->side;
    /* A faulted side feeds and polls nothing more, except to see a close it
     * asked for onto the transport. */
    if (h->fault && moq_session_state(h->s) != MOQ_SESS_CLOSED) return false;

    /* The peer closed the transport: the session learns it once, and
     * nothing this side still held can be written. */
    if (w->closed[peer] && !h->peer_close_told) {
        h->peer_close_told = true;
        (void)moq_session_on_transport_close(h->s, w->close_code[peer],
                                             host_now(h));
        host_out_drop_all(h);
        progress = true;
    }

    if (!h->fault && !h->peer_close_told && !h->wire_closed) {
        /* Held output past its deadline is settled before anything else:
         * no expired byte is written or retried, in this pass or later. */
        if (host_expire(h)) progress = true;
        if (!h->input_held && host_inputs(h)) progress = true;
        if (host_timer(h)) progress = true;
    }
    /* A closed session may or may not have a close action queued: one this
     * side asked for has, one the transport caused has not. Polling tells. */
    if (h->tagged && !h->peer_close_told && host_resets(h)) progress = true;
    if (host_actions(h)) progress = true;
    if (h->polled_empty && !h->peer_close_told && host_stop_debts(h)) progress = true;
    if (h->peer_close_told) {
        host_out_drop_all(h);
        h->claim_active = false;
    } else {
        /* Owed resets before the ordered output, so a final message sees
         * them accepted; the claimed reset after it, so its prefix written
         * in this pass counts. */
        if (h->tagged && host_owed_resets(h)) progress = true;
        if (host_flush(h)) progress = true;
        if (host_dgram_flush(h)) progress = true;
        if (h->tagged && host_claim_write(h)) progress = true;
    }
    return progress;
}

/* -- The pair -------------------------------------------------------- */

static uint32_t ph_accept_uri(void *ctx, uint32_t present,
                              moq_bytes_t authority, moq_bytes_t path_query)
{
    (void)ctx; (void)present; (void)authority; (void)path_query;
    return MOQ_URI_ACCEPT;
}

static moq_result_t ph_bind(moq_session_t *s)
{
    if (moq_session_binding_requirements(s) == 0) return MOQ_OK;
    static const uint8_t authority[] = "host.invalid";
    moq_transport_binding_t b;
    moq_transport_binding_init(&b);
    b.kind = MOQ_BINDING_RAW_QUIC;
    b.facts_known = MOQ_BINDING_FACTS_ALL;
    b.facts_supported = MOQ_BINDING_FACTS_ALL;
    if (moq_session_perspective(s) == MOQ_PERSPECTIVE_SERVER) {
        b.accept_uri = ph_accept_uri;
    } else {
        b.target_present = MOQ_BINDING_TARGET_COMPLETE;
        b.authority.data = authority;
        b.authority.len = sizeof(authority) - 1;
    }
    return moq_session_bind_transport(s, &b, sizeof(b));
}

static bool ph_create_session(ph_pair_t *p, int side, moq_version_t version,
                              const ph_options_t *o)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), &p->alloc,
                               side == 0 ? MOQ_PERSPECTIVE_CLIENT
                                         : MOQ_PERSPECTIVE_SERVER);
    cfg.version = version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    if (o && o->max_events[side]) cfg.max_events = o->max_events[side];
    if (o && o->max_actions[side]) cfg.max_actions = o->max_actions[side];
    if (o && o->max_data_streams[side])
        cfg.max_data_streams = o->max_data_streams[side];
    if (o && o->max_open_subgroups[side])
        cfg.max_open_subgroups = o->max_open_subgroups[side];
    if (o && o->idle_timeout_us[side]) cfg.idle_timeout_us = o->idle_timeout_us[side];
    if (o && o->goaway_timeout_us[side])
        cfg.goaway_timeout_us = o->goaway_timeout_us[side];
    if (moq_session_create(&cfg, p->now_us, &p->host[side].s) != MOQ_OK)
        return false;
    if (ph_bind(p->host[side].s) != MOQ_OK) return false;
    if (o && o->tagged[side]) {
        if (moq_session_enable_tagged_output(p->host[side].s) != MOQ_OK) return false;
        p->host[side].tagged = true;
    }
    return true;
}

ph_pair_t *ph_pair_create(moq_version_t version, const ph_options_t *options)
{
    ph_pair_t *p = (ph_pair_t *)calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->now_us = 1;
    p->alloc.ctx = &p->native;
    p->alloc.alloc = ph_alloc;
    p->alloc.realloc = ph_realloc;
    p->alloc.free = ph_free;
    if (options) {
        p->wire.write_limit = options->write_limit;
        p->wire.read_limit = options->read_limit;
        p->wire.drop_after_stop = options->drop_after_stop;
    }
    for (int side = 0; side < 2; side++) {
        ph_host_t *h = &p->host[side];
        h->pair = p;
        h->side = side;
        h->control_bidi = h->control_out = h->control_in = -1;
        h->claim_local = -1;
        h->refuse_local = -1;
        h->cur_deadline = UINT64_MAX;
        h->per_stream = options && options->per_stream_order[side];
    }
    bool ok = ph_create_session(p, 0, version, options) &&
              ph_create_session(p, 1, version, options);
    if (ok) {
        /* The profile this host follows is the one the sessions report
         * speaking. */
        moq_version_t spoken = moq_session_version(p->host[0].s);
        p->profile = ph_profile_for(spoken);
        ok = p->profile != NULL && moq_session_version(p->host[1].s) == spoken;
    }
    if (ok) ok = moq_session_start(p->host[0].s, p->now_us) == MOQ_OK;
    /* Where each side sends control on its own stream, both begin setup.
     * With one bidirectional control stream only the client starts it, and
     * the server answers. */
    if (ok && !p->profile->bidi_control)
        ok = moq_session_start(p->host[1].s, p->now_us) == MOQ_OK;
    if (!ok) {
        (void)ph_pair_destroy(p);
        return NULL;
    }
    return p;
}

size_t ph_pair_destroy(ph_pair_t *p)
{
    if (!p) return 0;
    for (int side = 0; side < 2; side++) {
        host_out_drop_all(&p->host[side]);
        if (p->host[side].s) moq_session_destroy(p->host[side].s);
    }
    size_t left = p->native.blocks + p->copies;
    free(p);
    return left;
}

moq_session_t *ph_session(ph_pair_t *p, int side) { return p->host[side].s; }
const ph_counters_t *ph_counters(const ph_pair_t *p, int side)
{
    return &p->host[side].n;
}
int ph_fault(const ph_pair_t *p, int side) { return p->host[side].fault; }
uint64_t ph_now(const ph_pair_t *p) { return p->now_us; }

/* A new wake for both sides: each may tick once more. */
static void ph_begin_wake(ph_pair_t *p)
{
    for (int side = 0; side < 2; side++) p->host[side].ticked = false;
}

/* One drain pass of both sides; true when either moved. */
static bool ph_pass(ph_pair_t *p)
{
    bool a = host_pass(&p->host[0]);
    bool b = host_pass(&p->host[1]);
    return a || b;
}

bool ph_run(ph_pair_t *p, unsigned max_passes)
{
    for (unsigned pass = 0; pass < max_passes; pass++) {
        p->now_us++;
        ph_begin_wake(p);
        if (!ph_pass(p)) return true;
    }
    return false;
}

bool ph_wake(ph_pair_t *p, uint64_t at_us, unsigned max_passes)
{
    if (at_us > p->now_us) p->now_us = at_us;
    ph_begin_wake(p);
    for (unsigned pass = 0; pass < max_passes; pass++)
        if (!ph_pass(p)) return true;
    return false;
}

size_t ph_held_outputs(const ph_pair_t *p, int side)
{
    return p->host[side].out_count + p->host[side].dg_count;
}

uint64_t ph_next_held_deadline(const ph_pair_t *p, int side)
{
    const ph_host_t *h = &p->host[side];
    uint64_t due = UINT64_MAX;
    for (size_t k = 0; k < h->out_count; k++) {
        const ph_out_t *o = &h->out[(h->out_head + k) % PH_OUT_SLOTS];
        if (host_out_can_expire(h, o) && o->deadline < due) due = o->deadline;
    }
    for (size_t k = 0; k < h->dg_count; k++)
        if (h->dg[k].deadline < due) due = h->dg[k].deadline;
    return due;
}

void ph_set_write_refusal(ph_pair_t *p, int side, unsigned what)
{
    p->wire.refuse[side] = what & (PH_REFUSE_STREAM_BYTES | PH_REFUSE_DATAGRAMS);
}

bool ph_bidi_control(const ph_pair_t *p) { return p->profile->bidi_control; }
bool ph_uses_request_streams(const ph_pair_t *p)
{
    return p->profile->request_streams;
}
const moq_alloc_t *ph_allocator(const ph_pair_t *p) { return &p->alloc; }

void ph_set_fin_hold(ph_pair_t *p, int side, bool held)
{
    p->wire.fin_hold[side] = held;
    if (held) return;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        ph_pipe_t *pipe = &p->wire.streams[i].dir[side];
        if (!p->wire.streams[i].used || !pipe->fin_held) continue;
        pipe->fin_held = false;
        if (!pipe->reset) pipe->fin = true;
    }
}

bool ph_peer_opens_uni(ph_pair_t *p, int side, const uint8_t *data,
                       size_t len, int end, uint64_t code)
{
    if (len > PH_PIPE_CAP || (len && !data)) return false;
    int opener = 1 - side;
    int wire = wire_open(&p->wire, opener, false);
    if (wire < 0) return false;
    ph_pipe_t *pipe = &p->wire.streams[wire].dir[opener];
    if (len) memcpy(pipe->buf, data, len);
    pipe->len = len;
    /* The reset keeps the bytes as its reliable part, so the reader sees
     * them first and the reset after them. */
    if (end == PH_END_FIN) pipe->fin = true;
    else if (end == PH_END_RESET) wire_reset(&p->wire, wire, opener, code, len);
    return true;
}

bool ph_peer_stops_request(ph_pair_t *p, int side, moq_stream_ref_t ref,
                           uint64_t code)
{
    const ph_host_t *h = &p->host[side];
    int index = host_find_ref(h, ref, PH_ROLE_REQUEST, PH_ROLE_REQUEST);
    if (index < 0) return false;
    /* The peer reads what `side` writes on the stream and stops it. */
    wire_stop(&p->wire, h->local[index].wire, 1 - side, code);
    return true;
}

unsigned ph_transport_resets(const ph_pair_t *p, int side, unsigned *repeats)
{
    if (repeats) *repeats = p->wire.reset_repeats[side];
    return p->wire.resets[side];
}

size_t ph_local_data_refs(const ph_pair_t *p, int side, moq_stream_ref_t *refs,
                          size_t cap)
{
    const ph_host_t *h = &p->host[side];
    size_t n = 0;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        const ph_local_t *l = &h->local[i];
        if (!l->used || l->role != PH_ROLE_DATA_OUT) continue;
        if (n < cap) refs[n] = l->ref;
        n++;
    }
    return n;
}

uint64_t ph_last_reset_code(const ph_pair_t *p, int side)
{
    return p->wire.last_reset_code[side];
}

void ph_refuse_resets(ph_pair_t *p, int side, unsigned count)
{
    p->host[side].refuse_resets = count;
}

bool ph_peer_stops_served_request(ph_pair_t *p, int side, uint64_t code)
{
    const ph_host_t *h = &p->host[side];
    int found = -1;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        const ph_local_t *l = &h->local[i];
        if (!l->used || l->role != PH_ROLE_REQUEST || !l->peer_opened) continue;
        if (found >= 0) return false;
        found = i;
    }
    if (found < 0) return false;
    wire_stop(&p->wire, h->local[found].wire, 1 - side, code);
    return true;
}

bool ph_peer_stops_data(ph_pair_t *p, int side, moq_stream_ref_t ref,
                        uint64_t code)
{
    const ph_host_t *h = &p->host[side];
    int index = host_find_ref(h, ref, PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
    if (index < 0) return false;
    wire_stop(&p->wire, h->local[index].wire, 1 - side, code);
    return true;
}

bool ph_transport_closed(const ph_pair_t *p, int side, uint64_t *out_code)
{
    if (!p->wire.closed[side]) return false;
    if (out_code) *out_code = p->wire.close_code[side];
    return true;
}

void ph_set_write_limit(ph_pair_t *p, size_t limit) { p->wire.write_limit = limit; }
void ph_set_read_limit(ph_pair_t *p, size_t limit) { p->wire.read_limit = limit; }

static bool ph_only(const ph_pair_t *p, int side, ph_role_t role, bool by_in,
                    moq_stream_ref_t *out_ref)
{
    const ph_host_t *h = &p->host[side];
    int found = -1;
    for (int i = 0; i < PH_MAX_STREAMS; i++) {
        const ph_local_t *l = &h->local[i];
        if (!l->used || l->role != role) continue;
        if (by_in ? l->in_done : (l->in_done && l->out_done)) continue;
        if (role == PH_ROLE_REQUEST && l->peer_opened) continue;
        if (found >= 0) return false;
        found = i;
    }
    if (found < 0) return false;
    *out_ref = h->local[found].ref;
    return true;
}

bool ph_only_open_peer_data_stream(const ph_pair_t *p, int side,
                                   moq_stream_ref_t *out_ref)
{
    return ph_only(p, side, PH_ROLE_DATA_IN, true, out_ref);
}

bool ph_only_open_local_data(const ph_pair_t *p, int side,
                             moq_stream_ref_t *out_ref)
{
    return ph_only(p, side, PH_ROLE_DATA_OUT, false, out_ref);
}

bool ph_only_open_local_request(const ph_pair_t *p, int side,
                                moq_stream_ref_t *out_ref)
{
    return ph_only(p, side, PH_ROLE_REQUEST, false, out_ref);
}

/* The transport stream the side's peer sends control on. */
static int ph_peer_control_wire(const ph_pair_t *p, int side)
{
    const ph_host_t *h = &p->host[side];
    int index = p->profile->bidi_control ? h->control_bidi : h->control_in;
    return index < 0 ? -1 : h->local[index].wire;
}

bool ph_peer_finishes_control(ph_pair_t *p, int side)
{
    int wire = ph_peer_control_wire(p, side);
    if (wire < 0) return false;
    wire_fin(&p->wire, wire, 1 - side);
    return true;
}

bool ph_peer_resets_control(ph_pair_t *p, int side, uint64_t code)
{
    int wire = ph_peer_control_wire(p, side);
    if (wire < 0) return false;
    wire_reset(&p->wire, wire, 1 - side, code, 0);
    return true;
}

bool ph_refuse_data_stream(ph_pair_t *p, int side, moq_stream_ref_t ref)
{
    ph_host_t *h = &p->host[side];
    if (ref._v == 0) {
        h->refuse_local = -1;
        h->refuse_next = true;
        return true;
    }
    int index = host_find_ref(h, ref, PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
    if (index < 0) return false;
    h->refuse_local = index;
    h->refuse_next = false;
    return true;
}

void ph_lift_data_refusal(ph_pair_t *p, int side)
{
    p->host[side].refuse_local = -1;
    p->host[side].refuse_next = false;
    p->host[side].refuse_allow = 0;
}

void ph_set_poll_budget(ph_pair_t *p, int side, unsigned actions)
{
    p->host[side].poll_budget = actions;
}

void ph_set_refusal_allowance(ph_pair_t *p, int side, uint64_t bytes)
{
    p->host[side].refuse_allow = bytes;
}

bool ph_refuse_request_stream(ph_pair_t *p, int side, moq_stream_ref_t ref)
{
    ph_host_t *h = &p->host[side];
    int index = host_find_ref(h, ref, PH_ROLE_REQUEST, PH_ROLE_REQUEST);
    if (index < 0) return false;
    h->refuse_local = index;
    h->refuse_next = false;
    return true;
}

bool ph_redeliver_stop(ph_pair_t *p, int side, moq_stream_ref_t ref, uint64_t code)
{
    ph_host_t *h = &p->host[side];
    int index = host_find_ref(h, ref, PH_ROLE_DATA_OUT, PH_ROLE_DATA_OUT);
    if (index < 0) index = host_find_ref(h, ref, PH_ROLE_REQUEST, PH_ROLE_REQUEST);
    if (index < 0) return false;
    (void)host_peer_stop(h, index, code);
    return true;
}

/* The trace of what `side` wrote on its stream `index` (into local). */
static void ph_trace_of(const ph_pair_t *p, int side, int index,
                        ph_stream_trace_t *out)
{
    const ph_host_t *h = &p->host[side];
    *out = p->wire.streams[h->local[index].wire].dir[side].trace;
}

bool ph_data_trace(const ph_pair_t *p, int side, moq_stream_ref_t ref,
                   ph_stream_trace_t *out)
{
    int index = host_find_ref(&p->host[side], ref, PH_ROLE_DATA_OUT,
                              PH_ROLE_DATA_OUT);
    if (index < 0) return false;
    ph_trace_of(p, side, index, out);
    return true;
}

bool ph_request_trace(const ph_pair_t *p, int side, moq_stream_ref_t ref,
                      ph_stream_trace_t *out)
{
    int index = host_find_ref(&p->host[side], ref, PH_ROLE_REQUEST,
                              PH_ROLE_REQUEST);
    if (index < 0) return false;
    ph_trace_of(p, side, index, out);
    return true;
}

size_t ph_served_request_refs(const ph_pair_t *p, int side,
                              moq_stream_ref_t *refs, size_t cap)
{
    /* Peer streams get their refs in the order they arrive. */
    const ph_host_t *h = &p->host[side];
    size_t n = 0;
    for (uint64_t next = 0; next < h->next_peer_ref; next++) {
        for (int i = 0; i < PH_MAX_STREAMS; i++) {
            const ph_local_t *l = &h->local[i];
            if (!l->used || l->role != PH_ROLE_REQUEST || !l->peer_opened ||
                l->ref._v != (PH_PEER_REF_BASE | next))
                continue;
            if (n < cap) refs[n] = l->ref;
            n++;
        }
    }
    return n;
}

bool ph_peer_writes_served_request(ph_pair_t *p, int side, const uint8_t *data,
                                   size_t len)
{
    moq_stream_ref_t ref;
    if (ph_served_request_refs(p, side, &ref, 1) != 1 || (len && !data))
        return false;
    const ph_host_t *h = &p->host[side];
    int index = host_find_ref(h, ref, PH_ROLE_REQUEST, PH_ROLE_REQUEST);
    ph_pipe_t *pipe = &p->wire.streams[h->local[index].wire].dir[1 - side];
    if (pipe->fin || pipe->fin_held || pipe->reset || pipe->dropped ||
        len > PH_PIPE_CAP - pipe->len)
        return false;
    if (len) memcpy(pipe->buf + pipe->len, data, len);
    pipe->len += len;
    return true;
}

void ph_set_input_hold(ph_pair_t *p, int side, bool held)
{
    p->host[side].input_held = held;
}
