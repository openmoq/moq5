#include "bind_internal.h"
#include "metadata_internal.h"
#include "intents_internal.h"
#include "request_answers_internal.h"

moqr_result_t
moqr_bind_meta_retain(moqr_bind_t *b, const moqr_bind_meta_ref_t *ref,
                      moqr_bind_meta_ref_t *out)
{
    if (b == NULL || ref == NULL || out == NULL || out == ref ||
        out->storage != NULL) {
        return MOQR_ERR_INVAL;
    }
    if (ref->storage != NULL) {
        if (ref->storage->bind != b || ref->storage->refs == SIZE_MAX) {
            return MOQR_ERR_INVAL;
        }
        ref->storage->refs++;
    }
    *out = *ref;
    return MOQR_OK;
}


void
moqr_bind_meta_release(moqr_bind_t *b, moqr_bind_meta_ref_t *ref)
{
    if (b == NULL || ref == NULL) return;
    struct moqr_bind_meta_blob *p = ref->storage;
    if (p != NULL && p->bind != b) return;
    if (p != NULL && --p->refs == 0) {
        size_t n = p->allocation;
        b->metadata_bytes -= n;
        b->alloc.free(p, n, b->alloc.ctx);
    }
    memset(ref, 0, sizeof(*ref));
}


moqr_result_t
moqr_bind_meta_retain_executing(moqr_bind_t *b, const moqr_intent_t *it,
                               moqr_bind_meta_ref_t *out)
{
    if (b == NULL || it == NULL || b->metadata_executing_intent != it ||
        b->metadata_executing == NULL || it->kind != MOQR_INTENT_ACCEPT_SUB) {
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_bind_meta_retain(b, b->metadata_executing, out);
}


moqr_result_t
moqr_bind_meta_latest(moqr_bind_t *b, moqr_track_t track,
                      moqr_bind_meta_ref_t *out)
{
    if (b == NULL || out == NULL || out->storage != NULL ||
        moq_handle_pool_tag(track._opaque) != MOQR_HANDLE_POOL_TRACK) {
        return MOQR_ERR_INVAL;
    }
    uint32_t slot = moq_handle_slot(track._opaque);
    if (slot >= b->metadata_cap) return MOQR_ERR_STALE_HANDLE;
    b_meta_entry_t *e = &b->metadata[slot];
    if (!moqr_track_eq(e->track, track)) {
        memset(out, 0, sizeof(*out));
        return MOQR_OK;
    }
    return moqr_bind_meta_retain(b, &e->latest, out);
}


moqr_result_t
moqr_bind_meta_set_observer(moqr_bind_t *b, moqr_bind_meta_changed_fn changed,
                            void *ctx)
{
    if (b == NULL) return MOQR_ERR_INVAL;
    if (b->in_pump || b->intent_drain_active || b->metadata_notifying ||
        b->metadata_changed != NULL) return MOQR_ERR_WRONG_STATE;
    b->metadata_changed = changed;
    b->metadata_changed_ctx = ctx;
    return MOQR_OK;
}


void
bind_meta_sweep(moqr_bind_t *b)
{
    for (uint32_t i = 0; i < b->metadata_cap; i++) {
        b_meta_entry_t *e = &b->metadata[i];
        if (moqr_track_eq(e->track, MOQR_TRACK_INVALID)) continue;
        uint64_t generation;
        if (moqr_core_track_current_generation(b->core, e->track, &generation) !=
            MOQR_ERR_STALE_HANDLE) continue;
        /* Drop only the directory ref. Reply/manager pins keep their charge. */
        moqr_bind_meta_release(b, &e->latest);
        memset(e, 0, sizeof(*e));
    }
}


moqr_result_t
bind_meta_copy(moqr_bind_t *b, const moqr_bind_meta_ref_t *input,
               moqr_bind_meta_ref_t *out)
{
    *out = *input;
    out->storage = NULL;
    out->revision = 0;
    out->bytes = (moq_bytes_t){ NULL, 0 };
    out->state = MOQR_BIND_META_UNAVAILABLE;
    if (input->state == MOQR_BIND_META_UNAVAILABLE) return MOQR_OK;
    if ((input->state != MOQR_BIND_META_EMPTY &&
         input->state != MOQR_BIND_META_PRESENT) ||
        (input->state == MOQR_BIND_META_EMPTY && input->bytes.len != 0) ||
        (input->state == MOQR_BIND_META_PRESENT &&
         (input->bytes.len == 0 || input->bytes.data == NULL))) {
        return MOQR_ERR_INVAL;
    }
    if (input->bytes.len > MOQR_BIND_META_SET_MAX) return MOQR_ERR_CAPACITY;
    bind_meta_sweep(b);
    size_t n = sizeof(struct moqr_bind_meta_blob) + input->bytes.len;
    if (n < input->bytes.len || n > MOQR_BIND_META_BYTES_MAX ||
        b->metadata_bytes > MOQR_BIND_META_BYTES_MAX - n) {
        return MOQR_ERR_CAPACITY;
    }
    b->metadata_bytes += n;
    struct moqr_bind_meta_blob *p = b->alloc.alloc(n, b->alloc.ctx);
    if (p == NULL) {
        b->metadata_bytes -= n;
        return MOQR_ERR_NOMEM;
    }
    p->bind = b;
    p->refs = 1;
    p->allocation = n;
    if (input->bytes.len != 0) memcpy(p->data, input->bytes.data, input->bytes.len);
    out->state = input->state;
    out->storage = p;
    out->bytes = (moq_bytes_t){ p->data, input->bytes.len };
    return MOQR_OK;
}


static moqr_result_t
bind_meta_track_current(moqr_bind_t *b, moqr_track_t track, uint64_t gen)
{
    bool has_largest;
    uint64_t g, o;
    return moqr_core_track_largest(b->core, track, gen, &has_largest, &g, &o);
}


/* Takes the allocation already reserved by ingress, without allocating while
 * dispatching an ACCEPT_PUBLISH or retaining a routed reply. */
/* The core enforces the source's MAX_CACHE_DURATION on every Object of the
 * track it later starts downstream. */
static void
bind_meta_apply_cache_duration(moqr_bind_t *b, moqr_track_t track,
                               const moqr_bind_meta_ref_t *meta)
{
    /* MAX_CACHE_DURATION, in the recorded set's own draft (draft-22 §10.3,
     * draft-18 §12.3, draft-16 §11.1.1). */
    bool has = false;
    uint64_t ms = 0;
    if (meta->state == MOQR_BIND_META_PRESENT && meta->bytes.len > 0) {
        has = moq_core_internal_track_max_cache_duration(
            meta->wire_version, meta->bytes.data, meta->bytes.len, &ms);
    }
    (void)moqr_core_internal_set_max_cache_duration(b->core, track, has, ms);
}


static void
bind_meta_commit(moqr_bind_t *b, moqr_track_t track, uint64_t gen,
                 moqr_bind_meta_ref_t *owned)
{
    uint32_t slot = moq_handle_slot(track._opaque);
    b_meta_entry_t *e = &b->metadata[slot];
    moqr_bind_meta_release(b, &e->latest);
    e->track = track;
    e->track_gen = gen;
    if (b->metadata_revision >= UINT64_MAX - 1u) {
        /* The final revision is permanently unavailable, never a present
         * set that a later invalidation could not supersede. */
        moqr_bind_meta_release(b, owned);
        b->metadata_revision = UINT64_MAX;
    } else {
        b->metadata_revision++;
    }
    owned->revision = b->metadata_revision;
    e->latest = *owned;
    memset(owned, 0, sizeof(*owned));
    bind_meta_apply_cache_duration(b, track, &e->latest);
    if (b->metadata_changed != NULL) {
        b->metadata_notifying = true;
        b->metadata_changed(b->metadata_changed_ctx, track, gen, &e->latest);
        b->metadata_notifying = false;
    }
}


moqr_result_t
moqr_bind_meta_replace(moqr_bind_t *b, moqr_track_t track, uint64_t gen,
                       const moqr_bind_meta_ref_t *input,
                       uint64_t now_us, uint64_t *out_revision)
{
    if (out_revision != NULL) *out_revision = 0;
    if (b == NULL || input == NULL) return MOQR_ERR_INVAL;
    if (b->intent_drain_active || b->metadata_notifying) return MOQR_ERR_WRONG_STATE;
    moqr_result_t rc = bind_meta_track_current(b, track, gen);
    if (rc != MOQR_OK) return rc;
    bool managed = false;
    rc = moqr_core_track_metadata_managed(b->core, track, &managed);
    if (rc != MOQR_OK) return rc;
    if (!managed) return MOQR_ERR_WRONG_STATE;
    moqr_bind_meta_ref_t owned;
    memset(&owned, 0, sizeof(owned));
    rc = bind_meta_copy(b, input, &owned);
    if (rc == MOQR_ERR_INVAL) return rc;
    /* Copy first: the drain can advance sessions and invalidate event scratch.
     * Replies already owed retain A before the directory switches to B. */
    (void)bind_execute_intents(b, now_us);
    moqr_result_t live = bind_meta_track_current(b, track, gen);
    if (live != MOQR_OK) {
        moqr_bind_meta_release(b, &owned);
        return live;
    }
    bind_meta_commit(b, track, gen, &owned);
    if (out_revision != NULL) {
        *out_revision = b->metadata[moq_handle_slot(track._opaque)].latest.revision;
    }
    return rc;
}


bool
bind_meta_publish_count(const moqr_core_limits_t *lim,
                        const moqr_bind_limits_t *bl, uint32_t *out)
{
    uint64_t n = (uint64_t)lim->max_parked + bl->pending_cap + BIND_PUMP_EVENT_POLL;
    if (n > UINT32_MAX || n > SIZE_MAX / sizeof(b_meta_publish_t) ||
        (lim->max_tracks != 0 &&
         sizeof(b_meta_entry_t) > SIZE_MAX / lim->max_tracks) ||
        (bl->pending_cap != 0 &&
         sizeof(moqr_bind_meta_ref_t) > SIZE_MAX / bl->pending_cap)) {
        return false;
    }
    *out = (uint32_t)n;
    return true;
}


b_meta_publish_t *
bind_meta_pub_find(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state != 0 && moqr_binding_eq(p->binding, cn->binding) &&
            p->pub == pub) return p;
    }
    return NULL;
}


b_meta_publish_t *
bind_meta_pub_reserve(moqr_bind_t *b, const b_conn_t *cn, uint64_t pub)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state == 0) {
            p->state = B_META_PUB_CORE;
            p->binding = cn->binding;
            p->cookie = (uint64_t)(cn - b->conns) + 1u;
            p->pub = pub;
            return p;
        }
    }
    return NULL;   /* event polling reserves headroom before consuming input */
}


void
bind_meta_pub_release(moqr_bind_t *b, b_meta_publish_t *p)
{
    moqr_bind_meta_release(b, &p->snapshot);
    memset(p, 0, sizeof(*p));
}


/* Refuse the PUBLISH a metadata stage belongs to: the stage is released and
 * the refusal goes through the connection's held-terminal table like every
 * other, so one the session cannot take yet is held, never lost. */
void
bind_meta_pub_refuse_ex(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                        uint64_t code, bool can_retry, uint64_t retry_ms,
                        uint64_t now_us)
{
    uint64_t handle = p->pub;
    bind_meta_pub_release(b, p);
    bind_refuse(b, cn, MOQR_AUTH_PUBLISH, handle, code, can_retry, retry_ms,
                now_us);
}


void
bind_meta_pub_refuse(moqr_bind_t *b, b_conn_t *cn, b_meta_publish_t *p,
                     uint64_t code, uint64_t now_us)
{
    bind_meta_pub_refuse_ex(b, cn, p, code, false, 0, now_us);
}


void
bind_meta_pub_purge(moqr_bind_t *b, const b_conn_t *cn)
{
    for (uint32_t i = 0; i < b->metadata_publish_cap; i++) {
        b_meta_publish_t *p = &b->metadata_publishes[i];
        if (p->state != 0 && moqr_binding_eq(p->binding, cn->binding)) {
            bind_meta_pub_release(b, p);
        }
    }
}


moqr_bind_meta_ref_t
bind_meta_input(const b_conn_t *cn, uint64_t handle, uint64_t gen,
                moq_bytes_t bytes)
{
    moqr_bind_meta_ref_t input;
    memset(&input, 0, sizeof(input));
    input.state = bytes.len == 0 ? MOQR_BIND_META_EMPTY : MOQR_BIND_META_PRESENT;
    input.wire_version = moq_session_version(cn->session);
    input.source_binding = cn->binding;
    input.source_handle = handle;
    input.source_generation = gen;
    input.bytes = bytes;
    return input;
}

/* The Key-Value-Pair integer codec `version` writes: QUIC varints in
 * draft-16, vi64 in draft-18 and draft-22; UNKNOWN for any other. */
static uint8_t
bind_version_codec(moq_version_t version)
{
    if (version == MOQ_VERSION_DRAFT_16) return MOQR_PROPS_CODEC_QUIC_VARINT;
    if (version == MOQ_VERSION_DRAFT_18 || version == MOQ_VERSION_DRAFT_22)
        return MOQR_PROPS_CODEC_VI64;
    return MOQR_PROPS_CODEC_UNKNOWN;
}


void
bind_track_props_release(moqr_bind_t *b, b_props_tmp_t *tmp)
{
    if (tmp->buf != NULL) b->alloc.free(tmp->buf, tmp->len, b->alloc.ctx);
    tmp->buf = NULL;
    tmp->len = 0;
}


/* A Track Property set read in `src` as `dst` writes it, for one session
 * call that borrows it: `in` itself when both codecs read it alike (or it is
 * empty), else a converted copy in *tmp that the caller releases after the
 * call. `in` is never changed. False, with nothing held and *out empty, when
 * the set is not representable in `dst`, does not parse in `src`, has no
 * stated codec, or the copy cannot be allocated. */
bool
bind_track_props_out(moqr_bind_t *b, uint8_t src, uint8_t dst, moq_bytes_t in,
                     moq_bytes_t *out, b_props_tmp_t *tmp)
{
    tmp->buf = NULL;
    tmp->len = 0;
    *out = in;
    if (in.len == 0) {
        *out = (moq_bytes_t){ NULL, 0 };
        return true;
    }
    size_t need = 0;
    moqr_props_conv_t r = moqr_core_internal_props_convert(src, dst, in.data, in.len, NULL, 0,
                                                           &need);
    if (r == MOQR_PROPS_CONV_SAME) return true;
    *out = (moq_bytes_t){ NULL, 0 };
    if (r != MOQR_PROPS_CONV_CONVERTED || need == 0) {
        FWD_DBG(b, track_props_refused);
        return false;
    }
    uint8_t *buf = b->alloc.alloc(need, b->alloc.ctx);
    size_t got = 0;
    if (buf == NULL ||
        moqr_core_internal_props_convert(src, dst, in.data, in.len, buf, need, &got) !=
            MOQR_PROPS_CONV_CONVERTED ||
        got != need) {
        if (buf != NULL) b->alloc.free(buf, need, b->alloc.ctx);
        FWD_DBG(b, track_props_refused);
        return false;
    }
    FWD_DBG(b, track_props_converted);
    tmp->buf = buf;
    tmp->len = need;
    *out = (moq_bytes_t){ buf, need };
    return true;
}


/* The pinned Track Property set as `version` carries it, through
 * bind_track_props_out from the draft that stated it: the pin stays the
 * exact revision it names, whatever the destination reads. An empty set
 * has no representation to convert; anything but EMPTY or PRESENT (an
 * ACK, UNAVAILABLE) cannot be stated. */
bool
bind_meta_out(moqr_bind_t *b, const moqr_bind_meta_ref_t *pin, moq_version_t version,
              moq_bytes_t *out, b_props_tmp_t *tmp)
{
    tmp->buf = NULL;
    tmp->len = 0;
    *out = (moq_bytes_t){ NULL, 0 };
    if (pin == NULL) return false;
    if (pin->state == MOQR_BIND_META_EMPTY) return true;
    if (pin->state != MOQR_BIND_META_PRESENT) return false;
    return bind_track_props_out(b, bind_version_codec(pin->wire_version),
                                bind_version_codec(version), pin->bytes, out, tmp);
}


/* The Track Properties a reply this relay originates for an established
 * track states (TRACK_STATUS_OK, FETCH_OK; draft-22 §7.7): the track's
 * recorded set, or EMPTY when this binding does not manage the source's
 * metadata. A managed track with nothing usable recorded stays UNAVAILABLE,
 * which bind_meta_out refuses, as it refuses an ACK. */
void
bind_meta_reply_pin(moqr_bind_t *b, moqr_track_t track, moqr_bind_meta_ref_t *pin)
{
    if (moqr_bind_meta_latest(b, track, pin) != MOQR_OK) return;
    uint32_t slot = moq_handle_slot(track._opaque);
    if (slot < b->metadata_cap && moqr_track_eq(b->metadata[slot].track, track))
        return; /* A recorded obligation, including UNAVAILABLE, wins. */
    bool managed = true;
    if (moqr_core_track_metadata_managed(b->core, track, &managed) != MOQR_OK ||
        managed) return;
    pin->state = MOQR_BIND_META_EMPTY;
}


moqr_result_t
moqr_bind_meta_reply(moqr_bind_t *b, moqr_track_t track, moqr_bind_meta_ref_t *out)
{
    if (b == NULL || out == NULL) return MOQR_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    bind_meta_reply_pin(b, track, out);
    return MOQR_OK;
}


/* The properties of a FETCH_OK this relay originates from the core fetch
 * `cfetch`, retained in *pin for the call and written to *out as this
 * session carries them (bind_meta_out; *tmp released after the call). False
 * when the track's set cannot be stated on this session; the fetch is
 * refused rather than answered without them. */
bool
bind_fetch_ok_pin(moqr_bind_t *b, const b_conn_t *cn, moqr_fetch_t cfetch,
                  moqr_bind_meta_ref_t *pin, moq_bytes_t *out, b_props_tmp_t *tmp)
{
    tmp->buf = NULL;
    tmp->len = 0;
    moqr_track_t track;
    uint64_t gen;
    if (moqr_core_internal_fetch_track(b->core, cfetch, &track, &gen) != MOQR_OK)
        return false;
    bind_meta_reply_pin(b, track, pin);
    return bind_meta_out(b, pin, moq_session_version(cn->session), out, tmp);
}


/* Runs for every fresh scalar BEFORE the blocked fast path. PUBLISH input
 * was copied before parking/admission; its core ACCEPT supplies the exact
 * accepted track/source identity even when its wire response must wait. */
void
bind_meta_prepare_reply(moqr_bind_t *b, moqr_intent_t *it,
                        moqr_bind_meta_ref_t *pin)
{
    if (it->kind == MOQR_INTENT_ACCEPT_PUBLISH ||
        it->kind == MOQR_INTENT_REJECT_PUBLISH) {
        b_conn_t *cn = conn_by_cookie(b, it->binding_cookie);
        if (cn == NULL || cn->closed) return;   /* core-only/unrouted source */
        b_meta_publish_t *p = bind_meta_pub_find(b, cn, it->cookie);
        if (it->kind == MOQR_INTENT_ACCEPT_PUBLISH) {
            if (p != NULL && p->state == B_META_PUB_CORE &&
                bind_meta_track_current(b, it->track, it->track_gen) == MOQR_OK) {
                p->snapshot.source_generation = it->track_gen;
                bind_meta_commit(b, it->track, it->track_gen, &p->snapshot);
            } else {
                (void)moqr_core_upstream_lost(b->core, it->track, it->track_gen);
                it->kind = MOQR_INTENT_REJECT_PUBLISH;
                it->error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
            }
        }
        if (p != NULL) bind_meta_pub_release(b, p);
    }
    if (it->kind == MOQR_INTENT_TRACK_STATUS_OK) {
        bind_meta_reply_pin(b, it->track, pin);
        return;
    }
    if (it->kind == MOQR_INTENT_ACCEPT_SUB) {
        moqr_track_t track;
        uint64_t generation;
        if (moqr_core_sub_track(b->core, it->sub, &track, &generation) != MOQR_OK ||
            moqr_bind_meta_latest(b, track, pin) != MOQR_OK) return;
        uint32_t slot = moq_handle_slot(track._opaque);
        if (slot >= b->metadata_cap || moqr_track_eq(b->metadata[slot].track, track))
            return; /* A recorded obligation, including UNAVAILABLE, wins. */
        bool managed = true;
        moqr_sub_state_t state;
        if (moqr_core_track_metadata_managed(b->core, track, &managed) != MOQR_OK ||
            managed || moqr_core_sub_state(b->core, it->sub, &state) != MOQR_OK ||
            state != MOQR_SUB_ACTIVE) return;
        /* Fresh PENDING demand cannot ACK. An ACTIVE sub proves admission,
         * including a retained direct identity's WARM rejoin. Do not put this
         * exception in generic latest lookup, which also serves pending demand. */
        pin->state = MOQR_BIND_META_EMPTY;
        pin->source_generation = generation;
    }
}


bool
bind_try_pinned(moqr_bind_t *b, const moqr_intent_t *it, uint64_t now_us,
                uint8_t mode, bool *parked, moqr_intent_t *defer,
                const moqr_bind_meta_ref_t *pin)
{
    b->metadata_executing_intent = it;
    b->metadata_executing = pin;
    bool done = bind_try_intent(b, it, now_us, mode, parked, defer);
    b->metadata_executing_intent = NULL;
    b->metadata_executing = NULL;
    return done;
}


/* -- Object property conversion -------------------------------------------- */

/* The Object Properties `props`, stated in `codec`, as `cn` writes them: the
 * same buffer when its draft reads them unchanged (an empty block, or the
 * same codec), else a converted copy in *owned: an ordinary owning rcbuf
 * (moq_rcbuf_create, the allocator held by value under the rcbuf lifetime
 * contract), so a session action that keeps it outlives nothing of the
 * binding. The caller drops its reference once the write call returns; the
 * conversion is redone on a retry after MOQ_ERR_WOULD_BLOCK, bounded
 * transient work. MOQ_ERR_UNSUPPORTED when the block cannot be
 * carried to this draft (an integer it cannot represent, Immutable
 * Properties that would read differently, or a nonempty block of unknown
 * codec), MOQ_ERR_PROTO when the block does not parse in its own codec (a
 * malformed Track the ingest did not catch), MOQ_ERR_NOMEM when the copy
 * cannot be made: the caller fails this request through its own local
 * failure path. */
moq_result_t
bind_props_out(moqr_bind_t *b, const b_conn_t *cn, const moq_rcbuf_t *props,
               uint8_t codec, moq_rcbuf_t **out, moq_rcbuf_t **owned)
{
    *owned = NULL;
    *out = (moq_rcbuf_t *)(void *)props;
    if (props == NULL || moq_rcbuf_len(props) == 0) return MOQ_OK;
    const uint8_t *in = moq_rcbuf_data(props);
    size_t len = moq_rcbuf_len(props), need = 0;
    uint8_t dst = bind_props_codec(cn);
    moqr_props_conv_t r = moqr_core_internal_props_convert(codec, dst, in, len, NULL, 0,
                                                           &need);
    if (r == MOQR_PROPS_CONV_SAME) return MOQ_OK;
    if (r == MOQR_PROPS_CONV_MALFORMED) {
        FWD_DBG(b, props_malformed);
        return MOQ_ERR_PROTO;
    }
    if (r != MOQR_PROPS_CONV_CONVERTED || need == 0) {
        FWD_DBG(b, props_refused);
        return MOQ_ERR_UNSUPPORTED;
    }
    uint8_t *buf = b->alloc.alloc(need, b->alloc.ctx);
    if (buf == NULL) {
        FWD_DBG(b, props_refused);
        return MOQ_ERR_NOMEM;
    }
    size_t got = 0;
    moq_rcbuf_t *rb = NULL;
    bool ok = moqr_core_internal_props_convert(codec, dst, in, len, buf, need, &got) ==
                  MOQR_PROPS_CONV_CONVERTED && got == need &&
              moq_rcbuf_create(&b->alloc, buf, need, &rb) == MOQ_OK;
    b->alloc.free(buf, need, b->alloc.ctx);   /* the rcbuf holds its own copy */
    if (!ok) {
        FWD_DBG(b, props_refused);
        return MOQ_ERR_NOMEM;
    }
    FWD_DBG(b, props_converted);
    *out = rb;
    *owned = rb;
    return MOQ_OK;
}
