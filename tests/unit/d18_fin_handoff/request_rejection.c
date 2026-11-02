#include "test_cases.h"

/* -- Pre-commit reject branches (Axis 3) ------------------------------ */

/* These branches answer before any destination owner exists, so they install no
 * handoff marker. The FIN they consult belongs to the generic staging owner
 * that carried the request: `handle_request_stream_bytes()` latches an arriving
 * FIN into `e->rs.recv_fin`, retains it across a refused call, and hands that
 * latch -- not the raw call flag -- to the profile, so an empty re-feed still
 * knows the peer's send half is closed. Every PUBLISH branch shares one
 * observed-FIN decision, `reject_drain = d->endpoint.has_stream_ref &&
 * !request_fin_observed` (`session_core_on_publish`), and both
 * PUBLISH_NAMESPACE branches share its counterpart beside `req_stream`
 * (`session_core_on_publish_namespace`), so an observed FIN spends no drain
 * reference while wire output and staging retirement still follow stream
 * correlation. */

static void srv_cfg_init(moq_session_cfg_t *cfg)
{
    moq_session_cfg_init_sized(cfg, sizeof(*cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg->version = MOQ_VERSION_DRAFT_18;
}


/* A draft-18 server established through the ordinary SETUP exchange with the
 * caller's caps. Establishment is asserted, so a fixture cannot silently drive
 * a session that never reached ESTABLISHED. */
static moq_session_t *make_session_srv(const moq_session_cfg_t *cfg)
{
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(cfg, 0, &s), (int)MOQ_OK);
    if (!s) return NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0),
        (int)MOQ_OK);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    return s;
}


moq_session_t *make_session_default(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    return make_session_srv(&cfg);
}


static moq_session_t *make_session_pub_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_publishes = 1;
    return make_session_srv(&cfg);
}


static moq_session_t *make_session_hist_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_track_history_records = 1;
    return make_session_srv(&cfg);
}


/* One event slot, so a single queued sentinel refuses the next handler that
 * needs to surface anything. */
static moq_session_t *make_session_events_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_events = 1;
    return make_session_srv(&cfg);
}


static moq_session_t *make_session_ann_cap_1(void)
{
    moq_session_cfg_t cfg;
    srv_cfg_init(&cfg);
    cfg.max_announcements = 1;
    return make_session_srv(&cfg);
}


/* -- Request encoders ------------------------------------------------ */

static size_t enc_publish_full(uint8_t *buf, size_t cap, uint64_t rid,
                               uint64_t alias, const char *name,
                               const moq_d18_msg_params_t *params)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_publish_t p = { 0 };
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    p.request_id = rid;
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    p.track_alias = alias;
    if (params) p.params = *params;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_publish(&w, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


static size_t enc_publish_plain(uint8_t *buf, size_t cap, uint64_t rid,
                                uint64_t alias, const char *name)
{
    return enc_publish_full(buf, cap, rid, alias, name, NULL);
}


/* One unknown USE_ALIAS, which the token walk rejects with 0x17 and no reason
 * phrase (session_auth.c:395). */
size_t enc_publish_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_d18_msg_params_t params = { 0 };
    params.auth_token_count = 1;
    params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    params.auth_tokens[0].alias = 99;
    return enc_publish_full(buf, cap, 0, 7, "v", &params);
}


/* The encoder refuses a mandatory Track Property, so a valid KVP (type 0x4000,
 * value 0) is appended to an encoded PUBLISH payload and the message reframed.
 * The envelope is decoded rather than assuming a header width, and the type is
 * re-emitted from the decoded value. An ODD property type would be malformed
 * rather than mandatory-unknown, and would reach a different terminal. */
static size_t enc_publish_mandatory_prop(uint8_t *buf, size_t cap)
{
    uint8_t base[192];
    size_t base_len = enc_publish_full(base, sizeof(base), 0, 7, "v", NULL);
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, base, base_len);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(env.msg_type, (uint64_t)MOQ_D18_PUBLISH);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), (size_t)0);

    uint8_t kvp[8];
    moq_buf_writer_t kw;
    moq_buf_writer_init(&kw, kvp, sizeof(kvp));
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&kw, 0x4000), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&kw, 0), (int)MOQ_OK);
    size_t kvp_len = moq_buf_writer_offset(&kw);

    uint8_t pl[224];
    MOQ_TEST_CHECK(env.payload_len + kvp_len <= sizeof(pl));
    if (env.payload_len + kvp_len > sizeof(pl)) return 0;
    memcpy(pl, env.payload, env.payload_len);
    memcpy(pl + env.payload_len, kvp, kvp_len);
    return enc_typed_envelope(buf, cap, env.msg_type, pl,
                              env.payload_len + kvp_len);
}


static size_t enc_publish_hist_target(uint8_t *buf, size_t cap)
{
    return enc_publish_plain(buf, cap, 0, 7, "target");
}


static size_t enc_publish_second(uint8_t *buf, size_t cap)
{
    return enc_publish_plain(buf, cap, 2, 8, "b");
}


size_t enc_pns_full(uint8_t *buf, size_t cap, uint64_t rid,
                    const char *part0,
                    const moq_d18_msg_params_t *params)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)part0, strlen(part0) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t empty = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_namespace(&w, rid, &ns,
                                              params ? params : &empty),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* The same unknown-USE_ALIAS shape, on the namespace request. */
static size_t enc_pns_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_d18_msg_params_t params = { 0 };
    params.auth_token_count = 1;
    params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    params.auth_tokens[0].alias = 99;
    return enc_pns_full(buf, cap, 0, "live", &params);
}


/* A namespace distinct from the one the fixture already retained. */
static size_t enc_pns_second(uint8_t *buf, size_t cap)
{
    return enc_pns_full(buf, cap, 2, "other", NULL);
}


/* -- Fixture preconditions ------------------------------------------- */

/* Every event and action a seed produces is counted by kind and required to be
 * exactly what the seed owes. Draining whatever appears would let a fixture
 * that reached a different path -- or that left a stray action behind -- seed
 * the case anyway. */
static void seed_output_exact(moq_session_t *s, uint32_t want_event_kind,
                              int want_events, const char *what)
{
    int matched = 0, other_events = 0, actions = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        if (e.kind == want_event_kind) matched++;
        else {
            other_events++;
            TXS_DIAG("TXN %s seed: unexpected event kind %u\n", what,
                     (unsigned)e.kind);
        }
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        actions++;
        TXS_DIAG("TXN %s seed: unexpected action kind %u\n", what,
                 (unsigned)a.kind);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(matched, want_events);
    MOQ_TEST_CHECK_EQ_INT(other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(actions, 0);
}


/* Occupy the only publication slot with a surfaced, unanswered request. */
static void seed_publication(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    uint8_t m[192];
    size_t n = enc_publish_plain(m, sizeof(m), 0, 7, "a");
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, m, n, false, 1),
        (int)MOQ_OK);
    seed_output_exact(s, MOQ_EVENT_PUBLISH_REQUEST, 1, "publication");
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
}


/* Occupy the only announcement slot with a surfaced, unanswered request. */
static void seed_announcement(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "seeded", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, seed_ref, m, n, false, 1),
        (int)MOQ_OK);
    seed_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, "announcement");
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
}


static moq_namespace_t hist_ns(moq_bytes_t *store)
{
    store[0] = MOQ_BYTES_LITERAL("live");
    return (moq_namespace_t){ store, 1 };
}


/* Pin the single history record on a DIFFERENT track identity, so the target
 * request finds the registry full rather than an existing record. */
static void seed_track_history(moq_session_t *s, moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    moq_bytes_t part;
    moq_namespace_t ns = hist_ns(&part);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_note_object_published(s, &ns,
                                               MOQ_BYTES_LITERAL("pinned"),
                                               1, 0),
        (int)MOQ_OK);
    /* Recording history is not application-visible: it owes no output. */
    seed_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, "track history");
}


/* The selection the reject branch will make, asserted before the request is
 * fed: a fixture whose seed left a free slot would exercise the success path
 * and never reach the branch under test. */
static void expect_history_full(moq_session_t *s)
{
    moq_bytes_t part;
    moq_namespace_t ns = hist_ns(&part);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_track_hist_select(s, &ns, MOQ_BYTES_LITERAL("target")),
        (int)MOQ_TH_SEL_FULL);
}


/* -- Destination-owner oracles --------------------------------------- */

void owners_publish_none(const moq_session_t *s, moq_stream_ref_t ref,
                         moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 0);
    MOQ_TEST_CHECK(pub_by_ref(s, ref) == NULL);
}


static void owners_publish_seeded(const moq_session_t *s, moq_stream_ref_t ref,
                                  moq_stream_ref_t seed_ref)
{
    MOQ_TEST_CHECK_EQ_INT(pub_busy_count(s), 1);
    MOQ_TEST_CHECK(pub_by_ref(s, seed_ref) != NULL);
    MOQ_TEST_CHECK(pub_by_ref(s, ref) == NULL);
}


static void owners_ann_none(const moq_session_t *s, moq_stream_ref_t ref,
                            moq_stream_ref_t seed_ref)
{
    (void)seed_ref;
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
}


static void owners_ann_seeded(const moq_session_t *s, moq_stream_ref_t ref,
                              moq_stream_ref_t seed_ref)
{
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 1);
    MOQ_TEST_CHECK(ann_by_ref(s, seed_ref) != NULL);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
}


#define SEED_DIFF_U64(what, label, have, want)                                \
    do {                                                                      \
        if ((uint64_t)(have) != (uint64_t)(want)) {                           \
            TXS_DIAG("TXN %s: seeded %s %llu, expected %llu\n", (what),        \
                     (label), (unsigned long long)(have),                     \
                     (unsigned long long)(want));                             \
            failures++;                                                       \
        }                                                                     \
    } while (0)


void req_seed_diff(const req_seed_snap_t *have,
                   const req_seed_snap_t *want, const char *what)
{
    if (have->buf_invalid || want->buf_invalid) {
        TXS_DIAG("TXN %s: retained bytes uncapturable (have %d, want %d)\n",
                 what, have->buf_invalid, want->buf_invalid);
        failures++;
        return;
    }
    SEED_DIFF_U64(what, "pool occupancy", have->busy, want->busy);
    SEED_DIFF_U64(what, "presence", have->found, want->found);
    SEED_DIFF_U64(what, "state", have->state, want->state);
    SEED_DIFF_U64(what, "role", have->role, want->role);
    SEED_DIFF_U64(what, "generation", have->generation, want->generation);
    SEED_DIFF_U64(what, "handle", have->handle, want->handle);
    SEED_DIFF_U64(what, "request id", have->request_id, want->request_id);
    SEED_DIFF_U64(what, "request stream ref", have->stream_ref,
                  want->stream_ref);
    SEED_DIFF_U64(what, "FIN latch", have->fin, want->fin);
    SEED_DIFF_U64(what, "buffered length", have->recv_len, want->recv_len);
    SEED_DIFF_U64(what, "registry kind", have->reg_kind, want->reg_kind);
    SEED_DIFF_U64(what, "registry slot", have->reg_slot, want->reg_slot);
    SEED_DIFF_U64(what, "registry has_stream_ref", have->reg_has_stream_ref,
                  want->reg_has_stream_ref);
    SEED_DIFF_U64(what, "registry stream ref", have->reg_stream_ref,
                  want->reg_stream_ref);
    SEED_DIFF_U64(what, "by-id kind", have->id_kind, want->id_kind);
    SEED_DIFF_U64(what, "by-id slot", have->id_slot, want->id_slot);
    SEED_DIFF_U64(what, "by-id has_request_id", have->id_has_request_id,
                  want->id_has_request_id);
    SEED_DIFF_U64(what, "by-id request id", have->id_request_id,
                  want->id_request_id);
    SEED_DIFF_U64(what, "got_response", have->ns_got_response,
                  want->ns_got_response);
    SEED_DIFF_U64(what, "parse_complete", have->ns_parse_complete,
                  want->ns_parse_complete);
    SEED_DIFF_U64(what, "closing_remote_error", have->ns_closing_remote_error,
                  want->ns_closing_remote_error);
    SEED_DIFF_U64(what, "local_teardown_pending",
                  have->ns_local_teardown_pending,
                  want->ns_local_teardown_pending);
    if (have->recv_len == want->recv_len && want->recv_len > 0 &&
        memcmp(have->buf, want->buf, want->recv_len) != 0) {
        TXS_DIAG("TXN %s: retained request bytes changed\n", what);
        failures++;
    }
}


/* Both registry keys for one owner, captured together so neither can be
 * retired without the other being noticed. */
void req_seed_capture_registry(req_seed_snap_t *r,
                               const moq_session_t *s,
                               moq_stream_ref_t ref, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    r->reg_kind = (int)ep.kind;
    r->reg_slot = ep.slot;
    r->reg_has_stream_ref = ep.has_stream_ref ? 1 : 0;
    r->reg_stream_ref = ep.stream_ref._v;
    moq_request_endpoint_t ip = request_registry_find_by_id(s, request_id);
    r->id_kind = (int)ip.kind;
    r->id_slot = ip.slot;
    r->id_has_request_id = ip.has_request_id ? 1 : 0;
    r->id_request_id = ip.request_id;
}


/* Append `tail` to a captured snapshot's retained bytes, refusing every
 * arithmetic that could leave the fixed buffer. An invalid base stays invalid,
 * and on any refusal nothing is added and nothing is copied -- a corrupt or
 * oversized captured length can never become a destination offset. */
void req_seed_append(req_seed_snap_t *r, const uint8_t *tail,
                     size_t tail_len)
{
    if (r->buf_invalid || r->recv_len > REQ_SEED_BUF_MAX ||
        tail_len > REQ_SEED_BUF_MAX - r->recv_len) {
        r->buf_invalid = 1;
        return;
    }
    if (tail_len > 0) memcpy(r->buf + r->recv_len, tail, tail_len);
    r->recv_len += tail_len;
}


/* Copy the owner's retained request bytes, or mark the snapshot uncapturable.
 * Shared by every request-owner family. */
void req_seed_copy_buf(req_seed_snap_t *r, const uint8_t *buf,
                       size_t len)
{
    if (len > REQ_SEED_BUF_MAX || (!buf && len > 0)) {
        r->buf_invalid = 1;
        return;
    }
    if (len > 0) memcpy(r->buf, buf, len);
}


static void pub_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = pub_busy_count(s);
    const moq_pub_entry_t *e = pub_by_ref(s, seed_ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->rs.ref._v;
        r->fin        = e->rs.recv_fin ? 1 : 0;
        r->recv_len   = e->rs.recv_len;
        req_seed_copy_buf(r, e->rs.recv_buf, e->rs.recv_len);
    }
    /* Both keys: a request-stream owner is stream-ref keyed and, once its
     * request id has been committed and consumed, absent from the by-ID index. */
    req_seed_capture_registry(r, s, seed_ref, r->request_id);
}


static void pub_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    pub_seed_capture(s, seed_ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


static void ann_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = ann_busy_count(s);
    const moq_ann_entry_t *e = ann_by_ref(s, seed_ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->rs.ref._v;
        r->fin        = e->rs.recv_fin ? 1 : 0;
        r->recv_len   = e->rs.recv_len;
        req_seed_copy_buf(r, e->rs.recv_buf, e->rs.recv_len);
    }
    /* Both keys: a request-stream owner is stream-ref keyed and, once its
     * request id has been committed and consumed, absent from the by-ID index. */
    req_seed_capture_registry(r, s, seed_ref, r->request_id);
}


static void ann_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    ann_seed_capture(s, seed_ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


/* The single pinned history record, identified by its own key bytes rather
 * than by slot index, so a record replaced in place cannot pass. */
static void hist_seed_capture(const moq_session_t *s, moq_stream_ref_t seed_ref,
                              seed_snap_t *o)
{
    (void)seed_ref;
    hist_seed_snap_t *h = &o->hist;
    memset(h, 0, sizeof(*h));
    for (size_t i = 0; i < s->th_cap; i++) {
        const moq_track_hist_t *r = &s->track_hist[i];
        if (!r->in_use) continue;
        h->occupied++;
        if (h->found) continue;
        h->found          = 1;
        h->in_use         = 1;
        h->has_largest    = r->has_largest ? 1 : 0;
        h->refs           = r->refs;
        h->largest_group  = r->largest_group;
        h->largest_object = r->largest_object;
        if (r->key_len > HIST_SEED_KEY_MAX || (!r->key && r->key_len > 0)) {
            h->invalid = 1;          /* copy nothing; key_len stays 0 */
        } else {
            h->key_len = r->key_len;
            if (r->key_len > 0) memcpy(h->key, r->key, r->key_len);
        }
    }
}


static void hist_seed_check(const moq_session_t *s, moq_stream_ref_t seed_ref,
                            const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    hist_seed_capture(s, seed_ref, &now);
    const hist_seed_snap_t *h = &now.hist, *w = &want->hist;
    /* An uncapturable key is never comparable: fail before any byte compare so
     * a length that outran the snapshot cannot index it. */
    if (h->invalid || w->invalid) {
        TXS_DIAG("TXN %s: seeded history key uncapturable (have %d, want %d)\n",
                 what, h->invalid, w->invalid);
        failures++;
        return;
    }
    SEED_DIFF_U64(what, "history occupancy", h->occupied, w->occupied);
    SEED_DIFF_U64(what, "history presence", h->found, w->found);
    SEED_DIFF_U64(what, "history in_use", h->in_use, w->in_use);
    SEED_DIFF_U64(what, "history has_largest", h->has_largest, w->has_largest);
    SEED_DIFF_U64(what, "history refs", h->refs, w->refs);
    SEED_DIFF_U64(what, "history largest group", h->largest_group,
                  w->largest_group);
    SEED_DIFF_U64(what, "history largest object", h->largest_object,
                  w->largest_object);
    SEED_DIFF_U64(what, "history key length", h->key_len, w->key_len);
    if (h->key_len == w->key_len && w->key_len > 0 &&
        memcmp(h->key, w->key, w->key_len) != 0) {
        TXS_DIAG("TXN %s: seeded history key bytes changed\n", what);
        failures++;
    }
}


const seed_ops_t pub_seed_ops = {
    seed_publication, pub_seed_capture, pub_seed_check
};

static const seed_ops_t ann_seed_ops = {
    seed_announcement, ann_seed_capture, ann_seed_check
};

static const seed_ops_t hist_seed_ops = {
    seed_track_history, hist_seed_capture, hist_seed_check
};


/* -- The staging owner ------------------------------------------------ */

/* The receiving staging entry that carries an inbound request before its
 * destination owner exists. On a retryable refusal it and its buffered bytes
 * must survive for replay; after a successful rejection its registry key is
 * gone. */
static void check_staging_retained(const moq_session_t *s,
                                   moq_stream_ref_t ref,
                                   const uint8_t *want_buf, size_t want_len,
                                   bool want_fin)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return;
    MOQ_TEST_CHECK(ep.slot >= 0 && (size_t)ep.slot < s->sub_cap);
    if (ep.slot < 0 || (size_t)ep.slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[ep.slot];
    MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
    MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, want_fin ? 1 : 0);
    /* The buffered request must be replayable byte for byte. */
    failures += txs_check_span_bytes(e->rs.recv_buf, e->rs.recv_len,
                                     want_buf, want_len,
                                     "staging retained request bytes");
}


/* Retirement is both index removal and slot release: an unindexed staging
 * slot left behind would leak. */
static void check_staging_retired(const moq_session_t *s, moq_stream_ref_t ref)
{
    check_unregistered(s, ref);
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 0);
}


/* A bundle must be complete in all three parts. Returns a problem count so the
 * check is self-checkable. */
int precommit_problems(const precommit_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->encode || !c->check_owners) bad++;
    if (c->seed_ops &&
        (!c->seed_ops->seed || !c->seed_ops->capture || !c->seed_ops->check))
        bad++;
    return bad;
}


static void want_precommit_error(txs_norm_vec_t *v, const precommit_case_t *c,
                                 moq_stream_ref_t ref)
{
    if (c->err_reason)
        want_reject_error_reason(v, ref, c->err_code, c->err_retry, c->err_reason);
    else
        want_reject_error_code(v, ref, c->err_code);
}


/* Exactly the branch's REQUEST_ERROR + FIN and nothing else -- no event, no
 * second action. */
static void check_precommit_output(moq_session_t *s, const precommit_case_t *c,
                                   moq_stream_ref_t ref, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, &pub_hooks_for_actions, &got);
    MOQ_TEST_CHECK_EQ_SIZE(got.count, (size_t)0);   /* no event, before actions */
    collect_actions(s, &pub_hooks_for_actions, &got);
    want_precommit_error(&want, c, ref);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


/* One branch under one (FIN, drain-ring) state. The drain expectations are
 * DECLARED from the ring captured before the call, never read back from the
 * ring the call produced. */
static void run_precommit(const precommit_case_t *c, bool fin_in_request,
                          bool fill_ring)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    moq_stream_ref_t seed_ref = moq_stream_ref_from_u64(3);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);

    MOQ_TEST_CHECK_EQ_INT(precommit_problems(c), 0);
    if (c->seed_ops) c->seed_ops->seed(s, seed_ref);
    if (c->precondition) c->precondition(s);
    seed_snap_t seed0;
    memset(&seed0, 0, sizeof(seed0));
    if (c->seed_ops) c->seed_ops->capture(s, seed_ref, &seed0);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);

    uint8_t m[256];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);
    moq_result_t want_rc = (fill_ring && !fin_in_request) ? MOQ_ERR_WOULD_BLOCK
                                                          : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, fin_in_request, 1),
        (int)want_rc);

    /* No destination owner was created on either outcome. The request-staging
     * entry that keys this stream is a different owner, asserted separately
     * per outcome below. */
    c->check_owners(s, ref, seed_ref);
    if (c->seed_ops) {
        snprintf(what, sizeof(what), "%s first call", c->name);
        c->seed_ops->check(s, seed_ref, &seed0, what);
    }
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Retryable: the staging entry and its buffered request survive. */
        check_staging_retained(s, ref, m, n, fin_in_request);
        drain_snap_t now;
        drain_snap(s, &now);
        snprintf(what, sizeof(what), "%s blocked", c->name);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, &pub_hooks_for_actions);

        /* Recovery: release one known filler reference, then replay with no
         * new bytes. The rejection now completes from the buffered request. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed, ring_now;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        drain_snap(s, &ring_now);
        snprintf(what, sizeof(what), "%s filler released", c->name);
        failures += drain_multiset_equals(&ring_now, &ring_freed, what);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s replay", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_precommit_output(s, c, ref, what);

        drain_snap_t after_replay, want_replay;
        drain_snap(s, &after_replay);
        drain_snap_plus(&ring_freed, ref, MOQ_DRAIN_NORMAL, &want_replay);
        snprintf(what, sizeof(what), "%s replay granted", c->name);
        failures += drain_multiset_equals(&after_replay, &want_replay, what);

        /* The peer's later FIN releases it, restoring the freed ring. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&after, &ring_freed, what);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s replay late FIN", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        check_no_output(s, &pub_hooks_for_actions);
        moq_session_destroy(s);
        return;
    }

    /* Committed: the staging key is gone. */
    check_staging_retired(s, ref);
    snprintf(what, sizeof(what), "%s output", c->name);
    check_precommit_output(s, c, ref, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_request) want_ring = ring_before;
    else drain_snap_plus(&ring_before, ref, MOQ_DRAIN_NORMAL, &want_ring);
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_request) {
        /* The reference is released by the peer's later FIN, restoring the
         * ring exactly and producing nothing. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        check_staging_retired(s, ref);
        c->check_owners(s, ref, seed_ref);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        if (c->seed_ops) {
            snprintf(what, sizeof(what), "%s late FIN", c->name);
            c->seed_ops->check(s, seed_ref, &seed0, what);
        }
        check_no_output(s, &pub_hooks_for_actions);
    }
    moq_session_destroy(s);
}


/* Every branch runs the same four states: the ordinary pair, and the pair
 * whose ring is already exhausted. Only the no-FIN exhausted row may refuse. */
static void run_precommit_matrix(const precommit_case_t *c)
{
    run_precommit(c, false, false);   /* no FIN: one drain reference */
    run_precommit(c, true,  false);   /* same-call FIN: none needed */
    run_precommit(c, true,  true);    /* exhausted ring, FIN observed */
    run_precommit(c, false, true);    /* exhausted ring, no FIN */
}


/* -- The retained request-FIN carrier --------------------------------- */

/* The FIN a pre-commit rejection consults is the request bidi's OBSERVED FIN,
 * not the flag of whichever transport call happens to reach the handler. An
 * earlier FIN-bearing call refused for event capacity leaves the generic
 * staging owner holding `req_recv_fin`, and `handle_request_stream_bytes()`
 * passes that latch -- never the raw call FIN -- to the profile, so the empty
 * re-feed that completes the rejection still knows the peer's send half is
 * closed. The four-state matrix only ever completes in the SAME call, so it
 * cannot tell the two apart. This row drives the other shape: refuse first,
 * then complete from an empty re-feed whose own `fin` is false, with the drain
 * ring full throughout -- which the rejection may only survive by taking no
 * reference at all. */

/* The sentinel that occupies the single event slot. It is declared in full and
 * compared field by field -- envelope metadata included -- both while it sits
 * in the queue across the refused call and again when it is handed out, so a
 * refusal cannot corrupt or lose it silently. This fixture owns the comparison
 * rather than widening the signed PUBLISH normalizer, which models only the
 * payload fields its own rows produce.
 *
 * The reason phrase is static storage, not event scratch: the sentinel then
 * outlives the poll without borrowing from the arena, so the refusal's scratch
 * cursor stays comparable while the byte comparison still has something to
 * compare. */
#define RFIN_SENTINEL_HANDLE 0x5E17E1u

#define RFIN_SENTINEL_STATUS 0x3u

#define RFIN_SENTINEL_COUNT  0x9u

static const char k_rfin_sentinel_reason[] = "capacity sentinel";


typedef struct rfin_sentinel {
    uint32_t detail_size;
    uint64_t epoch;        /* borrow_epoch stamped into the QUEUED record */
    uint64_t handle;
    uint64_t status;
    uint64_t count;
    const uint8_t *reason;
    size_t   reason_len;
} rfin_sentinel_t;


static void rfin_push_sentinel(moq_session_t *s, rfin_sentinel_t *d)
{
    memset(d, 0, sizeof(*d));
    d->detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    d->epoch       = s->borrow_epoch;
    d->handle      = RFIN_SENTINEL_HANDLE;
    d->status      = RFIN_SENTINEL_STATUS;
    d->count       = RFIN_SENTINEL_COUNT;
    d->reason      = (const uint8_t *)k_rfin_sentinel_reason;
    d->reason_len  = sizeof(k_rfin_sentinel_reason) - 1;

    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)0);
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_FINISHED;
    ev.detail_size = d->detail_size;
    ev.borrow_epoch = d->epoch;
    ev.u.publish_finished.pub._opaque = d->handle;
    ev.u.publish_finished.status_code = d->status;
    ev.u.publish_finished.stream_count = d->count;
    ev.u.publish_finished.reason.data = d->reason;
    ev.u.publish_finished.reason.len = d->reason_len;
    MOQ_TEST_CHECK_EQ_INT((int)push_event(s, &ev), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    MOQ_TEST_CHECK(event_queue_full(s));
}


/* Every declared field, against an independently supplied borrow epoch: the
 * queued record must still carry the epoch it was pushed with, while a polled
 * record carries the epoch of its TRANSFER, which the session restamps on the
 * way out (session.c, poll_events_ex). Comparing a delivered record against the
 * pre-push value would pin a fiction, so the two are asserted separately. */
static void rfin_check_sentinel(const moq_event_t *ev, const rfin_sentinel_t *d,
                                uint64_t want_epoch, const char *what)
{
    char label[160];
    MOQ_TEST_CHECK_EQ_INT((int)ev->kind, (int)MOQ_EVENT_PUBLISH_FINISHED);
    if (ev->kind != MOQ_EVENT_PUBLISH_FINISHED) return;
    MOQ_TEST_CHECK_EQ_U64((uint64_t)ev->detail_size, (uint64_t)d->detail_size);
    MOQ_TEST_CHECK_EQ_U64(ev->borrow_epoch, want_epoch);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.pub._opaque, d->handle);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.status_code, d->status);
    MOQ_TEST_CHECK_EQ_U64(ev->u.publish_finished.stream_count, d->count);
    /* A declared length with a NULL pointer fails here rather than in a copy:
     * the comparison must survive a malformed borrowed span, not crash on it. */
    snprintf(label, sizeof(label), "%s sentinel reason", what);
    failures += txs_check_span_bytes(ev->u.publish_finished.reason.data,
                                     ev->u.publish_finished.reason.len,
                                     d->reason, d->reason_len, label);
}


/* The sentinel as the queue still holds it: metadata a refused call must not
 * have touched. */
static void rfin_check_queued_sentinel(const moq_session_t *s,
                                       const rfin_sentinel_t *d,
                                       const char *what)
{
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)1);
    if (s->event_tail == s->event_head) return;
    rfin_check_sentinel(&s->events[s->event_head % s->event_cap], d, d->epoch,
                        what);
}


/* Exactly one event comes out, it is the sentinel, and the queue is then
 * empty. */
static void rfin_poll_sentinel(moq_session_t *s, const rfin_sentinel_t *d,
                               const char *what)
{
    uint64_t transfer_epoch = s->borrow_epoch;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events(s, &ev, 1), 1);
    rfin_check_sentinel(&ev, d, transfer_epoch, what);
    moq_event_cleanup(&ev);
    moq_event_t extra;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_events(s, &extra, 1), 0);
    MOQ_TEST_CHECK_EQ_SIZE(s->event_tail - s->event_head, (size_t)0);
}


/* A topology declared as the pre-call graph plus exactly one edge. Building the
 * expectation this way is what stops the blocked graph from being its own
 * baseline. */
static void rfin_topology_plus_edge(const og_graph_t *base, og_graph_t *out,
                                    og_domain_t domain, uint64_t key,
                                    int kind, int slot)
{
    *out = *base;
    MOQ_TEST_CHECK(out->edge_count < OG_MAX_EDGES);
    if (out->edge_count >= OG_MAX_EDGES) {
        out->overflow = 1;   /* incomparable, and loudly so */
        return;
    }
    og_edge_t *e = &out->edges[out->edge_count++];
    memset(e, 0, sizeof(*e));
    e->domain = domain;
    e->key = key;
    e->kind = kind;
    e->slot = slot;
    e->decodable = 1;
}


/* The staging slot the refusal must occupy, declared from the pool BEFORE
 * ingress: first free slot, its generation raised to the next live one. */
static int rfin_declare_staging_slot(const moq_session_t *s, uint32_t *out_gen)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        if (s->subs[i].state != MOQ_SUB_FREE) continue;
        *out_gen = s->subs[i].generation | 1u;
        return (int)i;
    }
    MOQ_TEST_CHECK(0);   /* no free staging slot: the fixture cannot run */
    return -1;
}


/* The DECLARED staging owner inventory -- the fields listed below, against
 * values declared before the call. Deliberately not the whole
 * `moq_sub_entry_t`: unrelated subscription state has its own coverage. */
static void rfin_check_staging(const moq_session_t *s, moq_stream_ref_t ref,
                               int slot, uint32_t gen,
                               const uint8_t *want_buf, size_t want_len)
{
    MOQ_TEST_CHECK_EQ_INT(sub_busy_count(s), 1);
    MOQ_TEST_CHECK(slot >= 0 && (size_t)slot < s->sub_cap);
    if (slot < 0 || (size_t)slot >= s->sub_cap) return;
    const moq_sub_entry_t *e = &s->subs[slot];
    MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_SUB_RECVING_REQUEST);
    MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)e->generation, (uint64_t)gen);
    MOQ_TEST_CHECK_EQ_U64(e->request_id, 0u);   /* no by-id key while receiving */
    MOQ_TEST_CHECK_EQ_U64(e->rs.ref._v, ref._v);
    MOQ_TEST_CHECK_EQ_INT(e->rs.recv_fin, 1);  /* the carrier under test */
    failures += txs_check_span_bytes(e->rs.recv_buf, e->rs.recv_len,
                                     want_buf, want_len,
                                     "retained-FIN staging request bytes");
    /* Also reachable through the registry the profile actually uses. */
    check_staging_retained(s, ref, want_buf, want_len, true);
}


static void run_precommit_retained_fin(const precommit_case_t *c)
{
    char what[128];
    moq_session_t *s = make_session_events_1();
    if (!s) return;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);

    MOQ_TEST_CHECK_EQ_INT(precommit_problems(c), 0);
    /* These two rows carry no seeded owner, so the whole-session oracle below
     * needs no seed conservation of its own. */
    MOQ_TEST_CHECK(c->seed_ops == NULL);
    if (c->precondition) c->precondition(s);

    uint32_t stage_gen = 0;
    int stage_slot = rfin_declare_staging_slot(s, &stage_gen);
    if (stage_slot < 0) { moq_session_destroy(s); return; }

    rfin_sentinel_t sentinel;
    rfin_push_sentinel(s, &sentinel);
    fill_drain_ring(s);
    drain_snap_t ring_full;
    drain_snap(s, &ring_full);

    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    MOQ_TEST_CHECK_EQ_INT(before.owners[0].kind, TXS_OWNER_NONE);
    og_graph_t g, g_pre;
    og_capture(s, &g_pre);
    snprintf(what, sizeof(what), "%s retained-FIN pre", c->name);
    failures += og_check_integrity(&g_pre, what);
    failures += og_check_no_edge(&g_pre, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g_pre, OG_DOM_NS_REF, ref._v, what);

    uint8_t m[256];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);

    /* The complete request arrives WITH the peer's FIN, and is refused for
     * event capacity -- not for the ring, which the rejection would not have
     * needed anyway. */
    MOQ_TEST_CHECK(event_queue_full(s));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, true, 1),
        (int)MOQ_ERR_WOULD_BLOCK);

    /* Transactional: the only declared change is the staging owner this call
     * created, built from the pre-call free slot rather than read back. */
    txs_snapshot_t want_blocked = before;
    want_blocked.owners[0].kind = (int)MOQ_REQ_SUBSCRIPTION;
    want_blocked.owners[0].slot = stage_slot;
    want_blocked.owners[0].state = (int)MOQ_SUB_RECVING_REQUEST;
    want_blocked.owners[0].generation = stage_gen;
    snprintf(what, sizeof(what), "%s retained-FIN blocked", c->name);
    failures += txs_check_eq(s, &ref, 1, &want_blocked, what);

    rfin_check_staging(s, ref, stage_slot, stage_gen, m, n);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    rfin_check_queued_sentinel(s, &sentinel, what);
    og_graph_t g_blocked;
    og_capture(s, &g_blocked);
    failures += og_check_integrity(&g_blocked, what);
    {
        /* The whole blocked topology is the pre-call one plus exactly the
         * staging edge -- not merely that owner's own closure. */
        og_graph_t want_topo;
        rfin_topology_plus_edge(&g_pre, &want_topo, OG_DOM_REQ_STREAMREF,
                                ref._v, (int)MOQ_REQ_SUBSCRIPTION, stage_slot);
        failures += og_check_same_topology(&g_blocked, &want_topo, what);
        og_edge_spec_t want_edges[1] = {
            { OG_DOM_REQ_STREAMREF, ref._v }
        };
        failures += og_check_owner_edges(&g_blocked, (int)MOQ_REQ_SUBSCRIPTION,
                                         stage_slot, want_edges, 1, what);
        failures += og_check_edge(&g_blocked, OG_DOM_REQ_STREAMREF, ref._v,
                                  (int)MOQ_REQ_SUBSCRIPTION, stage_slot, what);
        failures += og_check_no_edge(&g_blocked, OG_DOM_NS_REF, ref._v, what);
    }

    /* Exactly the sentinel, and nothing else, comes out. No peer bytes are
     * fed to free it. */
    snprintf(what, sizeof(what), "%s retained-FIN sentinel", c->name);
    rfin_poll_sentinel(s, &sentinel, what);

    /* The empty re-feed carries fin=false. It completes only because staging
     * retained the observed FIN -- and it must take no drain reference, which
     * the still-full ring is what proves. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s retained-FIN completed", c->name);
    check_precommit_output(s, c, ref, what);
    check_staging_retired(s, ref);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    txs_snapshot_t want_done = before;
    want_done.event_depth = 0;          /* the sentinel was polled */
    failures += txs_check_eq(s, &ref, 1, &want_done, what);
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_same_topology(&g, &g_pre, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIPTION,
                                            stage_slot, what);

    /* A second empty re-feed is silent and changes nothing -- including in the
     * domains a production lookup cannot see, which is what makes the claim
     * cover hidden and unreachable edges. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
        (int)MOQ_OK);
    snprintf(what, sizeof(what), "%s retained-FIN idempotent", c->name);
    check_no_output(s, &pub_hooks_for_actions);
    check_staging_retired(s, ref);
    c->check_owners(s, ref, moq_stream_ref_from_u64(0));
    failures += txs_check_eq(s, &ref, 1, &want_done, what);
    {
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_full, what);
    }
    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_same_topology(&g, &g_pre, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    failures += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    failures += og_check_owner_unreferenced(&g, (int)MOQ_REQ_SUBSCRIPTION,
                                            stage_slot, what);
    moq_session_destroy(s);
}


void test_precommit_rejects(void)
{
    /* PUBLISH, session_publish.c:389 -- message-level auth reject. */
    static const precommit_case_t publish_auth = {
        .name = "publish auth", .make = make_session_default,
        .encode = enc_publish_unknown_alias,
        .err_code = 0x17u, .err_reason = NULL,
        .check_owners = owners_publish_none
    };
    /* PUBLISH, session_publish.c:420 -- unknown mandatory Track Property. */
    static const precommit_case_t publish_prop = {
        .name = "publish mandatory property", .make = make_session_default,
        .encode = enc_publish_mandatory_prop,
        .err_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION,
        .err_reason = "unsupported mandatory track property",
        .check_owners = owners_publish_none
    };
    /* PUBLISH, session_publish.c:451 -- publication pool exhausted. A local
     * capacity refusal is EXCESSIVE_LOAD, "The responder is overloaded and
     * cannot process the request at this time" (draft-18 §10.6), retryable
     * at once (Retry Interval 1); so are the two exhaustions below. */
    static const precommit_case_t publish_pool = {
        .name = "publish pool full", .make = make_session_pub_cap_1,
        .seed_ops = &pub_seed_ops, .encode = enc_publish_second,
        .err_code = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, .err_retry = 1,
        .err_reason = "publish pool full",
        .check_owners = owners_publish_seeded,
    };
    /* PUBLISH, session_publish.c:500 -- track-history registry exhausted. */
    static const precommit_case_t publish_hist = {
        .name = "publish track history full", .make = make_session_hist_cap_1,
        .seed_ops = &hist_seed_ops, .precondition = expect_history_full,
        .encode = enc_publish_hist_target,
        .err_code = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, .err_retry = 1,
        .err_reason = "track history full",
        .check_owners = owners_publish_none,
    };
    /* PUBLISH_NAMESPACE, session_namespace.c:278 -- auth reject. */
    static const precommit_case_t pns_auth = {
        .name = "publish_namespace auth", .make = make_session_default,
        .encode = enc_pns_unknown_alias,
        .err_code = 0x17u, .err_reason = NULL,
        .check_owners = owners_ann_none
    };
    /* PUBLISH_NAMESPACE, session_namespace.c:310 -- pool exhausted. */
    static const precommit_case_t pns_pool = {
        .name = "publish_namespace pool full", .make = make_session_ann_cap_1,
        .seed_ops = &ann_seed_ops, .encode = enc_pns_second,
        .err_code = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD, .err_retry = 1,
        .err_reason = "announcement pool full",
        .check_owners = owners_ann_seeded,
    };

    run_precommit_matrix(&publish_auth);
    run_precommit_matrix(&publish_prop);
    run_precommit_matrix(&publish_pool);
    run_precommit_matrix(&publish_hist);
    run_precommit_matrix(&pns_auth);
    run_precommit_matrix(&pns_pool);

    /* One representative per semantic handler: the two boundaries the carrier
     * has to cross are `session_core_on_publish()` and
     * `session_core_on_publish_namespace()`, not the six branches. */
    run_precommit_retained_fin(&publish_auth);
    run_precommit_retained_fin(&pns_auth);
}


/* -- Axis 3 current-call-FIN terminals -------------------------------- */

/* These terminals close an owner the peer's message just condemned. No handoff
 * marker can be present, so the FIN they consult is the one on the call that
 * carried the message. Each reserves and adds its drain reference only when the
 * peer half is genuinely open: a peer that closed its send half in the same
 * chunk spends none, and an exhausted ring cannot refuse a same-call-FIN
 * transition that needs no reference at all. */

static size_t enc_request_update(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_msg_params_t p = { 0 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, rid, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* The armed owner consumed inbound request id 0, so the update is the next one
 * in sequence. */
size_t enc_update_next(uint8_t *buf, size_t cap)
{
    return enc_request_update(buf, cap, 2);
}


/* Drain whatever arming produced, requiring the exact counts the arm owes: a
 * stray record here would otherwise be attributed to the terminal. */
void arm_output_exact(moq_session_t *s, uint32_t want_event_kind,
                      int want_events, int want_actions,
                      const char *what, uint64_t *out_handle)
{
    int matched = 0, other_events = 0, actions = 0;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        if (e.kind == want_event_kind) {
            matched++;
            if (out_handle) {
                if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED)
                    *out_handle = e.u.namespace_published.ann._opaque;
                else if (e.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST)
                    *out_handle = e.u.subscribe_tracks_request.handle._opaque;
            }
        } else {
            other_events++;
            TXS_DIAG("TXN %s arm: unexpected event kind %u\n", what,
                     (unsigned)e.kind);
        }
        moq_event_cleanup(&e);
    }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        actions++;
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK_EQ_INT(matched, want_events);
    MOQ_TEST_CHECK_EQ_INT(other_events, 0);
    MOQ_TEST_CHECK_EQ_INT(actions, want_actions);
}


/* An inbound PUBLISH_NAMESPACE the application accepted: a receiver-role
 * announcement in ESTABLISHED, which is the only state its bidi accepts a
 * REQUEST_UPDATE in. */
moq_stream_ref_t arm_established_announcement(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);   /* peer-opened */
    uint8_t m[192];
    size_t n = enc_pns_full(m, sizeof(m), 0, "live", NULL);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, m, n, false, 1),
        (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_NAMESPACE_PUBLISHED, 1, 0, "announcement", &h);
    moq_announcement_t ann;
    ann._opaque = h;
    moq_accept_namespace_cfg_t ac;
    moq_accept_namespace_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(s, ann, &ac, 1),
                          (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, 1, "announcement accept",
                     NULL);
    const moq_ann_entry_t *e = ann_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_ANN_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_ANN_ROLE_RECEIVER);
    }
    check_registered(s, ref, MOQ_REQ_ANNOUNCEMENT,
                     e ? (int)(e - s->announcements) : -1);
    return ref;
}


/* The publisher-side mirror: an accepted SUBSCRIBE_TRACKS in ESTABLISHED. */
moq_stream_ref_t arm_established_track_sub(moq_session_t *s)
{
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);   /* peer-opened */
    st_ctx_t c = st_ctx;
    c.request_id = 0;
    MOQ_TEST_CHECK_EQ_INT((int)st_feed(s, &c, ref, false), (int)MOQ_OK);
    uint64_t h = 0;
    arm_output_exact(s, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, 1, 0,
                     "subscribe-tracks", &h);
    moq_track_sub_handle_t handle;
    handle._opaque = h;
    moq_accept_subscribe_tracks_cfg_t ac;
    moq_accept_subscribe_tracks_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_accept_subscribe_tracks(s, handle, &ac, 1), (int)MOQ_OK);
    arm_output_exact(s, MOQ_EVENT_SESSION_CLOSED, 0, 1,
                     "subscribe-tracks accept", NULL);
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    MOQ_TEST_CHECK(e != NULL);
    if (e) {
        MOQ_TEST_CHECK_EQ_INT((int)e->state, (int)MOQ_TRACK_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)e->role, (int)MOQ_TRACK_SUB_ROLE_PUBLISHER);
    }
    check_registered(s, ref, MOQ_REQ_SUBSCRIBE_TRACKS,
                     e ? (int)(e - s->track_subs) : -1);
    return ref;
}


/* Both helpers answer with the same NOT_SUPPORTED REQUEST_ERROR + FIN, encoded
 * independently of the product. */
void want_update_rejected(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    want_reject_error(v, ref);
}


static void check_ann_terminal_retired(const moq_session_t *s,
                                       moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(ann_busy_count(s), 0);
    MOQ_TEST_CHECK(ann_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}


static void check_st_terminal_retired(const moq_session_t *s,
                                      moq_stream_ref_t ref)
{
    MOQ_TEST_CHECK_EQ_INT(st_busy_count(s), 0);
    MOQ_TEST_CHECK(st_by_ref(s, ref) == NULL);
    check_unregistered(s, ref);
}


static void st_owner_capture(const moq_session_t *s, moq_stream_ref_t ref,
                             seed_snap_t *o)
{
    req_seed_snap_t *r = &o->req;
    memset(r, 0, sizeof(*r));
    r->busy = st_busy_count(s);
    const moq_track_sub_entry_t *e = st_by_ref(s, ref);
    r->found = e != NULL;
    if (e) {
        r->state      = (int)e->state;
        r->role       = (int)e->role;
        r->generation = e->generation;
        r->handle     = e->handle._opaque;
        r->request_id = e->request_id;
        r->stream_ref = e->rs.ref._v;
        r->fin        = e->rs.recv_fin ? 1 : 0;
        r->recv_len   = e->rs.recv_len;
        req_seed_copy_buf(r, e->rs.recv_buf, e->rs.recv_len);
    }
    req_seed_capture_registry(r, s, ref, r->request_id);
}


static void st_owner_check(const moq_session_t *s, moq_stream_ref_t ref,
                           const seed_snap_t *want, const char *what)
{
    seed_snap_t now;
    st_owner_capture(s, ref, &now);
    req_seed_diff(&now.req, &want->req, what);
}


const owner_ops_t ann_owner_ops = {
    ann_seed_capture, ann_seed_check, check_ann_terminal_retired
};

const owner_ops_t st_owner_ops = {
    st_owner_capture, st_owner_check, check_st_terminal_retired
};


/* A recognized, non-empty request family. MOQ_REQ_NONE would make every
 * owner-target scan pass vacuously, so it is rejected outright. */
static int terminal_graph_kind_ok(int kind)
{
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:
    case MOQ_REQ_SUBSCRIPTION_UPDATE:
    case MOQ_REQ_ANNOUNCEMENT:
    case MOQ_REQ_NAMESPACE_SUB:
    case MOQ_REQ_FETCH:
    case MOQ_REQ_PUBLISH:
    case MOQ_REQ_PUBLICATION_UPDATE:
    case MOQ_REQ_TRACK_STATUS:
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        return 1;
    default:
        return 0;
    }
}


int terminal_problems(const terminal_case_t *c)
{
    int bad = 0;
    if (!c->name || !c->make || !c->arm || !c->encode || !c->want_output) bad++;
    /* Output-only use: the runner needs both normalizers and nothing else, so
     * the full transactional hook set is deliberately not required here. */
    if (!c->hooks || !c->hooks->normalize_event ||
        !c->hooks->normalize_action) bad++;
    if (!c->owner || !c->owner->capture || !c->owner->check ||
        !c->owner->check_retired) bad++;
    if (c->extra && (!c->extra->capture || !c->extra->check_blocked ||
                     !c->extra->check_committed)) bad++;
    if (c->drain_reason != MOQ_DRAIN_NORMAL &&
        c->drain_reason != MOQ_DRAIN_GOAWAY_RECEIVED) bad++;
    /* All-or-nothing: a declared capability must be COMPLETE, and its kind must
     * be a real family -- MOQ_REQ_NONE (the value a deleted initializer leaves
     * behind) would make the owner-target retirement scan pass vacuously. */
    if (c->graph) {
        if (!c->graph->owner_edges || !c->graph->owner_slot) bad++;
        if (!terminal_graph_kind_ok(c->graph->owner_kind)) bad++;
    }
    return bad;
}


/* No edge in ANY domain still keys on the retired stream, and no edge anywhere
 * still points at the owner slot it occupied. */
static int check_terminal_owner_retired_edges(const moq_session_t *s,
                                              const terminal_case_t *c,
                                              moq_stream_ref_t ref,
                                              const char *what)
{
    if (!c->graph) return 0;
    og_graph_t g;
    og_capture(s, &g);
    int bad = og_check_integrity(&g, what);
    bad += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, ref._v, what);
    bad += og_check_no_edge(&g, OG_DOM_NS_REF, ref._v, what);
    if (*c->graph->owner_slot >= 0)
        bad += og_check_owner_edges(&g, c->graph->owner_kind,
                                    *c->graph->owner_slot, NULL, 0, what);
    return bad;
}


void check_terminal_output(moq_session_t *s, const terminal_case_t *c,
                           moq_stream_ref_t ref, const char *what)
{
    txs_norm_vec_t got, want;
    txs_norm_init(&got);
    txs_norm_init(&want);
    collect_events(s, c->hooks, &got);
    collect_actions(s, c->hooks, &got);
    c->want_output(&want, ref);
    failures += txs_norm_equals(&got, &want, what);
    txs_norm_free(&got);
    txs_norm_free(&want);
}


/* One terminal under one (FIN, drain-ring) state. Drain expectations are
 * declared from the ring captured before the call. */
static void run_terminal(const terminal_case_t *c, bool fin_in_call,
                         bool fill_ring, size_t split)
{
    char what[128];
    moq_session_t *s = c->make();
    if (!s) return;
    MOQ_TEST_CHECK_EQ_INT(terminal_problems(c), 0);
    moq_stream_ref_t ref = c->arm(s);
    MOQ_TEST_CHECK(ref._v != 0);
    if (ref._v == 0) { moq_session_destroy(s); return; }

    uint8_t m[192];
    size_t n = c->encode(m, sizeof(m));
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(split < n);
    if (split >= n) { moq_session_destroy(s); return; }

    /* A fragmented message: the head is retained on the owner and no terminal
     * runs, so the snapshot taken next already carries a real prefix and the
     * failing call contributes only the suffix. */
    if (split > 0) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, m, split, false, 1),
            (int)MOQ_OK);
        check_no_output(s, c->hooks);
    }

    seed_snap_t owner0;
    memset(&owner0, 0, sizeof(owner0));
    c->owner->capture(s, ref, &owner0);
    /* The baseline must be the bytes the fixture actually fed. Adopting
     * whatever the head left would let corruption during the first feed become
     * the expectation the blocked path is then measured against. */
    MOQ_TEST_CHECK_EQ_SIZE(owner0.req.recv_len, split);
    MOQ_TEST_CHECK_EQ_INT(owner0.req.buf_invalid, 0);
    if (!owner0.req.buf_invalid && owner0.req.recv_len == split && split > 0)
        MOQ_TEST_CHECK(memcmp(owner0.req.buf, m, split) == 0);

    if (fill_ring) fill_drain_ring(s);
    drain_snap_t ring_before;
    drain_snap(s, &ring_before);
    if (c->extra) c->extra->capture(s);

    /* The generic transactional snapshot: a refused terminal must leave the
     * WHOLE session as it found it, not merely its own owner. */
    txs_snapshot_t before;
    txs_capture(s, &ref, 1, &before);
    expect_after_call_prepare(&before);

    og_graph_t graph_before;
    og_capture(s, &graph_before);
    if (c->graph) {
        snprintf(what, sizeof(what), "%s armed", c->name);
        failures += og_check_integrity(&graph_before, what);
        failures += c->graph->owner_edges(&graph_before, ref, what);
    }

    const uint8_t *tail = m + split;
    size_t tail_len = n - split;
    moq_result_t want_rc = (fill_ring && !fin_in_call) ? MOQ_ERR_WOULD_BLOCK
                                                       : MOQ_OK;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, tail, tail_len,
                                              fin_in_call, 1),
        (int)want_rc);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    if (want_rc == MOQ_ERR_WOULD_BLOCK) {
        /* Retryable: the condemned owner survives whole apart from ONE declared
         * delta -- the suffix was buffered onto that same owner before the
         * terminal ran, and the refusal deliberately keeps the complete message
         * for replay. The pre-commit tranche avoids the delta by pre-admitting
         * its request first, which works there because the bytes land on a
         * SEPARATE staging owner. Here they land on the very owner under test,
         * so no admission order removes them, whatever gate refuses -- some of
         * these terminals owe an event and some do not. The expectation is
         * therefore the captured prefix followed by this call's suffix, byte
         * for byte. */
        seed_snap_t owner_blocked = owner0;
        req_seed_append(&owner_blocked.req, tail, tail_len);
        snprintf(what, sizeof(what), "%s blocked", c->name);
        c->owner->check(s, ref, &owner_blocked, what);
        failures += txs_check_eq(s, &ref, 1, &before, what);
        if (c->extra) failures += c->extra->check_blocked(s, what);
        if (c->graph) {
            /* Not merely a valid graph: the SAME graph. An edge quietly
             * removed or repointed is still internally consistent. */
            og_graph_t g_now;
            og_capture(s, &g_now);
            failures += og_check_integrity(&g_now, what);
            failures += c->graph->owner_edges(&g_now, ref, what);
            failures += og_check_same_topology(&g_now, &graph_before, what);
        }
        drain_snap_t now;
        drain_snap(s, &now);
        failures += drain_multiset_equals(&now, &ring_before, what);
        check_no_output(s, c->hooks);

        /* Release one known filler, then replay with no new bytes. */
        moq_stream_ref_t filler = moq_stream_ref_from_u64(0x4000);
        MOQ_TEST_CHECK(drain_ref_contains(s, filler));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, filler, NULL, 0, true, 1),
            (int)MOQ_OK);
        drain_snap_t ring_freed, ring_now;
        drain_snap_minus(&ring_before, filler, MOQ_DRAIN_NORMAL, &ring_freed);
        drain_snap(s, &ring_now);
        snprintf(what, sizeof(what), "%s filler released", c->name);
        failures += drain_multiset_equals(&ring_now, &ring_freed, what);
        if (c->graph) {
            /* Releasing an unrelated filler must not have touched the target
             * owner or the graph the replay is about to consume. */
            c->owner->check(s, ref, &owner_blocked, what);
            og_graph_t g;
            og_capture(s, &g);
            failures += og_check_integrity(&g, what);
            failures += c->graph->owner_edges(&g, ref, what);
            failures += og_check_same_topology(&g, &graph_before, what);
        }

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        c->owner->check_retired(s, ref);
        snprintf(what, sizeof(what), "%s replayed", c->name);
        check_terminal_output(s, c, ref, what);
        if (c->extra) failures += c->extra->check_committed(s, what);
        failures += check_terminal_owner_retired_edges(s, c, ref, what);

        drain_snap_t after_replay, want_replay;
        drain_snap(s, &after_replay);
        drain_snap_plus(&ring_freed, ref, c->drain_reason, &want_replay);
        snprintf(what, sizeof(what), "%s replay granted", c->name);
        failures += drain_multiset_equals(&after_replay, &want_replay, what);
        check_drain_ref_reason(s, ref, c->drain_reason);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s replay released", c->name);
        failures += drain_multiset_equals(&after, &ring_freed, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
        failures += check_terminal_owner_retired_edges(s, c, ref, what);
        c->owner->check_retired(s, ref);   /* no resurrection */
        moq_session_destroy(s);
        return;
    }

    c->owner->check_retired(s, ref);
    snprintf(what, sizeof(what), "%s output", c->name);
    check_terminal_output(s, c, ref, what);
    if (c->extra) failures += c->extra->check_committed(s, what);
    failures += check_terminal_owner_retired_edges(s, c, ref, what);

    drain_snap_t now, want_ring;
    drain_snap(s, &now);
    if (fin_in_call) {
        want_ring = ring_before;
    } else {
        drain_snap_plus(&ring_before, ref, c->drain_reason, &want_ring);
        check_drain_ref_reason(s, ref, c->drain_reason);
    }
    snprintf(what, sizeof(what), "%s granted", c->name);
    failures += drain_multiset_equals(&now, &want_ring, what);

    if (!fin_in_call) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        drain_snap_t after;
        drain_snap(s, &after);
        snprintf(what, sizeof(what), "%s released", c->name);
        failures += drain_multiset_equals(&after, &ring_before, what);
        c->owner->check_retired(s, ref);
        check_no_output(s, c->hooks);
    }
    moq_session_destroy(s);
}


/* The four (FIN, ring) states, delivered whole; then the no-FIN exhausted-ring
 * state again with the message split, so the retained-byte oracle compares a
 * genuine prefix-plus-suffix rather than one whole call's input. */
void run_terminal_matrix(const terminal_case_t *c, size_t split)
{
    run_terminal(c, false, false, 0);   /* no FIN: one releasable reference */
    run_terminal(c, true,  false, 0);   /* same-call FIN: none needed */
    run_terminal(c, true,  true,  0);   /* exhausted ring, FIN observed */
    run_terminal(c, false, true,  0);   /* exhausted ring, no FIN */
    run_terminal(c, false, true,  split);            /* fragmented */
}
