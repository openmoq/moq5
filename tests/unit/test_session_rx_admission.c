/* Direct-session admission: refusal owns neither bytes nor FIN. */
#include "test_fetch_abort_rig.h"

static int failures;

typedef struct response {
    uint8_t data[1024];
    size_t len, first_len;
    moq_stream_ref_t ref;
    held_action_t control[8];
    size_t control_n;
    bool fin;
} response_t;

typedef struct ledger {
    moq_fetch_t handles[3];
    unsigned objects[3], complete[3], ok[3];
} ledger_t;

static void observe(moq_session_t *s, ledger_t *o)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) {
        moq_fetch_t h = {0};
        if (e.kind == MOQ_EVENT_FETCH_OBJECT) h = e.u.fetch_object.fetch;
        else if (e.kind == MOQ_EVENT_FETCH_COMPLETE) h = e.u.fetch_complete.fetch;
        else if (e.kind == MOQ_EVENT_FETCH_OK) h = e.u.fetch_ok.fetch;
        else MOQ_TEST_CHECK(false); /* No failure or unrelated terminal. */
        int k;
        for (k = 0; k < 3; k++) if (o->handles[k]._opaque == h._opaque) break;
        MOQ_TEST_CHECK(k < 3);
        if (k < 3 && e.kind == MOQ_EVENT_FETCH_OBJECT) {
            const moq_fetch_object_event_t *f = &e.u.fetch_object;
            MOQ_TEST_CHECK(f->group_id == 0 && f->object_id == o->objects[k]);
            MOQ_TEST_CHECK(moq_rcbuf_len(f->payload) == 24);
            if (moq_rcbuf_len(f->payload) == 24) {
                for (size_t j = 0; j < 24; j++)
                    MOQ_TEST_CHECK(moq_rcbuf_data(f->payload)[j] == (uint8_t)(0x60 + o->objects[k]));
            }
            o->objects[k]++;
        } else if (k < 3 && e.kind == MOQ_EVENT_FETCH_COMPLETE) o->complete[k]++;
        else if (k < 3 && e.kind == MOQ_EVENT_FETCH_OK) o->ok[k]++;
        moq_event_cleanup(&e);
    }
}

static bool open_pair(raw_t *r, moq_version_t version)
{
    memset(r, 0, sizeof(*r));
    r->alloc = test_allocator(&r->ta);
    moq_session_cfg_t c = MOQ_SESSION_CFG_INIT, p = MOQ_SESSION_CFG_INIT;
    c.alloc = p.alloc = &r->alloc;
    c.version = p.version = version;
    c.perspective = MOQ_PERSPECTIVE_CLIENT;
    p.perspective = MOQ_PERSPECTIVE_SERVER;
    c.send_request_capacity = p.send_request_capacity = true;
    c.initial_request_capacity = p.initial_request_capacity = 64;
    c.max_data_streams = 2;
    if (moq_session_create(&c, 0, &r->fet) != MOQ_OK) return false;
    if (moq_session_create(&p, 0, &r->pub) != MOQ_OK) return false;
    MOQ_TEST_CHECK(moq_session_start(r->fet, 1) == MOQ_OK);
    if (version == MOQ_VERSION_DRAFT_18)
        MOQ_TEST_CHECK(moq_session_start(r->pub, 1) == MOQ_OK);
    for (int i = 0; i < 6; i++) raw_cycle(r);
    ev_cap_t initial = {0};
    drain(r->fet, &initial, false);
    drain(r->pub, &initial, false);
    return moq_session_state(r->fet) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(r->pub) == MOQ_SESS_ESTABLISHED;
}

static void collect(raw_t *r, response_t *out)
{
    moq_action_t a;
    while (moq_session_poll_actions(r->pub, &a, 1)) {
        if (a.kind == MOQ_ACTION_SEND_DATA) {
            const moq_send_data_action_t *d = &a.u.send_data;
            size_t n = moq_rcbuf_len(d->payload);
            MOQ_TEST_CHECK(out->len + d->header_len + n <= sizeof(out->data));
            if (out->len + d->header_len + n <= sizeof(out->data)) {
                memcpy(out->data + out->len, d->header, d->header_len);
                out->len += d->header_len;
                if (n) memcpy(out->data + out->len, moq_rcbuf_data(d->payload), n);
                out->len += n;
            }
            moq_stream_ref_t ref = raw_map_ref(d->stream_ref);
            MOQ_TEST_CHECK(out->ref._v == 0 || out->ref._v == ref._v);
            out->ref = ref;
            out->fin = d->fin;
        } else {
            MOQ_TEST_CHECK(out->control_n < 8);
            if (out->control_n < 8) raw_snapshot(&out->control[out->control_n++], &a);
        }
        moq_action_cleanup(&a);
    }
}

static void make_responses(raw_t *r, ledger_t *o, response_t responses[3])
{
    for (int k = 0; k < 3; k++) {
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("admission")};
        fc.track_namespace = (moq_namespace_t){.parts = ns, .count = 1};
        fc.track_name = MOQ_BYTES_LITERAL("track");
        fc.end_object = 1;
        MOQ_TEST_CHECK(moq_session_fetch(r->fet, &fc, r->now, &o->handles[k]) == MOQ_OK);
        raw_pump(r, r->fet, r->pub, NULL, NULL);
        ev_cap_t requests = {0};
        drain(r->pub, &requests, false);
        MOQ_TEST_CHECK(requests.requests == 1);
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_object = 1;
        MOQ_TEST_CHECK(moq_session_accept_fetch(r->pub, requests.last_request, &ac, r->now) == MOQ_OK);
        rig_t writer = {.alloc = {.vt = r->alloc}, .pub = r->pub, .now = r->now};
        MOQ_TEST_CHECK(pub_write(&writer, requests.last_request, 0) == MOQ_OK);
        collect(r, &responses[k]);
        responses[k].first_len = responses[k].len;
        MOQ_TEST_CHECK(pub_write(&writer, requests.last_request, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_end_fetch(r->pub, requests.last_request, r->now) == MOQ_OK);
        collect(r, &responses[k]);
        MOQ_TEST_CHECK(responses[k].fin && responses[k].first_len > 1 &&
                       responses[k].first_len < responses[k].len);
    }
}

static void controls(raw_t *r, const response_t *response)
{
    for (size_t i = 0; i < response->control_n; i++)
        MOQ_TEST_CHECK(raw_deliver(r, r->fet, &response->control[i]) == MOQ_OK);
}

static moq_result_t input(raw_t *r, bool rcbuf, moq_stream_ref_t ref,
                          const uint8_t *bytes, size_t len, bool fin)
{
    if (!rcbuf) return moq_session_on_data_bytes(r->fet, ref, bytes, len, fin, ++r->now);
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&r->alloc, bytes, len, &buf) == MOQ_OK);
    moq_result_t rc = moq_session_on_data_rcbuf(r->fet, ref, buf, fin, ++r->now);
    /* Whole-object mode copies input; refusal must not take a ref either. */
    MOQ_TEST_CHECK(moq_rcbuf_refcount(buf) == 1);
    moq_rcbuf_decref(buf);
    return rc;
}

static void refuse(raw_t *r, bool rcbuf, moq_stream_ref_t ref,
                   const uint8_t *bytes, size_t len, bool fin, moq_fetch_t fetch)
{
    moq_session_t *s = r->fet;
    size_t actions = s->action_tail - s->action_head;
    size_t events = s->event_tail - s->event_head;
    size_t stopped = s->rx_stopped_count, finished = s->rx_fin_count;
    size_t input_bytes = s->recv_input_bytes, payload_bytes = s->recv_payload_bytes;
    int64_t balance = r->ta.balance;
    int slot = fetch_resolve_handle(s, fetch);
    MOQ_TEST_CHECK(slot >= 0);
    if (slot < 0) return;
    moq_fetch_entry_t before = s->fetches[slot];
    MOQ_TEST_CHECK(input(r, rcbuf, ref, bytes, len, fin) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(s, ref));
    MOQ_TEST_CHECK(s->action_tail - s->action_head == actions);
    MOQ_TEST_CHECK(s->event_tail - s->event_head == events);
    MOQ_TEST_CHECK(s->rx_stopped_count == stopped && s->rx_fin_count == finished);
    MOQ_TEST_CHECK(s->recv_input_bytes == input_bytes && s->recv_payload_bytes == payload_bytes);
    MOQ_TEST_CHECK(memcmp(&before, &s->fetches[slot], sizeof(before)) == 0);
    MOQ_TEST_CHECK(r->ta.balance == balance);
    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
}

static void no_actions(raw_t *r)
{
    moq_action_t a;
    while (moq_session_poll_actions(r->fet, &a, 1)) {
        MOQ_TEST_CHECK(false);
        moq_action_cleanup(&a);
    }
}

static void run_replay(moq_version_t version, bool rcbuf, bool fragmented, bool pressure)
{
    int before = failures;
    raw_t r;
    if (!open_pair(&r, version)) { MOQ_TEST_CHECK(false); raw_down(&r); return; }
    ledger_t o = {0};
    response_t replies[3] = {0};
    make_responses(&r, &o, replies);
    for (int k = 0; k < 2; k++) {
        controls(&r, &replies[k]);
        MOQ_TEST_CHECK(input(&r, rcbuf, replies[k].ref, replies[k].data, replies[k].first_len, false) == MOQ_OK);
    }
    observe(r.fet, &o);
    MOQ_TEST_CHECK(o.objects[0] == 1 && o.objects[1] == 1 && o.ok[2] == 0);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(r.fet));
    if (pressure) {
        /* Legacy identities cannot be created by lossless admission. Seed
         * the bounded set to prove that a full set cannot affect refusal. */
        for (size_t i = 0; i < r.fet->rx_stopped_cap; i++) r.fet->rx_stopped_refs[i] = 10000 + i;
        r.fet->rx_stopped_count = r.fet->rx_stopped_cap;
        test_session_fill_action_queue(r.fet);
    }
    response_t *held = &replies[2];
    size_t first = fragmented ? 1 : held->len;
    for (int i = 0; i < 3; i++)
        refuse(&r, rcbuf, held->ref, held->data, first, !fragmented, o.handles[2]);
    moq_stream_ref_t empty = moq_stream_ref_from_u64(9999);
    refuse(&r, rcbuf, empty, NULL, 0, true, o.handles[2]);
    MOQ_TEST_CHECK(input(&r, rcbuf, empty, NULL, 0, false) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.fet, empty));
    if (pressure) {
        moq_action_t a;
        size_t n = 0;
        while (moq_session_poll_actions(r.fet, &a, 1)) {
            MOQ_TEST_CHECK(a.kind == MOQ_ACTION_SEND_CONTROL);
            n++;
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(n == r.fet->action_cap);
    } else no_actions(&r);

    /* An already-owned stream progresses while the refused one stays held. */
    response_t *other = &replies[1];
    MOQ_TEST_CHECK(input(&r, rcbuf, other->ref, other->data + other->first_len,
                         other->len - other->first_len, false) == MOQ_OK);
    observe(r.fet, &o);
    MOQ_TEST_CHECK(o.objects[1] == 2 && o.complete[1] == 0);
    refuse(&r, rcbuf, held->ref, held->data, first, !fragmented, o.handles[2]);
    MOQ_TEST_CHECK(input(&r, rcbuf, other->ref, NULL, 0, true) == MOQ_OK);
    observe(r.fet, &o);
    MOQ_TEST_CHECK(o.complete[1] == 1 && moq_session_can_admit_data_stream(r.fet));

    MOQ_TEST_CHECK(input(&r, rcbuf, held->ref, held->data, first, !fragmented) == MOQ_OK);
    if (fragmented) {
        MOQ_TEST_CHECK(moq_session_has_transport_stream(r.fet, held->ref));
        for (size_t i = 1; i < held->len; i++)
            MOQ_TEST_CHECK(input(&r, rcbuf, held->ref, held->data + i, 1, i + 1 == held->len) == MOQ_OK);
    }
    observe(r.fet, &o);
    MOQ_TEST_CHECK(o.objects[2] == 2 && o.complete[2] == 0 && o.ok[2] == 0);
    controls(&r, held); /* Data and FIN deliberately precede FETCH_OK. */
    observe(r.fet, &o);
    MOQ_TEST_CHECK(o.ok[2] == 1 && o.complete[2] == 1);
    MOQ_TEST_CHECK(moq_session_fetch_cancel(r.fet, o.handles[2], ++r.now) == MOQ_ERR_STALE_HANDLE);
    other = &replies[0];
    MOQ_TEST_CHECK(input(&r, rcbuf, other->ref, other->data + other->first_len,
                         other->len - other->first_len, true) == MOQ_OK);
    observe(r.fet, &o);
    for (int k = 0; k < 3; k++)
        MOQ_TEST_CHECK(o.objects[k] == 2 && o.complete[k] == 1 && o.ok[k] == 1);
    no_actions(&r);
    MOQ_TEST_CHECK(moq_session_state(r.fet) == MOQ_SESS_ESTABLISHED);
    raw_down(&r);
    MOQ_TEST_CHECK(r.ta.balance == 0);
    printf("%s: d%d %s %s pressure=%d\n", failures == before ? "PASS" : "FAIL",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, rcbuf ? "rcbuf" : "bytes",
           fragmented ? "fragmented" : "payload-fin", pressure);
}

static void run_terminals(moq_version_t version, bool rcbuf, bool finished)
{
    int before = failures;
    raw_t r;
    if (!open_pair(&r, version)) { MOQ_TEST_CHECK(false); raw_down(&r); return; }
    ledger_t o = {0};
    response_t replies[3] = {0};
    make_responses(&r, &o, replies);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(9999);
    if (finished) {
        controls(&r, &replies[0]);
        ref = replies[0].ref;
        MOQ_TEST_CHECK(input(&r, rcbuf, ref, replies[0].data, replies[0].len, true) == MOQ_OK);
        observe(r.fet, &o);
        MOQ_TEST_CHECK(o.complete[0] == 1);
    }
    for (int k = 1; k < 3; k++)
        MOQ_TEST_CHECK(input(&r, rcbuf, replies[k].ref, replies[k].data, replies[k].first_len, false) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(r.fet));
    if (!finished) {
        refuse(&r, rcbuf, ref, NULL, 0, true, o.handles[0]);
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.fet, replies[1].ref, 1, ++r.now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_can_admit_data_stream(r.fet));
        moq_action_t pending;
        while (moq_session_poll_actions(r.fet, &pending, 1)) moq_action_cleanup(&pending);
        size_t fins = r.fet->rx_fin_count;
        /* A headerless FIN is accepted by the existing parser. Only the replay
         * records it, without a STOP or a fabricated request completion. */
        MOQ_TEST_CHECK(input(&r, rcbuf, ref, NULL, 0, true) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(r.fet) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.fet, ref));
        MOQ_TEST_CHECK(r.fet->rx_fin_count == fins + 1);
        no_actions(&r);
    }
    /* A known finished ref must fail, including at full admission capacity. */
    (void)input(&r, rcbuf, ref, replies[0].data, 1, false);
    MOQ_TEST_CHECK(moq_session_state(r.fet) == MOQ_SESS_CLOSED);
    moq_action_t a = {0};
    size_t count = moq_session_poll_actions(r.fet, &a, 1);
    MOQ_TEST_CHECK(count == 1);
    MOQ_TEST_CHECK(a.kind == MOQ_ACTION_CLOSE_SESSION && a.u.close_session.code == 0x3);
    if (a.kind == MOQ_ACTION_CLOSE_SESSION) {
        MOQ_TEST_CHECK(a.u.close_session.reason.len == strlen("data after FIN"));
        if (a.u.close_session.reason.len == strlen("data after FIN"))
            MOQ_TEST_CHECK(memcmp(a.u.close_session.reason.data, "data after FIN",
                                  a.u.close_session.reason.len) == 0);
    }
    if (count) moq_action_cleanup(&a);
    MOQ_TEST_CHECK(input(&r, rcbuf, replies[2].ref, NULL, 0, false) == MOQ_ERR_CLOSED);
    raw_down(&r);
    MOQ_TEST_CHECK(r.ta.balance == 0);
    printf("%s: d%d %s %s\n", failures == before ? "PASS" : "FAIL",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, rcbuf ? "rcbuf" : "bytes",
           finished ? "finished-before-admission" : "fin-only-replay");
}

static void run_owned_stop(moq_version_t version, bool fin_while_owed)
{
    int before = failures;
    raw_t r;
    if (!open_pair(&r, version)) { MOQ_TEST_CHECK(false); raw_down(&r); return; }
    ledger_t o = {0};
    response_t replies[3] = {0};
    make_responses(&r, &o, replies);
    MOQ_TEST_CHECK(moq_session_fetch_cancel(r.fet, o.handles[0], ++r.now) == MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(r.fet, &a, 1)) moq_action_cleanup(&a);
    test_session_fill_action_queue(r.fet);
    response_t *late = &replies[0];
    MOQ_TEST_CHECK(input(&r, false, late->ref, late->data, late->first_len, false) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_session_has_transport_stream(r.fet, late->ref));
    if (fin_while_owed)
        MOQ_TEST_CHECK(input(&r, false, late->ref, NULL, 0, true) == MOQ_ERR_WOULD_BLOCK);
    unsigned stops = 0;
    while (moq_session_poll_actions(r.fet, &a, 1)) {
        if (a.kind == MOQ_ACTION_STOP_DATA) {
            stops++;
            MOQ_TEST_CHECK(a.u.stop_data.stream_ref._v == late->ref._v);
        } else MOQ_TEST_CHECK(a.kind == MOQ_ACTION_SEND_CONTROL);
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(stops == 1);
    if (!fin_while_owed) {
        MOQ_TEST_CHECK(moq_session_has_transport_stream(r.fet, late->ref));
        MOQ_TEST_CHECK(input(&r, false, late->ref, late->data + late->first_len,
                             late->len - late->first_len, false) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_data_reset(r.fet, late->ref, 1, ++r.now) == MOQ_OK);
    }
    MOQ_TEST_CHECK(!moq_session_has_transport_stream(r.fet, late->ref));
    moq_event_t event;
    MOQ_TEST_CHECK(moq_session_poll_events(r.fet, &event, 1) == 0);
    no_actions(&r);
    MOQ_TEST_CHECK(moq_session_state(r.fet) == MOQ_SESS_ESTABLISHED);
    raw_down(&r);
    MOQ_TEST_CHECK(r.ta.balance == 0);
    printf("%s: d%d owned-stop %s\n", failures == before ? "PASS" : "FAIL",
           version == MOQ_VERSION_DRAFT_16 ? 16 : 18, fin_while_owed ? "deferred-fin" : "reset");
}

int main(void)
{
    for (int draft = 0; draft < 2; draft++) {
        moq_version_t version = draft ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        for (int rcbuf = 0; rcbuf < 2; rcbuf++) {
            for (int fragmented = 0; fragmented < 2; fragmented++)
                for (int pressure = 0; pressure < 2; pressure++)
                    run_replay(version, rcbuf != 0, fragmented != 0, pressure != 0);
            run_terminals(version, rcbuf != 0, false);
            run_terminals(version, rcbuf != 0, true);
        }
        run_owned_stop(version, false);
        run_owned_stop(version, true);
    }
    MOQ_TEST_PASS("session_rx_admission");
    return failures ? 1 : 0;
}
