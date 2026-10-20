#include <moq/rcbuf.h>

static moq_result_t copy_create(const moq_alloc_t *, const uint8_t *, size_t,
                                moq_rcbuf_t **);
/* Reuse the real ACK/session setup and its unchanged ACK-failure controls. Only this
 * translation unit observes receiver copy allocation; no production seam. */
#define moq_rcbuf_create copy_create
#define main receiver_ack_failure_controls_main
#include "test_media_receiver_ack_failure.c"
#undef main
#undef moq_rcbuf_create

static struct {
    bool active, fail, disable, corrupt, reenable, poll_during_alloc;
    unsigned allocations, frees;
    uint64_t incoming;
    moq_media_track_t *track;
    moq_alloc_t parent;
} copy_watch;

static uint64_t charged(const moq_media_receiver_t *r)
{
#ifdef MOQ_RECEIVER_METADATA_BASELINE
    return r->obj_sizes_total;
#else
    return r->obj_budget_total;
#endif
}

static uint64_t queued_cost(const moq_media_receiver_t *r)
{
    uint64_t n = 0;
    for (uint32_t i = r->obj_head; i != r->obj_tail; i++) {
        const moq_media_object_t *o = &r->objs[i % r->ring_cap].obj;
        n += o->payload_ref ? moq_rcbuf_len(o->payload_ref) : 0;
        n += o->track_properties_ref ? moq_rcbuf_len(o->track_properties_ref) : 0;
    }
    return n;
}

static void *copy_alloc(size_t n, void *ctx)
{
    (void)ctx;
    copy_watch.allocations++;
    /* This is the actual allocator callback, not an observation after copying.
     * It also proves no receiver lock is held across allocation. */
    int lock = pthread_mutex_trylock(&current->r->mu);
    MOQ_TEST_CHECK(lock == 0);
    MOQ_TEST_CHECK(charged(current->r) == queued_cost(current->r) + copy_watch.incoming);
    if (lock == 0) pthread_mutex_unlock(&current->r->mu);
    if (copy_watch.poll_during_alloc) {
        unsigned wakes = current->wakes;
        moq_media_object_t out;
        MOQ_TEST_CHECK(moq_media_receiver_poll_object(current->r, &out, sizeof(out)) == MOQ_OK);
        moq_media_object_cleanup(&out);
        MOQ_TEST_CHECK(current->r->obj_head == current->r->obj_tail);
        MOQ_TEST_CHECK(charged(current->r) == copy_watch.incoming &&
            charged(current->r) > current->r->byte_cap / 2);
        MOQ_TEST_CHECK(current->r->paused && !current->r->resume_nudged && current->wakes == wakes);
    }
    if (copy_watch.disable)
        MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(current->r, copy_watch.track) == MOQ_OK);
    if (copy_watch.reenable)
        MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(current->r, copy_watch.track, NULL) == MOQ_OK);
    return copy_watch.fail ? NULL : copy_watch.parent.alloc(n, copy_watch.parent.ctx);
}

static void copy_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    copy_watch.frees++;
    copy_watch.parent.free(p, n, copy_watch.parent.ctx);
}

static moq_result_t copy_create(const moq_alloc_t *a, const uint8_t *p,
                                size_t n, moq_rcbuf_t **out)
{
    if (!copy_watch.active) return moq_rcbuf_create(a, p, n, out);
    copy_watch.parent = *a;
    moq_alloc_t observed = {.ctx = NULL, .alloc = copy_alloc, .free = copy_free};
    moq_result_t rc = moq_rcbuf_create(&observed, p, n, out);
    if (rc == MOQ_OK && copy_watch.corrupt)
        memset((uint8_t *)moq_rcbuf_data(*out), 0xff, n);
    return rc;
}

static moq_rcbuf_t *track_block(uint64_t scale, uint8_t tag)
{
    uint8_t video[] = {tag, 0, 0xfe}, audio[] = {0x12, tag};
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_PRESENT_MASK;
    loc.timestamp = 999; loc.timescale = scale;
    loc.marking_len = 1; loc.marking[0] = 0x20; loc.audio_level = 0x83;
    loc.video_config = (moq_bytes_t){video, sizeof(video)};
    loc.audio_config = (moq_bytes_t){audio, sizeof(audio)};
    moq_rcbuf_t *out = NULL;
    MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_04, &loc, &out) == MOQ_OK);
    return out;
}

static void accept_block(fixture_t *f, unsigned i, const moq_rcbuf_t *block)
{
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    if (block) ac.track_properties = (moq_bytes_t){moq_rcbuf_data(block), moq_rcbuf_len(block)};
    MOQ_TEST_CHECK(moq_session_accept_subscribe(server(f), f->peer[i].sub, &ac, now(f)) == MOQ_OK);
    settle(f);
}

static bool metadata_start(fixture_t *f, const moq_rcbuf_t *block)
{
    memset(&copy_watch, 0, sizeof(copy_watch));
    bool ready = start(f, MOQ_VERSION_DRAFT_22, 0) && bootstrap(f);
    MOQ_TEST_CHECK(ready);
    if (!ready) return false;
    accept_block(f, 1, block);
    accept(f, 2, false); settle(f);
    drain_events(f, MOQ_OK);
    return true;
}

static void input_object(fixture_t *f, moq_media_track_t *t, uint64_t group,
                          size_t payload_len, bool override, bool keyframe)
{
    uint8_t *payload = calloc(payload_len ? payload_len : 1, 1);
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return;
    moq_sub_object_t so = {0};
    so.track = t->sub_track; so.group_id = group; so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), payload, payload_len, &so.payload) == MOQ_OK);
    free(payload);
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    loc.timestamp = 2000; loc.marking_len = 1; loc.marking[0] = keyframe ? 0x20 : 0;
    if (override) {
        loc.present |= MOQ_LOC_METADATA_HAS_TIMESCALE | MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        loc.timescale = 2000;
        loc.video_config = (moq_bytes_t){NULL, 0};
    }
    MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_04, &loc, &so.properties) == MOQ_OK);
    moq_sub_track_result_t result;
    MOQ_TEST_CHECK(moq_sub_get_track_result(f->r->sub, so.track, &result, sizeof(result)) == MOQ_OK);
    copy_watch.incoming = payload_len + result.track_properties.len;
    copy_watch.track = t; copy_watch.active = true;
    receiver_on_media_object(f->r, t, &so, now(f));
    copy_watch.active = false;
    MOQ_TEST_CHECK(charged(f->r) == queued_cost(f->r));
}

static void drain_objects(fixture_t *f)
{
    moq_media_object_t out;
    while (moq_media_receiver_poll_object(f->r, &out, sizeof(out)) == MOQ_OK)
        moq_media_object_cleanup(&out);
    MOQ_TEST_CHECK(charged(f->r) == 0 && f->r->obj_sizes_total == 0);
}

static void check_backing(const moq_media_object_t *o, const moq_rcbuf_t *block,
                           uint64_t scale, uint8_t tag)
{
    MOQ_TEST_CHECK(o->track_properties_ref != NULL);
    if (!o->track_properties_ref) return;
    const uint8_t *p = moq_rcbuf_data(o->track_properties_ref);
    size_t n = moq_rcbuf_len(o->track_properties_ref);
    MOQ_TEST_CHECK(n == moq_rcbuf_len(block));
    MOQ_TEST_CHECK(n == moq_rcbuf_len(block) && memcmp(p, moq_rcbuf_data(block), n) == 0);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(o->track_properties_ref) == 1);
    MOQ_TEST_CHECK(o->metadata.loc.timescale == scale);
    const moq_bytes_t a = o->metadata.loc.audio_config;
    MOQ_TEST_CHECK(a.len == 2 && (uintptr_t)a.data >= (uintptr_t)p &&
        (uintptr_t)a.data + a.len <= (uintptr_t)p + n);
    if (a.len == 2) MOQ_TEST_CHECK(a.data[0] == 0x12 && a.data[1] == tag);
    MOQ_TEST_CHECK(!(o->metadata.loc.present & MOQ_LOC_METADATA_HAS_AUDIO_LEVEL));
}

static void exclusive_copy(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xab);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    input_object(&f, f.bad, 1, 1, false, true);
    input_object(&f, f.bad, 2, 1, true, true);
    MOQ_TEST_CHECK(copy_watch.allocations == 2);
    MOQ_TEST_CHECK(f.r->obj_sizes_total == 2);
    moq_media_object_t a = {0}, b = {0};
    const size_t short_sizes[] = {0, MEDIA_OBJECT_V0_SIZE,
        offsetof(moq_media_object_t, metadata), MEDIA_OBJECT_METADATA_SIZE - 1};
    for (size_t i = 0; i < sizeof(short_sizes) / sizeof(short_sizes[0]); i++) {
        memset(&a, 0xa5, sizeof(a)); b = a;
        MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &a, short_sizes[i]) == MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(memcmp(&a, &b, sizeof(a)) == 0 && f.r->obj_tail - f.r->obj_head == 2);
    }
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &a, sizeof(a)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &b, sizeof(b)) == MOQ_OK);
    check_backing(&a, block, 1000, 0xab); check_backing(&b, block, 2000, 0xab);
    MOQ_TEST_CHECK(a.track_properties_ref != b.track_properties_ref);
    MOQ_TEST_CHECK(a.metadata.time_us == 2000000 && b.metadata.time_us == 1000000);
    MOQ_TEST_CHECK(a.metadata.loc.video_config.len == 3 && b.metadata.loc.video_config.len == 0);
    MOQ_TEST_CHECK(a.properties_ref != a.track_properties_ref);
    moq_sub_track_result_t result;
    MOQ_TEST_CHECK(moq_sub_get_track_result(f.r->sub, f.bad->sub_track, &result, sizeof(result)) == MOQ_OK);
    /* Deliberately poison the facade borrow after transfer, without parsing it
     * again. Neither returned object's storage may depend on this source. */
    if (result.track_properties.len)
        memset((uint8_t *)result.track_properties.data, 0xdd, result.track_properties.len);
    check_backing(&a, block, 1000, 0xab); check_backing(&b, block, 2000, 0xab);
    MOQ_TEST_CHECK(charged(f.r) == 0);
    /* Keep the ledger context alive but destroy all source owners first. */
    f.probe = NULL; f.r->ep = NULL;
    moq_media_receiver_test_destroy_with_session(f.r, client(&f), now(&f)); f.r = NULL;
    moq_endpoint_test_free_bare(f.ep); f.ep = NULL;
    moq_simpair_destroy(f.pair); f.pair = NULL;
    check_backing(&a, block, 1000, 0xab); check_backing(&b, block, 2000, 0xab);
    moq_media_object_cleanup(&a); moq_media_object_cleanup(&b);
    MOQ_TEST_CHECK(copy_watch.frees == 2);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("exclusive_copy_precedence_poison_destroy_and_short_output");
}

static void policy_boundaries(moq_media_overflow_policy_t policy)
{
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    size_t m = moq_rcbuf_len(block), cost = m + 1;
    fixture_t f;
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.r->overflow.policy = policy; f.r->byte_cap = 2 * cost;
    input_object(&f, f.bad, 1, 1, false, true);
    input_object(&f, f.bad, 2, 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == 2 * cost && !f.r->paused);
    input_object(&f, f.bad, 3, 1, false, true);
    uint32_t count = policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL ? 3 : 2;
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == count);
    MOQ_TEST_CHECK(charged(f.r) == count * cost);
    moq_media_receiver_stats_t stats = {0};
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f.r, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.bytes_queued == count && stats.overflow_events == 1);
    if (policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL) {
        MOQ_TEST_CHECK(f.r->paused);
        receiver_reconcile_delivery(f.r, now(&f));
        MOQ_TEST_CHECK(f.r->paused && charged(f.r) == 3 * cost);
    }
    input_object(&f, f.bad, 4, 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == count * cost && copy_watch.frees == 4 - count);
    drain_objects(&f);
    /* Odd B, the exact ceiling, and one byte beyond it, on an empty queue. */
    f.r->byte_cap = 2 * cost + 1;
    uint64_t limit = f.r->byte_cap;
    if (policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL) limit += limit / 2;
    input_object(&f, f.bad, 5, (size_t)limit - m, false, true);
    MOQ_TEST_CHECK(charged(f.r) == limit);
    drain_objects(&f);
    unsigned calls = copy_watch.allocations;
    input_object(&f, f.bad, 6, (size_t)limit - m + 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == 0 && copy_watch.allocations == calls);
    drain_objects(&f);
    /* Metadata alone exceeds the bound while the payload would fit. */
    f.r->byte_cap = m / 2;
    calls = copy_watch.allocations;
    input_object(&f, f.bad, 7, 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == 0 && copy_watch.allocations == calls);
    drain_objects(&f);
    MOQ_TEST_CHECK(copy_watch.frees == copy_watch.allocations);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("combined_budget_policy_threshold_ceiling_odd_and_refusal");
}

static void local_copy_failures(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xcd);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    copy_watch.fail = true;
    input_object(&f, f.bad, 1, 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.r->obj_head == f.r->obj_tail);
    MOQ_TEST_CHECK(copy_watch.allocations == 1 && f.bad->local_result == MOQ_OK && !f.bad->ended);
    MOQ_TEST_CHECK(f.r->stats.objects_received == 1 && f.r->stats.objects_dropped == 1);
    MOQ_TEST_CHECK(f.bad->parse_drops == 1 && f.r->local_failure_pending_count == 0);
    moq_media_track_event_t ev;
    MOQ_TEST_CHECK(moq_media_receiver_poll_track(f.r, &ev, sizeof(ev)) == MOQ_OK);
    MOQ_TEST_CHECK(ev.kind == MOQ_MEDIA_TRACK_PARSE_DROP && ev.track == f.bad &&
        ev.parse_drop_class == MOQ_MEDIA_PARSE_DROP_MEDIA && ev.parse_drops_delta == 1);
    moq_media_track_state_t state;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f.r, f.bad, &state) == MOQ_OK &&
        state == MOQ_MEDIA_TRACK_STATE_ACTIVE && !moq_media_receiver_is_fatal(f.r));
    copy_watch.fail = false;
    input_object(&f, f.bad, 2, 1, false, true);
    MOQ_TEST_CHECK(charged(f.r) == moq_rcbuf_len(block) + 1);
    copy_watch.disable = true;
    input_object(&f, f.bad, 3, 1, false, true);
    copy_watch.disable = false;
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.r->obj_head == f.r->obj_tail);
    MOQ_TEST_CHECK(copy_watch.frees == 2);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_OK);
    copy_watch.corrupt = true;
    input_object(&f, f.bad, 4, 1, false, true);
    copy_watch.corrupt = false;
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.bad->parse_drops == 2);
    copy_watch.disable = true; copy_watch.reenable = true;
    input_object(&f, f.bad, 5, 1, false, true);
    copy_watch.disable = false; copy_watch.reenable = false;
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.bad->desired_subscribed);
    input_object(&f, f.bad, 6, 1, false, true);
    finish(&f);
    MOQ_TEST_CHECK(copy_watch.frees + 1 == copy_watch.allocations);
    moq_rcbuf_decref(block);
    MOQ_TEST_PASS("copy_oom_reparse_failure_disable_reenable_during_conversion_and_destroy");
}

static void compact_and_filter(moq_media_overflow_policy_t policy)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    size_t cost = moq_rcbuf_len(block) + 1;
    f.r->overflow.policy = policy;
    f.r->byte_cap = (policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL ? 2 : 3) * cost;
    input_object(&f, f.bad, 1, 1, false, true);
    input_object(&f, f.bad, 2, 1, false, true);
    input_object(&f, f.bad, 1, 1, false, false);
    input_object(&f, f.bad, 3, 1, false, true);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 2 && charged(f.r) == 2 * cost);
    MOQ_TEST_CHECK(copy_watch.allocations == 4 && copy_watch.frees == 2);
    input_object(&f, f.good, 4, 1, false, true);
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    MOQ_TEST_CHECK(charged(f.r) == 1 && f.r->obj_sizes_total == 1);
    moq_media_object_t out;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK && out.track == f.good);
    moq_media_object_cleanup(&out);
    MOQ_TEST_CHECK(charged(f.r) == 0 && copy_watch.allocations == copy_watch.frees);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_OK);
    unsigned calls = copy_watch.allocations;
    uint64_t dropped = f.r->stats.objects_dropped;
    input_object(&f, f.bad, 5, (size_t)f.r->byte_cap * 2, false, true);
    input_object(&f, f.bad, 5, 1, false, false);
    MOQ_TEST_CHECK(copy_watch.allocations == calls && f.r->stats.objects_dropped == dropped + 2);
    MOQ_TEST_CHECK(charged(f.r) == 0);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("interleaved_group_compaction_straggler_and_track_purge_balance");
}

static void wire_and_fill(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xad);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    send_payload(&f, 1, 3, "live", 4, true); pump(&f);
    MOQ_TEST_CHECK(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
    moq_fetch_object_cfg_t cfg;
    moq_fetch_object_cfg_init(&cfg);
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"fill", 4, &cfg.payload) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_fill_object(server(&f), f.peer[1].fill, &cfg, now(&f)) == MOQ_OK);
    moq_rcbuf_decref(cfg.payload);
    MOQ_TEST_CHECK(moq_session_end_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
    settle(&f);
    moq_media_object_t a, b;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &a, sizeof(a)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &b, sizeof(b)) == MOQ_OK);
    check_backing(&a, block, 1000, 0xad); check_backing(&b, block, 1000, 0xad);
    MOQ_TEST_CHECK(a.track_properties_ref != b.track_properties_ref);
    MOQ_TEST_CHECK(a.metadata.loc.timestamp == 12345 && b.metadata.loc.timestamp == 0);
    MOQ_TEST_CHECK(!(b.metadata.loc.present & (MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING)));
    MOQ_TEST_CHECK(a.payload.len == 4 && memcmp(a.payload.data, "live", 4) == 0);
    MOQ_TEST_CHECK(b.payload.len == 4 && memcmp(b.payload.data, "fill", 4) == 0);
    moq_media_object_cleanup(&a); moq_media_object_cleanup(&b);
    MOQ_TEST_CHECK(charged(f.r) == 0);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("real_live_and_fill_delivery_inherit_selected_ack");
}

static void legacy_budget(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true); settle(&f);
    f.r->byte_cap = 4; f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    for (uint64_t g = 1; g <= 3; g++) { send_payload(&f, 1, g, "ok", 2, true); pump(&f); }
    MOQ_TEST_CHECK(charged(f.r) == 6 && f.r->obj_sizes_total == 6);
    moq_media_object_t out;
    unsigned count = 0;
    while (moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK) {
        MOQ_TEST_CHECK(out.track_properties_ref == NULL && out.metadata.struct_size == 0);
        moq_media_object_cleanup(&out); count++;
    }
    MOQ_TEST_CHECK(count == 3 && charged(f.r) == 0);
    finish(&f);
    MOQ_TEST_PASS("legacy_nonempty_ack_still_uses_payload_only_budget");
}

static void empty_and_generation(void)
{
    fixture_t f;
    if (!metadata_start(&f, NULL)) { finish(&f); return; }
    input_object(&f, f.bad, 1, 1, false, true);
    MOQ_TEST_CHECK(copy_watch.allocations == 0 && charged(f.r) == 1);
    moq_media_object_t out;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK);
    MOQ_TEST_CHECK(out.track_properties_ref == NULL && out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US);
    moq_media_object_cleanup(&out); finish(&f);

    moq_rcbuf_t *block = track_block(1000, 0xde), *next = track_block(4000, 0xef);
    if (!metadata_start(&f, block)) { finish(&f); goto done; }
    input_object(&f, f.bad, 1, 1, false, true);
    moq_media_object_t old;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &old, sizeof(old)) == MOQ_OK);
    moq_sub_track_t *prior = f.bad->sub_track;
    MOQ_TEST_CHECK(moq_session_abort_fill(server(&f), f.peer[1].fill, 0, now(&f)) == MOQ_OK);
    settle(&f);
    MOQ_TEST_CHECK(moq_sub_unsubscribe(f.r->sub, prior, now(&f)) == MOQ_OK);
    network(&f);
    /* No queued facade items or child FILL remain in this generation. */
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(prior)));
    MOQ_TEST_CHECK(moq_sub_release_track(f.r->sub, prior) == MOQ_OK);
    f.bad->sub_track = NULL; f.probe = NULL;
    receiver_reconcile_subscriptions(f.r, now(&f)); network(&f);
    MOQ_TEST_CHECK(f.peer[1].requests == 2 && f.bad->sub_track != NULL);
    accept_block(&f, 1, next);
    input_object(&f, f.bad, 2, 1, false, true);
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK);
    check_backing(&old, block, 1000, 0xde); check_backing(&out, next, 4000, 0xef);
    MOQ_TEST_CHECK(out.metadata.time_us == 500000);
    moq_media_object_cleanup(&old); moq_media_object_cleanup(&out);
    finish(&f);
done:
    moq_rcbuf_decref(block); moq_rcbuf_decref(next);
    MOQ_TEST_PASS("empty_ack_no_copy_and_selected_subscription_generation");
}

static void cmaf_copy(void)
{
    static const uint8_t fragment[] = {
        0,0,0,84,'m','o','o','f', 0,0,0,76,'t','r','a','f',
        0,0,0,16,'t','f','h','d', 0,0,0,0, 0,0,0,1,
        0,0,0,20,'t','f','d','t', 1,0,0,0, 0,0,0,0, 0,1,0x5f,0x90,
        0,0,0,32,'t','r','u','n', 0,0,0x0f,0, 0,0,0,1,
        0,0,0x11,0x94, 0,0,0,4, 2,0,0,0, 0xff,0xff,0xee,0x6c,
        0,0,0,12,'m','d','a','t', 0x41,0x42,0x43,0x44
    };
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xfa);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.bad->desc.info.packaging = MOQ_MEDIA_PACKAGING_CMAF; f.bad->desc.info.timescale = 90000;
    moq_sub_object_t so = {0}; so.track = f.bad->sub_track;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), fragment, sizeof(fragment), &so.payload) == MOQ_OK);
    /* Public receiver construction uses the default allocator, including the
     * sample table cleaned by public object_cleanup. Keep that contract here. */
    moq_alloc_t saved = f.r->alloc; f.r->alloc = *moq_alloc_default();
    copy_watch.active = true; copy_watch.incoming = sizeof(fragment) + moq_rcbuf_len(block);
    receiver_on_media_object(f.r, f.bad, &so, now(&f)); copy_watch.active = false;
    moq_media_object_t out;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK);
    f.r->alloc = saved;
    check_backing(&out, block, 1000, 0xfa);
    MOQ_TEST_CHECK(out.sample_count == 1 && out.decode_time_us == 1000000 && out.presentation_time_us == 950000);
    MOQ_TEST_CHECK(charged(f.r) == 0);
    finish(&f);
    check_backing(&out, block, 1000, 0xfa);
    moq_media_object_cleanup(&out);
    MOQ_TEST_CHECK(copy_watch.allocations == 1 && copy_watch.frees == 1);
    moq_rcbuf_decref(block);
    MOQ_TEST_PASS("cmaf_inheritance_owns_bytes_without_replacing_fragment_timing");
}

static void arithmetic_limits(void)
{
#ifndef MOQ_RECEIVER_METADATA_BASELINE
    moq_media_receiver_t r = {0};
    r.obj_budget_total = UINT64_MAX - 2;
    MOQ_TEST_CHECK(obj_bytes_fit(&r, 2, UINT64_MAX));
    MOQ_TEST_CHECK(!obj_bytes_fit(&r, 3, UINT64_MAX));
    MOQ_TEST_CHECK(!obj_bytes_fit(&r, UINT64_MAX, UINT64_MAX - 1));
    obj_uncharge_locked(&r, UINT64_MAX - 2);
    MOQ_TEST_CHECK(r.obj_budget_total == 0 && obj_bytes_fit(&r, UINT64_MAX, UINT64_MAX));
#endif
    MOQ_TEST_PASS("byte_comparisons_and_charge_settlement_near_uint64_max");
}

typedef struct forward_counts {
    unsigned pause[2], resume[2];
} forward_counts_t;

/* Observe real peer events and let the real automatic session ACK return to
 * the facade. Repeated cycles must not manufacture extra Forward changes. */
static void forward_cycles(fixture_t *f, forward_counts_t *counts)
{
    for (unsigned cycle = 0; cycle < 4; cycle++) {
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(server(f), &ev, 1)) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                for (unsigned i = 0; i < 2; i++) {
                    if (!moq_subscription_eq(ev.u.subscribe_updated.sub, f->peer[i + 1].sub)) continue;
                    MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward && !ev.u.subscribe_updated.answer_owed);
                    if (ev.u.subscribe_updated.forward) counts->resume[i]++;
                    else counts->pause[i]++;
                }
            }
            moq_event_cleanup(&ev);
        }
        pump(f);
    }
}

static void poll_one(fixture_t *f)
{
    moq_media_object_t out;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f->r, &out, sizeof(out)) == MOQ_OK);
    moq_media_object_cleanup(&out);
}

static void byte_resume(bool odd)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    MOQ_TEST_CHECK(moq_rcbuf_len(block) == 21);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    f.r->byte_cap = odd ? 45 : 44;
    forward_counts_t counts = {0};
    for (unsigned i = 1; i <= 3; i++) input_object(&f, f.bad, i, odd && i == 3 ? 2 : 1, false, true);
    MOQ_TEST_CHECK(f.r->paused && charged(f.r) == (odd ? 67 : 66));
    receiver_reconcile_delivery(f.r, now(&f)); forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(f.r->paused && f.r->stats.pause_transitions == 1);
    MOQ_TEST_CHECK(counts.pause[0] == 1 && counts.pause[1] == 1 && counts.resume[0] == 0 && counts.resume[1] == 0);
    unsigned wakes = f.wakes;
    poll_one(&f);
    MOQ_TEST_CHECK(charged(f.r) == (odd ? 45 : 44) && f.wakes == wakes && !f.r->resume_nudged);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(f.r->paused && counts.resume[0] == 0 && counts.resume[1] == 0);
    poll_one(&f);
    MOQ_TEST_CHECK(charged(f.r) == (odd ? 23 : 22));
    if (odd) {
        MOQ_TEST_CHECK(f.wakes == wakes && !f.r->resume_nudged);
        forward_cycles(&f, &counts);
        MOQ_TEST_CHECK(f.r->paused && counts.resume[0] == 0 && counts.resume[1] == 0);
        poll_one(&f);
        MOQ_TEST_CHECK(charged(f.r) == 0);
    }
    MOQ_TEST_CHECK(f.r->paused && f.r->resume_nudged && f.wakes == wakes + 1);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(!f.r->paused && f.r->stats.pause_transitions == 2);
    MOQ_TEST_CHECK(counts.pause[0] == 1 && counts.pause[1] == 1 && counts.resume[0] == 1 && counts.resume[1] == 1);
    if (!odd) poll_one(&f);
    forward_cycles(&f, &counts);
    moq_media_object_t out;
    wakes = f.wakes;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_DONE && f.wakes == wakes);
    MOQ_TEST_CHECK(counts.resume[0] == 1 && counts.resume[1] == 1 && f.r->stats.pause_transitions == 2);
    fprintf(stderr, "HYSTERESIS B=%llu initial=%u final=%llu peer_pause=%u peer_resume=%u\n",
        (unsigned long long)f.r->byte_cap, odd ? 67u : 66u,
        (unsigned long long)charged(f.r), counts.pause[0], counts.resume[0]);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("d22_byte_half_threshold_real_forward_ack_and_odd_floor");
}

static void object_resume(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    f.r->byte_cap = 1000; f.r->obj_cap = 5;
    forward_counts_t counts = {0};
    for (unsigned i = 1; i <= 6; i++) input_object(&f, f.bad, i, 1, false, true);
    MOQ_TEST_CHECK(f.r->paused && charged(f.r) < f.r->byte_cap / 2);
    receiver_reconcile_delivery(f.r, now(&f)); forward_cycles(&f, &counts);
    unsigned wakes = f.wakes;
    for (unsigned i = 0; i < 3; i++) {
        poll_one(&f);
        MOQ_TEST_CHECK(f.wakes == wakes && !f.r->resume_nudged);
    }
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(f.r->paused && f.r->obj_tail - f.r->obj_head == 3 && counts.resume[0] == 0);
    poll_one(&f);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 2 && f.r->resume_nudged && f.wakes == wakes + 1);
    poll_one(&f); poll_one(&f);
    MOQ_TEST_CHECK(f.wakes == wakes + 1 && f.r->resume_nudged);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(!f.r->paused && counts.pause[0] == 1 && counts.resume[0] == 1 &&
        counts.pause[1] == 1 && counts.resume[1] == 1 && f.r->stats.pause_transitions == 2);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("d22_object_pressure_odd_depth_floor_and_nudge_deduplication");
}

static void reservation_resume(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL; f.r->byte_cap = 44;
    forward_counts_t counts = {0};
    for (unsigned i = 1; i <= 3; i++) input_object(&f, f.bad, i, 1, false, true);
    receiver_reconcile_delivery(f.r, now(&f)); forward_cycles(&f, &counts);
    poll_one(&f);
    MOQ_TEST_CHECK(f.r->paused && charged(f.r) == 44 && !f.r->resume_nudged);
    /* The next 23-byte admission evicts one group. The allocator observes one
     * queued 22-byte object plus its reservation, polls the queued object, then
     * refuses the allocation. Empty queue is NOT zero owned bytes until rollback. */
    copy_watch.poll_during_alloc = true; copy_watch.fail = true;
    input_object(&f, f.bad, 4, 2, false, true);
    copy_watch.poll_during_alloc = false; copy_watch.fail = false;
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.r->paused && !f.r->resume_nudged);
    MOQ_TEST_CHECK(f.bad->parse_drops == 1 && f.bad->local_result == MOQ_OK);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(!f.r->paused && counts.pause[0] == 1 && counts.resume[0] == 1 &&
        counts.pause[1] == 1 && counts.resume[1] == 1 && f.r->stats.pause_transitions == 2);
    MOQ_TEST_CHECK(copy_watch.allocations == copy_watch.frees + 1);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("d22_inflight_charge_blocks_nudge_until_copy_failure_rolls_back_to_zero");
}

static void purge_resume(void)
{
    fixture_t f;
    moq_rcbuf_t *block = track_block(1000, 0xbc);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL; f.r->byte_cap = 44;
    forward_counts_t counts = {0};
    for (unsigned i = 1; i <= 3; i++) input_object(&f, f.bad, i, 1, false, true);
    receiver_reconcile_delivery(f.r, now(&f)); forward_cycles(&f, &counts);
    unsigned wakes = f.wakes;
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    MOQ_TEST_CHECK(charged(f.r) == 0 && f.r->paused && f.wakes == wakes + 1);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(!f.r->paused && counts.pause[0] == 1 && counts.resume[0] == 0 &&
        counts.pause[1] == 1 && counts.resume[1] == 1 && f.r->stats.pause_transitions == 2);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_OK);
    forward_cycles(&f, &counts);
    MOQ_TEST_CHECK(counts.resume[0] == 1 && counts.resume[1] == 1 && f.r->stats.pause_transitions == 2);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("d22_purge_resumes_only_enabled_tracks_once");
}

static void legacy_resume(moq_version_t version)
{
    fixture_t f;
    bool ready = start(&f, version, 0) && bootstrap(&f);
    MOQ_TEST_CHECK(ready);
    if (!ready) { finish(&f); return; }
    accept(&f, 1, true); settle(&f);
    f.r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL; f.r->byte_cap = 4;
    for (uint64_t g = 1; g <= 4; g++) {
        send_payload(&f, 1, g, "ok", 2, true);
        MOQ_TEST_CHECK(moq_sub_tick(f.r->sub, now(&f)) == MOQ_OK);
        moq_sub_object_t so;
        MOQ_TEST_CHECK(moq_sub_poll_object(f.r->sub, &so) == MOQ_OK);
        receiver_on_media_object(f.r, f.bad, &so, now(&f));
        if (g == 3) {
            MOQ_TEST_CHECK(f.r->paused && charged(f.r) == 6);
            receiver_reconcile_delivery(f.r, now(&f));
            MOQ_TEST_CHECK(!f.r->paused && charged(f.r) == 6);
        }
    }
    MOQ_TEST_CHECK(f.r->paused && charged(f.r) == 6);
    unsigned wakes = f.wakes;
    poll_one(&f);
    MOQ_TEST_CHECK(charged(f.r) == 4 && f.wakes == wakes + 1 && f.r->resume_nudged);
    receiver_reconcile_delivery(f.r, now(&f));
    MOQ_TEST_CHECK(!f.r->paused && charged(f.r) == 4 && f.r->stats.pause_transitions == 4);
    drain_objects(&f); finish(&f);
    MOQ_TEST_PASS("legacy_depth_only_resume_and_nudge_ignore_byte_half");
}

int main(void)
{
    exclusive_copy();
    const moq_media_overflow_policy_t policies[] = {MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME,
        MOQ_MEDIA_OVERFLOW_DROP_GROUP, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL};
    for (size_t i = 0; i < sizeof(policies) / sizeof(policies[0]); i++) {
        policy_boundaries(policies[i]); compact_and_filter(policies[i]);
    }
    local_copy_failures(); empty_and_generation(); cmaf_copy(); wire_and_fill();
    legacy_budget(MOQ_VERSION_DRAFT_16); legacy_budget(MOQ_VERSION_DRAFT_18);
    arithmetic_limits();
    byte_resume(false); byte_resume(true); object_resume(); reservation_resume(); purge_resume();
    legacy_resume(MOQ_VERSION_DRAFT_16); legacy_resume(MOQ_VERSION_DRAFT_18);
    fprintf(stderr, "receiver metadata copy: %d failures\n", failures);
    return failures ? 1 : 0;
}
