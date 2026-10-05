/*
 * Draft-21 request layer through the d21 profile: how the session core's filter
 * and fetch-range model maps onto Location Filters, FILL_PARAMETERS and Range
 * Filters, and how SUBSCRIBE, REQUEST_UPDATE, FETCH, PUBLISH and PUBLISH_OK,
 * PUBLISH_STATE_NOTIFY and REQUEST_ERROR behave end to end on a request bidi.
 *
 * Wire inputs are built with the draft-21 codec (byte vectors pinned in
 * test_control_d21); this file checks what the PROFILE and SESSION do with them.
 * Where the core's four-type filter model cannot express a draft-21 request the
 * mapping is APPROXIMATED and flagged, and the exact wire fields are surfaced
 * beside it; those cases are asserted explicitly so the gap cannot widen silently
 * (plan Task 6.1 and Task 7 finish them).
 */
#include <moq/moq.h>
#include <moq/control_d21.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/profile.h"
#include "../../core/src/session/profile_d21_internal.h"

static int failures = 0;

static moq_bytes_t lit(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

/* ===================================================================== *
 * Part 1. The mapping helpers, directly
 * ===================================================================== */

static moq_d21_location_filter_t lf(uint8_t n, uint64_t sg, uint64_t so, uint64_t dg, uint64_t eo)
{
    moq_d21_location_filter_t f;
    memset(&f, 0, sizeof(f));
    f.field_count = n; f.start_group = sg; f.start_object = so;
    f.end_group_delta = dg; f.end_object = eo;
    return f;
}

static void t_filter_to_wire(void)
{
    moq_d21_location_filter_t f;
    bool emit;

    /* The four semantic types and how each is written (9.20.10). */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 9, 9, 9, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 1 && f.start_group == 0);   /* locations ignored */

    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 9, 9, 9, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 2 && f.start_group == 0 && f.start_object == 0);   /* Next Object */

    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 3, 0, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 2 && f.start_group == 5 && f.start_object == 3);

    /* An absolute start at {0,0} cannot be two zero fields (that reads as the Next
     * Object) and is "equivalent to unfiltered" (9.20.10): it is not sent. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(!emit);
    /* ... but a start in group 0 at a later object, or in a later group, is. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 1, 0, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 2 && f.start_object == 1);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 1, 0, 0, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 2 && f.start_group == 1);

    /* A range is three fields: the end group as a delta from the start, and no end
     * object, which keeps the whole end group. {0,0} start stays absolute here. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 3 && f.start_group == 5 && f.start_object == 3 && f.end_group_delta == 4);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 0, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK(emit && f.field_count == 3 && f.end_group_delta == 0);
    /* A range that ends before it starts, and an unknown type, are refused. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 0, 4, &f, &emit), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(99, 0, 0, 0, &f, &emit), (int)MOQ_ERR_INVAL);
    /* The full 64-bit range is carried. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 1, 0, UINT64_MAX, &f, &emit), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(f.end_group_delta, UINT64_MAX - 1);
}

typedef struct {
    const char *what;
    moq_d21_location_filter_t in;
    uint32_t type; uint64_t sg, so, eg; bool approx;
} from_case_t;

static void t_filter_from_wire(void)
{
    const from_case_t cases[] = {
        /* zero length: the filter is removed, i.e. an absolute start at {0,0}. */
        { "zero length",            lf(0, 0, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0, false },
        /* one field is a start RELATIVE to the Next Group; only 0 is exact. */
        { "next group",             lf(1, 0, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0, false },
        { "current group (1)",      lf(1, 1, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, true },
        { "two groups back (2)",    lf(1, 2, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, true },
        { "relative at max",        lf(1, UINT64_MAX, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, true },
        /* two fields: {0,0} is the Next Object, anything else an absolute start. */
        { "next object",            lf(2, 0, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, false },
        { "absolute start",         lf(2, 5, 3, 0, 0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 3, 0, false },
        { "start group 0 object 1", lf(2, 0, 1, 0, 0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 1, 0, false },
        /* three fields: an absolute range keeping the whole end group. */
        { "range",                  lf(3, 5, 3, 4, 0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9, false },
        { "range, empty delta",     lf(3, 5, 0, 0, 0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 0, 5, false },
        /* four fields add an inclusive end object the group-level range cannot hold. */
        { "range with end object",  lf(4, 5, 3, 4, 7), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9, true },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        uint32_t type; uint64_t sg, so, eg; bool approx;
        moq_d21_profile_filter_from_wire(&cases[i].in, &type, &sg, &so, &eg, &approx);
        if (type != cases[i].type || sg != cases[i].sg || so != cases[i].so ||
            eg != cases[i].eg || approx != cases[i].approx) {
            fprintf(stderr, "FAIL: from_wire %s: type %u sg %llu so %llu eg %llu approx %d\n",
                    cases[i].what, type, (unsigned long long)sg, (unsigned long long)so,
                    (unsigned long long)eg, (int)approx);
            failures++;
        }
    }

    /* What can be sent can be read back exactly: every exact semantic filter
     * survives to_wire -> from_wire. */
    struct { uint32_t t; uint64_t sg, so, eg; } rt[] = {
        { MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0 },
        { MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0 },
        { MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 7, 2, 0 },
        { MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 7, 2, 11 },
        { MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 0, 0, 3 },
    };
    for (size_t i = 0; i < sizeof(rt) / sizeof(rt[0]); i++) {
        moq_d21_location_filter_t f; bool emit;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_filter_to_wire(rt[i].t, rt[i].sg, rt[i].so, rt[i].eg, &f, &emit), (int)MOQ_OK);
        uint32_t type; uint64_t sg, so, eg; bool approx;
        moq_d21_profile_filter_from_wire(&f, &type, &sg, &so, &eg, &approx);
        MOQ_TEST_CHECK_EQ_INT((int)type, (int)rt[i].t);
        MOQ_TEST_CHECK_EQ_U64(sg, rt[i].sg);
        MOQ_TEST_CHECK_EQ_U64(so, rt[i].so);
        if (rt[i].t == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) MOQ_TEST_CHECK_EQ_U64(eg, rt[i].eg);
        MOQ_TEST_CHECK(!approx);
    }
}

static void t_surface_subscription_filters(void)
{
    moq_d21_msg_params_t p;
    bool has; uint32_t type; uint64_t sg, so, eg;
    moq_decoded_loc_filter_t loc; moq_decoded_fill_t fill; moq_decoded_range_filters_t rf;

    /* No parameters: unfiltered, nothing surfaced. */
    memset(&p, 0, sizeof(p));
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(!has && type == MOQ_SUBSCRIBE_FILTER_NONE);
    MOQ_TEST_CHECK(!loc.present && !fill.present && rf.ranges == 0 && !rf.invalid);

    /* A zero-length Location Filter on a SUBSCRIBE is just no filter, but is still
     * surfaced as present; on a REQUEST_UPDATE it REMOVES the filter, which is an
     * absolute start at {0,0} (9.20.10). */
    p.has_location_filter = true;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(!has && type == MOQ_SUBSCRIBE_FILTER_NONE);
    MOQ_TEST_CHECK(loc.present && loc.field_count == 0 && !loc.approximated);
    moq_d21_profile_surface_subscription_filters(&p, true, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(has && type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START && sg == 0 && so == 0);
    MOQ_TEST_CHECK(loc.present && loc.field_count == 0);

    /* The raw wire fields always ride along, including where the four types only
     * approximate: a relative start (one field, 2) and a four-field range. */
    p.location_filter = lf(1, 2, 0, 0, 0);
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(has && type == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    MOQ_TEST_CHECK(loc.present && loc.approximated && loc.field_count == 1 && loc.start_group == 2);
    p.location_filter = lf(4, 5, 3, 4, 7);
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(has && type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE && eg == 9);
    MOQ_TEST_CHECK(loc.approximated && loc.field_count == 4 && loc.end_group_delta == 4 && loc.end_object == 7);

    /* FILL_PARAMETERS (9.20.16): presence is the request; its own filter, timeout,
     * priority and group order are surfaced, and a fill with no filter of its own
     * takes the subscription's (not surfaced as present). */
    memset(&p, 0, sizeof(p));
    p.has_fill = true;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(fill.present && !fill.location.present && !fill.has_timeout &&
                   !fill.has_priority && !fill.has_group_order);
    p.fill.has_location_filter = true;
    p.fill.location_filter = lf(1, 1, 0, 0, 0);                 /* the current group */
    p.fill.has_fill_timeout = true; p.fill.fill_timeout_ms = 250;
    p.fill.has_subscriber_priority = true; p.fill.subscriber_priority = 9;
    p.fill.has_group_order = true; p.fill.group_order = 2;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(fill.present && fill.location.present && fill.location.field_count == 1 &&
                   fill.location.start_group == 1 && fill.location.approximated);
    MOQ_TEST_CHECK(fill.has_timeout && fill.timeout_ms == 250);
    MOQ_TEST_CHECK(fill.has_priority && fill.priority == 9);
    MOQ_TEST_CHECK(fill.has_group_order && fill.group_order == 2);
    MOQ_TEST_CHECK(!has && !loc.present);                       /* the subscription's own filter is separate */
    /* The whole-track fill: a zero-length filter inside FILL_PARAMETERS. */
    p.fill.location_filter = lf(0, 0, 0, 0, 0);
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(fill.location.present && fill.location.field_count == 0 && !fill.location.approximated);

    /* Range Filters are counted across the message and its fill, and either one
     * being invalid makes the whole request invalid (the session answers
     * INVALID_FILTER; nothing here is applied). */
    memset(&p, 0, sizeof(p));
    p.range_filter_params = 1; p.range_filter_ranges = 2;
    p.has_fill = true; p.fill.range_filter_params = 1; p.fill.range_filter_ranges = 3;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK_EQ_U64(rf.ranges, 5);
    MOQ_TEST_CHECK(!rf.invalid);
    p.fill.range_filter_invalid = true;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(rf.invalid);
    p.fill.range_filter_invalid = false; p.range_filter_invalid = true;
    moq_d21_profile_surface_subscription_filters(&p, false, &has, &type, &sg, &so, &eg, &loc, &fill, &rf);
    MOQ_TEST_CHECK(rf.invalid);
}

static void t_fetch_range(void)
{
    moq_d21_location_filter_t f;

    /* Outbound: the core's [start, end) with end_object 0 = the whole end group
     * becomes an inclusive Location Filter: three fields without an end object,
     * four with one (exclusive -> inclusive). */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_fetch_range_to_wire(3, 0, 5, 0, &f), (int)MOQ_OK);
    MOQ_TEST_CHECK(f.field_count == 3 && f.start_group == 3 && f.start_object == 0 && f.end_group_delta == 2);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_fetch_range_to_wire(3, 1, 5, 8, &f), (int)MOQ_OK);
    MOQ_TEST_CHECK(f.field_count == 4 && f.start_object == 1 && f.end_group_delta == 2 && f.end_object == 7);
    /* Always absolute, even from {0,0}: two fields would read as the Next Object. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_fetch_range_to_wire(0, 0, 0, 0, &f), (int)MOQ_OK);
    MOQ_TEST_CHECK(f.field_count == 3 && f.start_group == 0 && f.start_object == 0 && f.end_group_delta == 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_fetch_range_to_wire(5, 0, 4, 0, &f), (int)MOQ_ERR_INVAL);

    /* Inbound. */
    typedef struct { const char *what; bool has; moq_d21_location_filter_t in;
                     uint64_t sg, so, eg, eo; bool approx; } fc_t;
    const fc_t cases[] = {
        /* No filter and an empty filter: {0,0} up to Largest Object (3.3.1), written
         * as an end past any Largest (FETCH_OK clamps it). */
        { "no filter",      false, lf(0,0,0,0,0), 0, 0, UINT64_MAX, 0, false },
        { "empty filter",   true,  lf(0,0,0,0,0), 0, 0, UINT64_MAX, 0, false },
        /* The Next Group / Next Object lie beyond Largest Object: an out-of-range
         * start makes the core answer INVALID_RANGE, as 9.11 requires. */
        { "next group",     true,  lf(1,0,0,0,0), UINT64_MAX, 0, UINT64_MAX, 0, false },
        { "next object",    true,  lf(2,0,0,0,0), UINT64_MAX, 0, UINT64_MAX, 0, false },
        /* A relative start needs Largest Object: approximated as the whole track. */
        { "relative (1)",   true,  lf(1,1,0,0,0), 0, 0, UINT64_MAX, 0, true },
        { "absolute start", true,  lf(2,4,2,0,0), 4, 2, UINT64_MAX, 0, false },
        { "three fields",   true,  lf(3,3,0,2,0), 3, 0, 5, 0, false },
        /* four fields: inclusive end object -> exclusive; max means the whole group */
        { "four fields",    true,  lf(4,3,1,2,7), 3, 1, 5, 8, false },
        { "end object max", true,  lf(4,3,1,2,UINT64_MAX), 3, 1, 5, 0, false },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_d21_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_location_filter = cases[i].has;
        p.location_filter = cases[i].in;
        uint64_t sg, so, eg, eo; bool approx;
        moq_d21_profile_fetch_range_from_wire(&p, &sg, &so, &eg, &eo, &approx);
        if (sg != cases[i].sg || so != cases[i].so || eg != cases[i].eg ||
            eo != cases[i].eo || approx != cases[i].approx) {
            fprintf(stderr, "FAIL: fetch from_wire %s: %llu:%llu - %llu:%llu approx %d\n",
                    cases[i].what, (unsigned long long)sg, (unsigned long long)so,
                    (unsigned long long)eg, (unsigned long long)eo, (int)approx);
            failures++;
        }
    }
    /* A range the core can express survives a round trip. */
    moq_d21_msg_params_t q;
    memset(&q, 0, sizeof(q));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_profile_fetch_range_to_wire(6, 2, 9, 5, &q.location_filter), (int)MOQ_OK);
    q.has_location_filter = true;
    uint64_t sg, so, eg, eo; bool approx;
    moq_d21_profile_fetch_range_from_wire(&q, &sg, &so, &eg, &eo, &approx);
    MOQ_TEST_CHECK(sg == 6 && so == 2 && eg == 9 && eo == 5 && !approx);
}

/* ===================================================================== *
 * Part 2. Sessions
 * ===================================================================== */

static moq_session_t *make_session(moq_perspective_t persp)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_21;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[8];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d21_encode_setup_opts(&w, NULL);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    return s;
}

/* The first request message the session put on a bidi stream (a new stream or an
 * existing one); copies it out and drains the queue. Returns its length. */
static size_t take_bidi_message(moq_session_t *s, uint8_t *out, size_t cap,
                                moq_stream_ref_t *ref)
{
    size_t n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        const uint8_t *d = NULL; size_t len = 0; moq_stream_ref_t r = {0};
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            d = a.u.open_bidi_stream.data; len = a.u.open_bidi_stream.len;
            r = a.u.open_bidi_stream.stream_ref;
        } else if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            d = a.u.send_bidi_stream.data; len = a.u.send_bidi_stream.len;
            r = a.u.send_bidi_stream.stream_ref;
        }
        if (d && n == 0 && len <= cap) { memcpy(out, d, len); n = len; if (ref) *ref = r; }
        moq_action_cleanup(&a);
    }
    return n;
}

static bool next_event(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t e;
    bool found = false;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        if (e.kind == kind && !found) { *out = e; found = true; continue; }
        moq_event_cleanup(&e);
    }
    return found;
}

static uint64_t poll_close_code(moq_session_t *s)
{
    uint64_t code = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION) code = a.u.close_session.code;
        moq_action_cleanup(&a);
    }
    return code;
}


static bool decode_msg(const uint8_t *b, size_t n, uint64_t want_type, moq_control_envelope_t *env)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, b, n);
    return moq_d21_decode_envelope(&r, env) == MOQ_OK && env->msg_type == want_type;
}

static moq_namespace_t ns_live(moq_bytes_t *parts)
{
    parts[0] = lit("live");
    return (moq_namespace_t){ parts, 1 };
}

/* -- outbound SUBSCRIBE: how each semantic filter goes on the wire ---------- */
static void t_outbound_subscribe(void)
{
    typedef struct { const char *what; uint32_t filter; uint64_t sg, so, eg;
                     bool wire_filter; uint8_t fields; } oc_t;
    const oc_t cases[] = {
        { "none",              MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0, false, 0 },
        { "next group",        MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0, true, 1 },
        { "largest object",    MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0, true, 2 },
        { "absolute start",    MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 3, 0, true, 2 },
        { "absolute {0,0}",    MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0, false, 0 },
        { "absolute range",    MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9, true, 3 },
    };
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    MOQ_TEST_CHECK(s != NULL);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_bytes_t parts[1];
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns_live(parts);
        /* One track per case: the current core refuses a second concurrent
         * subscription to the same track, which draft 21 allows (3.1; Task 6.1). */
        char track[8];
        snprintf(track, sizeof(track), "v%zu", i);
        cfg.track_name = lit(track);
        cfg.filter = cases[i].filter;
        cfg.start_group = cases[i].sg; cfg.start_object = cases[i].so; cfg.end_group = cases[i].eg;
        moq_subscription_t h;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &cfg, 1, &h), (int)MOQ_OK);
        uint8_t m[128];
        size_t n = take_bidi_message(s, m, sizeof(m), NULL);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_SUBSCRIBE, &env));
        moq_bytes_t dparts[8];
        moq_d21_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_subscribe(env.payload, env.payload_len, dparts, 8, &sub), (int)MOQ_OK);
        bool has = sub.params.has_location_filter;
        if (has != cases[i].wire_filter ||
            (has && sub.params.location_filter.field_count != cases[i].fields)) {
            fprintf(stderr, "FAIL: outbound SUBSCRIBE %s: filter on wire %d (%u fields)\n",
                    cases[i].what, (int)has, has ? sub.params.location_filter.field_count : 0);
            failures++;
        }
        if (has && cases[i].filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) {
            MOQ_TEST_CHECK_EQ_U64(sub.params.location_filter.start_group, 5);
            MOQ_TEST_CHECK_EQ_U64(sub.params.location_filter.end_group_delta, 4);
        }
        /* GROUP_ORDER, FORWARD and priority are carried as ordinary parameters. */
        MOQ_TEST_CHECK(!sub.params.has_fill && sub.params.range_filter_params == 0);
    }
    /* An invalid range is refused before any state changes (no stream opened). */
    {
        moq_bytes_t parts[1];
        moq_subscribe_cfg_t cfg;
        moq_subscribe_cfg_init(&cfg);
        cfg.track_namespace = ns_live(parts);
        cfg.track_name = lit("bad");
        cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        cfg.start_group = 9; cfg.end_group = 2;
        moq_subscription_t h;
        moq_result_t rc = moq_session_subscribe(s, &cfg, 1, &h);
        MOQ_TEST_CHECK(rc < 0);
        uint8_t m[128];
        MOQ_TEST_CHECK_EQ_SIZE(take_bidi_message(s, m, sizeof(m), NULL), 0);
    }
    moq_session_destroy(s);
}

/* An established SUBSCRIBE on a client session, ready for updates. */
static bool establish_subscription(moq_session_t *s, moq_subscription_t *h, moq_stream_ref_t *ref,
                                   uint32_t filter, bool with_largest)
{
    moq_bytes_t parts[1];
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = ns_live(parts);
    cfg.track_name = lit("est");
    cfg.filter = filter;
    if (moq_session_subscribe(s, &cfg, 1, h) != MOQ_OK) return false;
    uint8_t m[128];
    if (take_bidi_message(s, m, sizeof(m), ref) == 0) return false;
    uint8_t ok[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (with_largest) { p.has_largest = true; p.largest_group = 4; p.largest_object = 2; }
    if (moq_d21_encode_subscribe_ok(&w, 1, &p, (moq_bytes_t){ NULL, 0 }) != MOQ_OK) return false;
    /* The bytes on the response half are the SUBSCRIBE_OK message. */
    if (moq_session_on_bidi_stream_bytes(s, *ref, ok, moq_buf_writer_offset(&w), false, 2) < 0) return false;
    moq_event_t ev;
    bool got = next_event(s, MOQ_EVENT_SUBSCRIBE_OK, &ev);
    if (got) moq_event_cleanup(&ev);
    return got;
}

static void t_outbound_update(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));

    /* A new filter on an update; an absolute start at {0,0} is SENT as an empty
     * Location Filter, because on an update that is how a filter is removed. */
    typedef struct { const char *what; uint32_t filter; uint64_t sg, so, eg; uint8_t fields; } uc_t;
    const uc_t cases[] = {
        { "next group",   MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0, 1 },
        { "absolute",     MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 6, 1, 0, 2 },
        { "range",        MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 6, 1, 8, 3 },
        { "removal",      MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 0, 0, 0, 0 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_subscription_update_cfg_t u;
        memset(&u, 0, sizeof(u));
        u.struct_size = sizeof(u);
        u.has_filter = true; u.filter = cases[i].filter;
        u.start_group = cases[i].sg; u.start_object = cases[i].so; u.end_group = cases[i].eg;
        moq_result_t rc = moq_session_update_subscription(s, h, &u, 3);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        uint8_t m[128];
        size_t n = take_bidi_message(s, m, sizeof(m), NULL);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_REQUEST_UPDATE, &env));
        moq_d21_request_update_t d;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_update(env.payload, env.payload_len, &d), (int)MOQ_OK);
        if (!d.params.has_location_filter || d.params.location_filter.field_count != cases[i].fields) {
            fprintf(stderr, "FAIL: outbound REQUEST_UPDATE %s\n", cases[i].what);
            failures++;
        }
        /* Acknowledge so the next update is allowed: a REQUEST_OK on the same bidi. */
        uint8_t ok[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d21_encode_request_ok(&w, MOQ_D21_REQUEST_OK_REQUEST_UPDATE, NULL, (moq_bytes_t){ NULL, 0 });
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w), false, 4) >= 0);
        moq_event_t ev;
        if (next_event(s, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev)) moq_event_cleanup(&ev);
    }
    moq_session_destroy(s);
}

static void t_outbound_fetch(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_bytes_t parts[1];
    moq_fetch_cfg_t f;
    moq_fetch_cfg_init_sized(&f, sizeof(f));
    f.track_namespace = ns_live(parts);
    f.track_name = lit("v");
    f.start_group = 3; f.start_object = 1; f.end_group = 5; f.end_object = 8;
    moq_fetch_t fh;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &f, 1, &fh), (int)MOQ_OK);
    uint8_t m[160];
    size_t n = take_bidi_message(s, m, sizeof(m), NULL);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_FETCH, &env));
    moq_bytes_t dparts[8];
    moq_d21_fetch_t d;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(env.payload, env.payload_len, dparts, 8, &d), (int)MOQ_OK);
    /* [3:1, 5:8) exclusive becomes the inclusive filter 3:1 to group +2 object 7. */
    MOQ_TEST_CHECK(d.params.has_location_filter && d.params.location_filter.field_count == 4);
    MOQ_TEST_CHECK_EQ_U64(d.params.location_filter.start_group, 3);
    MOQ_TEST_CHECK_EQ_U64(d.params.location_filter.start_object, 1);
    MOQ_TEST_CHECK_EQ_U64(d.params.location_filter.end_group_delta, 2);
    MOQ_TEST_CHECK_EQ_U64(d.params.location_filter.end_object, 7);

    /* end_object 0 keeps the whole end group: three fields. */
    f.end_object = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &f, 1, &fh), (int)MOQ_OK);
    n = take_bidi_message(s, m, sizeof(m), NULL);
    MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_FETCH, &env));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(env.payload, env.payload_len, dparts, 8, &d), (int)MOQ_OK);
    MOQ_TEST_CHECK(d.params.location_filter.field_count == 3);
    moq_session_destroy(s);
}

/* Draft 21 has no Joining FETCH: the profile refuses it. Nothing is sent and no
 * request id is consumed, so the next FETCH carries the id the failed one would
 * have used. (The core is expected to gate on supports_joining_fetch before it
 * gets here; plan Task 7.) */
static void t_joining_fetch_refused(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, true));

    moq_fetch_cfg_t f;
    moq_fetch_cfg_init_sized(&f, sizeof(f));
    f.is_joining = true; f.joining_relative = true; f.joining_start = 1;
    f.joining_sub = h;
    moq_fetch_t fh;
    moq_result_t rc = moq_session_fetch(s, &f, 5, &fh);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
    uint8_t m[160];
    MOQ_TEST_CHECK_EQ_SIZE(take_bidi_message(s, m, sizeof(m), NULL), 0);

    /* A standalone FETCH still works and takes the request id the subscription's
     * successor would (client ids: 0 = the subscribe, 2 = this fetch). */
    moq_bytes_t parts[1];
    moq_fetch_cfg_init_sized(&f, sizeof(f));
    f.track_namespace = ns_live(parts);
    f.track_name = lit("v");
    f.end_group = 3;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(s, &f, 6, &fh), (int)MOQ_OK);
    size_t n = take_bidi_message(s, m, sizeof(m), NULL);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_FETCH, &env));
    moq_bytes_t dparts[8];
    moq_d21_fetch_t d;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_fetch(env.payload, env.payload_len, dparts, 8, &d), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(d.request_id, 2);
    moq_session_destroy(s);
}

/* -- inbound SUBSCRIBE and FETCH ----------------------------------------- */
static moq_result_t feed_request(moq_session_t *s, uint64_t stream_id, const uint8_t *msg, size_t n)
{
    return moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(stream_id), msg, n, false, 1);
}

static void t_inbound_subscribe(void)
{
    typedef struct { const char *what; bool has; moq_d21_location_filter_t f;
                     uint32_t filter; uint64_t sg, so, eg; } ic_t;
    const ic_t cases[] = {
        { "no filter",         false, lf(0,0,0,0,0), MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0 },
        { "zero length",       true,  lf(0,0,0,0,0), MOQ_SUBSCRIBE_FILTER_NONE, 0, 0, 0 },
        { "next group",        true,  lf(1,0,0,0,0), MOQ_SUBSCRIBE_FILTER_NEXT_GROUP, 0, 0, 0 },
        { "relative (approx)", true,  lf(1,2,0,0,0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0 },
        { "next object",       true,  lf(2,0,0,0,0), MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT, 0, 0, 0 },
        { "absolute start",    true,  lf(2,5,3,0,0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START, 5, 3, 0 },
        { "range",             true,  lf(3,5,3,4,0), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9 },
        { "range+end object",  true,  lf(4,5,3,4,7), MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE, 5, 3, 9 },
    };
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_bytes_t parts[1];
        moq_namespace_t ns = ns_live(parts);
        moq_d21_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_location_filter = cases[i].has;
        p.location_filter = cases[i].f;
        uint8_t msg[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        char track[8];                  /* one track per case (see t_outbound_subscribe) */
        snprintf(track, sizeof(track), "v%zu", i);
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, i * 2, &ns, lit(track), &p), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4 * (i + 1), msg, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        moq_event_t ev;
        bool got = next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            const moq_subscribe_request_event_t *r = &ev.u.subscribe_request;
            if (r->filter != cases[i].filter || r->start_group != cases[i].sg ||
                r->start_object != cases[i].so || r->end_group != cases[i].eg) {
                fprintf(stderr, "FAIL: inbound SUBSCRIBE %s: filter %u %llu:%llu - %llu\n", cases[i].what,
                        (unsigned)r->filter, (unsigned long long)r->start_group,
                        (unsigned long long)r->start_object, (unsigned long long)r->end_group);
                failures++;
            }
            /* The defaults when nothing is sent (9.20.8, 9.20.19). */
            MOQ_TEST_CHECK(r->forward);
            MOQ_TEST_CHECK_EQ_INT((int)r->subscriber_priority, 128);
            moq_event_cleanup(&ev);
        }
    }
    moq_session_destroy(s);
}

static void t_inbound_fetch(void)
{
    typedef struct { const char *what; bool has; moq_d21_location_filter_t f;
                     uint64_t sg, so, eg, eo; } fc_t;
    const fc_t cases[] = {
        { "no filter",     false, lf(0,0,0,0,0), 0, 0, UINT64_MAX, 0 },
        { "three fields",  true,  lf(3,3,0,2,0), 3, 0, 5, 0 },
        { "four fields",   true,  lf(4,3,1,2,7), 3, 1, 5, 8 },
        { "absolute start",true,  lf(2,4,2,0,0), 4, 2, UINT64_MAX, 0 },
    };
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_bytes_t parts[1];
        moq_d21_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = i * 2;
        f.track_namespace = ns_live(parts);
        f.track_name = lit("v");
        f.params.has_location_filter = cases[i].has;
        f.params.location_filter = cases[i].f;
        uint8_t msg[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4 * (i + 1), msg, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        moq_event_t ev;
        bool got = next_event(s, MOQ_EVENT_FETCH_REQUEST, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            const moq_fetch_request_event_t *r = &ev.u.fetch_request;
            if (r->start_group != cases[i].sg || r->start_object != cases[i].so ||
                r->end_group != cases[i].eg || r->end_object != cases[i].eo) {
                fprintf(stderr, "FAIL: inbound FETCH %s: %llu:%llu - %llu:%llu\n", cases[i].what,
                        (unsigned long long)r->start_group, (unsigned long long)r->start_object,
                        (unsigned long long)r->end_group, (unsigned long long)r->end_object);
                failures++;
            }
            MOQ_TEST_CHECK_EQ_INT((int)r->group_order, (int)MOQ_GROUP_ORDER_ASCENDING);   /* omitted: ascending */
            moq_event_cleanup(&ev);
        }
    }
    moq_session_destroy(s);
}

/* -- PUBLISH and PUBLISH_OK ------------------------------------------------ */
static void t_publish_ok_semantics(void)
{
    /* Outbound PUBLISH with forward = 0 sends FORWARD 0; the REQUEST_OK that answers
     * it carries no FORWARD, so the subscription stays NOT forwarding until the
     * subscriber updates it (9.8). Draft 18 resolved this to "forwarding". */
    for (int forward = 0; forward <= 1; forward++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[1];
        moq_publish_cfg_t cfg;
        moq_publish_cfg_init(&cfg);
        cfg.track_namespace = ns_live(parts);
        cfg.track_name = lit("p");
        cfg.has_forward = true; cfg.forward = forward != 0;
        moq_publication_t ph;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &cfg, 1, &ph), (int)MOQ_OK);
        uint8_t m[128];
        moq_stream_ref_t ref;
        size_t n = take_bidi_message(s, m, sizeof(m), &ref);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_PUBLISH, &env));
        moq_bytes_t dparts[8];
        moq_d21_publish_t pub;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish(env.payload, env.payload_len, dparts, 8, &pub), (int)MOQ_OK);
        MOQ_TEST_CHECK(pub.params.has_forward == (forward == 0));         /* sent only when 0 */
        if (forward == 0) MOQ_TEST_CHECK_EQ_INT((int)pub.params.forward, 0);

        static const uint8_t ok[] = { 0x07, 0x00, 0x01, 0x00 };            /* PUBLISH_OK, no parameters */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(s, ref, ok, sizeof(ok), false, 2) >= 0);
        moq_event_t ev;
        bool got = next_event(s, MOQ_EVENT_PUBLISH_OK, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK(ev.u.publish_ok.send_allowed == (forward != 0));
            MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_ok.subscriber_priority, 128);   /* the default */
            MOQ_TEST_CHECK(!ev.u.publish_ok.has_filter && !ev.u.publish_ok.has_expires);
            moq_event_cleanup(&ev);
        }
        moq_session_destroy(s);
    }

    /* EXPIRES is the one parameter a PUBLISH_OK may carry. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[1];
        moq_publish_cfg_t cfg;
        moq_publish_cfg_init(&cfg);
        cfg.track_namespace = ns_live(parts);
        cfg.track_name = lit("p");
        moq_publication_t ph;
        moq_session_publish(s, &cfg, 1, &ph);
        uint8_t m[128];
        moq_stream_ref_t ref;
        take_bidi_message(s, m, sizeof(m), &ref);
        static const uint8_t ok[] = { 0x07, 0x00, 0x04, 0x01, 0x08, 0x83, 0xE8 };   /* EXPIRES 1000 */
        moq_session_on_bidi_stream_bytes(s, ref, ok, sizeof(ok), false, 2);
        moq_event_t ev;
        bool got = next_event(s, MOQ_EVENT_PUBLISH_OK, &ev);
        MOQ_TEST_CHECK(got && ev.u.publish_ok.has_expires && ev.u.publish_ok.expires_ms == 1000);
        if (got) moq_event_cleanup(&ev);
        moq_session_destroy(s);
    }
    /* Parameters a PUBLISH_OK may not carry, and Track Properties, close the
     * session: LARGEST_OBJECT, FORWARD (subscriber state moved to REQUEST_UPDATE),
     * and a property block (only TRACK_STATUS_OK has one, 9.3). */
    static const uint8_t bad_largest[] = { 0x07, 0x00, 0x04, 0x01, 0x09, 0x00, 0x00 };
    static const uint8_t bad_forward[] = { 0x07, 0x00, 0x03, 0x01, 0x10, 0x01 };
    static const uint8_t bad_props[]   = { 0x07, 0x00, 0x03, 0x00, 0x0E, 0x05 };
    const uint8_t *bad[] = { bad_largest, bad_forward, bad_props };
    const size_t bad_len[] = { sizeof(bad_largest), sizeof(bad_forward), sizeof(bad_props) };
    for (size_t i = 0; i < 3; i++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[1];
        moq_publish_cfg_t cfg;
        moq_publish_cfg_init(&cfg);
        cfg.track_namespace = ns_live(parts);
        cfg.track_name = lit("p");
        moq_publication_t ph;
        moq_session_publish(s, &cfg, 1, &ph);
        uint8_t m[128];
        moq_stream_ref_t ref;
        take_bidi_message(s, m, sizeof(m), &ref);
        moq_session_on_bidi_stream_bytes(s, ref, bad[i], bad_len[i], false, 2);
        if (s->state != MOQ_SESS_CLOSED || poll_close_code(s) != 0x3) {
            fprintf(stderr, "FAIL: bad PUBLISH_OK #%zu did not close with 0x3\n", i);
            failures++;
        }
        moq_session_destroy(s);
    }
}

/* Accepting a PUBLISH sends a bare REQUEST_OK whatever the subscriber's choices:
 * those cannot travel on it any more (`publish_ok_carries_params` is false). */
static void t_accept_publish_is_bare(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_bytes_t parts[1];
    moq_d21_publish_t pub;
    memset(&pub, 0, sizeof(pub));
    pub.request_id = 0;
    pub.track_namespace = ns_live(parts);
    pub.track_name = lit("p");
    pub.track_alias = 3;
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &pub), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, moq_buf_writer_offset(&w)), (int)MOQ_OK);
    moq_event_t ev;
    bool got = next_event(s, MOQ_EVENT_PUBLISH_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { moq_session_destroy(s); return; }
    moq_publication_t ph = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);

    moq_accept_publish_cfg_t acc;
    memset(&acc, 0, sizeof(acc));
    acc.struct_size = sizeof(acc);
    acc.has_subscriber_priority = true; acc.subscriber_priority = 9;
    acc.group_order = MOQ_GROUP_ORDER_DESCENDING;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, ph, &acc, 2), (int)MOQ_OK);
    uint8_t out[64];
    size_t n = take_bidi_message(s, out, sizeof(out), NULL);
    static const uint8_t bare[] = { 0x07, 0x00, 0x01, 0x00 };
    MOQ_TEST_CHECK(n == sizeof(bare) && memcmp(out, bare, n) == 0);
    moq_session_destroy(s);
}

/* -- PUBLISH_STATE_NOTIFY -------------------------------------------------- */
static void t_state_notify(void)
{
    static const uint8_t notify_fwd[] = { 0x22, 0x00, 0x03, 0x01, 0x10, 0x00 };   /* FORWARD 0 */
    uint8_t body[16];
    memcpy(body, notify_fwd, sizeof(notify_fwd));

    /* A subscriber (SUBSCRIBE-initiated) accepts it and keeps the subscription:
     * the notice is informative (9.10). */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscription_t h;
        moq_stream_ref_t ref;
        MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(s, ref, notify_fwd, sizeof(notify_fwd), false, 3) >= 0);
        MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
        /* ... as often as the publisher likes. */
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(s, ref, notify_fwd, sizeof(notify_fwd), false, 4) >= 0);
        MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
        /* A malformed one (the body is parameters and nothing else) closes. */
        static const uint8_t trailing[] = { 0x22, 0x00, 0x04, 0x00, 0x0E, 0x05, 0x00 };
        moq_session_on_bidi_stream_bytes(s, ref, trailing, sizeof(trailing), false, 5);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }
    /* A subscriber on a PUBLISH-initiated subscription (the peer opened it with
     * PUBLISH) accepts it too, both before and after it answers the PUBLISH. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_bytes_t parts[1];
        moq_d21_publish_t pub;
        memset(&pub, 0, sizeof(pub));
        pub.request_id = 0;
        pub.track_namespace = ns_live(parts);
        pub.track_name = lit("pn");
        pub.track_alias = 3;
        uint8_t msg[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &pub), (int)MOQ_OK);
        feed_request(s, 4, msg, moq_buf_writer_offset(&w));
        moq_event_t ev;
        MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_PUBLISH_REQUEST, &ev));
        moq_publication_t ph = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        /* Pending (we have not answered yet). */
        MOQ_TEST_CHECK(feed_request(s, 4, notify_fwd, sizeof(notify_fwd)) >= 0);
        MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
        /* Established. */
        moq_accept_publish_cfg_t acc;
        memset(&acc, 0, sizeof(acc));
        acc.struct_size = sizeof(acc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, ph, &acc, 2), (int)MOQ_OK);
        { moq_action_t a2; while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2); }
        MOQ_TEST_CHECK(feed_request(s, 4, notify_fwd, sizeof(notify_fwd)) >= 0);
        MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
        /* A malformed one closes it. */
        static const uint8_t bad[] = { 0x22, 0x00, 0x04, 0x00, 0x0E, 0x05, 0x00 };
        feed_request(s, 4, bad, sizeof(bad));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }
    /* A parameter it may not carry (SUBSCRIBER_PRIORITY) closes. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscription_t h;
        moq_stream_ref_t ref;
        MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
        static const uint8_t prio[] = { 0x22, 0x00, 0x03, 0x01, 0x20, 0x05 };
        moq_session_on_bidi_stream_bytes(s, ref, prio, sizeof(prio), false, 3);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }
    /* A publisher that receives one (the subscriber must not send it) closes. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_bytes_t parts[1];
        moq_namespace_t ns = ns_live(parts);
        moq_d21_msg_params_t p;
        memset(&p, 0, sizeof(p));
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d21_encode_subscribe(&w, 0, &ns, lit("v"), &p);
        feed_request(s, 4, msg, moq_buf_writer_offset(&w));
        moq_event_t ev;
        MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        moq_subscription_t sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        memset(&acc, 0, sizeof(acc));
        acc.struct_size = sizeof(acc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acc, 2), (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
        feed_request(s, 4, notify_fwd, sizeof(notify_fwd));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
        moq_session_destroy(s);
    }
}

/* -- REQUEST_ERROR codes ---------------------------------------------------- */
static void t_error_semantics(void)
{
    const moq_profile_ops_t *d18 = moq_profile_lookup(MOQ_VERSION_DRAFT_18);
    const moq_profile_ops_t *d21 = moq_profile_lookup(MOQ_VERSION_DRAFT_21);
    MOQ_TEST_CHECK(d18 && d21);
    if (!(d18 && d21)) return;
    /* New in draft 21. */
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x36), MOQ_REQUEST_ERROR_INVALID_FILTER);
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x35), MOQ_REQUEST_ERROR_CONFLICTING_FILTERS);
    /* Removed since draft 18: they now read as INTERNAL_ERROR, while draft 18 still
     * knows them. */
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x19), MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x32), MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    MOQ_TEST_CHECK_EQ_U64(d18->semantic_request_error(0x19), MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION);
    MOQ_TEST_CHECK_EQ_U64(d18->semantic_request_error(0x32), MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
    MOQ_TEST_CHECK_EQ_U64(d18->semantic_request_error(0x36), MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    /* Unchanged, and unknown (GREASE included) values never close (13). */
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x34), MOQ_REQUEST_ERROR_REDIRECT);
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x10), MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(0x9D), MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    MOQ_TEST_CHECK_EQ_U64(d21->semantic_request_error(UINT64_MAX), MOQ_REQUEST_ERROR_INTERNAL_ERROR);
}

/* -- exact window resolution from the wire Location Filter (Task 6.1) --- */
static moq_decoded_loc_filter_t dlf(uint8_t n, uint64_t sg, uint64_t so, uint64_t dg, uint64_t eo)
{
    moq_decoded_loc_filter_t f;
    memset(&f, 0, sizeof(f));
    f.present = true; f.field_count = n; f.start_group = sg; f.start_object = so;
    f.end_group_delta = dg; f.end_object = eo;
    return f;
}

static void t_resolve_loc_filter_window(void)
{
    const uint64_t ceil = UINT64_MAX;
    moq_resolved_window_t w;
    moq_decoded_loc_filter_t f;

    /* One field is a relative start from the largest {7,2}: 0 = Next Group {8,0},
     * 1 = the current group {7,0}, 3 = two groups back {5,0}; past the origin it
     * clamps to group 0. */
    f = dlf(1, 0, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.has_window && !w.has_end && w.start_group == 8 && w.start_object == 0);
    f = dlf(1, 1, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 7 && w.start_object == 0);
    f = dlf(1, 3, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 5 && w.start_object == 0);
    f = dlf(1, 100, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 0 && w.start_object == 0);
    /* No largest known: open from the origin. */
    f = dlf(1, 3, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, false, 0, 0, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 0 && w.start_object == 0 && !w.unsatisfiable);
    /* Next Group past the ceiling cannot be represented. */
    f = dlf(1, 0, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, UINT64_MAX, 0, ceil, &w);
    MOQ_TEST_CHECK(w.unsatisfiable);

    /* Two zero fields: the Next Object, {7,3}. */
    f = dlf(2, 0, 0, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 7 && w.start_object == 3 && !w.has_end);
    /* Absolute start, including group 0 object 1 and group 1 object 0. */
    f = dlf(2, 0, 1, 0, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 0 && w.start_object == 1);

    /* Three fields: end group = start + delta, whole end group. */
    f = dlf(3, 5, 3, 4, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.start_group == 5 && w.start_object == 3 && w.has_end &&
                   w.end_group == 9 && !w.has_end_object);
    /* Four fields: the end Object is inclusive. */
    f = dlf(4, 5, 3, 4, 7);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.has_end && w.end_group == 9 && w.has_end_object && w.end_object == 7);
    /* A delta that would pass 2^64-1 saturates rather than wrapping. */
    f = dlf(3, 5, 0, UINT64_MAX, 0);
    moq_resolve_loc_filter_window(&f, true, 7, 2, ceil, &w);
    MOQ_TEST_CHECK(w.has_end && w.end_group == UINT64_MAX);
}

/* An inbound SUBSCRIBE's exact filter reaches the accepted window, so a relative
 * start and an end Object are enforced rather than approximated. */
static void t_inbound_window(void)
{
    typedef struct { const char *what; moq_d21_location_filter_t f;
                     uint64_t sg, so; bool has_end; uint64_t eg; bool has_eo; uint64_t eo; } wc_t;
    const wc_t cases[] = {
        { "relative 3",   lf(1,3,0,0,0),   5, 0, false, 0, false, 0 },
        { "end object",   lf(4,5,3,4,7),   5, 3, true,  9, true,  7 },
        { "whole end grp", lf(3,5,3,4,0),  5, 3, true,  9, false, 0 },
    };
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_note_object_published(s, &ns, lit("w0"), 7, 2), (int)MOQ_OK);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char track[8];
        snprintf(track, sizeof(track), "w%zu", i);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_note_object_published(s, &ns, lit(track), 7, 2), (int)MOQ_OK);
        moq_d21_msg_params_t p;
        memset(&p, 0, sizeof(p));
        p.has_location_filter = true;
        p.location_filter = cases[i].f;
        uint8_t msg[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, i * 2, &ns, lit(track), &p), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4 * (i + 1), msg, moq_buf_writer_offset(&w)), (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        moq_subscription_t sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        memset(&acc, 0, sizeof(acc));
        acc.struct_size = sizeof(acc);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acc, 2), (int)MOQ_OK);
        const moq_resolved_window_t *rw = moq_session_sub_resolved_window(s, sub);
        MOQ_TEST_CHECK(rw != NULL);
        if (rw) {
            if (rw->start_group != cases[i].sg || rw->start_object != cases[i].so ||
                rw->has_end != cases[i].has_end || rw->end_group != cases[i].eg ||
                rw->has_end_object != cases[i].has_eo || rw->end_object != cases[i].eo) {
                fprintf(stderr, "FAIL: inbound window %s: %llu:%llu end %d/%llu eo %d/%llu\n", cases[i].what,
                        (unsigned long long)rw->start_group, (unsigned long long)rw->start_object,
                        rw->has_end, (unsigned long long)rw->end_group,
                        rw->has_end_object, (unsigned long long)rw->end_object);
                failures++;
            }
        }
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
    }
    moq_session_destroy(s);
}

/* -- Range Filters are declined (Task 6.7) -------------------------------- *
 * MAX_FILTER_RANGES is not advertised (default 0), so a peer MUST NOT send Range
 * Filters: SUBSCRIBE, FETCH and REQUEST_UPDATE carrying one are answered with
 * REQUEST_ERROR INVALID_FILTER (0x36) and surface no request to the application. */

/* Append one Range Filter parameter (SetID 0, Start 5) to a message built with no
 * parameters: it ends in the zero parameter count. Returns the new length. */
static size_t add_range_filter_bytes(uint8_t *m, size_t n, const uint8_t *rf, size_t rflen)
{
    m[n - 1] = 0x01;                      /* one parameter */
    memcpy(m + n, rf, rflen);
    n += rflen;
    uint16_t len = (uint16_t)(((uint16_t)m[1] << 8 | m[2]) + rflen);
    m[1] = (uint8_t)(len >> 8); m[2] = (uint8_t)len;     /* 16-bit envelope length */
    return n;
}

static size_t add_range_filter(uint8_t *m, size_t n)
{
    static const uint8_t rf[] = { 0x26, 0x02, 0x00, 0x05 };
    return add_range_filter_bytes(m, n, rf, sizeof(rf));
}

static bool took_invalid_filter(moq_session_t *s)
{
    uint8_t m[128];
    size_t n = take_bidi_message(s, m, sizeof(m), NULL);
    moq_control_envelope_t env;
    moq_d21_request_error_t re;
    return n > 0 && decode_msg(m, n, MOQ_D21_REQUEST_ERROR, &env) &&
           moq_d21_decode_request_error(env.payload, env.payload_len, &re) == MOQ_OK &&
           re.error_code == 0x36;
}

static void t_range_filters_declined(void)
{
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    moq_d21_msg_params_t p;
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_event_t ev;

    /* SUBSCRIBE. */
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    memset(&p, 0, sizeof(p));
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, 0, &ns, lit("v"), &p), (int)MOQ_OK);
    size_t n = add_range_filter(msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, n), (int)MOQ_OK);
    MOQ_TEST_CHECK(!next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    MOQ_TEST_CHECK(took_invalid_filter(s));
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    moq_session_destroy(s);

    /* FETCH. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_d21_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 0; f.track_namespace = ns; f.track_name = lit("v");
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_fetch(&w, &f), (int)MOQ_OK);
    n = add_range_filter(msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, n), (int)MOQ_OK);
    MOQ_TEST_CHECK(!next_event(s, MOQ_EVENT_FETCH_REQUEST, &ev));
    MOQ_TEST_CHECK(took_invalid_filter(s));
    moq_session_destroy(s);

    /* REQUEST_UPDATE on an accepted subscription. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    memset(&p, 0, sizeof(p));
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_subscribe(&w, 0, &ns, lit("v"), &p), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, moq_buf_writer_offset(&w)), (int)MOQ_OK);
    MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t acc;
    memset(&acc, 0, sizeof(acc));
    acc.struct_size = sizeof(acc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acc, 2), (int)MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
    memset(&p, 0, sizeof(p));
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_update(&w, 2, &p), (int)MOQ_OK);
    n = add_range_filter(msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, n), (int)MOQ_OK);
    MOQ_TEST_CHECK(!next_event(s, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    MOQ_TEST_CHECK(took_invalid_filter(s));
    moq_session_destroy(s);
}

/* -- REQUEST_UPDATE accounting (Task 6.3) --------------------------------- *
 * We send at most one update per request until it is answered (REQUEST_OK or
 * REQUEST_ERROR restores the credit), which satisfies any MAX_REQUEST_UPDATES the
 * peer advertises; we advertise none, so inbound updates are not limited. An update
 * that is not on an established subscription's bidi is a protocol violation. */
static void t_update_credit(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
    moq_subscription_update_cfg_t u;
    memset(&u, 0, sizeof(u));
    u.struct_size = sizeof(u);
    u.has_forward = true; u.forward = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(s, h, &u, 3), (int)MOQ_OK);
    /* No credit until the first is answered. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(s, h, &u, 4), (int)MOQ_ERR_WRONG_STATE);
    { uint8_t m[128]; (void)take_bidi_message(s, m, sizeof(m), NULL); }
    uint8_t ok[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    moq_d21_encode_request_ok(&w, MOQ_D21_REQUEST_OK_REQUEST_UPDATE, NULL, (moq_bytes_t){ NULL, 0 });
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w), false, 5) >= 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(s, h, &u, 6), (int)MOQ_OK);
    moq_session_destroy(s);

    /* REQUEST_UPDATE as the first message on a fresh request bidi. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    uint8_t msg[32];
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_request_update(&w, 0, &p), (int)MOQ_OK);
    (void)feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
    moq_session_destroy(s);
}

/* -- PUBLISH accept sends the subscriber's choices as a REQUEST_UPDATE ----- *
 * The PUBLISH_OK form is a bare REQUEST_OK, so a non-default priority, a forward
 * that differs from the PUBLISH's, a filter or a new-group request is not dropped:
 * it follows as a REQUEST_UPDATE on the same bidi (Task 6.2). */
static size_t take_two_bidi_messages(moq_session_t *s, uint8_t *a, size_t acap, size_t *an,
                                     uint8_t *b, size_t bcap, size_t *bn)
{
    size_t count = 0;
    *an = *bn = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        const uint8_t *d = NULL; size_t len = 0;
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) { d = act.u.open_bidi_stream.data; len = act.u.open_bidi_stream.len; }
        else if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM) { d = act.u.send_bidi_stream.data; len = act.u.send_bidi_stream.len; }
        if (d && count == 0 && len <= acap) { memcpy(a, d, len); *an = len; count++; }
        else if (d && count == 1 && len <= bcap) { memcpy(b, d, len); *bn = len; count++; }
        moq_action_cleanup(&act);
    }
    return count;
}

static moq_publication_t accept_setup_publish(moq_session_t *s, uint64_t request_id, const char *track)
{
    moq_bytes_t parts[1];
    moq_d21_publish_t pub;
    memset(&pub, 0, sizeof(pub));
    pub.request_id = request_id;
    pub.track_namespace = ns_live(parts);
    pub.track_name = lit(track);
    pub.track_alias = 3 + request_id;
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_publish(&w, &pub);
    feed_request(s, 4 * (request_id / 2 + 1), msg, moq_buf_writer_offset(&w));
    moq_event_t ev;
    moq_publication_t ph = {0};
    if (next_event(s, MOQ_EVENT_PUBLISH_REQUEST, &ev)) {
        ph = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
    }
    return ph;
}

static void t_accept_publish_followup_update(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    uint8_t a[96], b[96];
    size_t an, bn;
    moq_control_envelope_t env;
    moq_d21_request_update_t u;

    /* Priority, a filter and a forward change (the PUBLISH defaulted to forward 1). */
    moq_publication_t ph = accept_setup_publish(s, 0, "p0");
    moq_accept_publish_cfg_t acc;
    memset(&acc, 0, sizeof(acc));
    acc.struct_size = sizeof(acc);
    acc.has_subscriber_priority = true; acc.subscriber_priority = 9;
    acc.has_forward = true; acc.forward = false;
    acc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START; acc.start_group = 6; acc.start_object = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, ph, &acc, 2), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)take_two_bidi_messages(s, a, sizeof(a), &an, b, sizeof(b), &bn), 2);
    static const uint8_t bare[] = { 0x07, 0x00, 0x01, 0x00 };
    MOQ_TEST_CHECK(an == sizeof(bare) && memcmp(a, bare, an) == 0);
    MOQ_TEST_CHECK(decode_msg(b, bn, MOQ_D21_REQUEST_UPDATE, &env));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_request_update(env.payload, env.payload_len, &u), (int)MOQ_OK);
    MOQ_TEST_CHECK(u.params.has_subscriber_priority && u.params.subscriber_priority == 9);
    MOQ_TEST_CHECK(u.params.has_forward && u.params.forward == 0);
    MOQ_TEST_CHECK(u.params.has_location_filter && u.params.location_filter.field_count == 2 &&
                   u.params.location_filter.start_group == 6 && u.params.location_filter.start_object == 1);
    MOQ_TEST_CHECK(!u.params.has_group_order);
    /* A second accept-time follow-up would need its own credit; the first is pending. */
    { bool pending = false;
      for (size_t i = 0; i < s->pub_cap; i++)
          if (s->publishes[i].state == MOQ_PUB_ESTABLISHED && s->publishes[i].update_pending)
              pending = true;
      MOQ_TEST_CHECK(pending); }

    /* Nothing to say: the bare OK stands alone (no update, no credit used). */
    moq_publication_t ph2 = accept_setup_publish(s, 2, "p1");
    memset(&acc, 0, sizeof(acc));
    acc.struct_size = sizeof(acc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, ph2, &acc, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)take_two_bidi_messages(s, a, sizeof(a), &an, b, sizeof(b), &bn), 1);

    /* Priority 128 and a forward equal to the PUBLISH's are defaults: still bare. */
    moq_publication_t ph3 = accept_setup_publish(s, 4, "p2");
    memset(&acc, 0, sizeof(acc));
    acc.struct_size = sizeof(acc);
    acc.has_subscriber_priority = true; acc.subscriber_priority = 128;
    acc.has_forward = true; acc.forward = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(s, ph3, &acc, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)take_two_bidi_messages(s, a, sizeof(a), &an, b, sizeof(b), &bn), 1);
    moq_session_destroy(s);
}

/* Every Range Filter type a SUBSCRIBE may carry is declined, not just one (0x29 is
 * allowed on SUBSCRIBE_TRACKS only; on a SUBSCRIBE it is a parameter the message
 * does not take, which the codec refuses as a protocol violation); a Length-0 filter means "no
 * filter" (8.6) and is not a Range Filter at all. */
static void t_range_filter_types(void)
{
    static const struct { const char *what; uint8_t b[6]; size_t n; bool declined; } cases[] = {
        { "0x25 type 1",     { 0x25, 0x02, 0x00, 0x05 },       4, true },
        { "0x26 type 2",     { 0x26, 0x02, 0x00, 0x05 },       4, true },
        { "0x27 type 3",     { 0x27, 0x02, 0x00, 0x05 },       4, true },
        { "0x28 with prop",  { 0x28, 0x03, 0x00, 0x02, 0x05 }, 5, true },
        { "0x26 length 0",   { 0x26, 0x00 },                   2, false },
    };
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_d21_msg_params_t p;
        memset(&p, 0, sizeof(p));
        uint8_t msg[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d21_encode_subscribe(&w, 0, &ns, lit("v"), &p);
        size_t n = add_range_filter_bytes(msg, moq_buf_writer_offset(&w), cases[i].b, cases[i].n);
        MOQ_TEST_CHECK_EQ_INT((int)feed_request(s, 4, msg, n), (int)MOQ_OK);
        moq_event_t ev;
        bool surfaced = next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
        if (surfaced) moq_event_cleanup(&ev);
        if (surfaced == cases[i].declined || (cases[i].declined && !took_invalid_filter(s)) ||
            s->state == MOQ_SESS_CLOSED) {
            fprintf(stderr, "FAIL: range filter %s\n", cases[i].what);
            failures++;
        }
        moq_session_destroy(s);
    }
}

/* -- Request stream closure (Task 6.4, draft 21 6.4.2.2 / 6.4.2.3) --------- *
 * A FIN only says "no more messages in this direction"; it is not a cancellation.
 * A requester MAY FIN right after its SUBSCRIBE when it will not send a
 * REQUEST_UPDATE, and the subscription must live on. Cancelling is RESET_STREAM
 * (the peer's send half) or STOP_SENDING (asking us to stop sending). */
static moq_subscription_t accept_new_subscription(moq_session_t *s, uint64_t request_id,
                                                  const char *track, bool fin_with_request)
{
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_subscribe(&w, request_id, &ns, lit(track), &p);
    moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(4 * (request_id / 2 + 1)), msg,
                                     moq_buf_writer_offset(&w), fin_with_request, 1);
    moq_subscription_t sub = {0};
    moq_event_t ev;
    if (next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) {
        sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        memset(&acc, 0, sizeof(acc));
        acc.struct_size = sizeof(acc);
        moq_session_accept_subscribe(s, sub, &acc, 2);
    }
    { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
    return sub;
}

static void t_request_stream_closure(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);

    /* FIN with the request: the subscription is established and stays so. */
    moq_subscription_t a = accept_new_subscription(s, 0, "fin_with", true);
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(moq_session_sub_resolved_window(s, a) != NULL || s->state != MOQ_SESS_CLOSED);

    /* A FIN that arrives later, on an established subscription, is not a cancel. */
    moq_subscription_t b = accept_new_subscription(s, 2, "fin_later", false);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
        s, moq_stream_ref_from_u64(8), NULL, 0, true, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    { moq_event_t ev; bool cancelled = false;
      while (moq_session_poll_events(s, &ev, 1) > 0) {
          if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) cancelled = true;
          moq_event_cleanup(&ev);
      }
      MOQ_TEST_CHECK(!cancelled); }
    (void)b;

    /* RESET_STREAM from the subscriber cancels it (and only it). */
    moq_subscription_t c = accept_new_subscription(s, 4, "reset", false);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_reset(
        s, moq_stream_ref_from_u64(12), 0, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    { moq_event_t ev; bool cancelled = false;
      while (moq_session_poll_events(s, &ev, 1) > 0) {
          if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) cancelled = true;
          moq_event_cleanup(&ev);
      }
      MOQ_TEST_CHECK(cancelled); }
    (void)c;
    moq_session_destroy(s);

    /* The responder FINs an established subscription without PUBLISH_DONE: the
     * request failed (the subscription ends), the session does not. */
    s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 5), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    { moq_event_t ev; bool ended = false;
      while (moq_session_poll_events(s, &ev, 1) > 0) {
          if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) ended = true;
          moq_event_cleanup(&ev);
      }
      MOQ_TEST_CHECK(ended); }
    moq_session_destroy(s);
}

/* -- Per-request GOAWAY (Task 6.6, 9.2) --------------------------------- *
 * A GOAWAY on a request stream migrates only that request and leaves the session
 * up; a client sends no URI and a server that receives one closes the session. */
static size_t goaway_bytes(uint8_t *m, size_t cap, const char *uri, uint64_t timeout_ms)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, m, cap);
    moq_d21_encode_goaway(&w, (const uint8_t *)uri, uri ? strlen(uri) : 0, timeout_ms);
    return moq_buf_writer_offset(&w);
}

static void t_request_goaway(void)
{
    uint8_t m[128];
    moq_event_t ev;

    /* We are the subscriber; the publisher migrates the request. */
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
    size_t n = goaway_bytes(m, sizeof(m), "https://new.example/moq", 1500);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(s, ref, m, n, false, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->state != MOQ_SESS_CLOSED);
    bool got = next_event(s, MOQ_EVENT_REQUEST_GOAWAY, &ev);
    MOQ_TEST_CHECK(got);
    if (got) {
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.request_goaway.new_session_uri.len, 23);
        MOQ_TEST_CHECK_EQ_U64(ev.u.request_goaway.timeout_ms, 1500);
        moq_event_cleanup(&ev);
    }
    /* Only that request moved: the session is untouched. */
    MOQ_TEST_CHECK(s->state == MOQ_SESS_ESTABLISHED);
    moq_session_destroy(s);

    /* We are the publisher (a server): a subscriber's GOAWAY carries no URI. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_subscription_t sub = accept_new_subscription(s, 0, "ga", false);
    (void)sub;
    n = goaway_bytes(m, sizeof(m), "https://nope", 0);
    (void)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(4), m, n, false, 3);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
    moq_session_destroy(s);

    s = make_session(MOQ_PERSPECTIVE_SERVER);
    sub = accept_new_subscription(s, 0, "ga", false);
    n = goaway_bytes(m, sizeof(m), NULL, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(4), m, n, false, 3), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->state == MOQ_SESS_ESTABLISHED);
    if (next_event(s, MOQ_EVENT_REQUEST_GOAWAY, &ev)) moq_event_cleanup(&ev);
    /* A GOAWAY is terminal on its stream: nothing may follow it. */
    n = goaway_bytes(m, sizeof(m), NULL, 0);
    (void)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(4), m, n, false, 4);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x3);
    moq_session_destroy(s);
}

/* -- End of Timed-Out Range (Task 6.8, 11.4.1.2) ------------------------- */
static void t_timed_out_range(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_bytes_t parts[1];
    moq_d21_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = 0; f.track_namespace = ns_live(parts); f.track_name = lit("v");
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_fetch(&w, &f);
    feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    moq_event_t ev;
    MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_FETCH_REQUEST, &ev));
    moq_fetch_t fh = ev.u.fetch_request.fetch;
    moq_event_cleanup(&ev);
    moq_accept_fetch_cfg_t acc;
    moq_accept_fetch_cfg_init(&acc);
    acc.end_group = 9; acc.end_object = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_fetch(s, fh, &acc, 2), (int)MOQ_OK);
    { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_fetch_range(s, fh, MOQ_FETCH_RANGE_TIMED_OUT, 3, 5, 3), (int)MOQ_OK);
    /* The marker rides the data stream: Serialization Flags 0x20C, then the Location. */
    bool found = false;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len == 4) {
            static const uint8_t want[] = { 0x82, 0x0C, 0x03, 0x05 };   /* vi64 0x20C = 82 0C */
            found = memcmp(a.u.send_data.header, want, sizeof(want)) == 0;
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(found);
    moq_session_destroy(s);
}

/* -- Fill fetch streams (Task 7, draft 21 3.4) -------------------------------- */
static void t_resolve_fill_range(void)
{
    uint64_t sg, so, eg, eo;
    moq_decoded_loc_filter_t f;
    /* Largest Object {7,2}. No filter / zero-length: the whole track up to Largest. */
    MOQ_TEST_CHECK(moq_resolve_fill_range(NULL, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 0 && so == 0 && eg == 7 && eo == 2);
    f = dlf(0, 0, 0, 0, 0);
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 0 && eg == 7 && eo == 2);
    /* No content yet: nothing to fill. */
    MOQ_TEST_CHECK(!moq_resolve_fill_range(NULL, false, 0, 0, &sg, &so, &eg, &eo));
    /* One field is a relative start: 1 = the current group from its start. */
    f = dlf(1, 1, 0, 0, 0);
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 7 && so == 0 && eg == 7 && eo == 2);
    f = dlf(1, 3, 0, 0, 0);                    /* two groups back */
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 5 && so == 0 && eg == 7 && eo == 2);
    f = dlf(1, 100, 0, 0, 0);                  /* past the origin clamps to group 0 */
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 0);
    /* Next Group and Next Object start after Largest Object: no fill stream. */
    f = dlf(1, 0, 0, 0, 0);
    MOQ_TEST_CHECK(!moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    f = dlf(2, 0, 0, 0, 0);
    MOQ_TEST_CHECK(!moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    /* An absolute start after Largest Object is empty too. */
    f = dlf(2, 8, 0, 0, 0);
    MOQ_TEST_CHECK(!moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    f = dlf(2, 7, 3, 0, 0);
    MOQ_TEST_CHECK(!moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    f = dlf(2, 7, 2, 0, 0);                    /* exactly Largest Object */
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 7 && so == 2 && eg == 7 && eo == 2);
    /* An end inside the track ends the fill there; whole End Group without an end
     * Object; an inclusive end Object with four fields. */
    f = dlf(3, 2, 1, 3, 0);                    /* groups 2..5 */
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 2 && so == 1 && eg == 5 && eo == UINT64_MAX);
    f = dlf(4, 2, 1, 3, 4);
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(sg == 2 && so == 1 && eg == 5 && eo == 4);
    /* An end past Largest Object never extends the fill beyond it. */
    f = dlf(3, 2, 0, 50, 0);
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(eg == 7 && eo == 2);
    f = dlf(4, 7, 0, 0, 9);                    /* end object past the last one */
    MOQ_TEST_CHECK(moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
    MOQ_TEST_CHECK(eg == 7 && eo == 2);
    /* An end before the start is empty. */
    f = dlf(4, 2, 5, 0, 1);
    MOQ_TEST_CHECK(!moq_resolve_fill_range(&f, true, 7, 2, &sg, &so, &eg, &eo));
}

/* An inbound SUBSCRIBE with FILL_PARAMETERS; `fwd` is the Forward Parameter. */
static moq_subscription_t fill_subscribe(moq_session_t *s, uint64_t request_id, const char *track,
                                         moq_d21_location_filter_t fill_filter, bool has_fill_filter,
                                         bool has_fwd, uint8_t fwd)
{
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    moq_session_note_object_published(s, &ns, lit(track), 7, 2);
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = has_fwd; p.forward = fwd;
    p.has_fill = true;
    p.fill.has_location_filter = has_fill_filter;
    p.fill.location_filter = fill_filter;
    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_subscribe(&w, request_id, &ns, lit(track), &p);
    feed_request(s, 4 * (request_id / 2 + 1), msg, moq_buf_writer_offset(&w));
    moq_subscription_t sub = {0};
    moq_event_t ev;
    if (next_event(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) {
        sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t acc;
        memset(&acc, 0, sizeof(acc));
        acc.struct_size = sizeof(acc);
        moq_session_accept_subscribe(s, sub, &acc, 2);
    }
    { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a); }
    return sub;
}

static void t_fill_stream(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_fill_info_t info;
    moq_fetch_t fh;

    /* Forward 1 (the default) with a current-group fill: pending, then opened. */
    moq_subscription_t a = fill_subscribe(s, 0, "f0", lf(1, 1, 0, 0, 0), true, false, 0);
    MOQ_TEST_CHECK(moq_session_sub_fill_pending(s, a));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 3, &info, &fh), (int)MOQ_OK);
    MOQ_TEST_CHECK(!info.empty && info.start_group == 7 && info.start_object == 0 &&
                   info.end_group == 7 && info.end_object == 2 && info.request_id == 0);
    MOQ_TEST_CHECK(!moq_session_sub_fill_pending(s, a));
    /* The stream opens with FETCH_HEADER carrying the SUBSCRIBE's Request ID. */
    uint8_t want[8];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, want, sizeof(want));
    moq_d21_encode_fetch_header(&w, 0);
    bool opened = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_DATA && act.u.send_data.header_len == moq_buf_writer_offset(&w) &&
            memcmp(act.u.send_data.header, want, act.u.send_data.header_len) == 0 && !act.u.send_data.fin)
            opened = true;
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK(opened);
    /* A second open has nothing pending. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 4, &info, &fh), (int)MOQ_ERR_WRONG_STATE);
    moq_session_destroy(s);

    /* Forward 0: FILL_PARAMETERS opens nothing, and a later Forward 1 does not
     * open one either (3.4.1). */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    a = fill_subscribe(s, 0, "f1", lf(1, 1, 0, 0, 0), true, true, 0);
    MOQ_TEST_CHECK(!moq_session_sub_fill_pending(s, a));
    moq_session_destroy(s);

    /* Nothing to fill: Next Object. Consumes the request, opens no stream. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    a = fill_subscribe(s, 0, "f2", lf(2, 0, 0, 0, 0), true, false, 0);
    MOQ_TEST_CHECK(moq_session_sub_fill_pending(s, a));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 3, &info, &fh), (int)MOQ_OK);
    MOQ_TEST_CHECK(info.empty);
    MOQ_TEST_CHECK(!moq_session_sub_fill_pending(s, a));
    { bool data = false; moq_action_t x;
      while (moq_session_poll_actions(s, &x, 1) > 0) { if (x.kind == MOQ_ACTION_SEND_DATA) data = true; moq_action_cleanup(&x); }
      MOQ_TEST_CHECK(!data); }
    moq_session_destroy(s);
}

/* Fill streams end independently of the subscription, and with it. */
static void t_fill_lifecycle(void)
{
    moq_fill_info_t info;
    moq_fetch_t fh;
    moq_action_t act;

    /* The subscriber's RESET_STREAM of the subscription resets the open fill. */
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_subscription_t a = fill_subscribe(s, 0, "l0", lf(1, 1, 0, 0, 0), true, false, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 3, &info, &fh), (int)MOQ_OK);
    { while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_reset(s, moq_stream_ref_from_u64(4), 0, 4), (int)MOQ_OK);
    bool reset = false;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_RESET_DATA) reset = true;
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK(reset);
    /* The fetch handle is stale: the fill is gone. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(s, fh, 5), (int)MOQ_ERR_STALE_HANDLE);
    moq_session_destroy(s);

    /* STOP_SENDING on the fill stream cancels only the fill. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    a = fill_subscribe(s, 0, "l1", lf(1, 1, 0, 0, 0), true, false, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 3, &info, &fh), (int)MOQ_OK);
    uint64_t fill_ref = 0;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_DATA) fill_ref = act.u.send_data.stream_ref._v;
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK(fill_ref != 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_stop(s, moq_stream_ref_from_u64(fill_ref), 0, 4), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_fetch(s, fh, 5), (int)MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(s->state == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_sub_resolved_window(s, a) != NULL);   /* subscription lives */
    moq_session_destroy(s);

    /* Completion is a FIN; failure is a reset that leaves the subscription alone. */
    s = make_session(MOQ_PERSPECTIVE_SERVER);
    a = fill_subscribe(s, 0, "l2", lf(1, 1, 0, 0, 0), true, false, 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 3, &info, &fh), (int)MOQ_OK);
    { while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act); }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_fill(s, fh, 0x2, 4), (int)MOQ_OK);
    reset = false;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_RESET_DATA && act.u.reset_data.error_code == 0x2) reset = true;
        moq_action_cleanup(&act);
    }
    MOQ_TEST_CHECK(reset);
    MOQ_TEST_CHECK(moq_session_sub_resolved_window(s, a) != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reset_fill(s, fh, 0x2, 5), (int)MOQ_ERR_STALE_HANDLE);

    /* A second fill while the first is open (an update with its own Request ID). */
    moq_d21_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_fill = true;
    p.fill.has_location_filter = true;
    p.fill.location_filter = lf(1, 2, 0, 0, 0);
    uint8_t msg[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_request_update(&w, 2, &p);
    feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK(moq_session_sub_fill_pending(s, a));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_fill(s, a, 6, &info, &fh), (int)MOQ_OK);
    MOQ_TEST_CHECK(info.request_id == 2 && info.start_group == 6 && info.end_group == 7);
    /* An update WITHOUT FILL_PARAMETERS opens nothing new. */
    memset(&p, 0, sizeof(p));
    p.has_subscriber_priority = true; p.subscriber_priority = 5;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_request_update(&w, 4, &p);
    feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK(!moq_session_sub_fill_pending(s, a));
    moq_session_destroy(s);
}

/* A one-field FETCH start is relative to Largest Object, resolved by the core. */
static void t_fetch_relative_start(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_bytes_t parts[1];
    moq_namespace_t ns = ns_live(parts);
    moq_session_note_object_published(s, &ns, lit("have"), 7, 2);
    moq_d21_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.track_namespace = ns; f.track_name = lit("have");
    f.params.has_location_filter = true;
    f.params.location_filter = lf(1, 2, 0, 0, 0);       /* the previous group on */
    uint8_t msg[96];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_fetch(&w, &f);
    feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    moq_event_t ev;
    MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_FETCH_REQUEST, &ev));
    MOQ_TEST_CHECK(ev.u.fetch_request.start_group == 6 && ev.u.fetch_request.start_object == 0 &&
                   ev.u.fetch_request.end_group == 7 && ev.u.fetch_request.end_object == 3);
    moq_event_cleanup(&ev);

    /* No Largest known: nothing to fetch, answered INVALID_RANGE, no event. */
    f.track_name = lit("none");
    moq_buf_writer_init(&w, msg, sizeof(msg));
    f.request_id = 2;
    moq_d21_encode_fetch(&w, &f);
    feed_request(s, 8, msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK(!next_event(s, MOQ_EVENT_FETCH_REQUEST, &ev));
    uint8_t m[64];
    size_t n = take_bidi_message(s, m, sizeof(m), NULL);
    moq_control_envelope_t env;
    moq_d21_request_error_t re;
    MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_REQUEST_ERROR, &env) &&
                   moq_d21_decode_request_error(env.payload, env.payload_len, &re) == MOQ_OK &&
                   re.error_code != 0);
    moq_session_destroy(s);
}

/* The publisher's own initial Subscription Parameters on a PUBLISH are readable. */
static void t_publish_initial_params(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_bytes_t parts[1];
    moq_d21_publish_t pub;
    memset(&pub, 0, sizeof(pub));
    pub.track_namespace = ns_live(parts); pub.track_name = lit("ip"); pub.track_alias = 9;
    pub.params.has_subscriber_priority = true; pub.params.subscriber_priority = 7;
    pub.params.has_group_order = true; pub.params.group_order = MOQ_GROUP_ORDER_DESCENDING;
    pub.params.has_object_delivery_timeout = true; pub.params.object_delivery_timeout_ms = 1500;
    pub.params.has_location_filter = true;
    pub.params.location_filter = lf(2, 4, 1, 0, 0);
    uint8_t msg[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_encode_publish(&w, &pub), (int)MOQ_OK);
    feed_request(s, 4, msg, moq_buf_writer_offset(&w));
    moq_event_t ev;
    MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t ph = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_publish_initial_params_t ip;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_initial_params(s, ph, &ip), (int)MOQ_OK);
    MOQ_TEST_CHECK(ip.present && ip.subscriber_priority == 7 &&
                   ip.group_order == MOQ_GROUP_ORDER_DESCENDING &&
                   ip.has_delivery_timeout && ip.delivery_timeout_ms == 1500 &&
                   ip.has_filter && ip.filter_field_count == 2 &&
                   ip.filter_start_group == 4 && ip.filter_start_object == 1);
    /* Defaults when omitted. */
    memset(&pub.params, 0, sizeof(pub.params));
    pub.request_id = 2; pub.track_name = lit("ip2"); pub.track_alias = 10;
    moq_buf_writer_init(&w, msg, sizeof(msg));
    moq_d21_encode_publish(&w, &pub);
    feed_request(s, 8, msg, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK(next_event(s, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    ph = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish_initial_params(s, ph, &ip), (int)MOQ_OK);
    MOQ_TEST_CHECK(ip.present && ip.subscriber_priority == 128 && !ip.has_filter && !ip.has_delivery_timeout);
    moq_session_destroy(s);
}

/* A publisher can send PUBLISH_STATE_NOTIFY on an established subscription. */
static void t_send_state_notify(void)
{
    moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
    moq_subscription_t sub = accept_new_subscription(s, 0, "sn", false);
    moq_state_notify_cfg_t c;
    memset(&c, 0, sizeof(c));
    c.struct_size = sizeof(c);
    c.has_largest = true; c.largest_group = 5; c.largest_object = 1;
    c.has_forward = true; c.forward = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(s, sub, &c, 5), (int)MOQ_OK);
    uint8_t m[64];
    size_t n = take_bidi_message(s, m, sizeof(m), NULL);
    moq_control_envelope_t env;
    moq_d21_publish_state_notify_t nt;
    MOQ_TEST_CHECK(n > 0 && decode_msg(m, n, MOQ_D21_PUBLISH_STATE_NOTIFY, &env));
    MOQ_TEST_CHECK_EQ_INT((int)moq_d21_decode_publish_state_notify(env.payload, env.payload_len, &nt), (int)MOQ_OK);
    MOQ_TEST_CHECK(nt.params.has_largest && nt.params.largest_group == 5 && nt.params.largest_object == 1 &&
                   nt.params.has_forward && nt.params.forward == 0);
    /* Nothing to say, and a subscription that is not ours to notify, are refused. */
    moq_state_notify_cfg_t empty;
    memset(&empty, 0, sizeof(empty));
    empty.struct_size = sizeof(empty);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(s, sub, &empty, 6), (int)MOQ_ERR_INVAL);
    moq_session_destroy(s);
    s = make_session(MOQ_PERSPECTIVE_CLIENT);
    moq_subscription_t h;
    moq_stream_ref_t ref;
    MOQ_TEST_CHECK(establish_subscription(s, &h, &ref, MOQ_SUBSCRIBE_FILTER_NONE, true));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(s, h, &c, 7), (int)MOQ_ERR_WRONG_STATE);
    moq_session_destroy(s);
}

int main(void)
{
    t_filter_to_wire();
    t_filter_from_wire();
    t_surface_subscription_filters();
    t_fetch_range();
    t_outbound_subscribe();
    t_outbound_update();
    t_outbound_fetch();
    t_joining_fetch_refused();
    t_inbound_subscribe();
    t_inbound_fetch();
    t_publish_ok_semantics();
    t_accept_publish_is_bare();
    t_state_notify();
    t_error_semantics();
    t_resolve_loc_filter_window();
    t_inbound_window();
    t_range_filters_declined();
    t_range_filter_types();
    t_request_stream_closure();
    t_request_goaway();
    t_timed_out_range();
    t_resolve_fill_range();
    t_fill_stream();
    t_fill_lifecycle();
    t_fetch_relative_start();
    t_publish_initial_params();
    t_send_state_notify();
    t_update_credit();
    t_accept_publish_followup_update();
    if (failures) {
        fprintf(stderr, "test_d21_requests: %d failures\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("test_d21_requests");
}
