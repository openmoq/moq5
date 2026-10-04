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
    if (failures) {
        fprintf(stderr, "test_d21_requests: %d failures\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("test_d21_requests");
}
