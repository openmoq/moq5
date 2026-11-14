/*
 * Size-negotiated event poll for every event whose detail grew with the
 * draft-22 surface.
 *
 * A caller whose detail struct ends where the layout before the extension
 * ended polls with an element of the event prefix plus that layout's
 * sizeof. It receives the event with every byte of that layout intact,
 * detail_size naming the library's full detail (so a reader of a larger
 * element knows which appended fields are present), and nothing written past
 * its element. A caller polling with the full event -- whose size never
 * changes -- receives the appended fields too. The producers zero each event
 * and stamp detail_size with the current detail sizeof, so an event built
 * that way is injected here directly, with a distinct byte in every position
 * of the detail.
 *
 * The poll never refuses an ordinary event for a short element: an element
 * shorter than the event's frozen prefix still receives a truncated copy.
 * What a reader may use from it is bounded twice, by the bytes its element
 * holds and by detail_size. The last section checks both the permitted
 * truncation and that bound.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>
#include <string.h>

#define FIELD_END(T, f) (offsetof(T, f) + sizeof(((T *)0)->f))
#define SIZEOF_THROUGH(T, last) \
    ((FIELD_END(T, last) + _Alignof(T) - 1) / _Alignof(T) * _Alignof(T))

#define GUARD 32u
#define FILL  0xEEu

typedef struct grown {
    const char *name;
    uint32_t    kind;
    size_t      detail;    /* current sizeof */
    size_t      old;       /* sizeof through the last field before the extension */
} grown_t;

#define GROWN(K, T, last) { #K, MOQ_EVENT_##K, sizeof(T), SIZEOF_THROUGH(T, last) }

static const grown_t grown[] = {
    GROWN(NAMESPACE_DONE,             moq_namespace_done_event_t,             ann),
    GROWN(NAMESPACE_CANCELLED,        moq_namespace_cancelled_event_t,        reason),
    GROWN(UNSUBSCRIBED,               moq_unsubscribed_event_t,               sub),
    GROWN(SUBSCRIBE_UPDATED,          moq_subscribe_updated_event_t,          end_group),
    GROWN(FETCH_CANCELLED,            moq_fetch_cancelled_event_t,            fetch),
    GROWN(FETCH_RESET,                moq_fetch_reset_event_t,                _pad),
    GROWN(PUBLISH_FINISHED,           moq_publish_finished_event_t,           reason),
    GROWN(PUBLISH_UNSUBSCRIBED,       moq_publish_unsubscribed_event_t,       pub),
    GROWN(SUBSCRIBE_TRACKS_CANCELLED, moq_subscribe_tracks_cancelled_event_t, handle),
    GROWN(PUBLISH_UPDATED,            moq_publish_updated_event_t,            end_group),
    GROWN(FETCH_REQUEST,              moq_fetch_request_event_t,              joining_pub),
    /* Same size: PUBLISH_REQUEST's settings took over its explicit padding,
     * REDIRECT's handle union names two more request kinds. */
    GROWN(PUBLISH_REQUEST,            moq_publish_request_event_t,            expires_ms),
    GROWN(REQUEST_REDIRECT,           moq_request_redirect_event_t,           reason),
};

static unsigned char pattern(size_t i) { return (unsigned char)(0x11u + i * 7u); }

static moq_session_t *mk_session(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_22;
    moq_session_t *s = NULL;
    return moq_session_create(&cfg, 0, &s) == MOQ_OK ? s : NULL;
}

static void inject(moq_session_t *s, const grown_t *g)
{
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = g->kind;
    e.detail_size = (uint32_t)g->detail;
    e.borrow_epoch = s->borrow_epoch;
    for (size_t i = 0; i < g->detail; i++)
        ((unsigned char *)&e.u)[i] = pattern(i);
    (void)push_event(s, &e);
}

static int check_kind(moq_session_t *s, const grown_t *g)
{
    int failures = 0;
    const size_t prefix = offsetof(moq_event_t, u);
    unsigned char buf[sizeof(moq_event_t) + GUARD];

    MOQ_TEST_CHECK(g->detail <= MOQ_EVENT_DETAIL_MAX);
    MOQ_TEST_CHECK(g->old <= g->detail);

    /* The caller's element ends at the layout before the extension. */
    inject(s, g);
    const size_t elem = prefix + g->old;
    memset(buf, FILL, sizeof(buf));
    size_t n = 99;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(s, buf, 1, elem, &n) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    moq_event_t head;
    memcpy(&head, buf, prefix);
    if (head.kind != g->kind || head.detail_size != g->detail) {
        fprintf(stderr, "FAIL: %s: kind %u detail_size %u, expected %u / %zu\n",
                g->name, (unsigned)head.kind, (unsigned)head.detail_size,
                (unsigned)g->kind, g->detail);
        failures++;
    }
    for (size_t i = 0; i < g->old; i++) {
        if (buf[prefix + i] != pattern(i)) {
            fprintf(stderr, "FAIL: %s: detail byte %zu lost in a %zu-byte element\n",
                    g->name, i, elem);
            failures++;
            break;
        }
    }
    for (size_t i = elem; i < sizeof(buf); i++) {
        if (buf[i] != FILL) {
            fprintf(stderr, "FAIL: %s: wrote byte %zu past a %zu-byte element\n",
                    g->name, i, elem);
            failures++;
            break;
        }
    }

    /* The full event, whose size every revision shares, carries it all. */
    inject(s, g);
    memset(buf, FILL, sizeof(buf));
    n = 99;
    MOQ_TEST_CHECK(moq_session_poll_events_ex(s, buf, 1, sizeof(moq_event_t), &n) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    for (size_t i = 0; i < g->detail; i++) {
        if (buf[prefix + i] != pattern(i)) {
            fprintf(stderr, "FAIL: %s: detail byte %zu lost in the full event\n",
                    g->name, i);
            failures++;
            break;
        }
    }
    for (size_t i = sizeof(moq_event_t); i < sizeof(buf); i++) {
        if (buf[i] != FILL) {
            fprintf(stderr, "FAIL: %s: wrote past the full event\n", g->name);
            failures++;
            break;
        }
    }
    return failures;
}

/* -- Truncation is permitted; reading past what was given is not ---------- */

typedef struct floor_row {
    const char *name;
    uint32_t    kind;
    size_t      detail;     /* current sizeof */
    size_t      floor;      /* the published frozen prefix */
    size_t      tail_end;   /* end of the first field appended after it */
} floor_row_t;

#define FLOOR_ROW(K, T, V0, tail) \
    { #K, MOQ_EVENT_##K, sizeof(T), (V0), FIELD_END(T, tail) }

static const floor_row_t floor_rows[] = {
    FLOOR_ROW(UNSUBSCRIBED, moq_unsubscribed_event_t,
              MOQ_UNSUBSCRIBED_EVENT_V0_SIZE, cause),
    FLOOR_ROW(GOAWAY, moq_goaway_event_t,
              MOQ_GOAWAY_EVENT_V0_SIZE, has_timeout),
    FLOOR_ROW(PUBLISH_FINISHED, moq_publish_finished_event_t,
              MOQ_PUBLISH_FINISHED_EVENT_V0_SIZE, cause),
    FLOOR_ROW(SUBSCRIBE_UPDATED, moq_subscribe_updated_event_t,
              MOQ_SUBSCRIBE_UPDATED_EVENT_V0_SIZE, has_filter),
    FLOOR_ROW(FETCH_CANCELLED, moq_fetch_cancelled_event_t,
              MOQ_FETCH_CANCELLED_EVENT_V0_SIZE, cause),
};

/* Queue one event whose detail holds `fill` patterned bytes and whose
 * producer reports `reported` of them. */
static void inject_reported(moq_session_t *s, uint32_t kind, size_t fill,
                            uint32_t reported)
{
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = kind;
    e.detail_size = reported;
    e.borrow_epoch = s->borrow_epoch;
    for (size_t i = 0; i < fill; i++)
        ((unsigned char *)&e.u)[i] = pattern(i);
    (void)push_event(s, &e);
}

/* The detail bytes a reader may use from one polled element: no more than
 * the element holds, and no more than the producer wrote. */
static size_t usable_detail(size_t element, uint32_t detail_size)
{
    const size_t held = element - offsetof(moq_event_t, u);
    return held < detail_size ? held : (size_t)detail_size;
}

/* Poll one event into the first `elem` bytes of buf. Returns the failure
 * count; `head` receives the event prefix. A queue the poll did not empty is
 * drained, so one refused row cannot feed its event to the next. */
static int poll_one(moq_session_t *s, const floor_row_t *r, size_t elem,
                    unsigned char *buf, size_t buf_size, moq_event_t *head)
{
    int failures = 0;
    const size_t prefix = offsetof(moq_event_t, u);
    size_t n = 99;

    memset(buf, FILL, buf_size);
    memset(head, 0, sizeof(*head));
    MOQ_TEST_CHECK(moq_session_poll_events_ex(s, buf, 1, elem, &n) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    if (n != 1) {
        fprintf(stderr, "FAIL: %s: not delivered into a %zu-byte element\n",
                r->name, elem);
        moq_event_t drop;
        for (int guard = 0; guard < 8 &&
             moq_session_poll_events(s, &drop, 1) == 1; guard++) { }
        return failures;
    }
    memcpy(head, buf, prefix);
    MOQ_TEST_CHECK(head->kind == r->kind);
    for (size_t i = elem; i < buf_size; i++) {
        if (buf[i] != FILL) {
            fprintf(stderr, "FAIL: %s: wrote byte %zu past a %zu-byte element\n",
                    r->name, i, elem);
            failures++;
            break;
        }
    }
    return failures;
}

static int check_floor(moq_session_t *s, const floor_row_t *r)
{
    int failures = 0;
    const size_t prefix = offsetof(moq_event_t, u);
    unsigned char buf[sizeof(moq_event_t) + GUARD];
    moq_event_t head;
    size_t usable;

    MOQ_TEST_CHECK(r->floor >= 1 && r->floor < r->tail_end);
    MOQ_TEST_CHECK(r->tail_end <= r->detail);
    MOQ_TEST_CHECK(r->detail <= MOQ_EVENT_DETAIL_MAX);

    /* One byte short of the frozen prefix: delivered all the same, and
     * detail_size is the producer's extent, not the bytes copied. Nothing
     * of the record is usable to a reader that requires its prefix. */
    inject_reported(s, r->kind, r->detail, (uint32_t)r->detail);
    size_t elem = prefix + r->floor - 1;
    failures += poll_one(s, r, elem, buf, sizeof(buf), &head);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)head.detail_size, r->detail);
    MOQ_TEST_CHECK((size_t)head.detail_size > elem - prefix);
    usable = usable_detail(elem, head.detail_size);
    MOQ_TEST_CHECK_EQ_SIZE(usable, r->floor - 1);
    MOQ_TEST_CHECK(usable < r->floor);

    /* Exactly the frozen prefix: every byte of it arrives. The producer
     * wrote the appended field too, but this element does not hold it. */
    inject_reported(s, r->kind, r->detail, (uint32_t)r->detail);
    elem = prefix + r->floor;
    failures += poll_one(s, r, elem, buf, sizeof(buf), &head);
    for (size_t i = 0; i < r->floor; i++) {
        if (buf[prefix + i] != pattern(i)) {
            fprintf(stderr, "FAIL: %s: prefix byte %zu lost\n", r->name, i);
            failures++;
            break;
        }
    }
    usable = usable_detail(elem, head.detail_size);
    MOQ_TEST_CHECK_EQ_SIZE(usable, r->floor);
    MOQ_TEST_CHECK((size_t)head.detail_size >= r->tail_end);
    MOQ_TEST_CHECK(r->tail_end > usable);

    /* A producer that reports only the frozen prefix, polled with the full
     * event: the element holds bytes past the prefix, yet none of them is a
     * field that producer wrote. */
    inject_reported(s, r->kind, r->detail, (uint32_t)r->floor);
    failures += poll_one(s, r, sizeof(moq_event_t), buf, sizeof(buf), &head);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)head.detail_size, r->floor);
    usable = usable_detail(sizeof(moq_event_t), head.detail_size);
    MOQ_TEST_CHECK_EQ_SIZE(usable, r->floor);
    MOQ_TEST_CHECK(r->tail_end > usable);

    /* The full event from a producer that wrote the appended field: it is
     * covered both ways and arrives intact. */
    inject_reported(s, r->kind, r->detail, (uint32_t)r->detail);
    failures += poll_one(s, r, sizeof(moq_event_t), buf, sizeof(buf), &head);
    usable = usable_detail(sizeof(moq_event_t), head.detail_size);
    MOQ_TEST_CHECK(r->tail_end <= usable);
    for (size_t i = 0; i < r->tail_end; i++) {
        if (buf[prefix + i] != pattern(i)) {
            fprintf(stderr, "FAIL: %s: byte %zu of the appended field lost\n",
                    r->name, i);
            failures++;
            break;
        }
    }
    return failures;
}

int main(void)
{
    int failures = 0;
    moq_session_t *s = mk_session();
    MOQ_TEST_CHECK(s != NULL);
    if (s) {
        for (size_t i = 0; i < sizeof(grown) / sizeof(grown[0]); i++)
            failures += check_kind(s, &grown[i]);
        for (size_t i = 0; i < sizeof(floor_rows) / sizeof(floor_rows[0]); i++)
            failures += check_floor(s, &floor_rows[i]);
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 0);   /* drained */
        moq_session_destroy(s);
    }
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: abi_event_poll_d22\n");
    return 0;
}
