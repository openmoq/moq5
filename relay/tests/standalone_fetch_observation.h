#ifndef MOQR_STANDALONE_FETCH_OBSERVATION_H
#define MOQR_STANDALONE_FETCH_OBSERVATION_H

/* What a draft-22 FETCH subscriber and an origin observe through the public
 * session API: the origin's FETCH requests and cancellations, and the
 * subscriber's FETCH events, with exact payload and property bytes. Included
 * after a test rig that provides <string.h>, <stdio.h> and the session API. */
#define RFS_EVENTS 96u
#define RFS_REQUESTS 8u
typedef struct rfs_event {
    moq_event_kind_t kind;
    uint64_t fetch, group, object, code;
    size_t bytes, properties;
    uint8_t body[333], props[16];
} rfs_event_t;
typedef struct rfs_request {
    moq_fetch_t fetch;
    uint64_t sg, so, eg, eo, at, timeout_ms;
    moq_group_order_t order;
    uint8_t priority;
    bool timeout;
} rfs_request_t;
typedef struct rfs_observation {
    rfs_event_t events[RFS_EVENTS];
    rfs_request_t requests[RFS_REQUESTS];
    unsigned n, nr, cancelled, closed;
    int invalid;
} rfs_observation_t;

static inline bool
rfs_key(moq_namespace_t ns, moq_bytes_t name)
{
    return ns.count == 2 && ns.parts != NULL &&
        ns.parts[0].len == 4 && ns.parts[0].data != NULL &&
        memcmp(ns.parts[0].data, "live", 4) == 0 &&
        ns.parts[1].len == 3 && ns.parts[1].data != NULL &&
        memcmp(ns.parts[1].data, "cam", 3) == 0 &&
        name.len == 1 && name.data != NULL && name.data[0] == 'v';
}

static inline bool
rfs_observe(rfs_observation_t *o, const moq_event_t *ev, uint64_t now)
{
    if (ev->kind == MOQ_EVENT_SESSION_CLOSED) { o->closed++; return true; }
    if (ev->kind == MOQ_EVENT_FETCH_CANCELLED) {
        bool known = false;
        for (unsigned i = 0; i < o->nr; i++)
            known |= o->requests[i].fetch._opaque == ev->u.fetch_cancelled.fetch._opaque;
        if (!known) o->invalid++;
        o->cancelled++;
        return true;
    }
    if (ev->kind == MOQ_EVENT_FETCH_REQUEST) {
        const moq_fetch_request_event_t *q = &ev->u.fetch_request;
        if (o->nr == RFS_REQUESTS || !rfs_key(q->track_namespace, q->track_name) ||
            q->joining_sub._opaque != 0 || q->joining_pub._opaque != 0) {
            o->invalid++;
            return true;
        }
        rfs_request_t *r = &o->requests[o->nr++];
        *r = (rfs_request_t){ .fetch = q->fetch, .sg = q->start_group,
            .so = q->start_object, .eg = q->end_group, .eo = q->end_object,
            .at = now, .order = q->group_order, .priority = q->subscriber_priority };
        if (q->options != NULL &&
            q->options->struct_size >= MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE) {
            r->timeout = (q->options->flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) != 0;
            r->timeout_ms = q->options->fill_timeout_ms;
        }
        return true;
    }
    rfs_event_t r = { .kind = ev->kind };
    switch (ev->kind) {
    case MOQ_EVENT_FETCH_OK:
        r.fetch = ev->u.fetch_ok.fetch._opaque;
        r.group = ev->u.fetch_ok.end_group;
        r.object = ev->u.fetch_ok.end_object;
        break;
    case MOQ_EVENT_FETCH_OBJECT: {
        const moq_fetch_object_event_t *v = &ev->u.fetch_object;
        r.fetch = v->fetch._opaque; r.group = v->group_id; r.object = v->object_id;
        r.bytes = v->payload != NULL ? moq_rcbuf_len(v->payload) : 0;
        r.properties = v->properties != NULL ? moq_rcbuf_len(v->properties) : 0;
        if (r.bytes > sizeof(r.body) || r.properties > sizeof(r.props)) {
            o->invalid++;
            return true;
        }
        if (r.bytes != 0) memcpy(r.body, moq_rcbuf_data(v->payload), r.bytes);
        if (r.properties != 0) memcpy(r.props, moq_rcbuf_data(v->properties), r.properties);
        break;
    }
    case MOQ_EVENT_FETCH_GAP:
        r.fetch = ev->u.fetch_gap.fetch._opaque;
        r.group = ev->u.fetch_gap.group_id; r.object = ev->u.fetch_gap.object_id;
        r.code = ev->u.fetch_gap.range_kind;
        break;
    case MOQ_EVENT_FETCH_COMPLETE: r.fetch = ev->u.fetch_complete.fetch._opaque; break;
    case MOQ_EVENT_FETCH_ERROR:
        r.fetch = ev->u.fetch_error.fetch._opaque; r.code = ev->u.fetch_error.error_code;
        break;
    case MOQ_EVENT_FETCH_RESET:
        r.fetch = ev->u.fetch_reset.fetch._opaque; r.code = ev->u.fetch_reset.error_code;
        break;
    default: return false;
    }
    if (o->n == RFS_EVENTS) o->invalid++;
    else o->events[o->n++] = r;
    return true;
}

static inline unsigned
rfs_count(const rfs_observation_t *o, moq_fetch_t f, moq_event_kind_t kind)
{
    unsigned n = 0;
    for (unsigned i = 0; i < o->n; i++)
        n += o->events[i].fetch == f._opaque && o->events[i].kind == kind;
    return n;
}

static inline const rfs_event_t *
rfs_nth(const rfs_observation_t *o, moq_fetch_t f, moq_event_kind_t kind, unsigned n)
{
    for (unsigned i = 0; i < o->n; i++) {
        if (o->events[i].fetch == f._opaque && o->events[i].kind == kind && n-- == 0)
            return &o->events[i];
    }
    return NULL;
}

static inline bool
rfs_request_exact(const rfs_request_t *r, uint64_t g, uint64_t object,
                  moq_group_order_t order)
{
    return r->fetch._opaque != 0 && r->sg == g && r->so == object &&
        r->eg == g && r->eo == object + 1 && r->order == order &&
        r->priority == 61 && !r->timeout && r->timeout_ms == 0;
}

static inline void
rfs_dump(const char *row, const rfs_observation_t *o)
{
    for (unsigned i = 0; i < o->nr; i++) {
        const rfs_request_t *r = &o->requests[i];
        printf("OBS: %s upstream[%u]=%llu:%llu..%llu:%llu order=%u priority=%u "
               "timeout=%u/%llu at=%llu\n", row, i,
               (unsigned long long)r->sg, (unsigned long long)r->so,
               (unsigned long long)r->eg, (unsigned long long)r->eo,
               (unsigned)r->order, (unsigned)r->priority, (unsigned)r->timeout,
               (unsigned long long)r->timeout_ms, (unsigned long long)r->at);
    }
}
#endif
