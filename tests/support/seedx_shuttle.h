#ifndef MOQR_SEEDX_SHUTTLE_H
#define MOQR_SEEDX_SHUTTLE_H

/*
 * The fake-wire peer shuttle: a driver endpoint (bare sans-I/O session behind
 * a REAL moq_transport_bridge_t over the shared fake_endpoint) exchanging
 * bytes with a managed child (the real adapter over the fake MsQuic table).
 * Nothing here reimplements a transport contract: the driver's actions are
 * consumed by the shipping bridge (WOULD_BLOCK retry, half-close tombstones
 * and all), and the child's side is the shipping adapter. The shuttle only
 * moves bytes and transitions between the two fakes, deterministically, with
 * per-round budgets — and taps everything the child writes, per stream, so a
 * script can assert the exact frames on the wire rather than trusting
 * normalized events.
 *
 * "Bytes copied" is never "action completed": driver-side completion is the
 * bridge's own service loop draining its op queue; child-side completion is
 * the delivered SEND_COMPLETE plus the adapter's pending counters.
 */

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include <moq/session.h>
#include <moq/transport_bridge.h>

#include "held_bridge_driver.h"
#include "support/fake_msq_managed.h"

/* lives in shared test support: the RELAY tree never includes the bridge
 * header — its charter is to see only moq_session_t; this harness is
 * transport tooling, beside fake_endpoint.h where it belongs */

#define SHX_MAX_STREAMS 24
#define SHX_TAP_CAP     8192
#define SHX_MAX_EVENTS  128

typedef struct shx_tap {
    uint64_t id;
    bool     used;
    uint8_t  bytes[SHX_TAP_CAP];
    size_t   len;
    size_t   delivered;    /* bytes already handed to the driver bridge  */
    bool     fin_seen;
} shx_tap_t;

typedef struct shx_driver {
    moq_session_t          *sess;
    moq_transport_bridge_t *bridge;
    fake_endpoint_t         ep;
    fake_mgd_conn_t        *child;
    fake_mgd_t             *fake;
    moq_version_t           version;
    uint64_t                now;
    size_t                  ep_cursor;      /* fake_endpoint ops consumed  */
    int                     send_cursor;    /* child fake sends consumed   */
    uint64_t                ctrl_bidi_id;   /* d16 control EP stream id    */
    uint64_t                ctrl_child_id;  /* its QUIC id at the child    */
    bool                    ctrl_open;
    /* The fake endpoint allocates ids with a +1 stride, but the child's
     * adapter numbers ITS streams with real QUIC server ids (bidi 1,5,..;
     * uni 3,7,..). Without remapping, a driver-opened stream collides with a
     * child-opened one. Driver EP ids are remapped to proper QUIC client ids
     * toward the child; child ids are offset toward the driver so its bridge
     * can never confuse a peer stream with one it opened itself. */
    struct { uint64_t ep_id, quic_id; bool uni, used; }
                            idmap[SHX_MAX_STREAMS];
    uint32_t                uni_opened, bidi_opened;
    shx_tap_t               tap[SHX_MAX_STREAMS];
    /* per-child-stream shutdown transitions already forwarded */
    int                     shutdowns_seen[SHX_MAX_STREAMS];
    /* driver-side event log (kinds only; suffix checks read live events) */
    uint32_t                ev_kind[SHX_MAX_EVENTS];
    int                     ev_count;
    bool                    compact_ops; /* reset the consumed op history
                                          * after each full round (long
                                          * runs; forgoes post-hoc op
                                          * scanning)                     */
    /* deterministic fragmentation: deliver driver->child WRITE payloads in
     * pieces split at {1,2,7,len-1} when enabled */
    bool                    fragment;
    /* publisher-side capture: the first SUBSCRIBE_REQUEST's handle (event
     * payloads are cleaned at drain time, so the handle is latched here) */
    bool                    got_subscribe;
    moq_subscription_t      subscribe_handle;
    /* subscriber-side decoded-wire capture: terminal codes as the session
     * parsed them off the wire. SUBSCRIBE_DONE status_code is the
     * PUBLISH_DONE status registry; the subgroup-reset code is the
     * data-stream reset registry — distinct namespaces, latched
     * separately so tests pin each against its own registry. */
    int                     done_status_count;
    uint64_t                last_done_status;
    /* The decoded REQUEST_ERROR a downstream subscriber actually read, with
     * its retry fields: a relay that cannot reissue must never hand back a
     * retryable answer. */
    int                     sub_error_count;
    uint64_t                last_sub_error;
    bool                    last_sub_error_can_retry;
    uint64_t                last_sub_error_retry_ms;
    int                     sg_reset_count;
    uint64_t                last_sg_reset_code;
} shx_driver_t;

#define SHX_PEER_ID_OFFSET 1000000ull

/* one-shot: consumed by the next shx_driver_open (close-feed injection) */
static bool shx_arm_stream_start_fail;


/* driver EP id -> QUIC client id at the child (allocating on first sight) */
static uint64_t
shx_map_to_child(shx_driver_t *d, uint64_t ep_id, bool uni)
{
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (d->idmap[i].used && d->idmap[i].ep_id == ep_id &&
            d->idmap[i].uni == uni) {
            return d->idmap[i].quic_id;
        }
    }
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (!d->idmap[i].used) {
            d->idmap[i].used = true;
            d->idmap[i].ep_id = ep_id;
            d->idmap[i].uni = uni;
            d->idmap[i].quic_id =
                uni ? 2ull + 4ull * d->uni_opened++
                    : 0ull + 4ull * d->bidi_opened++;
            return d->idmap[i].quic_id;
        }
    }
    return ep_id; /* map exhausted: fail loudly downstream */
}

static bool
shx_ep_id_known(const shx_driver_t *d, uint64_t ep_id, bool uni,
                uint64_t *quic_id)
{
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (d->idmap[i].used && d->idmap[i].ep_id == ep_id &&
            d->idmap[i].uni == uni) {
            *quic_id = d->idmap[i].quic_id;
            return true;
        }
    }
    return false;
}

static shx_tap_t *
shx_tap_for(shx_driver_t *d, uint64_t id)
{
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (d->tap[i].used && d->tap[i].id == id) {
            return &d->tap[i];
        }
    }
    for (int i = 0; i < SHX_MAX_STREAMS; i++) {
        if (!d->tap[i].used) {
            d->tap[i].used = true;
            d->tap[i].id = id;
            return &d->tap[i];
        }
    }
    return NULL;
}

/* deliver one driver->child payload, optionally fragmented */
static void
shx_deliver_to_child(shx_driver_t *d, fake_msq_stream_t *st,
                     const uint8_t *data, size_t len, bool fin)
{
    if (!d->fragment || len < 3) {
        fake_msq_deliver_receive(st, data, len, fin);
        return;
    }
    size_t cuts[4] = { 1, 2, 7, len - 1 };
    size_t off = 0;

    for (int i = 0; i < 4 && off < len; i++) {
        size_t cut = cuts[i];

        if (cut <= off || cut >= len) {
            continue;
        }
        fake_msq_deliver_receive(st, data + off, cut - off, false);
        off = cut;
    }
    fake_msq_deliver_receive(st, data + off, len - off, fin);
}

static fake_msq_stream_t *
shx_child_stream(shx_driver_t *d, uint64_t id, bool uni, bool create)
{
    fake_msq_t *f = &d->child->fake;

    for (int i = 0; i < f->stream_count; i++) {
        if (f->streams[i].in_use && f->streams[i].id == id) {
            return &f->streams[i];
        }
    }
    if (!create) {
        return NULL;
    }
    return fake_mgd_peer_stream_started(d->child, id, uni);
}

/* resolve a driver-side op stream id (a mapped local open OR an offset
 * peer id) to the child's existing stream, or NULL */
static fake_msq_stream_t *
shx_resolve_op_stream(shx_driver_t *d, uint64_t op_id)
{
    uint64_t qid;

    if (op_id >= SHX_PEER_ID_OFFSET) {
        return shx_child_stream(d, op_id - SHX_PEER_ID_OFFSET, true, false);
    }
    if (shx_ep_id_known(d, op_id, false, &qid) ||
        shx_ep_id_known(d, op_id, true, &qid)) {
        return shx_child_stream(d, qid, true, false);
    }
    return NULL;
}

/* one shuttle round; returns true if any byte or transition moved */
static bool
shx_round(shx_driver_t *d)
{
    bool moved = false;
    size_t held_before = held_bridge_driver(d->bridge)->input.count;

    d->now += 1000;
    (void)held_bridge_service(d->bridge, d->now);

    /* driver -> child: consume newly recorded endpoint ops */
    while (d->ep_cursor < d->ep.count) {
        const fake_op_t *op = &d->ep.ops[d->ep_cursor++];

        moved = true;
        uint64_t qid;

        switch (op->kind) {
        case FAKE_OP_OPEN_UNI:
            qid = shx_map_to_child(d, op->stream_id, true);
            (void)shx_child_stream(d, qid, true, true);
            if (op->data_len > 0) {
                shx_deliver_to_child(d, shx_child_stream(d, qid, true, false),
                                     op->data, op->data_len, op->fin);
            }
            break;
        case FAKE_OP_OPEN_BIDI:
            qid = shx_map_to_child(d, op->stream_id, false);
            if (!d->ctrl_open) {
                d->ctrl_open = true;
                d->ctrl_bidi_id = op->stream_id; /* first bidi: d16 control */
                d->ctrl_child_id = qid;
            }
            (void)shx_child_stream(d, qid, false, true);
            if (op->data_len > 0) {
                shx_deliver_to_child(d,
                                     shx_child_stream(d, qid, false, false),
                                     op->data, op->data_len, op->fin);
            }
            break;
        case FAKE_OP_WRITE: {
            fake_msq_stream_t *st = shx_resolve_op_stream(d, op->stream_id);

            if (st != NULL) {
                shx_deliver_to_child(d, st, op->data, op->data_len, op->fin);
            }
            break;
        }
        case FAKE_OP_RESET: {
            fake_msq_stream_t *st = shx_resolve_op_stream(d, op->stream_id);

            if (st != NULL) {
                fake_mgd_deliver_peer_send_aborted(d->child, st,
                                                   op->error_code);
            }
            break;
        }
        case FAKE_OP_STOP: {
            fake_msq_stream_t *st = shx_resolve_op_stream(d, op->stream_id);

            if (st != NULL) {
                fake_mgd_deliver_peer_recv_aborted(d->child, st,
                                                   op->error_code);
            }
            break;
        }
        case FAKE_OP_CLOSE:
            fake_mgd_deliver_peer_close(d->child, op->error_code);
            fake_mgd_deliver_shutdown_complete(d->child);
            break;
        default:
            break;
        }
    }

    /* child -> driver: consume newly captured sends, tap them, deliver */
    fake_msq_t *f = &d->child->fake;

    while (d->send_cursor < f->send_count) {
        fake_msq_send_t *snd = &f->sends[d->send_cursor++];
        fake_msq_stream_t *st = snd->stream;
        shx_tap_t *tap = shx_tap_for(d, st->id);
        bool fin = (snd->flags & QUIC_SEND_FLAG_FIN) != 0;

        moved = true;
        if (tap != NULL && tap->len + snd->bytes_len <= SHX_TAP_CAP) {
            memcpy(tap->bytes + tap->len, snd->bytes, snd->bytes_len);
            tap->len += snd->bytes_len;
            if (fin) {
                tap->fin_seen = true;
            }
        }
        moq_result_t rc;

        bool child_owned = (st->id & 1u) != 0; /* server-initiated QUIC ids */
        uint64_t drv_id;

        if (child_owned) {
            drv_id = SHX_PEER_ID_OFFSET + st->id;
        } else {
            /* a driver-opened stream: translate back to its EP id */
            drv_id = st->id;
            for (int m2 = 0; m2 < SHX_MAX_STREAMS; m2++) {
                if (d->idmap[m2].used && d->idmap[m2].quic_id == st->id) {
                    drv_id = d->idmap[m2].ep_id;
                    break;
                }
            }
        }
        if (st->uni) {
            rc = held_bridge_uni_bytes(
                d->bridge, drv_id, snd->bytes, snd->bytes_len, fin, d->now);
        } else if (d->ctrl_open && st->id == d->ctrl_child_id &&
                   d->version == MOQ_VERSION_DRAFT_16) {
            rc = moq_transport_bridge_on_peer_control_bytes(
                d->bridge, drv_id, snd->bytes, snd->bytes_len, fin, d->now);
        } else {
            rc = moq_transport_bridge_on_peer_bidi_bytes(
                d->bridge, drv_id, snd->bytes, snd->bytes_len, fin, d->now);
        }
        (void)rc; /* Owned WOULD_BLOCK belongs to the bridge; refused input
                   * belongs to the bounded held driver, replayed by service. */
        (void)fake_msq_deliver_send_complete(f, false);
    }

    /* redeliver any held receive tails: the fake holds bytes the adapter
     * did not consume and stops indicating until re-enabled — exactly like
     * MsQuic — and redelivery is the harness's explicit job */
    for (int i = 0; i < f->stream_count; i++) {
        if (fake_msq_redeliver_held(&f->streams[i])) {
            moved = true;
        }
    }

    /* child stream shutdowns -> driver transitions (reset / stop-sending) */
    for (int i = 0; i < f->stream_count && i < SHX_MAX_STREAMS; i++) {
        fake_msq_stream_t *st = &f->streams[i];

        while (d->shutdowns_seen[i] < st->shutdown_calls &&
               d->shutdowns_seen[i] < FAKE_MSQ_SHUTDOWN_LOG) {
            int k = d->shutdowns_seen[i]++;
            uint32_t fl = st->shutdown_flags_log[k];
            uint64_t code = st->shutdown_codes_log[k];

            moved = true;
            uint64_t sdrv_id = st->id;

            if ((st->id & 1u) != 0) {
                sdrv_id = SHX_PEER_ID_OFFSET + st->id;
            } else {
                for (int m2 = 0; m2 < SHX_MAX_STREAMS; m2++) {
                    if (d->idmap[m2].used && d->idmap[m2].quic_id == st->id) {
                        sdrv_id = d->idmap[m2].ep_id;
                        break;
                    }
                }
            }
            if (fl & QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND) {
                (void)held_bridge_reset(
                    d->bridge, sdrv_id, code, d->now);
            }
            if (fl & QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE) {
                (void)moq_transport_bridge_on_peer_stop_sending(
                    d->bridge, sdrv_id, code, d->now);
            }
        }
    }

    (void)held_bridge_service(d->bridge, d->now);

    /* drain driver events into the log (payload checks read them here) */
    moved |= held_before != held_bridge_driver(d->bridge)->input.count;
    moq_event_t evs[8];
    size_t n;

    while ((n = moq_session_poll_events(d->sess, evs, 8)) > 0) {
        for (size_t e = 0; e < n; e++) {
            if (d->ev_count < SHX_MAX_EVENTS) {
                d->ev_kind[d->ev_count++] = evs[e].kind;
            }
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                !d->got_subscribe) {
                d->got_subscribe = true;
                d->subscribe_handle = evs[e].u.subscribe_request.sub;
            }
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
                d->sub_error_count++;
                d->last_sub_error =
                    (uint64_t)evs[e].u.subscribe_error.error_code;
                d->last_sub_error_can_retry =
                    evs[e].u.subscribe_error.can_retry;
                d->last_sub_error_retry_ms =
                    evs[e].u.subscribe_error.retry_after_ms;
            }
            if (evs[e].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                d->done_status_count++;
                d->last_done_status = evs[e].u.subscribe_done.status_code;
            }
#ifdef MOQ_EVENT_SUBGROUP_RESET
            if (evs[e].kind == MOQ_EVENT_SUBGROUP_RESET) {
                d->sg_reset_count++;
                d->last_sg_reset_code = evs[e].u.subgroup_reset.error_code;
            }
#endif
            moq_event_cleanup(&evs[e]);
        }
        moved = true;
    }
    if (d->compact_ops && d->ep_cursor == d->ep.count) {
        fake_endpoint_clear_ops(&d->ep);
        d->ep_cursor = 0;
    }
    return moved;
}

/* open a driver: bare client session behind the REAL bridge over the fake
 * endpoint, connected to a freshly accepted child of `fake` */
static bool
shx_driver_open(shx_driver_t *d, fake_mgd_t *fake, moq_version_t version,
                const char *alpn)
{
    memset(d, 0, sizeof(*d));
    d->fake = fake;
    d->version = version;
    d->now = 1;

    moq_session_cfg_t scfg;

    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    if (moq_session_create(&scfg, 0, &d->sess) < 0) {
        return false;
    }
    /* DISJOINT endpoint id spaces: the fake endpoint allocates ids with a
     * +1 stride per type, and the bridge treats stream ids by equality
     * alone — overlapping uni/bidi ranges would collide inside the
     * driver's own bridge (a real QUIC transport never produces that).
     * The id map below presents real QUIC client ids to the child. */
    held_endpoint_init(&d->ep, 1000, 2000);

    moq_transport_bridge_cfg_t bcfg;

    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, d->sess, &d->ep.vtable, &d->ep,
                                    &d->bridge) < 0) {
        return false;
    }
    d->child = fake_mgd_accept(fake, alpn);
    if (d->child == NULL) {
        return false;
    }
    if (shx_arm_stream_start_fail) {
        /* one-shot close-feed injection: the child's FIRST StreamStart
         * (its capacity uni at CONNECTED) is refused, so the bridge
         * latches its first fatal while the session stays open */
        shx_arm_stream_start_fail = false;
        d->child->fake.stream_start_fails = 1;
    }
    fake_mgd_deliver_connected(d->child, alpn);
    /* a client session starts in IDLE: begin the setup exchange */
    return moq_session_start(d->sess, d->now) == MOQ_OK;
}

static void
shx_driver_close(shx_driver_t *d)
{
    if (d->bridge != NULL) {
        held_bridge_destroy(d->bridge);
    }
    if (d->sess != NULL) {
        moq_session_destroy(d->sess);
    }
}

static int
shx_ev_count(const shx_driver_t *d, uint32_t kind)
{
    int c = 0;

    for (int i = 0; i < d->ev_count; i++) {
        if (d->ev_kind[i] == kind) {
            c++;
        }
    }
    return c;
}

/* -- wire-frame scanning over a tap ----------------------------------------- *
 * Control framing per both drafts: Type (varint) + Length (16-bit big-endian)
 * + payload. The scanner walks a tap from its start; a partial trailing frame
 * stops the walk (never an error: the tap is a live stream). */

typedef struct shx_frame {
    uint64_t type;
    const uint8_t *payload;
    size_t   len;
} shx_frame_t;

static size_t
shx_varint(const uint8_t *p, size_t avail, uint64_t *out)
{
    if (avail == 0) {
        return 0;
    }
    size_t len = (size_t)1 << (p[0] >> 6);

    if (avail < len) {
        return 0;
    }
    uint64_t v = p[0] & 0x3F;

    for (size_t i = 1; i < len; i++) {
        v = (v << 8) | p[i];
    }
    *out = v;
    return len;
}

/* Scan frames; returns the number found, fills up to cap entries. */
static int
shx_scan(const shx_tap_t *tap, shx_frame_t *out, int cap)
{
    size_t off = 0;
    int n = 0;

    while (off < tap->len && n < cap) {
        uint64_t type;
        size_t tl = shx_varint(tap->bytes + off, tap->len - off, &type);

        if (tl == 0 || tap->len - off < tl + 2) {
            break;
        }
        size_t flen = ((size_t)tap->bytes[off + tl] << 8) |
                      tap->bytes[off + tl + 1];

        if (tap->len - off < tl + 2 + flen) {
            break;
        }
        out[n].type = type;
        out[n].payload = tap->bytes + off + tl + 2;
        out[n].len = flen;
        n++;
        off += tl + 2 + flen;
    }
    return n;
}

/* keep the shared endpoint helper referenced under -Werror=unused-function:
 * consumers of this header use only the shuttle surface */
static inline void
shx_touch_shared_helpers(void)
{
    (void)fake_endpoint_clear_ops;
    (void)fake_endpoint_find;
    (void)fake_endpoint_count_kind;
    (void)fake_endpoint_enable_abort;
}

#endif /* MOQR_SEEDX_SHUTTLE_H */
