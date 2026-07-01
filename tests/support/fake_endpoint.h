#ifndef MOQ_FAKE_ENDPOINT_H
#define MOQ_FAKE_ENDPOINT_H

/*
 * Fake endpoint for testing moq_transport_bridge_t in isolation.
 *
 * Records all outbound operations in an outbox queue. Supports
 * configurable backpressure (block_write, block_open_uni, drop_datagram)
 * and result injection.
 *
 * NOT installed. Test infrastructure only.
 */

#include <moq/transport_bridge.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FAKE_EP_MAX_OPS    128
#define FAKE_EP_MAX_DATA   4096

typedef enum {
    FAKE_OP_OPEN_UNI,
    FAKE_OP_OPEN_BIDI,
    FAKE_OP_WRITE,
    FAKE_OP_RESET,
    FAKE_OP_STOP,
    FAKE_OP_DATAGRAM,
    FAKE_OP_CLOSE,
} fake_op_kind_t;

typedef struct {
    fake_op_kind_t kind;
    uint64_t       stream_id;
    uint8_t        data[256];
    size_t         data_len;
    bool           fin;
    uint64_t       error_code;
} fake_op_t;

typedef struct {
    fake_op_t ops[FAKE_EP_MAX_OPS];
    size_t    count;

    uint64_t  next_uni_id;
    uint64_t  next_bidi_id;

    bool      block_write;
    bool      block_open_uni;
    bool      block_open_bidi;
    bool      block_close;
    bool      drop_datagram;
    int       block_count;

    moq_transport_endpoint_ops_t vtable;
} fake_endpoint_t;

static moq_transport_result_t fake_open_uni(void *ctx, uint64_t *out)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->block_open_uni) { ep->block_count++; return MOQ_TRANSPORT_WOULD_BLOCK; }
    uint64_t id = ep->next_uni_id++;
    *out = id;
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_OPEN_UNI;
        o->stream_id = id;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_open_bidi(void *ctx, uint64_t *out)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->block_open_bidi) { ep->block_count++; return MOQ_TRANSPORT_WOULD_BLOCK; }
    uint64_t id = ep->next_bidi_id++;
    *out = id;
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_OPEN_BIDI;
        o->stream_id = id;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_write(void *ctx, uint64_t stream_id,
                                          const uint8_t *data, size_t len,
                                          bool fin)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->block_write) { ep->block_count++; return MOQ_TRANSPORT_WOULD_BLOCK; }
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_WRITE;
        o->stream_id = stream_id;
        o->fin = fin;
        size_t copy = len < sizeof(o->data) ? len : sizeof(o->data);
        if (data && copy > 0) memcpy(o->data, data, copy);
        o->data_len = copy;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_reset(void *ctx, uint64_t stream_id,
                                          uint64_t error_code)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_RESET;
        o->stream_id = stream_id;
        o->error_code = error_code;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_stop(void *ctx, uint64_t stream_id,
                                         uint64_t error_code)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_STOP;
        o->stream_id = stream_id;
        o->error_code = error_code;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_datagram(void *ctx,
                                             const uint8_t *data, size_t len)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->drop_datagram) return MOQ_TRANSPORT_DROPPED;
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_DATAGRAM;
        size_t copy = len < sizeof(o->data) ? len : sizeof(o->data);
        if (data && copy > 0) memcpy(o->data, data, copy);
        o->data_len = copy;
    }
    return MOQ_TRANSPORT_OK;
}

static moq_transport_result_t fake_close(void *ctx, uint64_t code,
                                          const uint8_t *reason,
                                          size_t reason_len)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (ep->block_close) { ep->block_count++; return MOQ_TRANSPORT_WOULD_BLOCK; }
    if (ep->count < FAKE_EP_MAX_OPS) {
        fake_op_t *o = &ep->ops[ep->count++];
        memset(o, 0, sizeof(*o));
        o->kind = FAKE_OP_CLOSE;
        o->error_code = code;
        size_t copy = reason_len < sizeof(o->data) ? reason_len : sizeof(o->data);
        if (reason && copy > 0) memcpy(o->data, reason, copy);
        o->data_len = copy;
    }
    return MOQ_TRANSPORT_OK;
}

static void fake_endpoint_init(fake_endpoint_t *ep,
                                uint64_t uni_base, uint64_t bidi_base)
{
    memset(ep, 0, sizeof(*ep));
    ep->next_uni_id = uni_base;
    ep->next_bidi_id = bidi_base;

    moq_transport_endpoint_ops_t ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    ops.capabilities = MOQ_TRANSPORT_CAP_DATAGRAM;
    ops.open_uni = fake_open_uni;
    ops.open_bidi = fake_open_bidi;
    ops.write = fake_write;
    ops.reset_stream = fake_reset;
    ops.stop_sending = fake_stop;
    ops.send_datagram = fake_datagram;
    ops.close_transport = fake_close;
    ep->vtable = ops;
}

static void fake_endpoint_clear_ops(fake_endpoint_t *ep)
{
    ep->count = 0;
}

static inline const fake_op_t *fake_endpoint_find(const fake_endpoint_t *ep,
                                            fake_op_kind_t kind)
{
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == kind)
            return &ep->ops[i];
    return NULL;
}

static inline size_t fake_endpoint_count_kind(const fake_endpoint_t *ep,
                                        fake_op_kind_t kind)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == kind) n++;
    return n;
}

#ifdef __cplusplus
}
#endif

#endif /* MOQ_FAKE_ENDPOINT_H */
