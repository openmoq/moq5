#ifndef MOQ_TEST_RAW_PAIR_D22_H
#define MOQ_TEST_RAW_PAIR_D22_H

/*
 * Raw draft-22 control messages fed into a raw pair's session. It encodes
 * with the internal draft-22 message codec, so only a test linking the
 * white-box core (moq-core-test-internals) includes it.
 */
#include "test_raw_pair_support.h"
#include "../../core/src/wire/messages_d22_internal.h"

/* A draft-22 control message on a request stream. */
RP_UNUSED static moq_result_t feed(moq_session_t *s, moq_stream_ref_t ref, const moq_d22_message_t *m,
                         const moq_d22_param_t *params, size_t n, uint64_t request_type,
                         bool from_subscriber, uint64_t now)
{
    moq_d22_message_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.request_type = request_type;
    ctx.update_from_subscriber = from_subscriber;
    ctx.max_filter_ranges = 8;
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_d22_encode_message(&w, m, params, n, &ctx);
    if (rc < 0) return rc;
    return moq_session_on_bidi_stream_bytes(s, ref, buf, moq_buf_writer_offset(&w), false, now);
}

#endif /* MOQ_TEST_RAW_PAIR_D22_H */
