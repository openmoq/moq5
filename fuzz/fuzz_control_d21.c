/*
 * Fuzz target for the draft-21 control and data-plane decoders.
 *
 * The first byte selects a decoder (or all of them when it is 0xFF) and the rest
 * is the input, so one corpus exercises every message family, the nested
 * FILL_PARAMETERS and Range Filter parsing, the setup options, and the datagram
 * and subgroup-header Type Flags. Decoders must never read outside their input or
 * crash; they report malformed input through a negative result.
 *
 * Parameter blocks also get a round-trip property: a block that decodes, uses only
 * what the encoder can emit, and re-encodes must decode again to the same values.
 */
#include <moq/control_d21.h>
#include <moq/buf.h>

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define ALL_MASK 0xFFFFFFFFu

static void check(int ok)
{
    if (!ok) abort();   /* a violated round-trip property is a real bug */
}

static void fuzz_params(const uint8_t *data, size_t size, uint32_t mask)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, size);
    uint64_t count;
    if (moq_buf_read_vi64(&r, &count) < 0) return;
    moq_d21_msg_params_t p;
    if (moq_d21_decode_msg_params(&r, count, mask, &p) != MOQ_OK) return;

    /* The encoder never emits Range Filters or a namespace-prefix update. */
    if (p.range_filter_params != 0 || p.has_track_namespace_prefix ||
        (p.has_fill && p.fill.range_filter_params != 0))
        return;
    uint8_t out[4096];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, sizeof(out));
    if (moq_d21_encode_msg_params(&w, &p) != MOQ_OK) return;   /* e.g. buffer full */

    moq_buf_reader_t r2;
    moq_buf_reader_init(&r2, out, moq_buf_writer_offset(&w));
    uint64_t count2;
    check(moq_buf_read_vi64(&r2, &count2) >= 0);
    moq_d21_msg_params_t q;
    check(moq_d21_decode_msg_params(&r2, count2, ALL_MASK, &q) == MOQ_OK);
    check(moq_buf_reader_remaining(&r2) == 0);
    check(p.has_forward == q.has_forward && p.forward == q.forward);
    check(p.has_subscriber_priority == q.has_subscriber_priority &&
          p.subscriber_priority == q.subscriber_priority);
    check(p.has_group_order == q.has_group_order && p.group_order == q.group_order);
    check(p.has_location_filter == q.has_location_filter);
    if (p.has_location_filter) {
        check(p.location_filter.field_count == q.location_filter.field_count);
        check(p.location_filter.start_group == q.location_filter.start_group);
    }
    check(p.has_largest == q.has_largest && p.largest_group == q.largest_group &&
          p.largest_object == q.largest_object);
    check(p.has_expires == q.has_expires && p.expires_ms == q.expires_ms);
    check(p.has_fill == q.has_fill);
    if (p.has_fill) {
        check(p.fill.has_location_filter == q.fill.has_location_filter);
        check(p.fill.has_group_order == q.fill.has_group_order);
    }
    check(p.auth_token_count == q.auth_token_count);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1) return 0;
    const uint8_t sel = data[0];
    const uint8_t *in = data + 1;
    const size_t n = size - 1;
    const bool all = (sel == 0xFF);
    moq_bytes_t parts[40];

    if (all || sel == 0) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, in, n);
        moq_control_envelope_t env;
        (void)moq_d21_decode_envelope(&r, &env);
    }
    if (all || sel == 1) { moq_d21_setup_opts_t o; (void)moq_d21_decode_setup_opts(in, n, &o); }
    if (all || sel == 2) { fuzz_params(in, n, ALL_MASK); }
    if (all || sel == 3) {
        /* The first input byte picks one of the per-message legality masks. */
        static const uint32_t masks[] = {
            MOQ_D21_MASK_SUBSCRIBE, MOQ_D21_MASK_SUBSCRIBE_TRACKS,
            MOQ_D21_MASK_REQUEST_UPDATE, MOQ_D21_MASK_PUBLISH, MOQ_D21_MASK_FETCH,
            MOQ_D21_MASK_TRACK_STATUS, MOQ_D21_MASK_NAMESPACE_REQUEST,
            MOQ_D21_MASK_SUBSCRIBE_OK, MOQ_D21_MASK_FETCH_OK,
            MOQ_D21_MASK_PUBLISH_STATE_NOTIFY, MOQ_D21_MASK_FILL_NESTED };
        if (n > 0) fuzz_params(in + 1, n - 1,
                               masks[in[0] % (sizeof(masks) / sizeof(masks[0]))]);
    }
    if (all || sel == 4) { moq_d21_subscribe_t m; (void)moq_d21_decode_subscribe(in, n, parts, 40, &m); }
    if (all || sel == 5) { moq_d21_subscribe_ok_t m; (void)moq_d21_decode_subscribe_ok(in, n, &m); }
    if (all || sel == 6) { moq_d21_request_update_t m; (void)moq_d21_decode_request_update(in, n, &m); }
    if (all || sel == 7) {
        moq_d21_request_ok_t m;
        for (int k = MOQ_D21_REQUEST_OK_PUBLISH; k <= MOQ_D21_REQUEST_OK_PUBLISH_NAMESPACE; k++)
            (void)moq_d21_decode_request_ok(in, n, (moq_d21_request_ok_kind_t)k, &m);
    }
    if (all || sel == 8) {
        moq_d21_request_error_t e; moq_d21_redirect_t red;
        (void)moq_d21_decode_request_error(in, n, &e);
        (void)moq_d21_decode_request_error_redirect(in, n, parts, 40, &e, &red);
    }
    if (all || sel == 9) { moq_d21_publish_t m; (void)moq_d21_decode_publish(in, n, parts, 40, &m); }
    if (all || sel == 10) { moq_d21_publish_done_t m; (void)moq_d21_decode_publish_done(in, n, &m); }
    if (all || sel == 11) { moq_d21_publish_state_notify_t m; (void)moq_d21_decode_publish_state_notify(in, n, &m); }
    if (all || sel == 12) { moq_d21_fetch_t m; (void)moq_d21_decode_fetch(in, n, parts, 40, &m); }
    if (all || sel == 13) { moq_d21_fetch_ok_t m; (void)moq_d21_decode_fetch_ok(in, n, &m); }
    if (all || sel == 14) { moq_d21_goaway_t m; (void)moq_d21_decode_goaway(in, n, &m); }
    if (all || sel == 15) { moq_d21_track_status_t m; (void)moq_d21_decode_track_status(in, n, parts, 40, &m); }
    if (all || sel == 16) {
        moq_d21_publish_namespace_t a; moq_d21_subscribe_namespace_t b;
        moq_d21_subscribe_tracks_t c; moq_d21_publish_skipped_t d; moq_namespace_t ns;
        (void)moq_d21_decode_publish_namespace(in, n, parts, 40, &a);
        (void)moq_d21_decode_subscribe_namespace(in, n, parts, 40, &b);
        (void)moq_d21_decode_subscribe_tracks(in, n, parts, 40, &c);
        (void)moq_d21_decode_publish_skipped(in, n, parts, 40, &d);
        (void)moq_d21_decode_namespace_msg(in, n, parts, 40, &ns);
    }
    if (all || sel == 17) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, in, n);
        moq_d21_subgroup_header_t h;
        (void)moq_d21_decode_subgroup_header(&r, &h);
        moq_buf_reader_init(&r, in, n);
        uint64_t id;
        (void)moq_d21_decode_fetch_header(&r, &id);
    }
    if (all || sel == 18) { moq_d21_object_datagram_t d; (void)moq_d21_decode_object_datagram(in, n, &d); }
    if (all || sel == 19) {
        (void)moq_d21_validate_properties(in, n);
        bool dyn;
        (void)moq_d21_scan_dynamic_groups(in, n, &dyn);
    }
    return 0;
}
