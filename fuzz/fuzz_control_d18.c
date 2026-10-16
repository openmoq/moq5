/*
 * libFuzzer target for the draft-18 wire codec (core/include/moq/control_d18.h).
 *
 * Invariants under test:
 *   1. Closed return-status sets:
 *      - moq_d18_decode_envelope, message decoders, stream-header decoders,
 *        and property scanners return only MOQ_OK, MOQ_ERR_BUFFER, or
 *        MOQ_ERR_PROTO (plus MOQ_D18_ERR_KVP_FORMAT on decoders that parse
 *        AUTHORIZATION_TOKEN structures, and MOQ_DONE on padding datagrams).
 *   2. Transactional reader rollback on failure:
 *      - moq_d18_decode_envelope, moq_d18_decode_subgroup_header, and
 *        moq_d18_decode_fetch_header restore r.pos on any negative return.
 *   3. Borrowed-slice bounds:
 *      - Every non-empty moq_bytes_t or byte pointer/length returned on
 *        MOQ_OK points inside [buf, buf + len].
 *   4. Encode/decode symmetry and transactional writer rollback:
 *      - Every decoded structure (when outbound-supported) re-encodes with
 *        MOQ_OK, re-decodes with MOQ_OK, and preserves semantic fields.
 *      - Encoding into an undersized 1-byte writer fails and rolls back
 *        w.pos to its starting offset.
 */

#include <moq/control_d18.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define FUZZ_MAX_INPUT 16384

static void check_rc_basic(moq_result_t rc)
{
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER && rc != MOQ_ERR_PROTO)
        __builtin_trap();
}

static void check_rc_kvp(moq_result_t rc)
{
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER &&
        rc != MOQ_ERR_PROTO && rc != MOQ_D18_ERR_KVP_FORMAT)
        __builtin_trap();
}

static void check_slice_in(const uint8_t *base, size_t len,
                           const uint8_t *ptr, size_t slen)
{
    if (slen == 0)
        return;
    if (!ptr)
        __builtin_trap();
    uintptr_t b = (uintptr_t)base;
    uintptr_t e = b + len;
    uintptr_t p = (uintptr_t)ptr;
    if (p < b || p > e || slen > (size_t)(e - p))
        __builtin_trap();
}

static void check_bytes_in(const uint8_t *base, size_t len, moq_bytes_t s)
{
    check_slice_in(base, len, s.data, s.len);
}

static void check_ns_in(const uint8_t *base, size_t len,
                        const moq_namespace_t *ns)
{
    if (ns->count > 32)
        __builtin_trap();
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len == 0)
            __builtin_trap();
        check_bytes_in(base, len, ns->parts[i]);
    }
}

static void check_params_in(const uint8_t *base, size_t len,
                            const moq_d18_msg_params_t *p)
{
    if (p->auth_token_count > MOQ_D18_MAX_AUTH_TOKENS)
        __builtin_trap();
    for (size_t i = 0; i < p->auth_token_count; i++)
        check_bytes_in(base, len, p->auth_tokens[i].token_value);
}

static bool bytes_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len)
        return false;
    return a.len == 0 || memcmp(a.data, b.data, a.len) == 0;
}

static bool ns_eq(const moq_namespace_t *a, const moq_namespace_t *b)
{
    if (a->count != b->count)
        return false;
    for (size_t i = 0; i < a->count; i++) {
        if (!bytes_eq(a->parts[i], b->parts[i]))
            return false;
    }
    return true;
}

static bool params_eq(const moq_d18_msg_params_t *a,
                      const moq_d18_msg_params_t *b)
{
    if (a->has_forward != b->has_forward ||
        (a->has_forward && a->forward != b->forward))
        return false;
    if (a->has_subscriber_priority != b->has_subscriber_priority ||
        (a->has_subscriber_priority &&
         a->subscriber_priority != b->subscriber_priority))
        return false;
    if (a->has_filter != b->has_filter)
        return false;
    if (a->has_filter) {
        if (a->filter_type != b->filter_type)
            return false;
        if ((a->filter_type == 3 || a->filter_type == 4) &&
            (a->filter_start_group != b->filter_start_group ||
             a->filter_start_object != b->filter_start_object))
            return false;
        if (a->filter_type == 4 &&
            a->filter_end_group != b->filter_end_group)
            return false;
    }
    if (a->has_group_order != b->has_group_order ||
        (a->has_group_order && a->group_order != b->group_order))
        return false;
    if (a->has_expires != b->has_expires ||
        (a->has_expires && a->expires_ms != b->expires_ms))
        return false;
    if (a->has_largest != b->has_largest ||
        (a->has_largest &&
         (a->largest_group != b->largest_group ||
          a->largest_object != b->largest_object)))
        return false;
    if (a->has_object_delivery_timeout != b->has_object_delivery_timeout ||
        (a->has_object_delivery_timeout &&
         a->object_delivery_timeout_ms != b->object_delivery_timeout_ms))
        return false;
    if (a->has_subgroup_delivery_timeout != b->has_subgroup_delivery_timeout ||
        (a->has_subgroup_delivery_timeout &&
         a->subgroup_delivery_timeout_ms != b->subgroup_delivery_timeout_ms))
        return false;
    if (a->has_new_group_request != b->has_new_group_request ||
        (a->has_new_group_request &&
         a->new_group_request != b->new_group_request))
        return false;
    if (a->auth_token_count != b->auth_token_count)
        return false;
    for (size_t i = 0; i < a->auth_token_count; i++) {
        const moq_d18_auth_token_t *ta = &a->auth_tokens[i];
        const moq_d18_auth_token_t *tb = &b->auth_tokens[i];
        if (ta->alias_type != tb->alias_type ||
            ta->alias != tb->alias ||
            ta->token_type != tb->token_type ||
            !bytes_eq(ta->token_value, tb->token_value))
            return false;
    }
    return true;
}

static void fuzz_one_envelope(const moq_control_envelope_t *env)
{
    const uint8_t *p = env->payload;
    size_t plen = env->payload_len;
    uint8_t scratch[32768];
    moq_buf_writer_t w, tw;
    moq_buf_reader_t rr;
    moq_control_envelope_t env2;

    uint8_t short_buf[32768];
    #define CHECK_SHORT_ROLLBACK(encode_expr, needed_len) do { \
        size_t _caps[3] = { 1, 2, (needed_len) > 0 ? (needed_len) - 1 : 0 }; \
        for (size_t _ci = 0; _ci < 3; _ci++) { \
            if (_caps[_ci] >= (needed_len)) continue; \
            moq_buf_writer_init(&tw, short_buf, _caps[_ci]); \
            if ((encode_expr) != MOQ_ERR_BUFFER || tw.pos != 0) \
                __builtin_trap(); \
        } \
    } while (0)

    /* 1. Try type-matched decoder(s) first on the real envelope. */
    switch (env->msg_type) {
    case MOQ_D18_STREAM_SETUP: {
        moq_d18_setup_opts_t opts;
        moq_result_t rc = moq_d18_decode_setup_opts(p, plen, &opts);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            if (opts.auth_token_count > MOQ_D18_MAX_AUTH_TOKENS)
                __builtin_trap();
            for (size_t i = 0; i < opts.auth_token_count; i++)
                check_bytes_in(p, plen, opts.auth_tokens[i].token_value);

            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            moq_result_t erc = moq_d18_encode_setup_opts(&w, &opts);
            if (opts.has_path || opts.has_authority ||
                opts.auth_token_count > 0) {
                if (erc != MOQ_ERR_INVAL || w.pos != 0)
                    __builtin_trap();
            } else {
                if (erc != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_setup_opts(&tw, &opts),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_buf_reader_remaining(&rr) != 0)
                    __builtin_trap();
                moq_d18_setup_opts_t opts2;
                if (moq_d18_decode_setup_opts(env2.payload, env2.payload_len,
                                              &opts2) != MOQ_OK)
                    __builtin_trap();
                if (opts2.has_max_auth_token_cache_size !=
                    opts.has_max_auth_token_cache_size ||
                    opts2.max_auth_token_cache_size !=
                    opts.max_auth_token_cache_size)
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_SUBSCRIBE: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_subscribe_t sub, sub2;
        moq_result_t rc = moq_d18_decode_subscribe(p, plen, parts, 32, &sub);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &sub.track_namespace);
            check_bytes_in(p, plen, sub.track_name);
            check_params_in(p, plen, &sub.params);

            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_subscribe(&w, sub.request_id,
                    &sub.track_namespace, sub.track_name,
                    &sub.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_subscribe(&tw, sub.request_id,
                    &sub.track_namespace, sub.track_name, &sub.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_buf_reader_remaining(&rr) != 0)
                __builtin_trap();
            if (moq_d18_decode_subscribe(env2.payload, env2.payload_len,
                    parts2, 32, &sub2) != MOQ_OK)
                __builtin_trap();
            if (sub2.request_id != sub.request_id ||
                !ns_eq(&sub2.track_namespace, &sub.track_namespace) ||
                !bytes_eq(sub2.track_name, sub.track_name) ||
                !params_eq(&sub2.params, &sub.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_SUBSCRIBE_OK: {
        moq_d18_subscribe_ok_t sok, sok2;
        moq_result_t rc = moq_d18_decode_subscribe_ok(p, plen, &sok);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_params_in(p, plen, &sok.params);
            check_bytes_in(p, plen, sok.track_properties);

            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            moq_result_t erc = moq_d18_encode_subscribe_ok(
                &w, sok.track_alias, &sok.params, sok.track_properties);
            if (sok.track_properties_unsupported) {
                if (erc != MOQ_ERR_INVAL || w.pos != 0)
                    __builtin_trap();
            } else {
                if (erc != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_subscribe_ok(
                        &tw, sok.track_alias, &sok.params,
                        sok.track_properties),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_buf_reader_remaining(&rr) != 0)
                    __builtin_trap();
                if (moq_d18_decode_subscribe_ok(env2.payload, env2.payload_len,
                        &sok2) != MOQ_OK)
                    __builtin_trap();
                if (sok2.track_alias != sok.track_alias ||
                    !params_eq(&sok2.params, &sok.params) ||
                    !bytes_eq(sok2.track_properties, sok.track_properties) ||
                    sok2.dynamic_groups != sok.dynamic_groups)
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_REQUEST_ERROR: {
        moq_d18_request_error_t err, err2;
        moq_result_t rc = moq_d18_decode_request_error(p, plen, &err);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_bytes_in(p, plen, err.reason);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_request_error(&w, err.error_code,
                    err.retry_interval, err.reason) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_request_error(&tw, err.error_code,
                    err.retry_interval, err.reason),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_request_error(env2.payload, env2.payload_len,
                    &err2) != MOQ_OK)
                __builtin_trap();
            if (err2.error_code != err.error_code ||
                err2.retry_interval != err.retry_interval ||
                !bytes_eq(err2.reason, err.reason))
                __builtin_trap();
        }

        moq_bytes_t rparts[32], rparts2[32];
        moq_d18_request_error_t rerr, rerr2;
        moq_d18_redirect_t redir, redir2;
        moq_result_t rrc = moq_d18_decode_request_error_redirect(
            p, plen, rparts, 32, &rerr, &redir);
        check_rc_basic(rrc);
        if (rrc == MOQ_OK) {
            check_bytes_in(p, plen, rerr.reason);
            if (rerr.error_code == MOQ_D18_ERROR_REDIRECT) {
                check_bytes_in(p, plen, redir.connect_uri);
                check_ns_in(p, plen, &redir.track_namespace);
                check_bytes_in(p, plen, redir.track_name);

                moq_buf_writer_init(&w, scratch, sizeof(scratch));
                if (moq_d18_encode_request_error_redirect(&w, rerr.error_code,
                        rerr.retry_interval, rerr.reason, &redir) != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_request_error_redirect(&tw, rerr.error_code,
                        rerr.retry_interval, rerr.reason, &redir),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_d18_decode_request_error_redirect(
                        env2.payload, env2.payload_len,
                        rparts2, 32, &rerr2, &redir2) != MOQ_OK)
                    __builtin_trap();
                if (rerr2.error_code != rerr.error_code ||
                    rerr2.retry_interval != rerr.retry_interval ||
                    !bytes_eq(rerr2.reason, rerr.reason) ||
                    !bytes_eq(redir2.connect_uri, redir.connect_uri) ||
                    !ns_eq(&redir2.track_namespace, &redir.track_namespace) ||
                    !bytes_eq(redir2.track_name, redir.track_name))
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_REQUEST_OK: {
        moq_result_t rc0 = moq_d18_decode_request_ok(p, plen);
        check_rc_basic(rc0);
        if (rc0 == MOQ_OK) {
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_request_ok(&w) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_request_ok(&tw),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_request_ok(env2.payload,
                                          env2.payload_len) != MOQ_OK)
                __builtin_trap();
        }

        moq_d18_publish_ok_t pok, pok2;
        moq_result_t rc2 = moq_d18_decode_publish_ok(p, plen, &pok);
        check_rc_basic(rc2);
        if (rc2 == MOQ_OK) {
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_publish_ok(&w, &pok.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_publish_ok(&tw, &pok.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_publish_ok(
                    env2.payload, env2.payload_len, &pok2) != MOQ_OK ||
                !params_eq(&pok2.params, &pok.params))
                __builtin_trap();
        }

        moq_d18_track_status_ok_t tsok, tsok2;
        moq_result_t rc3 = moq_d18_decode_track_status_ok(p, plen, &tsok);
        check_rc_basic(rc3);
        if (rc3 == MOQ_OK) {
            check_bytes_in(p, plen, tsok.track_properties);
            bool props_strict_ok = (tsok.track_properties.len == 0) ||
                (moq_d18_validate_properties(tsok.track_properties.data,
                                             tsok.track_properties.len) == MOQ_OK);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            moq_result_t erc = moq_d18_encode_track_status_ok(
                &w, &tsok.params, tsok.track_properties);
            if (!props_strict_ok) {
                if (erc != MOQ_ERR_INVAL || w.pos != 0)
                    __builtin_trap();
            } else {
                if (erc != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_track_status_ok(
                        &tw, &tsok.params, tsok.track_properties),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_d18_decode_track_status_ok(
                        env2.payload, env2.payload_len, &tsok2) != MOQ_OK ||
                    !params_eq(&tsok2.params, &tsok.params) ||
                    !bytes_eq(tsok2.track_properties, tsok.track_properties))
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_REQUEST_UPDATE: {
        moq_d18_request_update_t upd, upd2;
        moq_result_t rc = moq_d18_decode_request_update(p, plen, &upd);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_params_in(p, plen, &upd.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_request_update(&w, upd.request_id,
                                              &upd.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_request_update(&tw, upd.request_id,
                                              &upd.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_request_update(
                    env2.payload, env2.payload_len, &upd2) != MOQ_OK ||
                upd2.request_id != upd.request_id ||
                !params_eq(&upd2.params, &upd.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_NAMESPACE:
    case MOQ_D18_NAMESPACE_DONE: {
        bool is_done = (env->msg_type == MOQ_D18_NAMESPACE_DONE);
        moq_bytes_t parts[32], parts2[32];
        moq_namespace_t sfx, sfx2;
        moq_result_t rc = moq_d18_decode_namespace_msg(
            p, plen, parts, 32, &sfx);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &sfx);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_namespace_msg(&w, &sfx, is_done) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_namespace_msg(&tw, &sfx, is_done),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                env2.msg_type != env->msg_type ||
                moq_d18_decode_namespace_msg(
                    env2.payload, env2.payload_len, parts2, 32, &sfx2) != MOQ_OK ||
                !ns_eq(&sfx2, &sfx))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_PUBLISH_DONE: {
        moq_d18_publish_done_t pd, pd2;
        moq_result_t rc = moq_d18_decode_publish_done(p, plen, &pd);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_bytes_in(p, plen, pd.reason);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_publish_done(&w, pd.status_code,
                    pd.stream_count, pd.reason) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_publish_done(&tw, pd.status_code,
                    pd.stream_count, pd.reason),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_publish_done(
                    env2.payload, env2.payload_len, &pd2) != MOQ_OK ||
                pd2.status_code != pd.status_code ||
                pd2.stream_count != pd.stream_count ||
                !bytes_eq(pd2.reason, pd.reason))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_PUBLISH_NAMESPACE: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_publish_namespace_t pn, pn2;
        moq_result_t rc = moq_d18_decode_publish_namespace(
            p, plen, parts, 32, &pn);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &pn.track_namespace);
            check_params_in(p, plen, &pn.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_publish_namespace(&w, pn.request_id,
                    &pn.track_namespace, &pn.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_publish_namespace(&tw, pn.request_id,
                    &pn.track_namespace, &pn.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_publish_namespace(
                    env2.payload, env2.payload_len, parts2, 32, &pn2) != MOQ_OK ||
                pn2.request_id != pn.request_id ||
                !ns_eq(&pn2.track_namespace, &pn.track_namespace) ||
                !params_eq(&pn2.params, &pn.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_SUBSCRIBE_NAMESPACE: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_subscribe_namespace_t sn, sn2;
        moq_result_t rc = moq_d18_decode_subscribe_namespace(
            p, plen, parts, 32, &sn);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &sn.track_namespace_prefix);
            check_params_in(p, plen, &sn.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_subscribe_namespace(&w, sn.request_id,
                    &sn.track_namespace_prefix, &sn.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_subscribe_namespace(&tw, sn.request_id,
                    &sn.track_namespace_prefix, &sn.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_subscribe_namespace(
                    env2.payload, env2.payload_len, parts2, 32, &sn2) != MOQ_OK ||
                sn2.request_id != sn.request_id ||
                !ns_eq(&sn2.track_namespace_prefix,
                       &sn.track_namespace_prefix) ||
                !params_eq(&sn2.params, &sn.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_FETCH: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_fetch_t f, f2;
        moq_result_t rc = moq_d18_decode_fetch(p, plen, parts, 32, &f);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            if (f.fetch_type == MOQ_D18_FETCH_TYPE_STANDALONE) {
                check_ns_in(p, plen, &f.track_namespace);
                check_bytes_in(p, plen, f.track_name);
            }
            check_params_in(p, plen, &f.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_fetch(&w, &f) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_fetch(&tw, &f),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_buf_reader_remaining(&rr) != 0 ||
                moq_d18_decode_fetch(
                    env2.payload, env2.payload_len, parts2, 32, &f2) != MOQ_OK ||
                f2.request_id != f.request_id ||
                f2.fetch_type != f.fetch_type ||
                !params_eq(&f2.params, &f.params))
                __builtin_trap();
            if (f.fetch_type == MOQ_D18_FETCH_TYPE_STANDALONE) {
                if (!ns_eq(&f2.track_namespace, &f.track_namespace) ||
                    !bytes_eq(f2.track_name, f.track_name) ||
                    f2.start.group != f.start.group ||
                    f2.start.object != f.start.object ||
                    f2.end.group != f.end.group ||
                    f2.end.object != f.end.object)
                    __builtin_trap();
            } else {
                if (f2.joining_request_id != f.joining_request_id ||
                    f2.joining_start != f.joining_start)
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_FETCH_OK: {
        moq_d18_fetch_ok_t fok, fok2;
        moq_result_t rc = moq_d18_decode_fetch_ok(p, plen, &fok);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_bytes_in(p, plen, fok.track_properties);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            moq_result_t erc = moq_d18_encode_fetch_ok(
                &w, fok.end_of_track, fok.end, fok.track_properties);
            if (fok.track_properties_unsupported) {
                if (erc != MOQ_ERR_INVAL || w.pos != 0)
                    __builtin_trap();
            } else {
                if (erc != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_fetch_ok(
                        &tw, fok.end_of_track, fok.end, fok.track_properties),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_d18_decode_fetch_ok(
                        env2.payload, env2.payload_len, &fok2) != MOQ_OK ||
                    fok2.end_of_track != fok.end_of_track ||
                    fok2.end.group != fok.end.group ||
                    fok2.end.object != fok.end.object ||
                    !bytes_eq(fok2.track_properties, fok.track_properties))
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_PUBLISH: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_publish_t pub, pub2;
        moq_result_t rc = moq_d18_decode_publish(p, plen, parts, 32, &pub);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &pub.track_namespace);
            check_bytes_in(p, plen, pub.track_name);
            check_params_in(p, plen, &pub.params);
            check_bytes_in(p, plen, pub.track_properties);

            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            moq_result_t erc = moq_d18_encode_publish(&w, &pub);
            if (pub.track_properties_unsupported) {
                if (erc != MOQ_ERR_INVAL || w.pos != 0)
                    __builtin_trap();
            } else {
                if (erc != MOQ_OK)
                    __builtin_trap();
                CHECK_SHORT_ROLLBACK(
                    moq_d18_encode_publish(&tw, &pub),
                    moq_buf_writer_offset(&w));
                moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
                if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                    moq_d18_decode_publish(
                        env2.payload, env2.payload_len, parts2, 32, &pub2) != MOQ_OK ||
                    pub2.request_id != pub.request_id ||
                    pub2.track_alias != pub.track_alias ||
                    !ns_eq(&pub2.track_namespace, &pub.track_namespace) ||
                    !bytes_eq(pub2.track_name, pub.track_name) ||
                    !params_eq(&pub2.params, &pub.params) ||
                    !bytes_eq(pub2.track_properties, pub.track_properties) ||
                    pub2.dynamic_groups != pub.dynamic_groups)
                    __builtin_trap();
            }
        }
        break;
    }

    case MOQ_D18_GOAWAY: {
        moq_d18_goaway_t ga, ga2;
        moq_result_t rc = moq_d18_decode_goaway(p, plen, &ga);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_bytes_in(p, plen, ga.uri);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_goaway(&w, ga.uri.data, ga.uri.len,
                    ga.timeout_ms, ga.request_id) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_goaway(&tw, ga.uri.data, ga.uri.len,
                    ga.timeout_ms, ga.request_id),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_goaway(
                    env2.payload, env2.payload_len, &ga2) != MOQ_OK ||
                ga2.timeout_ms != ga.timeout_ms ||
                ga2.request_id != ga.request_id ||
                !bytes_eq(ga2.uri, ga.uri))
                __builtin_trap();
        }

        moq_d18_goaway_t gar, gar2;
        moq_result_t rcr = moq_d18_decode_goaway_request(p, plen, &gar);
        check_rc_basic(rcr);
        if (rcr == MOQ_OK) {
            check_bytes_in(p, plen, gar.uri);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_goaway_request(&w, gar.uri.data, gar.uri.len,
                    gar.timeout_ms) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_goaway_request(&tw, gar.uri.data, gar.uri.len,
                    gar.timeout_ms),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_goaway_request(
                    env2.payload, env2.payload_len, &gar2) != MOQ_OK ||
                gar2.timeout_ms != gar.timeout_ms ||
                !bytes_eq(gar2.uri, gar.uri))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_TRACK_STATUS: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_track_status_t ts, ts2;
        moq_result_t rc = moq_d18_decode_track_status(p, plen, parts, 32, &ts);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &ts.track_namespace);
            check_bytes_in(p, plen, ts.track_name);
            check_params_in(p, plen, &ts.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_track_status(&w, ts.request_id,
                    &ts.track_namespace, ts.track_name, &ts.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_track_status(&tw, ts.request_id,
                    &ts.track_namespace, ts.track_name, &ts.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_track_status(
                    env2.payload, env2.payload_len, parts2, 32, &ts2) != MOQ_OK ||
                ts2.request_id != ts.request_id ||
                !ns_eq(&ts2.track_namespace, &ts.track_namespace) ||
                !bytes_eq(ts2.track_name, ts.track_name) ||
                !params_eq(&ts2.params, &ts.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_SUBSCRIBE_TRACKS: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_subscribe_tracks_t st, st2;
        moq_result_t rc = moq_d18_decode_subscribe_tracks(
            p, plen, parts, 32, &st);
        check_rc_kvp(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &st.track_namespace_prefix);
            check_params_in(p, plen, &st.params);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_subscribe_tracks(&w, st.request_id,
                    &st.track_namespace_prefix, &st.params) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_subscribe_tracks(&tw, st.request_id,
                    &st.track_namespace_prefix, &st.params),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_subscribe_tracks(
                    env2.payload, env2.payload_len, parts2, 32, &st2) != MOQ_OK ||
                st2.request_id != st.request_id ||
                !ns_eq(&st2.track_namespace_prefix,
                       &st.track_namespace_prefix) ||
                !params_eq(&st2.params, &st.params))
                __builtin_trap();
        }
        break;
    }

    case MOQ_D18_PUBLISH_BLOCKED: {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_publish_blocked_t pb, pb2;
        moq_result_t rc = moq_d18_decode_publish_blocked(
            p, plen, parts, 32, &pb);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_ns_in(p, plen, &pb.track_namespace_suffix);
            check_bytes_in(p, plen, pb.track_name);
            moq_buf_writer_init(&w, scratch, sizeof(scratch));
            if (moq_d18_encode_publish_blocked(
                    &w, &pb.track_namespace_suffix, pb.track_name) != MOQ_OK)
                __builtin_trap();
            CHECK_SHORT_ROLLBACK(
                moq_d18_encode_publish_blocked(
                    &tw, &pb.track_namespace_suffix, pb.track_name),
                moq_buf_writer_offset(&w));
            moq_buf_reader_init(&rr, scratch, moq_buf_writer_offset(&w));
            if (moq_d18_decode_envelope(&rr, &env2) != MOQ_OK ||
                moq_d18_decode_publish_blocked(
                    env2.payload, env2.payload_len, parts2, 32, &pb2) != MOQ_OK ||
                !ns_eq(&pb2.track_namespace_suffix,
                       &pb.track_namespace_suffix) ||
                !bytes_eq(pb2.track_name, pb.track_name))
                __builtin_trap();
        }
        break;
    }

    default:
        break;
    }

    #undef CHECK_SHORT_ROLLBACK
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > FUZZ_MAX_INPUT)
        return 0;

    /* 1. Envelope stream loop. */
    {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data, size);
        while (moq_buf_reader_remaining(&r) > 0) {
            size_t before = r.pos;
            moq_control_envelope_t env;
            moq_result_t rc = moq_d18_decode_envelope(&r, &env);
            check_rc_basic(rc);
            if (rc != MOQ_OK) {
                if (r.pos != before)
                    __builtin_trap();
                break;
            }
            check_slice_in(data, size, env.payload, env.payload_len);
            fuzz_one_envelope(&env);
        }
    }

    /* 2. Subgroup stream header decode + round-trip + rollback. */
    {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data, size);
        moq_d18_subgroup_header_t hdr, hdr2;
        moq_result_t rc = moq_d18_decode_subgroup_header(&r, &hdr);
        check_rc_basic(rc);
        if (rc != MOQ_OK) {
            if (r.pos != 0)
                __builtin_trap();
        } else {
            if (!moq_d18_subgroup_type_valid(hdr.type))
                __builtin_trap();
            uint8_t buf[64];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            if (moq_d18_encode_subgroup_header(&w, &hdr) != MOQ_OK)
                __builtin_trap();
            moq_buf_reader_t r2;
            moq_buf_reader_init(&r2, buf, moq_buf_writer_offset(&w));
            if (moq_d18_decode_subgroup_header(&r2, &hdr2) != MOQ_OK ||
                moq_buf_reader_remaining(&r2) != 0 ||
                hdr2.type != hdr.type ||
                hdr2.track_alias != hdr.track_alias ||
                hdr2.group_id != hdr.group_id ||
                (hdr.subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT &&
                 hdr2.subgroup_id != hdr.subgroup_id) ||
                (!hdr.default_priority &&
                 hdr2.publisher_priority != hdr.publisher_priority))
                __builtin_trap();
        }
    }

    /* 3. Fetch stream header decode + round-trip + rollback. */
    {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data, size);
        uint64_t req_id = 0, req_id2 = 0;
        moq_result_t rc = moq_d18_decode_fetch_header(&r, &req_id);
        check_rc_basic(rc);
        if (rc != MOQ_OK) {
            if (r.pos != 0)
                __builtin_trap();
        } else {
            uint8_t buf[32];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            if (moq_d18_encode_fetch_header(&w, req_id) != MOQ_OK)
                __builtin_trap();
            moq_buf_reader_t r2;
            moq_buf_reader_init(&r2, buf, moq_buf_writer_offset(&w));
            if (moq_d18_decode_fetch_header(&r2, &req_id2) != MOQ_OK ||
                moq_buf_reader_remaining(&r2) != 0 ||
                req_id2 != req_id)
                __builtin_trap();
        }
    }

    /* 4. Object & padding datagram decode + round-trip. */
    {
        moq_d18_object_datagram_t dg, dg2;
        moq_result_t rc = moq_d18_decode_object_datagram(data, size, &dg);
        if (rc != MOQ_OK && rc != MOQ_DONE &&
            rc != MOQ_ERR_BUFFER && rc != MOQ_ERR_PROTO)
            __builtin_trap();
        if (rc == MOQ_OK) {
            if (dg.has_properties)
                check_slice_in(data, size, dg.properties, dg.properties_len);
            if (!dg.is_status)
                check_slice_in(data, size, dg.payload, dg.payload_len);

            uint8_t buf[32768];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            if (moq_d18_encode_object_datagram(&w, &dg) != MOQ_OK)
                __builtin_trap();
            if (moq_d18_decode_object_datagram(
                    buf, moq_buf_writer_offset(&w), &dg2) != MOQ_OK)
                __builtin_trap();
            if (dg2.track_alias != dg.track_alias ||
                dg2.group_id != dg.group_id ||
                dg2.object_id != dg.object_id ||
                dg2.default_priority != dg.default_priority ||
                (!dg.default_priority &&
                 dg2.publisher_priority != dg.publisher_priority) ||
                dg2.has_properties != dg.has_properties ||
                dg2.end_of_group != dg.end_of_group ||
                dg2.is_status != dg.is_status ||
                (dg.is_status && dg2.object_status != dg.object_status) ||
                (!dg.is_status &&
                 (dg2.payload_len != dg.payload_len ||
                  memcmp(dg2.payload, dg.payload, dg.payload_len) != 0)))
                __builtin_trap();
        }
    }

    /* 5. Standalone Redirect structure decode + round-trip. */
    {
        moq_bytes_t parts[32], parts2[32];
        moq_d18_redirect_t redir, redir2;
        moq_result_t rc = moq_d18_decode_redirect(
            data, size, parts, 32, &redir);
        check_rc_basic(rc);
        if (rc == MOQ_OK) {
            check_bytes_in(data, size, redir.connect_uri);
            check_ns_in(data, size, &redir.track_namespace);
            check_bytes_in(data, size, redir.track_name);

            uint8_t buf[32768];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            if (moq_d18_encode_redirect(&w, &redir) != MOQ_OK)
                __builtin_trap();
            if (moq_d18_decode_redirect(buf, moq_buf_writer_offset(&w),
                    parts2, 32, &redir2) != MOQ_OK ||
                !bytes_eq(redir2.connect_uri, redir.connect_uri) ||
                !ns_eq(&redir2.track_namespace, &redir.track_namespace) ||
                !bytes_eq(redir2.track_name, redir.track_name))
                __builtin_trap();
        }
    }

    /* 6. Property block validators & scanners. */
    {
        bool dg = false;
        moq_result_t vrc = moq_d18_validate_properties(data, size);
        moq_result_t src = moq_d18_scan_dynamic_groups(data, size, &dg);
        check_rc_basic(vrc);
        check_rc_basic(src);
        /* Strict validation is a subset of lenient dynamic-groups scan. */
        if (vrc == MOQ_OK && src != MOQ_OK)
            __builtin_trap();
    }

    return 0;
}
