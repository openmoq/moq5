/*
 * D16 interop wire test vector validator (+ generator).
 *
 * Default mode: read committed .bin fixtures, validate decode fields
 * and byte-identical re-encode against codec output. Read-only.
 *
 * --generate mode: write .bin files and manifest.json to the vector
 * directory. Use after adding new vectors or updating the codec.
 *
 * Usage:
 *   test_vectors_d16 [vector_dir]             # validate only
 *   test_vectors_d16 --generate [vector_dir]  # write + validate
 */

#include <moq/codec.h>
#include "test_support.h"
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

/* ------------------------------------------------------------------ */
/* File I/O helpers                                                    */
/* ------------------------------------------------------------------ */

static int write_vector(const char *dir, const char *filename,
                        const uint8_t *data, size_t len)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, filename);
    FILE *f = fopen(path, "wb");
    if (!f) {
        fprintf(stderr, "write_vector: cannot open %s: %s\n",
                path, strerror(errno));
        return -1;
    }
    if (fwrite(data, 1, len, f) != len) {
        fclose(f);
        return -1;
    }
    fclose(f);
    return 0;
}

static int read_vector(const char *dir, const char *filename,
                       uint8_t *buf, size_t cap, size_t *out_len)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, filename);
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "read_vector: cannot open %s: %s\n",
                path, strerror(errno));
        return -1;
    }
    *out_len = fread(buf, 1, cap, f);
    fclose(f);
    return 0;
}

/* ------------------------------------------------------------------ */
/* JSON helper                                                         */
/* ------------------------------------------------------------------ */

static void write_json_str(FILE *fp, const char *s)
{
    fputc('"', fp);
    for (const char *p = s; *p; p++) {
        if (*p == '"')       fputs("\\\"", fp);
        else if (*p == '\\') fputs("\\\\", fp);
        else                 fputc(*p, fp);
    }
    fputc('"', fp);
}

/* ------------------------------------------------------------------ */
/* KVP param builder helpers                                           */
/* ------------------------------------------------------------------ */

/* Encode a varint value into a static buffer, return kvp entry. */
static moq_kvp_entry_t make_varint_param(uint64_t type, uint64_t value,
                                          uint8_t *vbuf, size_t vbuf_cap)
{
    size_t vlen = moq_quic_varint_encode(value, vbuf, vbuf_cap);
    moq_kvp_entry_t e = {
        .type      = type,
        .value     = vbuf,
        .value_len = vlen,
        .is_varint = true,
        .raw       = NULL,
        .raw_len   = 0,
    };
    return e;
}

static moq_kvp_entry_t make_bytes_param(uint64_t type,
                                         const uint8_t *data, size_t len)
{
    moq_kvp_entry_t e = {
        .type      = type,
        .value     = data,
        .value_len = len,
        .is_varint = false,
        .raw       = NULL,
        .raw_len   = 0,
    };
    return e;
}

/* ------------------------------------------------------------------ */
/* Vector definition                                                   */
/* ------------------------------------------------------------------ */

typedef struct {
    const char  *filename;
    const char  *type_name;
    uint64_t     type_code;
    bool         is_datagram;
    const char  *description;
    const char  *expected_json;
    /* encoded wire bytes filled by generate */
    uint8_t      wire[256];
    size_t       wire_len;
} vector_t;

static void hex_digest(const uint8_t *data, size_t len, char *out, size_t cap)
{
    static const char hex[] = "0123456789abcdef";
    size_t i;
    for (i = 0; i < len && i * 2 + 1 < cap; i++) {
        out[i * 2]     = hex[(data[i] >> 4) & 0xf];
        out[i * 2 + 1] = hex[data[i] & 0xf];
    }
    out[i * 2] = '\0';
}

#define NUM_VECTORS 33

/* ------------------------------------------------------------------ */
/* Phase 1: Generate                                                   */
/* ------------------------------------------------------------------ */

static int generate_vectors(vector_t vecs[NUM_VECTORS])
{
    int idx = 0;

    /* -- 1. CLIENT_SETUP no params ---------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "client_setup_basic.bin";
        v->type_name = "CLIENT_SETUP";
        v->type_code = MOQ_D16_CLIENT_SETUP;
        v->description = "CLIENT_SETUP with no params";
        v->expected_json = "{ \"params_count\": 0 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_client_setup(&w, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 2. CLIENT_SETUP with MAX_REQUEST_ID=100 -------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "client_setup_max_request_id.bin";
        v->type_name = "CLIENT_SETUP";
        v->type_code = MOQ_D16_CLIENT_SETUP;
        v->description = "CLIENT_SETUP with MAX_REQUEST_ID=100";
        v->expected_json = "{ \"params_count\": 1, \"max_request_id\": 100 }";

        uint8_t vbuf[8];
        moq_kvp_entry_t params[1];
        params[0] = make_varint_param(MOQ_SETUP_PARAM_MAX_REQUEST_ID, 100,
                                       vbuf, sizeof(vbuf));

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_client_setup(&w, params, 1) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 3. SERVER_SETUP with MAX_REQUEST_ID + MAX_AUTH_TOKEN_CACHE_SIZE */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "server_setup_params.bin";
        v->type_name = "SERVER_SETUP";
        v->type_code = MOQ_D16_SERVER_SETUP;
        v->description = "SERVER_SETUP with MAX_REQUEST_ID=100 and MAX_AUTH_TOKEN_CACHE_SIZE=10";
        v->expected_json = "{ \"params_count\": 2, \"max_request_id\": 100, \"max_auth_token_cache_size\": 10 }";

        uint8_t vb1[8], vb2[8];
        moq_kvp_entry_t params[2];
        params[0] = make_varint_param(MOQ_SETUP_PARAM_MAX_REQUEST_ID, 100,
                                       vb1, sizeof(vb1));
        params[1] = make_varint_param(MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE, 10,
                                       vb2, sizeof(vb2));

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_server_setup(&w, params, 2) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 4. SUBSCRIBE minimal --------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_minimal.bin";
        v->type_name = "SUBSCRIBE";
        v->type_code = MOQ_D16_SUBSCRIBE;
        v->description = "SUBSCRIBE minimal: request_id=0, namespace=[\"ns\"], track=\"t\", no params";
        v->expected_json = "{ \"request_id\": 0, \"namespace\": [\"ns\"], \"track_name\": \"t\", \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe(&w, 0, &ns, name, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 5. SUBSCRIBE with params ----------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_with_params.bin";
        v->type_name = "SUBSCRIBE";
        v->type_code = MOQ_D16_SUBSCRIBE;
        v->expected_json = "{ \"request_id\": 2, \"namespace\": [\"live\", \"stream\"], \"track_name\": \"video\", \"forward\": true, \"subscriber_priority\": 64, \"group_order\": 1, \"filter_type\": 2 }";
        v->description = "SUBSCRIBE: request_id=2, namespace=[\"live\",\"stream\"], track=\"video\", "
                          "FORWARD=true, SUBSCRIBER_PRIORITY=64, GROUP_ORDER=ASCENDING, filter=NEXT_GROUP";

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("stream"),
        };
        moq_namespace_t ns = { ns_parts, 2 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("video");

        /* Build params in ascending type order:
         * FORWARD=0x10, SUBSCRIBER_PRIORITY=0x20, SUBSCRIPTION_FILTER=0x21,
         * GROUP_ORDER=0x22 */
        uint8_t fwd_v[8], pri_v[8], go_v[8];
        uint8_t filter_buf[16];
        size_t filter_len = 0;
        moq_d16_subscription_filter_t filter = { .filter_type = 0x2 }; /* NEXT_GROUP */
        if (moq_d16_encode_subscription_filter(filter_buf, sizeof(filter_buf),
                                                &filter_len, &filter) != MOQ_OK) return -1;

        moq_kvp_entry_t params[4];
        params[0] = make_varint_param(MOQ_MSG_PARAM_FORWARD, 1, fwd_v, sizeof(fwd_v));
        params[1] = make_varint_param(MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY, 64, pri_v, sizeof(pri_v));
        params[2] = make_bytes_param(MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
                                      filter_buf, filter_len);
        params[3] = make_varint_param(MOQ_MSG_PARAM_GROUP_ORDER, 1, go_v, sizeof(go_v));

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe(&w, 2, &ns, name, params, 4) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 6. SUBSCRIBE_OK minimal ------------------------------------ */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_ok_minimal.bin";
        v->type_name = "SUBSCRIBE_OK";
        v->type_code = MOQ_D16_SUBSCRIBE_OK;
        v->description = "SUBSCRIBE_OK minimal: request_id=0, track_alias=1, no params, no extensions";
        v->expected_json = "{ \"request_id\": 0, \"track_alias\": 1, \"params_count\": 0 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe_ok(&w, 0, 1, NULL, 0, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 7. SUBSCRIBE_OK with LARGEST_OBJECT + EXPIRES --------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_ok_with_params.bin";
        v->type_name = "SUBSCRIBE_OK";
        v->type_code = MOQ_D16_SUBSCRIBE_OK;
        v->expected_json = "{ \"request_id\": 2, \"track_alias\": 5, \"largest_group\": 10, \"largest_object\": 20, \"expires_ms\": 30000 }";
        v->description = "SUBSCRIBE_OK: request_id=2, track_alias=5, "
                          "EXPIRES=30000, LARGEST_OBJECT=(10,20)";

        /* Build EXPIRES (even type 0x08) and LARGEST_OBJECT (odd type 0x09).
         * Types must be ascending. */
        uint8_t exp_v[8];
        moq_kvp_entry_t params[2];
        params[0] = make_varint_param(MOQ_MSG_PARAM_EXPIRES, 30000,
                                       exp_v, sizeof(exp_v));

        /* LARGEST_OBJECT: group(i) + object(i) */
        uint8_t loc_buf[16];
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, loc_buf, sizeof(loc_buf));
        moq_buf_write_varint(&lw, 10);
        moq_buf_write_varint(&lw, 20);
        params[1] = make_bytes_param(MOQ_MSG_PARAM_LARGEST_OBJECT,
                                      loc_buf, moq_buf_writer_offset(&lw));

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe_ok(&w, 2, 5, params, 2, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 8. REQUEST_ERROR no retry ---------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "request_error_no_retry.bin";
        v->type_name = "REQUEST_ERROR";
        v->type_code = MOQ_D16_REQUEST_ERROR;
        v->description = "REQUEST_ERROR: request_id=0, error_code=0x1, retry_interval=0, reason=\"not found\"";
        v->expected_json = "{ \"request_id\": 0, \"error_code\": 1, \"retry_interval\": 0, \"reason\": \"not found\" }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_request_error(&w, 0, 0x1, 0,
                                          (const uint8_t *)"not found", 9) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 9. REQUEST_ERROR with retry -------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "request_error_with_retry.bin";
        v->type_name = "REQUEST_ERROR";
        v->type_code = MOQ_D16_REQUEST_ERROR;
        v->description = "REQUEST_ERROR: request_id=4, error_code=0x2, retry_interval=5001, reason=\"try again\"";
        v->expected_json = "{ \"request_id\": 4, \"error_code\": 2, \"retry_interval\": 5001, \"reason\": \"try again\" }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_request_error(&w, 4, 0x2, 5001,
                                          (const uint8_t *)"try again", 9) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 10. REQUEST_OK no params ----------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "request_ok.bin";
        v->type_name = "REQUEST_OK";
        v->type_code = MOQ_D16_REQUEST_OK;
        v->description = "REQUEST_OK: request_id=0, no params";
        v->expected_json = "{ \"request_id\": 0, \"params_count\": 0 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_request_ok(&w, 0, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 11. PUBLISH_NAMESPACE minimal ------------------------------ */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_namespace_minimal.bin";
        v->type_name = "PUBLISH_NAMESPACE";
        v->type_code = MOQ_D16_PUBLISH_NAMESPACE;
        v->description = "PUBLISH_NAMESPACE: request_id=0, namespace=[\"media\"], no params";
        v->expected_json = "{ \"request_id\": 0, \"namespace\": [\"media\"], \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("media") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish_namespace(&w, 0, &ns, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 12. PUBLISH_NAMESPACE multi-field --------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_namespace_multi.bin";
        v->type_name = "PUBLISH_NAMESPACE";
        v->type_code = MOQ_D16_PUBLISH_NAMESPACE;
        v->description = "PUBLISH_NAMESPACE: request_id=2, namespace=[\"org\",\"example\",\"live\"], no params";
        v->expected_json = "{ \"request_id\": 2, \"namespace\": [\"org\", \"example\", \"live\"], \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("org"),
            MOQ_BYTES_LITERAL("example"),
            MOQ_BYTES_LITERAL("live"),
        };
        moq_namespace_t ns = { ns_parts, 3 };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish_namespace(&w, 2, &ns, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 13. SUBSCRIBE_NAMESPACE with prefix ------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_namespace_prefix.bin";
        v->type_name = "SUBSCRIBE_NAMESPACE";
        v->type_code = MOQ_D16_SUBSCRIBE_NAMESPACE;
        v->description = "SUBSCRIBE_NAMESPACE: request_id=0, prefix=[\"live\"], subscribe_options=2 (BOTH), no params";
        v->expected_json = "{ \"request_id\": 0, \"prefix\": [\"live\"], \"subscribe_options\": 2, \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t prefix = { ns_parts, 1 };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe_namespace(&w, 0, &prefix, 2,
                                                NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 14. SUBSCRIBE_NAMESPACE empty prefix ------------------------ */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "subscribe_namespace_empty.bin";
        v->type_name = "SUBSCRIBE_NAMESPACE";
        v->type_code = MOQ_D16_SUBSCRIBE_NAMESPACE;
        v->description = "SUBSCRIBE_NAMESPACE: request_id=4, prefix=[] (zero fields), subscribe_options=1 (NAMESPACE), no params";
        v->expected_json = "{ \"request_id\": 4, \"prefix\": [], \"subscribe_options\": 1, \"params_count\": 0 }";

        moq_namespace_t prefix = { NULL, 0 };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_subscribe_namespace(&w, 4, &prefix, 1,
                                                NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 15. GOAWAY empty URI --------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "goaway_empty.bin";
        v->type_name = "GOAWAY";
        v->type_code = MOQ_D16_GOAWAY;
        v->description = "GOAWAY with zero-length URI";
        v->expected_json = "{ \"uri_len\": 0 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_goaway(&w, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 16. GOAWAY with URI ---------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "goaway_with_uri.bin";
        v->type_name = "GOAWAY";
        v->type_code = MOQ_D16_GOAWAY;
        v->description = "GOAWAY with URI \"wss://new.example.com\"";
        v->expected_json = "{ \"uri\": \"wss://new.example.com\" }";

        const char *uri = "wss://new.example.com";
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_goaway(&w, (const uint8_t *)uri,
                                   strlen(uri)) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 17. UNSUBSCRIBE -------------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "unsubscribe.bin";
        v->type_name = "UNSUBSCRIBE";
        v->type_code = MOQ_D16_UNSUBSCRIBE;
        v->description = "UNSUBSCRIBE: request_id=6";
        v->expected_json = "{ \"request_id\": 6 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, 6) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 18. PUBLISH_NAMESPACE_DONE --------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_namespace_done.bin";
        v->type_name = "PUBLISH_NAMESPACE_DONE";
        v->type_code = MOQ_D16_PUBLISH_NAMESPACE_DONE;
        v->description = "PUBLISH_NAMESPACE_DONE: request_id=2";
        v->expected_json = "{ \"request_id\": 2 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_varint_msg(&w, MOQ_D16_PUBLISH_NAMESPACE_DONE, 2) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 19. PUBLISH_NAMESPACE_CANCEL ------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_namespace_cancel.bin";
        v->type_name = "PUBLISH_NAMESPACE_CANCEL";
        v->type_code = MOQ_D16_PUBLISH_NAMESPACE_CANCEL;
        v->description = "PUBLISH_NAMESPACE_CANCEL: request_id=4, error_code=0x1, reason=\"cancelled\"";
        v->expected_json = "{ \"request_id\": 4, \"error_code\": 1, \"reason\": \"cancelled\" }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish_namespace_cancel(
                &w, 4, 0x1,
                (const uint8_t *)"cancelled", 9) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 20. FETCH standalone ----------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "fetch_standalone.bin";
        v->type_name = "FETCH";
        v->type_code = MOQ_D16_FETCH;
        v->description = "FETCH standalone: request_id=3, namespace=[\"live\"], track=\"audio\", "
                          "range [0,0)..[5,0), GROUP_ORDER=1";
        v->expected_json = "{ \"request_id\": 3, \"fetch_type\": 1, \"namespace\": [\"live\"], "
                           "\"track_name\": \"audio\", \"start_group\": 0, \"start_object\": 0, "
                           "\"end_group\": 5, \"end_object\": 0, \"params_count\": 1, \"group_order\": 1 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        uint8_t go_buf[8];
        moq_kvp_entry_t params[1];
        params[0] = make_varint_param(MOQ_MSG_PARAM_GROUP_ORDER, 1,
                                       go_buf, sizeof(go_buf));

        moq_d16_fetch_t fetch = {
            .request_id = 3,
            .fetch_type = MOQ_D16_FETCH_TYPE_STANDALONE,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("audio"),
            .start_group = 0, .start_object = 0,
            .end_group = 5, .end_object = 0,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch(&w, &fetch, params, 1) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 21. FETCH relative joining ----------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "fetch_relative_join.bin";
        v->type_name = "FETCH";
        v->type_code = MOQ_D16_FETCH;
        v->description = "FETCH relative joining: request_id=5, joining_request_id=2, joining_start=10, no params";
        v->expected_json = "{ \"request_id\": 5, \"fetch_type\": 2, \"joining_request_id\": 2, "
                           "\"joining_start\": 10, \"params_count\": 0 }";

        moq_d16_fetch_t fetch = {
            .request_id = 5,
            .fetch_type = MOQ_D16_FETCH_TYPE_RELATIVE_JOIN,
            .joining_request_id = 2,
            .joining_start = 10,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch(&w, &fetch, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 22. FETCH_OK ------------------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "fetch_ok.bin";
        v->type_name = "FETCH_OK";
        v->type_code = MOQ_D16_FETCH_OK;
        v->description = "FETCH_OK: request_id=3, end_of_track=1, end_group=20, end_object=5, no params, extensions=\"ext\"";
        v->expected_json = "{ \"request_id\": 3, \"end_of_track\": 1, \"end_group\": 20, "
                           "\"end_object\": 5, \"extensions_len\": 3 }";

        const uint8_t ext[] = "ext";
        moq_d16_fetch_ok_t ok = {
            .request_id = 3,
            .end_of_track = 1,
            .end_group = 20,
            .end_object = 5,
            .params = NULL, .params_count = 0, .params_cap = 0,
            .track_extensions = ext,
            .track_extensions_len = 3,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_ok(&w, &ok) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 23. FETCH_CANCEL --------------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "fetch_cancel.bin";
        v->type_name = "FETCH_CANCEL";
        v->type_code = MOQ_D16_FETCH_CANCEL;
        v->description = "FETCH_CANCEL: request_id=7";
        v->expected_json = "{ \"request_id\": 7 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_varint_msg(&w, MOQ_D16_FETCH_CANCEL, 7) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 24. PUBLISH minimal ------------------------------------------ */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_minimal.bin";
        v->type_name = "PUBLISH";
        v->type_code = MOQ_D16_PUBLISH;
        v->description = "PUBLISH minimal: request_id=0, namespace=[\"ns\"], track=\"t\", "
                          "track_alias=0, no params, no extensions";
        v->expected_json = "{ \"request_id\": 0, \"namespace\": [\"ns\"], \"track_name\": \"t\", "
                           "\"track_alias\": 0, \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_d16_publish_t pub = {
            .request_id = 0,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("t"),
            .track_alias = 0,
            .params = NULL, .params_count = 0, .params_cap = 0,
            .track_extensions = NULL, .track_extensions_len = 0,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish(&w, &pub) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 25. PUBLISH with FORWARD param ------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_with_params.bin";
        v->type_name = "PUBLISH";
        v->type_code = MOQ_D16_PUBLISH;
        v->description = "PUBLISH: request_id=2, namespace=[\"live\",\"stream\"], "
                          "track=\"video\", track_alias=1, FORWARD=true";
        v->expected_json = "{ \"request_id\": 2, \"namespace\": [\"live\", \"stream\"], "
                           "\"track_name\": \"video\", \"track_alias\": 1, "
                           "\"forward\": true, \"params_count\": 1 }";

        moq_bytes_t ns_parts[] = {
            MOQ_BYTES_LITERAL("live"),
            MOQ_BYTES_LITERAL("stream"),
        };
        moq_namespace_t ns = { ns_parts, 2 };

        uint8_t fwd_v[8];
        moq_kvp_entry_t params[1];
        params[0] = make_varint_param(MOQ_MSG_PARAM_FORWARD, 1, fwd_v, sizeof(fwd_v));

        moq_d16_publish_t pub = {
            .request_id = 2,
            .track_namespace = ns,
            .track_name = MOQ_BYTES_LITERAL("video"),
            .track_alias = 1,
            .params = params, .params_count = 1, .params_cap = 1,
            .track_extensions = NULL, .track_extensions_len = 0,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish(&w, &pub) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 26. PUBLISH_OK minimal --------------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_ok.bin";
        v->type_name = "PUBLISH_OK";
        v->type_code = MOQ_D16_PUBLISH_OK;
        v->description = "PUBLISH_OK: request_id=2, no params";
        v->expected_json = "{ \"request_id\": 2, \"params_count\": 0 }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish_ok(&w, 2, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 27. PUBLISH_DONE with reason --------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "publish_done.bin";
        v->type_name = "PUBLISH_DONE";
        v->type_code = MOQ_D16_PUBLISH_DONE;
        v->description = "PUBLISH_DONE: request_id=3, status_code=0, stream_count=10, "
                          "reason=\"finished\"";
        v->expected_json = "{ \"request_id\": 3, \"status_code\": 0, \"stream_count\": 10, "
                           "\"reason\": \"finished\" }";

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_publish_done(&w, 3, 0, 10,
                                         (const uint8_t *)"finished", 8) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 28. TRACK_STATUS minimal ----------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "track_status_minimal.bin";
        v->type_name = "TRACK_STATUS";
        v->type_code = MOQ_D16_TRACK_STATUS;
        v->description = "TRACK_STATUS minimal: request_id=0, namespace=[\"ns\"], track=\"t\", no params";
        v->expected_json = "{ \"request_id\": 0, \"namespace\": [\"ns\"], \"track_name\": \"t\", \"params_count\": 0 }";

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_bytes_t name = MOQ_BYTES_LITERAL("t");

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_track_status(&w, 0, &ns, name, NULL, 0) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 29. OBJECT_DATAGRAM minimal payload -------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "object_datagram_minimal.bin";
        v->type_name = "OBJECT_DATAGRAM";
        v->type_code = 0;
        v->is_datagram = true;
        v->description = "OBJECT_DATAGRAM: track_alias=1, group=0, object=2, priority=128, payload=\"hi\"";
        v->expected_json = "{ \"track_alias\": 1, \"group_id\": 0, \"object_id\": 2, \"priority\": 128 }";

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 0;
        dg.object_id = 2;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"hi";
        dg.payload_len = 2;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 30. OBJECT_DATAGRAM status ----------------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "object_datagram_status.bin";
        v->type_name = "OBJECT_DATAGRAM";
        v->type_code = 0;
        v->is_datagram = true;
        v->description = "OBJECT_DATAGRAM status: track_alias=1, group=5, object=1, status=END_OF_GROUP";
        v->expected_json = "{ \"track_alias\": 1, \"group_id\": 5, \"object_id\": 1, \"status\": 3 }";

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 5;
        dg.object_id = 1;
        dg.publisher_priority = 128;
        dg.is_status = true;
        dg.object_status = 0x3;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 31. OBJECT_DATAGRAM with properties -------------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "object_datagram_properties.bin";
        v->type_name = "OBJECT_DATAGRAM";
        v->type_code = 0;
        v->is_datagram = true;
        v->description = "OBJECT_DATAGRAM with properties: track_alias=2, group=1, object=3";
        v->expected_json = "{ \"track_alias\": 2, \"group_id\": 1, \"object_id\": 3, \"has_extensions\": true }";

        uint8_t props[] = { 0x01, 0x01, 0xAA };
        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 2;
        dg.group_id = 1;
        dg.object_id = 3;
        dg.publisher_priority = 64;
        dg.has_extensions = true;
        dg.extensions = props;
        dg.extensions_len = sizeof(props);
        dg.payload = (const uint8_t *)"data";
        dg.payload_len = 4;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 32. OBJECT_DATAGRAM status END_OF_TRACK ----------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "object_datagram_status_eot.bin";
        v->type_name = "OBJECT_DATAGRAM";
        v->type_code = 0;
        v->is_datagram = true;
        v->description = "OBJECT_DATAGRAM status: track_alias=3, group=10, object=0, status=END_OF_TRACK";
        v->expected_json = "{ \"track_alias\": 3, \"group_id\": 10, \"object_id\": 0, \"status\": 4 }";

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 3;
        dg.group_id = 10;
        dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.is_status = true;
        dg.object_status = 0x4;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* -- 33. OBJECT_DATAGRAM with ZERO_OBJECT_ID ----------------------- */
    {
        vector_t *v = &vecs[idx++];
        v->filename  = "object_datagram_zero_oid.bin";
        v->type_name = "OBJECT_DATAGRAM";
        v->type_code = 0;
        v->is_datagram = true;
        v->description = "OBJECT_DATAGRAM zero_object_id: track_alias=1, group=2, object=0 (omitted), payload=\"z\"";
        v->expected_json = "{ \"track_alias\": 1, \"group_id\": 2, \"object_id\": 0 }";

        moq_d16_object_datagram_t dg;
        memset(&dg, 0, sizeof(dg));
        dg.track_alias = 1;
        dg.group_id = 2;
        dg.object_id = 0;
        dg.publisher_priority = 128;
        dg.payload = (const uint8_t *)"z";
        dg.payload_len = 1;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_object_datagram(&w, &dg) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/* Phase 2: Write vectors to disk                                      */
/* ------------------------------------------------------------------ */

static int write_all_vectors(const char *dir, const vector_t vecs[NUM_VECTORS])
{
    for (int i = 0; i < NUM_VECTORS; i++) {
        if (write_vector(dir, vecs[i].filename,
                         vecs[i].wire, vecs[i].wire_len) < 0) return -1;
    }
    return 0;
}

/* Phase 3 (write_manifest) is now inline in main() to combine
 * control + fetch-data vectors into one manifest. */

/* ------------------------------------------------------------------ */
/* Phase 4: Validate — decode + re-encode roundtrip                    */
/* ------------------------------------------------------------------ */

static int validate_vectors(const char *dir, const vector_t vecs[NUM_VECTORS])
{
    int failures = 0;

    for (int i = 0; i < NUM_VECTORS; i++) {
        const vector_t *v = &vecs[i];
        uint8_t file_buf[256];
        size_t  file_len = 0;

        /* Read vector from disk */
        if (read_vector(dir, v->filename, file_buf, sizeof(file_buf),
                        &file_len) < 0) {
            fprintf(stderr, "FAIL: cannot read %s/%s\n", dir, v->filename);
            failures++;
            continue;
        }

        /* Compare file bytes with generated bytes */
        MOQ_TEST_CHECK_EQ_SIZE(file_len, v->wire_len);
        if (file_len != v->wire_len) continue;
        MOQ_TEST_CHECK(memcmp(file_buf, v->wire, v->wire_len) == 0);

        /* Datagram vectors skip envelope decode. */
        if (v->is_datagram) {
            moq_d16_object_datagram_t dg;
            MOQ_TEST_CHECK(moq_d16_decode_object_datagram(file_buf, file_len,
                                                            &dg) == MOQ_OK);

            if (strcmp(v->filename, "object_datagram_minimal.bin") == 0) {
                MOQ_TEST_CHECK_EQ_U64(dg.track_alias, 1);
                MOQ_TEST_CHECK_EQ_U64(dg.group_id, 0);
                MOQ_TEST_CHECK_EQ_U64(dg.object_id, 2);
                MOQ_TEST_CHECK(dg.publisher_priority == 128);
                MOQ_TEST_CHECK(!dg.is_status);
                MOQ_TEST_CHECK(!dg.has_extensions);
                MOQ_TEST_CHECK(dg.payload_len == 2);
                MOQ_TEST_CHECK(memcmp(dg.payload, "hi", 2) == 0);
            } else if (strcmp(v->filename, "object_datagram_status.bin") == 0) {
                MOQ_TEST_CHECK_EQ_U64(dg.track_alias, 1);
                MOQ_TEST_CHECK_EQ_U64(dg.group_id, 5);
                MOQ_TEST_CHECK_EQ_U64(dg.object_id, 1);
                MOQ_TEST_CHECK(dg.is_status);
                MOQ_TEST_CHECK_EQ_U64(dg.object_status, 0x3);
                MOQ_TEST_CHECK(dg.payload == NULL);
            } else if (strcmp(v->filename, "object_datagram_properties.bin") == 0) {
                MOQ_TEST_CHECK_EQ_U64(dg.track_alias, 2);
                MOQ_TEST_CHECK_EQ_U64(dg.group_id, 1);
                MOQ_TEST_CHECK_EQ_U64(dg.object_id, 3);
                MOQ_TEST_CHECK(dg.publisher_priority == 64);
                MOQ_TEST_CHECK(!dg.default_priority);
                MOQ_TEST_CHECK(!dg.is_status);
                MOQ_TEST_CHECK(dg.has_extensions);
                MOQ_TEST_CHECK(dg.extensions_len == 3);
                uint8_t exp_ext[] = { 0x01, 0x01, 0xAA };
                MOQ_TEST_CHECK(memcmp(dg.extensions, exp_ext, 3) == 0);
                MOQ_TEST_CHECK(dg.payload_len == 4);
                MOQ_TEST_CHECK(memcmp(dg.payload, "data", 4) == 0);
            } else if (strcmp(v->filename, "object_datagram_status_eot.bin") == 0) {
                MOQ_TEST_CHECK_EQ_U64(dg.track_alias, 3);
                MOQ_TEST_CHECK_EQ_U64(dg.group_id, 10);
                MOQ_TEST_CHECK_EQ_U64(dg.object_id, 0);
                MOQ_TEST_CHECK(dg.is_status);
                MOQ_TEST_CHECK(!dg.has_extensions);
                MOQ_TEST_CHECK_EQ_U64(dg.object_status, 0x4);
                MOQ_TEST_CHECK(dg.payload == NULL);
            } else if (strcmp(v->filename, "object_datagram_zero_oid.bin") == 0) {
                MOQ_TEST_CHECK_EQ_U64(dg.track_alias, 1);
                MOQ_TEST_CHECK_EQ_U64(dg.group_id, 2);
                MOQ_TEST_CHECK_EQ_U64(dg.object_id, 0);
                MOQ_TEST_CHECK(dg.payload_len == 1);
                MOQ_TEST_CHECK(dg.payload[0] == 'z');
            }

            /* Re-encode and compare. */
            uint8_t reencode[256];
            moq_buf_writer_t rw;
            moq_buf_writer_init(&rw, reencode, sizeof(reencode));
            MOQ_TEST_CHECK(moq_d16_encode_object_datagram(&rw, &dg) == MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&rw), file_len);
            MOQ_TEST_CHECK(memcmp(reencode, file_buf, file_len) == 0);
            continue;
        }

        /* Decode envelope */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, file_buf, file_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&r, &env) == MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == v->type_code);
        MOQ_TEST_CHECK(moq_buf_reader_remaining(&r) == 0);

        /* Decode payload and validate key fields */
        switch (v->type_code) {

        case MOQ_D16_CLIENT_SETUP: {
            moq_kvp_entry_t dp[8];
            moq_d16_setup_t setup = {
                .params = dp, .params_count = 0, .params_cap = 8
            };
            MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload,
                                                        env.payload_len,
                                                        &setup) == MOQ_OK);
            if (i == 0) { /* basic */
                MOQ_TEST_CHECK(setup.params_count == 0);
            } else if (i == 1) { /* max_request_id */
                MOQ_TEST_CHECK(setup.params_count == 1);
                MOQ_TEST_CHECK(setup.params[0].type == MOQ_SETUP_PARAM_MAX_REQUEST_ID);
                uint64_t val = 0;
                moq_quic_varint_decode(setup.params[0].value,
                                       setup.params[0].value_len, &val);
                MOQ_TEST_CHECK(val == 100);
            }
            break;
        }

        case MOQ_D16_SERVER_SETUP: {
            moq_kvp_entry_t dp[8];
            moq_d16_setup_t setup = {
                .params = dp, .params_count = 0, .params_cap = 8
            };
            MOQ_TEST_CHECK(moq_d16_decode_server_setup(env.payload,
                                                        env.payload_len,
                                                        &setup) == MOQ_OK);
            MOQ_TEST_CHECK(setup.params_count == 2);
            MOQ_TEST_CHECK(setup.params[0].type == MOQ_SETUP_PARAM_MAX_REQUEST_ID);
            MOQ_TEST_CHECK(setup.params[1].type == MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE);
            uint64_t v1 = 0, v2 = 0;
            moq_quic_varint_decode(setup.params[0].value,
                                   setup.params[0].value_len, &v1);
            moq_quic_varint_decode(setup.params[1].value,
                                   setup.params[1].value_len, &v2);
            MOQ_TEST_CHECK(v1 == 100);
            MOQ_TEST_CHECK(v2 == 10);
            break;
        }

        case MOQ_D16_SUBSCRIBE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_t sub = {
                .params = dp, .params_cap = 8,
            };
            MOQ_TEST_CHECK(moq_d16_decode_subscribe(env.payload, env.payload_len,
                                                     parts, 8, &sub) == MOQ_OK);
            if (i == 3) { /* minimal */
                MOQ_TEST_CHECK(sub.request_id == 0);
                MOQ_TEST_CHECK(sub.track_namespace.count == 1);
                MOQ_TEST_CHECK(sub.track_namespace.parts[0].len == 2);
                MOQ_TEST_CHECK(memcmp(sub.track_namespace.parts[0].data, "ns", 2) == 0);
                MOQ_TEST_CHECK(sub.track_name.len == 1);
                MOQ_TEST_CHECK(memcmp(sub.track_name.data, "t", 1) == 0);
                MOQ_TEST_CHECK(sub.params_count == 0);
            } else if (i == 4) { /* with params */
                MOQ_TEST_CHECK(sub.request_id == 2);
                MOQ_TEST_CHECK(sub.track_namespace.count == 2);
                MOQ_TEST_CHECK(sub.track_namespace.parts[0].len == 4);
                MOQ_TEST_CHECK(memcmp(sub.track_namespace.parts[0].data, "live", 4) == 0);
                MOQ_TEST_CHECK(sub.track_namespace.parts[1].len == 6);
                MOQ_TEST_CHECK(memcmp(sub.track_namespace.parts[1].data, "stream", 6) == 0);
                MOQ_TEST_CHECK(sub.track_name.len == 5);
                MOQ_TEST_CHECK(memcmp(sub.track_name.data, "video", 5) == 0);
                MOQ_TEST_CHECK(sub.params_count == 4);
                /* FORWARD=0x10 */
                MOQ_TEST_CHECK(sub.params[0].type == MOQ_MSG_PARAM_FORWARD);
                bool fwd = false;
                MOQ_TEST_CHECK(moq_d16_decode_param_forward(sub.params[0].value,
                    sub.params[0].value_len, &fwd) == MOQ_OK);
                MOQ_TEST_CHECK(fwd == true);
                /* SUBSCRIBER_PRIORITY=0x20 */
                MOQ_TEST_CHECK(sub.params[1].type == MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
                uint8_t pri = 0;
                MOQ_TEST_CHECK(moq_d16_decode_param_subscriber_priority(sub.params[1].value,
                    sub.params[1].value_len, &pri) == MOQ_OK);
                MOQ_TEST_CHECK(pri == 64);
                /* SUBSCRIPTION_FILTER=0x21 */
                MOQ_TEST_CHECK(sub.params[2].type == MOQ_MSG_PARAM_SUBSCRIPTION_FILTER);
                moq_d16_subscription_filter_t filt;
                MOQ_TEST_CHECK(moq_d16_decode_subscription_filter(sub.params[2].value,
                    sub.params[2].value_len, &filt) == MOQ_OK);
                MOQ_TEST_CHECK(filt.filter_type == 0x2); /* NEXT_GROUP */
                /* GROUP_ORDER=0x22 */
                MOQ_TEST_CHECK(sub.params[3].type == MOQ_MSG_PARAM_GROUP_ORDER);
                uint8_t go = 0;
                MOQ_TEST_CHECK(moq_d16_decode_param_group_order(sub.params[3].value,
                    sub.params[3].value_len, &go) == MOQ_OK);
                MOQ_TEST_CHECK(go == 1); /* ASCENDING */
            }
            break;
        }

        case MOQ_D16_SUBSCRIBE_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_ok_t ok = {
                .params = dp, .params_cap = 8,
            };
            MOQ_TEST_CHECK(moq_d16_decode_subscribe_ok(env.payload,
                                                        env.payload_len,
                                                        &ok) == MOQ_OK);
            if (i == 5) { /* minimal */
                MOQ_TEST_CHECK(ok.request_id == 0);
                MOQ_TEST_CHECK(ok.track_alias == 1);
                MOQ_TEST_CHECK(ok.params_count == 0);
                MOQ_TEST_CHECK(ok.track_extensions_len == 0);
            } else if (i == 6) { /* with params */
                MOQ_TEST_CHECK(ok.request_id == 2);
                MOQ_TEST_CHECK(ok.track_alias == 5);
                MOQ_TEST_CHECK(ok.params_count == 2);
                /* EXPIRES=0x08 */
                MOQ_TEST_CHECK(ok.params[0].type == MOQ_MSG_PARAM_EXPIRES);
                uint64_t exp = 0;
                MOQ_TEST_CHECK(moq_d16_decode_param_expires(ok.params[0].value,
                    ok.params[0].value_len, &exp) == MOQ_OK);
                MOQ_TEST_CHECK(exp == 30000);
                /* LARGEST_OBJECT=0x09 */
                MOQ_TEST_CHECK(ok.params[1].type == MOQ_MSG_PARAM_LARGEST_OBJECT);
                moq_d16_location_t loc;
                MOQ_TEST_CHECK(moq_d16_decode_location(ok.params[1].value,
                    ok.params[1].value_len, &loc) == MOQ_OK);
                MOQ_TEST_CHECK(loc.group == 10);
                MOQ_TEST_CHECK(loc.object == 20);
            }
            break;
        }

        case MOQ_D16_REQUEST_ERROR: {
            moq_d16_request_error_t err;
            MOQ_TEST_CHECK(moq_d16_decode_request_error(env.payload,
                                                         env.payload_len,
                                                         &err) == MOQ_OK);
            if (i == 7) { /* no retry */
                MOQ_TEST_CHECK(err.request_id == 0);
                MOQ_TEST_CHECK(err.error_code == 0x1);
                MOQ_TEST_CHECK(err.retry_interval == 0);
                MOQ_TEST_CHECK(err.reason_len == 9);
                MOQ_TEST_CHECK(memcmp(err.reason, "not found", 9) == 0);
            } else if (i == 8) { /* with retry */
                MOQ_TEST_CHECK(err.request_id == 4);
                MOQ_TEST_CHECK(err.error_code == 0x2);
                MOQ_TEST_CHECK(err.retry_interval == 5001);
                MOQ_TEST_CHECK(err.reason_len == 9);
                MOQ_TEST_CHECK(memcmp(err.reason, "try again", 9) == 0);
            }
            break;
        }

        case MOQ_D16_REQUEST_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_request_ok_t ok = { .params = dp, .params_cap = 8 };
            MOQ_TEST_CHECK(moq_d16_decode_request_ok(env.payload,
                                                      env.payload_len,
                                                      &ok) == MOQ_OK);
            MOQ_TEST_CHECK(ok.request_id == 0);
            MOQ_TEST_CHECK(ok.params_count == 0);
            break;
        }

        case MOQ_D16_PUBLISH_NAMESPACE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_publish_namespace_t pn = {
                .params = dp, .params_cap = 8,
            };
            MOQ_TEST_CHECK(moq_d16_decode_publish_namespace(env.payload,
                env.payload_len, parts, 8, &pn) == MOQ_OK);
            if (i == 10) { /* minimal */
                MOQ_TEST_CHECK(pn.request_id == 0);
                MOQ_TEST_CHECK(pn.track_namespace.count == 1);
                MOQ_TEST_CHECK(pn.track_namespace.parts[0].len == 5);
                MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[0].data, "media", 5) == 0);
                MOQ_TEST_CHECK(pn.params_count == 0);
            } else if (i == 11) { /* multi */
                MOQ_TEST_CHECK(pn.request_id == 2);
                MOQ_TEST_CHECK(pn.track_namespace.count == 3);
                MOQ_TEST_CHECK(pn.track_namespace.parts[0].len == 3);
                MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[0].data, "org", 3) == 0);
                MOQ_TEST_CHECK(pn.track_namespace.parts[1].len == 7);
                MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[1].data, "example", 7) == 0);
                MOQ_TEST_CHECK(pn.track_namespace.parts[2].len == 4);
                MOQ_TEST_CHECK(memcmp(pn.track_namespace.parts[2].data, "live", 4) == 0);
                MOQ_TEST_CHECK(pn.params_count == 0);
            }
            break;
        }

        case MOQ_D16_SUBSCRIBE_NAMESPACE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_namespace_t sn = {
                .params = dp, .params_cap = 8,
            };
            MOQ_TEST_CHECK(moq_d16_decode_subscribe_namespace(env.payload,
                env.payload_len, parts, 8, &sn) == MOQ_OK);
            if (i == 12) { /* with prefix */
                MOQ_TEST_CHECK(sn.request_id == 0);
                MOQ_TEST_CHECK(sn.track_namespace_prefix.count == 1);
                MOQ_TEST_CHECK(sn.track_namespace_prefix.parts[0].len == 4);
                MOQ_TEST_CHECK(memcmp(sn.track_namespace_prefix.parts[0].data, "live", 4) == 0);
                MOQ_TEST_CHECK(sn.subscribe_options == 2);
                MOQ_TEST_CHECK(sn.params_count == 0);
            } else if (i == 13) { /* empty prefix */
                MOQ_TEST_CHECK(sn.request_id == 4);
                MOQ_TEST_CHECK(sn.track_namespace_prefix.count == 0);
                MOQ_TEST_CHECK(sn.subscribe_options == 1);
                MOQ_TEST_CHECK(sn.params_count == 0);
            }
            break;
        }

        case MOQ_D16_GOAWAY: {
            moq_d16_goaway_t goaway;
            MOQ_TEST_CHECK(moq_d16_decode_goaway(env.payload, env.payload_len,
                                                  &goaway) == MOQ_OK);
            if (i == 14) { /* empty */
                MOQ_TEST_CHECK(goaway.uri == NULL);
                MOQ_TEST_CHECK(goaway.uri_len == 0);
            } else if (i == 15) { /* with URI */
                MOQ_TEST_CHECK(goaway.uri_len == 21);
                MOQ_TEST_CHECK(memcmp(goaway.uri, "wss://new.example.com", 21) == 0);
            }
            break;
        }

        case MOQ_D16_UNSUBSCRIBE: {
            uint64_t req_id = 0;
            MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len,
                                                      &req_id) == MOQ_OK);
            MOQ_TEST_CHECK(req_id == 6);
            break;
        }

        case MOQ_D16_PUBLISH_NAMESPACE_DONE: {
            uint64_t req_id = 0;
            MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len,
                                                      &req_id) == MOQ_OK);
            MOQ_TEST_CHECK(req_id == 2);
            break;
        }

        case MOQ_D16_PUBLISH_NAMESPACE_CANCEL: {
            moq_d16_publish_namespace_cancel_t cancel;
            MOQ_TEST_CHECK(moq_d16_decode_publish_namespace_cancel(env.payload,
                env.payload_len, &cancel) == MOQ_OK);
            MOQ_TEST_CHECK(cancel.request_id == 4);
            MOQ_TEST_CHECK(cancel.error_code == 0x1);
            MOQ_TEST_CHECK(cancel.reason_len == 9);
            MOQ_TEST_CHECK(memcmp(cancel.reason, "cancelled", 9) == 0);
            break;
        }

        case MOQ_D16_FETCH: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_fetch_t fetch = { .params = dp, .params_cap = 8 };
            MOQ_TEST_CHECK(moq_d16_decode_fetch(env.payload, env.payload_len,
                                                 parts, 8, &fetch) == MOQ_OK);
            if (i == 19) { /* standalone */
                MOQ_TEST_CHECK(fetch.request_id == 3);
                MOQ_TEST_CHECK(fetch.fetch_type == MOQ_D16_FETCH_TYPE_STANDALONE);
                MOQ_TEST_CHECK(fetch.track_namespace.count == 1);
                MOQ_TEST_CHECK(fetch.track_namespace.parts[0].len == 4);
                MOQ_TEST_CHECK(memcmp(fetch.track_namespace.parts[0].data, "live", 4) == 0);
                MOQ_TEST_CHECK(fetch.track_name.len == 5);
                MOQ_TEST_CHECK(memcmp(fetch.track_name.data, "audio", 5) == 0);
                MOQ_TEST_CHECK(fetch.start_group == 0);
                MOQ_TEST_CHECK(fetch.start_object == 0);
                MOQ_TEST_CHECK(fetch.end_group == 5);
                MOQ_TEST_CHECK(fetch.end_object == 0);
                MOQ_TEST_CHECK(fetch.params_count == 1);
                MOQ_TEST_CHECK(fetch.params[0].type == MOQ_MSG_PARAM_GROUP_ORDER);
                uint8_t go = 0;
                MOQ_TEST_CHECK(moq_d16_decode_param_group_order(fetch.params[0].value,
                    fetch.params[0].value_len, &go) == MOQ_OK);
                MOQ_TEST_CHECK(go == 1);
            } else if (i == 20) { /* relative joining */
                MOQ_TEST_CHECK(fetch.request_id == 5);
                MOQ_TEST_CHECK(fetch.fetch_type == MOQ_D16_FETCH_TYPE_RELATIVE_JOIN);
                MOQ_TEST_CHECK(fetch.joining_request_id == 2);
                MOQ_TEST_CHECK(fetch.joining_start == 10);
                MOQ_TEST_CHECK(fetch.params_count == 0);
            }
            break;
        }

        case MOQ_D16_FETCH_OK: {
            moq_d16_fetch_ok_t ok = { .params = NULL, .params_cap = 0 };
            MOQ_TEST_CHECK(moq_d16_decode_fetch_ok(env.payload,
                                                     env.payload_len, &ok) == MOQ_OK);
            MOQ_TEST_CHECK(ok.request_id == 3);
            MOQ_TEST_CHECK(ok.end_of_track == 1);
            MOQ_TEST_CHECK(ok.end_group == 20);
            MOQ_TEST_CHECK(ok.end_object == 5);
            MOQ_TEST_CHECK(ok.track_extensions_len == 3);
            MOQ_TEST_CHECK(memcmp(ok.track_extensions, "ext", 3) == 0);
            break;
        }

        case MOQ_D16_FETCH_CANCEL: {
            uint64_t req_id = 0;
            MOQ_TEST_CHECK(moq_d16_decode_varint_msg(env.payload, env.payload_len,
                                                      &req_id) == MOQ_OK);
            MOQ_TEST_CHECK(req_id == 7);
            break;
        }

        case MOQ_D16_PUBLISH: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_publish_t pub = { .params = dp, .params_cap = 8 };
            MOQ_TEST_CHECK(moq_d16_decode_publish(env.payload, env.payload_len,
                                                    parts, 8, &pub) == MOQ_OK);
            if (i == 23) { /* minimal */
                MOQ_TEST_CHECK(pub.request_id == 0);
                MOQ_TEST_CHECK(pub.track_namespace.count == 1);
                MOQ_TEST_CHECK(pub.track_namespace.parts[0].len == 2);
                MOQ_TEST_CHECK(memcmp(pub.track_namespace.parts[0].data, "ns", 2) == 0);
                MOQ_TEST_CHECK(pub.track_name.len == 1);
                MOQ_TEST_CHECK(memcmp(pub.track_name.data, "t", 1) == 0);
                MOQ_TEST_CHECK(pub.track_alias == 0);
                MOQ_TEST_CHECK(pub.params_count == 0);
                MOQ_TEST_CHECK(pub.track_extensions_len == 0);
            } else if (i == 24) { /* with params */
                MOQ_TEST_CHECK(pub.request_id == 2);
                MOQ_TEST_CHECK(pub.track_namespace.count == 2);
                MOQ_TEST_CHECK(pub.track_namespace.parts[0].len == 4);
                MOQ_TEST_CHECK(memcmp(pub.track_namespace.parts[0].data, "live", 4) == 0);
                MOQ_TEST_CHECK(pub.track_name.len == 5);
                MOQ_TEST_CHECK(memcmp(pub.track_name.data, "video", 5) == 0);
                MOQ_TEST_CHECK(pub.track_alias == 1);
                MOQ_TEST_CHECK(pub.params_count == 1);
                MOQ_TEST_CHECK(pub.params[0].type == MOQ_MSG_PARAM_FORWARD);
                bool fwd = false;
                MOQ_TEST_CHECK(moq_d16_decode_param_forward(pub.params[0].value,
                    pub.params[0].value_len, &fwd) == MOQ_OK);
                MOQ_TEST_CHECK(fwd == true);
            }
            break;
        }

        case MOQ_D16_PUBLISH_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_publish_ok_t ok = { .params = dp, .params_cap = 8 };
            MOQ_TEST_CHECK(moq_d16_decode_publish_ok(env.payload, env.payload_len,
                                                      &ok) == MOQ_OK);
            MOQ_TEST_CHECK(ok.request_id == 2);
            MOQ_TEST_CHECK(ok.params_count == 0);
            break;
        }

        case MOQ_D16_PUBLISH_DONE: {
            moq_d16_publish_done_t done;
            MOQ_TEST_CHECK(moq_d16_decode_publish_done(env.payload, env.payload_len,
                                                        &done) == MOQ_OK);
            MOQ_TEST_CHECK(done.request_id == 3);
            MOQ_TEST_CHECK(done.status_code == 0);
            MOQ_TEST_CHECK(done.stream_count == 10);
            MOQ_TEST_CHECK(done.reason_len == 8);
            MOQ_TEST_CHECK(memcmp(done.reason, "finished", 8) == 0);
            break;
        }

        case MOQ_D16_TRACK_STATUS: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_track_status_t ts = {
                .params = dp, .params_cap = 8,
            };
            MOQ_TEST_CHECK(moq_d16_decode_track_status(env.payload, env.payload_len,
                                                        parts, 8, &ts) == MOQ_OK);
            MOQ_TEST_CHECK(ts.request_id == 0);
            MOQ_TEST_CHECK(ts.track_namespace.count == 1);
            MOQ_TEST_CHECK(ts.track_namespace.parts[0].len == 2);
            MOQ_TEST_CHECK(memcmp(ts.track_namespace.parts[0].data, "ns", 2) == 0);
            MOQ_TEST_CHECK(ts.track_name.len == 1);
            MOQ_TEST_CHECK(memcmp(ts.track_name.data, "t", 1) == 0);
            MOQ_TEST_CHECK(ts.params_count == 0);
            break;
        }

        default:
            fprintf(stderr, "FAIL: unknown type_code 0x%02x for %s\n",
                    (unsigned)v->type_code, v->filename);
            failures++;
            break;
        }

        /* Re-encode into a fresh buffer and compare byte-identically */
        uint8_t reencode[256];
        moq_buf_writer_t rw;
        moq_buf_writer_init(&rw, reencode, sizeof(reencode));
        moq_result_t rc = MOQ_ERR_INTERNAL;

        switch (v->type_code) {
        case MOQ_D16_CLIENT_SETUP: {
            moq_kvp_entry_t dp[8];
            moq_d16_setup_t setup = {
                .params = dp, .params_count = 0, .params_cap = 8
            };
            moq_d16_decode_client_setup(env.payload, env.payload_len, &setup);
            rc = moq_d16_encode_client_setup(&rw, setup.params, setup.params_count);
            break;
        }
        case MOQ_D16_SERVER_SETUP: {
            moq_kvp_entry_t dp[8];
            moq_d16_setup_t setup = {
                .params = dp, .params_count = 0, .params_cap = 8
            };
            moq_d16_decode_server_setup(env.payload, env.payload_len, &setup);
            rc = moq_d16_encode_server_setup(&rw, setup.params, setup.params_count);
            break;
        }
        case MOQ_D16_SUBSCRIBE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_t sub = { .params = dp, .params_cap = 8 };
            moq_d16_decode_subscribe(env.payload, env.payload_len, parts, 8, &sub);
            rc = moq_d16_encode_subscribe(&rw, sub.request_id,
                                           &sub.track_namespace, sub.track_name,
                                           sub.params, sub.params_count);
            break;
        }
        case MOQ_D16_SUBSCRIBE_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_ok_t ok = { .params = dp, .params_cap = 8 };
            moq_d16_decode_subscribe_ok(env.payload, env.payload_len, &ok);
            rc = moq_d16_encode_subscribe_ok(&rw, ok.request_id, ok.track_alias,
                                              ok.params, ok.params_count,
                                              ok.track_extensions,
                                              ok.track_extensions_len);
            break;
        }
        case MOQ_D16_REQUEST_ERROR: {
            moq_d16_request_error_t err;
            moq_d16_decode_request_error(env.payload, env.payload_len, &err);
            rc = moq_d16_encode_request_error(&rw, err.request_id, err.error_code,
                                               err.retry_interval,
                                               err.reason, err.reason_len);
            break;
        }
        case MOQ_D16_REQUEST_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_request_ok_t ok = { .params = dp, .params_cap = 8 };
            moq_d16_decode_request_ok(env.payload, env.payload_len, &ok);
            rc = moq_d16_encode_request_ok(&rw, ok.request_id,
                                            ok.params, ok.params_count);
            break;
        }
        case MOQ_D16_PUBLISH_NAMESPACE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_publish_namespace_t pn = { .params = dp, .params_cap = 8 };
            moq_d16_decode_publish_namespace(env.payload, env.payload_len, parts, 8, &pn);
            rc = moq_d16_encode_publish_namespace(&rw, pn.request_id,
                                                   &pn.track_namespace,
                                                   pn.params, pn.params_count);
            break;
        }
        case MOQ_D16_SUBSCRIBE_NAMESPACE: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_subscribe_namespace_t sn = { .params = dp, .params_cap = 8 };
            moq_d16_decode_subscribe_namespace(env.payload, env.payload_len, parts, 8, &sn);
            rc = moq_d16_encode_subscribe_namespace(&rw, sn.request_id,
                                                     &sn.track_namespace_prefix,
                                                     sn.subscribe_options,
                                                     sn.params, sn.params_count);
            break;
        }
        case MOQ_D16_GOAWAY: {
            moq_d16_goaway_t goaway;
            moq_d16_decode_goaway(env.payload, env.payload_len, &goaway);
            rc = moq_d16_encode_goaway(&rw, goaway.uri, goaway.uri_len);
            break;
        }
        case MOQ_D16_UNSUBSCRIBE:
        case MOQ_D16_PUBLISH_NAMESPACE_DONE: {
            uint64_t val = 0;
            moq_d16_decode_varint_msg(env.payload, env.payload_len, &val);
            rc = moq_d16_encode_varint_msg(&rw, v->type_code, val);
            break;
        }
        case MOQ_D16_PUBLISH_NAMESPACE_CANCEL: {
            moq_d16_publish_namespace_cancel_t cancel;
            moq_d16_decode_publish_namespace_cancel(env.payload, env.payload_len, &cancel);
            rc = moq_d16_encode_publish_namespace_cancel(&rw, cancel.request_id,
                                                          cancel.error_code,
                                                          cancel.reason,
                                                          cancel.reason_len);
            break;
        }
        case MOQ_D16_FETCH: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_fetch_t fetch = { .params = dp, .params_cap = 8 };
            moq_d16_decode_fetch(env.payload, env.payload_len, parts, 8, &fetch);
            rc = moq_d16_encode_fetch(&rw, &fetch,
                                       fetch.params, fetch.params_count);
            break;
        }
        case MOQ_D16_FETCH_OK: {
            moq_d16_fetch_ok_t ok = { .params = NULL, .params_cap = 0 };
            moq_d16_decode_fetch_ok(env.payload, env.payload_len, &ok);
            rc = moq_d16_encode_fetch_ok(&rw, &ok);
            break;
        }
        case MOQ_D16_FETCH_CANCEL: {
            uint64_t val = 0;
            moq_d16_decode_varint_msg(env.payload, env.payload_len, &val);
            rc = moq_d16_encode_varint_msg(&rw, v->type_code, val);
            break;
        }
        case MOQ_D16_PUBLISH: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_publish_t pub = { .params = dp, .params_cap = 8 };
            moq_d16_decode_publish(env.payload, env.payload_len, parts, 8, &pub);
            rc = moq_d16_encode_publish(&rw, &pub);
            break;
        }
        case MOQ_D16_PUBLISH_OK: {
            moq_kvp_entry_t dp[8];
            moq_d16_publish_ok_t ok = { .params = dp, .params_cap = 8 };
            moq_d16_decode_publish_ok(env.payload, env.payload_len, &ok);
            rc = moq_d16_encode_publish_ok(&rw, ok.request_id,
                                            ok.params, ok.params_count);
            break;
        }
        case MOQ_D16_PUBLISH_DONE: {
            moq_d16_publish_done_t done;
            moq_d16_decode_publish_done(env.payload, env.payload_len, &done);
            rc = moq_d16_encode_publish_done(&rw, done.request_id, done.status_code,
                                              done.stream_count,
                                              done.reason, done.reason_len);
            break;
        }
        case MOQ_D16_TRACK_STATUS: {
            moq_bytes_t parts[8];
            moq_kvp_entry_t dp[8];
            moq_d16_track_status_t ts = {
                .params = dp, .params_cap = 8,
            };
            moq_d16_decode_track_status(env.payload, env.payload_len,
                                         parts, 8, &ts);
            rc = moq_d16_encode_track_status(&rw, ts.request_id,
                &ts.track_namespace, ts.track_name, ts.params, ts.params_count);
            break;
        }
        default:
            break;
        }

        MOQ_TEST_CHECK(rc == MOQ_OK);
        size_t re_len = moq_buf_writer_offset(&rw);
        if (re_len != file_len || memcmp(reencode, file_buf, re_len) != 0) {
            fprintf(stderr, "FAIL: %s re-encode mismatch "
                    "(re_len=%zu, file_len=%zu)\n",
                    v->filename, re_len, file_len);
            failures++;
        }
    }
    return failures;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* FETCH data-stream vectors                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    const char  *filename;
    const char  *description;
    const char  *expected_json;
    uint8_t      wire[512];
    size_t       wire_len;
    bool         has_prior;
    moq_d16_fetch_object_t prior;
    /* Expected decoded fields for semantic validation. */
    bool         exp_is_range;
    uint32_t     exp_range_kind;
    uint64_t     exp_group_id;
    uint64_t     exp_subgroup_id;
    uint64_t     exp_object_id;
    uint8_t      exp_priority;
    uint64_t     exp_payload_len;
    const uint8_t *exp_payload;
    size_t       exp_extensions_len;
    const uint8_t *exp_extensions;
} fvec_t;

#define NUM_FVECS 6

static int generate_fvecs(fvec_t fv[NUM_FVECS])
{
    int idx = 0;

    /* 1. First fetch object: payload, no properties */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_obj_first.bin";
        v->description = "First fetch object: g=1 sg=0 o=0 pri=128 payload=\"hello\"";
        v->expected_json = "{ \"group_id\": 1, \"subgroup_id\": 0, \"object_id\": 0, "
                           "\"publisher_priority\": 128, \"payload_len\": 5, "
                           "\"payload_hex\": \"68656c6c6f\" }";
        v->has_prior = false;
        v->exp_group_id = 1; v->exp_object_id = 0; v->exp_priority = 128;
        v->exp_payload_len = 5; v->exp_payload = (const uint8_t *)"hello";
        moq_d16_fetch_object_t obj;
        memset(&obj, 0, sizeof(obj));
        obj.group_id = 1; obj.object_id = 0; obj.publisher_priority = 128;
        obj.payload = (const uint8_t *)"hello"; obj.payload_len = 5;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_object(&w, &obj, NULL) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* 2. Fetch object with properties + payload */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_obj_props_payload.bin";
        v->description = "Fetch object with properties + payload: g=2 sg=0 o=0 pri=128 "
                          "extensions=\"prop\" payload=\"data\"";
        v->expected_json = "{ \"group_id\": 2, \"subgroup_id\": 0, \"object_id\": 0, "
                           "\"publisher_priority\": 128, \"extensions_len\": 4, "
                           "\"extensions_hex\": \"70726f70\", \"payload_len\": 4, "
                           "\"payload_hex\": \"64617461\" }";
        v->has_prior = false;
        v->exp_group_id = 2; v->exp_object_id = 0; v->exp_priority = 128;
        v->exp_extensions_len = 4; v->exp_extensions = (const uint8_t *)"prop";
        v->exp_payload_len = 4; v->exp_payload = (const uint8_t *)"data";
        moq_d16_fetch_object_t obj;
        memset(&obj, 0, sizeof(obj));
        obj.group_id = 2; obj.object_id = 0; obj.publisher_priority = 128;
        obj.extensions = (const uint8_t *)"prop"; obj.extensions_len = 4;
        obj.payload = (const uint8_t *)"data"; obj.payload_len = 4;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_object(&w, &obj, NULL) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* 3. Properties-only fetch object (zero-length payload) */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_obj_props_only.bin";
        v->description = "Properties-only fetch object: g=3 sg=0 o=0 pri=128 "
                          "extensions=\"meta\" payload_len=0";
        v->expected_json = "{ \"group_id\": 3, \"subgroup_id\": 0, \"object_id\": 0, "
                           "\"publisher_priority\": 128, \"extensions_len\": 4, "
                           "\"extensions_hex\": \"6d657461\", \"payload_len\": 0 }";
        v->has_prior = false;
        v->exp_group_id = 3; v->exp_object_id = 0; v->exp_priority = 128;
        v->exp_extensions_len = 4; v->exp_extensions = (const uint8_t *)"meta";
        moq_d16_fetch_object_t obj;
        memset(&obj, 0, sizeof(obj));
        obj.group_id = 3; obj.object_id = 0; obj.publisher_priority = 128;
        obj.extensions = (const uint8_t *)"meta"; obj.extensions_len = 4;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_object(&w, &obj, NULL) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* 4. End-of-range NON_EXISTENT */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_eor_non_existent.bin";
        v->description = "End-of-range NON_EXISTENT: group=5 object=10";
        v->expected_json = "{ \"is_end_of_range\": true, \"range_kind\": \"0x8c\", "
                           "\"group_id\": 5, \"object_id\": 10 }";
        v->has_prior = false;
        v->exp_is_range = true; v->exp_range_kind = MOQ_D16_FETCH_END_NON_EXISTENT;
        v->exp_group_id = 5; v->exp_object_id = 10;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_end_of_range(&w, MOQ_D16_FETCH_END_NON_EXISTENT, 5, 10) != MOQ_OK)
            return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* 5. End-of-range UNKNOWN */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_eor_unknown.bin";
        v->description = "End-of-range UNKNOWN: group=100 object=200";
        v->expected_json = "{ \"is_end_of_range\": true, \"range_kind\": \"0x10c\", "
                           "\"group_id\": 100, \"object_id\": 200 }";
        v->has_prior = false;
        v->exp_is_range = true; v->exp_range_kind = MOQ_D16_FETCH_END_UNKNOWN;
        v->exp_group_id = 100; v->exp_object_id = 200;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_end_of_range(&w, MOQ_D16_FETCH_END_UNKNOWN, 100, 200) != MOQ_OK)
            return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    /* 6. Delta-encoded second object after a prior object */
    {
        fvec_t *v = &fv[idx++];
        v->filename = "fetch_obj_delta.bin";
        v->description = "Delta-encoded second object: g=1(same) sg=0(same) o=1(prev+1) "
                          "pri=128(same) payload=\"ab\"";
        v->expected_json = "{ \"group_id\": 1, \"subgroup_id\": 0, \"object_id\": 1, "
                           "\"publisher_priority\": 128, \"payload_len\": 2, "
                           "\"payload_hex\": \"6162\", "
                           "\"prior\": { \"group_id\": 1, \"subgroup_id\": 0, "
                           "\"object_id\": 0, \"publisher_priority\": 128 } }";
        v->has_prior = true;
        memset(&v->prior, 0, sizeof(v->prior));
        v->prior.group_id = 1; v->prior.subgroup_id = 0;
        v->prior.object_id = 0; v->prior.publisher_priority = 128;
        v->exp_group_id = 1; v->exp_subgroup_id = 0; v->exp_object_id = 1;
        v->exp_priority = 128;
        v->exp_payload_len = 2; v->exp_payload = (const uint8_t *)"ab";
        moq_d16_fetch_object_t obj;
        memset(&obj, 0, sizeof(obj));
        obj.group_id = 1; obj.subgroup_id = 0; obj.object_id = 1;
        obj.publisher_priority = 128;
        obj.payload = (const uint8_t *)"ab"; obj.payload_len = 2;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, v->wire, sizeof(v->wire));
        if (moq_d16_encode_fetch_object(&w, &obj, &v->prior) != MOQ_OK) return -1;
        v->wire_len = moq_buf_writer_offset(&w);
    }

    return 0;
}

static int write_fvecs(const char *dir, const fvec_t fv[NUM_FVECS])
{
    for (int i = 0; i < NUM_FVECS; i++)
        if (write_vector(dir, fv[i].filename, fv[i].wire, fv[i].wire_len) < 0)
            return -1;
    return 0;
}

static int write_fvec_manifest(FILE *f, const fvec_t fv[NUM_FVECS])
{
    for (int i = 0; i < NUM_FVECS; i++) {
        char hex[1025];
        hex_digest(fv[i].wire, fv[i].wire_len, hex, sizeof(hex));
        fprintf(f, "    {\n");
        fprintf(f, "      \"file\": \"%s\",\n", fv[i].filename);
        fprintf(f, "      \"type\": \"FETCH_DATA\",\n");
        fprintf(f, "      \"description\": ");
        write_json_str(f, fv[i].description);
        fprintf(f, ",\n");
        fprintf(f, "      \"wire_length\": %zu,\n", fv[i].wire_len);
        fprintf(f, "      \"wire_hex\": \"%s\"", hex);
        if (fv[i].expected_json)
            fprintf(f, ",\n      \"expected\": %s\n", fv[i].expected_json);
        else
            fprintf(f, "\n");
        fprintf(f, "    }%s\n", (i < NUM_FVECS - 1) ? "," : "");
    }
    return 0;
}

static int validate_fvecs(const char *dir, const fvec_t fv[NUM_FVECS])
{
    int failures = 0;
    for (int i = 0; i < NUM_FVECS; i++) {
        uint8_t file_buf[512];
        size_t  file_len = 0;
        if (read_vector(dir, fv[i].filename, file_buf, sizeof(file_buf), &file_len) < 0) {
            fprintf(stderr, "FAIL: cannot read %s/%s\n", dir, fv[i].filename);
            failures++; continue;
        }
        MOQ_TEST_CHECK_EQ_SIZE(file_len, fv[i].wire_len);
        if (file_len != fv[i].wire_len) continue;
        MOQ_TEST_CHECK(memcmp(file_buf, fv[i].wire, fv[i].wire_len) == 0);

        /* Decode and validate semantic fields. */
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, file_buf, file_len);
        const moq_d16_fetch_object_t *prev = fv[i].has_prior ? &fv[i].prior : NULL;
        moq_d16_fetch_object_t dec;
        MOQ_TEST_CHECK(moq_d16_decode_fetch_object(&r, prev, &dec) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

        MOQ_TEST_CHECK(dec.is_end_of_range == fv[i].exp_is_range);
        MOQ_TEST_CHECK_EQ_U64(dec.group_id, fv[i].exp_group_id);
        MOQ_TEST_CHECK_EQ_U64(dec.object_id, fv[i].exp_object_id);

        if (dec.is_end_of_range) {
            MOQ_TEST_CHECK_EQ_INT((int)dec.range_kind, (int)fv[i].exp_range_kind);
        } else {
            MOQ_TEST_CHECK_EQ_U64(dec.subgroup_id, fv[i].exp_subgroup_id);
            MOQ_TEST_CHECK_EQ_INT((int)dec.publisher_priority, (int)fv[i].exp_priority);
            MOQ_TEST_CHECK_EQ_U64(dec.payload_len, fv[i].exp_payload_len);
            if (fv[i].exp_payload_len > 0) {
                MOQ_TEST_CHECK(dec.payload != NULL);
                MOQ_TEST_CHECK(memcmp(dec.payload, fv[i].exp_payload,
                    (size_t)fv[i].exp_payload_len) == 0);
            }
            MOQ_TEST_CHECK_EQ_SIZE(dec.extensions_len, fv[i].exp_extensions_len);
            if (fv[i].exp_extensions_len > 0) {
                MOQ_TEST_CHECK(dec.extensions != NULL);
                MOQ_TEST_CHECK(memcmp(dec.extensions, fv[i].exp_extensions,
                    fv[i].exp_extensions_len) == 0);
            }
        }

        /* Re-encode and compare byte-identically */
        uint8_t reencode[512];
        moq_buf_writer_t rw;
        moq_buf_writer_init(&rw, reencode, sizeof(reencode));
        moq_result_t rc;
        if (dec.is_end_of_range) {
            rc = moq_d16_encode_fetch_end_of_range(&rw, dec.range_kind,
                dec.group_id, dec.object_id);
        } else {
            rc = moq_d16_encode_fetch_object(&rw, &dec, prev);
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
        size_t re_len = moq_buf_writer_offset(&rw);
        if (re_len != file_len || memcmp(reencode, file_buf, re_len) != 0) {
            fprintf(stderr, "FAIL: %s re-encode mismatch (re_len=%zu, file_len=%zu)\n",
                    fv[i].filename, re_len, file_len);
            failures++;
        }
    }
    return failures;
}

int main(int argc, char **argv)
{
    int failures = 0;
    bool generate = false;
    const char *vec_dir = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--generate") == 0) {
            generate = true;
        } else {
            vec_dir = argv[i];
        }
    }
    if (!vec_dir) vec_dir = "tests/vectors/d16";

    vector_t vecs[NUM_VECTORS];
    memset(vecs, 0, sizeof(vecs));
    fvec_t fvecs[NUM_FVECS];
    memset(fvecs, 0, sizeof(fvecs));

    if (generate_vectors(vecs) != 0) {
        fprintf(stderr, "FAIL: vector generation failed\n");
        return 1;
    }
    if (generate_fvecs(fvecs) != 0) {
        fprintf(stderr, "FAIL: fetch data vector generation failed\n");
        return 1;
    }

    if (generate) {
        struct stat st;
        if (stat(vec_dir, &st) != 0) {
            if (mkdir(vec_dir, 0755) != 0 && errno != EEXIST) {
                fprintf(stderr, "FAIL: cannot create %s: %s\n",
                        vec_dir, strerror(errno));
                return 1;
            }
        }

        if (write_all_vectors(vec_dir, vecs) != 0) {
            fprintf(stderr, "FAIL: writing vector files failed\n");
            return 1;
        }
        if (write_fvecs(vec_dir, fvecs) != 0) {
            fprintf(stderr, "FAIL: writing fetch data vector files failed\n");
            return 1;
        }

        /* Write combined manifest. */
        char path[512];
        snprintf(path, sizeof(path), "%s/manifest.json", vec_dir);
        FILE *mf = fopen(path, "w");
        if (!mf) {
            fprintf(stderr, "write_manifest: cannot open %s: %s\n",
                    path, strerror(errno));
            return 1;
        }
        fprintf(mf, "{\n  \"vectors\": [\n");
        for (int i = 0; i < NUM_VECTORS; i++) {
            char hex[513];
            hex_digest(vecs[i].wire, vecs[i].wire_len, hex, sizeof(hex));
            fprintf(mf, "    {\n");
            fprintf(mf, "      \"file\": \"%s\",\n", vecs[i].filename);
            fprintf(mf, "      \"type\": \"%s\",\n", vecs[i].type_name);
            if (vecs[i].is_datagram && vecs[i].wire_len > 0)
                fprintf(mf, "      \"type_code\": \"0x%02x\",\n", (unsigned)vecs[i].wire[0]);
            else
                fprintf(mf, "      \"type_code\": \"0x%02x\",\n", (unsigned)vecs[i].type_code);
            fprintf(mf, "      \"description\": ");
            write_json_str(mf, vecs[i].description);
            fprintf(mf, ",\n");
            fprintf(mf, "      \"wire_length\": %zu,\n", vecs[i].wire_len);
            fprintf(mf, "      \"wire_hex\": \"%s\"", hex);
            if (vecs[i].expected_json)
                fprintf(mf, ",\n      \"expected\": %s\n", vecs[i].expected_json);
            else
                fprintf(mf, "\n");
            fprintf(mf, "    },\n");
        }
        write_fvec_manifest(mf, fvecs);
        fprintf(mf, "  ]\n}\n");
        fclose(mf);
    }

    failures += validate_vectors(vec_dir, vecs);
    failures += validate_fvecs(vec_dir, fvecs);

    MOQ_TEST_PASS("test_vectors_d16");
    return failures ? 1 : 0;
}
