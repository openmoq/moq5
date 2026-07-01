#include <moq/sim.h>
#include "test_support.h"
#include "test_oom_support.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static moq_result_t reject_sub(moq_session_t *s, moq_subscription_t sub,
                                moq_request_error_t code, moq_bytes_t reason,
                                bool can_retry, uint64_t retry_ms, uint64_t now)
{
    moq_reject_subscribe_cfg_t cfg;
    moq_reject_subscribe_cfg_init(&cfg);
    cfg.error_code = code;
    cfg.reason = reason;
    cfg.can_retry = can_retry;
    cfg.retry_after_ms = retry_ms;
    return moq_session_reject_subscribe(s, sub, &cfg, now);
}

/* -- Trace determinism helper -------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   count;
} trace_summary_t;

/*
 * FNV-1a-style semantic trace hash. Hashes all meaningful trace fields
 * including actual wire bytes — not just lengths. No pointers or raw
 * handle bits. Deterministic across process runs for the same seed.
 */
static void trace_hash_fn(void *ctx,
                           const moq_sim_trace_record_t *r)
{
    trace_summary_t *s = (trace_summary_t *)ctx;
    uint64_t h = s->hash;

    /* Scheduling/identity fields. */
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;

    /* Record type. */
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;

    /* Actual wire byte content — catches protocol divergence that
     * preserves message lengths but changes request IDs, aliases,
     * error codes, namespaces, etc. */
    h ^= r->bytes.len; h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0) {
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i];
            h *= 0x100000001B3ULL;
        }
    }

    s->hash = h;
    s->count++;
}

typedef struct test_alloc_state {
    int64_t balance;
} test_alloc_state_t;

static void *test_alloc(size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) state->balance++;
    return p;
}

static void *test_realloc(void *ptr, size_t old_size, size_t new_size,
                          void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}

static void test_free(void *ptr, size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    (void)size;
    if (ptr) state->balance--;
    free(ptr);
}

static moq_alloc_t test_allocator(test_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, test_alloc, test_realloc, test_free };
    return alloc;
}

typedef struct trace_counts {
    size_t inputs;
    size_t actions;
    size_t quiescent;
    size_t send_control_actions;
    size_t control_inputs;
    uint64_t seed;
} trace_counts_t;

static void trace_cb(void *ctx, const moq_sim_trace_record_t *record)
{
    trace_counts_t *counts = (trace_counts_t *)ctx;
    if (!record || record->struct_size < sizeof(*record))
        return;

    counts->seed = record->seed;
    switch (record->kind) {
    case MOQ_SIM_TRACE_INPUT:
        counts->inputs++;
        if (record->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES) {
            counts->control_inputs++;
            if (record->bytes.len == 0 || !record->bytes.data)
                counts->seed = 0;
        }
        break;
    case MOQ_SIM_TRACE_ACTION:
        counts->actions++;
        if (record->action_kind == MOQ_ACTION_SEND_CONTROL) {
            counts->send_control_actions++;
            if (record->bytes.len == 0 || !record->bytes.data)
                counts->seed = 0;
        }
        break;
    case MOQ_SIM_TRACE_QUIESCENT:
        counts->quiescent++;
        break;
    default:
        counts->seed = 0;
        break;
    }
}

/* -- Split-specific trace collector -------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   data_bytes_count;
    uint64_t data_bytes_len_hash;
} split_trace_t;

static void split_trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    split_trace_t *s = (split_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->total_count++;

    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_BYTES) {
        s->data_bytes_count++;
        s->data_bytes_len_hash ^= r->bytes.len;
        s->data_bytes_len_hash *= 0x100000001B3ULL;
    }
}

/* -- Delay trace collector ----------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   delay_enqueue_count;
    size_t   delay_stale_count;
    size_t   delay_fifo_forced_count;
    size_t   input_data_reset_count;
    size_t   input_data_stop_count;
    size_t   reset_action_count;
    size_t   inject_count;
} delay_trace_t;

static void delay_trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    delay_trace_t *s = (delay_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->count;                 h *= 0x100000001B3ULL;
    s->hash = h;
    s->total_count++;
    if (r->kind == MOQ_SIM_TRACE_DELAY_ENQUEUE) {
        s->delay_enqueue_count++;
        if (r->result == 1) s->delay_fifo_forced_count++;
    }
    if (r->kind == MOQ_SIM_TRACE_DELAY_STALE)
        s->delay_stale_count++;
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_RESET)
        s->input_data_reset_count++;
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_STOP)
        s->input_data_stop_count++;
    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        r->action_kind == MOQ_ACTION_RESET_DATA)
        s->reset_action_count++;
    if (r->kind == MOQ_SIM_TRACE_FAULT_INJECT)
        s->inject_count++;
}

/* -- Mutation trace collector -------------------------------------- */

#define MUT_MAX_BYTES 256

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   drop_control;
    size_t   drop_data;
    size_t   mutate_control;
    size_t   mutate_data;
    size_t   input_control;
    size_t   input_data;

    /* First FAULT_MUTATE record (control or data). */
    bool     got_mutate;
    moq_action_kind_t mut_action_kind;
    size_t   mut_byte_idx;
    unsigned mut_bit_idx;
    uint8_t  mut_bytes[MUT_MAX_BYTES];
    size_t   mut_len;

    /* ACTION record preceding the first mutate. For SEND_CONTROL,
     * orig_bytes = full control message. For SEND_DATA, orig_bytes =
     * header only, orig_payload_count = payload length from r->count.
     * Tests reconstruct full original from header + known payload. */
    bool     got_pre_action;
    uint8_t  orig_bytes[MUT_MAX_BYTES];
    size_t   orig_len;
    size_t   orig_payload_count;

    /* INPUT records after the first mutate. For control: one record.
     * For data: may be multiple chunks if split is active.
     * post_input_bytes concatenates all chunks. */
    bool     got_post_input;
    uint8_t  post_input_bytes[MUT_MAX_BYTES];
    size_t   post_input_len;
    size_t   post_input_chunks;

    bool     expect_mutate_next;
    bool     collecting_data_input;
} mutate_trace_t;

static void mutate_trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    mutate_trace_t *s = (mutate_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->total_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_DROP) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL) s->drop_control++;
        if (r->action_kind == MOQ_ACTION_SEND_DATA) s->drop_data++;
    }

    if (r->kind == MOQ_SIM_TRACE_INPUT) {
        if (r->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES) s->input_control++;
        if (r->input_kind == MOQ_SIM_INPUT_DATA_BYTES) s->input_data++;
    }

    /* Collect DATA_BYTES chunks after a data mutation. */
    if (s->collecting_data_input &&
        r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_BYTES) {
        if (r->bytes.data && r->bytes.len > 0 &&
            s->post_input_len + r->bytes.len <= MUT_MAX_BYTES) {
            memcpy(s->post_input_bytes + s->post_input_len,
                   r->bytes.data, r->bytes.len);
            s->post_input_len += r->bytes.len;
            s->post_input_chunks++;
        }
        if (s->post_input_len >= s->mut_len) {
            s->got_post_input = true;
            s->collecting_data_input = false;
        }
        return;
    }

    /* Capture latest ACTION for SEND_CONTROL or SEND_DATA, but only
     * before the first mutation is captured (after that, lock it). */
    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        !s->got_mutate &&
        (r->action_kind == MOQ_ACTION_SEND_CONTROL ||
         r->action_kind == MOQ_ACTION_SEND_DATA)) {
        s->expect_mutate_next = true;
        if (r->bytes.data && r->bytes.len <= MUT_MAX_BYTES) {
            memcpy(s->orig_bytes, r->bytes.data, r->bytes.len);
            s->orig_len = r->bytes.len;
        } else {
            s->orig_len = 0;
        }
        s->orig_payload_count = r->count;
        s->got_pre_action = true;
    } else if (r->kind == MOQ_SIM_TRACE_FAULT_MUTATE) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->mutate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->mutate_data++;
        if (!s->got_mutate && r->bytes.data &&
            r->bytes.len <= MUT_MAX_BYTES) {
            s->got_mutate = true;
            s->mut_action_kind = r->action_kind;
            s->mut_byte_idx = (size_t)r->code;
            s->mut_bit_idx = (unsigned)r->count;
            memcpy(s->mut_bytes, r->bytes.data, r->bytes.len);
            s->mut_len = r->bytes.len;
            if (r->action_kind == MOQ_ACTION_SEND_DATA)
                s->collecting_data_input = true;
        }
        s->expect_mutate_next = false;
    } else if (r->kind == MOQ_SIM_TRACE_INPUT &&
               r->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES &&
               s->got_mutate && !s->got_post_input &&
               s->mut_action_kind == MOQ_ACTION_SEND_CONTROL) {
        if (r->bytes.data && r->bytes.len <= MUT_MAX_BYTES) {
            s->got_post_input = true;
            memcpy(s->post_input_bytes, r->bytes.data, r->bytes.len);
            s->post_input_len = r->bytes.len;
            s->post_input_chunks = 1;
        }
    } else {
        s->expect_mutate_next = false;
    }
}

/* -- Reorder trace collector --------------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   reorder_count;

    /* First FAULT_REORDER record. */
    bool     got_first_reorder;
    moq_action_kind_t first_displaced_kind;
    size_t   first_code;
    size_t   first_count_val;

    /* First two ACTION records after the first FAULT_REORDER. */
    bool     capture_post_actions;
    size_t   post_action_idx;
    moq_action_kind_t post_action_kinds[2];
} reorder_trace_t;

static void reorder_trace_fn(void *ctx, const moq_sim_trace_record_t *r) {
    reorder_trace_t *s = (reorder_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->total_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_REORDER) {
        s->reorder_count++;
        if (!s->got_first_reorder) {
            s->got_first_reorder = true;
            s->first_displaced_kind = r->action_kind;
            s->first_code = (size_t)r->code;
            s->first_count_val = (size_t)r->count;
            s->capture_post_actions = true;
            s->post_action_idx = 0;
        }
    } else if (s->capture_post_actions &&
               r->kind == MOQ_SIM_TRACE_ACTION) {
        if (s->post_action_idx < 2)
            s->post_action_kinds[s->post_action_idx++] = r->action_kind;
        if (s->post_action_idx >= 2)
            s->capture_post_actions = false;
    }
}

/* -- Inject-specific trace collector -------------------------------- */

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   inject_reset;
    size_t   inject_stop;
    size_t   inject_close;
    moq_result_t last_inject_result;
    moq_perspective_t last_inject_from;
    moq_perspective_t last_inject_to;
    size_t   last_inject_slot;
    size_t   reset_action_count;
    size_t   reset_input_count;
    moq_result_t last_reset_input_result;
    size_t   close_action_count;
    size_t   closed_event_count;
} inject_trace_t;

static void inject_trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    inject_trace_t *s = (inject_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->total_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_INJECT) {
        if (r->action_kind == MOQ_ACTION_RESET_DATA)
            s->inject_reset++;
        else if (r->action_kind == MOQ_ACTION_STOP_DATA)
            s->inject_stop++;
        else if (r->action_kind == MOQ_ACTION_CLOSE_SESSION)
            s->inject_close++;
        s->last_inject_result = r->result;
        s->last_inject_from = r->from;
        s->last_inject_to = r->to;
        s->last_inject_slot = r->count;
    }
    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        r->action_kind == MOQ_ACTION_RESET_DATA)
        s->reset_action_count++;
    if (r->kind == MOQ_SIM_TRACE_ACTION &&
        r->action_kind == MOQ_ACTION_CLOSE_SESSION)
        s->close_action_count++;
    if (r->kind == MOQ_SIM_TRACE_INPUT &&
        r->input_kind == MOQ_SIM_INPUT_DATA_RESET) {
        s->reset_input_count++;
        s->last_reset_input_result = r->result;
    }
}

/* -- Truncation-specific trace collector ------------------------------ */

typedef struct {
    uint64_t hash;
    size_t   total_count;
    size_t   truncate_control;
    size_t   truncate_data;
    size_t   input_control;
    size_t   input_data;
    size_t   input_data_reset;

    /* First FAULT_TRUNCATE record. */
    bool     got_first;
    moq_action_kind_t first_action_kind;
    size_t   first_prefix_len;    /* bytes.len */
    size_t   first_original_len;  /* count */
} truncate_trace_t;

static void truncate_trace_fn(void *ctx, const moq_sim_trace_record_t *r)
{
    truncate_trace_t *s = (truncate_trace_t *)ctx;
    uint64_t h = s->hash;
    h ^= r->seed;   h *= 0x100000001B3ULL;
    h ^= r->step;   h *= 0x100000001B3ULL;
    h ^= r->now_us; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->kind;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->from;        h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->to;          h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->action_kind; h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->input_kind;  h *= 0x100000001B3ULL;
    h ^= (uint64_t)r->result;      h *= 0x100000001B3ULL;
    h ^= r->code;                  h *= 0x100000001B3ULL;
    h ^= r->bytes.len;             h *= 0x100000001B3ULL;
    if (r->bytes.data && r->bytes.len > 0)
        for (size_t i = 0; i < r->bytes.len; i++) {
            h ^= r->bytes.data[i]; h *= 0x100000001B3ULL;
        }
    s->hash = h;
    s->total_count++;

    if (r->kind == MOQ_SIM_TRACE_FAULT_TRUNCATE) {
        if (r->action_kind == MOQ_ACTION_SEND_CONTROL)
            s->truncate_control++;
        else if (r->action_kind == MOQ_ACTION_SEND_DATA)
            s->truncate_data++;
        if (!s->got_first) {
            s->got_first = true;
            s->first_action_kind = r->action_kind;
            s->first_prefix_len = (size_t)r->code;
            s->first_original_len = r->count;
        }
    }
    if (r->kind == MOQ_SIM_TRACE_INPUT) {
        if (r->input_kind == MOQ_SIM_INPUT_CONTROL_BYTES)
            s->input_control++;
        if (r->input_kind == MOQ_SIM_INPUT_DATA_BYTES)
            s->input_data++;
        if (r->input_kind == MOQ_SIM_INPUT_DATA_RESET)
            s->input_data_reset++;
    }
}

int main(void)
{
    int failures = 0;

    /* == Setup handshake through SimPair =========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        trace_counts_t trace = {0};

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 1234;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 100;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 50;
        cfg.trace_fn = trace_cb;
        cfg.trace_ctx = &trace;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_seed(sp) == 1234);
        MOQ_TEST_CHECK(moq_simpair_now_us(sp) == 1000);
        MOQ_TEST_CHECK(moq_simpair_client(sp) != NULL);
        MOQ_TEST_CHECK(moq_simpair_server(sp) != NULL);

        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);

        size_t steps = 0;
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 4, &steps) == MOQ_OK);
        MOQ_TEST_CHECK(steps == 2);

        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK(moq_session_state(client) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(client, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_CLIENT);

        MOQ_TEST_CHECK(moq_session_poll_events(server, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_SERVER);

        MOQ_TEST_CHECK(trace.seed == 1234);
        MOQ_TEST_CHECK(trace.inputs == 3);
        MOQ_TEST_CHECK(trace.actions == 2);
        MOQ_TEST_CHECK(trace.control_inputs == 2);
        MOQ_TEST_CHECK(trace.send_control_actions == 2);
        MOQ_TEST_CHECK(trace.quiescent == 1);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Step and max-step behavior ================================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);

        size_t steps = 99;
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 0, &steps) ==
                       MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(steps == 0);

        size_t delivered = 0;
        MOQ_TEST_CHECK(moq_simpair_step(sp, &delivered) == MOQ_OK);
        MOQ_TEST_CHECK(delivered == 2);
        MOQ_TEST_CHECK(moq_simpair_step(sp, &delivered) == MOQ_OK);
        MOQ_TEST_CHECK(delivered == 0);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Time advancement ========================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.initial_now_us = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_advance_to(sp, 20) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_now_us(sp) == 20);
        MOQ_TEST_CHECK(moq_simpair_advance_to(sp, 19) == MOQ_ERR_INVAL);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == NULL and invalid argument guards ========================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_simpair_t *sp = (moq_simpair_t *)1;

        MOQ_TEST_CHECK(moq_simpair_create(NULL, &sp) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(sp == NULL);
        MOQ_TEST_CHECK(moq_simpair_create(NULL, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_simpair_client(NULL) == NULL);
        MOQ_TEST_CHECK(moq_simpair_server(NULL) == NULL);
        MOQ_TEST_CHECK(moq_simpair_now_us(NULL) == 0);
        MOQ_TEST_CHECK(moq_simpair_seed(NULL) == 0);
        MOQ_TEST_CHECK(moq_simpair_start(NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_simpair_step(NULL, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(NULL, 1, NULL) ==
                       MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_simpair_advance_to(NULL, 1) == MOQ_ERR_INVAL);
        moq_simpair_destroy(NULL);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.struct_size = (uint32_t)offsetof(moq_simpair_cfg_t, alloc);
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(sp == NULL);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair SUBSCRIBE happy path ================================= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xBEEF;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Drain setup events. */
        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("audio");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
        MOQ_TEST_CHECK(moq_subscription_is_valid(sub));

        /* Pump SUBSCRIBE to server. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server accepts. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        accept.has_largest = true;
        accept.largest_group = 10;
        accept.largest_object = 99;

        MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
            ev.u.subscribe_request.sub, &accept,
            moq_simpair_now_us(sp)) == MOQ_OK);

        /* Pump SUBSCRIBE_OK to client. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client sees SUBSCRIBE_OK. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.has_largest == true);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.largest_group == 10);
        MOQ_TEST_CHECK(ev.u.subscribe_ok.largest_object == 99);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair SUBSCRIBE multi-seed determinism ===================== */
    {
        static const uint64_t seeds[] = { 0, 1, 0xBEEF, 0xDEADBEEF };
        for (size_t si = 0; si < sizeof(seeds)/sizeof(seeds[0]); si++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[si];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            /* Subscribe. */
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
            sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);

            moq_simpair_run_until_quiescent(sp, 8, NULL);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
                &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            accept.has_largest = true;
            accept.largest_group = seeds[si] & 0xFF;
            accept.largest_object = (seeds[si] >> 8) & 0xFF;

            MOQ_TEST_CHECK(moq_session_accept_subscribe(
                moq_simpair_server(sp), ev.u.subscribe_request.sub,
                &accept, moq_simpair_now_us(sp)) == MOQ_OK);

            moq_simpair_run_until_quiescent(sp, 8, NULL);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
                &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
            MOQ_TEST_CHECK(ev.u.subscribe_ok.largest_group ==
                            (seeds[si] & 0xFF));

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
    }

    /* == SimPair SUBSCRIBE rejection multi-seed ======================= */
    {
        static const uint64_t seeds[] = { 0, 1, 0xBEEF, 0xDEADBEEF };
        for (size_t si = 0; si < sizeof(seeds)/sizeof(seeds[0]); si++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[si];
            cfg.initial_now_us = 500;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("audio");

            moq_subscription_t sub;
            moq_session_subscribe(moq_simpair_client(sp), &sub_cfg,
                moq_simpair_now_us(sp), &sub);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

            reject_sub(moq_simpair_server(sp),
                ev.u.subscribe_request.sub,
                MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
                MOQ_BYTES_LITERAL("gone"), true, 100,
                moq_simpair_now_us(sp));

            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);
            MOQ_TEST_CHECK(ev.u.subscribe_error.can_retry == true);
            MOQ_TEST_CHECK(ev.u.subscribe_error.retry_after_ms == 100);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
    }

    /* == SimPair trace determinism: happy path ======================== */
    {
        trace_summary_t summaries[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            summaries[run] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xCAFE;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &summaries[run];

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
            sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
                &ev, 1) == 1);

            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            accept.has_largest = true;
            accept.largest_group = 10;
            accept.largest_object = 99;
            MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
                ev.u.subscribe_request.sub, &accept,
                moq_simpair_now_us(sp)) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
                &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        MOQ_TEST_CHECK(summaries[0].hash == summaries[1].hash);
        MOQ_TEST_CHECK(summaries[0].count == summaries[1].count);
        MOQ_TEST_CHECK(summaries[0].count > 0);
    }

    /* == SimPair trace determinism: rejection path ==================== */
    {
        trace_summary_t summaries[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            summaries[run] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xFACE;
            cfg.initial_now_us = 500;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &summaries[run];

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("audio");

            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
                &ev, 1) == 1);

            MOQ_TEST_CHECK(reject_sub(moq_simpair_server(sp),
                ev.u.subscribe_request.sub,
                MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
                MOQ_BYTES_LITERAL("gone"), true, 100,
                moq_simpair_now_us(sp)) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
                &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        MOQ_TEST_CHECK(summaries[0].hash == summaries[1].hash);
        MOQ_TEST_CHECK(summaries[0].count == summaries[1].count);
        MOQ_TEST_CHECK(summaries[0].count > 0);
    }

    /* == Content-different scenarios produce different hashes ========== */
    /*
     * Two subscribe scenarios with same-length but different track names.
     * The shape (message count, byte lengths) is identical, but wire
     * content differs. The byte-content hash must distinguish them.
     */
    {
        const char *names[] = { "aaaa", "bbbb" };
        trace_summary_t sums[2];

        for (int i = 0; i < 2; i++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            sums[i] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xABCD;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &sums[i];

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = moq_bytes_cstr(names[i]);

            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            MOQ_TEST_CHECK(moq_session_accept_subscribe(
                moq_simpair_server(sp), ev.u.subscribe_request.sub,
                &accept, moq_simpair_now_us(sp)) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);

            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        /* Same seed, same lengths, different content → different hash. */
        MOQ_TEST_CHECK(sums[0].count == sums[1].count);
        MOQ_TEST_CHECK(sums[0].hash != sums[1].hash);
    }

    /* == Accept vs reject same seed → different hash ================== */
    {
        trace_summary_t sums[2];

        for (int scenario = 0; scenario < 2; scenario++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            sums[scenario] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0x1234;
            cfg.initial_now_us = 500;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &sums[scenario];

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);
            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");

            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
                &ev, 1) == 1);

            if (scenario == 0) {
                moq_accept_subscribe_cfg_t accept;
                moq_accept_subscribe_cfg_init(&accept);
                MOQ_TEST_CHECK(moq_session_accept_subscribe(
                    moq_simpair_server(sp), ev.u.subscribe_request.sub,
                    &accept, moq_simpair_now_us(sp)) == MOQ_OK);
            } else {
                MOQ_TEST_CHECK(reject_sub(moq_simpair_server(sp),
                    ev.u.subscribe_request.sub,
                    MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
                    MOQ_BYTES_LITERAL("no"), false, 0,
                    moq_simpair_now_us(sp)) == MOQ_OK);
            }
            MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(sp, 8, NULL) == MOQ_OK);
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        /* Accept vs reject with same seed → different hash. */
        MOQ_TEST_CHECK(sums[0].hash != sums[1].hash);
    }

    /* == SimPair object delivery: pub writes 3, sub receives 3 ========= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDADA;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        /* Client subscribes. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);

        moq_subscription_t server_sub = ev.u.subscribe_request.sub;

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
            server_sub, &accept,
            moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);

        /* Server publishes 3 objects. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
            server_sub, &sg_cfg,
            moq_simpair_now_us(sp), &sg) == MOQ_OK);

        const char *frames[] = { "frame-0", "frame-1", "frame-2" };
        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *payload = NULL;
            MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
                (const uint8_t *)frames[i], strlen(frames[i]),
                &payload) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
                sg, (uint64_t)i, payload,
                moq_simpair_now_us(sp)) == MOQ_OK);
            moq_rcbuf_decref(payload);
        }
        MOQ_TEST_CHECK(moq_session_close_subgroup(moq_simpair_server(sp),
            sg, moq_simpair_now_us(sp)) == MOQ_OK);

        /* Pump data through SimPair. */
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Client should have received 3 OBJECT_RECEIVED events. */
        for (int i = 0; i < 3; i++) {
            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
                &ev, 1) == 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
            MOQ_TEST_CHECK(ev.u.object_received.group_id == 0);
            MOQ_TEST_CHECK(ev.u.object_received.subgroup_id == 0);
            MOQ_TEST_CHECK(ev.u.object_received.object_id == (uint64_t)i);
            MOQ_TEST_CHECK(ev.u.object_received.status == MOQ_OBJECT_NORMAL);
            MOQ_TEST_CHECK(ev.u.object_received.payload != NULL);
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) ==
                            strlen(frames[i]));
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                                   frames[i], strlen(frames[i])) == 0);
            moq_event_cleanup(&ev);
        }

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair: large payload (>256 bytes) exact delivery ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xB160;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");

        moq_subscription_t sub;
        moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        moq_subscription_t ssub = ev.u.subscribe_request.sub;

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        moq_session_accept_subscribe(moq_simpair_server(sp),
            ssub, &accept, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

        /* Write a 512-byte payload. */
        uint8_t big[512];
        for (int j = 0; j < 512; j++) big[j] = (uint8_t)(j & 0xFF);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp),
            ssub, &sg_cfg, moq_simpair_now_us(sp), &sg);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, big, 512, &p);
        moq_session_write_object(moq_simpair_server(sp), sg, 0, p,
            moq_simpair_now_us(sp));
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(moq_simpair_server(sp), sg,
            moq_simpair_now_us(sp));

        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.object_received.payload) == 512);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.object_received.payload),
                               big, 512) == 0);
        moq_event_cleanup(&ev);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == Same-size different payload → different trace hash ============= */
    {
        const uint8_t payloads[2][8] = {
            { 'A','A','A','A','A','A','A','A' },
            { 'B','B','B','B','B','B','B','B' },
        };
        trace_summary_t sums[2];

        for (int p = 0; p < 2; p++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);
            sums[p] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xD1FF;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &sums[p];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub;
            moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            moq_subscription_t ssub = ev.u.subscribe_request.sub;

            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                ssub, &accept, moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp),
                ssub, &sg_cfg, moq_simpair_now_us(sp), &sg);
            moq_rcbuf_t *buf = NULL;
            moq_rcbuf_create(&alloc, payloads[p], 8, &buf);
            moq_session_write_object(moq_simpair_server(sp), sg, 0, buf,
                moq_simpair_now_us(sp));
            moq_rcbuf_decref(buf);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_run_until_quiescent(sp, 16, NULL);
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_event_cleanup(&ev);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        MOQ_TEST_CHECK(sums[0].count == sums[1].count);
        MOQ_TEST_CHECK(sums[0].hash != sums[1].hash);
    }

    /* == SimPair data trace determinism ================================= */
    {
        trace_summary_t summaries[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);

            summaries[run] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xF00D;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &summaries[run];

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
            sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

            moq_subscription_t sub;
            moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            moq_subscription_t ssub = ev.u.subscribe_request.sub;

            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                ssub, &accept,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp),
                ssub, &sg_cfg,
                moq_simpair_now_us(sp), &sg);

            moq_rcbuf_t *p = NULL;
            moq_rcbuf_create(&alloc, (const uint8_t *)"test", 4, &p);
            moq_session_write_object(moq_simpair_server(sp), sg, 0, p,
                moq_simpair_now_us(sp));
            moq_rcbuf_decref(p);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_run_until_quiescent(sp, 16, NULL);

            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
            moq_event_cleanup(&ev);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }

        MOQ_TEST_CHECK(summaries[0].hash == summaries[1].hash);
        MOQ_TEST_CHECK(summaries[0].count == summaries[1].count);
        MOQ_TEST_CHECK(summaries[0].count > 0);
    }

    /* == SimPair: STOP_DATA routes to sender's on_data_stop ============= */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0x5D0F;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        moq_session_accept_subscribe(moq_simpair_server(sp),
            server_sub, &accept, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;

        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
            server_sub, &sg_cfg, moq_simpair_now_us(sp), &sg) == MOQ_OK);

        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"data", 4, &p);
        moq_session_write_object(moq_simpair_server(sp), sg, 0, p,
            moq_simpair_now_us(sp));
        moq_rcbuf_decref(p);

        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Client sends STOP_DATA on the data stream (via on_data_reset
         * which will be mapped to STOP by SimPair internally — but for
         * this test we need to trigger the client to emit STOP_DATA).
         * The receiver does not have an API to stop sending; the protocol
         * STOP_DATA is emitted when rx budget exceeded or unknown alias.
         * We can test by feeding another subgroup header with bogus alias
         * on the same stream_ref the client received on. But the client
         * doesn't know the sender stream_ref. Instead, test on_data_stop
         * directly on the server session: */
        moq_session_t *server = moq_simpair_server(sp);
        moq_rcbuf_t *p2 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"more", 4, &p2);
        moq_result_t wr = moq_session_write_object(server, sg, 1, p2,
            moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(wr == MOQ_OK);
        moq_rcbuf_decref(p2);

        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_event_cleanup(&ev);

        moq_stream_ref_t server_ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK(moq_session_on_data_stop(server, server_ref, 0,
            moq_simpair_now_us(sp)) == MOQ_OK);

        /* Now the subgroup handle should be stale. */
        moq_rcbuf_t *p3 = NULL;
        moq_rcbuf_create(&alloc, (const uint8_t *)"X", 1, &p3);
        MOQ_TEST_CHECK(moq_session_write_object(server, sg, 2, p3,
            moq_simpair_now_us(sp)) == MOQ_ERR_STALE_HANDLE);
        moq_rcbuf_decref(p3);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair PUBLISH_NAMESPACE happy path ============================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xAD01;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        /* Server publishes namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(moq_simpair_server(sp),
            &pn_cfg, moq_simpair_now_us(sp), &ann) == MOQ_OK);

        /* Pump PUBLISH_NAMESPACE to client. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client sees NAMESPACE_PUBLISHED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.track_namespace.count == 1);

        /* Client accepts. */
        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_namespace(moq_simpair_client(sp),
            ev.u.namespace_published.ann, &acc,
            moq_simpair_now_us(sp)) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server sees NAMESPACE_ACCEPTED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);
        MOQ_TEST_CHECK(moq_announcement_eq(ev.u.namespace_accepted.ann, ann));

        /* Server withdraws with done. */
        MOQ_TEST_CHECK(moq_session_publish_namespace_done(
            moq_simpair_server(sp), ann,
            moq_simpair_now_us(sp)) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client sees NAMESPACE_DONE. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_DONE);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair namespace rejection ====================================== */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xAD02;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        /* Server publishes namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(moq_simpair_server(sp),
            &pn_cfg, moq_simpair_now_us(sp), &ann) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client sees NAMESPACE_PUBLISHED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        /* Client rejects. */
        moq_reject_namespace_cfg_t rej;
        moq_reject_namespace_cfg_init(&rej);
        rej.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rej.reason = MOQ_BYTES_LITERAL("no thanks");
        MOQ_TEST_CHECK(moq_session_reject_namespace(moq_simpair_client(sp),
            ev.u.namespace_published.ann, &rej,
            moq_simpair_now_us(sp)) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server sees NAMESPACE_REJECTED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_REJECTED);
        MOQ_TEST_CHECK(ev.u.namespace_rejected.error_code ==
                        MOQ_REQUEST_ERROR_NOT_SUPPORTED);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair namespace cancel after accept ============================ */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xAD03;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
        moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

        /* Server publishes namespace. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        moq_publish_namespace_cfg_t pn_cfg;
        moq_publish_namespace_cfg_init(&pn_cfg);
        pn_cfg.track_namespace = ns;

        moq_announcement_t ann;
        MOQ_TEST_CHECK(moq_session_publish_namespace(moq_simpair_server(sp),
            &pn_cfg, moq_simpair_now_us(sp), &ann) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Client accepts. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);

        moq_accept_namespace_cfg_t acc;
        moq_accept_namespace_cfg_init(&acc);
        moq_announcement_t client_ann = ev.u.namespace_published.ann;
        MOQ_TEST_CHECK(moq_session_accept_namespace(moq_simpair_client(sp),
            client_ann, &acc, moq_simpair_now_us(sp)) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server sees NAMESPACE_ACCEPTED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_ACCEPTED);

        /* Client cancels. */
        moq_cancel_namespace_cfg_t can;
        moq_cancel_namespace_cfg_init(&can);
        can.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
        can.reason = MOQ_BYTES_LITERAL("done with it");
        MOQ_TEST_CHECK(moq_session_cancel_namespace(moq_simpair_client(sp),
            client_ann, &can, moq_simpair_now_us(sp)) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server sees NAMESPACE_CANCELLED. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_CANCELLED);
        MOQ_TEST_CHECK(ev.u.namespace_cancelled.error_code ==
                        MOQ_REQUEST_ERROR_INTERNAL_ERROR);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* == SimPair auth-bearing trace determinism ========================= */
    {
        trace_summary_t summaries[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);
            summaries[run] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xA0A0;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &summaries[run];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("video");

            moq_subscription_t sub;
            moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                ev.u.subscribe_request.sub, &accept,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
        MOQ_TEST_CHECK(summaries[0].hash == summaries[1].hash);
        MOQ_TEST_CHECK(summaries[0].count == summaries[1].count);
        MOQ_TEST_CHECK(summaries[0].count > 0);
    }

    /* == Same-seed different track name → different trace hash ========= */
    {
        const char *names[] = { "tokenA", "tokenB" };
        trace_summary_t sums[2];
        for (int v = 0; v < 2; v++) {
            test_alloc_state_t alloc_state = {0};
            moq_alloc_t alloc = test_allocator(&alloc_state);
            sums[v] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xB0B0;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &sums[v];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = moq_bytes_cstr(names[v]);

            moq_subscription_t sub;
            moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                ev.u.subscribe_request.sub, &accept,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_session_poll_events(moq_simpair_client(sp), &ev, 1);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(alloc_state.balance == 0);
        }
        MOQ_TEST_CHECK(sums[0].count == sums[1].count);
        MOQ_TEST_CHECK(sums[0].hash != sums[1].hash);
    }

    /* == SPLIT_DATA determinism ====================================== */
    /* Same seed with SPLIT_DATA produces same trace hash. */
    {
        trace_summary_t sums[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            sums[run] = (trace_summary_t){ 0xCBF29CE484222325ULL, 0 };

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0x5917;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &sums[run];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_simpair_enable_faults(sp);

            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            /* Subscribe + accept + write object with 16-byte payload. */
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            }

            uint8_t payload_data[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, payload_data, 16, &payload);

            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = 0;
            sgcfg.subgroup_id = 0;
            sgcfg.publisher_priority = 128;
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_run_until_quiescent(sp, 8, NULL);

            /* Verify object received correctly. */
            { moq_event_t evts[8]; size_t ne;
              ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8);
              bool got_obj = false;
              for (size_t j = 0; j < ne; j++) {
                  if (evts[j].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                      got_obj = true;
                      MOQ_TEST_CHECK(moq_rcbuf_len(evts[j].u.object_received.payload) == 16);
                      MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(evts[j].u.object_received.payload),
                                            payload_data, 16) == 0);
                  }
                  moq_event_cleanup(&evts[j]);
              }
              MOQ_TEST_CHECK(got_obj);
            }

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK(sums[0].hash == sums[1].hash);
        MOQ_TEST_CHECK(sums[0].count == sums[1].count);
        MOQ_TEST_CHECK(sums[0].count > 0);
    }

    /* == SPLIT_DATA produces more DATA_BYTES records than normal ====== */
    {
        split_trace_t strace[2];
        for (int mode = 0; mode < 2; mode++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&strace[mode], 0, sizeof(strace[mode]));
            strace[mode].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xC0FFEE;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = split_trace_fn;
            cfg.trace_ctx = &strace[mode];
            if (mode == 1) {
                cfg.fault_per_mille = 1000;
                cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_DATA;
            }

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (mode == 1) moq_simpair_enable_faults(sp);

            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            }

            uint8_t pdata[16] = {0xDE,0xAD,0xBE,0xEF,1,2,3,4,5,6,7,8,9,10,11,12};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 16, &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* Split mode must produce more DATA_BYTES records than normal. */
        MOQ_TEST_CHECK(strace[1].data_bytes_count > strace[0].data_bytes_count);
        /* DATA_BYTES length sequences differ (split vs two-call). */
        MOQ_TEST_CHECK(strace[1].data_bytes_len_hash != strace[0].data_bytes_len_hash);
    }

    /* == Different seed produces different split pattern ============== */
    {
        split_trace_t strace[2];
        uint64_t seeds[2] = { 0xAAAA, 0xBBBB };
        for (int s = 0; s < 2; s++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&strace[s], 0, sizeof(strace[s]));
            strace[s].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[s];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = split_trace_fn;
            cfg.trace_ctx = &strace[s];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_simpair_enable_faults(sp);

            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_subscription_t srv_sub = {0};
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
            }

            uint8_t pdata[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 16, &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* Different seeds produce different DATA_BYTES chunk patterns. */
        MOQ_TEST_CHECK(strace[0].data_bytes_len_hash != strace[1].data_bytes_len_hash);
        /* Overall trace hash differs too. */
        MOQ_TEST_CHECK(strace[0].hash != strace[1].hash);
    }

    /* == MUTATE_CONTROL trace oracle ================================== */
    /* Prove: FAULT_MUTATE emitted, original vs mutated differ by exactly
     * one bit, mutate record code/count identify the flip, input trace
     * matches mutated bytes. Deterministic across two runs. */
    {
        mutate_trace_t mt[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&mt[run], 0, sizeof(mt[run]));
            mt[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xA078;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = mutate_trace_fn;
            cfg.trace_ctx = &mt[run];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_MUTATE_CONTROL;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* Determinism. */
        MOQ_TEST_CHECK(mt[0].hash == mt[1].hash);
        MOQ_TEST_CHECK(mt[0].total_count == mt[1].total_count);
        /* FAULT_MUTATE was emitted. */
        MOQ_TEST_CHECK(mt[0].mutate_control > 0);
        MOQ_TEST_CHECK(mt[0].got_mutate);
        /* Original and mutated have same length. */
        MOQ_TEST_CHECK(mt[0].got_pre_action);
        MOQ_TEST_CHECK(mt[0].orig_len == mt[0].mut_len);
        /* Exactly one bit differs. */
        if (mt[0].orig_len == mt[0].mut_len && mt[0].orig_len > 0) {
            size_t diff_bytes = 0;
            size_t diff_idx = 0;
            for (size_t j = 0; j < mt[0].orig_len; j++) {
                if (mt[0].orig_bytes[j] != mt[0].mut_bytes[j]) {
                    diff_bytes++;
                    diff_idx = j;
                }
            }
            MOQ_TEST_CHECK(diff_bytes == 1);
            MOQ_TEST_CHECK(diff_idx == mt[0].mut_byte_idx);
            uint8_t xor = mt[0].orig_bytes[diff_idx] ^ mt[0].mut_bytes[diff_idx];
            MOQ_TEST_CHECK(xor == (1u << mt[0].mut_bit_idx));
        }
        /* Input trace matches mutated bytes. */
        MOQ_TEST_CHECK(mt[0].got_post_input);
        MOQ_TEST_CHECK(mt[0].post_input_len == mt[0].mut_len);
        if (mt[0].post_input_len == mt[0].mut_len)
            MOQ_TEST_CHECK(memcmp(mt[0].post_input_bytes, mt[0].mut_bytes,
                                   mt[0].mut_len) == 0);
        /* Same byte/bit across runs. */
        MOQ_TEST_CHECK(mt[0].mut_byte_idx == mt[1].mut_byte_idx);
        MOQ_TEST_CHECK(mt[0].mut_bit_idx == mt[1].mut_bit_idx);
    }

    /* == Different seed produces different mutation pattern =========== */
    {
        mutate_trace_t mt[2];
        uint64_t seeds[2] = { 0xA078, 0xB079 };
        for (int s = 0; s < 2; s++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&mt[s], 0, sizeof(mt[s]));
            mt[s].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[s];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = mutate_trace_fn;
            cfg.trace_ctx = &mt[s];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_MUTATE_CONTROL;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK(mt[0].got_mutate && mt[1].got_mutate);
        MOQ_TEST_CHECK(mt[0].mut_byte_idx != mt[1].mut_byte_idx ||
                        mt[0].mut_bit_idx != mt[1].mut_bit_idx ||
                        memcmp(mt[0].mut_bytes, mt[1].mut_bytes,
                               mt[0].mut_len) != 0);
    }

    /* == DROP_CONTROL + MUTATE_CONTROL precedence ===================== */
    /* At 1000‰ with DROP_CONTROL, all controls dropped, zero mutations,
     * zero control inputs. */
    {
        mutate_trace_t mt;
        memset(&mt, 0, sizeof(mt));
        mt.hash = 0xCBF29CE484222325ULL;

        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xD209;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.trace_fn = mutate_trace_fn;
        cfg.trace_ctx = &mt;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DROP_CONTROL |
                          MOQ_SIM_FAULT_MUTATE_CONTROL;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_enable_faults(sp);
        moq_simpair_run_until_quiescent(sp, 4, NULL);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp)) !=
                        MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(mt.drop_control > 0);
        MOQ_TEST_CHECK(mt.mutate_control == 0);
        MOQ_TEST_CHECK(mt.input_control == 0);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == REORDER trace oracle + determinism ============================ */
    /* Server accepts subscribe then writes an object. The poll batch
     * contains SUBSCRIBE_OK (SEND_CONTROL) + SEND_DATA. With 100%
     * reorder, they swap. Run twice to prove determinism. */
    {
        reorder_trace_t rt[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&rt[run], 0, sizeof(rt[run]));
            rt[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xBA7C4;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_REORDER_ACTION;
            cfg.trace_fn = reorder_trace_fn;
            cfg.trace_ctx = &rt[run];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            uint8_t pdata[] = {1, 2, 3, 4};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, sizeof(pdata), &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* Oracle: FAULT_REORDER was emitted. */
        MOQ_TEST_CHECK(rt[0].reorder_count > 0);
        MOQ_TEST_CHECK(rt[0].got_first_reorder);
        /* First reorder swapped positions 0 and 1. */
        MOQ_TEST_CHECK(rt[0].first_code == 0);
        MOQ_TEST_CHECK(rt[0].first_count_val == 1);
        /* Displaced kind is SEND_CONTROL (SUBSCRIBE_OK moved later). */
        MOQ_TEST_CHECK(rt[0].first_displaced_kind == MOQ_ACTION_SEND_CONTROL);
        /* ACTION trace after reorder: SEND_DATA first, SEND_CONTROL second. */
        MOQ_TEST_CHECK(rt[0].post_action_idx >= 2);
        MOQ_TEST_CHECK(rt[0].post_action_kinds[0] == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(rt[0].post_action_kinds[1] == MOQ_ACTION_SEND_CONTROL);
        /* Determinism: same results across runs. */
        MOQ_TEST_CHECK(rt[0].hash == rt[1].hash);
        MOQ_TEST_CHECK(rt[0].total_count == rt[1].total_count);
        MOQ_TEST_CHECK(rt[0].reorder_count == rt[1].reorder_count);
        MOQ_TEST_CHECK(rt[0].first_displaced_kind == rt[1].first_displaced_kind);
    }

    /* == Different seed changes reorder pattern ======================== */
    /* Use two seeds. Both create the same eligible batch (subscribe+
     * accept+write). Assert reorder_count or action order differs. */
    {
        reorder_trace_t rt[2];
        uint64_t seeds[2] = { 0xBA7C4, 0xCA7C5 };
        for (int s = 0; s < 2; s++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&rt[s], 0, sizeof(rt[s]));
            rt[s].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[s];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 500;
            cfg.fault_flags = MOQ_SIM_FAULT_REORDER_ACTION;
            cfg.trace_fn = reorder_trace_fn;
            cfg.trace_ctx = &rt[s];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            uint8_t pdata[] = {1, 2, 3, 4};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, sizeof(pdata), &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* At 50% rate, different seeds should produce different
         * reorder decisions for the same eligible batch. */
        MOQ_TEST_CHECK(rt[0].reorder_count != rt[1].reorder_count ||
                        (rt[0].post_action_idx >= 2 &&
                         rt[1].post_action_idx >= 2 &&
                         rt[0].post_action_kinds[0] != rt[1].post_action_kinds[0]));
    }

    /* == MUTATE_DATA determinism + oracle ============================= */
    /* Subscribe, accept, write object with known payload. 100% MUTATE_DATA.
     * Two runs must match. FAULT_MUTATE with action_kind=SEND_DATA emitted.
     * Exactly one bit differs between known original and mutated bytes. */
    {
        mutate_trace_t mt[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&mt[run], 0, sizeof(mt[run]));
            mt[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDA7A;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = mutate_trace_fn;
            cfg.trace_ctx = &mt[run];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_MUTATE_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev); }

            uint8_t pdata[8] = {0xDE,0xAD,0xBE,0xEF,0xCA,0xFE,0xBA,0xBE};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 8, &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        /* Determinism. */
        MOQ_TEST_CHECK(mt[0].hash == mt[1].hash);
        MOQ_TEST_CHECK(mt[0].total_count == mt[1].total_count);
        /* FAULT_MUTATE with SEND_DATA was emitted. */
        MOQ_TEST_CHECK(mt[0].mutate_data > 0);
        MOQ_TEST_CHECK(mt[0].got_mutate);
        MOQ_TEST_CHECK(mt[0].mut_action_kind == MOQ_ACTION_SEND_DATA);
        /* One-bit oracle: reconstruct original from ACTION header +
         * payload. The first mutated SEND_DATA may be the subgroup
         * header (no payload) or the object write (with payload).
         * Use orig_payload_count to determine which. */
        if (mt[0].got_mutate && mt[0].got_pre_action) {
            uint8_t orig_combined[MUT_MAX_BYTES];
            size_t orig_total = mt[0].orig_len + mt[0].orig_payload_count;
            MOQ_TEST_CHECK(orig_total == mt[0].mut_len);
            if (orig_total == mt[0].mut_len && orig_total <= MUT_MAX_BYTES) {
                memcpy(orig_combined, mt[0].orig_bytes, mt[0].orig_len);
                if (mt[0].orig_payload_count > 0) {
                    uint8_t known_payload[8] = {0xDE,0xAD,0xBE,0xEF,
                                                 0xCA,0xFE,0xBA,0xBE};
                    if (mt[0].orig_payload_count == 8)
                        memcpy(orig_combined + mt[0].orig_len,
                               known_payload, 8);
                }
                size_t diff_bytes = 0;
                size_t diff_idx = 0;
                for (size_t j = 0; j < orig_total; j++) {
                    if (orig_combined[j] != mt[0].mut_bytes[j]) {
                        diff_bytes++;
                        diff_idx = j;
                    }
                }
                MOQ_TEST_CHECK(diff_bytes == 1);
                MOQ_TEST_CHECK(diff_idx == mt[0].mut_byte_idx);
                uint8_t xor_val = orig_combined[diff_idx] ^
                                  mt[0].mut_bytes[diff_idx];
                MOQ_TEST_CHECK(xor_val == (1u << mt[0].mut_bit_idx));
            }
        }
        /* Input DATA_BYTES matches mutated bytes. */
        MOQ_TEST_CHECK(mt[0].got_post_input);
        MOQ_TEST_CHECK(mt[0].post_input_len == mt[0].mut_len);
        if (mt[0].post_input_len == mt[0].mut_len)
            MOQ_TEST_CHECK(memcmp(mt[0].post_input_bytes, mt[0].mut_bytes,
                                   mt[0].mut_len) == 0);
        /* Cross-run determinism of mutation details. */
        MOQ_TEST_CHECK(mt[0].mut_byte_idx == mt[1].mut_byte_idx);
        MOQ_TEST_CHECK(mt[0].mut_bit_idx == mt[1].mut_bit_idx);
        MOQ_TEST_CHECK(mt[0].mut_len == mt[1].mut_len);
    }

    /* == MUTATE_DATA different seed =================================== */
    {
        mutate_trace_t mt[2];
        uint64_t seeds[2] = { 0xDA7A, 0xDA7B };
        for (int s = 0; s < 2; s++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&mt[s], 0, sizeof(mt[s]));
            mt[s].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[s];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.trace_fn = mutate_trace_fn;
            cfg.trace_ctx = &mt[s];
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_MUTATE_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_subscription_t srv_sub = {0};
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                  srv_sub = ev.u.subscribe_request.sub;
                  moq_accept_subscribe_cfg_t acc;
                  moq_accept_subscribe_cfg_init(&acc);
                  moq_session_accept_subscribe(moq_simpair_server(sp),
                      srv_sub, &acc, moq_simpair_now_us(sp));
              }
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev); }

            uint8_t pdata[8] = {0xDE,0xAD,0xBE,0xEF,0xCA,0xFE,0xBA,0xBE};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 8, &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            moq_simpair_enable_faults(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            { moq_event_t evts[8]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[8]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK(mt[0].got_mutate && mt[1].got_mutate);
        MOQ_TEST_CHECK(mt[0].mut_byte_idx != mt[1].mut_byte_idx ||
                        mt[0].mut_bit_idx != mt[1].mut_bit_idx ||
                        memcmp(mt[0].mut_bytes, mt[1].mut_bytes,
                               mt[0].mut_len) != 0);
    }

    /* == MUTATE_DATA + SPLIT_DATA composition ========================= */
    {
        mutate_trace_t mt;
        memset(&mt, 0, sizeof(mt));
        mt.hash = 0xCBF29CE484222325ULL;

        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7C;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.trace_fn = mutate_trace_fn;
        cfg.trace_ctx = &mt;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_MUTATE_DATA | MOQ_SIM_FAULT_SPLIT_DATA;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
        }

        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_subscription_t srv_sub = {0};
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              srv_sub = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv_sub, &acc, moq_simpair_now_us(sp));
          }
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t pdata[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 16, &payload);
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);
        moq_session_close_subgroup(moq_simpair_server(sp), sg,
            moq_simpair_now_us(sp));

        moq_simpair_enable_faults(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Both mutation and split should have fired. */
        MOQ_TEST_CHECK(mt.mutate_data > 0);
        MOQ_TEST_CHECK(mt.got_mutate);
        /* Input was delivered (possibly in multiple chunks). */
        MOQ_TEST_CHECK(mt.got_post_input);
        MOQ_TEST_CHECK(mt.post_input_chunks >= 1);
        /* Concatenated input chunks match mutated bytes. */
        MOQ_TEST_CHECK(mt.post_input_len == mt.mut_len);
        if (mt.post_input_len == mt.mut_len)
            MOQ_TEST_CHECK(memcmp(mt.post_input_bytes, mt.mut_bytes,
                                   mt.mut_len) == 0);
        /* With 16-byte payload + header, total data should produce
         * multiple split chunks for at least the object SEND_DATA.
         * The input_data counter should show more records than
         * normal (2 calls for header+payload normally). */
        MOQ_TEST_CHECK(mt.input_data > 2);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DROP_DATA beats MUTATE_DATA ================================== */
    {
        mutate_trace_t mt;
        memset(&mt, 0, sizeof(mt));
        mt.hash = 0xCBF29CE484222325ULL;

        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xD20D;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.trace_fn = mutate_trace_fn;
        cfg.trace_ctx = &mt;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DROP_DATA | MOQ_SIM_FAULT_MUTATE_DATA;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
        }

        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_subscription_t srv_sub = {0};
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              srv_sub = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv_sub, &acc, moq_simpair_now_us(sp));
          }
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t pdata[4] = {1,2,3,4};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 4, &payload);
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);

        moq_simpair_enable_faults(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Drop wins: data dropped, not mutated, no data input. */
        MOQ_TEST_CHECK(mt.drop_data > 0);
        MOQ_TEST_CHECK(mt.mutate_data == 0);
        MOQ_TEST_CHECK(mt.input_data == 0);

        { moq_event_t evts[8]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 8)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[8]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 8)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Stream map exhaustion returns error ========================== */
    /* Open 16 subgroups (max stream map slots), then try a 17th.
     * SimPair should return an error instead of delivering to ref 0. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xFULL;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 64;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 64;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
        }

        /* Subscribe + accept. */
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        scfg.track_namespace.parts = ns_parts;
        scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_subscription_t srv_sub = {0};
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              srv_sub = ev.u.subscribe_request.sub;
              moq_accept_subscribe_cfg_t acc;
              moq_accept_subscribe_cfg_init(&acc);
              moq_session_accept_subscribe(moq_simpair_server(sp),
                  srv_sub, &acc, moq_simpair_now_us(sp));
          }
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        /* Open 17 subgroups: session pool allows it, but SimPair
         * stream map has only 16 slots. */
        moq_subgroup_handle_t sgs[17];
        int opened = 0;
        for (int g = 0; g < 17; g++) {
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            sgcfg.group_id = (uint64_t)g;
            moq_result_t rc = moq_session_open_subgroup(
                moq_simpair_server(sp), srv_sub, &sgcfg,
                moq_simpair_now_us(sp), &sgs[g]);
            MOQ_TEST_CHECK(rc == MOQ_OK);
            if (rc == MOQ_OK) opened++;
        }
        MOQ_TEST_CHECK(opened == 17);

        if (opened == 17) {
            /* Pump: 17th SEND_DATA exhausts the stream map. */
            moq_result_t pump_rc = moq_simpair_run_until_quiescent(
                sp, 8, NULL);
            MOQ_TEST_CHECK(pump_rc == MOQ_ERR_INTERNAL);
        }

        /* Cleanup. */
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == INJECT_RESET: synthetic reset delivered to receiver ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        inject_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xA001;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = inject_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_INJECT_RESET;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Subscribe: client -> server. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
            server_sub, &acc, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Server opens subgroup and writes one object. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
            server_sub, &sg_cfg, moq_simpair_now_us(sp), &sg) == MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"data", 4, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
            sg, 0, payload, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(payload);

        /* Pump data without faults to establish stream mapping. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Drain received object. */
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Enable faults and pump: INJECT_RESET should fire. */
        moq_simpair_enable_faults(sp);

        /* Write another object to trigger a pump with active stream. */
        payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"more", 4, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
            sg, 1, payload, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(payload);

        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(trace.inject_reset >= 1);
        MOQ_TEST_CHECK(trace.inject_stop == 0);
        MOQ_TEST_CHECK(trace.last_inject_from == MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(trace.last_inject_to == MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(trace.last_inject_result == MOQ_OK);

        /* Cleanup. */
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == INJECT_STOP: sender cleanup + follow-up RESET routes ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        inject_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xB002;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = inject_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_INJECT_STOP;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
            server_sub, &acc, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Server opens subgroup and writes object to create mapping. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
            server_sub, &sg_cfg, moq_simpair_now_us(sp), &sg) == MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"data", 4, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
            sg, 0, payload, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(payload);

        /* Pump data to establish stream mapping. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_OBJECT_RECEIVED);
        moq_event_cleanup(&ev);

        /* Enable faults. */
        moq_simpair_enable_faults(sp);

        /* Write another object so there is a pump with active stream. */
        payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"more", 4, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
            sg, 1, payload, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(payload);

        /* Pump: INJECT_STOP fires, sender gets on_data_stop and queues
         * RESET_DATA. Next pump delivers RESET_DATA to receiver and
         * removes the stream mapping. */
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(trace.inject_stop >= 1);
        MOQ_TEST_CHECK(trace.inject_reset == 0);
        MOQ_TEST_CHECK(trace.last_inject_from == MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(trace.last_inject_to == MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(trace.last_inject_result == MOQ_OK);
        /* Prove STOP→RESET follow-up chain routed. */
        MOQ_TEST_CHECK(trace.reset_action_count >= 1);
        MOQ_TEST_CHECK(trace.reset_input_count >= 1);
        MOQ_TEST_CHECK(trace.last_reset_input_result == MOQ_OK);

        /* Cleanup. */
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == INJECT with no active stream: no crash, no injection ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        inject_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xC003;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = inject_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_INJECT_RESET | MOQ_SIM_FAULT_INJECT_STOP;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_enable_faults(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(trace.inject_reset == 0);
        MOQ_TEST_CHECK(trace.inject_stop == 0);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == INJECT determinism: same seed → same counters ============== */
    {
        inject_trace_t traces[2];
        memset(traces, 0, sizeof(traces));
        traces[0].hash = 0xCBF29CE484222325ULL;
        traces[1].hash = 0xCBF29CE484222325ULL;

        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xD004;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 10;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 10;
            cfg.trace_fn = inject_trace_fn;
            cfg.trace_ctx = &traces[run];
            cfg.fault_per_mille = 500;
            cfg.fault_flags = MOQ_SIM_FAULT_ALL | MOQ_SIM_FAULT_ALL_INJECT;

            moq_simpair_t *sp = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            moq_event_t ev;
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_subscribe_cfg_t sub_cfg;
            moq_subscribe_cfg_init(&sub_cfg);
            sub_cfg.track_namespace = ns;
            sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
            sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t sub;
            MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
                &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
                &ev, 1) == 1);
            moq_subscription_t server_sub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);

            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
                server_sub, &acc, moq_simpair_now_us(sp)) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);

            moq_simpair_enable_faults(sp);

            moq_subgroup_cfg_t sg_cfg;
            moq_subgroup_cfg_init(&sg_cfg);
            sg_cfg.group_id = 0;
            sg_cfg.subgroup_id = 0;
            sg_cfg.publisher_priority = 128;
            moq_subgroup_handle_t sg;
            MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
                server_sub, &sg_cfg, moq_simpair_now_us(sp), &sg) == MOQ_OK);

            for (int obj = 0; obj < 5; obj++) {
                moq_rcbuf_t *p = NULL;
                MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
                    (const uint8_t *)"x", 1, &p) == MOQ_OK);
                MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
                    sg, (uint64_t)obj, p, moq_simpair_now_us(sp)) == MOQ_OK);
                moq_rcbuf_decref(p);
            }

            moq_simpair_run_until_quiescent(sp, 32, NULL);

            { moq_event_t evts[16]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);

            if (run == 1) {
                MOQ_TEST_CHECK(traces[0].hash == traces[1].hash);
                MOQ_TEST_CHECK(traces[0].inject_reset == traces[1].inject_reset);
                MOQ_TEST_CHECK(traces[0].inject_stop == traces[1].inject_stop);
                MOQ_TEST_CHECK(traces[0].total_count == traces[1].total_count);
            }
        }
    }

    /* == INJECT_CLOSE: synthetic protocol-close via malformed input == */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        inject_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xE005;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = inject_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_INJECT_CLOSE;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            == MOQ_SESS_ESTABLISHED);

        moq_simpair_enable_faults(sp);

        /* First pump direction injects close into the to_session.
         * Step once explicitly to control which direction fires first. */
        moq_simpair_step(sp, NULL);

        /* At least one session should be closed. */
        MOQ_TEST_CHECK(trace.inject_close >= 1);
        MOQ_TEST_CHECK(trace.last_inject_result == MOQ_OK);
        MOQ_TEST_CHECK(trace.close_action_count >= 1);

        bool server_closed =
            moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_CLOSED;
        bool client_closed =
            moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_CLOSED;
        MOQ_TEST_CHECK(server_closed || client_closed);

        /* Verify the closed session emits SESSION_CLOSED with code 0x3.
         * CLOSE_SESSION action was already consumed by pump_direction
         * (traced as ACTION); verify via trace counter. */
        MOQ_TEST_CHECK(trace.close_action_count >= 1);

        moq_session_t *closed_session = server_closed ?
            moq_simpair_server(sp) : moq_simpair_client(sp);

        bool found_closed_event = false;
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(closed_session, evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) {
                  if (evts[j].kind == MOQ_EVENT_SESSION_CLOSED) {
                      MOQ_TEST_CHECK(evts[j].u.closed.code == 0x3);
                      found_closed_event = true;
                  }
                  moq_event_cleanup(&evts[j]);
              }
        }
        MOQ_TEST_CHECK(found_closed_event);

        /* Drain the other session. */
        moq_session_t *other_session = server_closed ?
            moq_simpair_client(sp) : moq_simpair_server(sp);
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(other_session, evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(other_session, acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == INJECT_CLOSE: no injection when already closed ============= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        inject_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xF006;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = inject_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_INJECT_CLOSE;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_simpair_enable_faults(sp);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        size_t close_count_after_quiescent = trace.inject_close;
        MOQ_TEST_CHECK(close_count_after_quiescent >= 1);

        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }

        /* Pump again — both sessions closed, no re-injection. */
        moq_simpair_step(sp, NULL);
        MOQ_TEST_CHECK(trace.inject_close == close_count_after_quiescent);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == TRUNCATE_CONTROL: partial control delivery =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        truncate_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xC001;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = truncate_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_TRUNCATE_CONTROL;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);

        /* Complete setup without faults. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Subscribe: client -> server (this produces a control message). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);

        /* Enable faults before pumping the SUBSCRIBE. */
        moq_simpair_enable_faults(sp);

        /* Pump: the SUBSCRIBE control message should be truncated. */
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* At least one control truncation should have fired. */
        MOQ_TEST_CHECK(trace.truncate_control >= 1);
        MOQ_TEST_CHECK(trace.truncate_data == 0);
        MOQ_TEST_CHECK(trace.got_first == true);
        MOQ_TEST_CHECK(trace.first_action_kind == MOQ_ACTION_SEND_CONTROL);
        MOQ_TEST_CHECK(trace.first_prefix_len <= trace.first_original_len);

        /* Determinism: run with the same seed and compare hashes. */
        truncate_trace_t trace2 = {0};
        trace2.hash = 0xCBF29CE484222325ULL;
        {
            test_alloc_state_t as2 = {0};
            moq_alloc_t alloc2 = test_allocator(&as2);
            moq_simpair_cfg_t cfg2 = MOQ_SIMPAIR_CFG_INIT;
            cfg2.alloc = &alloc2;
            cfg2.seed = 0xC001;
            cfg2.initial_now_us = 1000;
            cfg2.client_send_request_capacity = true;
            cfg2.client_initial_request_capacity = 10;
            cfg2.server_send_request_capacity = true;
            cfg2.server_initial_request_capacity = 10;
            cfg2.trace_fn = truncate_trace_fn;
            cfg2.trace_ctx = &trace2;
            cfg2.fault_per_mille = 1000;
            cfg2.fault_flags = MOQ_SIM_FAULT_TRUNCATE_CONTROL;

            moq_simpair_t *sp2 = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg2, &sp2) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp2) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp2, 8, NULL);
            moq_event_t ev2;
            if (moq_session_poll_events(moq_simpair_client(sp2), &ev2, 1) == 1)
                moq_event_cleanup(&ev2);
            if (moq_session_poll_events(moq_simpair_server(sp2), &ev2, 1) == 1)
                moq_event_cleanup(&ev2);

            moq_bytes_t ns_parts2[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns2 = { ns_parts2, 1 };
            moq_subscribe_cfg_t sub_cfg2;
            moq_subscribe_cfg_init(&sub_cfg2);
            sub_cfg2.track_namespace = ns2;
            sub_cfg2.track_name = MOQ_BYTES_LITERAL("trk");
            sub_cfg2.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t sub2;
            moq_session_subscribe(moq_simpair_client(sp2),
                &sub_cfg2, moq_simpair_now_us(sp2), &sub2);
            moq_simpair_enable_faults(sp2);
            moq_simpair_run_until_quiescent(sp2, 16, NULL);

            /* Drain owned resources. */
            { moq_event_t evts[16]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp2), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp2), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp2), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp2), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }

            moq_simpair_destroy(sp2);
            MOQ_TEST_CHECK(as2.balance == 0);
        }

        MOQ_TEST_CHECK(trace.hash == trace2.hash);
        MOQ_TEST_CHECK(trace.total_count == trace2.total_count);
        MOQ_TEST_CHECK(trace.truncate_control == trace2.truncate_control);

        /* Cleanup. */
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == TRUNCATE_DATA: partial data + reset =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        truncate_trace_t trace = {0};
        trace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7A;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 10;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 10;
        cfg.trace_fn = truncate_trace_fn;
        cfg.trace_ctx = &trace;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_TRUNCATE_DATA;

        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_start(sp) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Subscribe: client -> server. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("trk");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp),
            &sub_cfg, moq_simpair_now_us(sp), &sub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_server(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(moq_simpair_server(sp),
            server_sub, &acc, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_session_poll_events(moq_simpair_client(sp),
            &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_OK);
        moq_event_cleanup(&ev);

        /* Server opens subgroup and writes an object. */
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        sg_cfg.group_id = 0;
        sg_cfg.subgroup_id = 0;
        sg_cfg.publisher_priority = 128;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK(moq_session_open_subgroup(moq_simpair_server(sp),
            server_sub, &sg_cfg, moq_simpair_now_us(sp), &sg) == MOQ_OK);

        moq_rcbuf_t *payload = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc,
            (const uint8_t *)"truncation_test_data", 20, &payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(moq_simpair_server(sp),
            sg, 0, payload, moq_simpair_now_us(sp)) == MOQ_OK);
        moq_rcbuf_decref(payload);

        /* Enable faults before pumping data. */
        moq_simpair_enable_faults(sp);

        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* At least one data truncation should have fired. */
        MOQ_TEST_CHECK(trace.truncate_data >= 1);
        MOQ_TEST_CHECK(trace.truncate_control == 0);
        MOQ_TEST_CHECK(trace.got_first == true);
        MOQ_TEST_CHECK(trace.first_action_kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK(trace.first_prefix_len <= trace.first_original_len);
        /* A data reset should follow the truncated data delivery. */
        MOQ_TEST_CHECK(trace.input_data_reset >= 1);

        /* Determinism: run with the same seed and compare hashes. */
        truncate_trace_t trace2 = {0};
        trace2.hash = 0xCBF29CE484222325ULL;
        {
            test_alloc_state_t as2 = {0};
            moq_alloc_t alloc2 = test_allocator(&as2);
            moq_simpair_cfg_t cfg2 = MOQ_SIMPAIR_CFG_INIT;
            cfg2.alloc = &alloc2;
            cfg2.seed = 0xDA7A;
            cfg2.initial_now_us = 1000;
            cfg2.client_send_request_capacity = true;
            cfg2.client_initial_request_capacity = 10;
            cfg2.server_send_request_capacity = true;
            cfg2.server_initial_request_capacity = 10;
            cfg2.trace_fn = truncate_trace_fn;
            cfg2.trace_ctx = &trace2;
            cfg2.fault_per_mille = 1000;
            cfg2.fault_flags = MOQ_SIM_FAULT_TRUNCATE_DATA;

            moq_simpair_t *sp2 = NULL;
            MOQ_TEST_CHECK(moq_simpair_create(&cfg2, &sp2) == MOQ_OK);
            MOQ_TEST_CHECK(moq_simpair_start(sp2) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp2, 8, NULL);
            moq_event_t ev2;
            if (moq_session_poll_events(moq_simpair_client(sp2), &ev2, 1) == 1)
                moq_event_cleanup(&ev2);
            if (moq_session_poll_events(moq_simpair_server(sp2), &ev2, 1) == 1)
                moq_event_cleanup(&ev2);

            moq_bytes_t ns_parts2[] = { MOQ_BYTES_LITERAL("ns") };
            moq_namespace_t ns2 = { ns_parts2, 1 };
            moq_subscribe_cfg_t sub_cfg2;
            moq_subscribe_cfg_init(&sub_cfg2);
            sub_cfg2.track_namespace = ns2;
            sub_cfg2.track_name = MOQ_BYTES_LITERAL("trk");
            sub_cfg2.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t sub2;
            moq_session_subscribe(moq_simpair_client(sp2),
                &sub_cfg2, moq_simpair_now_us(sp2), &sub2);
            moq_simpair_run_until_quiescent(sp2, 8, NULL);

            moq_event_t ev2b;
            if (moq_session_poll_events(moq_simpair_server(sp2), &ev2b, 1) == 1) {
                moq_subscription_t ssub2 = ev2b.u.subscribe_request.sub;
                moq_event_cleanup(&ev2b);
                moq_accept_subscribe_cfg_t acc2;
                moq_accept_subscribe_cfg_init(&acc2);
                moq_session_accept_subscribe(moq_simpair_server(sp2),
                    ssub2, &acc2, moq_simpair_now_us(sp2));
                moq_simpair_run_until_quiescent(sp2, 8, NULL);
                if (moq_session_poll_events(moq_simpair_client(sp2), &ev2b, 1) == 1)
                    moq_event_cleanup(&ev2b);

                moq_subgroup_cfg_t sg_cfg2;
                moq_subgroup_cfg_init(&sg_cfg2);
                sg_cfg2.group_id = 0; sg_cfg2.subgroup_id = 0;
                sg_cfg2.publisher_priority = 128;
                moq_subgroup_handle_t sg2;
                moq_session_open_subgroup(moq_simpair_server(sp2),
                    ssub2, &sg_cfg2, moq_simpair_now_us(sp2), &sg2);
                moq_rcbuf_t *p2 = NULL;
                moq_rcbuf_create(&alloc2,
                    (const uint8_t *)"truncation_test_data", 20, &p2);
                if (p2) {
                    moq_session_write_object(moq_simpair_server(sp2),
                        sg2, 0, p2, moq_simpair_now_us(sp2));
                    moq_rcbuf_decref(p2);
                }
                moq_simpair_enable_faults(sp2);
                moq_simpair_run_until_quiescent(sp2, 16, NULL);
            }

            /* Drain owned resources. */
            { moq_event_t evts[16]; size_t ne;
              while ((ne = moq_session_poll_events(moq_simpair_client(sp2), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              while ((ne = moq_session_poll_events(moq_simpair_server(sp2), evts, 16)) > 0)
                  for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
              moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp2), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp2), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }

            moq_simpair_destroy(sp2);
            MOQ_TEST_CHECK(as2.balance == 0);
        }

        MOQ_TEST_CHECK(trace.hash == trace2.hash);
        MOQ_TEST_CHECK(trace.total_count == trace2.total_count);
        MOQ_TEST_CHECK(trace.truncate_data == trace2.truncate_data);
        MOQ_TEST_CHECK(trace.input_data_reset == trace2.input_data_reset);

        /* Cleanup. */
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
              for (size_t j = 0; j < ne; j++) moq_event_cleanup(&evts[j]);
          moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* S1: SPLIT_CONTROL / SPLIT_BIDI deterministic chunking           */
    /* ================================================================ */

    /* == SPLIT_CONTROL: REQUEST_UPDATE split over control stream ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0x5C117C0;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_CONTROL;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        /* Client publishes, server accepts — capture sv_pub at accept. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(moq_simpair_client(sp), &pcfg,
            moq_simpair_now_us(sp), &pub_h);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        moq_publication_t sv_pub = MOQ_PUBLICATION_INVALID;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
            moq_simpair_server(sp), &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_REQUEST);
        sv_pub = ev.u.publish_request.pub;
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(moq_simpair_server(sp),
            sv_pub, &acfg, moq_simpair_now_us(sp));
        moq_event_cleanup(&ev);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Server (subscriber) sends REQUEST_UPDATE — split over control. */
        moq_publication_update_cfg_t ucfg;
        moq_publication_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true;
        ucfg.subscriber_priority = 77;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_publication(
            moq_simpair_server(sp), sv_pub, &ucfg, moq_simpair_now_us(sp)),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Client (publisher) must see PUBLISH_UPDATED. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
            moq_simpair_client(sp), &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_PUBLISH_UPDATED);
        MOQ_TEST_CHECK(ev.u.publish_updated.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_INT(ev.u.publish_updated.subscriber_priority, 77);
        moq_event_cleanup(&ev);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SPLIT_CONTROL: PUBLISH_DONE split still emits terminal event = */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xD00ED00E;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_CONTROL;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(moq_simpair_client(sp), &pcfg,
            moq_simpair_now_us(sp), &pub_h);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
            moq_simpair_server(sp), &ev, 1), 1);
        moq_accept_publish_cfg_t acfg;
        moq_accept_publish_cfg_init(&acfg);
        moq_session_accept_publish(moq_simpair_server(sp),
            ev.u.publish_request.pub, &acfg, moq_simpair_now_us(sp));
        moq_event_cleanup(&ev);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(moq_simpair_client(sp), pub_h,
            &fcfg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool found_finished = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_PUBLISH_FINISHED)
                found_finished = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_finished);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SPLIT_BIDI: request + response split over bidi stream ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xB1D1B1D1;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_BIDI;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        /* Server publishes namespace (control stream, not bidi). */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t ncfg;
        moq_publish_namespace_cfg_init(&ncfg);
        ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        moq_announcement_t ann;
        moq_session_publish_namespace(moq_simpair_server(sp), &ncfg,
            moq_simpair_now_us(sp), &ann);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Client subscribes to namespace — OPEN_BIDI_STREAM split. */
        moq_subscribe_namespace_cfg_t sncfg;
        moq_subscribe_namespace_cfg_init(&sncfg);
        sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
        moq_ns_sub_handle_t nsh;
        moq_session_subscribe_namespace(moq_simpair_client(sp), &sncfg,
            moq_simpair_now_us(sp), &nsh);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Server sees NS_SUB_REQUEST despite split request. */
        moq_ns_sub_handle_t srv_nsh = MOQ_NS_SUB_HANDLE_INVALID;
        bool found_ns_sub = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                found_ns_sub = true;
                srv_nsh = ev.u.ns_sub_request.handle;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_ns_sub);

        /* Server accepts — SEND_BIDI_STREAM response split. */
        moq_accept_ns_sub_cfg_t ans;
        moq_accept_ns_sub_cfg_init(&ans);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(
            moq_simpair_server(sp), srv_nsh, &ans,
            moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Client must receive NS_SUB_OK despite split response. */
        bool found_ns_ok = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_NS_SUB_OK)
                found_ns_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_ns_ok);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SPLIT_CONTROL: randomized chunking across SUBSCRIBE ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0x1B47E;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_SPLIT_CONTROL;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("track");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        bool found_sub = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                found_sub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_sub);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* S2a: DELAY fault — control + bidi                               */
    /* ================================================================ */

    /* == Delayed control does not arrive at same time ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A1;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        /* Subscribe — control action will be delayed. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);

        /* Pump at same time — delayed entries should NOT arrive. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Server should NOT see SUBSCRIBE_REQUEST yet. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
            moq_simpair_server(sp), &ev, 1), 0);

        /* Delayed count > 0, deadline in the future. */
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 0);
        uint64_t deadline = moq_simpair_next_deadline_us(sp);
        MOQ_TEST_CHECK(deadline > moq_simpair_now_us(sp));

        /* advance_to alone does not deliver. */
        moq_simpair_advance_to(sp, deadline);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
            moq_simpair_server(sp), &ev, 1), 0);

        /* advance + run_until_quiescent delivers. */
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        bool found_sub = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                found_sub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_sub);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Determinism: same seed same trace hash ======================== */
    {
        trace_summary_t traces[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&traces[run], 0, sizeof(traces[run]));
            traces[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDE1A2;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 500;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &traces[run];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }
            moq_simpair_enable_faults(sp);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);

            moq_simpair_run_until_quiescent(sp, 8, NULL);

            uint64_t deadline = moq_simpair_next_deadline_us(sp);
            if (deadline != UINT64_MAX) {
                moq_simpair_advance_to(sp, deadline);
                moq_simpair_run_until_quiescent(sp, 16, NULL);
            }

            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }
            { moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK_EQ_U64(traces[0].hash, traces[1].hash);
        MOQ_TEST_CHECK(traces[0].count == traces[1].count);
    }

    /* == Destroy with pending delayed entries: clean balance =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A3;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 0);
        /* Destroy without delivering. */
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Matured delivery can enqueue responses, quiescence continues = */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A4;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        /* Client publishes — delayed control. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_publish_cfg_t pcfg;
        moq_publish_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pcfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_publication_t pub_h;
        moq_session_publish(moq_simpair_client(sp), &pcfg,
            moq_simpair_now_us(sp), &pub_h);

        /* Same-time quiescence — nothing delivered. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 0);

        /* Advance and drain. The delayed PUBLISH delivery will produce
         * a PUBLISH_REQUEST on the server, which may produce response
         * actions. run_until_quiescent must continue pumping those. */
        uint64_t dl = moq_simpair_next_deadline_us(sp);
        moq_simpair_advance_to(sp, dl);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        /* Server should see PUBLISH_REQUEST. */
        moq_event_t ev;
        bool found = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST)
                found = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DELAY + SPLIT_CONTROL compose: multiple delayed chunks ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A5;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_CONTROL;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("track");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 1);
        MOQ_TEST_CHECK(dtrace.delay_enqueue_count > 1);

        uint64_t dl = moq_simpair_next_deadline_us(sp);
        while (dl != UINT64_MAX && dl > moq_simpair_now_us(sp)) {
            moq_simpair_advance_to(sp, dl);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            dl = moq_simpair_next_deadline_us(sp);
        }

        moq_event_t ev;
        bool found_sub = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                found_sub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_sub);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DELAY + SPLIT_BIDI: NS_SUB_OK arrives after advance ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A6;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t ncfg;
        moq_publish_namespace_cfg_init(&ncfg);
        ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        moq_announcement_t ann;
        moq_session_publish_namespace(moq_simpair_server(sp), &ncfg,
            moq_simpair_now_us(sp), &ann);

        /* Drain everything at all deadlines until stable. */
        for (int iter = 0; iter < 20; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_subscribe_namespace_cfg_t sncfg;
        moq_subscribe_namespace_cfg_init(&sncfg);
        sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
        moq_ns_sub_handle_t nsh;
        moq_session_subscribe_namespace(moq_simpair_client(sp), &sncfg,
            moq_simpair_now_us(sp), &nsh);

        /* Drain all deadlines to deliver OPEN_BIDI + request. */
        for (int iter = 0; iter < 20; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Server must have received NS_SUB_REQUEST. */
        moq_ns_sub_handle_t srv_nsh = MOQ_NS_SUB_HANDLE_INVALID;
        bool found_ns_sub = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                srv_nsh = ev.u.ns_sub_request.handle;
                found_ns_sub = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_ns_sub);
        MOQ_TEST_CHECK(moq_ns_sub_handle_is_valid(srv_nsh));

        /* Server accepts. */
        moq_accept_ns_sub_cfg_t ans;
        moq_accept_ns_sub_cfg_init(&ans);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_ns_sub(
            moq_simpair_server(sp), srv_nsh, &ans,
            moq_simpair_now_us(sp)), (int)MOQ_OK);

        for (int iter = 0; iter < 20; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Client must receive NS_SUB_OK. */
        bool found_ok = false;
        for (int k = 0; k < 8; k++) {
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
                break;
            if (ev.kind == MOQ_EVENT_NS_SUB_OK)
                found_ok = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_ok);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Different seeds produce different trace hashes ================ */
    {
        trace_summary_t traces[2];
        uint64_t seeds[2] = { 0xDE1A7, 0xDE1A8 };
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&traces[run], 0, sizeof(traces[run]));
            traces[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = seeds[run];
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 500;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &traces[run];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }
            moq_simpair_enable_faults(sp);

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);

            for (int iter = 0; iter < 10; iter++) {
                moq_simpair_run_until_quiescent(sp, 8, NULL);
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
            }
            moq_simpair_run_until_quiescent(sp, 16, NULL);

            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }
            { moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK(traces[0].hash != traces[1].hash);
    }

    /* == Destroy with pending delayed bidi entries: clean balance ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE1A9;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }
        moq_simpair_enable_faults(sp);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_namespace_cfg_t ncfg;
        moq_publish_namespace_cfg_init(&ncfg);
        ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        moq_announcement_t ann;
        moq_session_publish_namespace(moq_simpair_server(sp), &ncfg,
            moq_simpair_now_us(sp), &ann);

        for (int iter = 0; iter < 10; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev);
        }

        moq_subscribe_namespace_cfg_t sncfg;
        moq_subscribe_namespace_cfg_init(&sncfg);
        sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
        moq_ns_sub_handle_t nsh;
        moq_session_subscribe_namespace(moq_simpair_client(sp), &sncfg,
            moq_simpair_now_us(sp), &nsh);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 0);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == OPEN_BIDI alloc failure: no stale delayed chunks delivered ==== */
    {
        /* Run once without failure to count allocations needed for the
         * delayed OPEN_BIDI_STREAM path. Then rerun with fail_at set
         * to fail the last delay chunk alloc (mid-split). */
        uint64_t baseline_allocs = 0;
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDE1AA;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_publish_namespace_cfg_t ncfg;
            moq_publish_namespace_cfg_init(&ncfg);
            ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            moq_announcement_t ann;
            moq_session_publish_namespace(moq_simpair_server(sp), &ncfg,
                moq_simpair_now_us(sp), &ann);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_simpair_enable_faults(sp);
            uint64_t pre_allocs = oas.alloc_count;

            moq_subscribe_namespace_cfg_t sncfg;
            moq_subscribe_namespace_cfg_init(&sncfg);
            sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
            moq_ns_sub_handle_t nsh;
            moq_session_subscribe_namespace(moq_simpair_client(sp), &sncfg,
                moq_simpair_now_us(sp), &nsh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            baseline_allocs = oas.alloc_count - pre_allocs;
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }

        /* Now rerun failing the last delay alloc. */
        if (baseline_allocs > 1) {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDE1AA;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
            moq_publish_namespace_cfg_t ncfg;
            moq_publish_namespace_cfg_init(&ncfg);
            ncfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            moq_announcement_t ann;
            moq_session_publish_namespace(moq_simpair_server(sp), &ncfg,
                moq_simpair_now_us(sp), &ann);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev);
            }

            moq_simpair_enable_faults(sp);
            uint64_t pre_allocs = oas.alloc_count;
            oas.fail_at = pre_allocs + baseline_allocs;

            moq_subscribe_namespace_cfg_t sncfg;
            moq_subscribe_namespace_cfg_init(&sncfg);
            sncfg.track_namespace_prefix = (moq_namespace_t){ ns_parts, 1 };
            moq_ns_sub_handle_t nsh;
            moq_session_subscribe_namespace(moq_simpair_client(sp), &sncfg,
                moq_simpair_now_us(sp), &nsh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            /* Advance past any delayed entries and drain. */
            for (int iter = 0; iter < 10; iter++) {
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
                moq_simpair_run_until_quiescent(sp, 16, NULL);
            }

            /* Server must NOT see NS_SUB_REQUEST from a rolled-back
             * open whose partial chunks were stale-dropped. */
            moq_event_t ev;
            bool found_ns_sub = false;
            while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                    found_ns_sub = true;
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(!found_ns_sub);

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
    }

    /* ================================================================ */
    /* S2b: data stream delay                                          */
    /* ================================================================ */

    /* S2b: data stream delay tests.
     * STOP_DATA delay is implemented but not directly testable here
     * because STOP requires INJECT_STOP (transport-level injection).
     * The STOP delay path shares the same deliver_or_delay pattern
     * as RESET and is exercised indirectly by scenario runners with
     * INJECT_STOP + DELAY enabled together. */

    /* == DELAY + SPLIT_DATA: object payload reconstructs correctly ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7A1;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        /* Subscribe + accept without faults. */
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Enable faults, write object. */
        moq_simpair_enable_faults(sp);

        uint8_t pdata[16] = {0xDE,0xAD,0xBE,0xEF,1,2,3,4,5,6,7,8,9,10,11,12};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 16, &payload);
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);
        moq_session_close_subgroup(moq_simpair_server(sp), sg,
            moq_simpair_now_us(sp));

        /* Drain all delayed entries. */
        for (int iter = 0; iter < 30; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Client should have received the object. */
        bool found_obj = false;
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && ev.u.object_received.payload) {
                size_t plen = moq_rcbuf_len(ev.u.object_received.payload);
                const uint8_t *pd = moq_rcbuf_data(ev.u.object_received.payload);
                if (plen == 16 && memcmp(pd, pdata, 16) == 0)
                    found_obj = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found_obj);
        MOQ_TEST_CHECK(dtrace.delay_enqueue_count > 0);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Data delay determinism: same seed same trace hash ============ */
    {
        trace_summary_t traces[2];
        for (int run = 0; run < 2; run++) {
            test_alloc_state_t as = {0};
            moq_alloc_t alloc = test_allocator(&as);
            memset(&traces[run], 0, sizeof(traces[run]));
            traces[run].hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDA7A2;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 500;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
            cfg.trace_fn = trace_hash_fn;
            cfg.trace_ctx = &traces[run];

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_subscription_t srv_sub = {0};
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                srv_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(moq_simpair_server(sp),
                    srv_sub, &acc, moq_simpair_now_us(sp));
                moq_event_cleanup(&ev);
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);

            moq_simpair_enable_faults(sp);

            uint8_t pdata[8] = {1,2,3,4,5,6,7,8};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 8, &payload);
            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));

            for (int iter = 0; iter < 30; iter++) {
                moq_simpair_run_until_quiescent(sp, 16, NULL);
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
            }
            moq_simpair_run_until_quiescent(sp, 16, NULL);

            { moq_event_t ev2;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
                  moq_event_cleanup(&ev2);
              while (moq_session_poll_events(moq_simpair_server(sp), &ev2, 1) == 1)
                  moq_event_cleanup(&ev2);
            }
            { moq_action_t acts[16]; size_t na;
              while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
              while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
                  for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(as.balance == 0);
        }
        MOQ_TEST_CHECK_EQ_U64(traces[0].hash, traces[1].hash);
        MOQ_TEST_CHECK(traces[0].count == traces[1].count);
    }

    /* == Delayed RESET_DATA retires data map at deadline =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7A3;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Open subgroup, write one object, then reset. */
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        uint8_t pdata[4] = {1,2,3,4};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 4, &payload);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);
        moq_session_reset_subgroup(moq_simpair_server(sp), sg,
            0x1, moq_simpair_now_us(sp));

        moq_simpair_enable_faults(sp);

        /* At same time, nothing delivered. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) > 0);

        /* Advance and drain. */
        for (int iter = 0; iter < 30; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) == 0);
        MOQ_TEST_CHECK(dtrace.input_data_reset_count > 0);

        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }
        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Multiple delayed subgroups with FIN drain without crash ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7A4;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_simpair_enable_faults(sp);

        /* Write object + close (FIN). Both split+delayed. */
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        uint8_t pdata[8] = {0xAA,0xBB,0xCC,0xDD,1,2,3,4};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 8, &payload);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);
        moq_session_close_subgroup(moq_simpair_server(sp), sg,
            moq_simpair_now_us(sp));

        /* Open a SECOND subgroup on the same subscription to produce
         * post-FIN data actions on the same stream slot once it recycles. */
        moq_subgroup_cfg_t sgcfg2;
        moq_subgroup_cfg_init(&sgcfg2);
        sgcfg2.group_id = 1;
        moq_subgroup_handle_t sg2;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg2, moq_simpair_now_us(sp), &sg2);
        moq_rcbuf_t *payload2 = NULL;
        moq_rcbuf_create(&alloc, pdata, 4, &payload2);
        moq_session_write_object(moq_simpair_server(sp), sg2, 0,
            payload2, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload2);
        moq_session_close_subgroup(moq_simpair_server(sp), sg2,
            moq_simpair_now_us(sp));

        /* Drain all delays. */
        for (int iter = 0; iter < 40; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) == 0);

        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }
        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SEND_DATA OOM rollback: partial chunks invalidated =========== */
    {
        uint64_t baseline_allocs = 0;
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDA7A5;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_subscription_t srv_sub = {0};
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                srv_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(moq_simpair_server(sp),
                    srv_sub, &acc, moq_simpair_now_us(sp));
                moq_event_cleanup(&ev);
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);

            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;

            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            uint8_t pdata[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 16, &payload);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            baseline_allocs = oas.alloc_count - pre;
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }

        if (baseline_allocs > 2) {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);

            delay_trace_t dtrace;
            memset(&dtrace, 0, sizeof(dtrace));
            dtrace.hash = 0xCBF29CE484222325ULL;

            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc;
            cfg.seed = 0xDA7A5;
            cfg.trace_fn = delay_trace_fn;
            cfg.trace_ctx = &dtrace;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;

            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
            }

            moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
            scfg.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(sp), &scfg,
                moq_simpair_now_us(sp), &sub_h);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_subscription_t srv_sub = {0};
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                srv_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(moq_simpair_server(sp),
                    srv_sub, &acc, moq_simpair_now_us(sp));
                moq_event_cleanup(&ev);
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);

            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            oas.fail_at = pre + baseline_allocs;

            moq_subgroup_cfg_t sgcfg;
            moq_subgroup_cfg_init(&sgcfg);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
            uint8_t pdata[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *payload = NULL;
            moq_rcbuf_create(&alloc, pdata, 16, &payload);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                payload, moq_simpair_now_us(sp));
            moq_rcbuf_decref(payload);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);

            /* Drain any partial delayed entries — they should be stale. */
            for (int iter = 0; iter < 20; iter++) {
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
                moq_simpair_run_until_quiescent(sp, 16, NULL);
            }

            /* Partial chunks must have been enqueued then stale-dropped. */
            MOQ_TEST_CHECK(dtrace.delay_enqueue_count > 0);
            MOQ_TEST_CHECK(dtrace.delay_stale_count > 0);

            /* No object delivered from the failed send. */
            { moq_event_t ev2;
              bool found_obj = false;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1) {
                  if (ev2.kind == MOQ_EVENT_OBJECT_RECEIVED) found_obj = true;
                  moq_event_cleanup(&ev2);
              }
              MOQ_TEST_CHECK(!found_obj);
            }

            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
    }

    /* == Partial FIFO: later chunk queued when earlier is delayed ===== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDA7A7;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 300;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_simpair_enable_faults(sp);

        /* Write a large object to produce many split chunks, some of
         * which will have delay fire and force FIFO on the rest. */
        uint8_t pdata[64];
        for (int k = 0; k < 64; k++) pdata[k] = (uint8_t)k;
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 64, &payload);
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);
        moq_session_close_subgroup(moq_simpair_server(sp), sg,
            moq_simpair_now_us(sp));

        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* With 30% fault rate and many split chunks, at least some
         * should be delayed, and FIFO forcing should have queued
         * later chunks whose own delay decision missed. */
        size_t delayed = moq_simpair_delayed_count(sp);
        size_t enqueued = dtrace.delay_enqueue_count;
        MOQ_TEST_CHECK(delayed > 0);
        MOQ_TEST_CHECK(enqueued > 0);
        MOQ_TEST_CHECK(dtrace.delay_fifo_forced_count > 0);

        /* Advance and drain completely. */
        for (int iter = 0; iter < 40; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Object should have been delivered correctly. */
        bool found_obj = false;
        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1) {
              if (ev2.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                  ev2.u.object_received.payload &&
                  moq_rcbuf_len(ev2.u.object_received.payload) == 64)
                  found_obj = true;
              moq_event_cleanup(&ev2);
          }
        }
        MOQ_TEST_CHECK(found_obj);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(sp))
            != MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) == 0);

        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* S5: DELAY + injection coverage                                  */
    /* ================================================================ */

    /* == DELAY + INJECT_STOP: delayed data + injected stop ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0x570A1;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY |
                          MOQ_SIM_FAULT_INJECT_STOP;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Write data to create a stream. */
        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        uint8_t pdata[8] = {1,2,3,4,5,6,7,8};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 8, &payload);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);

        /* Pump without faults to establish stream mapping. */
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }

        moq_simpair_enable_faults(sp);

        /* Write more data. Delay is 100%, so bytes are delayed.
         * INJECT_STOP fires on the active stream during pump. */
        moq_rcbuf_t *payload2 = NULL;
        moq_rcbuf_create(&alloc, pdata, 8, &payload2);
        moq_session_write_object(moq_simpair_server(sp), sg, 1,
            payload2, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload2);

        /* Pump + drain deadlines. INJECT_STOP may have fired. */
        for (int iter = 0; iter < 30; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(dtrace.delay_enqueue_count > 0);
        MOQ_TEST_CHECK(dtrace.input_data_stop_count > 0);
        /* STOP→RESET follow-up chain: sender queues RESET_DATA action
         * after on_data_stop, which routes to receiver. */
        MOQ_TEST_CHECK(dtrace.reset_action_count > 0);
        MOQ_TEST_CHECK(dtrace.input_data_reset_count > 0);
        /* terminal_pending guard: injection must fire exactly once. */
        MOQ_TEST_CHECK(dtrace.inject_count == 1);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) == 0);

        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
          while (moq_session_poll_events(moq_simpair_server(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }
        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == DELAY + INJECT_RESET: delayed data + injected reset ========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        delay_trace_t dtrace;
        memset(&dtrace, 0, sizeof(dtrace));
        dtrace.hash = 0xCBF29CE484222325ULL;

        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xDE5E7;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        cfg.fault_per_mille = 1000;
        cfg.fault_flags = MOQ_SIM_FAULT_DELAY |
                          MOQ_SIM_FAULT_INJECT_RESET;
        cfg.trace_fn = delay_trace_fn;
        cfg.trace_ctx = &dtrace;

        moq_simpair_t *sp = NULL;
        moq_simpair_create(&cfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
          moq_session_poll_events(moq_simpair_server(sp), &ev, 1);
        }

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("t");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_event_t ev;
        moq_subscription_t srv_sub = {0};
        if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
            srv_sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t acc;
            moq_accept_subscribe_cfg_init(&acc);
            moq_session_accept_subscribe(moq_simpair_server(sp),
                srv_sub, &acc, moq_simpair_now_us(sp));
            moq_event_cleanup(&ev);
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(moq_simpair_server(sp), srv_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        uint8_t pdata[8] = {1,2,3,4,5,6,7,8};
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, pdata, 8, &payload);
        moq_session_write_object(moq_simpair_server(sp), sg, 0,
            payload, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }

        moq_simpair_enable_faults(sp);

        moq_rcbuf_t *payload2 = NULL;
        moq_rcbuf_create(&alloc, pdata, 8, &payload2);
        moq_session_write_object(moq_simpair_server(sp), sg, 1,
            payload2, moq_simpair_now_us(sp));
        moq_rcbuf_decref(payload2);

        for (int iter = 0; iter < 30; iter++) {
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            uint64_t dl = moq_simpair_next_deadline_us(sp);
            if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
            moq_simpair_advance_to(sp, dl);
        }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        MOQ_TEST_CHECK(dtrace.delay_enqueue_count > 0);
        MOQ_TEST_CHECK(dtrace.input_data_reset_count > 0);
        MOQ_TEST_CHECK(moq_simpair_delayed_count(sp) == 0);

        { moq_event_t ev2;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
          while (moq_session_poll_events(moq_simpair_server(sp), &ev2, 1) == 1)
              moq_event_cleanup(&ev2);
        }
        { moq_action_t acts[16]; size_t na;
          while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
          while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
              for (size_t j = 0; j < na; j++) moq_action_cleanup(&acts[j]);
        }
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================ */
    /* S6: delay fault OOM coverage                                    */
    /* ================================================================ */

    /* == Control delay OOM: propagates NOMEM, clean balance =========== */
    {
        uint64_t baseline = 0;
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A1;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ ns, 1 };
            sc.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sh;
            moq_session_subscribe(moq_simpair_client(sp), &sc,
                moq_simpair_now_us(sp), &sh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            baseline = oas.alloc_count - pre;
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
        MOQ_TEST_CHECK(baseline > 0);
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A1;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            oas.fail_at = pre + baseline;
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ ns, 1 };
            sc.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sh;
            moq_session_subscribe(moq_simpair_client(sp), &sc,
                moq_simpair_now_us(sp), &sh);
            moq_result_t qrc = moq_simpair_run_until_quiescent(sp, 8, NULL);
            MOQ_TEST_CHECK_EQ_INT((int)qrc, (int)MOQ_ERR_NOMEM);
            /* No subscribe event on server. */
            moq_event_t ev;
            MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(
                moq_simpair_server(sp), &ev, 1), 0);
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
    }

    /* == Data delay OOM: partial split chunks stale-dropped =========== */
    {
        uint64_t baseline = 0;
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            delay_trace_t dt; memset(&dt, 0, sizeof(dt));
            dt.hash = UINT64_C(0xCBF29CE484222325);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A2;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
            cfg.trace_fn = delay_trace_fn; cfg.trace_ctx = &dt;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            /* Subscribe + accept without faults. */
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ ns, 1 };
            sc.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sh;
            moq_session_subscribe(moq_simpair_client(sp), &sc,
                moq_simpair_now_us(sp), &sh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_subscription_t srv = {0};
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                srv = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(moq_simpair_server(sp),
                    srv, &acc, moq_simpair_now_us(sp));
                moq_event_cleanup(&ev);
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv,
                &sgc, moq_simpair_now_us(sp), &sg);
            uint8_t pd[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *pl = NULL;
            moq_rcbuf_create(&alloc, pd, 16, &pl);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                pl, moq_simpair_now_us(sp));
            moq_rcbuf_decref(pl);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            baseline = oas.alloc_count - pre;
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
        MOQ_TEST_CHECK(baseline > 2);
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            delay_trace_t dt; memset(&dt, 0, sizeof(dt));
            dt.hash = UINT64_C(0xCBF29CE484222325);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A2;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_DATA;
            cfg.trace_fn = delay_trace_fn; cfg.trace_ctx = &dt;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
            sc.track_namespace = (moq_namespace_t){ ns, 1 };
            sc.track_name = MOQ_BYTES_LITERAL("t");
            moq_subscription_t sh;
            moq_session_subscribe(moq_simpair_client(sp), &sc,
                moq_simpair_now_us(sp), &sh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            moq_event_t ev;
            moq_subscription_t srv = {0};
            if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                srv = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acc;
                moq_accept_subscribe_cfg_init(&acc);
                moq_session_accept_subscribe(moq_simpair_server(sp),
                    srv, &acc, moq_simpair_now_us(sp));
                moq_event_cleanup(&ev);
            }
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                moq_event_cleanup(&ev);
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            oas.fail_at = pre + baseline;
            moq_subgroup_cfg_t sgc; moq_subgroup_cfg_init(&sgc);
            moq_subgroup_handle_t sg;
            moq_session_open_subgroup(moq_simpair_server(sp), srv,
                &sgc, moq_simpair_now_us(sp), &sg);
            uint8_t pd[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            moq_rcbuf_t *pl = NULL;
            moq_rcbuf_create(&alloc, pd, 16, &pl);
            moq_session_write_object(moq_simpair_server(sp), sg, 0,
                pl, moq_simpair_now_us(sp));
            moq_rcbuf_decref(pl);
            moq_session_close_subgroup(moq_simpair_server(sp), sg,
                moq_simpair_now_us(sp));
            moq_result_t qrc = moq_simpair_run_until_quiescent(sp, 8, NULL);
            MOQ_TEST_CHECK_EQ_INT((int)qrc, (int)MOQ_ERR_NOMEM);
            /* Drain: partial entries must be stale-dropped. */
            for (int iter = 0; iter < 20; iter++) {
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
                moq_simpair_run_until_quiescent(sp, 16, NULL);
            }
            MOQ_TEST_CHECK(dt.delay_enqueue_count > 0);
            MOQ_TEST_CHECK(dt.delay_stale_count > 0);
            /* No object delivered from the failed send. */
            { moq_event_t ev2;
              bool found = false;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev2, 1) == 1) {
                  if (ev2.kind == MOQ_EVENT_OBJECT_RECEIVED) found = true;
                  moq_event_cleanup(&ev2);
              }
              MOQ_TEST_CHECK(!found);
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
    }

    /* == Bidi delay OOM: ns_sub request not delivered, clean balance == */
    {
        uint64_t baseline = 0;
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A3;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_publish_namespace_cfg_t nc;
            moq_publish_namespace_cfg_init(&nc);
            nc.track_namespace = (moq_namespace_t){ ns, 1 };
            moq_announcement_t ann;
            moq_session_publish_namespace(moq_simpair_server(sp), &nc,
                moq_simpair_now_us(sp), &ann);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev); }
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            moq_subscribe_namespace_cfg_t snc;
            moq_subscribe_namespace_cfg_init(&snc);
            snc.track_namespace_prefix = (moq_namespace_t){ ns, 1 };
            moq_ns_sub_handle_t nsh;
            moq_session_subscribe_namespace(moq_simpair_client(sp), &snc,
                moq_simpair_now_us(sp), &nsh);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            baseline = oas.alloc_count - pre;
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
        MOQ_TEST_CHECK(baseline > 1);
        {
            oom_alloc_state_t oas = {0};
            moq_alloc_t alloc = oom_allocator(&oas);
            moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
            cfg.alloc = &alloc; cfg.seed = 0x00A3;
            cfg.initial_now_us = 1000;
            cfg.client_send_request_capacity = true;
            cfg.client_initial_request_capacity = 16;
            cfg.server_send_request_capacity = true;
            cfg.server_initial_request_capacity = 16;
            cfg.fault_per_mille = 1000;
            cfg.fault_flags = MOQ_SIM_FAULT_DELAY | MOQ_SIM_FAULT_SPLIT_BIDI;
            moq_simpair_t *sp = NULL;
            moq_simpair_create(&cfg, &sp);
            moq_simpair_start(sp);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            { moq_event_t ev;
              moq_session_poll_events(moq_simpair_client(sp), &ev, 1);
              moq_session_poll_events(moq_simpair_server(sp), &ev, 1); }
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("ns") };
            moq_publish_namespace_cfg_t nc;
            moq_publish_namespace_cfg_init(&nc);
            nc.track_namespace = (moq_namespace_t){ ns, 1 };
            moq_announcement_t ann;
            moq_session_publish_namespace(moq_simpair_server(sp), &nc,
                moq_simpair_now_us(sp), &ann);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            { moq_event_t ev;
              while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
                  moq_event_cleanup(&ev); }
            moq_simpair_enable_faults(sp);
            uint64_t pre = oas.alloc_count;
            oas.fail_at = pre + baseline;
            moq_subscribe_namespace_cfg_t snc;
            moq_subscribe_namespace_cfg_init(&snc);
            snc.track_namespace_prefix = (moq_namespace_t){ ns, 1 };
            moq_ns_sub_handle_t nsh;
            moq_session_subscribe_namespace(moq_simpair_client(sp), &snc,
                moq_simpair_now_us(sp), &nsh);
            moq_result_t qrc = moq_simpair_run_until_quiescent(sp, 8, NULL);
            MOQ_TEST_CHECK_EQ_INT((int)qrc, (int)MOQ_ERR_NOMEM);
            /* Drain stale entries. */
            for (int iter = 0; iter < 20; iter++) {
                uint64_t dl = moq_simpair_next_deadline_us(sp);
                if (dl == UINT64_MAX || dl <= moq_simpair_now_us(sp)) break;
                moq_simpair_advance_to(sp, dl);
                moq_simpair_run_until_quiescent(sp, 16, NULL);
            }
            /* Server must NOT see NS_SUB_REQUEST. */
            moq_event_t ev;
            bool found_ns = false;
            while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) found_ns = true;
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(!found_ns);
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(oas.balance == 0);
        }
    }

    MOQ_TEST_PASS("test_simpair");
    return failures;
}
