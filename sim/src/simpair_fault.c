#include "simpair_internal.h"

void trace_record(moq_simpair_t *sp,
                  const moq_sim_trace_record_t *record)
{
    if (sp->trace_fn)
        sp->trace_fn(sp->trace_ctx, record);
}

void trace_input(moq_simpair_t *sp,
                 moq_sim_trace_input_kind_t input_kind,
                 moq_perspective_t from,
                 moq_perspective_t to,
                 moq_bytes_t bytes,
                 moq_result_t result)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_INPUT;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.input_kind = input_kind;
    r.bytes = bytes;
    r.result = result;
    trace_record(sp, &r);
}

void trace_action(moq_simpair_t *sp,
                  moq_perspective_t from,
                  moq_perspective_t to,
                  const moq_action_t *action)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_ACTION;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.action_kind = action->kind;
    if (action->kind == MOQ_ACTION_SEND_CONTROL) {
        r.bytes.data = action->u.send_control.data;
        r.bytes.len = action->u.send_control.len;
    } else if (action->kind == MOQ_ACTION_CLOSE_SESSION) {
        r.code = action->u.close_session.code;
    } else if (action->kind == MOQ_ACTION_SEND_DATAGRAM) {
        r.bytes.data = action->u.send_datagram.data;
        r.bytes.len = action->u.send_datagram.len;
    } else if (action->kind == MOQ_ACTION_SEND_DATA) {
        r.bytes.data = action->u.send_data.header;
        r.bytes.len = action->u.send_data.header_len;
        if (action->u.send_data.payload) {
            r.count = moq_rcbuf_len(action->u.send_data.payload);
        }
    } else if (action->kind == MOQ_ACTION_RESET_DATA) {
        r.code = action->u.reset_data.error_code;
    } else if (action->kind == MOQ_ACTION_STOP_DATA) {
        r.code = action->u.stop_data.error_code;
    }
    trace_record(sp, &r);
}

void trace_quiescent(moq_simpair_t *sp, size_t count)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_QUIESCENT;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.count = count;
    trace_record(sp, &r);
}

void trace_fault_drop(moq_simpair_t *sp,
                      moq_perspective_t from,
                      moq_perspective_t to,
                      const moq_action_t *action)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_FAULT_DROP;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.action_kind = action->kind;
    if (action->kind == MOQ_ACTION_SEND_CONTROL) {
        r.bytes.data = action->u.send_control.data;
        r.bytes.len = action->u.send_control.len;
    } else if (action->kind == MOQ_ACTION_SEND_DATAGRAM) {
        r.bytes.data = action->u.send_datagram.data;
        r.bytes.len = action->u.send_datagram.len;
    } else if (action->kind == MOQ_ACTION_SEND_DATA) {
        r.bytes.data = action->u.send_data.header;
        r.bytes.len = action->u.send_data.header_len;
        if (action->u.send_data.payload)
            r.count = moq_rcbuf_len(action->u.send_data.payload);
    } else if (action->kind == MOQ_ACTION_RESET_DATA) {
        r.code = action->u.reset_data.error_code;
    } else if (action->kind == MOQ_ACTION_STOP_DATA) {
        r.code = action->u.stop_data.error_code;
    }
    trace_record(sp, &r);
}

uint64_t sim_mix64(uint64_t z)
{
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

bool sim_fault_fires(const moq_simpair_t *sp,
                     size_t action_index,
                     moq_action_kind_t action_kind)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;

    uint32_t flag;
    switch (action_kind) {
    case MOQ_ACTION_SEND_CONTROL: flag = MOQ_SIM_FAULT_DROP_CONTROL; break;
    case MOQ_ACTION_SEND_DATA:    flag = MOQ_SIM_FAULT_DROP_DATA;    break;
    case MOQ_ACTION_RESET_DATA:   flag = MOQ_SIM_FAULT_DROP_RESET;   break;
    case MOQ_ACTION_STOP_DATA:    flag = MOQ_SIM_FAULT_DROP_STOP;    break;
    default: return false;
    }
    if (!(sp->fault_flags & flag))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= (uint64_t)action_kind * 0xA0761D6478BD642FULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_split_fires(const moq_simpair_t *sp,
                     size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_SPLIT_DATA))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0x3C79AC492BA7B653ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_split_control_fires(const moq_simpair_t *sp,
                             size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_SPLIT_CONTROL))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xA1B2C3D4E5F60718ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_split_bidi_fires(const moq_simpair_t *sp,
                          size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_SPLIT_BIDI))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xF0E1D2C3B4A59687ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_mutate_fires(const moq_simpair_t *sp,
                      size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_MUTATE_CONTROL))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0x7E57A7E000000001ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

void trace_fault_mutate(moq_simpair_t *sp,
                        moq_perspective_t from,
                        moq_perspective_t to,
                        moq_action_kind_t action_kind,
                        const uint8_t *mutated_data,
                        size_t mutated_len,
                        size_t byte_idx,
                        unsigned bit_idx)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_FAULT_MUTATE;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.action_kind = action_kind;
    r.bytes.data = mutated_data;
    r.bytes.len = mutated_len;
    r.code = byte_idx;
    r.count = bit_idx;
    trace_record(sp, &r);
}

bool sim_reorder_eligible(moq_action_kind_t kind)
{
    return kind == MOQ_ACTION_SEND_CONTROL || kind == MOQ_ACTION_SEND_DATA;
}

bool sim_reorder_fires(const moq_simpair_t *sp,
                       size_t batch_index,
                       moq_action_kind_t kind_i,
                       moq_action_kind_t kind_j)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_REORDER_ACTION))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)batch_index * 0xD6E8FEB86659FD93ULL;
    x ^= (uint64_t)kind_i * 0xA0761D6478BD642FULL;
    x ^= (uint64_t)kind_j * 0x3C79AC492BA7B653ULL;
    x ^= 0xADACE4720DEC0001ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

void trace_fault_reorder(moq_simpair_t *sp,
                         moq_perspective_t from,
                         moq_perspective_t to,
                         moq_action_kind_t displaced_kind,
                         size_t orig_i, size_t orig_j)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_FAULT_REORDER;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.action_kind = displaced_kind;
    r.code = orig_i;
    r.count = orig_j;
    trace_record(sp, &r);
}

bool sim_mutate_data_fires(const moq_simpair_t *sp,
                           size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_MUTATE_DATA))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xDA7ADA7A00000001ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_truncate_control_fires(const moq_simpair_t *sp,
                                size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_TRUNCATE_CONTROL))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0x7ECCA7E000000011ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

bool sim_truncate_data_fires(const moq_simpair_t *sp,
                             size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_TRUNCATE_DATA))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xDA7A7ECDA7E00011ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

void trace_fault_truncate(moq_simpair_t *sp,
                          moq_perspective_t from,
                          moq_perspective_t to,
                          moq_action_kind_t action_kind,
                          size_t prefix_len,
                          size_t original_len)
{
    moq_sim_trace_record_t r;
    memset(&r, 0, sizeof(r));
    r.struct_size = sizeof(r);
    r.kind = MOQ_SIM_TRACE_FAULT_TRUNCATE;
    r.seed = sp->seed;
    r.step = sp->step;
    r.now_us = sp->now_us;
    r.from = from;
    r.to = to;
    r.action_kind = action_kind;
    r.code = prefix_len;
    r.count = original_len;
    trace_record(sp, &r);
}

bool sim_delay_fires(const moq_simpair_t *sp,
                     size_t action_index)
{
    if (!sp->fault_enabled || sp->fault_per_mille == 0)
        return false;
    if (!(sp->fault_flags & MOQ_SIM_FAULT_DELAY))
        return false;

    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xDE1A7DE1A7000001ULL;
    return (sim_mix64(x) % 1000) < sp->fault_per_mille;
}

uint64_t sim_delay_compute_due(const moq_simpair_t *sp,
                               size_t action_index)
{
    uint64_t x = sp->seed;
    x ^= sp->step * 0x9E3779B97F4A7C15ULL;
    x ^= (uint64_t)action_index * 0xD6E8FEB86659FD93ULL;
    x ^= 0xD0E71AE000000001ULL;
    uint64_t delay_us = (sim_mix64(x) % 900) + 100;
    return sp->now_us + delay_us;
}
