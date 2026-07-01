#include "simpair_internal.h"

int sim_stream_map_find(moq_simpair_t *sp, uint64_t sender_ref,
                        moq_perspective_t sender)
{
    for (int i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; i++)
        if (sp->stream_map[i].active &&
            sp->stream_map[i].sender_ref == sender_ref &&
            sp->stream_map[i].sender == sender)
            return i;
    return -1;
}

moq_result_t sim_stream_map_get_or_create(moq_simpair_t *sp,
                                          uint64_t sender_ref,
                                          moq_perspective_t sender,
                                          uint64_t *out_rx_ref,
                                          int *out_slot,
                                          bool *out_created)
{
    int slot = sim_stream_map_find(sp, sender_ref, sender);
    if (slot >= 0) {
        *out_rx_ref = sp->stream_map[slot].receiver_ref;
        if (out_slot) *out_slot = slot;
        if (out_created) *out_created = false;
        return MOQ_OK;
    }

    for (int i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; i++) {
        if (!sp->stream_map[i].active) {
            sp->stream_map[i].active = true;
            sp->stream_map[i].terminal_pending = false;
            sp->stream_map[i].sender_ref = sender_ref;
            sp->stream_map[i].sender = sender;
            sp->stream_map[i].receiver_ref = sp->next_rx_ref++;
            sp->stream_map[i].generation++;
            sp->stream_map[i].last_due_us = 0;
            *out_rx_ref = sp->stream_map[i].receiver_ref;
            if (out_slot) *out_slot = i;
            if (out_created) *out_created = true;
            return MOQ_OK;
        }
    }
    return MOQ_ERR_INTERNAL;
}

void sim_stream_map_remove(moq_simpair_t *sp, uint64_t sender_ref,
                           moq_perspective_t sender)
{
    int slot = sim_stream_map_find(sp, sender_ref, sender);
    if (slot >= 0) {
        sp->stream_map[slot].active = false;
        sp->stream_map[slot].generation++;
    }
}

/* -- Bidi stream map ----------------------------------------------- */

int sim_bidi_find_by_opener(moq_simpair_t *sp, uint64_t opener_ref,
                            moq_perspective_t opener)
{
    for (int i = 0; i < MOQ_SIM_MAX_BIDI_STREAMS; i++)
        if (sp->bidi_map[i].active &&
            sp->bidi_map[i].opener_ref == opener_ref &&
            sp->bidi_map[i].opener == opener)
            return i;
    return -1;
}

int sim_bidi_find_by_responder(moq_simpair_t *sp, uint64_t responder_ref,
                               moq_perspective_t opener)
{
    for (int i = 0; i < MOQ_SIM_MAX_BIDI_STREAMS; i++)
        if (sp->bidi_map[i].active &&
            sp->bidi_map[i].responder_ref == responder_ref &&
            sp->bidi_map[i].opener == opener)
            return i;
    return -1;
}

moq_result_t pump_direction(moq_simpair_t *sp,
                            moq_session_t *from_session,
                            moq_session_t *to_session,
                            moq_perspective_t from,
                            moq_perspective_t to,
                            size_t *delivered)
{
    uint8_t mutate_buf[2048];
    moq_action_t actions[16];
    size_t n;
    while ((n = moq_session_poll_actions(from_session, actions, 16)) > 0) {
        /* Reorder pass: adjacent swap of eligible pairs. */
        for (size_t ri = 0; ri + 1 < n; ri++) {
            if (sim_reorder_eligible(actions[ri].kind) &&
                sim_reorder_eligible(actions[ri + 1].kind) &&
                sim_reorder_fires(sp, ri, actions[ri].kind,
                                  actions[ri + 1].kind)) {
                trace_fault_reorder(sp, from, to, actions[ri].kind,
                                     ri, ri + 1);
                moq_action_t tmp = actions[ri];
                actions[ri] = actions[ri + 1];
                actions[ri + 1] = tmp;
                ri++;
            }
        }

        for (size_t i = 0; i < n; i++) {
            trace_action(sp, from, to, &actions[i]);
            (*delivered)++;

            if (sim_fault_fires(sp, *delivered - 1, actions[i].kind)) {
                trace_fault_drop(sp, from, to, &actions[i]);
                if (actions[i].kind == MOQ_ACTION_SEND_DATA &&
                    actions[i].u.send_data.fin)
                    sim_stream_map_remove(sp,
                        actions[i].u.send_data.stream_ref._v, from);
                if (actions[i].kind == MOQ_ACTION_RESET_DATA)
                    sim_stream_map_remove(sp,
                        actions[i].u.reset_data.stream_ref._v, from);
                if (actions[i].kind == MOQ_ACTION_STOP_DATA) {
                    uint64_t stop_ref = actions[i].u.stop_data.stream_ref._v;
                    for (int j = 0; j < MOQ_SIM_MAX_DATA_STREAMS; j++) {
                        if (sp->stream_map[j].active &&
                            sp->stream_map[j].receiver_ref == stop_ref &&
                            sp->stream_map[j].sender != from) {
                            sp->stream_map[j].active = false;
                            break;
                        }
                    }
                }
                moq_action_cleanup(&actions[i]);
                continue;
            }

            if (actions[i].kind == MOQ_ACTION_SEND_CONTROL ||
                actions[i].kind == MOQ_ACTION_OPEN_UNI_CONTROL ||
                actions[i].kind == MOQ_ACTION_SEND_UNI_CONTROL) {
                /* A unidirectional control-channel pair carries the same control
                 * bytes as a single bidirectional control channel; route both
                 * through the control delivery path so either control topology
                 * gets the same truncation / mutation / delay machinery. */
                const uint8_t *ctl_data;
                size_t ctl_len;
                if (actions[i].kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                    ctl_data = actions[i].u.open_uni_control.data;
                    ctl_len  = actions[i].u.open_uni_control.len;
                } else if (actions[i].kind == MOQ_ACTION_SEND_UNI_CONTROL) {
                    ctl_data = actions[i].u.send_uni_control.data;
                    ctl_len  = actions[i].u.send_uni_control.len;
                } else {
                    ctl_data = actions[i].u.send_control.data;
                    ctl_len  = actions[i].u.send_control.len;
                }

                /* Truncation: deliver a random prefix, skip the rest. */
                if (sim_truncate_control_fires(sp, *delivered - 1) &&
                    ctl_len > 0) {
                    uint64_t tx = sim_mix64(sp->seed ^
                        (sp->step * 0x9E3779B97F4A7C15ULL) ^
                        ((*delivered - 1) * 0xD6E8FEB86659FD93ULL) ^
                        0x7ECCA7E0C0117E01ULL);
                    size_t prefix_len = (size_t)(tx % (ctl_len + 1));

                    trace_fault_truncate(sp, from, to,
                                         MOQ_ACTION_SEND_CONTROL,
                                         prefix_len, ctl_len);

                    if (prefix_len > 0) {
                        moq_result_t rc = deliver_or_delay_control_chunk(
                            sp, to_session, ctl_data, prefix_len, from, to);
                        if (rc < 0) {
                            moq_action_cleanup(&actions[i]);
                            for (size_t j = i + 1; j < n; j++)
                                moq_action_cleanup(&actions[j]);
                            return rc;
                        }
                    }
                    moq_action_cleanup(&actions[i]);
                    continue;
                }

                if (sim_mutate_fires(sp, *delivered - 1) &&
                    ctl_len > 0 && ctl_len <= sizeof(mutate_buf)) {
                    memcpy(mutate_buf, ctl_data, ctl_len);
                    uint64_t bx = sim_mix64(sp->seed ^
                        (sp->step * 0x9E3779B97F4A7C15ULL) ^
                        ((*delivered - 1) * 0xD6E8FEB86659FD93ULL) ^
                        0xB17E1D0000000001ULL);
                    size_t byte_idx = bx % ctl_len;
                    uint64_t bi = sim_mix64(bx);
                    unsigned bit_idx = (unsigned)(bi % 8);
                    mutate_buf[byte_idx] ^= (1u << bit_idx);
                    ctl_data = mutate_buf;
                    trace_fault_mutate(sp, from, to,
                                       MOQ_ACTION_SEND_CONTROL,
                                       mutate_buf, ctl_len,
                                       byte_idx, bit_idx);
                }

                {
                    moq_result_t rc = deliver_or_delay_control(
                        sp, to_session, ctl_data, ctl_len,
                        *delivered - 1, from, to);
                    if (rc < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            } else if (actions[i].kind == MOQ_ACTION_SEND_DATA) {
                uint64_t rx_ref = 0;
                int dslot = -1;
                bool map_created = false;
                moq_result_t map_rc = sim_stream_map_get_or_create(
                    sp, actions[i].u.send_data.stream_ref._v, from,
                    &rx_ref, &dslot, &map_created);
                if (map_rc < 0) {
                    moq_action_cleanup(&actions[i]);
                    for (size_t j = i + 1; j < n; j++)
                        moq_action_cleanup(&actions[j]);
                    return map_rc;
                }
                moq_stream_ref_t ref = moq_stream_ref_from_u64(rx_ref);
                bool fin = actions[i].u.send_data.fin;
                moq_result_t rc;

                /* Truncation: deliver prefix then reset, both via delay helpers. */
                if (sim_truncate_data_fires(sp, *delivered - 1)) {
                    size_t hlen = actions[i].u.send_data.header_len;
                    size_t plen = actions[i].u.send_data.payload
                        ? moq_rcbuf_len(actions[i].u.send_data.payload) : 0;
                    if (plen <= SIZE_MAX - hlen) {
                        size_t total = hlen + plen;
                        uint64_t tx = sim_mix64(sp->seed ^
                            (sp->step * 0x9E3779B97F4A7C15ULL) ^
                            ((*delivered - 1) * 0xD6E8FEB86659FD93ULL) ^
                            0xDA7A7ECD7E01C001ULL);
                        size_t prefix_len = (size_t)(tx % (total + 1));

                        trace_fault_truncate(sp, from, to,
                                             MOQ_ACTION_SEND_DATA,
                                             prefix_len, total);

                        if (prefix_len > 0) {
                            size_t off = 0;
                            if (off < prefix_len && hlen > 0) {
                                size_t piece = hlen;
                                if (piece > prefix_len) piece = prefix_len;
                                rc = deliver_or_delay_data_chunk(
                                    sp, to_session, ref,
                                    actions[i].u.send_data.header, piece,
                                    false, dslot, from, to);
                                if (rc < 0) goto send_data_fail;
                                off += piece;
                            }
                            if (off < prefix_len && plen > 0) {
                                const uint8_t *pdata = moq_rcbuf_data(
                                    actions[i].u.send_data.payload);
                                size_t piece = prefix_len - off;
                                rc = deliver_or_delay_data_chunk(
                                    sp, to_session, ref,
                                    pdata, piece, false, dslot, from, to);
                                if (rc < 0) goto send_data_fail;
                            }
                        }

                        rc = deliver_or_delay_data_reset(
                            sp, to_session, ref, 0x1, dslot, from, to);
                        if (rc < 0) goto send_data_fail;

                        moq_action_cleanup(&actions[i]);
                        continue;
                    }
                }

                /* Data mutation: copy header+payload, flip one bit. */
                bool data_mutated = false;
                size_t mut_total = 0;
                if (sim_mutate_data_fires(sp, *delivered - 1)) {
                    size_t hlen = actions[i].u.send_data.header_len;
                    size_t plen = actions[i].u.send_data.payload
                        ? moq_rcbuf_len(actions[i].u.send_data.payload) : 0;
                    if (plen <= SIZE_MAX - hlen) {
                        mut_total = hlen + plen;
                        if (mut_total > 0 && mut_total <= sizeof(mutate_buf)) {
                            if (hlen > 0)
                                memcpy(mutate_buf,
                                       actions[i].u.send_data.header, hlen);
                            if (plen > 0)
                                memcpy(mutate_buf + hlen,
                                       moq_rcbuf_data(actions[i].u.send_data.payload),
                                       plen);
                            uint64_t bx = sim_mix64(sp->seed ^
                                (sp->step * 0x9E3779B97F4A7C15ULL) ^
                                ((*delivered - 1) * 0xD6E8FEB86659FD93ULL) ^
                                0xDA7A0F1100000001ULL);
                            size_t byte_idx = bx % mut_total;
                            uint64_t bi = sim_mix64(bx);
                            unsigned bit_idx = (unsigned)(bi % 8);
                            mutate_buf[byte_idx] ^= (1u << bit_idx);
                            trace_fault_mutate(sp, from, to,
                                MOQ_ACTION_SEND_DATA, mutate_buf,
                                mut_total, byte_idx, bit_idx);
                            data_mutated = true;
                        }
                    }
                }

                if (data_mutated) {
                    rc = deliver_or_delay_data(sp, to_session, ref,
                        mutate_buf, mut_total, NULL, 0,
                        fin, *delivered - 1, dslot, from, to);
                } else {
                    size_t hlen = actions[i].u.send_data.header_len;
                    const uint8_t *pdata = actions[i].u.send_data.payload
                        ? moq_rcbuf_data(actions[i].u.send_data.payload) : NULL;
                    size_t plen = actions[i].u.send_data.payload
                        ? moq_rcbuf_len(actions[i].u.send_data.payload) : 0;
                    rc = deliver_or_delay_data(sp, to_session, ref,
                        actions[i].u.send_data.header, hlen,
                        pdata, plen,
                        fin, *delivered - 1, dslot, from, to);
                }

                if (rc < 0) goto send_data_fail;

                if (0) {
                send_data_fail:
                    if (dslot >= 0) {
                        sp->stream_map[dslot].generation++;
                        if (map_created)
                            sp->stream_map[dslot].active = false;
                    }
                    moq_action_cleanup(&actions[i]);
                    for (size_t j = i + 1; j < n; j++)
                        moq_action_cleanup(&actions[j]);
                    return rc;
                }
            } else if (actions[i].kind == MOQ_ACTION_RESET_DATA) {
                int slot = sim_stream_map_find(
                    sp, actions[i].u.reset_data.stream_ref._v, from);
                if (slot >= 0) {
                    moq_stream_ref_t ref = moq_stream_ref_from_u64(
                        sp->stream_map[slot].receiver_ref);
                    moq_result_t rc = deliver_or_delay_data_reset(
                        sp, to_session, ref,
                        actions[i].u.reset_data.error_code,
                        slot, from, to);
                    if (rc < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            } else if (actions[i].kind == MOQ_ACTION_STOP_DATA) {
                uint64_t stop_ref = actions[i].u.stop_data.stream_ref._v;
                for (int j = 0; j < MOQ_SIM_MAX_DATA_STREAMS; j++) {
                    if (sp->stream_map[j].active &&
                        sp->stream_map[j].receiver_ref == stop_ref &&
                        sp->stream_map[j].sender != from) {
                        moq_stream_ref_t sender_ref = moq_stream_ref_from_u64(
                            sp->stream_map[j].sender_ref);
                        moq_perspective_t sender_persp = sp->stream_map[j].sender;
                        moq_session_t *sender_session =
                            (sender_persp == MOQ_PERSPECTIVE_CLIENT)
                                ? sp->client : sp->server;
                        moq_result_t rc = deliver_or_delay_data_stop(
                            sp, sender_session, sender_ref,
                            actions[i].u.stop_data.error_code,
                            j, from, sender_persp);
                        if (rc < 0) {
                            moq_action_cleanup(&actions[i]);
                            for (size_t j2 = i + 1; j2 < n; j2++)
                                moq_action_cleanup(&actions[j2]);
                            return rc;
                        }
                        break;
                    }
                }
            }
            else if (actions[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                int bslot = -1;
                for (int j = 0; j < MOQ_SIM_MAX_BIDI_STREAMS; j++) {
                    if (!sp->bidi_map[j].active) { bslot = j; break; }
                }
                if (bslot < 0) {
                    moq_action_cleanup(&actions[i]);
                    for (size_t j = i + 1; j < n; j++)
                        moq_action_cleanup(&actions[j]);
                    return MOQ_ERR_INTERNAL;
                }
                sp->bidi_map[bslot].active = true;
                sp->bidi_map[bslot].opener = from;
                sp->bidi_map[bslot].opener_ref =
                    actions[i].u.open_bidi_stream.stream_ref._v;
                sp->bidi_map[bslot].responder_ref = sp->next_bidi_ref++;
                sp->bidi_map[bslot].generation++;
                sp->bidi_map[bslot].last_due_us = 0;
                sp->bidi_map[bslot].opener_fin = false;
                sp->bidi_map[bslot].responder_fin = false;

                moq_stream_ref_t peer_ref = moq_stream_ref_from_u64(
                    sp->bidi_map[bslot].responder_ref);
                const uint8_t *bdata = actions[i].u.open_bidi_stream.data;
                size_t blen = actions[i].u.open_bidi_stream.len;
                bool bopen_fin = actions[i].u.open_bidi_stream.fin;
                {
                    /* The opener's open-with-FIN closes only its send half; the
                     * slot retires once the responder half also closes (both-halves
                     * tracking lives in deliver_or_delay_bidi). */
                    moq_result_t rc = deliver_or_delay_bidi(
                        sp, to_session, peer_ref, bdata, blen, bopen_fin,
                        *delivered - 1, bslot, from, to);
                    if (rc < 0) {
                        sp->bidi_map[bslot].active = false;
                        sp->bidi_map[bslot].generation++;
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            } else if (actions[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                uint64_t send_ref = actions[i].u.send_bidi_stream.stream_ref._v;
                moq_perspective_t opener_persp =
                    (from == MOQ_PERSPECTIVE_CLIENT) ?
                    MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
                /* A request bidi carries an ongoing conversation in both
                 * directions, so deliver to the peer (`to`) on the ref it reads,
                 * not always to the opener: a responder writes to the opener
                 * ref, an opener writes to the responder ref. */
                int bslot = sim_bidi_find_by_responder(sp, send_ref, opener_persp);
                moq_stream_ref_t peer_ref;
                if (bslot >= 0) {
                    peer_ref = moq_stream_ref_from_u64(
                        sp->bidi_map[bslot].opener_ref);
                } else {
                    bslot = sim_bidi_find_by_opener(sp, send_ref, from);
                    if (bslot < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return MOQ_ERR_INTERNAL;
                    }
                    peer_ref = moq_stream_ref_from_u64(
                        sp->bidi_map[bslot].responder_ref);
                }
                bool bfin = actions[i].u.send_bidi_stream.fin;
                const uint8_t *bdata = actions[i].u.send_bidi_stream.data;
                size_t blen = actions[i].u.send_bidi_stream.len;
                {
                    moq_result_t rc = deliver_or_delay_bidi(
                        sp, to_session, peer_ref, bdata, blen, bfin,
                        *delivered - 1, bslot, from, to);
                    if (rc < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            } else if (actions[i].kind == MOQ_ACTION_CLOSE_BIDI_STREAM) {
                /* CLOSE_BIDI is a graceful FIN of the closer's send half (matching
                 * the transport bridge's write(NULL,0,fin=true)), not a reset.
                 * Either end may close its half, so resolve the direction like
                 * SEND_BIDI: a responder writes to the opener ref, an opener writes
                 * to the responder ref. The slot retires once both halves close. */
                uint64_t close_ref = actions[i].u.close_bidi_stream.stream_ref._v;
                moq_perspective_t opener_persp =
                    (from == MOQ_PERSPECTIVE_CLIENT) ?
                    MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
                int bslot = sim_bidi_find_by_responder(sp, close_ref, opener_persp);
                moq_stream_ref_t peer_ref;
                if (bslot >= 0) {
                    peer_ref = moq_stream_ref_from_u64(
                        sp->bidi_map[bslot].opener_ref);
                } else {
                    bslot = sim_bidi_find_by_opener(sp, close_ref, from);
                    /* The closer may close after the peer already FIN'd and the
                     * slot retired (e.g. closing after a terminal response). That
                     * is a no-op: the bidi is already gone. */
                    if (bslot < 0) {
                        moq_action_cleanup(&actions[i]);
                        continue;
                    }
                    peer_ref = moq_stream_ref_from_u64(
                        sp->bidi_map[bslot].responder_ref);
                }
                {
                    moq_result_t rc = deliver_or_delay_bidi(
                        sp, to_session, peer_ref, NULL, 0, true,
                        *delivered - 1, bslot, from, to);
                    if (rc < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            }
            else if (actions[i].kind == MOQ_ACTION_RESET_BIDI_STREAM ||
                     actions[i].kind == MOQ_ACTION_STOP_BIDI_STREAM) {
                bool is_reset =
                    actions[i].kind == MOQ_ACTION_RESET_BIDI_STREAM;
                uint64_t tref = is_reset
                    ? actions[i].u.reset_bidi_stream.stream_ref._v
                    : actions[i].u.stop_bidi_stream.stream_ref._v;
                uint64_t terr = is_reset
                    ? actions[i].u.reset_bidi_stream.error_code
                    : actions[i].u.stop_bidi_stream.error_code;

                /* Resolve which end emitted the teardown, then deliver it to
                 * the peer. RESET_STREAM appears to the peer as a reset of our
                 * send half; STOP_SENDING asks the peer to stop sending on
                 * theirs. Either end of a request bidi may emit either. */
                moq_perspective_t opener_persp =
                    (from == MOQ_PERSPECTIVE_CLIENT) ?
                    MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
                int bslot = sim_bidi_find_by_responder(sp, tref, opener_persp);
                moq_stream_ref_t peer_ref;
                moq_perspective_t peer_persp;
                if (bslot >= 0) {
                    /* from is the responder; deliver to the opener. */
                    peer_persp = sp->bidi_map[bslot].opener;
                    peer_ref = moq_stream_ref_from_u64(
                        sp->bidi_map[bslot].opener_ref);
                } else {
                    bslot = sim_bidi_find_by_opener(sp, tref, from);
                    if (bslot >= 0) {
                        /* from is the opener; deliver to the responder. */
                        peer_persp = opener_persp;
                        peer_ref = moq_stream_ref_from_u64(
                            sp->bidi_map[bslot].responder_ref);
                    }
                }
                if (bslot >= 0) {
                    moq_session_t *peer_session =
                        (peer_persp == MOQ_PERSPECTIVE_CLIENT) ?
                        sp->client : sp->server;
                    moq_result_t rc = is_reset
                        ? deliver_or_delay_bidi_reset(
                              sp, peer_session, peer_ref, terr,
                              bslot, from, peer_persp)
                        : deliver_or_delay_bidi_stop(
                              sp, peer_session, peer_ref, terr,
                              bslot, from, peer_persp);
                    if (rc < 0) {
                        moq_action_cleanup(&actions[i]);
                        for (size_t j = i + 1; j < n; j++)
                            moq_action_cleanup(&actions[j]);
                        return rc;
                    }
                }
            }
            else if (actions[i].kind == MOQ_ACTION_SEND_DATAGRAM) {
                const uint8_t *dgdata = actions[i].u.send_datagram.data;
                size_t dglen = actions[i].u.send_datagram.len;
                moq_bytes_t bytes = { dgdata, dglen };
                moq_result_t rc = moq_session_on_datagram(
                    to_session, dgdata, dglen, sp->now_us);
                trace_input(sp, MOQ_SIM_INPUT_DATAGRAM, from, to,
                            bytes, rc);
                if (rc < 0) {
                    moq_action_cleanup(&actions[i]);
                    for (size_t j = i + 1; j < n; j++)
                        moq_action_cleanup(&actions[j]);
                    return rc;
                }
            }
            /* CLOSE_SESSION: traced by trace_action above but not
             * delivered to peer. The session API has no peer-close
             * input function. SimPair does not model transport
             * connection close; the session's own close event is
             * the observable signal for scenario runners. */

            moq_action_cleanup(&actions[i]);
        }
    }

    /* Injection phase: synthetic RESET/STOP for active streams. */
    if (sp->fault_enabled && sp->fault_per_mille > 0 &&
        (sp->fault_flags & (MOQ_SIM_FAULT_INJECT_RESET |
                            MOQ_SIM_FAULT_INJECT_STOP))) {
        bool can_reset = (sp->fault_flags & MOQ_SIM_FAULT_INJECT_RESET) != 0;
        bool can_stop  = (sp->fault_flags & MOQ_SIM_FAULT_INJECT_STOP) != 0;

        for (int j = 0; j < MOQ_SIM_MAX_DATA_STREAMS; j++) {
            if (!sp->stream_map[j].active ||
                sp->stream_map[j].sender != from ||
                sp->stream_map[j].terminal_pending)
                continue;

            uint64_t x = sp->seed;
            x ^= sp->step * 0x9E3779B97F4A7C15ULL;
            x ^= (uint64_t)j * 0xD6E8FEB86659FD93ULL;
            x ^= 0xA0761D6478BD642FULL;
            if ((sim_mix64(x) % 1000) >= sp->fault_per_mille)
                continue;

            bool do_reset;
            if (can_reset && can_stop)
                do_reset = (sim_mix64(x ^ 0x1ULL) & 1) != 0;
            else
                do_reset = can_reset;

            moq_perspective_t receiver_persp =
                (from == MOQ_PERSPECTIVE_CLIENT) ?
                MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
            moq_session_t *receiver_session =
                (receiver_persp == MOQ_PERSPECTIVE_CLIENT) ?
                sp->client : sp->server;

            if (do_reset) {
                moq_stream_ref_t ref = moq_stream_ref_from_u64(
                    sp->stream_map[j].receiver_ref);

                moq_sim_trace_record_t r;
                memset(&r, 0, sizeof(r));
                r.struct_size = sizeof(r);
                r.kind = MOQ_SIM_TRACE_FAULT_INJECT;
                r.seed = sp->seed;
                r.step = sp->step;
                r.now_us = sp->now_us;
                r.from = receiver_persp;
                r.to = receiver_persp;
                r.action_kind = MOQ_ACTION_RESET_DATA;
                r.code = 0x100;
                r.count = (size_t)j;
                trace_record(sp, &r);

                moq_result_t rc = deliver_or_delay_data_reset(
                    sp, receiver_session, ref, 0x100,
                    j, receiver_persp, receiver_persp);
                if (rc < 0) return rc;
                (*delivered)++;
            } else {
                moq_stream_ref_t sender_ref = moq_stream_ref_from_u64(
                    sp->stream_map[j].sender_ref);

                moq_sim_trace_record_t r;
                memset(&r, 0, sizeof(r));
                r.struct_size = sizeof(r);
                r.kind = MOQ_SIM_TRACE_FAULT_INJECT;
                r.seed = sp->seed;
                r.step = sp->step;
                r.now_us = sp->now_us;
                r.from = receiver_persp;
                r.to = from;
                r.action_kind = MOQ_ACTION_STOP_DATA;
                r.code = 0x100;
                r.count = (size_t)j;
                trace_record(sp, &r);

                moq_result_t rc = deliver_or_delay_data_stop(
                    sp, from_session, sender_ref, 0x100,
                    j, receiver_persp, from);
                if (rc < 0) return rc;
                (*delivered)++;
            }
        }
    }

    /* Close injection: feed a malformed control envelope (unknown
     * message type 0xFF, zero-length payload) to the target session.
     * This triggers close_with_error(0x3, "unknown control message
     * type") through the real public input path. */
    if (sp->fault_enabled && sp->fault_per_mille > 0 &&
        (sp->fault_flags & MOQ_SIM_FAULT_INJECT_CLOSE) &&
        moq_session_state(to_session) != MOQ_SESS_CLOSED) {
        uint64_t x = sp->seed;
        x ^= sp->step * 0x9E3779B97F4A7C15ULL;
        x ^= (uint64_t)from * 0x517CC1B727220A95ULL;
        if ((sim_mix64(x) % 1000) < sp->fault_per_mille) {
            static const uint8_t bad_msg[4] = {
                0x40, 0xFF,   /* QUIC varint 255 (unknown type) */
                0x00, 0x00    /* uint16 payload length = 0 */
            };
            moq_result_t rc = moq_session_on_control_bytes(
                to_session, bad_msg, sizeof(bad_msg), sp->now_us);

            moq_sim_trace_record_t r;
            memset(&r, 0, sizeof(r));
            r.struct_size = sizeof(r);
            r.kind = MOQ_SIM_TRACE_FAULT_INJECT;
            r.seed = sp->seed;
            r.step = sp->step;
            r.now_us = sp->now_us;
            r.from = from;
            r.to = to;
            r.action_kind = MOQ_ACTION_CLOSE_SESSION;
            r.code = 0x3;
            r.bytes.data = bad_msg;
            r.bytes.len = sizeof(bad_msg);
            r.result = rc;
            trace_record(sp, &r);

            trace_input(sp, MOQ_SIM_INPUT_CONTROL_BYTES,
                        from, to,
                        (moq_bytes_t){ bad_msg, sizeof(bad_msg) }, rc);

            (*delivered)++;
        }
    }

    return MOQ_OK;
}
