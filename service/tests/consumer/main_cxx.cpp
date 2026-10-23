/*
 * C++17 public-header consumer for the libmoq service tier.
 *
 * Proves the PUBLIC service headers compile, link, and run from plain C++17
 * (extern "C" linkage, designated-initializer-free, no C-only constructs
 * leaking through) -- the mirror of the C consumer in main.c. Init-only:
 * no network, no certs, no connect.
 */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/publisher.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

/* Open kind sets remain fixed-width across C and C++ consumers. */
static_assert(std::is_same<moq_media_packaging_t, uint32_t>::value,
              "packaging is uint32_t");
static_assert(std::is_same<moq_media_drop_reason_t, uint32_t>::value,
              "drop reason is uint32_t");
static_assert(std::is_same<moq_msf_ext_kind_t, uint32_t>::value,
              "extension kind is uint32_t");
static_assert(MOQ_MEDIA_PACKAGING_RAW == 1u && MOQ_MEDIA_PACKAGING_CMAF == 2u &&
              MOQ_MEDIA_PACKAGING_MPEG2TS == 3u, "packaging values");
static_assert(MOQ_MEDIA_DROP_MALFORMED_LOC == 1u &&
              MOQ_MEDIA_DROP_KEYFRAME_WAIT == 7u &&
              MOQ_MEDIA_DROP_MALFORMED_MPEG2TS == 8u, "drop reason values");
#include <cstring>

/* Complete receiver config before the MPEG2TS and companion tails were
 * appended. */
struct receiver_cfg_v0 {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
};

struct track_cfg_v0 {
    uint32_t struct_size;
    moq_media_start_mode_t start;
    bool has_priority;
    uint8_t priority;
};

static moq_tls_certificate_verdict_t consumerVerifyCertificate(
    void *ctx, moq_bytes_t serverName,
    const moq_bytes_t *certificates, std::size_t certificateCount)
{
    (void)ctx; (void)serverName; (void)certificates; (void)certificateCount;
    return MOQ_TLS_CERTIFICATE_REJECT;
}

int main()
{
    /* The pointer-only endpoint init stamps the FROZEN v0 prefix (the
     * WebTransport wire-profile knob is an appended, struct_size-gated
     * tail); the sized init covers the full current struct. */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    if (ec.struct_size == 0 || ec.struct_size > sizeof(moq_endpoint_cfg_t))
        return 1;
    moq_endpoint_cfg_init_sized(&ec, sizeof(ec));
    if (ec.struct_size != sizeof(moq_endpoint_cfg_t))
        return 1;
    /* The appended tail fields are reachable from C++ through the same public
     * headers: defaults are zero/no pointers, and assignable fields remain
     * assignable after the SETUP-token tail was inserted before trust. */
    if (ec.wt_profile != static_cast<uint32_t>(MOQ_WT_PROFILE_BACKEND_DEFAULT))
        return 1;
    if (ec.handshake_timeout_us != 0)
        return 1;
    if (ec.setup_auth_tokens != nullptr || ec.setup_auth_token_count != 0)
        return 1;
    ec.handshake_timeout_us = 5000000ull;
    if (ec.handshake_timeout_us != 5000000ull)
        return 1;
    if (ec.verify_certificate != nullptr || ec.verify_certificate_ctx != nullptr)
        return 1;
    ec.verify_certificate = consumerVerifyCertificate;
    if (ec.verify_certificate != consumerVerifyCertificate)
        return 1;

    moq_media_auth_cfg_t auth;
    moq_media_auth_cfg_init(&auth);
    if (auth.struct_size != sizeof(auth) || auth.select != nullptr ||
        auth.tokens != nullptr || auth.on_outcome != nullptr)
        return 1;

    /* Exact frozen stamp, with no write into the appended tails. */
    moq_media_receiver_cfg_t rcfg;
    std::memset(&rcfg, 0xa5, sizeof(rcfg));
    moq_media_receiver_cfg_init_live(&rcfg);
    if (rcfg.struct_size != MOQ_MEDIA_RECEIVER_CFG_V0_SIZE ||
        rcfg.struct_size != sizeof(receiver_cfg_v0) ||
        rcfg.overflow.policy != MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME)
        return 2;
    for (size_t i = sizeof(receiver_cfg_v0); i < sizeof(rcfg); ++i)
        if (reinterpret_cast<const unsigned char *>(&rcfg)[i] != 0xa5)
            return 2;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    if (rcfg.struct_size != sizeof(rcfg) || rcfg.overflow.policy != MOQ_MEDIA_OVERFLOW_UNSET ||
        rcfg.companion_profile != MOQ_MEDIA_COMPANION_AUTO)
        return 2;
    moq_media_receiver_cfg_init_flow_control_sized(&rcfg, sizeof(rcfg));
    if (rcfg.struct_size != sizeof(rcfg) || rcfg.overflow.policy != MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
        return 2;
    moq_media_receiver_cfg_init_live_sized(&rcfg, sizeof(rcfg));
    if (rcfg.struct_size != sizeof(rcfg) || rcfg.overflow.policy != MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME)
        return 2;
    rcfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    if (rcfg.companion_profile != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return 2;
    rcfg.mpeg2ts_reorder_slots = 32;
    rcfg.mpeg2ts_reorder_window_us = 200000;
    rcfg.mpeg2ts_strip_prefix = true;
    rcfg.mpeg2ts_join_psi = true;
    moq_media_receiver_cfg_init_sized(&rcfg, sizeof(rcfg));
    moq_media_receiver_cfg_init_flow_control_sized(&rcfg, sizeof(rcfg));
    if (rcfg.mpeg2ts_strip_prefix)
        return 2;

    moq_media_receiver_track_subscribe_cfg_t sub;
    moq_media_receiver_track_subscribe_cfg_init(&sub);
    if (sub.struct_size != MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE)
        return 12;
    moq_media_receiver_track_subscribe_cfg_init_sized(&sub, sizeof(sub));
    if (sub.struct_size != sizeof(sub) || sub.mpeg2ts_program_number != 0)
        return 12;
    sub.mpeg2ts_program_number = 1;

    moq_media_track_desc_t desc = {};
    if (moq_msf_ext_mpeg2ts(desc.extensions, desc.extension_count) != nullptr ||
        desc.has_alt_group)
        return 13;
    moq_media_object_t obj = {};
    if (obj.has_loss_report || obj.gap || obj.gap_tail_unknown ||
        obj.gap_duration_known || obj.discontinuous ||
        obj.gap_group != 0 || obj.gap_object != 0 ||
        obj.gap_duration_27mhz != 0)
        return 14;
    moq_media_receiver_stats_t rst = {};
    if (rst.gaps_reported != 0 || rst.gap_tails_unknown != 0 ||
        rst.reorder_refused != 0)
        return 15;

    /* Pointer-only sender preset: stamps the FROZEN v0 prefix (a non-zero
     * prefix of the current struct, never more). */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    if (scfg.struct_size == 0 ||
        scfg.struct_size > sizeof(moq_media_sender_cfg_t))
        return 3;

    /* Sized presets stamp the full struct and enable the appended fields --
     * the documented C++ push-sender flow. */
    moq_media_sender_cfg_init_live_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 4;
    scfg.publish_tracks = true;
    scfg.drop_without_demand = true;
    moq_media_sender_cfg_init_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 5;
    moq_media_sender_cfg_init_lossless_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 6;

    moq_media_sender_callbacks_t scb;
    moq_media_sender_callbacks_init(&scb);
    if (scb.struct_size == 0 ||
        scb.struct_size > sizeof(moq_media_sender_callbacks_t))
        return 7;
    moq_media_sender_callbacks_init_sized(&scb, sizeof(scb));
    if (scb.struct_size != sizeof(moq_media_sender_callbacks_t))
        return 8;


    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    if (oc.struct_size != sizeof(oc)) return 10;
    oc.end_of_group = true;
    moq_pub_begin_object_cfg_t bo;
    moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
    if (bo.struct_size != sizeof(bo)) return 11;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    if (tc.struct_size != sizeof(moq_media_track_cfg_t))
        return 9;
    moq_msf_mpeg2ts_t rec;
    moq_msf_mpeg2ts_init(&rec);
    rec.has_mode = true;
    rec.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
    rec.has_packet_size = true;
    rec.packet_size = 188;
    moq_msf_track_ext_t ext = {};
    ext.kind = MOQ_MSF_EXT_MPEG2TS;
    ext.u.mpeg2ts = &rec;
    tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    tc.extensions = &ext;
    tc.extension_count = 1;
    tc.mpeg2ts_app_objects = false;
    tc.mpeg2ts_object_packets = 7;
    tc.mpeg2ts_min_group_ms = 0;
    tc.mpeg2ts_forward_before_random_access = false;
    if (moq_msf_ext_mpeg2ts(tc.extensions, tc.extension_count) != &rec)
        return 9;

    static const uint8_t pkt[188] = { 0x47 };
    moq_media_send_mpeg2ts_t ts_in = {};
    ts_in.struct_size = sizeof(ts_in);
    ts_in.data.data = pkt;
    ts_in.data.len = sizeof(pkt);
    ts_in.end_object = true;
    size_t consumed = 99;
    if (moq_media_sender_write_mpeg2ts(nullptr, nullptr, &ts_in, &consumed) !=
            MOQ_ERR_INVAL || consumed != 0)
        return 16;
    moq_media_sender_track_stats_t tst = {};
    if (moq_media_sender_get_track_stats(nullptr, nullptr, &tst, sizeof(tst)) !=
        MOQ_ERR_INVAL)
        return 17;

    moq_media_receiver_track_subscribe_cfg_t receive;
    moq_media_receiver_track_subscribe_cfg_init(&receive);
    if (receive.struct_size != sizeof(track_cfg_v0) ||
        receive.start != MOQ_MEDIA_START_CURRENT)
        return 12;
    moq_media_receiver_track_subscribe_cfg_init_sized(&receive, sizeof(receive));
    if (receive.struct_size != sizeof(receive) || receive.location || receive.fill)
        return 12;

    return 0;
}
