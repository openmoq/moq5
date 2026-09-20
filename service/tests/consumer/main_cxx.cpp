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
#include <moq/subscriber.h>

#include <cstddef>
#include <cstring>

/* Independent original layouts keep tail-padding regressions visible. */
struct old_receiver_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
};
struct old_join_cfg {
    uint32_t struct_size;
    moq_sub_track_t *track;
    bool relative;
    uint64_t joining_start;
    moq_group_order_t group_order;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
};
struct old_update_cfg {
    uint32_t struct_size;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    bool has_forward;
    bool forward;
    bool has_delivery_timeout;
    uint64_t delivery_timeout_us;
};
static_assert(offsetof(moq_media_receiver_cfg_t, request_auth) >= sizeof(old_receiver_cfg));
static_assert(offsetof(moq_sub_joining_fetch_cfg_t, auth_tokens) >= sizeof(old_join_cfg));

static bool receiver_abi()
{
    void (*old_init[])(moq_media_receiver_cfg_t *) = {
        moq_media_receiver_cfg_init, moq_media_receiver_cfg_init_live,
        moq_media_receiver_cfg_init_flow_control};
    void (*sized_init[])(moq_media_receiver_cfg_t *, size_t) = {
        moq_media_receiver_cfg_init_sized, moq_media_receiver_cfg_init_live_sized,
        moq_media_receiver_cfg_init_flow_control_sized};
    for (size_t preset = 0; preset < 3; ++preset) {
        alignas(moq_media_receiver_cfg_t) unsigned char bytes[sizeof(moq_media_receiver_cfg_t) + 8];
        std::memset(bytes, 0xa5, sizeof(bytes));
        auto *cfg = reinterpret_cast<moq_media_receiver_cfg_t *>(bytes);
        old_init[preset](cfg);
        if (cfg->struct_size != sizeof(old_receiver_cfg)) return false;
        for (size_t i = sizeof(old_receiver_cfg); i < sizeof(bytes); ++i)
            if (bytes[i] != 0xa5) return false;
        for (size_t n = 0; n <= sizeof(bytes); ++n) {
            std::memset(bytes, 0xa5, sizeof(bytes));
            sized_init[preset](cfg, n);
            size_t written = n < sizeof(uint32_t) ? 0 :
                n < sizeof(*cfg) ? n : sizeof(*cfg);
            if (written && cfg->struct_size != written) return false;
            for (size_t i = written; i < sizeof(bytes); ++i)
                if (bytes[i] != 0xa5) return false;
            if (n >= sizeof(*cfg) && cfg->request_auth != nullptr) return false;
        }
    }
    alignas(moq_sub_joining_fetch_cfg_t) unsigned char bytes[sizeof(moq_sub_joining_fetch_cfg_t) + 8];
    std::memset(bytes, 0xa5, sizeof(bytes));
    auto *cfg = reinterpret_cast<moq_sub_joining_fetch_cfg_t *>(bytes);
    moq_sub_joining_fetch_cfg_init(cfg);
    if (cfg->struct_size != sizeof(old_join_cfg)) return false;
    for (size_t i = sizeof(old_join_cfg); i < sizeof(bytes); ++i)
        if (bytes[i] != 0xa5) return false;
    moq_sub_joining_fetch_cfg_init_sized(cfg, sizeof(*cfg));
    if (cfg->struct_size != sizeof(*cfg) || cfg->auth_tokens != nullptr ||
        cfg->auth_token_count != 0) return false;
    alignas(moq_sub_update_cfg_t) unsigned char ub[sizeof(moq_sub_update_cfg_t) + 8];
    std::memset(ub, 0xa5, sizeof(ub));
    auto *update = reinterpret_cast<moq_sub_update_cfg_t *>(ub);
    moq_sub_update_cfg_init(update);
    if (update->struct_size != sizeof(old_update_cfg)) return false;
    for (size_t i = sizeof(old_update_cfg); i < sizeof(ub); ++i)
        if (ub[i] != 0xa5) return false;
    return true;
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
     * headers: both default to their zero value (no selection / the backend's
     * own handshake value) and both are assignable. */
    if (ec.wt_profile != static_cast<uint32_t>(MOQ_WT_PROFILE_BACKEND_DEFAULT))
        return 1;
    if (ec.handshake_timeout_us != 0)
        return 1;
    ec.handshake_timeout_us = 5000000ull;
    if (ec.handshake_timeout_us != 5000000ull)
        return 1;

    moq_media_receiver_cfg_t rcfg;
    if (!receiver_abi()) return 12;
    moq_media_receiver_cfg_init_live_sized(&rcfg, sizeof(rcfg));
    if (rcfg.struct_size != sizeof(moq_media_receiver_cfg_t))
        return 2;

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

    return 0;
}
