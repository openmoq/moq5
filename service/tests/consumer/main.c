/*
 * pkg-config consumer for the libmoq service tier.
 *
 * Consumes the service exactly as a Meson/autotools/VLC-style downstream would:
 * built with ONLY `pkg-config --cflags --libs libmoq-service` from an installed
 * prefix -- no CMake, no source-tree includes. Includes only public service
 * headers and calls init-only APIs (no network, no certs, no connect).
 */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/publisher.h>

#include <stddef.h>
#include <stdint.h>

int main(void)
{
    /* Endpoint cfg ABI: the pointer-only init stamps ONLY the frozen v0 prefix
     * (so it can never overflow an old caller); the sized init stamps the full
     * struct so the appended wt_profile tail field is usable. */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    if (ec.struct_size != MOQ_ENDPOINT_CFG_V0_SIZE)
        return 1;

    /* The new WebTransport wire-profile API, exercised through the INSTALLED
     * public headers: reachable via the sized init, defaults to BACKEND_DEFAULT
     * (the zero value -- selects nothing), and accepts an explicit compat
     * dialect. */
    moq_endpoint_cfg_t ecs;
    moq_endpoint_cfg_init_sized(&ecs, sizeof(ecs));
    if (ecs.struct_size != sizeof(moq_endpoint_cfg_t))
        return 1;
    if (ecs.wt_profile != (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT)
        return 1;
    ecs.wt_profile = (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT;
    if (ecs.wt_profile != (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT)
        return 1;

    /* The handshake bound, the next appended tail field, reached the same way:
     * the sized init defaults it to 0 (leave the backend's own value), and a
     * caller can set a real microsecond bound. */
    if (ecs.handshake_timeout_us != 0)
        return 1;
    ecs.handshake_timeout_us = 5000000ull;
    if (ecs.handshake_timeout_us != 5000000ull)
        return 1;

    /* Receiver cfg (live preset): forces an overflow policy, stamps struct_size. */
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    if (rcfg.struct_size != sizeof(moq_media_receiver_cfg_t))
        return 2;

    /* Sender cfg (live preset). The pointer-only initializer stamps the
     * FROZEN v0 prefix (it cannot know the caller's storage size), so the
     * stamp is a non-zero prefix of the current struct -- never more. */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    if (scfg.struct_size == 0 || scfg.struct_size > sizeof(moq_media_sender_cfg_t))
        return 3;

    /* Sized presets stamp the caller's full struct and enable the appended
     * fields (the documented push-sender flow). */
    moq_media_sender_cfg_init_live_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 31;
    scfg.publish_tracks = true;
    scfg.drop_without_demand = true;
    moq_media_sender_cfg_init_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 32;
    moq_media_sender_cfg_init_lossless_sized(&scfg, sizeof(scfg));
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 33;

    /* Sender callbacks: frozen pointer-only init vs sized init. */
    moq_media_sender_callbacks_t scb;
    moq_media_sender_callbacks_init(&scb);
    if (scb.struct_size == 0 ||
        scb.struct_size > sizeof(moq_media_sender_callbacks_t))
        return 34;
    moq_media_sender_callbacks_init_sized(&scb, sizeof(scb));
    if (scb.struct_size != sizeof(moq_media_sender_callbacks_t))
        return 35;


    /* Core publisher cfg initializers (frozen pointer + sized forms). */
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init(&oc);
    if (oc.struct_size == 0 || oc.struct_size > sizeof(oc)) return 40;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    if (oc.struct_size != sizeof(oc)) return 41;
    oc.end_of_group = true;
    moq_pub_begin_object_cfg_t bo;
    moq_pub_begin_object_cfg_init_sized(&bo, sizeof(bo));
    if (bo.struct_size != sizeof(bo)) return 42;
    /* Per-track subscribe cfg: the manual-subscription surface. */
    moq_media_receiver_track_subscribe_cfg_t tcfg;
    moq_media_receiver_track_subscribe_cfg_init(&tcfg);
    if (tcfg.struct_size != sizeof(moq_media_receiver_track_subscribe_cfg_t))
        return 4;
    if (tcfg.start != MOQ_MEDIA_START_CURRENT)
        return 5;

    /* Wake/wait contract surface: NULL-safe forms only (no network), so the
     * installed prefix proves both new public symbols link and behave. */
    moq_endpoint_wake(NULL);                       /* documented no-op */
    if (moq_media_sender_wait(NULL, 0) != MOQ_ERR_INVAL)
        return 6;

    return 0;
}
