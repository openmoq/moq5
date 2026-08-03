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

    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
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
