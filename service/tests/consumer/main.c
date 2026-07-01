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

#include <stddef.h>

int main(void)
{
    /* Endpoint cfg: init must stamp the ABI guard. */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    if (ec.struct_size != sizeof(moq_endpoint_cfg_t))
        return 1;

    /* Receiver cfg (live preset): forces an overflow policy, stamps struct_size. */
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    if (rcfg.struct_size != sizeof(moq_media_receiver_cfg_t))
        return 2;

    /* Sender cfg (live preset). */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    if (scfg.struct_size != sizeof(moq_media_sender_cfg_t))
        return 3;

    /* Per-track subscribe cfg: the manual-subscription surface. */
    moq_media_receiver_track_subscribe_cfg_t tcfg;
    moq_media_receiver_track_subscribe_cfg_init(&tcfg);
    if (tcfg.struct_size != sizeof(moq_media_receiver_track_subscribe_cfg_t))
        return 4;
    if (tcfg.start != MOQ_MEDIA_START_CURRENT)
        return 5;

    return 0;
}
