/*
 * media_send — consumer-facing libmoq example (moq::service).
 *
 * Connects an endpoint to a MoQ relay, attaches a media sender, advertises a
 * namespace, adds one video track (the service derives + publishes the MSF
 * catalog), and writes media objects. Public headers only; links moq::service.
 *
 * IMPORTANT: the payloads here are placeholder bytes, NOT a real codec
 * bitstream. This example demonstrates the service SEND LIFECYCLE -- connect,
 * attach, add track, write in decode order with typed timing, clean teardown
 * -- and the service generates the LOC-01 property block from the typed
 * fields. A real sender supplies encoded access units (e.g. H.264 AUs) as the
 * payload; everything else stays the same.
 *
 * Usage: media_send <url> <namespace> [track] [--insecure-skip-verify]
 *   url        moqt://host:port (raw QUIC) or https://host:port/path (WT)
 *   namespace  slash-separated, e.g. "example"
 *   track      track name (default "video")
 *   --insecure-skip-verify  disable TLS certificate verification; for
 *              LOCAL/self-signed testing ONLY (verification is on by default)
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* Async-signal-safe: the handler only sets a flag. The send loop checks it
 * each iteration. (Do NOT call libmoq from a signal handler --
 * moq_endpoint_set_interrupted is the "unblock now" hook but is not
 * async-signal-safe; call it from a normal thread.) */
static volatile sig_atomic_t g_stop = 0;

enum { ENDPOINT_DRAIN_TIMEOUT_US = 5000000 };

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static size_t split_namespace(char *buf, moq_bytes_t *parts, size_t max)
{
    size_t n = 0;
    char *p = buf;
    while (*p && n < max) {
        char *slash = strchr(p, '/');
        if (slash)
            *slash = '\0';
        parts[n].data = (const uint8_t *)p;
        parts[n].len = strlen(p);
        n++;
        if (!slash)
            break;
        p = slash + 1;
    }
    return n;
}

/* The placeholder payload is a static buffer that outlives every object, so
 * the rcbuf release callback has nothing to free. A real integration would
 * release/unref its own access-unit buffer here. */
static void payload_release(void *ctx, const uint8_t *data, size_t len)
{
    (void)ctx;
    (void)data;
    (void)len;
}

static void drain_before_stop(moq_endpoint_t *ep)
{
    moq_result_t dr = moq_endpoint_drain(ep, ENDPOINT_DRAIN_TIMEOUT_US);
    if (dr == MOQ_DONE) {
        fprintf(stderr, "endpoint drain timed out; stopping anyway\n");
    } else if (dr == MOQ_ERR_UNSUPPORTED) {
        fprintf(stderr,
                "endpoint drain unsupported by this backend; stopping\n");
    } else if (dr != MOQ_OK && dr != MOQ_ERR_CLOSED) {
        fprintf(stderr, "endpoint drain failed: %d; stopping\n", (int)dr);
    }
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr,
            "usage: %s <url> <namespace> [track] [--insecure-skip-verify]\n"
            "  --insecure-skip-verify  disable TLS certificate verification\n"
            "                          (LOCAL/self-signed testing ONLY)\n",
            argv[0]);
        return 2;
    }
    const char *url = argv[1];
    char nsbuf[256];
    snprintf(nsbuf, sizeof(nsbuf), "%s", argv[2]);
    moq_bytes_t ns_parts[32];
    size_t ns_count = split_namespace(nsbuf, ns_parts, 32);
    const char *track = "video";
    bool insecure_skip_verify = false;   /* TLS verification ON by default */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--insecure-skip-verify") == 0)
            insecure_skip_verify = true;
        else
            track = argv[i];
    }

    signal(SIGINT, on_signal);

    /* 1. Open the endpoint (see media_receive.c for the connect details). */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url.data = (const uint8_t *)url;
    ec.url.len = strlen(url);
    /* TLS certificate verification is ON by default; disable it only when the
     * operator explicitly opts in via --insecure-skip-verify (local/self-signed
     * testing). */
    ec.insecure_skip_verify = insecure_skip_verify;

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&ec, &ep);
    if (rc != MOQ_OK) {
        fprintf(stderr, "endpoint connect failed: %d\n", (int)rc);
        return 1;
    }

    /* 2. Attach a sender (live preset: DROP_TO_KEYFRAME, never blocks the
     *    encoder). It advertises the namespace and publishes the catalog. */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    scfg.endpoint = NULL;                 /* attach mode: we own ep above */
    scfg.namespace_.parts = ns_parts;
    scfg.namespace_.count = ns_count;

    moq_media_sender_t *tx = NULL;
    rc = moq_media_sender_attach(ep, &scfg, &tx);
    if (rc != MOQ_OK) {
        fprintf(stderr, "sender attach failed: %d\n", (int)rc);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return 1;
    }

    /* 3. Add a track before readiness. The service builds the catalog entry. */
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name.data = (const uint8_t *)track;
    tc.name.len = strlen(track);
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;     /* RAW/LOC: service makes LOC */
    tc.codec.data = (const uint8_t *)"avc1.42e01e";
    tc.codec.len = 11;
    /* MSF-01 5.2.22: audio/video tracks require a (maximum) bitrate in bits/s.
     * Placeholder value for this example. */
    tc.bitrate = 1500000;

    /* Decoder init config. A real sender passes the encoder's extradata here
     * -- e.g. H.264/HEVC SPS/PPS/VPS, an AAC AudioSpecificConfig, or a CMAF
     * init segment. How the service publishes it depends on packaging:
     *   - RAW/LOC (this example): the init bytes are base64'd INLINE on the
     *     catalog track entry.
     *   - CMAF: the init segment is published in the catalog's MSF-01
     *     initDataList and the track points at it via initRef. The emitter
     *     deduplicates: tracks whose CMAF init segment bytes are identical
     *     share one initDataList entry, whose id is the name of the first
     *     track that introduced that init segment.
     * Either way the receiver resolves it to desc.init_data at TRACK_ADDED; it
     * is NOT per-object LOC metadata. These are placeholder bytes (the payloads
     * below are placeholders too); leave init_data empty if the codec carries
     * its parameter sets in-band. The buffer is copied during add_track. */
    static const uint8_t example_init_data[] = { 0x01, 0x42, 0xe0, 0x1e };
    tc.init_data.data = example_init_data;
    tc.init_data.len = sizeof(example_init_data);

    /* For a CMAF track (tc.packaging = MOQ_MEDIA_PACKAGING_CMAF, init_data = a
     * real ftyp+moov init segment) the service validates each written CMAF
     * object against CMSF §3.3/§3.4 -- matching the object track_ID to the init
     * segment and rejecting a group-start object whose first sample is known not
     * to be a SAP. This is STRICT BY DEFAULT (the cfg_init* initializers set
     * scfg.validate_cmaf = true); set scfg.validate_cmaf = false after init for
     * deliberate passthrough. It is a no-op for RAW/LOC (this example). */

    tc.is_live = true;

    moq_media_track_t *trk = NULL;
    rc = moq_media_sender_add_track(tx, &tc, &trk);
    if (rc != MOQ_OK) {
        fprintf(stderr, "add_track failed: %d\n", (int)rc);
        moq_media_sender_destroy(tx);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return 1;
    }

    /* 4. Write objects in decode order. write() transfers the payload rcbuf on
     *    MOQ_OK; on any non-OK return the caller keeps it. A keyframe opens a
     *    new group (GOP-per-group); deltas extend it. ~30 fps for 10 seconds. */
    static const uint8_t frame[1024] = {0};   /* placeholder access unit */
    uint64_t pts = 0;
    unsigned long long sent = 0;
    for (int i = 0; i < 300 && !g_stop; i++) {
        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_wrap(moq_alloc_default(), frame, sizeof(frame),
                           payload_release, NULL, &payload) != MOQ_OK)
            break;

        bool keyframe = (i % 30 == 0);
        moq_media_send_object_t o;
        memset(&o, 0, sizeof(o));
        o.struct_size = sizeof(o);
        o.payload = payload;
        o.is_sync = keyframe;
        o.starts_group = keyframe;
        o.presentation_time_us = pts;
        o.decode_time_us = pts;

        moq_result_t wr = moq_media_sender_write(tx, trk, &o);
        if (wr == MOQ_OK) {
            sent++;
            pts += 33333;
        } else if (wr == MOQ_ERR_WOULD_BLOCK) {
            moq_rcbuf_decref(payload);    /* dropped by the live policy */
        } else if (wr == MOQ_ERR_INTERRUPTED || wr == MOQ_ERR_CLOSED) {
            moq_rcbuf_decref(payload);
            break;
        } else {
            moq_rcbuf_decref(payload);
            fprintf(stderr, "write failed: %d\n", (int)wr);
            break;
        }

        struct timespec ts = { 0, 33333000 };   /* ~30 fps pacing */
        nanosleep(&ts, NULL);
    }

    printf("wrote %llu objects\n", sent);

    /* 5. Teardown: child first, then a bounded local stream flush before the
     * endpoint's abrupt stop. The drain waits for bytes already queued in the
     * transport to leave local stream queues; it is not an ACK or peer-consumed
     * guarantee. */
    moq_media_sender_destroy(tx);
    drain_before_stop(ep);
    moq_endpoint_stop(ep);
    moq_endpoint_destroy(ep);
    return 0;
}
