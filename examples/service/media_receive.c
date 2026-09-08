/*
 * media_receive — consumer-facing libmoq example (moq::service).
 *
 * Connects an endpoint to a MoQ relay, attaches a media receiver for a
 * namespace, prints catalog discovery (TRACK_ADDED / CATALOG_READY), and
 * polls media objects until interrupted or the endpoint closes.
 *
 * This is the high-level surface: only public headers are used and only
 * moq::service is linked. There is no session, subscriber, adapter, or
 * transport plumbing here -- the endpoint owns the network thread, version
 * negotiation, TLS, the catalog subscription, and object parsing.
 *
 * Usage: media_receive <url> <namespace> [track] [--insecure-skip-verify]
 *                      [--backend NAME] [--draft 16|18]
 *   url        moqt://host:port           (raw QUIC), or
 *              https://host:port/path     (WebTransport)
 *   namespace  slash-separated, e.g. "example" or "live/cam1"
 *   track      optional; informational only (the receiver auto-subscribes
 *              every catalog track)
 *   --insecure-skip-verify  disable TLS certificate verification; for
 *              LOCAL/self-signed testing ONLY (verification is on by default)
 *   --backend NAME  explicit endpoint backend: auto, picoquic, msquic, mvfst,
 *              proxygen, wtquic-msquic, or wtquic-network
 *   --draft N  pin the offered MoQ draft to exactly 16 or 18
 */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>

#include "../common/moq_example_term.h"

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Async-signal-safe: the handler only sets a flag. The poll loop uses a short
 * wait timeout so it notices g_stop promptly. (Do NOT call libmoq from a
 * signal handler -- moq_endpoint_set_interrupted is the right "unblock now"
 * hook, but it is not async-signal-safe; call it from a normal thread, e.g.
 * GStreamer unlock() or a VLC abort.) */
static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* Split "a/b/c" in place into borrowed namespace parts. Returns the count. */
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

static void print_usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s <url> <namespace> [track] [--insecure-skip-verify]\n"
        "       [--backend NAME] [--draft 16|18]\n"
        "  --backend NAME  auto|picoquic|msquic|mvfst|proxygen|"
        "wtquic-msquic|wtquic-network\n"
        "  --draft N       offer exactly draft 16 or 18; omit to negotiate\n"
        "  --insecure-skip-verify  disable TLS certificate verification\n"
        "                          (LOCAL/self-signed testing ONLY)\n",
        prog);
}

typedef struct backend_row {
    const char *name;
    moq_transport_backend_t backend;
    bool raw_quic;
    bool webtransport;
} backend_row_t;

static const backend_row_t k_backends[] = {
    { "auto",           MOQ_TRANSPORT_BACKEND_AUTO,           true,  true  },
    { "picoquic",       MOQ_TRANSPORT_BACKEND_PICOQUIC,       true,  true  },
    { "msquic",         MOQ_TRANSPORT_BACKEND_MSQUIC,         true,  false },
    { "mvfst",          MOQ_TRANSPORT_BACKEND_MVFST,          true,  false },
    { "proxygen",       MOQ_TRANSPORT_BACKEND_PROXYGEN,       false, true  },
    { "wtquic-msquic",  MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC,  false, true  },
    { "wtquic-network", MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK, false, true  },
};

static int parse_draft(const char *s)
{
    if (!s || !*s) return 0;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (*end != '\0' || (v != 16 && v != 18)) return -1;
    return (int)v;
}

static const backend_row_t *find_backend(const char *name)
{
    for (size_t i = 0; i < sizeof(k_backends) / sizeof(k_backends[0]); i++)
        if (strcmp(name, k_backends[i].name) == 0)
            return &k_backends[i];
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc == 2 &&
        (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0)) {
        print_usage(argv[0]);
        return 0;
    }
    if (argc < 3) {
        print_usage(argv[0]);
        return 2;
    }
    const char *url = argv[1];
    char nsbuf[256];
    snprintf(nsbuf, sizeof(nsbuf), "%s", argv[2]);
    moq_bytes_t ns_parts[32];
    size_t ns_count = split_namespace(nsbuf, ns_parts, 32);
    bool insecure_skip_verify = false;   /* TLS verification ON by default */
    const backend_row_t *backend = &k_backends[0];
    int draft_pin = 0;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--insecure-skip-verify") == 0) {
            insecure_skip_verify = true;
        } else if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            backend = find_backend(argv[++i]);
            if (!backend) {
                fprintf(stderr, "unknown backend: %s\n", argv[i]);
                print_usage(argv[0]);
                return 2;
            }
        } else if (strncmp(argv[i], "--backend=", 10) == 0) {
            backend = find_backend(argv[i] + 10);
            if (!backend) {
                fprintf(stderr, "unknown backend: %s\n", argv[i] + 10);
                print_usage(argv[0]);
                return 2;
            }
        } else if (strcmp(argv[i], "--draft") == 0 && i + 1 < argc) {
            draft_pin = parse_draft(argv[++i]);
            if (draft_pin < 0) {
                fprintf(stderr, "invalid draft: %s\n", argv[i]);
                print_usage(argv[0]);
                return 2;
            }
        } else if (strncmp(argv[i], "--draft=", 8) == 0) {
            draft_pin = parse_draft(argv[i] + 8);
            if (draft_pin < 0) {
                fprintf(stderr, "invalid draft: %s\n", argv[i] + 8);
                print_usage(argv[0]);
                return 2;
            }
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "unknown flag: %s\n", argv[i]);
            print_usage(argv[0]);
            return 2;
        } else {
            /* Other positionals (e.g. track) are informational here. */
        }
    }
    {
        bool is_wt = strncmp(url, "https://", 8) == 0;
        if (is_wt && !backend->webtransport) {
            fprintf(stderr, "backend %s is raw-QUIC only; use moqt://\n",
                    backend->name);
            return 2;
        }
        if (!is_wt && !backend->raw_quic) {
            fprintf(stderr, "backend %s is WebTransport only; use https://\n",
                    backend->name);
            return 2;
        }
    }

    signal(SIGINT, on_signal);

    /* 1. Open the endpoint. Protocol/backend are derived from the URL scheme;
     *    real certificate verification is the default. It is disabled only when
     *    the operator explicitly passes --insecure-skip-verify (local/self-signed
     *    test relays). Connection completes asynchronously. */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init_sized(&ec, sizeof(ec));
    ec.url.data = (const uint8_t *)url;
    ec.url.len = strlen(url);
    ec.backend = backend->backend;
    ec.insecure_skip_verify = insecure_skip_verify;
    if (draft_pin != 0) {
        static moq_version_t pinned;
        pinned = (moq_version_t)draft_pin;
        ec.versions.struct_size = sizeof(moq_version_offer_t);
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = &pinned;
        ec.versions.version_count = 1;
    }

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&ec, &ep);
    if (rc != MOQ_OK) {
        fprintf(stderr, "endpoint connect failed: %d (%s)\n",
                (int)rc, moq_strerror(rc));
        return 1;
    }

    /* 2. Attach a receiver (live preset: DROP_TO_KEYFRAME overflow). It owns
     *    the catalog subscription and auto-subscribes the catalog's tracks. */
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.endpoint = NULL;                 /* attach mode: we own ep above */
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = ns_count;
    rcfg.auto_subscribe = true;           /* simple player: take every track.
                                           * A selective consumer (FFmpeg/VLC/
                                           * GStreamer) sets this false, polls
                                           * TRACK_ADDED to build streams/pads,
                                           * then calls
                                           * moq_media_receiver_subscribe_track()
                                           * for the chosen tracks and
                                           * _unsubscribe_track() to toggle. */

    moq_media_receiver_t *rx = NULL;
    rc = moq_media_receiver_attach(ep, &rcfg, &rx);
    if (rc != MOQ_OK) {
        fprintf(stderr, "receiver attach failed: %d\n", (int)rc);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return 1;
    }

    /* 3. Drain discovery events and media objects from the app thread. Track
     *    events are drained before objects so every handle is known first. */
    unsigned long long objects = 0;
    while (!g_stop) {
        moq_media_track_event_t tev;
        while (moq_media_receiver_poll_track(rx, &tev, sizeof(tev)) == MOQ_OK) {
            if (tev.kind == MOQ_MEDIA_TRACK_ADDED && tev.desc) {
                /* Two distinct config bytes (both borrowed for the event --
                 * copy if retaining beyond this handler):
                 *   - desc.init.codec_config: DECODER EXTRADATA (SPS/PPS/VPS,
                 *     AAC ASC). Feed this to the decoder.
                 *   - desc.init_data: the full container/init segment (for CMAF
                 *     the ftyp+moov). Muxers/recorders want this, not decoders.
                 * desc.init_data is the *resolved* bytes regardless of how the
                 * catalog carried them: a CMAF track references a shared entry
                 * via MSF-01 initRef -> initDataList; RAW/LOC keeps init inline.
                 * Either way the receiver hands you the resolved segment here. */
                /* name and codec are PEER-CONTROLLED: escape before printing so
                 * a remote peer cannot inject terminal control sequences. */
                fputs("TRACK_ADDED name=", stdout);
                moq_example_term_fprint(stdout, tev.desc->name.data,
                                        tev.desc->name.len);
                fputs(" codec=", stdout);
                moq_example_term_fprint(stdout, tev.desc->codec.data,
                                        tev.desc->codec.len);
                printf(" codec_config(extradata)=%zu init_data(init segment)=%zu\n",
                       tev.desc->init.codec_config.len,
                       tev.desc->init_data.len);

                /* CMSF metadata (all optional; absent on a plain MSF catalog).
                 * SAP starting types (CMSF §3.5.2) describe random-access
                 * structure; libmoq surfaces them as advisory hints. */
                if (tev.desc->has_max_grp_sap || tev.desc->has_max_obj_sap) {
                    printf("  cmsf sap:");
                    if (tev.desc->has_max_grp_sap)
                        printf(" maxGrp=%u", tev.desc->max_grp_sap);
                    if (tev.desc->has_max_obj_sap)
                        printf(" maxObj=%u", tev.desc->max_obj_sap);
                    printf("\n");
                }

                /* Content protection (CMSF §4): the track lists refIDs; resolve
                 * each to its root DRM entry. libmoq carries this metadata only
                 * -- decryption is the application/CDM's job. */
                for (size_t ci = 0;
                     ci < tev.desc->content_protection_ref_id_count; ci++) {
                    moq_bytes_t ref = tev.desc->content_protection_ref_ids[ci];
                    const moq_cmsf_content_protection_t *cp =
                        moq_media_receiver_find_content_protection(rx, ref);
                    /* ref and scheme are PEER-CONTROLLED: escape before print. */
                    fputs("  cmsf protection ref=", stdout);
                    moq_example_term_fprint(stdout, ref.data, ref.len);
                    fputs(" scheme=", stdout);
                    if (cp)
                        moq_example_term_fprint(stdout, cp->scheme.data,
                                                cp->scheme.len);
                    printf(" kids=%zu%s\n",
                           cp ? cp->default_kid_count : 0,
                           cp ? "" : " (unresolved)");
                }
            } else if (tev.kind == MOQ_MEDIA_CATALOG_READY) {
                printf("CATALOG_READY\n");
            }
        }

        moq_media_object_t obj;
        moq_result_t prc = moq_media_receiver_poll_object(rx, &obj, sizeof(obj));
        if (prc == MOQ_OK) {
            /* Media bytes live in different places by packaging:
             *   - CMAF: the full fragment (container framing) is obj.fragment;
             *     the sample/media bytes are the mdat slice
             *     obj.fragment.data + obj.mdat_offset for obj.mdat_len. Decode
             *     the mdat slice; keep the fragment if you need the framing.
             *   - RAW/LOC/simple: the media bytes are obj.payload (fragment is
             *     empty). Do NOT read obj.payload for CMAF -- it is empty. */
            if (obj.packaging == MOQ_MEDIA_PACKAGING_CMAF) {
                printf("object CMAF %s pts=%llu media=%zu bytes "
                       "(mdat_offset=%zu) fragment=%zu bytes%s%s\n",
                       obj.keyframe ? "keyframe" : "delta",
                       (unsigned long long)obj.presentation_time_us,
                       obj.mdat_len, obj.mdat_offset, obj.fragment.len,
                       obj.datagram ? " datagram" : "",
                       obj.status != MOQ_OBJECT_NORMAL ? " status" : "");
            } else {
                printf("object RAW %s pts=%llu media=%zu bytes%s%s\n",
                       obj.keyframe ? "keyframe" : "delta",
                       (unsigned long long)obj.presentation_time_us,
                       obj.payload.len,
                       obj.datagram ? " datagram" : "",
                       obj.status != MOQ_OBJECT_NORMAL ? " status" : "");
            }
            moq_media_object_cleanup(&obj);   /* releases the transferred refs */
            objects++;
            continue;
        }
        if (prc == MOQ_ERR_INTERRUPTED)
            break;
        if (prc == MOQ_ERR_CLOSED) {
            printf("endpoint closed\n");
            break;
        }
        /* MOQ_DONE: nothing queued -- block until the next event/wakeup. */
        moq_result_t wr = moq_media_receiver_wait(rx, 200000);
        if (wr == MOQ_ERR_INTERRUPTED || wr == MOQ_ERR_CLOSED)
            break;
    }

    printf("received %llu objects\n", objects);

    /* 4. Teardown: destroy the child before stopping/destroying the endpoint. */
    moq_media_receiver_destroy(rx);
    moq_endpoint_stop(ep);
    moq_endpoint_destroy(ep);
    return 0;
}
