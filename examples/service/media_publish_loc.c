/*
 * media_publish_loc — publish a real H.264 elementary stream over moq::service
 * using RAW/LOC packaging. POC companion to media_send.c.
 *
 * Unlike media_send.c (placeholder payloads), this reads an H.264 Annex-B
 * byte-stream from stdin (e.g. piped from ffmpeg), splits it into access units,
 * derives the decoder config (avcC) from the first SPS/PPS, advertises a video
 * track, and writes each access unit as a RAW/LOC media object. The service
 * generates the LOC-01 properties (Capture Timestamp from presentation_time_us,
 * Video Frame Marking independent=is_sync) that a LOC player (e.g. moq-playa)
 * consumes via WebCodecs.
 *
 * The stdin stream MUST carry an Access Unit Delimiter (NAL type 9) at the start
 * of every access unit so AUs can be split cheaply; ffmpeg inserts these with
 *   -bsf:v h264_metadata=aud=insert
 *
 * Usage: media_publish_loc <url> <namespace> [track]
 *   url        moqt://host:port[/path]  (raw QUIC), or https://host:port/path (WT)
 *   namespace  slash-separated, e.g. "demo"
 *   track      track name (default "video")
 *
 * Example:
 *   ffmpeg -re -stream_loop -1 -f lavfi -i testsrc=size=1280x720:rate=30 \
 *     -c:v libx264 -profile:v baseline -pix_fmt yuv420p -g 30 -keyint_min 30 \
 *     -x264-params scenecut=0 -bsf:v h264_metadata=aud=insert -f h264 pipe:1 \
 *   | ./moq_example_media_publish_loc moqt://localhost:4433/moq-relay demo video
 */
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Video geometry advertised in the catalog. Match these to the ffmpeg source
 * (testsrc=size=WxH:rate=FPS). Width/height are informational hints; the player
 * also derives geometry from the in-band SPS. */
#define VIDEO_WIDTH    1280
#define VIDEO_HEIGHT   720
#define VIDEO_FPS      30
#define VIDEO_BITRATE  2000000ull
#define FRAME_DUR_US   (1000000ull / VIDEO_FPS)

static volatile sig_atomic_t g_stop = 0;
static void on_signal(int sig) { (void)sig; g_stop = 1; }

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

/* -- H.264 Annex-B helpers ------------------------------------------------- */

/* Is there a 3- or 4-byte start code at b[i]? Sets *scl to its length. */
static int is_start_code(const uint8_t *b, size_t len, size_t i, int *scl)
{
    if (i + 4 <= len && b[i] == 0 && b[i + 1] == 0 && b[i + 2] == 0 &&
        b[i + 3] == 1) { *scl = 4; return 1; }
    if (i + 3 <= len && b[i] == 0 && b[i + 1] == 0 && b[i + 2] == 1) {
        *scl = 3; return 1; }
    return 0;
}

/* Index of the next start code (at/after `from`) whose NAL type is `want`,
 * or -1 if none is found within the buffer. */
static long find_nal(const uint8_t *b, size_t len, size_t from, int want)
{
    for (size_t i = from; i + 4 < len; i++) {
        int scl;
        if (is_start_code(b, len, i, &scl)) {
            size_t h = i + scl;
            if (h < len && (b[h] & 0x1F) == want)
                return (long)i;
            i += scl - 1;   /* skip the start code we just matched */
        }
    }
    return -1;
}

/* Captured parameter sets (NAL bytes incl. header, no start code). */
static uint8_t g_sps[256]; static size_t g_sps_len = 0;
static uint8_t g_pps[256]; static size_t g_pps_len = 0;
static bool    g_have_params = false;

/* Walk the NALs of one access unit: set *has_idr if it contains an IDR slice
 * (NAL type 5), and capture the first SPS (7) and PPS (8) we ever see. */
static void scan_au(const uint8_t *b, size_t len, int *has_idr)
{
    size_t i = 0;
    while (i < len) {
        int scl;
        if (!is_start_code(b, len, i, &scl)) { i++; continue; }
        size_t h = i + scl;
        if (h >= len) break;
        int type = b[h] & 0x1F;

        /* End of this NAL = next start code, or end of AU. */
        size_t nal_end = len;
        for (size_t j = h + 1; j + 3 <= len; j++) {
            int s2;
            if (is_start_code(b, len, j, &s2)) { nal_end = j; break; }
        }
        size_t nal_len = nal_end - h;   /* bytes incl. NAL header */

        if (type == 5) *has_idr = 1;
        if (type == 7 && g_sps_len == 0 && nal_len <= sizeof(g_sps)) {
            memcpy(g_sps, b + h, nal_len); g_sps_len = nal_len;
        }
        if (type == 8 && g_pps_len == 0 && nal_len <= sizeof(g_pps)) {
            memcpy(g_pps, b + h, nal_len); g_pps_len = nal_len;
        }
        i = nal_end;
    }
    if (g_sps_len >= 4 && g_pps_len > 0) g_have_params = true;
}

/* Build a minimal AVCDecoderConfigurationRecord (avcC) from SPS+PPS. */
static size_t build_avcc(uint8_t *out, size_t cap)
{
    size_t need = 8 + g_sps_len + 3 + g_pps_len;
    if (g_sps_len < 4 || need > cap) return 0;
    size_t p = 0;
    out[p++] = 0x01;            /* configurationVersion */
    out[p++] = g_sps[1];        /* AVCProfileIndication */
    out[p++] = g_sps[2];        /* profile_compatibility */
    out[p++] = g_sps[3];        /* AVCLevelIndication */
    out[p++] = 0xFF;            /* 6 bits reserved + lengthSizeMinusOne (=3) */
    out[p++] = 0xE1;            /* 3 bits reserved + numOfSPS (=1) */
    out[p++] = (uint8_t)(g_sps_len >> 8);
    out[p++] = (uint8_t)(g_sps_len & 0xFF);
    memcpy(out + p, g_sps, g_sps_len); p += g_sps_len;
    out[p++] = 0x01;            /* numOfPPS */
    out[p++] = (uint8_t)(g_pps_len >> 8);
    out[p++] = (uint8_t)(g_pps_len & 0xFF);
    memcpy(out + p, g_pps, g_pps_len); p += g_pps_len;
    return p;
}

/* -- Publish state --------------------------------------------------------- */

typedef struct {
    moq_media_sender_t *tx;
    moq_media_track_t  *trk;
    bool                track_added;
    uint64_t            frame_idx;
    unsigned long long  sent;
    unsigned long long  dropped;
} pub_state_t;

/* Add the video track once SPS/PPS are known (avcC -> init_data). */
static int ensure_track(pub_state_t *st, const char *track)
{
    if (st->track_added) return 0;
    if (!g_have_params) return 0;

    uint8_t avcc[600];
    size_t avcc_len = build_avcc(avcc, sizeof(avcc));
    if (avcc_len == 0) {
        fprintf(stderr, "failed to build avcC\n");
        return -1;
    }
    char codec[32];
    snprintf(codec, sizeof(codec), "avc1.%02x%02x%02x",
             g_sps[1], g_sps[2], g_sps[3]);

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name.data = (const uint8_t *)track;
    tc.name.len = strlen(track);
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;   /* RAW/LOC: service makes LOC */
    tc.codec.data = (const uint8_t *)codec;
    tc.codec.len = strlen(codec);
    tc.init_data.data = avcc;                  /* copied during add_track */
    tc.init_data.len = avcc_len;
    tc.width = VIDEO_WIDTH;
    tc.height = VIDEO_HEIGHT;
    tc.framerate_millis = (uint64_t)VIDEO_FPS * 1000;
    tc.bitrate = VIDEO_BITRATE;
    tc.is_live = true;

    moq_result_t rc = moq_media_sender_add_track(st->tx, &tc, &st->trk);
    if (rc != MOQ_OK) {
        fprintf(stderr, "add_track failed: %d\n", (int)rc);
        return -1;
    }
    st->track_added = true;
    fprintf(stderr, "track added: name=%s codec=%s avcC=%zuB (sps=%zu pps=%zu)\n",
            track, codec, avcc_len, g_sps_len, g_pps_len);
    return 0;
}

/* Emit one access unit (Annex-B bytes) as a RAW object.
 * The browser uses WebCodecs in Annex-B mode (no description). */
static int emit_au(pub_state_t *st, const char *track,
                   const uint8_t *au, size_t au_len)
{
    int has_idr = 0;
    scan_au(au, au_len, &has_idr);

    if (ensure_track(st, track) != 0) return -1;
    if (!st->track_added) return 0;

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), au, au_len, &payload) != MOQ_OK)
        return -1;

    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = payload;
    o.is_sync = has_idr ? true : false;
    o.starts_group = o.is_sync;       /* keyframe opens a new group (GOP) */
    o.presentation_time_us = st->frame_idx * FRAME_DUR_US;
    o.decode_time_us = o.presentation_time_us;

    moq_result_t wr = moq_media_sender_write(st->tx, st->trk, &o);
    if (wr == MOQ_OK) {
        st->sent++;
        st->frame_idx++;
    } else if (wr == MOQ_ERR_WOULD_BLOCK) {
        moq_rcbuf_decref(payload);    /* dropped by the live policy */
        st->dropped++;
        st->frame_idx++;
    } else if (wr == MOQ_ERR_INTERRUPTED || wr == MOQ_ERR_CLOSED) {
        moq_rcbuf_decref(payload);
        return -1;
    } else {
        moq_rcbuf_decref(payload);
        fprintf(stderr, "write failed: %d\n", (int)wr);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <url> <namespace> [track]\n", argv[0]);
        return 2;
    }
    const char *url = argv[1];
    char nsbuf[256];
    snprintf(nsbuf, sizeof(nsbuf), "%s", argv[2]);
    moq_bytes_t ns_parts[32];
    size_t ns_count = split_namespace(nsbuf, ns_parts, 32);
    const char *track = (argc > 3) ? argv[3] : "video";

    signal(SIGINT, on_signal);

    /* 1. Pre-read stdin until we have SPS+PPS (first IDR AU). This ensures
     *    the track config (avcC) is known BEFORE we connect to the relay, so
     *    the initial catalog published after ANNOUNCE_OK already contains the
     *    video track — avoiding an empty first-catalog race. */
    uint8_t *pb = NULL;
    size_t pb_len = 0, pb_cap = 0;
    uint8_t chunk[65536];

    fprintf(stderr, "pre-reading stdin for SPS/PPS...\n");
    while (!g_stop && !g_have_params) {
        size_t got = fread(chunk, 1, sizeof(chunk), stdin);
        if (got == 0) { if (feof(stdin) || ferror(stdin)) break; continue; }
        if (pb_len + got > pb_cap) {
            size_t ncap = (pb_len + got) * 2 + 131072;
            uint8_t *np = (uint8_t *)realloc(pb, ncap);
            if (!np) { free(pb); fprintf(stderr, "oom\n"); return 1; }
            pb = np; pb_cap = ncap;
        }
        memcpy(pb + pb_len, chunk, got);
        pb_len += got;
        /* Scan all complete AUs so far to extract SPS/PPS. */
        size_t scan = 0;
        long a;
        while ((a = find_nal(pb, pb_len, scan, 9)) >= 0) {
            long b = find_nal(pb, pb_len, (size_t)a + 4, 9);
            if (b < 0) break;
            int dummy = 0; scan_au(pb + a, (size_t)(b - a), &dummy);
            scan = (size_t)b;
        }
    }
    if (!g_have_params) {
        free(pb);
        fprintf(stderr, "failed to get SPS/PPS from stdin\n");
        return 1;
    }
    fprintf(stderr, "SPS/PPS found, connecting to relay...\n");

    /* 2. Open the endpoint (raw QUIC for moqt://, WebTransport for https://). */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url.data = (const uint8_t *)url;
    ec.url.len = strlen(url);
    ec.insecure_skip_verify = true;   /* demo only */

    moq_endpoint_t *ep = NULL;
    moq_result_t rc = moq_endpoint_connect(&ec, &ep);
    if (rc != MOQ_OK) {
        free(pb); fprintf(stderr, "endpoint connect failed: %d\n", (int)rc);
        return 1;
    }

    /* 3. Attach a sender (live preset: DROP_TO_KEYFRAME). */
    moq_media_sender_cfg_t scfg;
    moq_media_sender_cfg_init_live(&scfg);
    scfg.endpoint = NULL;                 /* attach mode: we own ep above */
    scfg.namespace_.parts = ns_parts;
    scfg.namespace_.count = ns_count;

    moq_media_sender_t *tx = NULL;
    rc = moq_media_sender_attach(ep, &scfg, &tx);
    if (rc != MOQ_OK) {
        free(pb); fprintf(stderr, "sender attach failed: %d\n", (int)rc);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return 1;
    }

    pub_state_t st;
    memset(&st, 0, sizeof(st));
    st.tx = tx;

    /* Add the track immediately — before the network thread has a chance to
     * process ANNOUNCE_OK and publish the initial catalog. g_have_params is
     * already true from the pre-read phase. */
    if (ensure_track(&st, track) != 0) {
        free(pb);
        moq_media_sender_destroy(tx);
        moq_endpoint_stop(ep);
        moq_endpoint_destroy(ep);
        return 1;
    }

    /* 4. Read Annex-B from stdin (pb already holds the pre-read bytes from
     *    phase 1). Continue splitting on AUDs and emitting. */
    bool eof = false;

    while (!g_stop && !eof) {
        uint8_t chunk2[65536];
        size_t got = fread(chunk2, 1, sizeof(chunk2), stdin);
        if (got == 0) {
            if (feof(stdin)) eof = true;
            else if (ferror(stdin)) break;
        }
        if (got > 0) {
            if (pb_len + got > pb_cap) {
                size_t ncap = (pb_len + got) * 2;
                uint8_t *np = (uint8_t *)realloc(pb, ncap);
                if (!np) { fprintf(stderr, "oom\n"); break; }
                pb = np; pb_cap = ncap;
            }
            memcpy(pb + pb_len, chunk2, got);
            pb_len += got;
        }

        /* Find the first AU start (AUD). */
        long au_start = find_nal(pb, pb_len, 0, 9);
        if (au_start < 0) {
            if (eof) break;
            continue;   /* need more bytes */
        }

        /* Emit each complete AU [au_start, next_aud). */
        size_t search = (size_t)au_start + 4;
        long next;
        while ((next = find_nal(pb, pb_len, search, 9)) >= 0) {
            if (emit_au(&st, track, pb + au_start, (size_t)(next - au_start)) != 0) {
                g_stop = 1; break;
            }
            au_start = next;
            search = (size_t)au_start + 4;
        }

        if (eof && !g_stop) {
            emit_au(&st, track, pb + au_start, pb_len - (size_t)au_start);
            pb_len = 0;
        } else if (au_start > 0) {
            /* Keep the in-progress AU; drop everything before it. */
            memmove(pb, pb + au_start, pb_len - (size_t)au_start);
            pb_len -= (size_t)au_start;
        }
    }

    free(pb);

    if (st.track_added)
        moq_media_sender_end_track(tx, st.trk);

    fprintf(stderr, "done: sent=%llu dropped=%llu\n", st.sent, st.dropped);

    /* 4. Teardown: child first, then the endpoint. */
    moq_media_sender_destroy(tx);
    moq_endpoint_stop(ep);
    moq_endpoint_destroy(ep);
    return 0;
}
