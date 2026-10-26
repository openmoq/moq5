/*
 * media_player — consumer-facing libmoq example (moq::service).
 *
 * Plays a namespace from a relay through moq_media_player: the player
 * subscribes to the catalog, selects tracks, orders and times their media,
 * and hands out decoder-ready access units. This is the API to start from
 * when an application wants to play RAW/LOC media; media_receive.c shows the
 * lower-level receiver objects instead (any packaging, CMAF/CMSF included).
 *
 * What an application does with the outputs, this example does without a
 * decoder:
 *   CONFIGURE      (re)configure the track's decoder: codec string and
 *                  extradata (avcC, AudioSpecificConfig, ...);
 *   TIMELINE       map the track's media time onto the local clock;
 *   UNIT           decode one access unit, in decode order, then present it
 *                  at its presentation time on that timeline;
 *   DISCONTINUITY  flush the decoder before the next unit;
 *   END            the track is over; its cause and disposition say how.
 * READY, END and "receiver failed" lines carry t_ms, milliseconds on this program's
 * monotonic clock since it started. A NOTICE line, on the same clock, says
 * when this loop first saw the receiver's track state ended or failed: the
 * receiver's state, sampled once per pass (a pass waits up to 20 ms, longer
 * if the process is delayed), not the wire arrival of the peer's terminal
 * notice. A CLOCK line at start pairs that clock with the wall clock, so a
 * fault timed elsewhere can be placed on it, approximately.
 * Here each output is printed and checked (CONFIGURE before the track's first
 * unit, its timeline's TIMELINE before the timeline's first unit -- tracks of
 * one render group share a timeline and its one TIMELINE -- units in decode
 * order, END last), and with --dump the encoded units are
 * written to files. Dumping encoded units is not rendering: an application
 * feeds them to its decoder and renderer.
 *
 * Threading: the player is confined to one owner thread. This program's
 * main thread is that owner: it alone calls wait, tick, poll_output,
 * poll_event, handle_feedback and destroy. The signal handler only sets a
 * flag the loop reads.
 *
 * Usage: media_player <url> <namespace> [--draft 16|18|22] [--seconds N]
 *            [--track NAME]... [--dump DIR] [--finite] [--expect-tracks N]
 *            [--decoder-stall AT_S,FOR_MS] [--verbose]
 *            [--insecure-skip-verify]
 *   url        moqt://host:port[/path] (raw QUIC) or https://host:port/path (WT)
 *   namespace  slash-separated, e.g. "demo"
 *   --draft    offer exactly this MoQ draft (default: every supported draft)
 *   --seconds  stop after N seconds (default 30)
 *   --track    play these tracks (MANUAL selection, each on its own timeline
 *              unless the catalog groups them); default: AUTO selection
 *   --dump DIR write per track, to files it creates (an existing file is an
 *              error, so DIR should be fresh):
 *                <track>.bin       the units' bytes, in output order (for
 *                                  RAW/LOC H.264, an Annex-B stream);
 *                <track>.units     one line per unit, "offset len dts_us
 *                                  pts_us generation independent group
 *                                  object", locating it in <track>.bin and
 *                                  in the track, so units without
 *                                  in-band framing (raw AAC, Opus) can be
 *                                  split again; "#configure" lines record
 *                                  each configuration and its packaging;
 *                <track>.config.N  the extradata of configuration generation N.
 *              Tracks whose names sanitize alike get a numeric suffix; a
 *              failed open, write, flush or close fails the run.
 *   --finite   succeed only if every track ends by its publisher's
 *              END_OF_TRACK with every location settled (END cause
 *              PEER_END_OF_TRACK, disposition COMPLETE) before the deadline;
 *              the run stops once every track has ended, and reaching the
 *              deadline is a failure. With --expect-tracks N, exactly N
 *              tracks must be selected and end so.
 *   --decoder-stall AT_S,FOR_MS
 *              AT_S seconds after the first unit, report a full decoder queue
 *              for every track for FOR_MS milliseconds (DECODER_QUEUE
 *              feedback), then an empty one: the player pauses release and
 *              resumes.
 *   --verbose  print every unit and every dropped object
 *   --insecure-skip-verify  disable TLS certificate verification; for
 *              LOCAL/self-signed testing ONLY (verification is on by default)
 * Exit status: 0 when every selected track produced units and every check
 * held (and, with --finite, ended as above); 1 otherwise; 2 for bad usage.
 */
#include <moq/endpoint.h>
#include <moq/media_player.h>

#include "../common/moq_example_term.h"

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static uint64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static uint64_t g_start_us;   /* now_us() at start: the t_ms origin */

static double t_ms(void)
{
    return (double)(now_us() - g_start_us) / 1000.0;
}

static void sleep_us(uint64_t us)
{
    struct timespec ts = { (time_t)(us / 1000000u), (long)(us % 1000000u) * 1000 };
    nanosleep(&ts, NULL);
}

static size_t split_namespace(char *buf, moq_bytes_t *parts, size_t max)
{
    size_t n = 0;
    char *p = buf;
    while (*p && n < max) {
        char *slash = strchr(p, '/');
        if (slash) *slash = '\0';
        parts[n].data = (const uint8_t *)p;
        parts[n].len = strlen(p);
        n++;
        if (!slash) break;
        p = slash + 1;
    }
    return n;
}

/* -- Per-track accounting and dump files ---------------------------------- */

typedef struct {
    moq_media_track_t *track;
    char     name[64];      /* sanitized and unique: the dump file stem */
    bool     configured, ended, order_ok;
    bool     noticed;       /* NOTICE printed */
    uint64_t configures, units, discontinuities, bytes;
    uint64_t generation;    /* the latest output's configuration_generation */
    uint32_t packaging;     /* from CONFIGURE */
    uint64_t last_dts, last_group, last_object;
    bool     has_last_dts, has_last_location;
    uint32_t end_cause, end_disposition;
    char     base[512];     /* DIR/<name>; empty without --dump */
    FILE    *bin, *units_log;
    uint64_t bin_offset;
    bool     io_failed;
} track_tally_t;

enum { MAX_TRACKS = 8 };
static track_tally_t g_tracks[MAX_TRACKS];
static size_t g_track_count;

/* Timelines whose TIMELINE output has been seen. A render group's tracks
 * share one timeline, mapped by one TIMELINE before its first unit. */
static uint32_t g_timelines[MAX_TRACKS];
static size_t g_timeline_count;

static bool timeline_seen(uint32_t id)
{
    for (size_t i = 0; i < g_timeline_count; i++)
        if (g_timelines[i] == id) return true;
    return false;
}

static void io_fail(track_tally_t *t, const char *what)
{
    if (!t->io_failed) fprintf(stderr, "dump: %s failed for %s\n", what, t->base);
    t->io_failed = true;
}

/* Close a dump file; a failed flush of buffered writes or a failed close
 * fails the track. */
static void close_dump(track_tally_t *t, FILE **f)
{
    if (!*f) return;
    if (fflush(*f) != 0) io_fail(t, "flush");
    if (fclose(*f) != 0) io_fail(t, "close");
    *f = NULL;
}

static bool name_taken(const char *name)
{
    for (size_t i = 0; i < g_track_count; i++)
        if (strcmp(g_tracks[i].name, name) == 0) return true;
    return false;
}

static void print_name(moq_media_track_t *t)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    if (d) moq_example_term_fprint(stdout, d->name.data, d->name.len);
    else fputs("?", stdout);
}

static bool track_named(moq_media_track_t *t, const char *name)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    return d && d->name.len == strlen(name) &&
           memcmp(d->name.data, name, d->name.len) == 0;
}

static track_tally_t *tally(moq_media_track_t *t, const char *dump_dir)
{
    for (size_t i = 0; i < g_track_count; i++)
        if (g_tracks[i].track == t) return &g_tracks[i];
    if (g_track_count == MAX_TRACKS) return NULL;
    track_tally_t *x = &g_tracks[g_track_count];
    memset(x, 0, sizeof(*x));
    x->track = t;
    x->order_ok = true;
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    char name[48] = "track";
    if (d && d->name.len > 0 && d->name.len < sizeof(name)) {
        for (size_t i = 0; i < d->name.len; i++) {
            char c = (char)d->name.data[i];
            name[i] = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '-' || c == '_' ? c : '_';
        }
        name[d->name.len] = '\0';
    }
    char unique[sizeof(x->name)];
    snprintf(unique, sizeof(unique), "%s", name);
    for (unsigned n = 1; name_taken(unique); n++)
        snprintf(unique, sizeof(unique), "%s-%u", name, n);
    memcpy(x->name, unique, sizeof(unique));
    g_track_count++;
    if (dump_dir) {
        int w = snprintf(x->base, sizeof(x->base), "%s/%s", dump_dir, unique);
        if (w < 0 || (size_t)w >= sizeof(x->base)) {
            io_fail(x, "path");
            x->base[0] = '\0';
            return x;
        }
        char path[600];
        snprintf(path, sizeof(path), "%s.bin", x->base);
        x->bin = fopen(path, "wbx");
        if (!x->bin) io_fail(x, "create .bin");
        snprintf(path, sizeof(path), "%s.units", x->base);
        x->units_log = fopen(path, "wx");
        if (!x->units_log) io_fail(x, "create .units");
        else if (fputs("# offset len dts_us pts_us generation independent group object\n",
                       x->units_log) == EOF)
            io_fail(x, "write .units");
    }
    return x;
}

static void dump_configure(track_tally_t *t, const moq_media_player_output_t *o)
{
    if (!t->base[0] || t->io_failed) return;
    char path[600];
    snprintf(path, sizeof(path), "%s.config.%llu", t->base,
             (unsigned long long)o->configuration_generation);
    FILE *cf = fopen(path, "wbx");
    if (!cf) {
        io_fail(t, "create .config");
        return;
    }
    const moq_rcbuf_t *cc = o->u.configure.codec_config;
    size_t n = cc ? moq_rcbuf_len(cc) : 0;
    if (n && fwrite(moq_rcbuf_data(cc), 1, n, cf) != n) io_fail(t, "write .config");
    close_dump(t, &cf);
    const moq_rcbuf_t *codec = o->u.configure.codec;
    if (fprintf(t->units_log, "#configure %llu packaging=%s codec=%.*s config_len=%zu "
                "samplerate=%u channels=%u width=%u height=%u\n",
                (unsigned long long)o->configuration_generation,
                o->u.configure.packaging == MOQ_MEDIA_PACKAGING_CMAF ? "cmaf" : "raw",
                codec ? (int)moq_rcbuf_len(codec) : 0,
                codec ? (const char *)moq_rcbuf_data(codec) : "", n,
                o->u.configure.samplerate, o->u.configure.channel_count,
                o->u.configure.width, o->u.configure.height) < 0)
        io_fail(t, "write .units");
}

static void dump_unit(track_tally_t *t, const moq_media_player_output_t *o)
{
    if (!t->base[0] || t->io_failed || !o->u.unit.payload) return;
    size_t len = o->u.unit.len;
    if (fwrite(moq_rcbuf_data(o->u.unit.payload) + o->u.unit.offset, 1, len,
               t->bin) != len) {
        io_fail(t, "write .bin");
        return;
    }
    if (fprintf(t->units_log, "%llu %zu %llu %llu %llu %d %llu %llu\n",
                (unsigned long long)t->bin_offset, len,
                (unsigned long long)o->u.unit.decode_time_us,
                (unsigned long long)o->u.unit.presentation_time_us,
                (unsigned long long)o->configuration_generation,
                o->u.unit.independence_known ? (int)o->u.unit.independent : -1,
                (unsigned long long)o->u.unit.group_id,
                (unsigned long long)o->u.unit.object_id) < 0)
        io_fail(t, "write .units");
    t->bin_offset += len;
}

static bool all_ended(void)
{
    for (size_t i = 0; i < g_track_count; i++)
        if (!g_tracks[i].ended) return false;
    return g_track_count > 0;
}

/* -- Outputs --------------------------------------------------------------- */

static void handle_output(const moq_media_player_output_t *o, const char *dump_dir,
                          bool verbose)
{
    track_tally_t *t = tally(o->track, dump_dir);
    if (!t) return;
    t->generation = o->configuration_generation;
    switch (o->kind) {
    case MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE: {
        const moq_rcbuf_t *codec = o->u.configure.codec;
        t->configured = true;
        t->configures++;
        t->packaging = o->u.configure.packaging;
        fputs("CONFIGURE ", stdout);
        print_name(o->track);
        printf(" generation=%llu codec=", (unsigned long long)o->configuration_generation);
        if (codec) moq_example_term_fprint(stdout, moq_rcbuf_data(codec), moq_rcbuf_len(codec));
        printf(" config=%zuB", o->u.configure.codec_config
                                   ? moq_rcbuf_len(o->u.configure.codec_config) : 0);
        if (o->u.configure.width)
            printf(" %ux%u", o->u.configure.width, o->u.configure.height);
        if (o->u.configure.samplerate)
            printf(" %uHz/%uch", o->u.configure.samplerate, o->u.configure.channel_count);
        printf("\n");
        dump_configure(t, o);
        break;
    }
    case MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE:
        if (!timeline_seen(o->timeline_id) && g_timeline_count < MAX_TRACKS)
            g_timelines[g_timeline_count++] = o->timeline_id;
        fputs("TIMELINE ", stdout);
        print_name(o->track);
        printf(" id=%u\n", o->timeline_id);
        break;
    case MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY:
        t->discontinuities++;
        t->has_last_dts = false;
        fputs("DISCONTINUITY ", stdout);
        print_name(o->track);
        printf(" reason=%u\n", (unsigned)o->u.discontinuity.reason);
        break;
    case MOQ_MEDIA_PLAYER_OUTPUT_UNIT:
        /* A decoder takes units in decode order, which is the order of their
         * locations (group, then object); presentation order may differ
         * (B-frames). CMAF's decode time is a decode clock and never
         * decreases; RAW/LOC's copies the media time, which may. */
        if (!t->configured || !timeline_seen(o->timeline_id) || t->ended)
            t->order_ok = false;
        if (t->has_last_location &&
            (o->u.unit.group_id < t->last_group ||
             (o->u.unit.group_id == t->last_group && o->u.unit.object_id <= t->last_object)))
            t->order_ok = false;
        t->last_group = o->u.unit.group_id;
        t->last_object = o->u.unit.object_id;
        t->has_last_location = true;
        if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF && t->has_last_dts &&
            o->u.unit.decode_time_us < t->last_dts)
            t->order_ok = false;
        t->last_dts = o->u.unit.decode_time_us;
        t->has_last_dts = true;
        t->units++;
        t->bytes += o->u.unit.len;
        if (verbose) {
            fputs("UNIT ", stdout);
            print_name(o->track);
            printf(" g=%llu o=%llu dts=%llu pts=%llu len=%zu%s\n",
                   (unsigned long long)o->u.unit.group_id,
                   (unsigned long long)o->u.unit.object_id,
                   (unsigned long long)o->u.unit.decode_time_us,
                   (unsigned long long)o->u.unit.presentation_time_us, o->u.unit.len,
                   o->u.unit.independent ? " key" : "");
        }
        dump_unit(t, o);
        break;
    case MOQ_MEDIA_PLAYER_OUTPUT_END:
        t->ended = true;
        t->end_cause = o->u.end.cause;
        t->end_disposition = o->u.end.disposition;
        fputs("END ", stdout);
        print_name(o->track);
        printf(" cause=%u disposition=%u t_ms=%.1f\n", t->end_cause, t->end_disposition,
               t_ms());
        break;
    default:
        break;   /* a kind this example does not know: cleanup still returns its refs */
    }
}

/* -- Decoder queue feedback ------------------------------------------------ */

/* Report a decoder queue depth for every track that has output: above
 * max_recommended the player pauses release, below it release resumes. */
static void report_decoder_queue(moq_media_player_t *p, uint32_t depth, uint64_t now)
{
    for (size_t i = 0; i < g_track_count; i++) {
        if (g_tracks[i].ended) continue;
        moq_media_player_feedback_t fb;
        moq_media_player_feedback_init(&fb);
        fb.kind = MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE;
        fb.track = g_tracks[i].track;
        fb.configuration_generation = g_tracks[i].generation;
        fb.u.decoder_queue.depth = depth;
        fb.u.decoder_queue.max_recommended = 4;
        moq_result_t rc = moq_media_player_handle_feedback(p, &fb, now);
        if (rc != MOQ_OK)
            fprintf(stderr, "feedback for %s: %s\n", g_tracks[i].name, moq_strerror(rc));
    }
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "usage: %s <url> <namespace> [--draft 16|18|22] [--seconds N]\n"
        "       [--track NAME]... [--dump DIR] [--finite] [--expect-tracks N]\n"
        "       [--decoder-stall AT_S,FOR_MS] [--verbose] [--insecure-skip-verify]\n",
        prog);
}

int main(int argc, char **argv)
{
    g_start_us = now_us();
    if (argc < 3) {
        usage(argv[0]);
        return 2;
    }
    const char *url = argv[1];
    char nsbuf[256];
    snprintf(nsbuf, sizeof(nsbuf), "%s", argv[2]);
    moq_bytes_t ns_parts[32];
    size_t ns_count = split_namespace(nsbuf, ns_parts, 32);
    uint64_t seconds = 30;
    int draft = 0;
    const char *dump_dir = NULL;
    const char *wanted[MAX_TRACKS];
    size_t wanted_count = 0;
    bool insecure = false, finite = false, verbose = false;
    unsigned expect_tracks = 0;
    double stall_at_s = -1;
    unsigned stall_ms = 0;
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            seconds = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--draft") == 0 && i + 1 < argc) {
            draft = atoi(argv[++i]);
            if (draft != 16 && draft != 18 && draft != 22) {
                usage(argv[0]);
                return 2;
            }
        } else if (strcmp(argv[i], "--track") == 0 && i + 1 < argc &&
                   wanted_count < MAX_TRACKS) {
            wanted[wanted_count++] = argv[++i];
        } else if (strcmp(argv[i], "--dump") == 0 && i + 1 < argc) {
            dump_dir = argv[++i];
        } else if (strcmp(argv[i], "--finite") == 0) {
            finite = true;
        } else if (strcmp(argv[i], "--expect-tracks") == 0 && i + 1 < argc) {
            expect_tracks = (unsigned)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--decoder-stall") == 0 && i + 1 < argc) {
            if (sscanf(argv[++i], "%lf,%u", &stall_at_s, &stall_ms) != 2 ||
                stall_at_s < 0) {
                usage(argv[0]);
                return 2;
            }
        } else if (strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (strcmp(argv[i], "--insecure-skip-verify") == 0) {
            insecure = true;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /* The endpoint (connection) and receiver (catalog + subscriptions)
     * configurations are borrowed by create, which copies what it keeps. */
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init_sized(&ec, sizeof(ec));
    ec.url = (moq_bytes_t){ (const uint8_t *)url, strlen(url) };
    ec.insecure_skip_verify = insecure;
    moq_version_t pinned = (moq_version_t)draft;
    if (draft) {
        ec.versions.struct_size = sizeof(moq_version_offer_t);
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = &pinned;
        ec.versions.version_count = 1;
    }
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live_sized(&rcfg, sizeof(rcfg));
    rcfg.endpoint = &ec;
    rcfg.namespace_ = (moq_namespace_t){ ns_parts, ns_count };
    moq_media_player_cfg_t pc;
    moq_media_player_cfg_init(&pc);
    pc.receiver = &rcfg;
    pc.selection = wanted_count ? MOQ_MEDIA_SELECTION_MANUAL : MOQ_MEDIA_SELECTION_AUTO;
    moq_media_player_t *p = NULL;
    moq_result_t rc = moq_media_player_create(&pc, &p);
    if (rc != MOQ_OK) {
        fprintf(stderr, "player create failed: %d (%s)\n", (int)rc, moq_strerror(rc));
        return 1;
    }

    {
        struct timespec rt;
        clock_gettime(CLOCK_REALTIME, &rt);
        printf("CLOCK t_ms=%.1f realtime_us=%llu\n", t_ms(),
               (unsigned long long)rt.tv_sec * 1000000ull +
                   (unsigned long long)rt.tv_nsec / 1000u);
    }
    uint64_t deadline = now_us() + seconds * 1000000u;
    uint64_t first_unit_at = 0, stall_until = 0;
    bool stall_done = stall_at_s < 0, stalled = false;
    uint64_t events[32] = {0};
    uint64_t drop_reasons[80] = {0};
    bool failed = false, closed = false, session_seen = false;
    while (!g_stop && now_us() < deadline) {
        uint64_t now = now_us();
        if (!session_seen) {
            /* The negotiated draft is known once a session is up: before the
             * catalog arrives this tells "not connected" from "connected,
             * waiting for the catalog". */
            moq_media_receiver_info_t info;
            memset(&info, 0, sizeof(info));
            if (moq_media_receiver_get_info(moq_media_player_receiver(p), &info,
                                            sizeof(info)) == MOQ_OK &&
                info.transport_version != 0) {
                printf("SESSION draft=%u\n", info.transport_version);
                session_seen = true;
            }
        }
        if (!stall_done && first_unit_at &&
            now >= first_unit_at + (uint64_t)(stall_at_s * 1e6)) {
            if (!stalled) {
                printf("DECODER stalled for %u ms\n", stall_ms);
                report_decoder_queue(p, 8, now);
                stalled = true;
                stall_until = now + (uint64_t)stall_ms * 1000u;
            } else if (now >= stall_until) {
                printf("DECODER resumed\n");
                report_decoder_queue(p, 0, now);
                stall_done = true;
            }
        }
        rc = moq_media_player_wait(p, now, 20000);
        if (rc == MOQ_ERR_CLOSED) { closed = true; break; }
        if (rc == MOQ_ERR_INTERRUPTED) break;
        /* MOQ_DONE right away while DECODER_QUEUE feedback pauses release:
         * the decoder, not the player, is what to wait for. */
        if (rc == MOQ_DONE && stalled && !stall_done) sleep_us(5000);
        rc = moq_media_player_tick(p, now_us());
        if (rc == MOQ_ERR_CLOSED) closed = true;

        moq_media_player_output_t o;
        while (moq_media_player_poll_output(p, &o, sizeof(o)) == MOQ_OK) {
            if (o.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && !first_unit_at)
                first_unit_at = now_us();
            handle_output(&o, dump_dir, verbose);
            moq_media_player_output_cleanup(&o);   /* every kind owns refs */
        }
        /* The receiver's own view of each track: when it first says ended or
         * failed is when the peer's terminal notice (or a local failure)
         * reached it, whatever the player then waits for. */
        for (size_t i = 0; i < g_track_count; i++) {
            track_tally_t *t = &g_tracks[i];
            moq_media_track_state_t st;
            if (t->noticed || !t->track ||
                moq_media_receiver_track_state(moq_media_player_receiver(p), t->track,
                                               &st) != MOQ_OK)
                continue;
            if (st != MOQ_MEDIA_TRACK_STATE_ENDED && st != MOQ_MEDIA_TRACK_STATE_FAILED)
                continue;
            t->noticed = true;
            printf("NOTICE %s state=%s t_ms=%.1f\n", t->name,
                   st == MOQ_MEDIA_TRACK_STATE_ENDED ? "ended" : "failed", t_ms());
        }
        moq_media_player_event_t ev;
        while (moq_media_player_poll_event(p, &ev, sizeof(ev)) == MOQ_OK) {
            if (ev.kind < 32) events[ev.kind]++;
            switch (ev.kind) {
            case MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE:
                for (size_t i = 0; i < wanted_count; i++) {
                    if (!track_named(ev.track, wanted[i])) continue;
                    moq_media_player_select_cfg_t sc;
                    moq_media_player_select_cfg_init(&sc);
                    moq_result_t src = moq_media_player_select_track(p, ev.track, &sc);
                    fputs("SELECT ", stdout);
                    print_name(ev.track);
                    printf(": %s\n", moq_strerror(src));
                    if (src != MOQ_OK) failed = true;
                }
                break;
            case MOQ_MEDIA_PLAYER_EVENT_READY: {
                moq_media_receiver_info_t info;
                memset(&info, 0, sizeof(info));
                if (moq_media_receiver_get_info(moq_media_player_receiver(p), &info,
                                                sizeof(info)) == MOQ_OK)
                    printf("READY draft=%u companion=%u t_ms=%.1f\n", info.transport_version,
                           (unsigned)info.companion_profile, t_ms());
                else
                    printf("READY t_ms=%.1f\n", t_ms());
                break;
            }
            case MOQ_MEDIA_PLAYER_EVENT_STARTED:
                fputs("STARTED ", stdout);
                print_name(ev.track);
                printf(" timeline=%u\n", ev.u.started.timeline_id);
                break;
            case MOQ_MEDIA_PLAYER_EVENT_SKIP:
                printf("SKIP groups %llu..%llu\n",
                       (unsigned long long)ev.u.skip.from_group_id,
                       (unsigned long long)ev.u.skip.to_group_id);
                break;
            case MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE:
                printf("SYNC_UNAVAILABLE reason=%u\n", (unsigned)ev.u.sync.reason);
                break;
            case MOQ_MEDIA_PLAYER_EVENT_DROP:
                if (ev.u.drop.reason < 80) drop_reasons[ev.u.drop.reason]++;
                if (verbose) {
                    fputs("DROP ", stdout);
                    print_name(ev.track);
                    printf(" g=%llu o=%llu reason=%u\n",
                           (unsigned long long)ev.u.drop.group_id,
                           (unsigned long long)ev.u.drop.object_id,
                           (unsigned)ev.u.drop.reason);
                }
                break;
            case MOQ_MEDIA_PLAYER_EVENT_ENDED:
                printf("ENDED\n");
                break;
            case MOQ_MEDIA_PLAYER_EVENT_CLOSED:
                printf("CLOSED\n");
                break;
            default:
                break;
            }
        }
        if (moq_media_player_is_fatal(p)) {
            fprintf(stderr, "receiver failed: %llu t_ms=%.1f\n",
                    (unsigned long long)moq_media_player_fatal_code(p), t_ms());
            failed = true;
            break;
        }
        if (closed) break;
        if (finite && all_ended() && (!expect_tracks || g_track_count >= expect_tracks))
            break;
    }
    bool deadline_reached = now_us() >= deadline;
    if (g_stop) printf("interrupted\n");

    moq_media_player_stats_t st;
    memset(&st, 0, sizeof(st));
    if (moq_media_player_get_stats(p, &st, sizeof(st)) == MOQ_OK)
        printf("player: selected=%u taken=%llu pushed=%llu units=%llu dropped=%llu "
               "discarded=%llu events_dropped=%llu\n", st.tracks_selected,
               (unsigned long long)st.objects_taken, (unsigned long long)st.objects_pushed,
               (unsigned long long)st.units_output, (unsigned long long)st.objects_dropped,
               (unsigned long long)st.objects_discarded,
               (unsigned long long)st.events_dropped);
    printf("events: gap=%llu skip=%llu shed=%llu drop=%llu loss=%llu stall=%llu\n",
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_GAP],
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_SKIP],
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_SHED],
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_DROP],
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_LOSS],
           (unsigned long long)events[MOQ_MEDIA_PLAYER_EVENT_STALL]);
    for (unsigned i = 0; i < 80; i++)
        if (drop_reasons[i])
            printf("  drop reason %u: %llu\n", i, (unsigned long long)drop_reasons[i]);
    if (g_track_count == 0) failed = true;
    for (size_t i = 0; i < g_track_count; i++) {
        track_tally_t *t = &g_tracks[i];
        fputs("track ", stdout);
        print_name(t->track);
        printf(": configures=%llu units=%llu bytes=%llu discontinuities=%llu order=%s",
               (unsigned long long)t->configures, (unsigned long long)t->units,
               (unsigned long long)t->bytes, (unsigned long long)t->discontinuities,
               t->order_ok ? "ok" : "BROKEN");
        if (t->ended) printf(" end=%u/%u", t->end_cause, t->end_disposition);
        printf("\n");
        if (!t->order_ok || t->units == 0) failed = true;
        close_dump(t, &t->bin);
        close_dump(t, &t->units_log);
        if (t->io_failed) failed = true;
        if (finite && !(t->ended && t->end_cause == MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK &&
                        t->end_disposition == MOQ_MEDIA_PLAYER_END_COMPLETE))
            failed = true;
    }
    if (finite) {
        bool count_ok = !expect_tracks ||
                        (g_track_count == expect_tracks && st.tracks_selected == expect_tracks);
        bool done = all_ended() && !deadline_reached && !g_stop;
        printf("finite: %s (tracks=%zu expected=%u%s)\n",
               done && count_ok && !failed ? "COMPLETE" : "FAILED", g_track_count,
               expect_tracks, deadline_reached ? ", deadline reached" : "");
        if (!done || !count_ok) failed = true;
    }
    moq_media_player_destroy(p);
    return failed ? 1 : 0;
}
