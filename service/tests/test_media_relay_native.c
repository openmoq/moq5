/* Public service clients only. The opaque helper owns the real relay host.
 * Run one finite row per process:
 * CERT KEY d18_raw|d22_raw|d22_wt|d22_split|d22_cmaf|d18_push|d22_push.
 * The _push rows publish the tracks with PUBLISH (publish_tracks): the media
 * reaches the relay without demand, so the receiver subscribes before it is
 * written, and the track's end reaches it through the relay as the
 * publication's END_OF_TRACK. The _complete rows end the broadcast instead
 * (MSF 11.3): the same END_OF_TRACK, then the terminal isComplete catalog. */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include "media_relay_native_host.h"
#include "media_relay_native_cmaf.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEADLINE_US UINT64_C(15000000)
#define OBJECTS 4u
#define PAYLOAD_SIZE 37u

_Static_assert(OBJECTS == sizeof(native_cmaf_decode_us) /
                         sizeof(native_cmaf_decode_us[0]),
               "CMAF finite tail has one vector per object");

static const char *phase = "setup";
static unsigned checks;
#define REQUIRE(x) do { ++checks; if (!(x)) { \
    fprintf(stderr, "FAIL phase=%s line=%d: %s\n", phase, __LINE__, #x); \
    goto fail; } } while (0)

typedef struct viewer {
    moq_endpoint_t *ep;
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    unsigned catalogs, added, ended, objects;
    bool cmaf;
    /* A completed broadcast's terminal catalog removes the track: once, and
     * only after the track's own END_OF_TRACK ended it. */
    bool removal_expected;
    unsigned removed;
} viewer_t;

/* The Group ID every object of the run carries. Legacy numbering starts at
 * 0. The draft-22 companion starts at the wallclock milliseconds the sender
 * reads at creation (MSF-01 6.1), so the window around the attach bounds it,
 * and the first object fixes it for the rest of the run. */
static uint64_t group_lo, group_hi;
static bool group_seen;
static uint64_t group_id;

static uint64_t wallclock_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) abort();
    return (uint64_t)ts.tv_sec * UINT64_C(1000) +
           (uint64_t)ts.tv_nsec / 1000000u;
}

static bool expected_group(uint64_t g)
{
    if (!group_seen) {
        if (g < group_lo || g > group_hi) return false;
        group_seen = true;
        group_id = g;
    }
    return g == group_id;
}

static uint64_t now_us(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) abort();
    return (uint64_t)ts.tv_sec * UINT64_C(1000000) +
           (uint64_t)ts.tv_nsec / 1000;
}

static moq_bytes_t bytes(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

static bool equal(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && b.data != NULL &&
           memcmp(b.data, s, b.len) == 0;
}

static void payload(unsigned index, uint8_t out[PAYLOAD_SIZE])
{
    for (unsigned i = 0; i < PAYLOAD_SIZE; ++i)
        out[i] = (uint8_t)(index * 61u + i * 17u);
}

static uint64_t timestamp(unsigned index)
{
    return UINT64_C(1700000123456000) + (uint64_t)index * 33333u;
}

static void cmaf_fragment(unsigned index, uint8_t out[NATIVE_CMAF_FRAGMENT_SIZE])
{
    memcpy(out, native_cmaf_fragment_template, NATIVE_CMAF_FRAGMENT_SIZE);
    out[23] = (uint8_t)(index + 1); /* mfhd sequence_number */
    memcpy(out + 64, native_cmaf_decode_words[index], 4); /* tfdt low word */
    memcpy(out + NATIVE_CMAF_MDAT_OFFSET, native_cmaf_sample_bytes[index], 4);
}

static bool verify_cmaf_desc(const moq_media_track_desc_t *desc)
{
    REQUIRE(equal(desc->name, "audio") && equal(desc->codec, "mp4a.40.2"));
    REQUIRE(desc->info.media_type == MOQ_MEDIA_TYPE_AUDIO &&
            desc->info.packaging == MOQ_MEDIA_PACKAGING_CMAF &&
            desc->info.timescale == 48000);
    const moq_cmaf_init_info_t *rec = desc->cmaf;
    REQUIRE(rec && rec->struct_size == sizeof(*rec));
    REQUIRE(rec->codec_kind == MOQ_CMAF_CODEC_AAC &&
            rec->track_id == 1 && rec->timescale == 48000 &&
            rec->samplerate == 48000 && rec->channel_count == 2 &&
            !rec->has_cenc && rec->sample_entry == UINT32_C(0x6d703461));
    REQUIRE(desc->codec_config.len == sizeof(native_cmaf_asc) &&
            desc->codec_config.data == rec->codec_config.data &&
            desc->codec_config.len == rec->codec_config.len &&
            memcmp(desc->codec_config.data, native_cmaf_asc,
                   sizeof(native_cmaf_asc)) == 0);
    REQUIRE(desc->init_data.len == sizeof(k_ffmpeg_aac_stereo_init) &&
            desc->init_data.data != NULL &&
            memcmp(desc->init_data.data, k_ffmpeg_aac_stereo_init,
                   sizeof(k_ffmpeg_aac_stereo_init)) == 0);
    return true;
fail:
    return false;
}

/* No borrowed track/receiver access: the same checks run on the held final
 * object after both media owners and the receiving endpoint are destroyed. */
static bool verify_cmaf_object(const moq_media_object_t *o, unsigned index)
{
    REQUIRE(index < OBJECTS && o->struct_size == sizeof(*o));
    uint8_t expected[NATIVE_CMAF_FRAGMENT_SIZE];
    cmaf_fragment(index, expected);
    REQUIRE(o->config_generation == 0 &&
            o->packaging == MOQ_MEDIA_PACKAGING_CMAF &&
            o->status == MOQ_OBJECT_NORMAL && !o->datagram && o->keyframe);
    REQUIRE(o->payload.len == 0 && o->payload.data == NULL &&
            o->payload_ref != NULL && o->fragment.data != NULL);
    REQUIRE(o->fragment.len == sizeof(expected) &&
            memcmp(o->fragment.data, expected, sizeof(expected)) == 0);
    REQUIRE(o->mdat_offset == NATIVE_CMAF_MDAT_OFFSET && o->mdat_len == 4 &&
            memcmp(o->fragment.data + o->mdat_offset,
                   native_cmaf_sample_bytes[index], 4) == 0);
    REQUIRE(o->sample_count == 2 && o->samples != NULL);
    for (size_t i = 0; i < 2; ++i) {
        REQUIRE(o->samples[i].duration == 1024 && o->samples[i].size == 2 &&
                o->samples[i].flags == UINT32_C(0x02000000) &&
                o->samples[i].composition_offset == 0);
        REQUIRE(memcmp(o->fragment.data + o->mdat_offset + i * 2,
                       native_cmaf_sample_bytes[index] + i * 2, 2) == 0);
    }
    REQUIRE(!o->has_capture_time && o->capture_time_us == 0 &&
            o->decode_time_us == native_cmaf_decode_us[index] &&
            o->presentation_time_us == native_cmaf_decode_us[index] &&
            o->composition_offset_us == 0);
    REQUIRE(expected_group(o->group_id) && o->object_id == index &&
            o->has_subgroup && o->subgroup_id == 0 &&
            o->publisher_priority == 128);
    REQUIRE(o->delivery_origin == MOQ_MEDIA_DELIVERY_LIVE &&
            o->initiating_request_id == 0 && o->track_properties_ref == NULL);
    REQUIRE(o->metadata.struct_size == sizeof(o->metadata) &&
            o->metadata.loc.present == 0 && !o->metadata.has_time_us &&
            !o->metadata.independence_known);
    return true;
fail:
    return false;
}

static bool endpoint_connect(media_relay_native_host_t *host, bool wt,
                             moq_version_t version, moq_endpoint_t **out)
{
    char url[96];
    int n = snprintf(url, sizeof(url), "%s://127.0.0.1:%u/moq",
        wt ? "https" : "moqt", media_relay_native_host_port(host, wt));
    if (n <= 0 || (size_t)n >= sizeof(url)) return false;
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.url = bytes(url);
    cfg.backend = wt ? MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC
                     : MOQ_TRANSPORT_BACKEND_MSQUIC;
    cfg.versions.struct_size = sizeof(cfg.versions);
    cfg.versions.policy = MOQ_VERSION_POLICY_EXACT;
    cfg.versions.versions = &version;
    cfg.versions.version_count = 1;
    cfg.insecure_skip_verify = true; /* owned localhost test credentials only */
    moq_result_t rc = moq_endpoint_connect(&cfg, out);
    if (rc != MOQ_OK) fprintf(stderr, "connect result=%d\n", (int)rc);
    return rc == MOQ_OK;
}

static bool viewer_open(viewer_t *v, media_relay_native_host_t *host, bool wt,
                        moq_version_t version, moq_namespace_t ns)
{
    if (!endpoint_connect(host, wt, version, &v->ep)) return false;
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = ns;
    cfg.auto_subscribe = false;
    if (version == MOQ_VERSION_DRAFT_22)
        cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    return moq_media_receiver_attach(v->ep, &cfg, &v->receiver) == MOQ_OK;
}

static bool viewer_close(viewer_t *v)
{
    moq_media_receiver_destroy(v->receiver);
    v->receiver = NULL;
    bool ok = v->ep == NULL || moq_endpoint_stop(v->ep) == MOQ_OK;
    if (v->ep != NULL) moq_endpoint_destroy(v->ep);
    v->ep = NULL;
    return ok;
}

static bool poll_tracks(viewer_t *v)
{
    moq_media_track_event_t ev;
    moq_result_t rc;
    while ((rc = moq_media_receiver_poll_track(v->receiver, &ev,
                                               sizeof(ev))) == MOQ_OK) {
        REQUIRE(ev.struct_size == sizeof(ev));
        if (ev.kind == MOQ_MEDIA_CATALOG_READY) {
            REQUIRE(ev.track == NULL && ev.desc == NULL && ++v->catalogs == 1);
        } else if (ev.kind == MOQ_MEDIA_TRACK_ADDED) {
            REQUIRE(++v->added == 1 && ev.track != NULL && ev.desc != NULL);
            v->track = ev.track;
            moq_media_track_desc_t desc;
            REQUIRE(moq_media_receiver_track_desc_copy(v->receiver, v->track,
                        &desc, sizeof(desc)) == MOQ_OK);
            REQUIRE(desc.struct_size == sizeof(desc));
            if (v->cmaf) {
                REQUIRE(verify_cmaf_desc(&desc));
            } else {
                REQUIRE(equal(desc.name, "video") && equal(desc.codec, "av01"));
                REQUIRE(desc.info.media_type == MOQ_MEDIA_TYPE_VIDEO &&
                        desc.info.packaging == MOQ_MEDIA_PACKAGING_RAW);
            }
            REQUIRE(!desc.is_live && desc.has_track_duration &&
                    desc.track_duration_ms == 2000);
            REQUIRE(desc.has_bitrate && desc.bitrate == 1500000);
        } else if (ev.kind == MOQ_MEDIA_TRACK_ENDED) {
            REQUIRE(ev.track == v->track && ++v->ended == 1 && v->removed == 0);
        } else if (ev.kind == MOQ_MEDIA_TRACK_REMOVED && v->removal_expected) {
            REQUIRE(ev.track == v->track && ++v->removed == 1 && v->ended == 1);
        } else {
            fprintf(stderr, "unexpected track event=%u local=%d fill=%u/%u/%"
                    PRIu64 "\n", (unsigned)ev.kind, ev.local_result,
                    ev.fill_reason, ev.fill_error_domain, ev.fill_error_code);
            REQUIRE(false);
        }
    }
    REQUIRE(rc == MOQ_DONE);
    return true;
fail:
    return false;
}

static bool healthy(media_relay_native_host_t *host, moq_media_sender_t *s,
                    const viewer_t *v)
{
    return media_relay_native_host_running(host) &&
        !moq_media_sender_is_closed(s) && !moq_media_sender_is_fatal(s) &&
        (v == NULL || (!moq_media_receiver_is_closed(v->receiver) &&
                       !moq_media_receiver_is_fatal(v->receiver)));
}

static bool discover(viewer_t *v, media_relay_native_host_t *host,
                     moq_media_sender_t *sender, moq_version_t version)
{
    uint64_t deadline = now_us() + DEADLINE_US;
    while (v->catalogs == 0 || v->added == 0) {
        REQUIRE(healthy(host, sender, v));
        REQUIRE(poll_tracks(v));
        if (v->catalogs && v->added) break;
        REQUIRE(now_us() < deadline);
        moq_result_t rc = moq_media_receiver_wait(v->receiver, 20000);
        REQUIRE(rc == MOQ_OK || rc == MOQ_DONE);
    }
    REQUIRE(moq_endpoint_negotiated_version(v->ep) == version);
    moq_media_track_state_t state;
    REQUIRE(moq_media_receiver_track_state(v->receiver, v->track, &state) ==
            MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
    return true;
fail:
    return false;
}

static bool verify_object(const moq_media_object_t *o, const viewer_t *v,
                          moq_version_t version)
{
    unsigned index = v->objects;
    if (v->cmaf) {
        REQUIRE(version == MOQ_VERSION_DRAFT_22 && o->track == v->track);
        REQUIRE(verify_cmaf_object(o, index));
        return true;
    }
    uint8_t expected[PAYLOAD_SIZE];
    payload(index, expected);
    REQUIRE(index < OBJECTS && o->struct_size == sizeof(*o));
    REQUIRE(o->track == v->track && o->config_generation == 0);
    REQUIRE(o->packaging == MOQ_MEDIA_PACKAGING_RAW &&
            o->status == MOQ_OBJECT_NORMAL && !o->datagram);
    REQUIRE(o->payload.len == sizeof(expected) && o->payload.data != NULL &&
            memcmp(o->payload.data, expected, sizeof(expected)) == 0);
    REQUIRE(o->fragment.len == 0 && o->payload_ref != NULL);
    REQUIRE(expected_group(o->group_id) && o->object_id == index &&
            o->has_subgroup && o->subgroup_id == 0 &&
            o->publisher_priority == 128);
    REQUIRE(o->has_capture_time && o->capture_time_us == timestamp(index) &&
            o->decode_time_us == timestamp(index) &&
            o->presentation_time_us == timestamp(index));
    REQUIRE(o->keyframe == (index == 0));
    REQUIRE(o->track_properties_ref == NULL);
    if (version == MOQ_VERSION_DRAFT_22) {
        REQUIRE(o->delivery_origin == MOQ_MEDIA_DELIVERY_LIVE &&
                o->initiating_request_id == 0);
        REQUIRE(o->metadata.struct_size == sizeof(o->metadata));
        REQUIRE(o->metadata.loc.present ==
                (MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING));
        REQUIRE(o->metadata.loc.timestamp == timestamp(index) &&
                o->metadata.loc.marking_len == 1 &&
                o->metadata.loc.marking[0] == (index == 0 ? 0x20 : 0));
        REQUIRE(o->metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US &&
                o->metadata.has_time_us &&
                o->metadata.time_us == timestamp(index));
        REQUIRE(o->metadata.independence_known &&
                o->metadata.independent == (index == 0));
    }
    return true;
fail:
    return false;
}

static void diagnostics(moq_media_sender_t *s, const viewer_t *v)
{
    moq_media_sender_stats_t ss = {0};
    if (s != NULL) {
        (void)moq_media_sender_get_stats(s, &ss, sizeof(ss));
        fprintf(stderr, "sender ready=%d fatal=%d code=%" PRIu64
            " written=%" PRIu64 " sent=%" PRIu64 " queued=%" PRIu64
            " dropped=%" PRIu64 " error=%d\n", moq_media_sender_is_ready(s),
            moq_media_sender_is_fatal(s), moq_media_sender_fatal_code(s),
            ss.objects_written, ss.objects_sent, ss.objects_queued,
            ss.objects_dropped, (int)ss.last_error);
    }
    if (v->receiver != NULL) {
        moq_media_receiver_stats_t rs = {0};
        (void)moq_media_receiver_get_stats(v->receiver, &rs, sizeof(rs));
        fprintf(stderr, "receiver catalogs=%u added=%u objects=%u ended=%u"
            " fatal=%d code=%" PRIu64 " received=%" PRIu64
            " queued=%" PRIu64 " dropped=%" PRIu64 " parse=%" PRIu64 "\n",
            v->catalogs, v->added, v->objects, v->ended,
            moq_media_receiver_is_fatal(v->receiver),
            moq_media_receiver_fatal_code(v->receiver), rs.objects_received,
            rs.objects_queued, rs.objects_dropped, rs.parse_drops);
    }
    if (v->ep != NULL) {
        moq_endpoint_terminal_t t = {0};
        (void)moq_endpoint_get_terminal(v->ep, &t, sizeof(t));
        fprintf(stderr, "receiver endpoint state=%d version=%" PRIu64
                " terminal=%d detail=%" PRIu64 "\n",
                (int)moq_endpoint_state(v->ep),
                (uint64_t)moq_endpoint_negotiated_version(v->ep),
                (int)t.reason, t.detail_code);
    }
}

int main(int argc, char **argv)
{
    if (argc != 4 || (strcmp(argv[3], "d18_raw") != 0 &&
        strcmp(argv[3], "d22_raw") != 0 && strcmp(argv[3], "d22_wt") != 0 &&
        strcmp(argv[3], "d22_split") != 0 && strcmp(argv[3], "d22_cmaf") != 0 &&
        strcmp(argv[3], "d18_push") != 0 && strcmp(argv[3], "d22_push") != 0 &&
        strcmp(argv[3], "d18_complete") != 0 && strcmp(argv[3], "d22_complete") != 0 &&
        strcmp(argv[3], "d18_complete_now") != 0 &&
        strcmp(argv[3], "d22_complete_now") != 0 &&
        strcmp(argv[3], "d18_complete_slow") != 0 &&
        strcmp(argv[3], "d22_complete_slow") != 0)) {
        fprintf(stderr, "usage: %s CERT KEY d18_raw|d22_raw|d22_wt|d22_split|"
                "d22_cmaf|d18_push|d22_push|d18_complete|d22_complete|"
                "d18_complete_now|d22_complete_now|d18_complete_slow|"
                "d22_complete_slow\n",
                argv[0]);
        return 2;
    }
    bool cmaf = strcmp(argv[3], "d22_cmaf") == 0;
    /* _complete_now: complete() straight after the writes, with no end_track
     * or wait: the accepted media and the track's end still go out first. */
    /* _complete_slow: the same, and the application polls no media until the
     * receiver has adopted the terminal catalog. */
    bool complete_slow = strcmp(argv[3], "d18_complete_slow") == 0 ||
                         strcmp(argv[3], "d22_complete_slow") == 0;
    bool complete_now = complete_slow || strcmp(argv[3], "d18_complete_now") == 0 ||
                        strcmp(argv[3], "d22_complete_now") == 0;
    bool complete = complete_now || strcmp(argv[3], "d18_complete") == 0 ||
                    strcmp(argv[3], "d22_complete") == 0;
    bool push = complete || strcmp(argv[3], "d18_push") == 0 ||
                strcmp(argv[3], "d22_push") == 0;
    bool wt_sender = strcmp(argv[3], "d22_wt") == 0;
    bool wt_receiver = wt_sender || strcmp(argv[3], "d22_split") == 0;
    moq_version_t version = strncmp(argv[3], "d18_", 4) == 0
        ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
    media_relay_native_host_t *host = NULL;
    moq_endpoint_t *sender_ep = NULL;
    moq_media_sender_t *sender = NULL;
    moq_media_track_t *track = NULL;
    viewer_t prime = { .cmaf = cmaf }, late = { .cmaf = cmaf };
    moq_media_object_t held = {0};
    bool success = false;
    moq_bytes_t parts[] = {bytes("native-service"), bytes(argv[3])};
    moq_namespace_t ns = { .parts = parts, .count = 2 };
    REQUIRE(media_relay_native_host_start(argv[1], argv[2],
        version == MOQ_VERSION_DRAFT_18 ? 18 : 22, wt_receiver, &host) == 0);
    REQUIRE(endpoint_connect(host, wt_sender, version, &sender_ep));
    moq_media_sender_cfg_t sc;
    moq_media_sender_cfg_init_live_sized(&sc, sizeof(sc));
    REQUIRE(!cmaf || sc.validate_cmaf);
    sc.namespace_ = ns;
    sc.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
    sc.publish_tracks = push;
    sc.catalog_refresh_interval_us = UINT64_MAX;
    if (version == MOQ_VERSION_DRAFT_22)
        sc.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    group_lo = version == MOQ_VERSION_DRAFT_22 ? wallclock_ms() : 0;
    REQUIRE(moq_media_sender_attach(sender_ep, &sc, &sender) == MOQ_OK);
    group_hi = version == MOQ_VERSION_DRAFT_22 ? wallclock_ms() : 0;
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = bytes(cmaf ? "audio" : "video");
    tc.media_type = cmaf ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = cmaf ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = bytes(cmaf ? "mp4a.40.2" : "av01");
    if (cmaf) {
        tc.timescale = 48000;
        tc.samplerate = 48000;
        tc.channel_config = bytes("2");
        tc.init_data = (moq_bytes_t){ .data = k_ffmpeg_aac_stereo_init,
                                    .len = sizeof(k_ffmpeg_aac_stereo_init) };
    }
    tc.bitrate = 1500000;
    tc.is_live = false;
    tc.has_track_duration = true;
    tc.track_duration_ms = 2000;
    REQUIRE(moq_media_sender_add_track(sender, &tc, &track) == MOQ_OK);
    phase = "sender readiness before any receiver";
    uint64_t deadline = now_us() + DEADLINE_US;
    while (!moq_media_sender_is_ready(sender)) {
        REQUIRE(healthy(host, sender, NULL) && now_us() < deadline);
        moq_result_t rc = moq_media_sender_wait(sender, 20000);
        REQUIRE(rc == MOQ_OK || rc == MOQ_DONE);
    }
    REQUIRE(moq_endpoint_negotiated_version(sender_ep) == version);
    phase = "initial retained catalog";
    REQUIRE(viewer_open(&prime, host, wt_receiver, version, ns));
    REQUIRE(discover(&prime, host, sender, version));
    /* No timed refresh or catalog mutation follows. The second receiver must
     * discover the retained generation, not a newly published replacement. */
    phase = "late retained catalog";
    REQUIRE(viewer_open(&late, host, wt_receiver, version, ns));
    REQUIRE(discover(&late, host, sender, version));
    REQUIRE(viewer_close(&prime));
    REQUIRE(!moq_media_sender_track_has_subscriber(sender, track));
    /* Only the catalog needs history here. Explicit NONE suppresses the
     * media subscription's implicit current-group fill on draft 22. */
    moq_location_filter_cfg_t location = { .struct_size = sizeof(location),
                                         .kind = MOQ_LOCATION_FILTER_NONE };
    moq_media_receiver_track_subscribe_cfg_t subscription;
    moq_media_receiver_track_subscribe_cfg_init_sized(&subscription,
                                                    sizeof(subscription));
    subscription.location = &location;
    late.removal_expected = complete;
    if (push) {
        phase = "live subscription before the pushed media";
        REQUIRE(moq_media_receiver_subscribe_track(late.receiver, late.track,
                                                   &subscription) == MOQ_OK);
        deadline = now_us() + DEADLINE_US;
        moq_media_track_state_t st = MOQ_MEDIA_TRACK_STATE_PENDING;
        while (moq_media_receiver_track_state(late.receiver, late.track, &st) ==
                   MOQ_OK && st != MOQ_MEDIA_TRACK_STATE_ACTIVE) {
            REQUIRE(healthy(host, sender, &late) && poll_tracks(&late));
            REQUIRE(now_us() < deadline);
            moq_result_t wrc = moq_media_receiver_wait(late.receiver, 20000);
            REQUIRE(wrc == MOQ_OK || wrc == MOQ_DONE);
        }
    }
    phase = push ? "pushed finite tail" : "queued finite tail before media demand";
    for (unsigned i = 0; i < OBJECTS; ++i) {
        uint8_t data[PAYLOAD_SIZE];
        uint8_t fragment[NATIVE_CMAF_FRAGMENT_SIZE];
        const uint8_t *body = data;
        size_t body_size = sizeof(data);
        if (cmaf) {
            cmaf_fragment(i, fragment);
            body = fragment;
            body_size = sizeof(fragment);
        } else {
            payload(i, data);
        }
        moq_rcbuf_t *buf = NULL;
        REQUIRE(moq_rcbuf_create(moq_alloc_default(), body, body_size,
                                &buf) == MOQ_OK);
        moq_media_metadata_t md = { .struct_size = sizeof(md) };
        moq_loc_metadata_init_sized(&md.loc, sizeof(md.loc));
        md.loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
        md.loc.timestamp = timestamp(i);
        md.loc.marking_len = 1;
        md.loc.marking[0] = i == 0 ? 0x20 : 0;
        moq_media_send_object_t obj = { .struct_size = sizeof(obj),
            .payload = buf, .is_sync = i == 0, .starts_group = i == 0,
            .ends_group = i + 1 == OBJECTS, .presentation_time_us = timestamp(i),
            .has_capture_time = true, .capture_time_us = timestamp(i),
            .metadata = &md };
        if (cmaf) {
            obj.is_sync = true;
            obj.presentation_time_us = 0;
            obj.has_capture_time = false;
            obj.capture_time_us = 0;
            obj.metadata = NULL;
            obj.has_sap_type = true;
            obj.sap_type = MOQ_SAP_TYPE_1;
        }
        moq_result_t rc = moq_media_sender_write(sender, track, &obj);
        if (rc != MOQ_OK) moq_rcbuf_decref(buf);
        REQUIRE(rc == MOQ_OK);
    }
    moq_media_sender_stats_t ss;
    REQUIRE(moq_media_sender_get_stats(sender, &ss, sizeof(ss)) == MOQ_OK);
    REQUIRE(ss.objects_written == OBJECTS && ss.objects_dropped == 0);
    REQUIRE(push || (ss.objects_queued == OBJECTS && ss.objects_sent == 0));
    if (complete_now) {
        REQUIRE(moq_media_sender_complete(sender) == MOQ_OK);
        REQUIRE(moq_media_sender_complete(sender) == MOQ_OK);
        REQUIRE(moq_media_sender_end_track(sender, track) == MOQ_ERR_WRONG_STATE);
    } else {
        REQUIRE(moq_media_sender_end_track(sender, track) == MOQ_OK);
        REQUIRE(moq_media_sender_end_track(sender, track) == MOQ_OK);
    }
    if (!push)
        REQUIRE(moq_media_receiver_subscribe_track(late.receiver, late.track,
                                                   &subscription) ==
                MOQ_OK);
    if (complete_slow) {
        phase = "terminal catalog adopted before any poll";
        deadline = now_us() + DEADLINE_US;
        moq_media_receiver_stats_t early;
        while (moq_media_receiver_get_stats(late.receiver, &early, sizeof(early)) ==
                   MOQ_OK && !early.catalog_complete) {
            REQUIRE(healthy(host, sender, &late) && now_us() < deadline);
            moq_result_t wrc = moq_media_receiver_wait(late.receiver, 20000);
            REQUIRE(wrc == MOQ_OK || wrc == MOQ_DONE);
        }
        REQUIRE(early.objects_dropped == 0);
    }
    phase = "live tail and END_OF_TRACK";
    deadline = now_us() + DEADLINE_US;
    while (late.objects < OBJECTS || late.ended == 0) {
        REQUIRE(healthy(host, sender, &late));
        REQUIRE(poll_tracks(&late));
        moq_media_object_t obj;
        moq_result_t rc;
        while ((rc = moq_media_receiver_poll_object(late.receiver, &obj,
                                                    sizeof(obj))) == MOQ_OK) {
            bool valid = verify_object(&obj, &late, version);
            ++late.objects;
            if (valid && late.objects == OBJECTS) held = obj;
            else moq_media_object_cleanup(&obj);
            REQUIRE(valid);
        }
        REQUIRE(rc == MOQ_DONE);
        if (late.objects == OBJECTS && late.ended == 1) break;
        REQUIRE(now_us() < deadline);
        rc = moq_media_receiver_wait(late.receiver, 20000);
        REQUIRE(rc == MOQ_OK || rc == MOQ_DONE);
    }
    REQUIRE(late.objects == OBJECTS && late.ended == 1);
    if (complete && !complete_now) {
        /* MSF 11.3 in its order: the track's end, received; then the
         * broadcast's terminal catalog. */
        REQUIRE(moq_media_sender_complete(sender) == MOQ_OK);
        REQUIRE(moq_media_sender_complete(sender) == MOQ_OK);
    }
    moq_media_track_state_t state;
    if (late.removed == 0) {
        REQUIRE(moq_media_receiver_track_state(late.receiver, late.track, &state) ==
                MOQ_OK && state == MOQ_MEDIA_TRACK_STATE_ENDED);
    } else {
        /* The terminal catalog's removal already followed the end. */
        REQUIRE(complete && moq_media_receiver_track_state(late.receiver, late.track,
                                                           &state) == MOQ_ERR_WRONG_STATE);
    }
    REQUIRE(moq_media_receiver_subscribe_track(late.receiver, late.track, NULL) ==
            MOQ_ERR_WRONG_STATE);
    REQUIRE(moq_endpoint_state(sender_ep) == MOQ_ENDPOINT_ESTABLISHED &&
            moq_endpoint_state(late.ep) == MOQ_ENDPOINT_ESTABLISHED);
    phase = "public drain and ownership cleanup";
    REQUIRE(moq_media_sender_get_stats(sender, &ss, sizeof(ss)) == MOQ_OK);
    REQUIRE(ss.objects_written == OBJECTS && ss.objects_sent == OBJECTS &&
            ss.objects_queued == 0 && ss.objects_dropped == 0);
    moq_media_receiver_stats_t rs;
    REQUIRE(moq_media_receiver_get_stats(late.receiver, &rs, sizeof(rs)) == MOQ_OK);
    REQUIRE(rs.objects_received == OBJECTS && rs.objects_queued == 0 &&
            rs.objects_dropped == 0 && rs.parse_drops == 0 &&
            rs.overflow_events == 0 && rs.catalog_drops == 0);
    if (complete) {
        phase = "terminal catalog";
        deadline = now_us() + DEADLINE_US;
        while (moq_media_receiver_get_stats(late.receiver, &rs, sizeof(rs)) == MOQ_OK &&
               !rs.catalog_complete) {
            REQUIRE(healthy(host, sender, &late) && poll_tracks(&late));
            REQUIRE(now_us() < deadline);
            moq_result_t wrc = moq_media_receiver_wait(late.receiver, 20000);
            REQUIRE(wrc == MOQ_OK || wrc == MOQ_DONE);
        }
        REQUIRE(rs.catalog_complete && rs.catalog_drops == 0);
        while (late.removed == 0) {
            REQUIRE(healthy(host, sender, &late) && poll_tracks(&late));
            REQUIRE(now_us() < deadline);
            moq_result_t wrc = moq_media_receiver_wait(late.receiver, 20000);
            REQUIRE(wrc == MOQ_OK || wrc == MOQ_DONE);
        }
        REQUIRE(late.ended == 1 && late.removed == 1);
    }
    moq_media_sender_destroy(sender);
    sender = NULL;
    REQUIRE(moq_endpoint_drain(sender_ep, DEADLINE_US) == MOQ_OK);
    REQUIRE(viewer_close(&late));
    if (cmaf) {
        REQUIRE(verify_cmaf_object(&held, OBJECTS - 1));
    } else {
        uint8_t last[PAYLOAD_SIZE];
        payload(OBJECTS - 1, last);
        REQUIRE(held.payload.len == sizeof(last) &&
                memcmp(held.payload.data, last, sizeof(last)) == 0);
        if (version == MOQ_VERSION_DRAFT_22)
            REQUIRE(held.metadata.loc.timestamp == timestamp(OBJECTS - 1) &&
                    held.metadata.loc.marking_len == 1 &&
                    held.metadata.loc.marking[0] == 0);
    }
    moq_media_object_cleanup(&held);
    moq_media_object_cleanup(&held);
    success = true;
fail:
    if (!success) {
        diagnostics(sender, late.receiver != NULL ? &late : &prime);
        fprintf(stderr, "FAIL row=%s phase=%s checks=%u\n", argv[3], phase, checks);
    }
    moq_media_object_cleanup(&held);
    if (!viewer_close(&prime)) success = false;
    if (!viewer_close(&late)) success = false;
    moq_media_sender_destroy(sender);
    if (sender_ep != NULL) {
        if (moq_endpoint_stop(sender_ep) != MOQ_OK) success = false;
        moq_endpoint_destroy(sender_ep);
    }
    if (media_relay_native_host_stop(host) != 0) success = false;
    printf("%s public-service-native row=%s checks=%u\n",
           success ? "PASS" : "FAIL", argv[3], checks);
    return success ? 0 : 1;
}
