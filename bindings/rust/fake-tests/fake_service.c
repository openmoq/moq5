/* A scripted stand-in for the libmoq service tier.
 *
 * It defines the service-tier symbols the moq5 crate calls, with the real
 * signatures, and a control surface (moq5_fake_*) the test crate drives:
 * knobs select results and stamps, queues hold the records a poll returns,
 * and a text log records what each call received. The core library is real
 * (rcbuf, allocator, CMAF, MSF come from the SDK), so transferred buffers
 * are real references.
 *
 * It is substituted for the SDK's service library through pkg-config alone:
 * run.sh writes a libmoq-service.pc whose Libs name this archive and whose
 * Requires is the real libmoq. Nothing in moq5-sys or moq5 knows it exists.
 *
 * Symbols deliberately absent: moq_media_track_desc_get (the binding reads
 * descriptions only through the synchronized copy; a reference to the lock-
 * free getter would fail to link) and the sender callback initializers (the
 * binding installs no callbacks). */

#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- Allocation through the endpoint's allocator ----------------------- */

static const moq_alloc_t *g_alloc;   /* the current endpoint's allocator */
static int64_t g_alloc_live;

static void *fake_alloc(size_t size)
{
    const moq_alloc_t *a = g_alloc ? g_alloc : moq_alloc_default();
    void *p = a->alloc(size, a->ctx);
    if (p) {
        memset(p, 0, size);
        g_alloc_live++;
    }
    return p;
}

static void fake_free(void *p, size_t size)
{
    if (!p) return;
    const moq_alloc_t *a = g_alloc ? g_alloc : moq_alloc_default();
    a->free(p, size, a->ctx);
    g_alloc_live--;
}

/* -- Log ---------------------------------------------------------------- */

static char  *g_log;
static size_t g_log_len, g_log_cap;

/* Append one formatted line to the log, whatever its length: the line is
 * measured first and the log grown to hold all of it, so a long span (a
 * URL, a catalog name) is logged whole. Allocation failure aborts: a test
 * provider that silently dropped a line would hide what it was asked to
 * record. */
static void logf_(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) abort();
    size_t need = (size_t)n + 2;   /* the newline and the terminator */
    if (g_log_cap - g_log_len < need) {
        size_t cap = g_log_cap ? g_log_cap : 4096;
        while (cap - g_log_len < need) cap *= 2;
        char *grown = realloc(g_log, cap);
        if (!grown) abort();
        g_log = grown;
        g_log_cap = cap;
    }
    va_start(ap, fmt);
    int written = vsnprintf(g_log + g_log_len, g_log_cap - g_log_len, fmt, ap);
    va_end(ap);
    if (written != n) abort();
    g_log_len += (size_t)n;
    g_log[g_log_len++] = '\n';
    g_log[g_log_len] = 0;
}

size_t moq5_fake_log_take(char *buf, size_t cap)
{
    size_t len = g_log_len;
    if (buf && cap) {
        size_t n = len < cap ? len : cap;
        memcpy(buf, g_log ? g_log : "", n);
    }
    if (buf && cap >= len) g_log_len = 0;
    return len;
}

#define SPAN(b) (int)((b).len), (const char *)((b).data ? (b).data : (const uint8_t *)"")

/* -- Knobs and counters ------------------------------------------------- */

typedef struct knob { const char *name; int64_t value; } knob_t;
static knob_t g_knobs[256];
static size_t g_knob_count;

/* Knob names outlive the caller's string (strdup is not ISO C). */
static const char *copy_string(const char *s)
{
    size_t n = strlen(s) + 1;
    char *copy = malloc(n);
    if (!copy) abort();
    memcpy(copy, s, n);
    return copy;
}

static knob_t *knob(const char *name)
{
    for (size_t i = 0; i < g_knob_count; i++)
        if (strcmp(g_knobs[i].name, name) == 0) return &g_knobs[i];
    if (g_knob_count == sizeof g_knobs / sizeof g_knobs[0]) abort();
    g_knobs[g_knob_count].name = copy_string(name);
    g_knobs[g_knob_count].value = 0;
    return &g_knobs[g_knob_count++];
}

void moq5_fake_set(const char *name, int64_t value) { knob(name)->value = value; }
int64_t moq5_fake_get(const char *name)
{
    if (strcmp(name, "alloc_live") == 0) return g_alloc_live;
    return knob(name)->value;
}
static int64_t get(const char *name) { return knob(name)->value; }
static void bump(const char *name) { knob(name)->value++; }

/* A stamp knob: 0 selects the real rule, min(out_size, library size). */
static uint32_t stamp_for(const char *name, size_t out_size, size_t lib_size)
{
    int64_t v = get(name);
    if (v) return (uint32_t)v;
    return (uint32_t)(out_size < lib_size ? out_size : lib_size);
}

/* -- Objects ------------------------------------------------------------ */

#define FAKE_TRACKS 8
#define FAKE_QUEUE  32

typedef struct fake_track {
    struct moq_media_sender   *sender;
    struct moq_media_receiver *receiver;
    bool                  used;
    bool                  removed;
    bool                  ended;
    uint32_t              state;        /* receiver: moq_media_track_state_t */
    moq_media_track_desc_t desc;
    bool                  has_desc;
} fake_track_t;

struct moq_endpoint {
    const moq_alloc_t *alloc;
    bool interrupted;
    int attachments;
};

struct moq_media_sender {
    moq_endpoint_t *ep;
    fake_track_t tracks[FAKE_TRACKS];
    size_t track_count;
};

struct moq_media_receiver {
    moq_endpoint_t *ep;
    fake_track_t tracks[FAKE_TRACKS];
    moq_media_track_event_t events[FAKE_QUEUE];
    size_t event_head, event_count;
    moq_media_object_t objects[FAKE_QUEUE];
    size_t object_head, object_count;
    moq_media_stream_terminal_t terminals[FAKE_QUEUE];
    size_t terminal_head, terminal_count;
    moq_media_sap_record_t saps[FAKE_QUEUE];
    size_t sap_head, sap_count;
    moq_media_timeline_record_t timelines[FAKE_QUEUE];
    size_t timeline_head, timeline_count;
};

static moq_media_receiver_t *g_receiver;   /* the receiver pushes target */
#ifdef MOQ5_FAKE_PLAYER
static struct moq_media_player *g_player;  /* the player pushes target */
#endif
static const moq_cmsf_content_protection_t *g_cp;
static uint8_t g_goaway[256];
static size_t  g_goaway_len;
static bool    g_goaway_present;

void moq5_fake_reset(void)
{
    g_log_len = 0;
    if (g_log) g_log[0] = 0;
    for (size_t i = 0; i < g_knob_count; i++) g_knobs[i].value = 0;
    g_alloc = NULL;
    g_alloc_live = 0;
    g_receiver = NULL;
    g_cp = NULL;
    g_goaway_len = 0;
    g_goaway_present = false;
    knob("wait_result")->value = MOQ_DONE;
    knob("drain_result")->value = MOQ_DONE;
    knob("sender_wait_result")->value = MOQ_DONE;
    knob("receiver_wait_result")->value = MOQ_DONE;
    knob("poll_track_result")->value = MOQ_DONE;
    knob("poll_object_result")->value = MOQ_DONE;
    knob("poll_terminal_result")->value = MOQ_DONE;
    knob("poll_sap_result")->value = MOQ_DONE;
    knob("poll_timeline_result")->value = MOQ_DONE;
    knob("player_wait_result")->value = MOQ_DONE;
    knob("poll_output_result")->value = MOQ_DONE;
    knob("poll_event_result")->value = MOQ_DONE;
    knob("player_deadline")->value = -1;   /* UINT64_MAX: no deadline */
#ifdef MOQ5_FAKE_PLAYER
    g_player = NULL;
#endif
}

void moq5_fake_set_goaway(const uint8_t *uri, size_t len, bool present)
{
    if (len > sizeof g_goaway) abort();
    if (len) memcpy(g_goaway, uri, len);
    g_goaway_len = len;
    g_goaway_present = present;
}

void moq5_fake_set_content_protection(const moq_cmsf_content_protection_t *cp) { g_cp = cp; }

static int track_index(const fake_track_t *tracks, const moq_media_track_t *t)
{
    for (int i = 0; i < FAKE_TRACKS; i++)
        if ((const void *)&tracks[i] == (const void *)t) return i;
    return -1;
}

/* -- Endpoint ----------------------------------------------------------- */

void moq_endpoint_cfg_init_sized(moq_endpoint_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    if (cfg_size > sizeof *cfg) cfg_size = sizeof *cfg;
    memset(cfg, 0, cfg_size);
    if (cfg_size >= sizeof(uint32_t)) cfg->struct_size = (uint32_t)cfg_size;
    if (cfg_size >= offsetof(moq_endpoint_cfg_t, perspective) + sizeof cfg->perspective)
        cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
}

moq_result_t moq_endpoint_connect(const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out)
{
    if (!cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    bump("connect");
    char versions[128] = "";
    for (size_t i = 0; i < cfg->versions.version_count && i < 8; i++) {
        char one[16];
        snprintf(one, sizeof one, "%s%u", i ? "," : "", (unsigned)cfg->versions.versions[i]);
        strncat(versions, one, sizeof versions - strlen(versions) - 1);
    }
    char tokens[512] = "";
    for (size_t i = 0; i < cfg->setup_auth_token_count && i < 4; i++) {
        char one[160];
        const moq_auth_token_t *t = &cfg->setup_auth_tokens[i];
        snprintf(one, sizeof one, "%s%" PRIu64 ":%.*s", i ? ";" : "", t->token_type,
                 SPAN(t->token_value));
        strncat(tokens, one, sizeof tokens - strlen(tokens) - 1);
    }
    logf_("connect url=%.*s protocol=%d backend=%d policy=%d versions=%s sni=%.*s "
          "ca_file=%.*s insecure=%d wt_path=%.*s perspective=%d alloc=%d wt_profile=%u "
          "handshake_us=%" PRIu64 " tokens=%s verify_cb=%d uri_mode=%u preferred=%u "
          "accept_uri=%d struct_size=%u",
          SPAN(cfg->url), (int)cfg->protocol, (int)cfg->backend, (int)cfg->versions.policy,
          versions, SPAN(cfg->sni), SPAN(cfg->ca_file), cfg->insecure_skip_verify ? 1 : 0,
          SPAN(cfg->wt_path), (int)cfg->perspective, cfg->alloc ? 1 : 0, cfg->wt_profile,
          cfg->handshake_timeout_us, tokens, cfg->verify_certificate ? 1 : 0, cfg->uri_mode,
          (unsigned)cfg->preferred_version, cfg->accept_uri ? 1 : 0, cfg->struct_size);
    moq_result_t rc = (moq_result_t)get("connect_result");
    if (rc != MOQ_OK) return rc;
    g_alloc = cfg->alloc;
    moq_endpoint_t *ep = fake_alloc(sizeof *ep);
    if (!ep) return MOQ_ERR_NOMEM;
    ep->alloc = cfg->alloc;
    *out = ep;
    return MOQ_OK;
}

moq_result_t moq_endpoint_stop(moq_endpoint_t *ep)
{
    if (!ep) return MOQ_ERR_INVAL;
    bump("stop");
    logf_("stop");
    if (ep->attachments > 0) return MOQ_ERR_WRONG_STATE;
    return (moq_result_t)get("stop_result");
}

void moq_endpoint_destroy(moq_endpoint_t *ep)
{
    if (!ep) return;
    bump("destroy");
    logf_("destroy");
    fake_free(ep, sizeof *ep);
    g_alloc = NULL;
}

moq_result_t moq_endpoint_wait(moq_endpoint_t *ep, uint64_t timeout_us)
{
    if (!ep) return MOQ_ERR_INVAL;
    logf_("wait %" PRIu64, timeout_us);
    if (ep->interrupted) return MOQ_ERR_INTERRUPTED;
    return (moq_result_t)get("wait_result");
}

void moq_endpoint_wake(moq_endpoint_t *ep)
{
    if (!ep) return;
    bump("wake");
    logf_("wake");
}

moq_result_t moq_endpoint_drain(moq_endpoint_t *ep, uint64_t timeout_us)
{
    if (!ep) return MOQ_ERR_INVAL;
    logf_("drain %" PRIu64, timeout_us);
    if (ep->interrupted) return MOQ_ERR_INTERRUPTED;
    return (moq_result_t)get("drain_result");
}

moq_endpoint_state_t moq_endpoint_state(const moq_endpoint_t *ep)
{
    (void)ep;
    return (moq_endpoint_state_t)get("state");
}

moq_result_t moq_endpoint_get_goaway_uri(moq_endpoint_t *ep, uint8_t *buf, size_t cap,
                                         size_t *uri_len)
{
    if (!ep || !uri_len || (!buf && cap > 0)) return MOQ_ERR_INVAL;
    bump("goaway");
    if (!g_goaway_present) return MOQ_DONE;
    *uri_len = g_goaway_len;
    if (cap < g_goaway_len) return MOQ_ERR_BUFFER;
    if (g_goaway_len) memcpy(buf, g_goaway, g_goaway_len);
    return MOQ_OK;
}

bool moq_endpoint_is_closed(const moq_endpoint_t *ep) { (void)ep; return get("is_closed") != 0; }
bool moq_endpoint_is_fatal(const moq_endpoint_t *ep) { (void)ep; return get("is_fatal") != 0; }
uint64_t moq_endpoint_fatal_code(const moq_endpoint_t *ep)
{
    (void)ep;
    return (uint64_t)get("fatal_code");
}

moq_result_t moq_endpoint_get_terminal(const moq_endpoint_t *ep, moq_endpoint_terminal_t *out,
                                       size_t out_size)
{
    if (!ep || !out || out_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE) return MOQ_ERR_INVAL;
    moq_endpoint_terminal_t t = {
        .struct_size = 0,
        .reason = (moq_endpoint_terminal_reason_t)get("terminal_reason"),
        .detail_code = (uint64_t)get("terminal_detail"),
    };
    size_t n = out_size < sizeof t ? out_size : sizeof t;
    memcpy(out, &t, n);
    out->struct_size = stamp_for("terminal_stamp", out_size, sizeof t);
    return MOQ_OK;
}

moq_version_t moq_endpoint_negotiated_version(const moq_endpoint_t *ep)
{
    (void)ep;
    return (moq_version_t)get("negotiated_version");
}

void moq_endpoint_set_interrupted(moq_endpoint_t *ep, bool interrupted)
{
    if (!ep) return;
    ep->interrupted = interrupted;
    bump("set_interrupted");
    logf_("set_interrupted %d", interrupted ? 1 : 0);
}

/* -- Sender ------------------------------------------------------------- */

static void sender_cfg_init(moq_media_sender_cfg_t *cfg, size_t cfg_size,
                            moq_media_send_backpressure_t bp)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof *cfg ? cfg_size : sizeof *cfg;
    if (n < sizeof cfg->struct_size) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_sender_cfg_t, validate_cmaf) + sizeof cfg->validate_cmaf)
        cfg->validate_cmaf = true;
    if (n >= offsetof(moq_media_sender_cfg_t, companion_profile) + sizeof cfg->companion_profile)
        cfg->companion_profile = MOQ_MEDIA_COMPANION_AUTO;
    if (n >= offsetof(moq_media_sender_cfg_t, backpressure) + sizeof cfg->backpressure)
        cfg->backpressure = bp;
    if (n >= offsetof(moq_media_sender_cfg_t, block_timeout_us) + sizeof cfg->block_timeout_us &&
        bp == MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT)
        cfg->block_timeout_us = 2000000;
}

void moq_media_sender_cfg_init_sized(moq_media_sender_cfg_t *cfg, size_t cfg_size)
{
    sender_cfg_init(cfg, cfg_size, MOQ_MEDIA_SEND_BP_UNSET);
}
void moq_media_sender_cfg_init_live_sized(moq_media_sender_cfg_t *cfg, size_t cfg_size)
{
    sender_cfg_init(cfg, cfg_size, MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME);
}
void moq_media_sender_cfg_init_lossless_sized(moq_media_sender_cfg_t *cfg, size_t cfg_size)
{
    sender_cfg_init(cfg, cfg_size, MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT);
}

void moq_media_track_cfg_init_sized(moq_media_track_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof *cfg ? cfg_size : sizeof *cfg;
    if (n < sizeof cfg->struct_size) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_track_cfg_t, is_live) + sizeof cfg->is_live)
        cfg->is_live = true;
}

static void log_namespace(char *buf, size_t cap, moq_namespace_t ns)
{
    buf[0] = 0;
    for (size_t i = 0; i < ns.count && i < 8; i++) {
        char one[256];
        snprintf(one, sizeof one, "%s%.*s", i ? "/" : "", SPAN(ns.parts[i]));
        strncat(buf, one, cap - strlen(buf) - 1);
    }
}

moq_result_t moq_media_sender_attach(moq_endpoint_t *ep, const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    if (!ep || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    bump("sender_attach");
    if (cfg->endpoint) return MOQ_ERR_INVAL;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts) return MOQ_ERR_INVAL;
    if (cfg->backpressure == MOQ_MEDIA_SEND_BP_UNSET) return MOQ_ERR_INVAL;
    char ns[512];
    log_namespace(ns, sizeof ns, cfg->namespace_);
    char cps[512] = "";
    for (size_t i = 0; i < cfg->content_protection_count && i < 4; i++) {
        const moq_cmsf_content_protection_t *cp = &cfg->content_protections[i];
        char one[256];
        snprintf(one, sizeof one, "%s%.*s:%zu:%.*s:%.*s:%s%s", i ? ";" : "", SPAN(cp->ref_id),
                 cp->default_kid_count, SPAN(cp->scheme), SPAN(cp->drm_system.system_id),
                 cp->drm_system.la_url.present ? "la" : "-",
                 cp->drm_system.has_pssh ? "+pssh" : "");
        strncat(cps, one, sizeof cps - strlen(cps) - 1);
    }
    logf_("sender_attach ns=%s catalog=%.*s bp=%d block_timeout_us=%" PRIu64
          " queue=%u/%u pre_ready=%u/%u validate_cmaf=%d cps=%zu[%s] callbacks=%d publish=%d "
          "drop_without_demand=%d refresh_us=%" PRIu64 " companion=%u struct_size=%u",
          ns, SPAN(cfg->catalog_track), (int)cfg->backpressure, cfg->block_timeout_us,
          cfg->queue_max_objects, cfg->queue_max_bytes, cfg->pre_ready_max_objects,
          cfg->pre_ready_max_bytes, cfg->validate_cmaf ? 1 : 0, cfg->content_protection_count,
          cps,
          (cfg->callbacks.on_ready || cfg->callbacks.on_closed ||
           cfg->callbacks.on_subscriber_joined || cfg->callbacks.on_subscriber_left ||
           cfg->callbacks.on_track_closed || cfg->callbacks.ctx) ? 1 : 0,
          cfg->publish_tracks ? 1 : 0, cfg->drop_without_demand ? 1 : 0,
          cfg->catalog_refresh_interval_us, cfg->companion_profile, cfg->struct_size);
    moq_result_t rc = (moq_result_t)get("sender_attach_result");
    if (rc != MOQ_OK) return rc;
    moq_media_sender_t *s = fake_alloc(sizeof *s);
    if (!s) return MOQ_ERR_NOMEM;
    s->ep = ep;
    ep->attachments++;
    *out = s;
    return MOQ_OK;
}

void moq_media_sender_destroy(moq_media_sender_t *s)
{
    if (!s) return;
    bump("sender_destroy");
    logf_("sender_destroy");
    s->ep->attachments--;
    fake_free(s, sizeof *s);
}

moq_result_t moq_media_sender_add_track(moq_media_sender_t *s, const moq_media_track_cfg_t *cfg,
                                        moq_media_track_t **out)
{
    if (!s || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    bump("add_track");
    char label[300];
    if (cfg->has_label) snprintf(label, sizeof label, "\"%.*s\"", SPAN(cfg->label));
    else snprintf(label, sizeof label, "none");
    char maxsap[40], duration[40], alt[24], prio[24], render[24], sapt[24], mediat[24];
    snprintf(maxsap, sizeof maxsap, "%s/%s",
             cfg->has_max_grp_sap ? (char[12]){0} : "none", cfg->has_max_obj_sap ? "" : "none");
    if (cfg->has_max_grp_sap && cfg->has_max_obj_sap)
        snprintf(maxsap, sizeof maxsap, "%u/%u", cfg->max_grp_sap, cfg->max_obj_sap);
    else if (cfg->has_max_grp_sap)
        snprintf(maxsap, sizeof maxsap, "%u/none", cfg->max_grp_sap);
    else if (cfg->has_max_obj_sap)
        snprintf(maxsap, sizeof maxsap, "none/%u", cfg->max_obj_sap);
    else
        snprintf(maxsap, sizeof maxsap, "none/none");
    if (cfg->has_track_duration) snprintf(duration, sizeof duration, "%" PRIu64, cfg->track_duration_ms);
    else snprintf(duration, sizeof duration, "none");
    if (cfg->has_alt_group) snprintf(alt, sizeof alt, "%d", cfg->alt_group);
    else snprintf(alt, sizeof alt, "none");
    if (cfg->has_publisher_priority) snprintf(prio, sizeof prio, "%u", cfg->publisher_priority);
    else snprintf(prio, sizeof prio, "none");
    if (cfg->has_render_group) snprintf(render, sizeof render, "%d", cfg->render_group);
    else snprintf(render, sizeof render, "none");
    if (cfg->emit_sap_timeline) snprintf(sapt, sizeof sapt, "on:%u", cfg->sap_timeline_history_groups);
    else snprintf(sapt, sizeof sapt, "off");
    if (cfg->emit_media_timeline)
        snprintf(mediat, sizeof mediat, "on:%u", cfg->media_timeline_history_groups);
    else snprintf(mediat, sizeof mediat, "off");
    char refs[256] = "";
    for (size_t i = 0; i < cfg->content_protection_ref_id_count && i < 4; i++) {
        char one[64];
        snprintf(one, sizeof one, "%s%.*s", i ? "," : "", SPAN(cfg->content_protection_ref_ids[i]));
        strncat(refs, one, sizeof refs - strlen(refs) - 1);
    }
    /* The MPEG-TS record of the first extension entry, key by key: mode,
     * packet size, then each optional key as its value or '-'. */
    char ts[256] = "none";
    if (cfg->extension_count && cfg->extensions && cfg->extensions[0].kind == MOQ_MSF_EXT_MPEG2TS &&
        cfg->extensions[0].u.mpeg2ts) {
        const moq_msf_mpeg2ts_t *m = cfg->extensions[0].u.mpeg2ts;
        char es[16] = "-", prog[16] = "-", pcr[16] = "-", mux[24] = "-", ra[4] = "-", tsm[8] = "-",
             scte[16] = "-", si[96] = "-";
        if (m->has_es_pid) snprintf(es, sizeof es, "%u", m->es_pid);
        if (m->has_program_number) snprintf(prog, sizeof prog, "%u", m->program_number);
        if (m->has_pcr_pid) snprintf(pcr, sizeof pcr, "%u", m->pcr_pid);
        if (m->has_mux_rate) snprintf(mux, sizeof mux, "%" PRIu64, m->mux_rate);
        if (m->has_random_access) snprintf(ra, sizeof ra, "%d", m->random_access ? 1 : 0);
        if (m->has_timestamp_mode) snprintf(tsm, sizeof tsm, "%d", (int)m->timestamp_mode);
        if (m->has_scte35_pid) snprintf(scte, sizeof scte, "%u", m->scte35_pid);
        if (m->has_si_pids) {
            si[0] = 0;
            for (size_t i = 0; i < m->si_pid_count && i < 8; i++) {
                char one[12];
                snprintf(one, sizeof one, "%s%u", i ? "+" : "", m->si_pids[i]);
                strncat(si, one, sizeof si - strlen(si) - 1);
            }
            if (!si[0]) snprintf(si, sizeof si, "empty");
        }
        snprintf(ts, sizeof ts, "%d:%u:%s:%s:%s:%s:%s:%s:%s:%s:struct=%u",
                 m->has_mode ? (int)m->mode : -1, m->has_packet_size ? m->packet_size : 0, es, prog,
                 pcr, mux, si, ra, tsm, scte, m->struct_size);
    }
    char props[80] = "";
    for (size_t i = 0; i < cfg->track_properties.len && i < 32; i++) {
        char one[4];
        snprintf(one, sizeof one, "%02x", cfg->track_properties.data[i]);
        strncat(props, one, sizeof props - strlen(props) - 1);
    }
    logf_("add_track name=%.*s media_type=%d packaging=%u codec=%.*s timescale=%u "
          "init_data=%zu role=%.*s lang=%.*s is_live=%d dims=%ux%u framerate_millis=%" PRIu64
          " samplerate=%u channel_config=%.*s bitrate=%" PRIu64 " max_sap=%s refs=%zu[%s] "
          "sap_timeline=%s duration=%s media_timeline=%s alt_group=%s priority=%s "
          "extensions=%zu mpeg2ts_app_objects=%d track_properties=%zu dynamic_groups=%d "
          "render_group=%s label=%s mpeg2ts=%s object_packets=%u min_group_ms=%u forward=%d "
          "properties=[%s] struct_size=%u",
          SPAN(cfg->name), (int)cfg->media_type, cfg->packaging, SPAN(cfg->codec), cfg->timescale,
          cfg->init_data.len, SPAN(cfg->role), SPAN(cfg->lang), cfg->is_live ? 1 : 0, cfg->width,
          cfg->height, cfg->framerate_millis, cfg->samplerate, SPAN(cfg->channel_config),
          cfg->bitrate, maxsap, cfg->content_protection_ref_id_count, refs, sapt, duration, mediat,
          alt, prio, cfg->extension_count, cfg->mpeg2ts_app_objects ? 1 : 0,
          cfg->track_properties.len, cfg->dynamic_groups ? 1 : 0, render, label, ts,
          cfg->mpeg2ts_object_packets, cfg->mpeg2ts_min_group_ms,
          cfg->mpeg2ts_forward_before_random_access ? 1 : 0, props, cfg->struct_size);
    moq_result_t rc = (moq_result_t)get("add_track_result");
    if (rc != MOQ_OK) return rc;
    if (s->track_count == FAKE_TRACKS) return MOQ_ERR_NOMEM;
    fake_track_t *t = &s->tracks[s->track_count++];
    t->sender = s;
    t->used = true;
    *out = (moq_media_track_t *)t;
    return MOQ_OK;
}

static int sender_track(moq_media_sender_t *s, const moq_media_track_t *t)
{
    int i = track_index(s->tracks, t);
    return (i >= 0 && s->tracks[i].used) ? i : -1;
}

moq_result_t moq_media_sender_remove_track(moq_media_sender_t *s, moq_media_track_t *track)
{
    if (!s) return MOQ_ERR_INVAL;
    int i = sender_track(s, track);
    if (i < 0) return MOQ_ERR_INVAL;
    logf_("remove_track %d", i);
    moq_result_t rc = (moq_result_t)get("remove_track_result");
    if (rc != MOQ_OK) return rc;
    if (s->tracks[i].removed) return MOQ_ERR_WRONG_STATE;
    s->tracks[i].removed = true;
    return MOQ_OK;
}

moq_result_t moq_media_sender_write(moq_media_sender_t *s, moq_media_track_t *track,
                                    const moq_media_send_object_t *obj)
{
    if (!s || !obj) return MOQ_ERR_INVAL;
    int i = sender_track(s, track);
    if (i < 0) return MOQ_ERR_INVAL;
    bump("write");
    char capture[32], sap[16], props[32];
    if (obj->has_capture_time) snprintf(capture, sizeof capture, "%" PRIu64, obj->capture_time_us);
    else snprintf(capture, sizeof capture, "none");
    if (obj->has_sap_type) snprintf(sap, sizeof sap, "%d", (int)obj->sap_type);
    else snprintf(sap, sizeof sap, "none");
    if (obj->properties) snprintf(props, sizeof props, "%zu", moq_rcbuf_len(obj->properties));
    else snprintf(props, sizeof props, "none");
    /* The metadata record as given: its stamps, presence bits, then each
     * value (marking bytes in hex, configurations as their lengths). */
    char meta[200] = "none";
    if (obj->metadata) {
        const moq_media_metadata_t *m = obj->metadata;
        char mark[12] = "";
        for (size_t k = 0; k < m->loc.marking_len && k < 4; k++) {
            char one[4];
            snprintf(one, sizeof one, "%02x", m->loc.marking[k]);
            strncat(mark, one, sizeof mark - strlen(mark) - 1);
        }
        snprintf(meta, sizeof meta,
                 "struct=%u:loc_struct=%u:present=%u:ts=%" PRIu64 ":scale=%" PRIu64
                 ":mark=%s:level=%u:video=%zu:audio=%zu",
                 m->struct_size, m->loc.struct_size, m->loc.present, m->loc.timestamp,
                 m->loc.timescale, mark, m->loc.audio_level, m->loc.video_config.len,
                 m->loc.audio_config.len);
    }
    logf_("write track=%d len=%zu props=%s sync=%d starts=%d ends=%d dts=%" PRIu64
          " pts=%" PRIu64 " capture=%s sap=%s metadata=%d meta=%s struct_size=%u",
          i, moq_rcbuf_len(obj->payload), props, obj->is_sync ? 1 : 0, obj->starts_group ? 1 : 0,
          obj->ends_group ? 1 : 0, obj->decode_time_us, obj->presentation_time_us, capture, sap,
          obj->metadata ? 1 : 0, meta, obj->struct_size);
    if (s->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    if (s->tracks[i].removed || s->tracks[i].ended) return MOQ_ERR_WRONG_STATE;
    moq_result_t rc = (moq_result_t)get("write_result");
    if (rc != MOQ_OK) return rc;
    /* Transferred: the service owns the references now and releases them
     * once sent; here, at once. */
    moq_rcbuf_decref(obj->payload);
    moq_rcbuf_decref(obj->properties);
    return MOQ_OK;
}

moq_result_t moq_media_sender_end_track(moq_media_sender_t *s, moq_media_track_t *track)
{
    if (!s) return MOQ_ERR_INVAL;
    int i = sender_track(s, track);
    if (i < 0) return MOQ_ERR_INVAL;
    logf_("end_track %d", i);
    if (s->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    moq_result_t rc = (moq_result_t)get("end_track_result");
    if (rc != MOQ_OK) return rc;
    s->tracks[i].ended = true;
    return MOQ_OK;
}

moq_result_t moq_media_sender_complete(moq_media_sender_t *s)
{
    if (!s) return MOQ_ERR_INVAL;
    logf_("complete");
    if (s->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    return (moq_result_t)get("complete_result");
}

moq_result_t moq_media_sender_convert_to_vod(moq_media_sender_t *s,
                                             const moq_media_vod_track_t *items, size_t count)
{
    if (!s || (!items && count)) return MOQ_ERR_INVAL;
    char list[256] = "";
    for (size_t k = 0; k < count && k < 4; k++) {
        int i = sender_track(s, items[k].track);
        if (i < 0) return MOQ_ERR_INVAL;
        char one[48];
        snprintf(one, sizeof one, "%s%d:%" PRIu64, k ? "," : "", i, items[k].duration_ms);
        strncat(list, one, sizeof list - strlen(list) - 1);
    }
    logf_("convert_to_vod %s", list);
    if (count == 0) return MOQ_ERR_INVAL;
    return (moq_result_t)get("convert_result");
}

moq_result_t moq_media_sender_get_stats(const moq_media_sender_t *s,
                                        moq_media_sender_stats_t *out, size_t out_size)
{
    if (!s || !out) return MOQ_ERR_INVAL;
    moq_result_t rc = (moq_result_t)get("sender_stats_result");
    if (rc != MOQ_OK) return rc;
    moq_media_sender_stats_t st = {
        .objects_written = 7, .objects_sent = 4, .objects_queued = 2, .bytes_queued = 300,
        .objects_dropped = 1, .groups_dropped = 1, .keyframes_dropped = 0, .groups_abandoned = 1,
        .backpressure_stalls = 3,
        .last_error = (moq_result_t)get("stats_last_error"),
        .sap_records_evicted = (uint64_t)get("stats_sap_evicted"),
        .completion_error = (moq_result_t)get("stats_completion_error"),
    };
    size_t n = out_size < sizeof st ? out_size : sizeof st;
    memcpy(out, &st, n);
    out->struct_size = stamp_for("sender_stats_stamp", out_size, sizeof st);
    return MOQ_OK;
}

/* The bytes taken are scripted: mpeg2ts_consumed when set (and at most the
 * input), else all of it; the result likewise, written with the count on
 * every return, as the real call does. */
moq_result_t moq_media_sender_write_mpeg2ts(moq_media_sender_t *s, moq_media_track_t *track,
                                            const moq_media_send_mpeg2ts_t *in, size_t *consumed)
{
    if (consumed) *consumed = 0;
    if (!s || !in || !consumed) return MOQ_ERR_INVAL;
    int i = sender_track(s, track);
    if (i < 0) return MOQ_ERR_INVAL;
    bump("write_mpeg2ts");
    logf_("write_mpeg2ts track=%d len=%zu end_object=%d first=%d struct_size=%u", i, in->data.len,
          in->end_object ? 1 : 0, in->data.len ? in->data.data[0] : -1, in->struct_size);
    if (in->struct_size < offsetof(moq_media_send_mpeg2ts_t, end_object) + sizeof in->end_object)
        return MOQ_ERR_INVAL;
    int64_t take = get("mpeg2ts_consumed");
    size_t n = take > 0 && (size_t)take < in->data.len ? (size_t)take : in->data.len;
    if (take < 0) n = 0;
    *consumed = n;
    if (s->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    return (moq_result_t)get("write_mpeg2ts_result");
}

moq_result_t moq_media_sender_get_track_stats(const moq_media_sender_t *s,
                                              const moq_media_track_t *track,
                                              moq_media_sender_track_stats_t *out, size_t out_size)
{
    if (!s || !track || !out) return MOQ_ERR_INVAL;
    int i = track_index(s->tracks, track);
    if (i < 0 || !s->tracks[i].used) return MOQ_ERR_INVAL;
    logf_("track_stats track=%d out_size=%zu", i, out_size);
    moq_result_t rc = (moq_result_t)get("track_stats_result");
    if (rc != MOQ_OK) return rc;
    moq_media_sender_track_stats_t st = {
        .objects_written = 9, .objects_sent = 5, .objects_queued = 3, .objects_dropped = 1,
        .bytes_queued = 600, .groups_dropped = 1, .keyframes_dropped = 1, .groups_abandoned = 2,
        .mpeg2ts_groups = 4, .mpeg2ts_objects = 12, .mpeg2ts_packets_in = 48,
        .mpeg2ts_packets_dropped_before_random_access = 3, .mpeg2ts_max_group_27mhz = 27000000,
        .mpeg2ts_groups_over_2s = 0, .mpeg2ts_peak_rate_bps = 4000000,
        .mpeg2ts_source_pcr_discontinuities = 1, .mpeg2ts_mux_rate_below_peak = true,
        .mpeg2ts_program_gone = get("track_stats_program_gone") != 0,
        .mpeg2ts_objects_parked = (uint64_t)get("track_stats_parked"),
        .mpeg2ts_partial_bytes_discarded = 7, .mpeg2ts_continuation_objects_discarded = 2,
    };
    size_t n = out_size < sizeof st ? out_size : sizeof st;
    memcpy(out, &st, n);
    out->struct_size = stamp_for("track_stats_stamp", out_size, sizeof st);
    return MOQ_OK;
}

/* A pending request (new_group_pending) is taken once: it clears, as the
 * real one does, and its value is new_group_value. */
moq_result_t moq_media_sender_take_new_group_request(moq_media_sender_t *s,
                                                     moq_media_track_t *track,
                                                     uint64_t *requested_group)
{
    if (!s) return MOQ_ERR_INVAL;
    int i = sender_track(s, track);
    if (i < 0) return MOQ_ERR_INVAL;
    logf_("take_new_group_request track=%d out=%d", i, requested_group ? 1 : 0);
    if (!get("new_group_pending")) return MOQ_DONE;
    knob("new_group_pending")->value = 0;
    if (requested_group) *requested_group = (uint64_t)get("new_group_value");
    return MOQ_OK;
}

bool moq_media_sender_is_ready(const moq_media_sender_t *s) { (void)s; return get("sender_ready") != 0; }

moq_result_t moq_media_sender_wait(moq_media_sender_t *s, uint64_t timeout_us)
{
    if (!s) return MOQ_ERR_INVAL;
    logf_("sender_wait %" PRIu64, timeout_us);
    if (s->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    return (moq_result_t)get("sender_wait_result");
}

size_t moq_media_sender_track_subscriptions(const moq_media_sender_t *s,
                                            const moq_media_track_t *track)
{
    if (!s || track_index(s->tracks, track) < 0) return 0;
    return (size_t)get("track_subscriptions");
}

bool moq_media_sender_track_has_subscriber(const moq_media_sender_t *s,
                                           const moq_media_track_t *track)
{
    return moq_media_sender_track_subscriptions(s, track) > 0;
}

bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s)
{
    (void)s;
    return get("has_media_subscriber") != 0;
}

bool moq_media_sender_is_closed(const moq_media_sender_t *s) { (void)s; return get("sender_is_closed") != 0; }
bool moq_media_sender_is_fatal(const moq_media_sender_t *s) { (void)s; return get("sender_is_fatal") != 0; }
uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s)
{
    (void)s;
    return (uint64_t)get("sender_fatal_code");
}

/* -- Receiver ----------------------------------------------------------- */

static void receiver_cfg_init(moq_media_receiver_cfg_t *cfg, size_t cfg_size,
                              moq_media_overflow_policy_t policy)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof *cfg ? cfg_size : sizeof *cfg;
    if (n < sizeof cfg->struct_size) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_receiver_cfg_t, overflow) + sizeof cfg->overflow)
        cfg->overflow.policy = policy;
    if (n >= offsetof(moq_media_receiver_cfg_t, companion_profile) + sizeof cfg->companion_profile)
        cfg->companion_profile = MOQ_MEDIA_COMPANION_AUTO;
}

void moq_media_receiver_cfg_init_sized(moq_media_receiver_cfg_t *cfg, size_t cfg_size)
{
    receiver_cfg_init(cfg, cfg_size, MOQ_MEDIA_OVERFLOW_UNSET);
}
void moq_media_receiver_cfg_init_live_sized(moq_media_receiver_cfg_t *cfg, size_t cfg_size)
{
    receiver_cfg_init(cfg, cfg_size, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME);
}
void moq_media_receiver_cfg_init_flow_control_sized(moq_media_receiver_cfg_t *cfg,
                                                    size_t cfg_size)
{
    receiver_cfg_init(cfg, cfg_size, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL);
}

void moq_media_receiver_track_subscribe_cfg_init_sized(
    moq_media_receiver_track_subscribe_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof cfg->struct_size) return;
    size_t n = cfg_size < sizeof *cfg ? cfg_size : sizeof *cfg;
    memset(cfg, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(cfg, &stamp, sizeof stamp);
}

moq_result_t moq_media_receiver_attach(moq_endpoint_t *ep, const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    if (!ep || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    bump("receiver_attach");
    if (cfg->endpoint) return MOQ_ERR_INVAL;
    if (cfg->namespace_.count == 0 || !cfg->namespace_.parts) return MOQ_ERR_INVAL;
    if (cfg->overflow.policy == MOQ_MEDIA_OVERFLOW_UNSET) return MOQ_ERR_INVAL;
    char ns[512];
    log_namespace(ns, sizeof ns, cfg->namespace_);
    char terminals[24];
    if (cfg->surface_stream_terminals)
        snprintf(terminals, sizeof terminals, "on:%u", cfg->max_stream_terminals);
    else
        snprintf(terminals, sizeof terminals, "off");
    logf_("receiver_attach ns=%s catalog=%.*s auto=%d time_mode=%d policy=%d max_objects=%u "
          "max_bytes=%" PRIu64 " max_events=%u mpeg2ts=%u/%" PRIu64 "/%d/%d companion=%u "
          "terminals=%s struct_size=%u",
          ns, SPAN(cfg->catalog_track), cfg->auto_subscribe ? 1 : 0, (int)cfg->time_mode,
          (int)cfg->overflow.policy, cfg->overflow.max_objects, cfg->overflow.max_bytes,
          cfg->max_track_events, cfg->mpeg2ts_reorder_slots, cfg->mpeg2ts_reorder_window_us,
          cfg->mpeg2ts_strip_prefix ? 1 : 0, cfg->mpeg2ts_join_psi ? 1 : 0,
          cfg->companion_profile, terminals, cfg->struct_size);
    moq_result_t rc = (moq_result_t)get("receiver_attach_result");
    if (rc != MOQ_OK) return rc;
    moq_media_receiver_t *r = fake_alloc(sizeof *r);
    if (!r) return MOQ_ERR_NOMEM;
    r->ep = ep;
    for (int i = 0; i < FAKE_TRACKS; i++) {
        r->tracks[i].receiver = r;
        r->tracks[i].used = true;
    }
    ep->attachments++;
    g_receiver = r;
    *out = r;
    return MOQ_OK;
}

void moq_media_receiver_destroy(moq_media_receiver_t *r)
{
    if (!r) return;
    bump("receiver_destroy");
    logf_("receiver_destroy");
    /* Queued objects still own their references. */
    while (r->object_count) {
        moq_media_object_t *o = &r->objects[r->object_head];
        moq_rcbuf_decref(o->payload_ref);
        moq_rcbuf_decref(o->properties_ref);
        moq_rcbuf_decref(o->track_properties_ref);
        r->object_head = (r->object_head + 1) % FAKE_QUEUE;
        r->object_count--;
    }
    r->ep->attachments--;
    if (g_receiver == r) g_receiver = NULL;
    fake_free(r, sizeof *r);
}

moq_media_track_t *moq5_fake_receiver_track(moq_media_receiver_t *r, size_t index)
{
    if (!r || index >= FAKE_TRACKS) abort();
    return (moq_media_track_t *)&r->tracks[index];
}

void moq5_fake_set_track_desc(moq_media_receiver_t *r, size_t index,
                              const moq_media_track_desc_t *desc)
{
    if (!r || index >= FAKE_TRACKS) abort();
    r->tracks[index].desc = *desc;
    r->tracks[index].has_desc = true;
}

void moq5_fake_set_track_state(moq_media_receiver_t *r, size_t index, uint32_t state)
{
    if (!r || index >= FAKE_TRACKS) abort();
    r->tracks[index].state = state;
}

#define PUSH(queue, count, head, item) \
    do { \
        moq_media_receiver_t *r_ = g_receiver; \
        if (!r_ || r_->count == FAKE_QUEUE) abort(); \
        r_->queue[(r_->head + r_->count) % FAKE_QUEUE] = *(item); \
        r_->count++; \
    } while (0)

void moq5_fake_push_track_event(const moq_media_track_event_t *ev) { PUSH(events, event_count, event_head, ev); }
void moq5_fake_push_object(const moq_media_object_t *obj) { PUSH(objects, object_count, object_head, obj); }
void moq5_fake_push_stream_terminal(const moq_media_stream_terminal_t *t)
{
    PUSH(terminals, terminal_count, terminal_head, t);
}
void moq5_fake_push_sap(const moq_media_sap_record_t *rec) { PUSH(saps, sap_count, sap_head, rec); }
void moq5_fake_push_timeline(const moq_media_timeline_record_t *rec)
{
    PUSH(timelines, timeline_count, timeline_head, rec);
}

static int receiver_track(const moq_media_receiver_t *r, const moq_media_track_t *t)
{
    return track_index(r->tracks, t);
}

moq_result_t moq_media_receiver_track_desc_copy(const moq_media_receiver_t *r,
                                                const moq_media_track_t *track,
                                                moq_media_track_desc_t *out, size_t out_size)
{
    if (!r || !track || !out || out_size < MOQ_MEDIA_TRACK_DESC_V0_SIZE) return MOQ_ERR_INVAL;
    int i = receiver_track(r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    bump("desc_copy");
    logf_("desc_copy track=%d out_size=%zu", i, out_size);
    moq_result_t rc = (moq_result_t)get("desc_result");
    if (rc != MOQ_OK) return rc;
    const moq_media_track_desc_t *d = &r->tracks[i].desc;
    size_t n = out_size < sizeof *d ? out_size : sizeof *d;
    memcpy(out, d, n);
    out->struct_size = stamp_for("desc_stamp", out_size, sizeof *d);
    return MOQ_OK;
}

const moq_cmsf_content_protection_t *moq_media_receiver_find_content_protection(
    const moq_media_receiver_t *r, moq_bytes_t ref_id)
{
    if (!r) return NULL;
    logf_("find_cp %.*s", SPAN(ref_id));
    if (!g_cp) return NULL;
    if (g_cp->ref_id.len == ref_id.len &&
        (ref_id.len == 0 || memcmp(g_cp->ref_id.data, ref_id.data, ref_id.len) == 0))
        return g_cp;
    return NULL;
}

/* A location filter record as kind:relative:start:end, or "none". */
static void log_location(char *buf, size_t cap, const moq_location_filter_cfg_t *l)
{
    if (!l) {
        snprintf(buf, cap, "none");
        return;
    }
    snprintf(buf, cap, "%u:%" PRIu64 ":%" PRIu64 ".%" PRIu64 ":%" PRIu64 ".%" PRIu64 ":struct=%u",
             l->kind, l->relative_group_offset, l->start_group, l->start_object, l->end_group,
             l->end_object, l->struct_size);
}

moq_result_t moq_media_receiver_subscribe_track(moq_media_receiver_t *r, moq_media_track_t *track,
                                                const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    int i = receiver_track(r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    if (cfg) {
        char prio[16];
        if (cfg->has_priority) snprintf(prio, sizeof prio, "%u", cfg->priority);
        else snprintf(prio, sizeof prio, "none");
        char loc[120], fill[200], fill_loc[120];
        log_location(loc, sizeof loc, cfg->location);
        if (cfg->fill) {
            const moq_fill_cfg_t *f = cfg->fill;
            log_location(fill_loc, sizeof fill_loc, f->location);
            char fprio[8] = "-", order[8] = "-", timeout[24] = "-";
            if (f->has_subscriber_priority) snprintf(fprio, sizeof fprio, "%u", f->subscriber_priority);
            if (f->has_group_order) snprintf(order, sizeof order, "%u", f->group_order);
            if (f->has_fill_timeout) snprintf(timeout, sizeof timeout, "%" PRIu64, f->fill_timeout_ms);
            snprintf(fill, sizeof fill, "%s/%s/%s/%s/struct=%u", fprio, order, timeout, fill_loc,
                     f->struct_size);
        } else {
            snprintf(fill, sizeof fill, "none");
        }
        logf_("subscribe track=%d start=%d priority=%s program=%u location=%d fill=%d "
              "loc=%s fill_cfg=%s struct_size=%u",
              i, (int)cfg->start, prio, cfg->mpeg2ts_program_number, cfg->location ? 1 : 0,
              cfg->fill ? 1 : 0, loc, fill, cfg->struct_size);
        if (cfg->struct_size < MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    } else {
        logf_("subscribe track=%d config=none", i);
    }
    return (moq_result_t)get("subscribe_result");
}

moq_result_t moq_media_receiver_unsubscribe_track(moq_media_receiver_t *r,
                                                  moq_media_track_t *track)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    int i = receiver_track(r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    logf_("unsubscribe track=%d", i);
    return (moq_result_t)get("unsubscribe_result");
}

moq_result_t moq_media_receiver_track_state(const moq_media_receiver_t *r,
                                            const moq_media_track_t *track,
                                            moq_media_track_state_t *out)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;
    int i = receiver_track(r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    *out = (moq_media_track_state_t)r->tracks[i].state;
    return MOQ_OK;
}

void moq_media_object_cleanup(moq_media_object_t *obj)
{
    if (!obj) return;
    bump("object_cleanup");
    size_t n = obj->struct_size;
    if (n >= offsetof(moq_media_object_t, payload_ref) + sizeof obj->payload_ref) {
        moq_rcbuf_decref(obj->payload_ref);
        obj->payload_ref = NULL;
    }
    if (n >= offsetof(moq_media_object_t, properties_ref) + sizeof obj->properties_ref) {
        moq_rcbuf_decref(obj->properties_ref);
        obj->properties_ref = NULL;
    }
    if (n >= offsetof(moq_media_object_t, track_properties_ref) +
                 sizeof obj->track_properties_ref) {
        moq_rcbuf_decref(obj->track_properties_ref);
        obj->track_properties_ref = NULL;
    }
}

/* Only poll_object consults the interrupt latch, as the real one does. */
#define POLL(queue, count, head, result_knob, stamp_knob, type) \
    do { \
        if (!r || !out) return MOQ_ERR_INVAL; \
        if (r->count == 0) return (moq_result_t)get(result_knob); \
        type *item = &r->queue[r->head]; \
        size_t n = out_size < sizeof *item ? out_size : sizeof *item; \
        memcpy(out, item, n); \
        out->struct_size = stamp_for(stamp_knob, out_size, sizeof *item); \
        r->head = (r->head + 1) % FAKE_QUEUE; \
        r->count--; \
        return MOQ_OK; \
    } while (0)

moq_result_t moq_media_receiver_poll_track(moq_media_receiver_t *r, moq_media_track_event_t *out,
                                           size_t out_size)
{
    if (r && out && out_size < offsetof(moq_media_track_event_t, config_generation) +
                                    sizeof out->config_generation)
        return MOQ_ERR_INVAL;
    bump("poll_track");
    POLL(events, event_count, event_head, "poll_track_result", "poll_track_stamp",
         moq_media_track_event_t);
}

moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r, moq_media_object_t *out,
                                            size_t out_size)
{
    if (r && out && out_size < offsetof(moq_media_object_t, samples_owned) +
                                    sizeof out->samples_owned)
        return MOQ_ERR_INVAL;
    bump("poll_object");
    if (r && r->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    POLL(objects, object_count, object_head, "poll_object_result", "poll_object_stamp",
         moq_media_object_t);
}

moq_result_t moq_media_receiver_poll_stream_terminal(moq_media_receiver_t *r,
                                                     moq_media_stream_terminal_t *out,
                                                     size_t out_size)
{
    if (r && out && out_size < MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    bump("poll_terminal");
    POLL(terminals, terminal_count, terminal_head, "poll_terminal_result",
         "poll_terminal_stamp", moq_media_stream_terminal_t);
}

moq_result_t moq_media_receiver_poll_sap(moq_media_receiver_t *r, moq_media_sap_record_t *out,
                                         size_t out_size)
{
    if (r && out && out_size < sizeof *out) return MOQ_ERR_INVAL;
    POLL(saps, sap_count, sap_head, "poll_sap_result", "poll_sap_stamp", moq_media_sap_record_t);
}

moq_result_t moq_media_receiver_poll_media_timeline(moq_media_receiver_t *r,
                                                    moq_media_timeline_record_t *out,
                                                    size_t out_size)
{
    if (r && out && out_size < sizeof *out) return MOQ_ERR_INVAL;
    POLL(timelines, timeline_count, timeline_head, "poll_timeline_result",
         "poll_timeline_stamp", moq_media_timeline_record_t);
}

moq_result_t moq_media_receiver_wait(moq_media_receiver_t *r, uint64_t timeout_us)
{
    if (!r) return MOQ_ERR_INVAL;
    logf_("receiver_wait %" PRIu64, timeout_us);
    if (r->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    if (r->event_count || r->object_count || r->terminal_count || r->sap_count ||
        r->timeline_count)
        return MOQ_OK;
    return (moq_result_t)get("receiver_wait_result");
}

moq_result_t moq_media_receiver_get_stats(const moq_media_receiver_t *r,
                                          moq_media_receiver_stats_t *out, size_t out_size)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    moq_result_t rc = (moq_result_t)get("receiver_stats_result");
    if (rc != MOQ_OK) return rc;
    moq_media_receiver_stats_t st = {
        .objects_received = 11, .objects_queued = 3, .bytes_queued = 900, .objects_dropped = 2,
        .groups_dropped = 1, .keyframes_dropped = 0, .parse_drops = 1, .overflow_events = 2,
        .pause_transitions = 4, .paused = get("rstats_paused") != 0,
        .catalog_drops = (uint64_t)get("rstats_catalog_drops"),
        .catalog_complete = get("rstats_catalog_complete") != 0,
        .gaps_reported = 5, .gap_tails_unknown = 1, .reorder_refused = 2,
        .tracks_malformed = (uint64_t)get("rstats_tracks_malformed"),
    };
    size_t n = out_size < sizeof st ? out_size : sizeof st;
    memcpy(out, &st, n);
    out->struct_size = stamp_for("receiver_stats_stamp", out_size, sizeof st);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_cancel_fill(moq_media_receiver_t *r, moq_media_track_t *track,
                                            uint64_t initiating_request_id)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    int i = receiver_track(r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    bump("cancel_fill");
    logf_("cancel_fill track=%d id=%" PRIu64, i, initiating_request_id);
    return (moq_result_t)get("cancel_fill_result");
}

/* What a polled object holds, from what it owns: its buffers' bytes and its
 * owned sample table, each read only when the stamp covers it. */
uint64_t moq_media_object_charge(const moq_media_object_t *obj)
{
    if (!obj) return 0;
    size_t n = obj->struct_size;
    uint64_t charge = 0;
    if (n >= offsetof(moq_media_object_t, payload_ref) + sizeof obj->payload_ref)
        charge += moq_rcbuf_len(obj->payload_ref);
    if (n >= offsetof(moq_media_object_t, properties_ref) + sizeof obj->properties_ref)
        charge += moq_rcbuf_len(obj->properties_ref);
    if (n >= offsetof(moq_media_object_t, samples_owned) + sizeof obj->samples_owned &&
        obj->samples_owned)
        charge += (uint64_t)obj->sample_count * sizeof(moq_cmaf_sample_t);
    if (n >= offsetof(moq_media_object_t, track_properties_ref) + sizeof obj->track_properties_ref)
        charge += moq_rcbuf_len(obj->track_properties_ref);
    return charge;
}

moq_result_t moq_media_receiver_poll_object_within(moq_media_receiver_t *r, uint64_t max_charge,
                                                   uint64_t *charge, moq_media_object_t *out,
                                                   size_t out_size)
{
    if (r && out && out_size < offsetof(moq_media_object_t, samples_owned) +
                                    sizeof out->samples_owned)
        return MOQ_ERR_INVAL;
    if (!r || !out) return MOQ_ERR_INVAL;
    bump("poll_object_within");
    logf_("poll_object_within max_charge=%" PRIu64 " charge_out=%d", max_charge, charge ? 1 : 0);
    if (r->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    if (r->object_count == 0) return (moq_result_t)get("poll_object_result");
    moq_media_object_t *head = &r->objects[r->object_head];
    /* A queued record is unstamped: it is charged as the whole record. */
    moq_media_object_t whole = *head;
    whole.struct_size = (uint32_t)sizeof whole;
    uint64_t need = moq_media_object_charge(&whole);
    if (charge) *charge = need;
    if (need > max_charge) return MOQ_ERR_BUFFER;
    size_t n = out_size < sizeof *head ? out_size : sizeof *head;
    memcpy(out, head, n);
    out->struct_size = stamp_for("poll_object_stamp", out_size, sizeof *head);
    r->object_head = (r->object_head + 1) % FAKE_QUEUE;
    r->object_count--;
    return MOQ_OK;
}

moq_result_t moq_media_receiver_get_info(const moq_media_receiver_t *r,
                                         moq_media_receiver_info_t *out, size_t out_size)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_RECEIVER_INFO_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    logf_("get_info out_size=%zu", out_size);
    moq_media_receiver_info_t info = {
        .time_mode = (uint32_t)get("info_time_mode"),
        .overflow_policy = (uint32_t)get("info_overflow_policy"),
        .max_objects = 256,
        .max_bytes = 33554432,
        .auto_subscribe = get("info_auto_subscribe") != 0,
        .surface_stream_terminals = get("info_terminals") != 0,
        .companion_profile = (moq_media_companion_profile_t)get("info_companion"),
        .transport_version = (uint32_t)get("info_version"),
        .max_stream_terminals = (uint32_t)get("info_terminals"),
        .track_events_polled = (uint64_t)get("poll_track_polled"),
    };
    size_t n = out_size < sizeof info ? out_size : sizeof info;
    memcpy(out, &info, n);
    out->struct_size = stamp_for("info_stamp", out_size, sizeof info);
    return MOQ_OK;
}

bool moq_media_receiver_is_closed(const moq_media_receiver_t *r) { (void)r; return get("receiver_is_closed") != 0; }
bool moq_media_receiver_is_fatal(const moq_media_receiver_t *r) { (void)r; return get("receiver_is_fatal") != 0; }
uint64_t moq_media_receiver_fatal_code(const moq_media_receiver_t *r)
{
    (void)r;
    return (uint64_t)get("receiver_fatal_code");
}

/* -- Player (an SDK with the player) ------------------------------------ *
 * Compiled when run.sh finds the SDK's media_player.h. Outputs and events
 * are pushed by the tests; an output's references pass to the poller and
 * come back through output_cleanup, which counts what it releases. The
 * player's create is deliberately absent: the binding attaches only. */
#ifdef MOQ5_FAKE_PLAYER
#include <moq/media_player.h>

struct moq_media_player {
    moq_media_receiver_t *r;
    moq_media_player_output_t outputs[FAKE_QUEUE];
    size_t output_head, output_count;
    moq_media_player_event_t events[FAKE_QUEUE];
    size_t event_head, event_count;
};

static void cfg_clear(void *cfg, size_t cfg_size, size_t lib_size)
{
    if (!cfg || cfg_size < sizeof(uint32_t)) return;
    size_t n = cfg_size < lib_size ? cfg_size : lib_size;
    memset(cfg, 0, n);
    uint32_t stamp = (uint32_t)n;
    memcpy(cfg, &stamp, sizeof stamp);
}

void moq_media_player_cfg_init_sized(moq_media_player_cfg_t *cfg, size_t cfg_size)
{
    cfg_clear(cfg, cfg_size, sizeof *cfg);
}

void moq_media_player_select_cfg_init_sized(moq_media_player_select_cfg_t *cfg, size_t cfg_size)
{
    cfg_clear(cfg, cfg_size, sizeof *cfg);
}

void moq_media_player_feedback_init_sized(moq_media_player_feedback_t *fb, size_t fb_size)
{
    cfg_clear(fb, fb_size, sizeof *fb);
}

moq_result_t moq_media_player_attach(moq_media_receiver_t *r, const moq_media_player_cfg_t *cfg,
                                     moq_media_player_t **out)
{
    if (!r || !cfg || !out) return MOQ_ERR_INVAL;
    *out = NULL;
    bump("player_attach");
    char playback[200] = "none";
    if (cfg->playback) {
        const moq_playback_cfg_t *p = cfg->playback;
        snprintf(playback, sizeof playback,
                 "%u/%u/%" PRIu64 "/%u/%u/%u/%" PRIu64 "/%u/%u/%u/%u/struct=%u", p->max_tracks,
                 p->max_buffered_objects, p->max_buffered_bytes, p->max_backlog_groups,
                 p->max_commands, p->max_events, p->gap_timeout_us, p->max_release_per_tick,
                 p->max_track_config_bytes, p->max_samples_per_object, p->max_known_ranges,
                 p->struct_size);
    }
    logf_("player_attach selection=%u receiver_cfg=%d playback=%s lang=%.*s tail_us=%" PRIu64
          " cushion_us=%" PRIu64 " outputs=%u events=%u timing=%u held=%" PRIu64
          " shared_epoch=%d struct_size=%u",
          cfg->selection, cfg->receiver ? 1 : 0, playback, SPAN(cfg->preferred_lang),
          cfg->end_tail_wait_us, cfg->cushion_us, cfg->max_outputs, cfg->max_events,
          cfg->timing_policy, cfg->max_held_bytes, cfg->assume_shared_epoch ? 1 : 0,
          cfg->struct_size);
    if (cfg->struct_size < MOQ_MEDIA_PLAYER_CFG_V0_SIZE || cfg->receiver) return MOQ_ERR_INVAL;
    moq_result_t rc = (moq_result_t)get("player_attach_result");
    if (rc != MOQ_OK) return rc;
    moq_media_player_t *p = fake_alloc(sizeof *p);
    if (!p) return MOQ_ERR_NOMEM;
    p->r = r;
    g_player = p;
    *out = p;
    return MOQ_OK;
}

void moq_media_player_output_cleanup(moq_media_player_output_t *out)
{
    if (!out) return;
    bump("output_cleanup");
    switch (out->kind) {
    case MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE:
        if (out->u.configure.codec) bump("output_refs_released");
        if (out->u.configure.codec_config) bump("output_refs_released");
        moq_rcbuf_decref(out->u.configure.codec);
        moq_rcbuf_decref(out->u.configure.codec_config);
        out->u.configure.codec = NULL;
        out->u.configure.codec_config = NULL;
        break;
    case MOQ_MEDIA_PLAYER_OUTPUT_UNIT:
        if (out->u.unit.payload) bump("output_refs_released");
        moq_rcbuf_decref(out->u.unit.payload);
        out->u.unit.payload = NULL;
        break;
    default:
        break;
    }
}

void moq_media_player_destroy(moq_media_player_t *p)
{
    if (!p) return;
    bump("player_destroy");
    logf_("player_destroy");
    /* Queued outputs still own their references. */
    while (p->output_count) {
        moq_media_player_output_cleanup(&p->outputs[p->output_head]);
        p->output_head = (p->output_head + 1) % FAKE_QUEUE;
        p->output_count--;
    }
    if (g_player == p) g_player = NULL;
    fake_free(p, sizeof *p);
}

moq_media_receiver_t *moq_media_player_receiver(const moq_media_player_t *p)
{
    return p ? p->r : NULL;
}

void moq5_fake_push_player_output(const moq_media_player_output_t *out)
{
    moq_media_player_t *p = g_player;
    if (!p || p->output_count == FAKE_QUEUE) abort();
    p->outputs[(p->output_head + p->output_count) % FAKE_QUEUE] = *out;
    p->output_count++;
}

void moq5_fake_push_player_event(const moq_media_player_event_t *ev)
{
    moq_media_player_t *p = g_player;
    if (!p || p->event_count == FAKE_QUEUE) abort();
    p->events[(p->event_head + p->event_count) % FAKE_QUEUE] = *ev;
    p->event_count++;
}

moq_result_t moq_media_player_select_track(moq_media_player_t *p, moq_media_track_t *track,
                                           const moq_media_player_select_cfg_t *cfg)
{
    if (!p || !track) return MOQ_ERR_INVAL;
    int i = receiver_track(p->r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    if (cfg)
        logf_("select track=%d start=%u independent=%d shared_epoch=%d struct_size=%u", i,
              cfg->start, cfg->independent_timing ? 1 : 0, cfg->shared_epoch ? 1 : 0,
              cfg->struct_size);
    else
        logf_("select track=%d config=none", i);
    if (cfg && cfg->struct_size < MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    return (moq_result_t)get("select_result");
}

moq_result_t moq_media_player_deselect_track(moq_media_player_t *p, moq_media_track_t *track)
{
    if (!p || !track) return MOQ_ERR_INVAL;
    int i = receiver_track(p->r, track);
    if (i < 0) return MOQ_ERR_INVAL;
    logf_("deselect track=%d", i);
    return (moq_result_t)get("deselect_result");
}

moq_result_t moq_media_player_tick(moq_media_player_t *p, uint64_t now_us)
{
    if (!p) return MOQ_ERR_INVAL;
    logf_("tick %" PRIu64, now_us);
    return (moq_result_t)get("tick_result");
}

moq_result_t moq_media_player_wait(moq_media_player_t *p, uint64_t now_us, uint64_t timeout_us)
{
    if (!p) return MOQ_ERR_INVAL;
    logf_("player_wait now=%" PRIu64 " timeout=%" PRIu64, now_us, timeout_us);
    if (p->r->ep->interrupted) return MOQ_ERR_INTERRUPTED;
    if (p->output_count || p->event_count) return MOQ_OK;
    return (moq_result_t)get("player_wait_result");
}

uint64_t moq_media_player_next_deadline_us(const moq_media_player_t *p)
{
    if (!p) return UINT64_MAX;
    return (uint64_t)get("player_deadline");
}

moq_result_t moq_media_player_poll_output(moq_media_player_t *p, moq_media_player_output_t *out,
                                          size_t out_size)
{
    if (!p || !out) return MOQ_ERR_INVAL;
    bump("poll_output");
    /* A record owns references: it is never truncated. */
    if (out_size < sizeof *out) return MOQ_ERR_ABI_MISMATCH;
    if (p->output_count == 0) return (moq_result_t)get("poll_output_result");
    *out = p->outputs[p->output_head];
    out->struct_size = stamp_for("poll_output_stamp", out_size, sizeof *out);
    p->output_head = (p->output_head + 1) % FAKE_QUEUE;
    p->output_count--;
    return MOQ_OK;
}

moq_result_t moq_media_player_poll_event(moq_media_player_t *p, moq_media_player_event_t *ev,
                                         size_t ev_size)
{
    if (!p || !ev) return MOQ_ERR_INVAL;
    if (ev_size < sizeof *ev) return MOQ_ERR_ABI_MISMATCH;
    if (p->event_count == 0) return (moq_result_t)get("poll_event_result");
    *ev = p->events[p->event_head];
    ev->struct_size = stamp_for("poll_event_stamp", ev_size, sizeof *ev);
    p->event_head = (p->event_head + 1) % FAKE_QUEUE;
    p->event_count--;
    return MOQ_OK;
}

moq_result_t moq_media_player_handle_feedback(moq_media_player_t *p,
                                              const moq_media_player_feedback_t *fb,
                                              uint64_t now_us)
{
    if (!p || !fb) return MOQ_ERR_INVAL;
    int i = receiver_track(p->r, fb->track);
    char detail[120] = "";
    if (fb->kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE)
        snprintf(detail, sizeof detail, "%u/%u", fb->u.decoder_queue.depth,
                 fb->u.decoder_queue.max_recommended);
    else if (fb->kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR)
        snprintf(detail, sizeof detail, "\"%.*s\"", SPAN(fb->u.decode_error.message));
    logf_("feedback track=%d kind=%u generation=%" PRIu64 " detail=%s now=%" PRIu64
          " struct_size=%u",
          i, fb->kind, fb->configuration_generation, detail[0] ? detail : "-", now_us,
          fb->struct_size);
    if (fb->struct_size < MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE || i < 0) return MOQ_ERR_INVAL;
    return (moq_result_t)get("feedback_result");
}

bool moq_media_player_is_closed(const moq_media_player_t *p) { (void)p; return get("player_is_closed") != 0; }
bool moq_media_player_is_fatal(const moq_media_player_t *p) { (void)p; return get("player_is_fatal") != 0; }
uint64_t moq_media_player_fatal_code(const moq_media_player_t *p)
{
    (void)p;
    return (uint64_t)get("player_fatal_code");
}

moq_result_t moq_media_player_get_stats(const moq_media_player_t *p, moq_media_player_stats_t *out,
                                        size_t out_size)
{
    if (!p || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_PLAYER_STATS_V0_SIZE) return MOQ_ERR_ABI_MISMATCH;
    moq_media_player_stats_t st = {
        .tracks_selected = 2, .outputs_queued = (uint64_t)p->output_count,
        .events_queued = (uint64_t)p->event_count, .events_dropped = 1, .held_objects = 1,
        .held_bytes = 4096, .objects_taken = 20, .objects_pushed = 18, .objects_dropped = 1,
        .objects_discarded = 1, .terminals_taken = 6, .units_output = 17,
        .configures_output = 2, .discontinuities_output = 1, .ends_output = 1,
        .timeline_records_discarded = 3, .feedback_stale = 4, .outputs_bytes = 2048,
        .selections_failed = 1,
    };
    size_t n = out_size < sizeof st ? out_size : sizeof st;
    memcpy(out, &st, n);
    out->struct_size = stamp_for("player_stats_stamp", out_size, sizeof st);
    return MOQ_OK;
}
#endif /* MOQ5_FAKE_PLAYER */
