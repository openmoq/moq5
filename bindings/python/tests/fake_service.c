#define _POSIX_C_SOURCE 200809L
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "fake_service.h"

/* A test-only provider implementing public signatures, not a MoQ/TLS model.
 * Compile the unchanged bridge against this for deterministic ownership tests.
 * Nothing here is linked or installed into the production extension.
 *
 * Two fixture facts are deliberately NOT production facts:
 *
 *  * FIXTURE_WATCHDOG is the fixture's OWN failure identity, returned when a
 *    barrier is never released. It is outside every reserved moq_result_t so
 *    a fixture breakdown can never be mistaken for an injected native error;
 *    test_watchdog_fired() reports it independently of the return value.
 *  * FIXTURE_MAX_VERSIONS mirrors the reviewed SDK's OFFER CAPACITY,
 *    MOQ_ENDPOINT_MAX_VERSIONS in service/src/endpoint_internal.h (8), so on
 *    that one point the fake refuses what the SDK refuses (MOQ_ERR_INVAL, no
 *    effect). It is a fixture constant derived from that source, not an
 *    imported header and not a Python API. The fake is deliberately NOT a
 *    version-support, URL or transport model; the SDK remains the authority
 *    for everything else.
 *  * FIXTURE_PTHREAD is returned when a condition wait fails for any reason
 *    other than ETIMEDOUT; the errno is recorded. Only ETIMEDOUT is the
 *    ordinary timeout.
 *
 * The blocking wait honours its timeout: a slice that elapses returns
 * MOQ_DONE like the SDK, and only a barrier that outlives the watchdog is a
 * fixture failure. */
#define FIXTURE_WATCHDOG (-1999)
#define FIXTURE_PTHREAD (-1998)
#define FIXTURE_MAX_VERSIONS 8
#define FIXTURE_WATCHDOG_SECONDS 2

struct moq_endpoint {
    bool stopped;
    bool interrupted;
    moq_endpoint_terminal_reason_t reason;
    uint64_t detail;
    moq_endpoint_state_t state;
    moq_version_t version;
};

/* -- receiver fixture ----------------------------------------------------- *
 * A minimal provider for the receiver SHELL contract: attachment against the
 * fixture endpoint (one slot; the endpoint refuses stop while attached, as
 * the SDK does), the documented wait priority over scripted queue depth and
 * terminal knobs, a sized stats snapshot, and the per-track control commands
 * over fixture-minted track handles. It is not a catalog, object or wire
 * model. */
#define FIXTURE_MAX_TRACKS 32
#define FIXTURE_MAX_PARTS 32
#define FIXTURE_MAX_SUBSCRIPTIONS 32

/* Fixture-owned backing for every span a description can carry, so the
 * borrowed view (event.desc) and the sized copy both point at bytes the test
 * can later scramble to prove the binding copied them. */
#define FIXTURE_TEXT 256
#define FIXTURE_ARRAY 4
#define FIXTURE_EVENTS 16
typedef struct { uint8_t data[FIXTURE_TEXT]; size_t len; } text_t;

struct moq_media_track {
    struct moq_media_receiver *owner;
    int state;
    bool ended;      /* peer reject / end-of-track: subscribe refused, unsubscribe OK, state ENDED */
    bool removed;    /* gone from the catalog: every command WRONG_STATE */
    int index;
    /* description: the borrowed view, its backing, the CURRENT triple the
     * sized copy reports (may differ from the borrowed scalars), and the
     * stamps the copy writes. */
    moq_media_track_desc_t desc;
    text_t name, role, codec, lang, label, packaging_text, event_type, mime_type,
           channel_config, init_data, codec_config, scheme, kid;
    text_t depends_text[FIXTURE_ARRAY], cp_text[FIXTURE_ARRAY];
    moq_bytes_t depends[FIXTURE_ARRAY], cp[FIXTURE_ARRAY];
    bool current_is_live, current_has_duration;
    uint64_t current_duration;
    uint32_t desc_stamp, info_stamp, init_stamp;
};

typedef struct {
    int kind;
    struct moq_media_track *track;
    uint32_t stamp;
    bool has_largest;
    uint64_t largest_group, largest_object;
    bool has_expires;
    uint64_t expires_ms;
    int parse_drop_class;
    uint64_t parse_drops_total, parse_drops_delta;
    bool orphan_desc;    /* no handle, but a non-NULL descriptor pointer (malformed shape) */
} fixture_event_t;

struct moq_media_receiver {
    moq_endpoint_t *ep;
    bool closed;                 /* is_closed knob */
    bool fatal;                  /* is_fatal knob */
    uint64_t fatal_code;
    int queued_events, queued_objects;
    struct moq_media_track tracks[FIXTURE_MAX_TRACKS];
    int track_count;
};

typedef struct {
    int subscribe;               /* 1 subscribe, 0 unsubscribe */
    int track;
    int start;
    bool has_priority;
    int priority;
} subscription_t;

static moq_media_receiver_t *current_receiver;
static int receivers_attached, receivers_destroyed;
static moq_result_t attach_result, subscribe_result, stats_result, terminal_result;
static moq_result_t poll_track_result, desc_copy_result, poll_object_result;
static fixture_event_t events[FIXTURE_EVENTS];
static int event_head, event_count;

/* -- media objects: genuine ownership, separable public-shape corruption --- *
 * Every queued object carries REAL refcounted buffers created through the
 * core allocator and a REAL samples_owned block, so the production cleanup
 * semantics (decref, free, bounded zeroing) apply and are counted. Malformed
 * PUBLIC fields (spans, counts, mdat range, status/payload contradictions)
 * are injected separately and never touch the ownership refs, so every
 * injected object stays cleanable. The fixture keeps one extra reference to
 * each buffer so it can scramble the bytes AFTER the binding's cleanup. */
#define FIXTURE_OBJECTS 16
#define FIXTURE_SAMPLES 8
typedef struct {
    moq_media_object_t obj;
    uint32_t stamp;
    moq_rcbuf_t *keep_payload, *keep_properties;   /* fixture's own extra refs */
    moq_cmaf_sample_t borrowed_samples[FIXTURE_SAMPLES]; /* for a NULL samples_owned case */
} fixture_object_t;
static fixture_object_t objects[FIXTURE_OBJECTS];
static moq_rcbuf_t *kept[FIXTURE_OBJECTS * 2];
static int kept_count;
static int object_head, object_count;
static int objects_pushed, objects_dequeued, cleanup_calls, cleanup_effective;
static int refs_created, refs_released, samples_allocated, samples_freed, released_on_destroy;
/* Independent evidence, not counters beside the operation they claim to
 * prove: sample blocks come from a bounded fixture allocator that keeps a
 * LIVE table (a free that does not match a live block is a bad_free), and
 * refcounts are read back from the real core buffers. Cleanup records the
 * thread it ran on against the thread that dequeued. */
#define FIXTURE_SAMPLE_BLOCKS 64
static moq_cmaf_sample_t *sample_blocks[FIXTURE_SAMPLE_BLOCKS];
static size_t sample_block_sizes[FIXTURE_SAMPLE_BLOCKS];
static int sample_blocks_live, sample_bad_free;
static pthread_t last_poll_thread;
static bool have_poll_thread;
static int cleanups_on_polling_thread, cleanups_off_polling_thread;

static moq_cmaf_sample_t *sample_block_alloc(size_t count)
{
    for (int i = 0; i < FIXTURE_SAMPLE_BLOCKS; ++i) {
        if (sample_blocks[i]) continue;
        size_t bytes = count * sizeof(moq_cmaf_sample_t);
        moq_cmaf_sample_t *block = malloc(bytes ? bytes : 1);
        if (!block) return NULL;
        sample_blocks[i] = block;
        sample_block_sizes[i] = bytes;
        sample_blocks_live++;
        return block;
    }
    return NULL;
}

static void sample_block_free(moq_cmaf_sample_t *block, size_t bytes)
{
    for (int i = 0; i < FIXTURE_SAMPLE_BLOCKS; ++i) {
        if (sample_blocks[i] == block && sample_block_sizes[i] == bytes) {
            free(block);
            sample_blocks[i] = NULL;
            sample_blocks_live--;
            return;
        }
    }
    sample_bad_free++;   /* unknown pointer or wrong size: never freed blindly */
}
static moq_media_receiver_stats_t scripted_stats;
static uint32_t stats_size = sizeof(moq_media_receiver_stats_t);
static subscription_t subscriptions[FIXTURE_MAX_SUBSCRIPTIONS];
static int subscription_count;
/* The last attach configuration, copied byte-exact (parts may contain NUL). */
static uint8_t rcfg_parts[FIXTURE_MAX_PARTS][256];
static size_t rcfg_part_len[FIXTURE_MAX_PARTS];
static size_t rcfg_part_count;
static uint8_t rcfg_catalog[256];
static size_t rcfg_catalog_len;
static bool rcfg_auto_subscribe, rcfg_full_size, rcfg_endpoint_null;
static int rcfg_time_mode, rcfg_policy;
static uint32_t rcfg_max_objects, rcfg_max_track_events;
static uint64_t rcfg_max_bytes;

/* -- sender fixture ------------------------------------------------------- *
 * The SHELL contract only: attachment against the fixture endpoint (one slot,
 * with the endpoint refusing stop while it is held, as the SDK does), the
 * exact configuration the caller passed, per-operation ENTRY witnesses and an
 * ordered lifecycle log, and scripted ready/fatal knobs. It is deliberately
 * NOT a queue, backpressure, catalog or wire model -- the real C tests own all
 * of that.
 *
 * Capture is separated from the declared input. Spans are validated for
 * pointer/length coherence BEFORE anything is read, only what fits the
 * fixture's storage is captured, and any input the fixture cannot retain
 * completely sets a sticky, named ORACLE LIMIT instead of being silently
 * truncated. Declared counts and lengths are always reported, so a test can
 * tell an argument refusal from a fixture representability failure. The
 * storage sizes below are fixture capacity, never a public limit. */
#define FIXTURE_SCFG_PART_CAP 256
#define FIXTURE_SCFG_CATALOG_CAP 256
#define FIXTURE_SENDER_LOG 32

struct moq_media_sender {
    moq_endpoint_t *ep;
    bool ready;
    bool fatal;
    uint64_t fatal_code;
};

#define FIXTURE_SEND_TRACKS 8

/* Three distinct states, never conflated:
 *   allocated       a slot was handed out by add_track
 *   removed         remove_track succeeded on it
 *   owner_destroyed its sender was destroyed, so the native handle is gone
 * There is no flag that can never change: destruction updates every slot. */
static moq_media_track_t send_track_pool[FIXTURE_SEND_TRACKS];
static text_t send_track_name_store[FIXTURE_SEND_TRACKS];
static struct moq_media_sender *send_track_owner[FIXTURE_SEND_TRACKS];
static bool send_track_removed_flag[FIXTURE_SEND_TRACKS];
static bool send_track_owner_destroyed[FIXTURE_SEND_TRACKS];
static size_t send_track_count;

/* The substrate rows drive the recorder directly; when no sender is attached
 * they still need a distinct non-NULL owner. This stub is never dereferenced
 * as a sender: only its address is compared. */
static moq_media_sender_t send_track_stub_owner;


static moq_media_sender_t *current_sender;

static moq_media_sender_t *current_sender_or_stub(void)
{
    return current_sender ? current_sender : &send_track_stub_owner;
}

static int senders_attached, senders_destroyed;
static int sender_attach_entries, sender_destroy_entries;
static moq_result_t sender_attach_result;
static const char *sender_log[FIXTURE_SENDER_LOG];
static int sender_log_count;
static bool sender_log_overflow;

/* captured configuration */
static uint8_t scfg_parts[FIXTURE_MAX_PARTS][FIXTURE_SCFG_PART_CAP];
static size_t scfg_part_captured[FIXTURE_MAX_PARTS];   /* bytes actually held */
static size_t scfg_part_declared[FIXTURE_MAX_PARTS];   /* length the caller declared */
static size_t scfg_declared_count, scfg_captured_count;
static uint8_t scfg_catalog[FIXTURE_SCFG_CATALOG_CAP];
static size_t scfg_catalog_captured, scfg_catalog_declared;
static int scfg_backpressure;
static uint64_t scfg_block_timeout_us, scfg_catalog_refresh_interval_us;
static uint32_t scfg_queue_max_objects, scfg_queue_max_bytes;
static uint32_t scfg_pre_ready_max_objects, scfg_pre_ready_max_bytes;
static bool scfg_validate_cmaf, scfg_publish_tracks, scfg_drop_without_demand;
static bool scfg_endpoint_null, scfg_full_size, scfg_callbacks_absent;
static uint32_t scfg_callbacks_struct_size;
static bool scfg_content_protections_absent;
static bool scfg_seen;
static bool scfg_oracle_limit;
static const char *scfg_oracle_limit_reason;
static bool scfg_shape_refused;            /* a span was incoherent: nothing read */
static const char *scfg_shape_refused_reason;
static bool scfg_unsupported_prefix;       /* advertised size below the full config */
static const char *scfg_unsupported_reason;

/* Ordered lifecycle witness, appended by both the endpoint and the sender. */
static void sender_log_add(const char *what);

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
static moq_endpoint_t *current;
static int created, stopped, destroyed;
static moq_result_t connect_result, stop_result, wait_result = MOQ_DONE;
static bool block_wait, entered, block_stop, stop_entered;
static bool force_watchdog, watchdog_fired;
/* Endpoint ENTRY witnesses: how often the endpoint's own wait and wake were
 * actually entered. A log of names cannot say that an operation did NOT
 * happen if that operation never writes to the log; these counters can. */
static int ep_wait_entries, ep_wake_entries;
static int forced_pthread_error, pthread_error;
static uint32_t terminal_size = sizeof(moq_endpoint_terminal_t);
static char url[4096], sni[4096], ca_file[4096], wt_path[4096];
static moq_version_t versions[FIXTURE_MAX_VERSIONS];
static size_t version_count;
static uint64_t handshake_timeout;
static bool full_size, insecure;
static int protocol, backend, profile, version_policy;

static struct timespec watchdog(void)
{
    struct timespec end;
    int rc = clock_gettime(CLOCK_REALTIME, &end);
    assert(rc == 0);
    /* A forced watchdog expires at once: the failure path is exercised
     * deterministically, never by waiting for the scheduler. */
    if (!force_watchdog) end.tv_sec += FIXTURE_WATCHDOG_SECONDS;
    return end;
}

static struct timespec earliest(struct timespec a, struct timespec b)
{
    if (a.tv_sec < b.tv_sec || (a.tv_sec == b.tv_sec && a.tv_nsec < b.tv_nsec)) return a;
    return b;
}

/* now + timeout_us, saturating at the watchdog so INT64_MAX cannot overflow. */
static struct timespec slice_deadline(uint64_t timeout_us, struct timespec cap)
{
    struct timespec end;
    int rc = clock_gettime(CLOCK_REALTIME, &end);
    assert(rc == 0);
    if (timeout_us / 1000000u > (uint64_t)(cap.tv_sec - end.tv_sec) + 1u) return cap;
    end.tv_sec += (time_t)(timeout_us / 1000000u);
    end.tv_nsec += (long)((timeout_us % 1000000u) * 1000u);
    if (end.tv_nsec >= 1000000000L) { end.tv_sec++; end.tv_nsec -= 1000000000L; }
    return earliest(end, cap);
}

static bool reached(struct timespec deadline)
{
    struct timespec now;
    int rc = clock_gettime(CLOCK_REALTIME, &now);
    assert(rc == 0);
    return now.tv_sec > deadline.tv_sec || (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec);
}

void moq_endpoint_cfg_init_sized(moq_endpoint_cfg_t *cfg, size_t size)
{
    size_t n = size < sizeof(*cfg) ? size : sizeof(*cfg);
    memset(cfg, 0, n);
    if (n >= sizeof(cfg->struct_size)) cfg->struct_size = (uint32_t)n;
}

static void copy_text(char *out, moq_bytes_t input)
{
    memcpy(out, input.data, input.len);
    out[input.len] = '\0';
}

moq_result_t moq_endpoint_connect(const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out)
{
    *out = NULL;
    pthread_mutex_lock(&mutex);
    if (connect_result != MOQ_OK) {
        moq_result_t rc = connect_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    /* Over-capacity offers are refused as the SDK refuses them on that point:
     * MOQ_ERR_INVAL, nothing created, nothing recorded. */
    if (current || cfg->url.len >= sizeof(url) || cfg->sni.len >= sizeof(sni) ||
        cfg->ca_file.len >= sizeof(ca_file) || cfg->wt_path.len >= sizeof(wt_path) ||
        cfg->versions.version_count > FIXTURE_MAX_VERSIONS) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    current = calloc(1, sizeof(*current));
    if (!current) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_NOMEM;
    }
    full_size = cfg->struct_size == sizeof(*cfg);
    protocol = cfg->protocol;
    backend = cfg->backend;
    profile = (int)cfg->wt_profile;
    version_policy = cfg->versions.policy;
    insecure = cfg->insecure_skip_verify;
    handshake_timeout = cfg->handshake_timeout_us;
    copy_text(url, cfg->url);
    copy_text(sni, cfg->sni);
    copy_text(ca_file, cfg->ca_file);
    copy_text(wt_path, cfg->wt_path);
    version_count = cfg->versions.version_count;
    if (version_count) memcpy(versions, cfg->versions.versions, version_count * sizeof(*versions));
    created++;
    *out = current;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

moq_result_t moq_endpoint_stop(moq_endpoint_t *ep)
{
    pthread_mutex_lock(&mutex);
    assert(ep == current);
    /* The SDK refuses stop while a media service is attached (WRONG_STATE);
     * the fixture mirrors that one point so retention is observable. */
    if ((current_receiver && current_receiver->ep == ep) ||
        (current_sender && current_sender->ep == ep)) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_WRONG_STATE;
    }
    stopped++;
    sender_log_add("endpoint_stop");
    stop_entered = true;
    pthread_cond_broadcast(&condition);
    struct timespec end = watchdog();
    int result = 0;
    while (block_stop && result == 0)
        result = pthread_cond_timedwait(&condition, &mutex, &end);
    if (forced_pthread_error && block_stop) { result = forced_pthread_error; }
    moq_result_t rc;
    if (result == ETIMEDOUT) {
        watchdog_fired = true;
        rc = FIXTURE_WATCHDOG;
    } else if (result != 0) {
        pthread_error = result;
        rc = FIXTURE_PTHREAD;
    } else {
        rc = stop_result;
    }
    if (rc != MOQ_OK) {
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    ep->stopped = true;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

void moq_endpoint_destroy(moq_endpoint_t *ep)
{
    pthread_mutex_lock(&mutex);
    assert(ep == current && ep->stopped);
    sender_log_add("endpoint_destroy");
    destroyed++;
    current = NULL;
    free(ep);
    pthread_mutex_unlock(&mutex);
}

moq_endpoint_state_t moq_endpoint_state(const moq_endpoint_t *ep)
{
    return ep->stopped ? MOQ_ENDPOINT_CLOSED : ep->state;
}

moq_version_t moq_endpoint_negotiated_version(const moq_endpoint_t *ep)
{
    return ep->version;
}

moq_result_t moq_endpoint_get_terminal(const moq_endpoint_t *ep,
                                     moq_endpoint_terminal_t *out, size_t size)
{
    assert(size == sizeof(*out));
    if (terminal_result != MOQ_OK) return terminal_result;
    out->struct_size = terminal_size;
    out->reason = ep->reason;
    out->detail_code = ep->detail;
    return MOQ_OK;
}

moq_result_t moq_endpoint_wait(moq_endpoint_t *ep, uint64_t timeout_us)
{
    pthread_mutex_lock(&mutex);
    ep_wait_entries++;
    entered = true;
    pthread_cond_broadcast(&condition);
    /* Blocked waits honour the caller's slice: the earlier of the slice
     * deadline and the watchdog ends the wait. Expiry of the SLICE is the
     * SDK's ordinary MOQ_DONE; expiry of the WATCHDOG is fixture failure. */
    struct timespec limit = watchdog();
    struct timespec slice = slice_deadline(timeout_us, limit);
    int result = 0;
    if (forced_pthread_error && block_wait && timeout_us) {
        result = forced_pthread_error;   /* a scripted pthread failure, for the named-failure row */
    } else {
        while (block_wait && !ep->interrupted && timeout_us && result == 0)
            result = pthread_cond_timedwait(&condition, &mutex, &slice);
    }
    moq_result_t rc;
    if (result == 0) {
        rc = ep->interrupted ? MOQ_ERR_INTERRUPTED : wait_result;
    } else if (result != ETIMEDOUT) {
        pthread_error = result;          /* only ETIMEDOUT is an ordinary timeout */
        rc = FIXTURE_PTHREAD;
    } else if (ep->interrupted) {
        rc = MOQ_ERR_INTERRUPTED;
    } else if (reached(limit)) {
        watchdog_fired = true;
        rc = FIXTURE_WATCHDOG;
    } else {
        rc = MOQ_DONE;   /* the bounded slice elapsed, as the SDK would report */
    }
    pthread_mutex_unlock(&mutex);
    return rc;
}

void moq_endpoint_set_interrupted(moq_endpoint_t *ep, bool value)
{
    pthread_mutex_lock(&mutex);
    ep->interrupted = value;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);
}

void moq_endpoint_wake(moq_endpoint_t *ep)
{
    (void)ep;
    pthread_mutex_lock(&mutex);
    ep_wake_entries++;
    block_wait = false;
    wait_result = MOQ_OK;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);
}

static void hooks_clear(void);

PyObject *test_reset(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    if (current) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked a native endpoint");
        return NULL;
    }
    if (current_receiver) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked a native receiver");
        return NULL;
    }
    if (current_sender) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked a native sender");
        return NULL;
    }
    /* Transferred-resource evidence is never erased: a previous test that
     * left a queued object, an unreleased transferred buffer (fixture
     * refcount > 1) or a live sample block fails here by name. */
    if (object_count) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked queued objects");
        return NULL;
    }
    for (int i = 0; i < kept_count; ++i) {
        if (moq_rcbuf_refcount(kept[i]) != 1) {
            pthread_mutex_unlock(&mutex);
            PyErr_SetString(PyExc_RuntimeError, "previous test left a transferred buffer unreleased");
            return NULL;
        }
    }
    if (sample_blocks_live || sample_bad_free) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test left live or badly freed sample blocks");
        return NULL;
    }
    ep_wait_entries = ep_wake_entries = 0;
    receivers_attached = receivers_destroyed = 0;
    attach_result = MOQ_OK;
    subscribe_result = MOQ_OK;
    stats_result = MOQ_OK;
    terminal_result = MOQ_OK;
    poll_track_result = MOQ_OK;
    desc_copy_result = MOQ_OK;
    poll_object_result = MOQ_OK;
    event_head = event_count = 0;
    for (int i = 0; i < kept_count; ++i) moq_rcbuf_decref(kept[i]);
    kept_count = 0;
    object_head = object_count = 0;
    objects_pushed = objects_dequeued = cleanup_calls = cleanup_effective = 0;
    refs_created = refs_released = samples_allocated = samples_freed = released_on_destroy = 0;
    have_poll_thread = false;
    cleanups_on_polling_thread = cleanups_off_polling_thread = 0;
    hooks_clear();
    memset(&scripted_stats, 0, sizeof(scripted_stats));
    stats_size = sizeof(moq_media_receiver_stats_t);
    subscription_count = 0;
    rcfg_part_count = 0;
    rcfg_catalog_len = 0;
    created = stopped = destroyed = 0;
    connect_result = MOQ_OK;
    stop_result = MOQ_OK;
    wait_result = MOQ_DONE;
    block_wait = entered = block_stop = stop_entered = false;
    force_watchdog = watchdog_fired = false;
    forced_pthread_error = pthread_error = 0;
    terminal_size = sizeof(moq_endpoint_terminal_t);
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* How often the endpoint's own wait and wake were entered. Read as a DELTA
 * around an operation, so a row can say that the operation entered neither. */
PyObject *test_endpoint_entries(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i}",
        "wait", ep_wait_entries, "wake", ep_wake_entries);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_counts(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    int c = created, s = stopped, d = destroyed;
    pthread_mutex_unlock(&mutex);
    return Py_BuildValue("iii", c, s, d);
}

PyObject *test_config(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    PyObject *offers = PyTuple_New((Py_ssize_t)version_count);
    if (!offers) return NULL;
    for (size_t i = 0; i < version_count; ++i) {
        PyObject *v = PyLong_FromUnsignedLong((uint32_t)versions[i]);
        if (!v || PyTuple_SetItem(offers, (Py_ssize_t)i, v) < 0) {
            Py_DECREF(offers);
            return NULL;
        }
    }
    PyObject *result = Py_BuildValue("{s:y,s:y,s:y,s:y,s:O,s:K,s:O,s:O,s:i,s:i,s:i,s:i}",
        "url", url, "sni", sni, "ca_file", ca_file, "wt_path", wt_path,
        "versions", offers, "handshake_timeout_us", (unsigned long long)handshake_timeout,
        "insecure_skip_verify", insecure ? Py_True : Py_False,
        "full_size", full_size ? Py_True : Py_False,
        "protocol", protocol, "backend", backend, "wt_profile", profile,
        "version_policy", version_policy);
    Py_DECREF(offers);
    return result;
}

PyObject *test_connect_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    connect_result = (moq_result_t)rc;
    Py_RETURN_NONE;
}

PyObject *test_wait_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    wait_result = (moq_result_t)rc;
    Py_RETURN_NONE;
}

PyObject *test_stop_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    stop_result = (moq_result_t)rc;
    Py_RETURN_NONE;
}

PyObject *test_snapshot(PyObject *module, PyObject *args)
{
    (void)module;
    int state;
    unsigned int version;
    if (!PyArg_ParseTuple(args, "iI", &state, &version)) return NULL;
    assert(current);
    current->state = (moq_endpoint_state_t)state;
    current->version = (moq_version_t)version;
    Py_RETURN_NONE;
}

PyObject *test_terminal_size(PyObject *module, PyObject *value)
{
    (void)module;
    unsigned long size = PyLong_AsUnsignedLong(value);
    if (PyErr_Occurred()) return NULL;
    terminal_size = (uint32_t)size;
    Py_RETURN_NONE;
}

PyObject *test_terminal(PyObject *module, PyObject *args)
{
    (void)module;
    int reason;
    unsigned long long detail;
    if (!PyArg_ParseTuple(args, "iK", &reason, &detail)) return NULL;
    assert(current);
    current->reason = (moq_endpoint_terminal_reason_t)reason;
    current->detail = (uint64_t)detail;
    Py_RETURN_NONE;
}

PyObject *test_block_wait(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    block_wait = true;
    entered = false;
    Py_RETURN_NONE;
}

static PyObject *wait_for_entry(const bool *flag, const char *message)
{
    int result = 0;
    Py_BEGIN_ALLOW_THREADS
    pthread_mutex_lock(&mutex);
    struct timespec end = watchdog();
    while (!*flag && result == 0)
        result = pthread_cond_timedwait(&condition, &mutex, &end);
    pthread_mutex_unlock(&mutex);
    Py_END_ALLOW_THREADS
    if (result) {
        pthread_mutex_lock(&mutex);
        watchdog_fired = true;
        pthread_mutex_unlock(&mutex);
        PyErr_Format(PyExc_AssertionError, "fixture watchdog: %s", message);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *test_wait_entered(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    return wait_for_entry(&entered, "native wait-entry barrier was not reached");
}

PyObject *test_block_stop(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    block_stop = true;
    stop_entered = false;
    Py_RETURN_NONE;
}

PyObject *test_stop_entered(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    return wait_for_entry(&stop_entered, "native stop-entry barrier was not reached");
}

PyObject *test_force_watchdog(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    force_watchdog = true;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_watchdog_fired(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    bool fired = watchdog_fired;
    pthread_mutex_unlock(&mutex);
    return PyBool_FromLong(fired);
}

PyObject *test_force_pthread_error(PyObject *module, PyObject *value)
{
    (void)module;
    long code = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    forced_pthread_error = (int)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_pthread_error(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    int code = pthread_error;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong(code);
}

PyObject *test_unblock(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    block_wait = false;
    block_stop = false;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* -- receiver provider ------------------------------------------------------ */

void moq_media_receiver_cfg_init(moq_media_receiver_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

void moq_media_receiver_track_subscribe_cfg_init(moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
    cfg->start = MOQ_MEDIA_START_CURRENT;
}

moq_result_t moq_media_receiver_attach(moq_endpoint_t *ep, const moq_media_receiver_cfg_t *cfg,
                                       moq_media_receiver_t **out)
{
    *out = NULL;
    pthread_mutex_lock(&mutex);
    if (attach_result != MOQ_OK) {
        moq_result_t rc = attach_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    assert(ep == current);
    if (current_receiver || ep->stopped || cfg->namespace_.count > FIXTURE_MAX_PARTS ||
        cfg->catalog_track.len >= sizeof(rcfg_catalog)) {
        pthread_mutex_unlock(&mutex);
        return current_receiver || ep->stopped ? MOQ_ERR_WRONG_STATE : MOQ_ERR_INVAL;
    }
    for (size_t i = 0; i < cfg->namespace_.count; ++i) {
        if (cfg->namespace_.parts[i].len >= sizeof(rcfg_parts[0])) {
            pthread_mutex_unlock(&mutex);
            return MOQ_ERR_INVAL;
        }
    }
    current_receiver = calloc(1, sizeof(*current_receiver));
    if (!current_receiver) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_NOMEM;
    }
    current_receiver->ep = ep;
    rcfg_full_size = cfg->struct_size == sizeof(*cfg);
    rcfg_endpoint_null = cfg->endpoint == NULL;
    rcfg_part_count = cfg->namespace_.count;
    for (size_t i = 0; i < cfg->namespace_.count; ++i) {
        rcfg_part_len[i] = cfg->namespace_.parts[i].len;
        memcpy(rcfg_parts[i], cfg->namespace_.parts[i].data, rcfg_part_len[i]);
    }
    rcfg_catalog_len = cfg->catalog_track.len;
    if (rcfg_catalog_len) memcpy(rcfg_catalog, cfg->catalog_track.data, rcfg_catalog_len);
    rcfg_auto_subscribe = cfg->auto_subscribe;
    rcfg_time_mode = (int)cfg->time_mode;
    rcfg_policy = (int)cfg->overflow.policy;
    rcfg_max_objects = cfg->overflow.max_objects;
    rcfg_max_bytes = cfg->overflow.max_bytes;
    rcfg_max_track_events = cfg->max_track_events;
    receivers_attached++;
    *out = current_receiver;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_create(const moq_media_receiver_cfg_t *cfg, moq_media_receiver_t **out)
{
    (void)cfg;
    *out = NULL;
    return MOQ_ERR_UNSUPPORTED;   /* the shell only attaches */
}

/* -- declared-boundary hooks ------------------------------------------------ *
 * A loop integration test may inject work or state at ONE declared provider
 * boundary: the Nth time (since reset) the track queue reads empty inside
 * poll_track, the Nth time the object queue reads empty inside poll_object
 * (after the latch check, as the SDK orders it), or the Nth receiver wait
 * call (before its priority check). Each hook is a Python callable run once
 * with the fixture mutex released and the GIL held; it may use the ordinary
 * seams (push an object or event, set state, stats, the latch). A hook that
 * raises is not swallowed: the exception is kept and re-raised by
 * _test_boundary_report(). Waits are budgeted: past the budget the fake
 * returns MOQ_ERR_INTERNAL so a loop that never settles fails by name. */
#define FIXTURE_HOOKS 8
enum { HOOK_TRACK_EMPTY = 1, HOOK_OBJECT_EMPTY = 2, HOOK_WAIT = 3 };
typedef struct { int boundary; int nth; PyObject *callable; bool fired; } fixture_hook_t;
static fixture_hook_t hooks[FIXTURE_HOOKS];
static int hook_count, hooks_fired, hook_failures;
static PyObject *hook_error;
static int track_polls, object_polls, track_empties, object_empties, wait_calls, wait_budget;

static bool hook_due_locked(int boundary, int occurrence)
{
    for (int i = 0; i < hook_count; ++i)
        if (!hooks[i].fired && hooks[i].boundary == boundary && hooks[i].nth == occurrence) return true;
    return false;
}

/* mutex NOT held; acquires the GIL (a poll already holds it, a wait released it) */
static void hooks_run(int boundary, int occurrence)
{
    PyGILState_STATE gil = PyGILState_Ensure();
    for (int i = 0; i < hook_count; ++i) {
        fixture_hook_t *h = &hooks[i];
        if (h->fired || h->boundary != boundary || h->nth != occurrence) continue;
        h->fired = true;
        hooks_fired++;
        PyObject *r = PyObject_CallNoArgs(h->callable);
        if (r) {
            Py_DECREF(r);
        } else {
            hook_failures++;
            PyObject *err = PyErr_GetRaisedException();
            if (hook_error) Py_DECREF(err); else hook_error = err;   /* the first failure is kept */
        }
    }
    PyGILState_Release(gil);
}

static void hooks_clear(void)
{
    for (int i = 0; i < hook_count; ++i) Py_CLEAR(hooks[i].callable);
    hook_count = hooks_fired = hook_failures = 0;
    Py_CLEAR(hook_error);
    track_polls = object_polls = track_empties = object_empties = wait_calls = 0;
    wait_budget = 0;
}

/* The fixture's own release: real core decrefs and the fixture allocator's
 * free. This MIRRORS moq_media_object_cleanup's shape; it is not the SDK's
 * function (the fixture replaces the service library). */
static void release_refs_locked(moq_media_object_t *o)
{
    if (o->payload_ref) { moq_rcbuf_decref(o->payload_ref); o->payload_ref = NULL; refs_released++; }
    if (o->properties_ref) { moq_rcbuf_decref(o->properties_ref); o->properties_ref = NULL; refs_released++; }
    if (o->samples_owned) {
        sample_block_free(o->samples_owned, o->sample_count * sizeof(moq_cmaf_sample_t));
        o->samples_owned = NULL;
        samples_freed++;
    }
}

void moq_media_receiver_destroy(moq_media_receiver_t *r)
{
    pthread_mutex_lock(&mutex);
    assert(r == current_receiver);
    /* queued, never-polled objects are released exactly once here */
    while (object_count > 0) {
        release_refs_locked(&objects[object_head].obj);
        object_head = (object_head + 1) % FIXTURE_OBJECTS;
        object_count--;
        released_on_destroy++;
    }
    receivers_destroyed++;
    current_receiver = NULL;
    free(r);
    pthread_mutex_unlock(&mutex);
}

static bool receiver_terminal(const moq_media_receiver_t *r)
{
    return r->closed || r->fatal || r->ep->stopped;
}

moq_result_t moq_media_receiver_wait(moq_media_receiver_t *r, uint64_t timeout_us)
{
    /* The SDK's documented priority: queued work, then terminal, then the
     * endpoint wait whose INTERRUPTED/DONE are returned as is, then the same
     * recheck. Queue depth and terminal are fixture knobs. */
    pthread_mutex_lock(&mutex);
    int n = ++wait_calls;
    bool over_budget = wait_budget > 0 && n > wait_budget;
    bool due = hook_due_locked(HOOK_WAIT, n);
    pthread_mutex_unlock(&mutex);
    if (over_budget) return MOQ_ERR_INTERNAL;   /* fixture wait budget exhausted */
    if (due) hooks_run(HOOK_WAIT, n);
    pthread_mutex_lock(&mutex);
    bool have = r->queued_events > 0 || r->queued_objects > 0 || event_count > 0 || object_count > 0;
    bool terminal = receiver_terminal(r);
    pthread_mutex_unlock(&mutex);
    if (have) return MOQ_OK;
    if (terminal) return MOQ_ERR_CLOSED;
    moq_result_t rc = moq_endpoint_wait(r->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;
    pthread_mutex_lock(&mutex);
    have = r->queued_events > 0 || r->queued_objects > 0 || event_count > 0 || object_count > 0;
    terminal = receiver_terminal(r);
    pthread_mutex_unlock(&mutex);
    if (have) return MOQ_OK;
    if (terminal) return MOQ_ERR_CLOSED;
    return rc < 0 ? rc : MOQ_OK;
}

moq_result_t moq_media_receiver_get_stats(const moq_media_receiver_t *r,
                                          moq_media_receiver_stats_t *out, size_t out_size)
{
    assert(r == current_receiver && out_size == sizeof(*out));
    pthread_mutex_lock(&mutex);
    if (stats_result != MOQ_OK) {
        moq_result_t rc = stats_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    /* Write only the scripted stamped prefix, as a sized native call would. */
    uint32_t n = stats_size < sizeof(*out) ? stats_size : (uint32_t)sizeof(*out);
    memcpy(out, &scripted_stats, n);
    out->struct_size = stats_size;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

bool moq_media_receiver_is_closed(const moq_media_receiver_t *r)
{
    return r->closed || r->ep->stopped;
}

bool moq_media_receiver_is_fatal(const moq_media_receiver_t *r)
{
    return r->fatal;
}

uint64_t moq_media_receiver_fatal_code(const moq_media_receiver_t *r)
{
    return r->fatal_code;
}

static bool owns(const moq_media_receiver_t *r, const moq_media_track_t *track)
{
    return track && track->owner == r;
}

static moq_result_t record_subscription(int subscribe, const moq_media_track_t *track,
                                        const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (subscription_count >= FIXTURE_MAX_SUBSCRIPTIONS) return MOQ_ERR_NOMEM;
    subscription_t *s = &subscriptions[subscription_count++];
    s->subscribe = subscribe;
    s->track = track->index;
    s->start = cfg ? (int)cfg->start : -1;
    s->has_priority = cfg ? cfg->has_priority : false;
    s->priority = cfg ? cfg->priority : -1;
    return MOQ_OK;
}

moq_result_t moq_media_receiver_subscribe_track(moq_media_receiver_t *r, moq_media_track_t *track,
                                                const moq_media_receiver_track_subscribe_cfg_t *cfg)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&mutex);
    moq_result_t rc = MOQ_OK;
    if (subscribe_result != MOQ_OK) rc = subscribe_result;
    else if (cfg && (cfg->struct_size < sizeof(*cfg) ||
                     (cfg->start != MOQ_MEDIA_START_CURRENT && cfg->start != MOQ_MEDIA_START_NEXT_GROUP))) rc = MOQ_ERR_INVAL;
    else if (receiver_terminal(r)) rc = MOQ_ERR_CLOSED;
    else if (!owns(r, track)) rc = MOQ_ERR_INVAL;
    else if (track->ended || track->removed) rc = MOQ_ERR_WRONG_STATE;
    else rc = record_subscription(1, track, cfg);
    pthread_mutex_unlock(&mutex);
    return rc;
}

moq_result_t moq_media_receiver_unsubscribe_track(moq_media_receiver_t *r, moq_media_track_t *track)
{
    if (!r || !track) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&mutex);
    moq_result_t rc = MOQ_OK;
    if (subscribe_result != MOQ_OK) rc = subscribe_result;
    else if (receiver_terminal(r)) rc = MOQ_ERR_CLOSED;
    else if (!owns(r, track)) rc = MOQ_ERR_INVAL;
    else if (track->removed) rc = MOQ_ERR_WRONG_STATE;   /* an ended track may still be disabled */
    else rc = record_subscription(0, track, NULL);
    pthread_mutex_unlock(&mutex);
    return rc;
}

moq_result_t moq_media_receiver_track_state(const moq_media_receiver_t *r, const moq_media_track_t *track,
                                            moq_media_track_state_t *out)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&mutex);
    if (!owns(r, track)) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    if (track->removed) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_WRONG_STATE;
    }
    *out = (receiver_terminal(r) || track->ended) ? MOQ_MEDIA_TRACK_STATE_ENDED
                                                   : (moq_media_track_state_t)track->state;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

/* -- receiver seams --------------------------------------------------------- */

PyObject *test_receiver_counts(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    int a = receivers_attached, d = receivers_destroyed;
    pthread_mutex_unlock(&mutex);
    return Py_BuildValue("ii", a, d);
}

PyObject *test_receiver_config(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    PyObject *parts = PyTuple_New((Py_ssize_t)rcfg_part_count);
    if (!parts) return NULL;
    for (size_t i = 0; i < rcfg_part_count; ++i) {
        PyObject *part = PyBytes_FromStringAndSize((const char *)rcfg_parts[i], (Py_ssize_t)rcfg_part_len[i]);
        if (!part || PyTuple_SetItem(parts, (Py_ssize_t)i, part) < 0) {
            Py_DECREF(parts);
            return NULL;
        }
    }
    PyObject *result = Py_BuildValue("{s:O,s:y#,s:O,s:i,s:i,s:I,s:K,s:I,s:O,s:O}",
        "namespace", parts, "catalog_track", (const char *)rcfg_catalog, (Py_ssize_t)rcfg_catalog_len,
        "auto_subscribe", rcfg_auto_subscribe ? Py_True : Py_False,
        "time_mode", rcfg_time_mode, "overflow", rcfg_policy,
        "max_objects", (unsigned int)rcfg_max_objects, "max_bytes", (unsigned long long)rcfg_max_bytes,
        "max_track_events", (unsigned int)rcfg_max_track_events,
        "full_size", rcfg_full_size ? Py_True : Py_False,
        "endpoint_null", rcfg_endpoint_null ? Py_True : Py_False);
    Py_DECREF(parts);
    return result;
}

PyObject *test_attach_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    attach_result = (moq_result_t)rc;
    Py_RETURN_NONE;
}

PyObject *test_receiver_state(PyObject *module, PyObject *args)
{
    (void)module;
    int closed, fatal;
    unsigned long long code;
    if (!PyArg_ParseTuple(args, "ppK", &closed, &fatal, &code)) return NULL;
    pthread_mutex_lock(&mutex);
    assert(current_receiver);
    current_receiver->closed = closed;
    current_receiver->fatal = fatal;
    current_receiver->fatal_code = (uint64_t)code;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_receiver_queue(PyObject *module, PyObject *args)
{
    (void)module;
    int events, objects;
    if (!PyArg_ParseTuple(args, "ii", &events, &objects)) return NULL;
    pthread_mutex_lock(&mutex);
    assert(current_receiver);
    current_receiver->queued_events = events;
    current_receiver->queued_objects = objects;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_stats(PyObject *module, PyObject *args)
{
    (void)module;
    unsigned long long v[9], catalog_drops;
    int paused, complete;
    if (!PyArg_ParseTuple(args, "KKKKKKKKKpKp", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8],
                          &paused, &catalog_drops, &complete)) return NULL;
    pthread_mutex_lock(&mutex);
    scripted_stats.objects_received = v[0];
    scripted_stats.objects_queued = v[1];
    scripted_stats.bytes_queued = v[2];
    scripted_stats.objects_dropped = v[3];
    scripted_stats.groups_dropped = v[4];
    scripted_stats.keyframes_dropped = v[5];
    scripted_stats.parse_drops = v[6];
    scripted_stats.overflow_events = v[7];
    scripted_stats.pause_transitions = v[8];
    scripted_stats.paused = paused;
    scripted_stats.catalog_drops = catalog_drops;
    scripted_stats.catalog_complete = complete;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_stats_size(PyObject *module, PyObject *value)
{
    (void)module;
    unsigned long size = PyLong_AsUnsignedLong(value);
    if (PyErr_Occurred()) return NULL;
    stats_size = (uint32_t)size;
    Py_RETURN_NONE;
}

PyObject *test_new_track(PyObject *module, PyObject *capsule)
{
    (void)module;
    moq_media_receiver_t *r = moq5_test_receiver(capsule);
    if (!r) return NULL;
    pthread_mutex_lock(&mutex);
    if (r != current_receiver || r->track_count >= FIXTURE_MAX_TRACKS) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture cannot mint another track");
        return NULL;
    }
    moq_media_track_t *track = &r->tracks[r->track_count];
    track->owner = r;
    track->index = r->track_count++;
    track->state = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    track->ended = false;
    track->removed = false;
    memset(&track->desc, 0, sizeof(track->desc));
    track->desc.struct_size = sizeof(track->desc);
    track->desc.info.struct_size = sizeof(track->desc.info);
    track->desc.init.struct_size = sizeof(track->desc.init);
    track->desc_stamp = sizeof(track->desc);
    track->info_stamp = sizeof(track->desc.info);
    track->init_stamp = sizeof(track->desc.init);
    pthread_mutex_unlock(&mutex);
    return moq5_test_track_capsule(capsule, track);
}

PyObject *test_track_state(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *capsule;
    int state, ended, removed;
    if (!PyArg_ParseTuple(args, "Oipp", &capsule, &state, &ended, &removed)) return NULL;
    moq_media_track_t *track = moq5_test_track(capsule);
    if (!track) return NULL;
    pthread_mutex_lock(&mutex);
    track->state = state;
    track->ended = ended;
    track->removed = removed;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_subscribe_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    subscribe_result = (moq_result_t)rc;
    Py_RETURN_NONE;
}

PyObject *test_subscriptions(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    PyObject *list = PyList_New(subscription_count);
    if (!list) return NULL;
    for (int i = 0; i < subscription_count; ++i) {
        const subscription_t *s = &subscriptions[i];
        PyObject *item = Py_BuildValue("(siiOi)", s->subscribe ? "subscribe" : "unsubscribe", s->track,
                                       s->start, s->has_priority ? Py_True : Py_False, s->priority);
        if (!item) {
            Py_DECREF(list);
            return NULL;
        }
        if (PyList_SetItem(list, i, item) < 0) {
            Py_DECREF(list);
            return NULL;
        }
    }
    return list;
}

PyObject *test_stats_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    stats_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_terminal_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    terminal_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* -- track events and descriptions ------------------------------------------ */

moq_result_t moq_media_receiver_poll_track(moq_media_receiver_t *r, moq_media_track_event_t *ev,
                                           size_t ev_size)
{
    assert(r == current_receiver && ev_size == sizeof(*ev));
    pthread_mutex_lock(&mutex);
    track_polls++;
    if (poll_track_result != MOQ_OK) {
        moq_result_t rc = poll_track_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    if (event_count == 0) {
        int n = ++track_empties;
        if (hook_due_locked(HOOK_TRACK_EMPTY, n)) {
            pthread_mutex_unlock(&mutex);
            hooks_run(HOOK_TRACK_EMPTY, n);
            pthread_mutex_lock(&mutex);
        }
    }
    if (event_count == 0) {
        bool terminal = receiver_terminal(r);
        pthread_mutex_unlock(&mutex);
        return terminal ? MOQ_ERR_CLOSED : MOQ_DONE;
    }
    fixture_event_t e = events[event_head];
    event_head = (event_head + 1) % FIXTURE_EVENTS;
    event_count--;
    moq_media_track_event_t full;
    memset(&full, 0, sizeof(full));
    full.struct_size = e.stamp;
    full.kind = (moq_media_track_event_kind_t)e.kind;
    full.track = e.track;
    /* A malformed shape can carry a descriptor without a handle; the pointer
     * is a fixture sentinel the binding must never dereference. */
    static moq_media_track_desc_t orphan_sentinel;
    full.desc = e.track ? &e.track->desc : (e.orphan_desc ? &orphan_sentinel : NULL);
    full.has_largest = e.has_largest;
    full.largest_group = e.largest_group;
    full.largest_object = e.largest_object;
    full.has_expires = e.has_expires;
    full.expires_ms = e.expires_ms;
    full.parse_drop_class = (moq_media_parse_drop_class_t)e.parse_drop_class;
    full.parse_drops_total = e.parse_drops_total;
    full.parse_drops_delta = e.parse_drops_delta;
    /* Write exactly the stamped prefix (clamped to sizeof), like a sized
     * native poll; the caller's bytes beyond it stay whatever they were. */
    size_t n = e.stamp < sizeof(full) ? e.stamp : sizeof(full);
    memcpy(ev, &full, n);
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

moq_result_t moq_media_receiver_track_desc_copy(const moq_media_receiver_t *r,
                                                const moq_media_track_t *track,
                                                moq_media_track_desc_t *out, size_t out_size)
{
    if (!r || !track || !out) return MOQ_ERR_INVAL;
    if (out_size < MOQ_MEDIA_TRACK_DESC_V0_SIZE) return MOQ_ERR_INVAL;
    pthread_mutex_lock(&mutex);
    if (desc_copy_result != MOQ_OK) {
        moq_result_t rc = desc_copy_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    if (!owns(r, track)) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    /* The CURRENT triple, which may differ from the borrowed view's. */
    moq_media_track_desc_t current = track->desc;
    current.is_live = track->current_is_live;
    current.has_track_duration = track->current_has_duration;
    current.track_duration_ms = track->current_duration;
    current.struct_size = track->desc_stamp;
    current.info.struct_size = track->info_stamp;
    current.init.struct_size = track->init_stamp;
    /* Poison the nested bodies beyond their stamped prefixes: a reader that
     * ignores a nested stamp observes garbage, not the scripted values. */
    if (track->info_stamp < sizeof(current.info))
        memset((uint8_t *)&current.info + track->info_stamp, 0xEE, sizeof(current.info) - track->info_stamp);
    if (track->init_stamp < sizeof(current.init))
        memset((uint8_t *)&current.init + track->init_stamp, 0xEE, sizeof(current.init) - track->init_stamp);
    size_t n = out_size < sizeof(current) ? out_size : sizeof(current);
    if (track->desc_stamp < n) n = track->desc_stamp;
    memcpy(out, &current, n);
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

/* dict readers: absent or None means "not present" */
static PyObject *field(PyObject *dict, const char *key)
{
    PyObject *v = PyDict_GetItemString(dict, key);
    return (v && v != Py_None) ? v : NULL;
}

static int take_text(PyObject *dict, const char *key, text_t *t, moq_bytes_t *span)
{
    PyObject *v = field(dict, key);
    t->len = 0;
    *span = (moq_bytes_t){ t->data, 0 };
    if (!v) return 0;
    char *data;
    Py_ssize_t size;
    if (!PyBytes_Check(v)) {
        PyErr_Format(PyExc_TypeError, "fixture %s must be bytes", key);
        return -1;
    }
    if (PyBytes_AsStringAndSize(v, &data, &size) < 0) return -1;
    if ((size_t)size > sizeof(t->data)) {
        PyErr_Format(PyExc_ValueError, "fixture %s too long", key);
        return -1;
    }
    memcpy(t->data, data, (size_t)size);
    t->len = (size_t)size;
    span->len = t->len;
    return 0;
}

static int take_u64(PyObject *dict, const char *key, bool *has, uint64_t *value)
{
    PyObject *v = field(dict, key);
    *has = false;
    *value = 0;
    if (!v) return 0;
    unsigned long long n = PyLong_AsUnsignedLongLong(v);
    if (PyErr_Occurred()) return -1;
    *has = true;
    *value = (uint64_t)n;
    return 0;
}

static int take_array(PyObject *dict, const char *key, text_t *texts, moq_bytes_t *spans, size_t *count)
{
    PyObject *v = field(dict, key);
    *count = 0;
    if (!v) return 0;
    if (!PyTuple_Check(v) || PyTuple_Size(v) > FIXTURE_ARRAY) {
        PyErr_Format(PyExc_TypeError, "fixture %s must be a tuple of at most %d bytes", key, FIXTURE_ARRAY);
        return -1;
    }
    for (Py_ssize_t i = 0; i < PyTuple_Size(v); ++i) {
        PyObject *one = PyDict_New();
        if (!one) return -1;
        int rc = PyDict_SetItemString(one, "x", PyTuple_GetItem(v, i));
        if (rc == 0) rc = take_text(one, "x", &texts[i], &spans[i]);
        Py_DECREF(one);
        if (rc < 0) return -1;
    }
    *count = (size_t)PyTuple_Size(v);
    return 0;
}

/* _test_track_desc(track_capsule, dict): script the borrowed description and
 * the copy's stamps. Keys absent or None are "not carried". `null_span`
 * names one span whose data pointer is nulled while its length is kept, to
 * exercise the binding's malformed-output path. */
PyObject *test_track_desc(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *capsule, *dict;
    if (!PyArg_ParseTuple(args, "OO!", &capsule, &PyDict_Type, &dict)) return NULL;
    moq_media_track_t *t = moq5_test_track(capsule);
    if (!t) return NULL;
    moq_media_track_desc_t *d = &t->desc;
    bool has;
    uint64_t v;
    pthread_mutex_lock(&mutex);
    memset(d, 0, sizeof(*d));
    d->struct_size = sizeof(*d);
    if (take_text(dict, "name", &t->name, &d->name) < 0 ||
        take_text(dict, "role", &t->role, &d->role) < 0 ||
        take_text(dict, "codec", &t->codec, &d->codec) < 0 ||
        take_text(dict, "lang", &t->lang, &d->lang) < 0 ||
        take_text(dict, "label", &t->label, &d->label) < 0 ||
        take_text(dict, "packaging_text", &t->packaging_text, &d->packaging) < 0 ||
        take_text(dict, "event_type", &t->event_type, &d->event_type) < 0 ||
        take_text(dict, "mime_type", &t->mime_type, &d->mime_type) < 0 ||
        take_text(dict, "channel_config", &t->channel_config, &d->channel_config) < 0 ||
        take_text(dict, "init_data", &t->init_data, &d->init_data) < 0 ||
        take_array(dict, "depends", t->depends_text, t->depends, &d->depends_count) < 0 ||
        take_array(dict, "content_protection_ref_ids", t->cp_text, t->cp, &d->content_protection_ref_id_count) < 0)
        goto fail;
    d->depends = d->depends_count ? t->depends : NULL;
    d->content_protection_ref_ids = d->content_protection_ref_id_count ? t->cp : NULL;
    if (take_u64(dict, "media_type", &has, &v) < 0) goto fail;
    d->info.media_type = (moq_media_type_t)v;
    if (take_u64(dict, "packaging", &has, &v) < 0) goto fail;
    d->info.packaging = (moq_media_packaging_t)v;
    if (take_u64(dict, "timescale", &has, &v) < 0) goto fail;
    d->info.timescale = (uint32_t)v;
    if (take_u64(dict, "transport_version", &has, &v) < 0) goto fail;
    d->info.transport_version = (moq_version_t)v;
    d->info.struct_size = sizeof(d->info);
    t->info_stamp = has ? (uint32_t)sizeof(d->info) : (uint32_t)MOQ_MEDIA_TRACK_INFO_V0_SIZE;
    PyObject *init = field(dict, "init");
    d->has_init = init != NULL;
    d->init.struct_size = sizeof(d->init);
    t->init_stamp = sizeof(d->init);
    if (init) {
        if (!PyDict_Check(init)) { PyErr_SetString(PyExc_TypeError, "fixture init must be a dict"); goto fail; }
        if (take_u64(init, "codec_kind", &has, &v) < 0) goto fail;
        d->init.codec_kind = (moq_cmaf_codec_kind_t)v;
        if (take_u64(init, "timescale", &has, &v) < 0) goto fail;
        d->init.timescale = (uint32_t)v;
        if (take_u64(init, "width", &has, &v) < 0) goto fail;
        d->init.width = (uint32_t)v;
        if (take_u64(init, "height", &has, &v) < 0) goto fail;
        d->init.height = (uint32_t)v;
        if (take_u64(init, "samplerate", &has, &v) < 0) goto fail;
        d->init.samplerate = (uint32_t)v;
        if (take_u64(init, "channel_count", &has, &v) < 0) goto fail;
        d->init.channel_count = (uint32_t)v;
        if (take_u64(init, "track_id", &has, &v) < 0) goto fail;
        d->init.track_id = (uint32_t)v;
        if (take_text(init, "codec_config", &t->codec_config, &d->init.codec_config) < 0) goto fail;
        PyObject *cenc = field(init, "cenc");
        d->init.has_cenc = cenc != NULL;
        if (cenc) {
            PyObject *scheme, *kid;
            int protected_, iv;
            if (!PyArg_ParseTuple(cenc, "OiiO", &scheme, &protected_, &iv, &kid)) goto fail;
            PyObject *one = Py_BuildValue("{s:O,s:O}", "scheme", scheme, "kid", kid);
            if (!one) goto fail;
            int rc = take_text(one, "scheme", &t->scheme, &d->init.scheme);
            if (rc == 0) rc = take_text(one, "kid", &t->kid, &d->init.default_kid);
            Py_DECREF(one);
            if (rc < 0) goto fail;
            d->init.default_is_protected = (uint8_t)protected_;
            d->init.default_per_sample_iv_size = (uint8_t)iv;
        }
    }
    if (take_u64(dict, "width", &d->has_width, &v) < 0) goto fail;
    d->width = (uint32_t)v;
    if (take_u64(dict, "height", &d->has_height, &v) < 0) goto fail;
    d->height = (uint32_t)v;
    if (take_u64(dict, "samplerate", &d->has_samplerate, &v) < 0) goto fail;
    d->samplerate = (uint32_t)v;
    if (take_u64(dict, "framerate_millis", &d->has_framerate, &d->framerate_millis) < 0) goto fail;
    if (take_u64(dict, "bitrate", &d->has_bitrate, &d->bitrate) < 0) goto fail;
    if (take_u64(dict, "max_grp_sap", &d->has_max_grp_sap, &v) < 0) goto fail;
    d->max_grp_sap = (uint32_t)v;
    if (take_u64(dict, "max_obj_sap", &d->has_max_obj_sap, &v) < 0) goto fail;
    d->max_obj_sap = (uint32_t)v;
    PyObject *tmpl = field(dict, "template");
    d->has_template = tmpl != NULL;
    if (tmpl) {
        unsigned long long m[8];
        if (!PyArg_ParseTuple(tmpl, "KKKKKKKK", &m[0], &m[1], &m[2], &m[3], &m[4], &m[5], &m[6], &m[7])) goto fail;
        d->template_.start_media_ms = m[0]; d->template_.delta_media_ms = m[1];
        d->template_.start_group = m[2]; d->template_.start_object = m[3];
        d->template_.delta_group = m[4]; d->template_.delta_object = m[5];
        d->template_.start_wallclock_ms = m[6]; d->template_.delta_wallclock_ms = m[7];
    }
    {
        PyObject *live = field(dict, "is_live");
        d->is_live = live ? PyObject_IsTrue(live) == 1 : false;
        if (take_u64(dict, "track_duration_ms", &d->has_track_duration, &d->track_duration_ms) < 0) goto fail;
        /* the copy's current triple defaults to the borrowed values */
        t->current_is_live = d->is_live;
        t->current_has_duration = d->has_track_duration;
        t->current_duration = d->track_duration_ms;
    }
    if (take_u64(dict, "desc_stamp", &has, &v) < 0) goto fail;
    t->desc_stamp = has ? (uint32_t)v : (uint32_t)sizeof(*d);
    if (take_u64(dict, "info_stamp", &has, &v) < 0) goto fail;
    if (has) t->info_stamp = (uint32_t)v;
    if (take_u64(dict, "init_stamp", &has, &v) < 0) goto fail;
    if (has) t->init_stamp = (uint32_t)v;
    {
        PyObject *null_span = field(dict, "null_span");
        if (null_span) {
            const char *which = PyUnicode_AsUTF8AndSize(null_span, NULL);
            if (!which) goto fail;
            if (strcmp(which, "name") == 0) d->name.data = NULL;
            else if (strcmp(which, "depends") == 0) d->depends = NULL;
            else if (strcmp(which, "content_protection_ref_ids") == 0) d->content_protection_ref_ids = NULL;
            else if (strcmp(which, "codec_config") == 0) d->init.codec_config.data = NULL;
            else if (strcmp(which, "init_data") == 0) d->init_data.data = NULL;
            else { PyErr_SetString(PyExc_ValueError, "unknown null_span"); goto fail; }
        }
        /* Malformed COUNTS and LENGTHS with valid pointers, never backed by
         * storage: an impossible array product, and a span longer than
         * Py_ssize_t. The binding must refuse them by arithmetic alone. */
        PyObject *oversized_count = field(dict, "oversized_count");
        if (oversized_count) {
            const char *which = PyUnicode_AsUTF8AndSize(oversized_count, NULL);
            if (!which) goto fail;
            size_t impossible = SIZE_MAX / sizeof(moq_bytes_t) + 1;
            if (strcmp(which, "depends") == 0) { d->depends = t->depends; d->depends_count = impossible; }
            else if (strcmp(which, "content_protection_ref_ids") == 0) { d->content_protection_ref_ids = t->cp; d->content_protection_ref_id_count = impossible; }
            else { PyErr_SetString(PyExc_ValueError, "unknown oversized_count"); goto fail; }
        }
        PyObject *oversized_len = field(dict, "oversized_len");
        if (oversized_len) {
            const char *which = PyUnicode_AsUTF8AndSize(oversized_len, NULL);
            if (!which) goto fail;
            size_t too_long = (size_t)PY_SSIZE_T_MAX + 1u;
            if (strcmp(which, "name") == 0) d->name.len = too_long;
            else if (strcmp(which, "codec_config") == 0) d->init.codec_config.len = too_long;
            else if (strcmp(which, "depends") == 0) { d->depends = t->depends; d->depends_count = 1; t->depends[0].len = too_long; }
            else { PyErr_SetString(PyExc_ValueError, "unknown oversized_len"); goto fail; }
        }
    }
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
fail:
    pthread_mutex_unlock(&mutex);
    return NULL;
}

PyObject *test_track_current(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *capsule, *duration;
    int is_live;
    if (!PyArg_ParseTuple(args, "OpO", &capsule, &is_live, &duration)) return NULL;
    moq_media_track_t *t = moq5_test_track(capsule);
    if (!t) return NULL;
    unsigned long long value = 0;
    if (duration != Py_None) {
        value = PyLong_AsUnsignedLongLong(duration);
        if (PyErr_Occurred()) return NULL;
    }
    pthread_mutex_lock(&mutex);
    t->current_is_live = is_live;
    t->current_has_duration = duration != Py_None;
    t->current_duration = (uint64_t)value;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* _test_push_event(kind, track_capsule|None, stamp|None, largest|None,
 *                  expires|None, parse_drop|None) */
PyObject *test_push_event(PyObject *module, PyObject *args)
{
    (void)module;
    int kind;
    PyObject *capsule, *stamp, *largest, *expires, *drop;
    if (!PyArg_ParseTuple(args, "iOOOOO", &kind, &capsule, &stamp, &largest, &expires, &drop)) return NULL;
    fixture_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = kind;
    e.stamp = sizeof(moq_media_track_event_t);
    if (capsule != Py_None) {
        e.track = moq5_test_track(capsule);
        if (!e.track) return NULL;
    }
    if (stamp != Py_None) {
        unsigned long v = PyLong_AsUnsignedLong(stamp);
        if (PyErr_Occurred()) return NULL;
        e.stamp = (uint32_t)v;
    }
    if (largest != Py_None) {
        unsigned long long g, o;
        if (!PyArg_ParseTuple(largest, "KK", &g, &o)) return NULL;
        e.has_largest = true;
        e.largest_group = g;
        e.largest_object = o;
    }
    if (expires != Py_None) {
        unsigned long long v = PyLong_AsUnsignedLongLong(expires);
        if (PyErr_Occurred()) return NULL;
        e.has_expires = true;
        e.expires_ms = v;
    }
    if (drop != Py_None) {
        unsigned long long total, delta;
        if (!PyArg_ParseTuple(drop, "iKK", &e.parse_drop_class, &total, &delta)) return NULL;
        e.parse_drops_total = total;
        e.parse_drops_delta = delta;
    }
    pthread_mutex_lock(&mutex);
    if (event_count >= FIXTURE_EVENTS) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture event queue is full");
        return NULL;
    }
    events[(event_head + event_count) % FIXTURE_EVENTS] = e;
    event_count++;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_poll_track_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    poll_track_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_desc_copy_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    desc_copy_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Overwrite every backing byte the track's spans point at, keeping lengths:
 * a binding that borrowed would now observe garbage. */
PyObject *test_scramble_track(PyObject *module, PyObject *capsule)
{
    (void)module;
    moq_media_track_t *t = moq5_test_track(capsule);
    if (!t) return NULL;
    pthread_mutex_lock(&mutex);
    text_t *texts[] = { &t->name, &t->role, &t->codec, &t->lang, &t->label, &t->packaging_text,
                        &t->event_type, &t->mime_type, &t->channel_config, &t->init_data,
                        &t->codec_config, &t->scheme, &t->kid };
    for (size_t i = 0; i < sizeof(texts) / sizeof(texts[0]); ++i) memset(texts[i]->data, 0xAA, sizeof(texts[i]->data));
    for (int i = 0; i < FIXTURE_ARRAY; ++i) {
        memset(t->depends_text[i].data, 0xAA, sizeof(t->depends_text[i].data));
        memset(t->cp_text[i].data, 0xAA, sizeof(t->cp_text[i].data));
    }
    t->desc.width = 0xAAAAAAAAu;
    t->desc.template_.start_media_ms = 0xAAAAAAAAAAAAAAAAull;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Compiled field ends and sizes, so the tests gate on the real ABI. */
#define END(type, f) (offsetof(type, f) + sizeof(((type *)0)->f))
PyObject *test_layout(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    return Py_BuildValue("{s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n}",
        "event_v0", (Py_ssize_t)(offsetof(moq_media_track_event_t, config_generation) + sizeof(uint32_t)),
        "event_has_largest_end", (Py_ssize_t)END(moq_media_track_event_t, has_largest),
        "event_largest_object_end", (Py_ssize_t)END(moq_media_track_event_t, largest_object),
        "event_has_expires_end", (Py_ssize_t)END(moq_media_track_event_t, has_expires),
        "event_expires_end", (Py_ssize_t)END(moq_media_track_event_t, expires_ms),
        "event_parse_drop_class_end", (Py_ssize_t)END(moq_media_track_event_t, parse_drop_class),
        "event_parse_drops_total_end", (Py_ssize_t)END(moq_media_track_event_t, parse_drops_total),
        "event_parse_drops_delta_end", (Py_ssize_t)END(moq_media_track_event_t, parse_drops_delta),
        "event_sizeof", (Py_ssize_t)sizeof(moq_media_track_event_t),
        "desc_v0", (Py_ssize_t)MOQ_MEDIA_TRACK_DESC_V0_SIZE,
        "desc_sizeof", (Py_ssize_t)sizeof(moq_media_track_desc_t),
        "info_v0", (Py_ssize_t)MOQ_MEDIA_TRACK_INFO_V0_SIZE,
        "info_transport_version_end", (Py_ssize_t)END(moq_media_track_info_t, transport_version),
        "info_sizeof", (Py_ssize_t)sizeof(moq_media_track_info_t),
        "init_codec_kind_end", (Py_ssize_t)END(moq_cmaf_init_info_t, codec_kind),
        "init_timescale_end", (Py_ssize_t)END(moq_cmaf_init_info_t, timescale),
        "init_width_end", (Py_ssize_t)END(moq_cmaf_init_info_t, width),
        "init_height_end", (Py_ssize_t)END(moq_cmaf_init_info_t, height),
        "init_samplerate_end", (Py_ssize_t)END(moq_cmaf_init_info_t, samplerate),
        "init_channel_count_end", (Py_ssize_t)END(moq_cmaf_init_info_t, channel_count),
        "init_codec_config_end", (Py_ssize_t)END(moq_cmaf_init_info_t, codec_config),
        "init_track_id_end", (Py_ssize_t)END(moq_cmaf_init_info_t, track_id),
        "init_has_cenc_end", (Py_ssize_t)END(moq_cmaf_init_info_t, has_cenc),
        "init_scheme_end", (Py_ssize_t)END(moq_cmaf_init_info_t, scheme),
        "init_default_is_protected_end", (Py_ssize_t)END(moq_cmaf_init_info_t, default_is_protected),
        "init_default_per_sample_iv_size_end", (Py_ssize_t)END(moq_cmaf_init_info_t, default_per_sample_iv_size),
        "init_default_kid_end", (Py_ssize_t)END(moq_cmaf_init_info_t, default_kid),
        "init_sizeof", (Py_ssize_t)sizeof(moq_cmaf_init_info_t),
        "stats_sizeof", (Py_ssize_t)sizeof(moq_media_receiver_stats_t),
        "object_v0", (Py_ssize_t)(offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *)),
        "object_sizeof", (Py_ssize_t)sizeof(moq_media_object_t),
        "sample_sizeof", (Py_ssize_t)sizeof(moq_cmaf_sample_t),
        "status_normal", (Py_ssize_t)MOQ_OBJECT_NORMAL,
        "status_end_of_group", (Py_ssize_t)MOQ_OBJECT_END_OF_GROUP,
        "status_end_of_track", (Py_ssize_t)MOQ_OBJECT_END_OF_TRACK);
}

/* _test_push_orphan_desc(kind): queue an event of `kind` with NO handle and a
 * non-NULL descriptor pointer. */
PyObject *test_push_orphan_desc(PyObject *module, PyObject *value)
{
    (void)module;
    long kind = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    fixture_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = (int)kind;
    e.stamp = sizeof(moq_media_track_event_t);
    e.orphan_desc = true;
    pthread_mutex_lock(&mutex);
    if (event_count >= FIXTURE_EVENTS) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture event queue is full");
        return NULL;
    }
    events[(event_head + event_count) % FIXTURE_EVENTS] = e;
    event_count++;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* -- media objects ------------------------------------------------------------ */

#define OBJECT_V0_SIZE (offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *))

moq_result_t moq_media_receiver_poll_object(moq_media_receiver_t *r, moq_media_object_t *obj,
                                            size_t obj_size)
{
    /* The SDK's order: argument floor, interrupt latch, queue, terminal. */
    if (!r || !obj || obj_size < OBJECT_V0_SIZE) return MOQ_ERR_INVAL;
    size_t n = obj_size < sizeof(*obj) ? obj_size : sizeof(*obj);
    memset(obj, 0, n);
    pthread_mutex_lock(&mutex);
    object_polls++;
    if (poll_object_result != MOQ_OK) {
        moq_result_t rc = poll_object_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    if (r->ep->interrupted) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INTERRUPTED;
    }
    if (object_count == 0) {
        int n = ++object_empties;
        if (hook_due_locked(HOOK_OBJECT_EMPTY, n)) {
            pthread_mutex_unlock(&mutex);
            hooks_run(HOOK_OBJECT_EMPTY, n);
            pthread_mutex_lock(&mutex);
        }
    }
    if (object_count == 0) {
        bool terminal = receiver_terminal(r);
        pthread_mutex_unlock(&mutex);
        return terminal ? MOQ_ERR_CLOSED : MOQ_DONE;
    }
    fixture_object_t *f = &objects[object_head];
    object_head = (object_head + 1) % FIXTURE_OBJECTS;
    object_count--;
    /* exclusive ownership transfers: the queue entry forgets its refs */
    moq_media_object_t full = f->obj;
    full.struct_size = f->stamp;
    size_t copy = n < sizeof(full) ? n : sizeof(full);
    memcpy(obj, &full, copy);
    memset(&f->obj, 0, sizeof(f->obj));
    objects_dequeued++;
    last_poll_thread = pthread_self();
    have_poll_thread = true;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

void moq_media_object_cleanup(moq_media_object_t *obj)
{
    if (!obj) return;
    pthread_mutex_lock(&mutex);
    cleanup_calls++;
    bool effective = obj->payload_ref || obj->properties_ref || obj->samples_owned;
    if (effective) {
        if (have_poll_thread && pthread_equal(pthread_self(), last_poll_thread)) cleanups_on_polling_thread++;
        else cleanups_off_polling_thread++;
    }
    release_refs_locked(obj);
    if (effective) cleanup_effective++;
    size_t n = obj->struct_size;
    if (n > sizeof(*obj)) n = sizeof(*obj);
    if (n) memset(obj, 0, n);
    pthread_mutex_unlock(&mutex);
}

static int take_i64(PyObject *dict, const char *key, int64_t *value)
{
    PyObject *v = field(dict, key);
    *value = 0;
    if (!v) return 0;
    long long n = PyLong_AsLongLong(v);
    if (PyErr_Occurred()) return -1;
    *value = (int64_t)n;
    return 0;
}

static int take_bool(PyObject *dict, const char *key)
{
    PyObject *v = field(dict, key);
    return v ? PyObject_IsTrue(v) == 1 : 0;
}

static int make_ref(PyObject *dict, const char *key, moq_rcbuf_t **ref, moq_bytes_t *span)
{
    PyObject *v = field(dict, key);
    *ref = NULL;
    *span = (moq_bytes_t){ NULL, 0 };
    if (!v) return 0;
    char *data;
    Py_ssize_t size;
    if (!PyBytes_Check(v)) {
        PyErr_Format(PyExc_TypeError, "fixture %s must be bytes", key);
        return -1;
    }
    if (PyBytes_AsStringAndSize(v, &data, &size) < 0) return -1;
    if (moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)data, (size_t)size, ref) != MOQ_OK) {
        PyErr_NoMemory();
        return -1;
    }
    refs_created++;
    span->data = moq_rcbuf_data(*ref);
    span->len = (size_t)size;
    return 0;
}

/* _test_push_object(track_capsule|None, dict): queue one object with genuine
 * buffers. Keys: packaging, status, end_of_group, datagram, keyframe,
 * capture_time_us (None -> absent), decode_time_us, composition_offset_us
 * (signed), presentation_time_us, payload / fragment / properties (bytes or
 * None -> no buffer), mdat_offset, mdat_len, samples (tuple of 4-tuples),
 * config_generation, stamp (>= v0), and `malformed` naming ONE public-shape
 * corruption that leaves every ownership ref intact:
 *   payload_null_len, fragment_null_len, payload_len_over, samples_null,
 *   sample_count_product, mdat_offset_over, mdat_len_over,
 *   status_with_payload, normal_without_payload. */
PyObject *test_push_object(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *capsule, *dict;
    if (!PyArg_ParseTuple(args, "OO!", &capsule, &PyDict_Type, &dict)) return NULL;
    moq_media_track_t *track = NULL;
    if (capsule != Py_None) {
        track = moq5_test_track(capsule);
        if (!track) return NULL;
    }
    pthread_mutex_lock(&mutex);
    if (object_count >= FIXTURE_OBJECTS || kept_count + 2 > (int)(sizeof(kept) / sizeof(kept[0]))) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture object queue is full");
        return NULL;
    }
    fixture_object_t *f = &objects[(object_head + object_count) % FIXTURE_OBJECTS];
    memset(f, 0, sizeof(*f));
    moq_media_object_t *o = &f->obj;
    o->struct_size = sizeof(*o);
    f->stamp = sizeof(*o);
    o->track = track;
    bool has;
    uint64_t u;
    int64_t i64;
    if (take_u64(dict, "config_generation", &has, &u) < 0) goto fail;
    o->config_generation = (uint32_t)u;
    if (take_u64(dict, "packaging", &has, &u) < 0) goto fail;
    o->packaging = (moq_media_packaging_t)u;
    if (take_u64(dict, "status", &has, &u) < 0) goto fail;
    o->status = (moq_object_status_t)u;
    o->end_of_group = take_bool(dict, "end_of_group");
    o->datagram = take_bool(dict, "datagram");
    o->keyframe = take_bool(dict, "keyframe");
    if (take_u64(dict, "capture_time_us", &o->has_capture_time, &o->capture_time_us) < 0) goto fail;
    if (take_u64(dict, "decode_time_us", &has, &o->decode_time_us) < 0) goto fail;
    if (take_i64(dict, "composition_offset_us", &i64) < 0) goto fail;
    o->composition_offset_us = i64;
    if (take_u64(dict, "presentation_time_us", &has, &o->presentation_time_us) < 0) goto fail;
    /* buffers: RAW media in payload_ref; a CMAF fragment also lives in
     * payload_ref (the production layout) with the public payload span empty */
    PyObject *fragment = field(dict, "fragment");
    if (fragment) {
        if (make_ref(dict, "fragment", &o->payload_ref, &o->fragment) < 0) goto fail;
    } else if (make_ref(dict, "payload", &o->payload_ref, &o->payload) < 0) {
        goto fail;
    }
    if (make_ref(dict, "properties", &o->properties_ref, &(moq_bytes_t){ NULL, 0 }) < 0) goto fail;
    if (take_u64(dict, "mdat_offset", &has, &u) < 0) goto fail;
    o->mdat_offset = (size_t)u;
    if (take_u64(dict, "mdat_len", &has, &u) < 0) goto fail;
    o->mdat_len = (size_t)u;
    PyObject *samples = field(dict, "samples");
    if (samples) {
        if (!PyTuple_Check(samples) || PyTuple_Size(samples) > FIXTURE_SAMPLES) {
            PyErr_SetString(PyExc_TypeError, "fixture samples must be a tuple of at most 8 records");
            goto fail;
        }
        size_t count = (size_t)PyTuple_Size(samples);
        moq_cmaf_sample_t *owned = sample_block_alloc(count);
        if (!owned) { PyErr_NoMemory(); goto fail; }
        for (size_t i = 0; i < count; ++i) {
            unsigned long duration, size, flags;
            long offset;
            if (!PyArg_ParseTuple(PyTuple_GetItem(samples, (Py_ssize_t)i), "kkkl", &duration, &size, &flags, &offset)) {
                sample_block_free(owned, count * sizeof(*owned));
                goto fail;
            }
            owned[i].duration = (uint32_t)duration;
            owned[i].size = (uint32_t)size;
            owned[i].flags = (uint32_t)flags;
            owned[i].composition_offset = (int32_t)offset;
        }
        samples_allocated++;
        o->samples_owned = owned;
        o->samples = owned;
        o->sample_count = count;
    }
    if (take_u64(dict, "stamp", &has, &u) < 0) goto fail;
    if (has) {
        /* Representability is checked on the full value BEFORE narrowing: a
         * stamp below v0 would strand ownership, and one beyond uint32 would
         * wrap into that range. Neither leaves a queued object or a buffer. */
        if (u < OBJECT_V0_SIZE || u > UINT32_MAX) {
            PyErr_SetString(PyExc_ValueError, "an object stamp below v0 or beyond uint32 is not representable");
            goto fail;
        }
        f->stamp = (uint32_t)u;
    }
    PyObject *malformed = field(dict, "malformed");
    if (malformed) {
        const char *which = PyUnicode_AsUTF8AndSize(malformed, NULL);
        if (!which) goto fail;
        if (strcmp(which, "payload_null_len") == 0) { if (!o->payload.len) o->payload.len = 1; o->payload.data = NULL; }
        else if (strcmp(which, "fragment_null_len") == 0) { if (!o->fragment.len) o->fragment.len = 1; o->fragment.data = NULL; }
        else if (strcmp(which, "payload_len_over") == 0) o->payload.len = (size_t)PY_SSIZE_T_MAX + 1u;
        else if (strcmp(which, "samples_null") == 0) { o->samples = NULL; if (!o->sample_count) o->sample_count = 1; }
        else if (strcmp(which, "sample_count_product") == 0) {
            /* a public array that cannot exist: borrowed pointer, impossible
             * count; samples_owned/sample_count for CLEANUP stay coherent by
             * moving ownership to NULL (nothing owned, nothing freed) */
            if (o->samples_owned) {
                sample_block_free(o->samples_owned, o->sample_count * sizeof(moq_cmaf_sample_t));
                samples_allocated--;
                o->samples_owned = NULL;
            }
            o->samples = f->borrowed_samples;
            o->sample_count = SIZE_MAX / sizeof(moq_cmaf_sample_t) + 1;
        }
        else if (strcmp(which, "mdat_offset_over") == 0) o->mdat_offset = o->fragment.len + 1;
        else if (strcmp(which, "mdat_len_over") == 0) o->mdat_len = o->fragment.len - o->mdat_offset + 1;
        else if (strcmp(which, "status_with_payload") == 0) { /* status object with a payload ref: refs genuine */ o->status = MOQ_OBJECT_END_OF_TRACK; }
        else if (strcmp(which, "normal_without_payload") == 0) { release_refs_locked(o); o->payload = (moq_bytes_t){ NULL, 0 }; o->status = MOQ_OBJECT_NORMAL; }
        else { PyErr_SetString(PyExc_ValueError, "unknown malformed"); goto fail; }
    }
    /* keep one extra reference to every buffer so the bytes can be scrambled
     * after the binding's cleanup */
    if (o->payload_ref) kept[kept_count++] = moq_rcbuf_incref(o->payload_ref);
    if (o->properties_ref) kept[kept_count++] = moq_rcbuf_incref(o->properties_ref);
    object_count++;
    objects_pushed++;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
fail:
    release_refs_locked(o);
    memset(f, 0, sizeof(*f));
    pthread_mutex_unlock(&mutex);
    return NULL;
}

PyObject *test_object_counts(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    /* actual_kept_refcounts: the sum of the REAL refcounts of every buffer
     * the fixture still references; each equals 1 once the transferred
     * reference is gone, so the sum equals kept_count exactly then. */
    long actual = 0;
    for (int i = 0; i < kept_count; ++i) actual += (long)moq_rcbuf_refcount(kept[i]);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:l,s:i,s:i,s:i,s:i}",
        "pushed", objects_pushed, "dequeued", objects_dequeued,
        "cleanup_calls", cleanup_calls, "cleanup_effective", cleanup_effective,
        "refs_created", refs_created, "refs_released", refs_released,
        "samples_allocated", samples_allocated, "samples_freed", samples_freed,
        "released_on_destroy", released_on_destroy, "queued", object_count,
        "kept_buffers", kept_count, "actual_kept_refcounts", actual,
        "sample_blocks_live", sample_blocks_live, "sample_bad_free", sample_bad_free,
        "cleanups_on_polling_thread", cleanups_on_polling_thread,
        "cleanups_off_polling_thread", cleanups_off_polling_thread);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Overwrite the bytes of every buffer the fixture still references (after
 * the binding released its own reference), so a borrowing binding would
 * observe garbage. */
PyObject *test_scramble_objects(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    for (int i = 0; i < kept_count; ++i)
        memset((uint8_t *)(uintptr_t)moq_rcbuf_data(kept[i]), 0xAA, moq_rcbuf_len(kept[i]));
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_poll_object_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    poll_object_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Fixture self-check: one native poll into a local struct, then cleanup --
 * proves the transfer/cleanup accounting without the binding. Returns
 * (rc, stamp, payload_len, fragment_len, sample_count, cleanup_effective_delta). */
PyObject *test_native_object_cycle(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    if (!current_receiver) {
        PyErr_SetString(PyExc_RuntimeError, "no receiver");
        return NULL;
    }
    moq_media_object_t obj;
    int before = cleanup_effective;
    moq_result_t rc = moq_media_receiver_poll_object(current_receiver, &obj, sizeof(obj));
    unsigned stamp = obj.struct_size;
    size_t plen = obj.payload.len, flen = obj.fragment.len, count = obj.sample_count;
    if (rc == MOQ_OK) moq_media_object_cleanup(&obj);
    return Py_BuildValue("(iInnni)", (int)rc, stamp, (Py_ssize_t)plen, (Py_ssize_t)flen, (Py_ssize_t)count,
                         cleanup_effective - before);
}

/* Checker controls for the polling-thread evidence: hold dequeues one object
 * on the calling thread into a static holder; cycle_split cleans the held
 * object on WHATEVER thread calls it (serialized by the fixture mutex; no
 * concurrent release ever happens) and returns the effective-cleanup delta. */
static moq_media_object_t held_object;
static bool holding;

PyObject *test_native_object_hold(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    if (!current_receiver || holding) {
        PyErr_SetString(PyExc_RuntimeError, holding ? "already holding" : "no receiver");
        return NULL;
    }
    moq_result_t rc = moq_media_receiver_poll_object(current_receiver, &held_object, sizeof(held_object));
    holding = rc == MOQ_OK;
    return PyLong_FromLong((long)rc);
}

PyObject *test_native_object_cycle_split(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    if (!holding) {
        PyErr_SetString(PyExc_RuntimeError, "nothing held");
        return NULL;
    }
    int before = cleanup_effective;
    moq_media_object_cleanup(&held_object);
    holding = false;
    return Py_BuildValue("(i)", cleanup_effective - before);
}

/* Final settlement for a test's own teardown (independent of any later
 * reset): no queued object, every fixture-kept buffer at refcount 1, no live
 * or badly freed sample block; then the fixture's own references are
 * dropped. Raises by name otherwise, leaving the evidence in place. */
PyObject *test_object_settle(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    const char *problem = NULL;
    if (object_count) problem = "queued objects were never released";
    for (int i = 0; i < kept_count && !problem; ++i)
        if (moq_rcbuf_refcount(kept[i]) != 1) problem = "a transferred buffer is still referenced";
    if (!problem && sample_blocks_live) problem = "sample blocks are still live";
    if (!problem && sample_bad_free) problem = "a sample block was freed with a wrong pointer or size";
    if (problem) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_AssertionError, problem);
        return NULL;
    }
    for (int i = 0; i < kept_count; ++i) moq_rcbuf_decref(kept[i]);
    int settled = kept_count;
    kept_count = 0;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong(settled);
}

/* _test_at_boundary("track_empty"|"object_empty"|"wait", nth, callable) */
PyObject *test_at_boundary(PyObject *module, PyObject *args)
{
    (void)module;
    const char *name;
    int nth;
    PyObject *callable;
    if (!PyArg_ParseTuple(args, "siO", &name, &nth, &callable)) return NULL;
    int boundary = strcmp(name, "track_empty") == 0 ? HOOK_TRACK_EMPTY
                 : strcmp(name, "object_empty") == 0 ? HOOK_OBJECT_EMPTY
                 : strcmp(name, "wait") == 0 ? HOOK_WAIT : 0;
    if (!boundary || nth < 1 || !PyCallable_Check(callable)) {
        PyErr_SetString(PyExc_ValueError, "boundary must be track_empty/object_empty/wait, nth >= 1, callable");
        return NULL;
    }
    pthread_mutex_lock(&mutex);
    if (hook_count >= FIXTURE_HOOKS) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture hook table is full");
        return NULL;
    }
    hooks[hook_count++] = (fixture_hook_t){ boundary, nth, Py_NewRef(callable), false };
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_wait_budget(PyObject *module, PyObject *value)
{
    (void)module;
    long n = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    wait_budget = (int)n;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_loop_counts(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:i,s:i,s:i,s:i,s:i,s:i}",
        "track_polls", track_polls, "object_polls", object_polls,
        "track_empties", track_empties, "object_empties", object_empties,
        "waits", wait_calls, "hooks_armed", hook_count, "hooks_fired", hooks_fired,
        "hook_failures", hook_failures);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Re-raises the first hook exception (once) or returns the number of hooks
 * that fired; an armed hook that never fired is reported by name. */
PyObject *test_boundary_report(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *err = hook_error;
    hook_error = NULL;
    int fired = hooks_fired, armed = hook_count;
    pthread_mutex_unlock(&mutex);
    if (err) {
        PyErr_SetRaisedException(err);
        return NULL;
    }
    if (fired != armed) {
        PyErr_Format(PyExc_AssertionError, "%d of %d armed boundary hooks never fired", armed - fired, armed);
        return NULL;
    }
    return PyLong_FromLong(fired);
}


/* -- sender fixture implementation ---------------------------------------- */

static void sender_log_add(const char *what)
{
    if (sender_log_count < FIXTURE_SENDER_LOG) sender_log[sender_log_count++] = what;
    else sender_log_overflow = true;
}

/* A span is usable only when its pointer and length agree. Nothing is read
 * before this returns true. */
static bool span_coherent(moq_bytes_t b)
{
    return b.len == 0 || b.data != NULL;
}

typedef enum {
    SENDER_SHAPE_OK,
    SENDER_SHAPE_UNSUPPORTED_PREFIX,   /* fixture restriction, not a C ABI rule */
    SENDER_SHAPE_REFUSED               /* a span's pointer and length disagree */
} sender_shape_t;

static sender_shape_t sender_record_cfg(const moq_media_sender_cfg_t *cfg)
{
    scfg_seen = true;
    /* The captured image belongs to THIS call. Clear it before anything else,
     * including before the size gate, so no slot, scalar or byte can survive
     * from an earlier call. The sticky diagnostic witnesses below are separate
     * and deliberately retain their history. */
    memset(scfg_parts, 0, sizeof(scfg_parts));
    memset(scfg_part_captured, 0, sizeof(scfg_part_captured));
    memset(scfg_part_declared, 0, sizeof(scfg_part_declared));
    scfg_declared_count = scfg_captured_count = 0;
    memset(scfg_catalog, 0, sizeof(scfg_catalog));
    scfg_catalog_captured = scfg_catalog_declared = 0;
    scfg_backpressure = 0;
    scfg_block_timeout_us = scfg_catalog_refresh_interval_us = 0;
    scfg_queue_max_objects = scfg_queue_max_bytes = 0;
    scfg_pre_ready_max_objects = scfg_pre_ready_max_bytes = 0;
    scfg_validate_cmaf = scfg_publish_tracks = scfg_drop_without_demand = false;
    scfg_endpoint_null = scfg_callbacks_absent = false;
    scfg_callbacks_struct_size = 0;
    scfg_content_protections_absent = false;

    /* Only struct_size may be read before the advertised prefix is known. This
     * fixture records the FULL CURRENT configuration and nothing shorter; the
     * real SDK accepts older prefixes, so this is an oracle restriction and is
     * reported as one, never as a public rule. */
    scfg_full_size = (cfg->struct_size == sizeof(*cfg));
    if (!scfg_full_size) {
        scfg_unsupported_prefix = true;
        scfg_unsupported_reason =
            "fixture records the full current sender config only";
        return SENDER_SHAPE_UNSUPPORTED_PREFIX;
    }
    bool refused_here = false;
    scfg_endpoint_null = (cfg->endpoint == NULL);
    scfg_backpressure = (int)cfg->backpressure;
    scfg_block_timeout_us = cfg->block_timeout_us;
    scfg_queue_max_objects = cfg->queue_max_objects;
    scfg_queue_max_bytes = cfg->queue_max_bytes;
    scfg_pre_ready_max_objects = cfg->pre_ready_max_objects;
    scfg_pre_ready_max_bytes = cfg->pre_ready_max_bytes;
    scfg_validate_cmaf = cfg->validate_cmaf;
    scfg_publish_tracks = cfg->publish_tracks;
    scfg_drop_without_demand = cfg->drop_without_demand;
    scfg_catalog_refresh_interval_us = cfg->catalog_refresh_interval_us;
    scfg_content_protections_absent =
        (cfg->content_protections == NULL && cfg->content_protection_count == 0);
    /* Absence is a property of the POINTERS. The nested struct_size is a
     * legitimate stamp the real initializer writes, so it is recorded on its
     * own rather than folded into absence. */
    scfg_callbacks_absent =
        (cfg->callbacks.ctx == NULL &&
         cfg->callbacks.on_subscriber_joined == NULL &&
         cfg->callbacks.on_subscriber_left == NULL &&
         cfg->callbacks.on_ready == NULL && cfg->callbacks.on_closed == NULL &&
         cfg->callbacks.on_track_closed == NULL);
    scfg_callbacks_struct_size = cfg->callbacks.struct_size;

    /* Namespace: refuse an incoherent shape before reading anything. */
    scfg_declared_count = cfg->namespace_.count;
    scfg_captured_count = 0;
    if (cfg->namespace_.count > 0 && cfg->namespace_.parts == NULL) {
        refused_here = true;
        scfg_shape_refused = true;
        scfg_shape_refused_reason = "namespace.parts is NULL with a nonzero count";
    } else {
        if (cfg->namespace_.count > FIXTURE_MAX_PARTS) {
            scfg_oracle_limit = true;
            scfg_oracle_limit_reason = "namespace part count exceeds fixture capacity";
        }
        /* Coherence is checked for EVERY declared part -- the caller promises
         * `count` valid entries -- while only the first FIXTURE_MAX_PARTS are
         * captured. No part's bytes are read until its span is coherent, and
         * the captured image is the LEADING RUN of fully captured parts: once
         * a part cannot be captured, no later part is exposed, so a malformed
         * slot is never filled with fabricated or stale bytes. */
        bool capture_broken = false;
        for (size_t i = 0; i < cfg->namespace_.count; ++i) {
            moq_bytes_t part = cfg->namespace_.parts[i];
            if (i < FIXTURE_MAX_PARTS) scfg_part_declared[i] = part.len;
            if (!span_coherent(part)) {
                refused_here = true;
                capture_broken = true;
                scfg_shape_refused = true;
                scfg_shape_refused_reason =
                    "a namespace part has a NULL pointer with a nonzero length";
                continue;                       /* never dereferenced */
            }
            if (i >= FIXTURE_MAX_PARTS) { capture_broken = true; continue; }
            if (part.len > FIXTURE_SCFG_PART_CAP) {
                capture_broken = true;
                scfg_oracle_limit = true;
                scfg_oracle_limit_reason = "a namespace part exceeds fixture capacity";
                continue;                       /* captured length stays 0 */
            }
            if (part.len) memcpy(scfg_parts[i], part.data, part.len);
            scfg_part_captured[i] = part.len;
            if (!capture_broken) scfg_captured_count = i + 1;
        }
    }

    /* Catalog: same rules. */
    scfg_catalog_declared = cfg->catalog_track.len;
    scfg_catalog_captured = 0;
    if (!span_coherent(cfg->catalog_track)) {
        refused_here = true;
        scfg_shape_refused = true;
        scfg_shape_refused_reason = "catalog_track has a NULL pointer with a nonzero length";
    } else if (cfg->catalog_track.len > FIXTURE_SCFG_CATALOG_CAP) {
        scfg_oracle_limit = true;
        scfg_oracle_limit_reason = "catalog_track exceeds fixture capacity";
    } else if (cfg->catalog_track.len) {
        memcpy(scfg_catalog, cfg->catalog_track.data, cfg->catalog_track.len);
        scfg_catalog_captured = cfg->catalog_track.len;
    }
    return refused_here ? SENDER_SHAPE_REFUSED : SENDER_SHAPE_OK;
}

/* The sized preset the bridge calls. Mirrors the SDK's shape: clear and stamp
 * the prefix the caller's storage covers, and apply the strict-CMAF default
 * where that prefix reaches it. The bridge overwrites every field it sets. */
/* Mirrors the SDK's own initializer, nested stamp included. A control
 * compares this against the real implementation so the two cannot drift. */
void moq_media_sender_callbacks_init_sized(moq_media_sender_callbacks_t *cb,
                                           size_t cb_size)
{
    if (!cb) return;
    size_t n = cb_size < sizeof(*cb) ? cb_size : sizeof(*cb);
    if (n < sizeof(cb->struct_size)) return;
    memset(cb, 0, n);
    cb->struct_size = (uint32_t)n;
}

void moq_media_sender_cfg_init_sized(moq_media_sender_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    {
        size_t cb_off = offsetof(moq_media_sender_cfg_t, callbacks);
        if (n > cb_off)
            moq_media_sender_callbacks_init_sized(&cfg->callbacks, n - cb_off);
    }
    if (n >= offsetof(moq_media_sender_cfg_t, validate_cmaf) + sizeof(cfg->validate_cmaf))
        cfg->validate_cmaf = true;
}

/* The SDK's closed set (sender_validate_cfg): UNSET and anything else fail. */
static bool sender_backpressure_known(moq_media_send_backpressure_t bp)
{
    switch (bp) {
    case MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME:
    case MOQ_MEDIA_SEND_BP_DROP_GROUP:
    case MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT:
    case MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK:
        return true;
    default:
        return false;
    }
}

moq_result_t moq_media_sender_attach(moq_endpoint_t *ep,
                                     const moq_media_sender_cfg_t *cfg,
                                     moq_media_sender_t **out)
{
    pthread_mutex_lock(&mutex);
    sender_attach_entries++;                 /* API ENTRY, before any outcome */
    sender_log_add("sender_attach");
    if (!out || !cfg) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    *out = NULL;
    /* Record what was passed on EVERY entry, including scripted refusals. The
     * result below comes from THIS call's classification; the sticky witnesses
     * are observation only and never decide a later call. */
    sender_shape_t shape = sender_record_cfg(cfg);
    if (sender_attach_result != MOQ_OK) {
        moq_result_t rc = sender_attach_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    if (shape != SENDER_SHAPE_OK) {     /* nothing beyond the prefix was read */
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    assert(ep == current);
    if (cfg->endpoint != NULL ||
        cfg->namespace_.count == 0 || cfg->namespace_.parts == NULL ||
        !sender_backpressure_known(cfg->backpressure)) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    for (size_t i = 0; i < cfg->namespace_.count; ++i) {
        if (cfg->namespace_.parts[i].len == 0 ||
            cfg->namespace_.parts[i].data == NULL) {
            pthread_mutex_unlock(&mutex);
            return MOQ_ERR_INVAL;
        }
    }
    /* moq_endpoint_attach_hook order: a stopped endpoint or an occupied slot
     * is WRONG_STATE; a TERMINAL endpoint is CLOSED. */
    if (ep->stopped || current_sender) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_WRONG_STATE;
    }
    if (ep->state == MOQ_ENDPOINT_CLOSED) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_CLOSED;
    }
    current_sender = calloc(1, sizeof(*current_sender));
    if (!current_sender) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_NOMEM;
    }
    current_sender->ep = ep;
    senders_attached++;
    sender_log_add("sender_attached");
    *out = current_sender;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

void moq_media_sender_destroy(moq_media_sender_t *s)
{
    pthread_mutex_lock(&mutex);
    sender_destroy_entries++;
    sender_log_add("sender_destroy");
    if (!s) { pthread_mutex_unlock(&mutex); return; }
    assert(s == current_sender);
    for (size_t i = 0; i < send_track_count; i++)
        if (send_track_owner[i] == s ||
            send_track_owner[i] == &send_track_stub_owner)
            send_track_owner_destroyed[i] = true;
    free(current_sender);
    current_sender = NULL;
    senders_destroyed++;
    pthread_mutex_unlock(&mutex);
}

bool moq_media_sender_is_ready(const moq_media_sender_t *s)
{
    if (!s) return false;
    pthread_mutex_lock(&mutex);
    bool r = s->ready && !s->fatal;
    pthread_mutex_unlock(&mutex);
    return r;
}

bool moq_media_sender_is_closed(const moq_media_sender_t *s)
{
    if (!s) return true;
    pthread_mutex_lock(&mutex);
    bool c = s->ep->stopped || s->ep->state == MOQ_ENDPOINT_CLOSED;
    pthread_mutex_unlock(&mutex);
    return c;
}

/* Mirrors the SDK: the sender is fatal when IT is fatal or its endpoint is. */
bool moq_media_sender_is_fatal(const moq_media_sender_t *s)
{
    if (!s) return false;
    pthread_mutex_lock(&mutex);
    bool f = s->fatal || s->ep->reason == MOQ_ENDPOINT_TERMINAL_PROTOCOL;
    pthread_mutex_unlock(&mutex);
    return f;
}

/* Mirrors the SDK: the endpoint's code is the fallback when the sender itself
 * is not fatal. */
uint64_t moq_media_sender_fatal_code(const moq_media_sender_t *s)
{
    if (!s) return 0;
    pthread_mutex_lock(&mutex);
    uint64_t c = s->fatal ? s->fatal_code : s->ep->detail;
    pthread_mutex_unlock(&mutex);
    return c;
}

/* -- send-track recorder --------------------------------------------------- *
 * An input/outcome recorder for moq_media_sender_add_track/remove_track. It is
 * NOT a second sender: it registers nothing, schedules nothing and builds no
 * catalog. Real C tests own catalog, policy and lifecycle semantics.
 *
 * All state is bounded. Anything that does not fit is reported through a named
 * oracle-limit witness rather than truncated silently, and the per-call image
 * is cleared before each call so no slot survives from an earlier one. */

static int tk_add_entries, tk_added, tk_remove_entries, tk_removed;
static moq_result_t tk_add_result, tk_remove_result;
static bool tk_pool_exhausted;

/* The captured image of the LAST add_track configuration. */
static text_t tcfg_name, tcfg_codec, tcfg_init_data, tcfg_role, tcfg_lang,
              tcfg_channel_config;
static uint32_t tcfg_struct_size;
static bool tcfg_full_size;
static int tcfg_media_type, tcfg_packaging;
static uint32_t tcfg_timescale, tcfg_width, tcfg_height, tcfg_samplerate;
static uint64_t tcfg_framerate_millis, tcfg_bitrate, tcfg_track_duration_ms;
static bool tcfg_is_live, tcfg_has_track_duration;
/* The fields this Python slice defers: observed, never exposed. */
static bool tcfg_has_max_grp_sap, tcfg_has_max_obj_sap, tcfg_emit_sap_timeline,
            tcfg_emit_media_timeline, tcfg_has_alt_group;
static size_t tcfg_cp_ref_count;
static bool tcfg_cp_ptr_null;
static bool tcfg_seen;
static bool tcfg_oracle_limit;
static const char *tcfg_oracle_limit_reason;

/* Copy a span into bounded storage, or report the limit by name. */
static void tcfg_take(text_t *dst, moq_bytes_t src, const char *what)
{
    dst->len = 0;
    if (!span_coherent(src)) {
        tcfg_oracle_limit = true;
        tcfg_oracle_limit_reason = what;
        return;
    }
    if (src.len > sizeof(dst->data)) {
        tcfg_oracle_limit = true;
        tcfg_oracle_limit_reason = what;
        return;
    }
    if (src.len) memcpy(dst->data, src.data, src.len);
    dst->len = src.len;
}

static void tcfg_clear(void)
{
    memset(&tcfg_name, 0, sizeof(tcfg_name));
    memset(&tcfg_codec, 0, sizeof(tcfg_codec));
    memset(&tcfg_init_data, 0, sizeof(tcfg_init_data));
    memset(&tcfg_role, 0, sizeof(tcfg_role));
    memset(&tcfg_lang, 0, sizeof(tcfg_lang));
    memset(&tcfg_channel_config, 0, sizeof(tcfg_channel_config));
    tcfg_struct_size = 0; tcfg_full_size = false;
    tcfg_media_type = tcfg_packaging = 0;
    tcfg_timescale = tcfg_width = tcfg_height = tcfg_samplerate = 0;
    tcfg_framerate_millis = tcfg_bitrate = tcfg_track_duration_ms = 0;
    tcfg_is_live = tcfg_has_track_duration = false;
    tcfg_has_max_grp_sap = tcfg_has_max_obj_sap = false;
    tcfg_emit_sap_timeline = tcfg_emit_media_timeline = tcfg_has_alt_group = false;
    tcfg_cp_ref_count = 0;
    tcfg_cp_ptr_null = false;
    tcfg_oracle_limit = false;
    tcfg_oracle_limit_reason = NULL;
}

/* Mirrors the SDK initializer: clear everything, stamp the size, is_live true.
 * A control compares this against the real SDK's own authority program. */
void moq_media_track_cfg_init(moq_media_track_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
    cfg->is_live = true;
}

/* Returns false when the advertised prefix is one this fixture cannot record,
 * which is a named fixture restriction reported as a refusal -- never a
 * silently accepted partial image. */
static bool tcfg_record(const moq_media_track_cfg_t *cfg)
{
    tcfg_seen = true;
    tcfg_clear();
    tcfg_struct_size = cfg->struct_size;
    tcfg_full_size = (cfg->struct_size == sizeof(*cfg));
    if (!tcfg_full_size) {
        tcfg_oracle_limit = true;
        tcfg_oracle_limit_reason = "fixture records the full current track config only";
        return false;                 /* nothing else is read */
    }
    tcfg_take(&tcfg_name, cfg->name, "name exceeds fixture capacity");
    tcfg_take(&tcfg_codec, cfg->codec, "codec exceeds fixture capacity");
    tcfg_take(&tcfg_init_data, cfg->init_data, "init_data exceeds fixture capacity");
    tcfg_take(&tcfg_role, cfg->role, "role exceeds fixture capacity");
    tcfg_take(&tcfg_lang, cfg->lang, "lang exceeds fixture capacity");
    tcfg_take(&tcfg_channel_config, cfg->channel_config,
              "channel_config exceeds fixture capacity");
    tcfg_media_type = (int)cfg->media_type;
    tcfg_packaging = (int)cfg->packaging;
    tcfg_timescale = cfg->timescale;
    tcfg_width = cfg->width;
    tcfg_height = cfg->height;
    tcfg_framerate_millis = cfg->framerate_millis;
    tcfg_samplerate = cfg->samplerate;
    tcfg_bitrate = cfg->bitrate;
    tcfg_is_live = cfg->is_live;
    tcfg_has_track_duration = cfg->has_track_duration;
    tcfg_track_duration_ms = cfg->track_duration_ms;
    tcfg_has_max_grp_sap = cfg->has_max_grp_sap;
    tcfg_has_max_obj_sap = cfg->has_max_obj_sap;
    tcfg_emit_sap_timeline = cfg->emit_sap_timeline;
    tcfg_emit_media_timeline = cfg->emit_media_timeline;
    tcfg_has_alt_group = cfg->has_alt_group;
    tcfg_cp_ref_count = cfg->content_protection_ref_id_count;
    tcfg_cp_ptr_null = (cfg->content_protection_ref_ids == NULL);
    return true;
}

moq_result_t moq_media_sender_add_track(moq_media_sender_t *s,
                                        const moq_media_track_cfg_t *cfg,
                                        moq_media_track_t **out)
{
    pthread_mutex_lock(&mutex);
    tk_add_entries++;
    sender_log_add("add_track");
    if (!s || !cfg || !out) { pthread_mutex_unlock(&mutex); return MOQ_ERR_INVAL; }
    *out = NULL;
    if (cfg->struct_size < sizeof(uint32_t)) {
        pthread_mutex_unlock(&mutex); return MOQ_ERR_INVAL;
    }
    if (!tcfg_record(cfg)) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;         /* the image is refused, and witnessed */
    }
    if (tk_add_result != MOQ_OK) {
        moq_result_t scripted = tk_add_result;
        pthread_mutex_unlock(&mutex);
        return scripted;            /* the call and its config are still witnessed */
    }
    if (send_track_count >= FIXTURE_SEND_TRACKS) {
        tk_pool_exhausted = true;
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_NOMEM;
    }
    size_t i = send_track_count++;
    memset(&send_track_pool[i], 0, sizeof(send_track_pool[i]));
    /* The declared name per index, so a test can name a track without
     * reaching into a capsule. */
    send_track_name_store[i].len = 0;
    if (cfg->name.data && cfg->name.len <= sizeof(send_track_name_store[i].data)) {
        memcpy(send_track_name_store[i].data, cfg->name.data, cfg->name.len);
        send_track_name_store[i].len = cfg->name.len;
    }
    send_track_owner[i] = s;
    send_track_removed_flag[i] = false;
    send_track_owner_destroyed[i] = false;
    tk_added++;
    *out = &send_track_pool[i];
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

moq_result_t moq_media_sender_remove_track(moq_media_sender_t *s,
                                           moq_media_track_t *track)
{
    pthread_mutex_lock(&mutex);
    tk_remove_entries++;
    sender_log_add("remove_track");
    moq_result_t rc = MOQ_ERR_INVAL;
    if (tk_remove_result != MOQ_OK) {
        rc = tk_remove_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    for (size_t i = 0; i < send_track_count; i++) {
        if (&send_track_pool[i] != track) continue;
        if (send_track_owner[i] != s) { rc = MOQ_ERR_INVAL; break; }   /* foreign */
        if (send_track_removed_flag[i]) { rc = MOQ_ERR_WRONG_STATE; break; }
        send_track_removed_flag[i] = true;
        tk_removed++;
        rc = MOQ_OK;
        break;
    }
    pthread_mutex_unlock(&mutex);
    return rc;
}

/* -- media write recorder -------------------------------------------------- *
 * A bridge witness, not a second scheduler: nothing is queued, no policy is
 * applied and no catalog is touched. It records the exact object image the
 * bridge passed, the payload and properties bytes and their refcounts at
 * ENTRY, and it honours transfer-on-success: on MOQ_OK it takes the caller's
 * references (and releases them at reset, as the real owner eventually would);
 * on any non-OK it takes nothing. */

static int wr_entries, wr_accepted;
static moq_result_t wr_result;
static bool wr_seen;
static bool wr_track_null, wr_payload_null, wr_properties_null;
/* WHICH sender and track were passed, not merely whether they were NULL: a
 * bridge choosing the wrong valid handle must not satisfy the mapping rows. */
static Py_ssize_t wr_track_index, wr_sender_index;
static uint32_t wr_struct_size;
static bool wr_full_size;
static bool wr_is_sync, wr_starts_group, wr_ends_group;
static bool wr_has_capture_time, wr_has_sap_type;
static int wr_sap_type;
static uint64_t wr_decode_time_us, wr_presentation_time_us, wr_capture_time_us;
static text_t wr_payload, wr_properties;
static size_t wr_payload_len, wr_properties_len;   /* DECLARED lengths */
static uint32_t wr_payload_refs, wr_properties_refs;   /* at entry */
static bool wr_oracle_limit;
static const char *wr_oracle_limit_reason;
/* References this recorder accepted on MOQ_OK and still owns. */
static moq_rcbuf_t *wr_owned_payload, *wr_owned_properties;

/* A counting allocator at the REAL rcbuf allocation and free boundaries. It is
 * the observation the transfer rules need, because "no transfer" is not proof
 * of release.
 *
 * Two things are recorded. Per-SPAN counters, so the payload and the
 * properties buffer are accounted separately rather than as one aggregate:
 * each span gets its own allocator instance whose ctx names it, and an rcbuf
 * carries its allocator (ctx included) to its own free. And a bounded table of
 * LIVE blocks, so a saved pointer can be asked whether it is still allocated
 * WITHOUT dereferencing it: a bridge that released a transferred buffer early
 * is then a named failure here instead of a use-after-free in the observer.
 *
 * The rcbuf header is the allocated block, so block identity is buffer
 * identity. Table capacity is a harness bound: exceeding it is reported, never
 * silently rounded off. */
#define RC_SPAN_PAYLOAD    0
#define RC_SPAN_PROPERTIES 1
#define RC_SPANS           2
#define RC_LIVE_MAX        64

static const char *const rc_span_name[RC_SPANS] = { "payload", "properties" };
static int rc_span_tag[RC_SPANS] = { RC_SPAN_PAYLOAD, RC_SPAN_PROPERTIES };

static size_t rc_alloc_calls[RC_SPANS], rc_free_calls[RC_SPANS];
static size_t rc_bytes_outstanding[RC_SPANS];
static struct { void *p; size_t size; int span; } rc_live[RC_LIVE_MAX];
static size_t rc_live_n;
static bool rc_live_overflow;      /* harness bound reached, never semantics */
static bool rc_free_unknown;       /* a free of a block this table never saw */

static int rc_span_of(void *ctx)
{
    int span = ctx ? *(const int *)ctx : RC_SPAN_PAYLOAD;
    return (span >= 0 && span < RC_SPANS) ? span : RC_SPAN_PAYLOAD;
}

static size_t rc_live_find(const void *p)
{
    for (size_t i = 0; i < rc_live_n; i++)
        if (rc_live[i].p == p) return i;
    return (size_t)-1;
}

static bool rc_is_live(const void *p)
{
    return p && rc_live_find(p) != (size_t)-1;
}

static void rc_live_add(void *p, size_t size, int span)
{
    if (rc_live_n >= RC_LIVE_MAX) { rc_live_overflow = true; return; }
    rc_live[rc_live_n].p = p;
    rc_live[rc_live_n].size = size;
    rc_live[rc_live_n].span = span;
    rc_live_n++;
}

static void rc_live_drop(size_t i)
{
    rc_live[i] = rc_live[rc_live_n - 1];
    rc_live_n--;
}

static void *counting_alloc(size_t size, void *ctx)
{
    int span = rc_span_of(ctx);
    void *p = malloc(size);
    if (!p) return NULL;
    rc_alloc_calls[span]++;
    rc_bytes_outstanding[span] += size;
    rc_live_add(p, size, span);
    return p;
}

static void *counting_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    int span = rc_span_of(ctx);
    /* Resolve the table slot BEFORE the block moves: the old pointer must not
     * be read, even for comparison, once realloc has returned. */
    size_t i = rc_live_find(ptr);
    void *p = realloc(ptr, new_size);
    if (!p) return NULL;
    rc_bytes_outstanding[span] -= old_size;
    rc_bytes_outstanding[span] += new_size;
    if (i != (size_t)-1) { rc_live[i].p = p; rc_live[i].size = new_size; }
    else if (!rc_live_overflow) rc_free_unknown = true;
    return p;
}

static void counting_free(void *ptr, size_t size, void *ctx)
{
    if (ptr) {
        int span = rc_span_of(ctx);
        rc_free_calls[span]++;
        rc_bytes_outstanding[span] -= size;
        size_t i = rc_live_find(ptr);
        if (i != (size_t)-1) rc_live_drop(i);
        else if (!rc_live_overflow) rc_free_unknown = true;
    }
    free(ptr);
}

static const moq_alloc_t counting_allocator[RC_SPANS] = {
    { .ctx = &rc_span_tag[RC_SPAN_PAYLOAD], .alloc = counting_alloc,
      .realloc = counting_realloc, .free = counting_free },
    { .ctx = &rc_span_tag[RC_SPAN_PROPERTIES], .alloc = counting_alloc,
      .realloc = counting_realloc, .free = counting_free },
};

const moq_alloc_t *moq5_test_counting_alloc(void)
{
    return &counting_allocator[RC_SPAN_PAYLOAD];
}

const moq_alloc_t *moq5_test_counting_alloc_for(int span)
{
    return &counting_allocator[(span == RC_SPAN_PROPERTIES) ? RC_SPAN_PROPERTIES
                                                            : RC_SPAN_PAYLOAD];
}

/* An inventory recorded AT a named fault's firing, on the thread that fired
 * it, so a row can assert what ownership looked like at that instant instead
 * of after the stack has unwound. No lock: this runs synchronously inside the
 * call that consumed the fault. */
static bool fault_snap_valid;
static char fault_snap_site[64];
static size_t fault_snap_alloc[RC_SPANS], fault_snap_free[RC_SPANS];

void moq5_test_note_fault_site(const char *name)
{
    if (!name) return;
    snprintf(fault_snap_site, sizeof(fault_snap_site), "%s", name);
    for (int i = 0; i < RC_SPANS; i++) {
        fault_snap_alloc[i] = rc_alloc_calls[i];
        fault_snap_free[i] = rc_free_calls[i];
    }
    fault_snap_valid = true;
}

/* A REAL barrier for the GIL boundary: the fake parks inside the native call
 * until another thread releases it. No sleeps, and a bounded deadline so a
 * mistake can never hang the suite. */
static pthread_cond_t wr_gate = PTHREAD_COND_INITIALIZER;
static bool wr_block_armed, wr_in_call, wr_released;
static bool wr_gate_timed_out;
/* A clock or condvar failure on the PARKED side. Like the waiter's own bound
 * it is a named harness failure, never read as ordinary absence. */
static bool wr_gate_fault;

static void wr_park(void)
{
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        wr_gate_fault = true;      /* the bound is unknowable: harness failure */
        return;
    }
    deadline.tv_sec += 10;                    /* harness bound, never a policy */
    wr_in_call = true;
    pthread_cond_broadcast(&wr_gate);
    while (!wr_released) {
        int rc = pthread_cond_timedwait(&wr_gate, &mutex, &deadline);
        if (rc == ETIMEDOUT) { wr_gate_timed_out = true; break; }
        if (rc != 0) { wr_gate_fault = true; break; }
    }
    wr_in_call = false;
    pthread_cond_broadcast(&wr_gate);
}

static void wr_take(text_t *dst, size_t *declared, const moq_rcbuf_t *buf,
                    const char *what)
{
    dst->len = 0;
    *declared = 0;
    if (!buf) return;
    size_t n = moq_rcbuf_len(buf);
    *declared = n;
    if (n > sizeof(dst->data)) {
        wr_oracle_limit = true;
        wr_oracle_limit_reason = what;
        return;
    }
    if (n) memcpy(dst->data, moq_rcbuf_data(buf), n);
    dst->len = n;
}

/* References deliberately NOT released by a fault driver, kept here so the
 * test that arranged the omission can observe the intended outstanding state
 * and then release them. Dropping them on the floor would be a real leak. */
#define WR_STRANDED_MAX 4
static moq_rcbuf_t *wr_stranded[WR_STRANDED_MAX];
static size_t wr_stranded_n;
static bool wr_stranded_overflow;

static void wr_strand(moq_rcbuf_t *b)
{
    if (!b) return;
    if (wr_stranded_n >= WR_STRANDED_MAX) { wr_stranded_overflow = true; return; }
    wr_stranded[wr_stranded_n++] = b;
}

static size_t wr_release_stranded(void)
{
    size_t n = 0;
    while (wr_stranded_n) {
        moq_rcbuf_t *b = wr_stranded[--wr_stranded_n];
        wr_stranded[wr_stranded_n] = NULL;
        if (rc_is_live(b)) { moq_rcbuf_decref(b); n++; }
    }
    return n;
}

/* What the LAST settlement did, so a premature release can be reported by
 * name instead of being discovered as a crash. */
static bool wr_premature_release;
static unsigned wr_released_owned_count;

static void wr_release_owned(void)
{
    /* Ask the allocation table whether the saved block is still live BEFORE
     * touching it. A bridge that released a transferred reference early is a
     * named observation here, not a use-after-free. */
    if (wr_owned_payload) {
        if (rc_is_live(wr_owned_payload)) {
            moq_rcbuf_decref(wr_owned_payload);
            wr_released_owned_count++;
        } else {
            wr_premature_release = true;
        }
        wr_owned_payload = NULL;
    }
    if (wr_owned_properties) {
        if (rc_is_live(wr_owned_properties)) {
            moq_rcbuf_decref(wr_owned_properties);
            wr_released_owned_count++;
        } else {
            wr_premature_release = true;
        }
        wr_owned_properties = NULL;
    }
}

moq_result_t moq_media_sender_write(moq_media_sender_t *s,
                                    moq_media_track_t *track,
                                    const moq_media_send_object_t *obj)
{
    pthread_mutex_lock(&mutex);
    wr_entries++;
    sender_log_add("write");
    wr_seen = true;
    wr_track_null = (track == NULL);
    wr_track_index = -1;
    for (size_t i = 0; i < send_track_count; i++)
        if (&send_track_pool[i] == track) { wr_track_index = (Py_ssize_t)i; break; }
    wr_sender_index = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    wr_oracle_limit = false;
    wr_oracle_limit_reason = NULL;
    if (!s || !obj) { pthread_mutex_unlock(&mutex); return MOQ_ERR_INVAL; }
    wr_struct_size = obj->struct_size;
    wr_full_size = (obj->struct_size == sizeof(*obj));
    if (!wr_full_size) {
        wr_oracle_limit = true;
        wr_oracle_limit_reason = "fixture records the full current send object only";
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    wr_payload_null = (obj->payload == NULL);
    wr_properties_null = (obj->properties == NULL);
    wr_payload_refs = obj->payload ? moq_rcbuf_refcount(obj->payload) : 0;
    wr_properties_refs = obj->properties ? moq_rcbuf_refcount(obj->properties) : 0;
    wr_take(&wr_payload, &wr_payload_len, obj->payload, "payload exceeds fixture capacity");
    wr_take(&wr_properties, &wr_properties_len, obj->properties,
            "properties exceed fixture capacity");
    wr_is_sync = obj->is_sync;
    wr_starts_group = obj->starts_group;
    wr_ends_group = obj->ends_group;
    wr_decode_time_us = obj->decode_time_us;
    wr_presentation_time_us = obj->presentation_time_us;
    wr_has_capture_time = obj->has_capture_time;
    wr_capture_time_us = obj->capture_time_us;
    wr_has_sap_type = obj->has_sap_type;
    wr_sap_type = (int)obj->sap_type;

    if (wr_block_armed) wr_park();

    if (wr_result != MOQ_OK) {
        moq_result_t scripted = wr_result;
        pthread_mutex_unlock(&mutex);
        return scripted;              /* no transfer: the caller keeps its refs */
    }
    /* Accepted: the references become ours (transfer-on-success). */
    wr_release_owned();
    wr_owned_payload = obj->payload;
    wr_owned_properties = obj->properties;
    wr_accepted++;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

/* -- sender observation seams --------------------------------------------- */

/* What THIS build's headers and mirrored initializer produce, so a control can
 * compare them against the real SDK's own authority program. */
/* What THIS build's headers and mirrored track initializer produce, so the
 * control can compare them against the real SDK's authority program. */
/* Drive the recorder's own add_track with a declared name and struct_size, so
 * its entry safety and bounds can be exercised without the Python layer. */
PyObject *test_write_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    wr_premature_release = false;
    wr_released_owned_count = 0;
    wr_release_owned();          /* the eventual owner releases the fixture */
    wr_release_stranded();       /* and nothing a fault driver held is dropped */
    wr_entries = wr_accepted = 0;
    wr_result = MOQ_OK;
    wr_seen = false;
    wr_track_null = wr_payload_null = wr_properties_null = false;
    wr_struct_size = 0; wr_full_size = false;
    wr_is_sync = wr_starts_group = wr_ends_group = false;
    wr_has_capture_time = wr_has_sap_type = false;
    wr_sap_type = 0;
    wr_decode_time_us = wr_presentation_time_us = wr_capture_time_us = 0;
    memset(&wr_payload, 0, sizeof(wr_payload));
    memset(&wr_properties, 0, sizeof(wr_properties));
    wr_payload_len = wr_properties_len = 0;
    wr_payload_refs = wr_properties_refs = 0;
    wr_oracle_limit = false; wr_oracle_limit_reason = NULL;
    wr_block_armed = wr_in_call = wr_gate_timed_out = wr_gate_fault = false;
    wr_released = true;
    pthread_cond_broadcast(&wr_gate);
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* The scripted result currently in force, so a scoped stand-in can follow the
 * same result-to-fault-site table the row declares. */
PyObject *test_write_result_current(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    long r = (long)wr_result;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong(r);
}

PyObject *test_write_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:O,s:O}",
        "entries", wr_entries,
        "accepted", wr_accepted,
        "holds_references",
        (wr_owned_payload || wr_owned_properties) ? Py_True : Py_False,
        "gate_fault", wr_gate_fault ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_write_object(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    if (!wr_seen) { pthread_mutex_unlock(&mutex); Py_RETURN_NONE; }
    PyObject *r = Py_BuildValue(
        "{s:y#,s:n,s:y#,s:n,s:O,s:O,s:O,s:n,s:n,s:O,s:O,s:O,s:K,s:K,s:O,s:K,"
        "s:O,s:i,s:I,s:I,s:I,s:O,s:O,s:z}",
        "payload", (const char *)wr_payload.data, (Py_ssize_t)wr_payload.len,
        "payload_declared_length", (Py_ssize_t)wr_payload_len,
        "properties", (const char *)wr_properties.data,
        (Py_ssize_t)wr_properties.len,
        "properties_declared_length", (Py_ssize_t)wr_properties_len,
        "payload_null", wr_payload_null ? Py_True : Py_False,
        "properties_null", wr_properties_null ? Py_True : Py_False,
        "track_null", wr_track_null ? Py_True : Py_False,
        "track_index", wr_track_index,
        "sender_index", wr_sender_index,
        "is_sync", wr_is_sync ? Py_True : Py_False,
        "starts_group", wr_starts_group ? Py_True : Py_False,
        "ends_group", wr_ends_group ? Py_True : Py_False,
        "decode_time_us", (unsigned long long)wr_decode_time_us,
        "presentation_time_us", (unsigned long long)wr_presentation_time_us,
        "has_capture_time", wr_has_capture_time ? Py_True : Py_False,
        "capture_time_us", (unsigned long long)wr_capture_time_us,
        "has_sap_type", wr_has_sap_type ? Py_True : Py_False,
        "sap_type", wr_sap_type,
        "struct_size", (unsigned int)wr_struct_size,
        "payload_refs_at_entry", (unsigned int)wr_payload_refs,
        "properties_refs_at_entry", (unsigned int)wr_properties_refs,
        "full_size", wr_full_size ? Py_True : Py_False,
        "oracle_limit", wr_oracle_limit ? Py_True : Py_False,
        "oracle_limit_reason", wr_oracle_limit_reason);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Arm or disarm the in-call barrier. */
/* Drive the recorder's own write with declared spans, so the recorder, its
 * transfer rule and the barrier can all be exercised without the bridge. */
/* The real allocation boundary inventory. `outstanding` is what has been
 * acquired and not yet freed: a leaked refused buffer shows up HERE, which is
 * the observation "no transfer" alone cannot give. */
PyObject *test_rcbuf_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    size_t alloc_total = 0, free_total = 0, bytes_total = 0;
    for (int i = 0; i < RC_SPANS; i++) {
        alloc_total += rc_alloc_calls[i];
        free_total += rc_free_calls[i];
        bytes_total += rc_bytes_outstanding[i];
    }
    PyObject *r = Py_BuildValue(
        "{s:n,s:n,s:n,s:n,s:{s:n,s:n,s:n,s:n},s:{s:n,s:n,s:n,s:n},s:O,s:O}",
        "allocations", (Py_ssize_t)alloc_total,
        "frees", (Py_ssize_t)free_total,
        "outstanding", (Py_ssize_t)(alloc_total - free_total),
        "bytes_outstanding", (Py_ssize_t)bytes_total,
        rc_span_name[RC_SPAN_PAYLOAD],
        "allocations", (Py_ssize_t)rc_alloc_calls[RC_SPAN_PAYLOAD],
        "frees", (Py_ssize_t)rc_free_calls[RC_SPAN_PAYLOAD],
        "outstanding", (Py_ssize_t)(rc_alloc_calls[RC_SPAN_PAYLOAD]
                                    - rc_free_calls[RC_SPAN_PAYLOAD]),
        "bytes_outstanding", (Py_ssize_t)rc_bytes_outstanding[RC_SPAN_PAYLOAD],
        rc_span_name[RC_SPAN_PROPERTIES],
        "allocations", (Py_ssize_t)rc_alloc_calls[RC_SPAN_PROPERTIES],
        "frees", (Py_ssize_t)rc_free_calls[RC_SPAN_PROPERTIES],
        "outstanding", (Py_ssize_t)(rc_alloc_calls[RC_SPAN_PROPERTIES]
                                    - rc_free_calls[RC_SPAN_PROPERTIES]),
        "bytes_outstanding",
        (Py_ssize_t)rc_bytes_outstanding[RC_SPAN_PROPERTIES],
        "table_overflow", rc_live_overflow ? Py_True : Py_False,
        "unknown_frees", rc_free_unknown ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_write_simulate(PyObject *self, PyObject *args)
{
    (void)self;
    const char *payload = NULL, *properties = NULL;
    Py_ssize_t payload_len = 0, properties_len = 0;
    int is_sync = 0, omit_payload_cleanup = 0, omit_properties_cleanup = 0;
    if (!PyArg_ParseTuple(args, "y#z#i|ii", &payload, &payload_len,
                          &properties, &properties_len, &is_sync,
                          &omit_payload_cleanup, &omit_properties_cleanup))
        return NULL;
    moq_rcbuf_t *pay = NULL, *props = NULL;
    if (moq_rcbuf_create(moq5_test_counting_alloc_for(RC_SPAN_PAYLOAD),
                         (const uint8_t *)payload, (size_t)payload_len,
                         &pay) != MOQ_OK)
        return PyErr_NoMemory();
    if (properties &&
        moq_rcbuf_create(moq5_test_counting_alloc_for(RC_SPAN_PROPERTIES),
                         (const uint8_t *)properties,
                         (size_t)properties_len, &props) != MOQ_OK) {
        moq_rcbuf_decref(pay);
        return PyErr_NoMemory();
    }
    moq_media_send_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.struct_size = (uint32_t)sizeof(obj);
    obj.payload = pay;
    obj.properties = props;
    obj.is_sync = is_sync ? true : false;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_write(current_sender_or_stub(), NULL, &obj);
    Py_END_ALLOW_THREADS
    if (rc != MOQ_OK) {                 /* no transfer: this caller still owns */
        /* A deliberately omitted cleanup is RETAINED, not dropped: the test
         * that asked for the omission observes it and then releases it. */
        pthread_mutex_lock(&mutex);
        if (omit_payload_cleanup) wr_strand(pay); else moq_rcbuf_decref(pay);
        if (props) {
            if (omit_properties_cleanup) wr_strand(props);
            else moq_rcbuf_decref(props);
        }
        pthread_mutex_unlock(&mutex);
    }
    return PyLong_FromLong((long)rc);
}

/* Drive the PROPOSED write ordering against the recorder, so the ordering
 * observer can be proved before the bridge exists. This is a fixture driver
 * over the fake, not a Python write surface: it takes no moq5 object, returns
 * no outcome value, and the product rows stay RED.
 *
 * `release_before_result` selects the ordering under test. True is the
 * proposed order -- release both spans, THEN allocate the result. False is the
 * mutant, and the fault-site inventory tells them apart at the fault itself. */
PyObject *test_write_simulate_ordered(PyObject *self, PyObject *args)
{
    (void)self;
    const char *payload = NULL, *properties = NULL, *site = "write_result";
    Py_ssize_t payload_len = 0, properties_len = 0, site_len = 0;
    int release_before_result = 1;
    if (!PyArg_ParseTuple(args, "y#z#|is#", &payload, &payload_len,
                          &properties, &properties_len, &release_before_result,
                          &site, &site_len))
        return NULL;

    moq_rcbuf_t *pay = NULL, *props = NULL;
    int payload_fault = 0, properties_fault = 0, result_fault = 0;

    if (moq5_test_site_fails("send_object_payload")) {
        PyErr_Clear();     /* the driver reports the fault, it does not raise */
        payload_fault = 1;
        goto done;                       /* nothing acquired, no native call */
    }
    if (moq_rcbuf_create(moq5_test_counting_alloc_for(RC_SPAN_PAYLOAD),
                         (const uint8_t *)payload, (size_t)payload_len,
                         &pay) != MOQ_OK)
        return PyErr_NoMemory();
    if (moq5_test_site_fails("send_object_properties")) {
        PyErr_Clear();
        properties_fault = 1;
        moq_rcbuf_decref(pay);           /* the earlier span is released */
        pay = NULL;
        goto done;
    }
    if (properties &&
        moq_rcbuf_create(moq5_test_counting_alloc_for(RC_SPAN_PROPERTIES),
                         (const uint8_t *)properties,
                         (size_t)properties_len, &props) != MOQ_OK) {
        moq_rcbuf_decref(pay);
        return PyErr_NoMemory();
    }

    moq_media_send_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.struct_size = (uint32_t)sizeof(obj);
    obj.payload = pay;
    obj.properties = props;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_write(current_sender_or_stub(), NULL, &obj);
    Py_END_ALLOW_THREADS
    if (rc == MOQ_OK) {                  /* transferred: release nothing */
        return Py_BuildValue("{s:i,s:i,s:i,s:i}", "rc", (int)rc,
                             "payload_fault", 0, "properties_fault", 0,
                             "result_fault", 0);
    }
    if (release_before_result) {
        if (pay) moq_rcbuf_decref(pay);
        if (props) moq_rcbuf_decref(props);
        pay = props = NULL;
        result_fault = moq5_test_site_fails(site);
        if (result_fault) PyErr_Clear();
    } else {
        result_fault = moq5_test_site_fails(site);
        if (result_fault) PyErr_Clear();
        if (pay) moq_rcbuf_decref(pay);
        if (props) moq_rcbuf_decref(props);
        pay = props = NULL;
    }
    return Py_BuildValue("{s:i,s:i,s:i,s:i}", "rc", (int)rc,
                         "payload_fault", 0, "properties_fault", 0,
                         "result_fault", result_fault);
done:
    return Py_BuildValue("{s:i,s:i,s:i,s:i}", "rc", 0,
                         "payload_fault", payload_fault,
                         "properties_fault", properties_fault,
                         "result_fault", result_fault);
}

/* What the fixture is deliberately holding after an omitted cleanup. */
PyObject *test_write_stranded(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:n,s:O}",
        "held", (Py_ssize_t)wr_stranded_n,
        "overflow", wr_stranded_overflow ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_write_release_stranded(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    size_t n = wr_release_stranded();
    pthread_mutex_unlock(&mutex);
    return PyLong_FromSsize_t((Py_ssize_t)n);
}

/* What the LAST reset's settlement did, including whether it found a
 * transferred reference already released. */
PyObject *test_write_settlement(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:I,s:O}",
        "released", wr_released_owned_count,
        "premature_release", wr_premature_release ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Perturbation: release what the recorder accepted, as a bridge that wrongly
 * released a transferred reference would. It exists to PROVE the liveness
 * detector reports that by name instead of crashing on it. */
PyObject *test_write_force_release_owned(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    unsigned n = 0;
    if (wr_owned_payload && rc_is_live(wr_owned_payload)) {
        moq_rcbuf_decref(wr_owned_payload); n++;
    }
    if (wr_owned_properties && rc_is_live(wr_owned_properties)) {
        moq_rcbuf_decref(wr_owned_properties); n++;
    }
    pthread_mutex_unlock(&mutex);
    return PyLong_FromUnsignedLong(n);
}

/* The ownership inventory as it stood when the named fault fired. */
PyObject *test_fault_site_inventory(PyObject *self, PyObject *value)
{
    (void)self;
    Py_ssize_t name_len;
    const char *name = PyUnicode_AsUTF8AndSize(value, &name_len);
    if (!name) return NULL;
    pthread_mutex_lock(&mutex);
    if (!fault_snap_valid || strcmp(fault_snap_site, name) != 0) {
        pthread_mutex_unlock(&mutex);
        Py_RETURN_NONE;
    }
    PyObject *r = Py_BuildValue("{s:{s:n,s:n,s:n},s:{s:n,s:n,s:n},s:n}",
        rc_span_name[RC_SPAN_PAYLOAD],
        "allocations", (Py_ssize_t)fault_snap_alloc[RC_SPAN_PAYLOAD],
        "frees", (Py_ssize_t)fault_snap_free[RC_SPAN_PAYLOAD],
        "outstanding", (Py_ssize_t)(fault_snap_alloc[RC_SPAN_PAYLOAD]
                                    - fault_snap_free[RC_SPAN_PAYLOAD]),
        rc_span_name[RC_SPAN_PROPERTIES],
        "allocations", (Py_ssize_t)fault_snap_alloc[RC_SPAN_PROPERTIES],
        "frees", (Py_ssize_t)fault_snap_free[RC_SPAN_PROPERTIES],
        "outstanding", (Py_ssize_t)(fault_snap_alloc[RC_SPAN_PROPERTIES]
                                    - fault_snap_free[RC_SPAN_PROPERTIES]),
        "outstanding",
        (Py_ssize_t)((fault_snap_alloc[RC_SPAN_PAYLOAD]
                      + fault_snap_alloc[RC_SPAN_PROPERTIES])
                     - (fault_snap_free[RC_SPAN_PAYLOAD]
                        + fault_snap_free[RC_SPAN_PROPERTIES])));
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_write_block(PyObject *self, PyObject *value)
{
    (void)self;
    int armed = PyObject_IsTrue(value);
    if (armed < 0) return NULL;
    pthread_mutex_lock(&mutex);
    wr_block_armed = (armed != 0);
    if (!wr_block_armed) {
        wr_released = true;
        pthread_cond_broadcast(&wr_gate);
    } else {
        wr_released = false;
        wr_gate_timed_out = false;
    }
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Block until the native call is actually in flight. Releases the GIL while
 * waiting, so this IS the handshake rather than a poll. */
PyObject *test_write_wait_entered(PyObject *self, PyObject *args)
{
    (void)self;
    double timeout = 5.0;
    if (!PyArg_ParseTuple(args, "|d", &timeout)) return NULL;
    if (!(timeout >= 0.0) || timeout > 3600.0) {
        PyErr_SetString(PyExc_ValueError,
                        "the bounded test timeout must be 0..3600 seconds");
        return NULL;
    }
    bool entered = false;
    int failure = 0;                     /* a clock or condvar fault, not absence */
    Py_BEGIN_ALLOW_THREADS
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        failure = errno ? errno : EINVAL;
    } else {
        /* keep the FRACTION: 0.2 seconds is 200ms, not zero */
        long whole = (long)timeout;
        long nanos = (long)((timeout - (double)whole) * 1e9);
        deadline.tv_sec += (time_t)whole;
        deadline.tv_nsec += nanos;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec += 1;
            deadline.tv_nsec -= 1000000000L;
        }
        pthread_mutex_lock(&mutex);
        while (!wr_in_call) {
            int rc = pthread_cond_timedwait(&wr_gate, &mutex, &deadline);
            if (rc == ETIMEDOUT) break;          /* ordinary absence */
            if (rc != 0) { failure = rc; break; }  /* anything else is a fault */
        }
        entered = wr_in_call;
        pthread_mutex_unlock(&mutex);
    }
    Py_END_ALLOW_THREADS
    if (failure) {
        PyErr_Format(PyExc_RuntimeError,
                     "the test barrier failed (errno %d), not an absent call",
                     failure);
        return NULL;
    }
    return PyBool_FromLong(entered);
}

PyObject *test_write_release(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    wr_released = true;
    pthread_cond_broadcast(&wr_gate);
    bool timed_out = wr_gate_timed_out;
    pthread_mutex_unlock(&mutex);
    return PyBool_FromLong(!timed_out);      /* false = the harness bound hit */
}

PyObject *test_write_result(PyObject *self, PyObject *args)
{
    (void)self;
    int code = 0;
    if (!PyArg_ParseTuple(args, "i", &code)) return NULL;
    pthread_mutex_lock(&mutex);
    wr_result = (moq_result_t)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Refcounts of the references this recorder accepted, so a test can prove the
 * bridge did NOT release what it transferred. */
PyObject *test_write_owned_refs(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    bool pay_live = rc_is_live(wr_owned_payload);
    bool props_live = rc_is_live(wr_owned_properties);
    PyObject *r = Py_BuildValue("{s:I,s:I,s:O,s:O,s:O,s:O}",
        "payload", pay_live ? moq_rcbuf_refcount(wr_owned_payload) : 0u,
        "properties", props_live ? moq_rcbuf_refcount(wr_owned_properties) : 0u,
        "payload_held", wr_owned_payload ? Py_True : Py_False,
        "properties_held", wr_owned_properties ? Py_True : Py_False,
        "payload_live", pay_live ? Py_True : Py_False,
        "properties_live", props_live ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* -- sender wait mirror ---------------------------------------------------- *
 * Follows the SOURCE's priority exactly (media_sender.c:4843-4867): the
 * interrupt latch, then terminal, then the write LEVEL, each checked before
 * and after the endpoint wait, whose INTERRUPTED and DONE are returned as is;
 * a negative endpoint result propagates and a non-negative one becomes OK --
 * so an endpoint WAKE can return OK with the level still not holding.
 *
 * The level itself is a fixture knob: no queue engine, no drain, no policy.
 * It records every call's timeout and owner so the public slicer can be
 * observed without waiting. */

/* An OPT-IN hold: the qualified call parks INSIDE itself, on its own gate,
 * until the declared action releases it or a bounded harness watchdog expires.
 * That turns "the worker acted at some point" into "the worker acted inside
 * THIS call", with no 250 ms scheduling race and no change to any production
 * wait or to the fixture's ordinary timeout behaviour: nothing holds unless a
 * test arms it. */
static pthread_cond_t sw_gate = PTHREAD_COND_INITIALIZER;
static bool sw_hold_armed;        /* the opt-in */
static int sw_held_call;          /* which call is parked, -1 when none */
static bool sw_hold_released;     /* the declared action has released it */
static bool sw_hold_timed_out;    /* the harness bound expired: a FAILURE */
static int sw_action_call = -1;   /* the call that was parked when acted upon */

static int sw_resets;             /* wait-fixture reset ENTRIES, observable */
static int sw_calls;
static unsigned long long sw_last_timeout, sw_total_timeout;
static Py_ssize_t sw_last_sender = -1;
static bool sw_level;
static int sw_level_after;        /* the level starts holding at this call */

/* One bounded snapshot of every state this decision reads, taken under the
 * fixture's own mutex. The interrupt latch and the endpoint state are written
 * by other threads under that mutex, and this driver releases the GIL, so
 * reading them unlocked would be a genuine data race -- not something a
 * sanitizer run could be said to have ruled out. The mutex is ALWAYS released
 * before moq_endpoint_wait, which takes it itself. */
typedef struct {
    bool interrupted;
    bool terminal;      /* sender fatal OR endpoint closed */
    bool level;
} sender_wait_state_t;

static sender_wait_state_t sender_wait_snapshot_locked(const moq_media_sender_t *s,
                                                       int call)
{
    sender_wait_state_t snap = { false, false, false };
    if (!s) return snap;
    snap.interrupted = s->ep && s->ep->interrupted;
    snap.terminal = s->fatal || (s->ep && s->ep->state == MOQ_ENDPOINT_CLOSED);
    snap.level = sw_level || (sw_level_after > 0 && call >= sw_level_after);
    return snap;
}

moq_result_t moq_media_sender_wait(moq_media_sender_t *s, uint64_t timeout_us)
{
    pthread_mutex_lock(&mutex);
    int n = ++sw_calls;
    sw_last_timeout = (unsigned long long)timeout_us;
    sw_total_timeout += (unsigned long long)timeout_us;
    sw_last_sender = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    sender_log_add("wait");
    if (sw_hold_armed) {
        /* Park INSIDE this call until the action releases it. */
        struct timespec deadline;
        if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
            sw_hold_timed_out = true;
        } else {
            deadline.tv_sec += 10;          /* harness bound, never a policy */
            sw_held_call = n;
            sw_hold_released = false;
            pthread_cond_broadcast(&sw_gate);
            while (!sw_hold_released) {
                int rc_wait = pthread_cond_timedwait(&sw_gate, &mutex, &deadline);
                if (rc_wait != 0) { sw_hold_timed_out = true; break; }
            }
            sw_held_call = -1;
            pthread_cond_broadcast(&sw_gate);
        }
    }
    /* The decision snapshot is taken AFTER the hold, so an action performed
     * inside this call is the one this call sees. */
    sender_wait_state_t before = sender_wait_snapshot_locked(s, n);
    pthread_mutex_unlock(&mutex);

    if (!s) return MOQ_ERR_INVAL;
    if (before.interrupted) return MOQ_ERR_INTERRUPTED;
    if (before.terminal) return MOQ_ERR_CLOSED;
    if (before.level) return MOQ_OK;

    moq_result_t rc = moq_endpoint_wait(s->ep, timeout_us);
    if (rc == MOQ_ERR_INTERRUPTED || rc == MOQ_DONE) return rc;

    pthread_mutex_lock(&mutex);
    sender_wait_state_t after = sender_wait_snapshot_locked(s, sw_calls);
    pthread_mutex_unlock(&mutex);
    if (after.interrupted) return MOQ_ERR_INTERRUPTED;
    if (after.terminal) return MOQ_ERR_CLOSED;
    if (after.level) return MOQ_OK;
    return rc < 0 ? rc : MOQ_OK;
}

/* Arm or disarm the opt-in hold. Disarming releases anything parked. */
PyObject *test_sender_wait_hold(PyObject *self, PyObject *value)
{
    (void)self;
    int armed = PyObject_IsTrue(value);
    if (armed < 0) return NULL;
    pthread_mutex_lock(&mutex);
    sw_hold_armed = (armed != 0);
    if (!sw_hold_armed) {
        sw_hold_released = true;
        pthread_cond_broadcast(&sw_gate);
    }
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Block until a call is PARKED, and return its index (-1 on the bound). */
PyObject *test_sender_wait_held(PyObject *self, PyObject *args)
{
    (void)self;
    double timeout = 5.0;
    if (!PyArg_ParseTuple(args, "|d", &timeout)) return NULL;
    if (!(timeout >= 0.0) || timeout > 3600.0) {
        PyErr_SetString(PyExc_ValueError,
                        "the bounded test timeout must be 0..3600 seconds");
        return NULL;
    }
    int held = -1;
    int failure = 0;
    Py_BEGIN_ALLOW_THREADS
    struct timespec deadline;
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        failure = errno ? errno : EINVAL;
    } else {
        long whole = (long)timeout;
        long nanos = (long)((timeout - (double)whole) * 1e9);
        deadline.tv_sec += (time_t)whole;
        deadline.tv_nsec += nanos;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec += 1; deadline.tv_nsec -= 1000000000L;
        }
        pthread_mutex_lock(&mutex);
        while (sw_held_call < 0) {
            int rc = pthread_cond_timedwait(&sw_gate, &mutex, &deadline);
            if (rc == ETIMEDOUT) break;
            if (rc != 0) { failure = rc; break; }
        }
        held = sw_held_call;
        pthread_mutex_unlock(&mutex);
    }
    Py_END_ALLOW_THREADS
    if (failure) {
        PyErr_Format(PyExc_RuntimeError,
                     "the hold barrier failed (errno %d), not an absent call",
                     failure);
        return NULL;
    }
    return PyLong_FromLong(held);
}

/* Record WHICH call was parked at the moment of the declared action: -1 when
 * none was, which is exactly what a late action or a cleanup release leaves. */
PyObject *test_sender_wait_note_action(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    sw_action_call = sw_held_call;
    int noted = sw_action_call;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong(noted);
}

/* Release the parked call. This alone records NOTHING: releasing is not
 * acting, so neither cleanup nor a late worker can be credited with the
 * action by releasing. */
PyObject *test_sender_wait_release_held(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    sw_hold_armed = false;      /* one-shot: no LATER call parks */
    sw_hold_released = true;
    pthread_cond_broadcast(&sw_gate);
    bool timed_out = sw_hold_timed_out;
    pthread_mutex_unlock(&mutex);
    return PyBool_FromLong(!timed_out);
}

/* Which call was parked when the action ran, and whether the bound expired. */
PyObject *test_sender_wait_hold_report(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:O}",
        "action_call", sw_action_call,
        "held_call", sw_held_call,
        "timed_out", sw_hold_timed_out ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_sender_wait_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    /* Counted at ENTRY, before anything else: an attempted reset is visible
     * even if the rest of this function were to refuse. The counter itself is
     * deliberately NOT cleared here. */
    sw_resets++;
    sw_hold_armed = false;
    sw_hold_released = true;
    sw_hold_timed_out = false;
    sw_held_call = -1;
    sw_action_call = -1;
    pthread_cond_broadcast(&sw_gate);
    sw_calls = 0;
    sw_last_timeout = sw_total_timeout = 0;
    sw_last_sender = -1;
    sw_level = false;
    sw_level_after = 0;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_wait_level(PyObject *self, PyObject *args)
{
    (void)self;
    int level = 0, after = 0;
    if (!PyArg_ParseTuple(args, "p|i", &level, &after)) return NULL;
    pthread_mutex_lock(&mutex);
    sw_level = level ? true : false;
    sw_level_after = after;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_wait_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:K,s:K,s:n,s:i}",
        "calls", sw_calls,
        "last_timeout_us", sw_last_timeout,
        "total_timeout_us", sw_total_timeout,
        "last_sender", sw_last_sender,
        "resets", sw_resets);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Drive the ACTUAL mirror without the bridge. */
PyObject *test_sender_wait_simulate(PyObject *self, PyObject *args)
{
    (void)self;
    unsigned long long timeout = 0;
    int foreign = 0;
    if (!PyArg_ParseTuple(args, "K|i", &timeout, &foreign)) return NULL;
    moq_media_sender_t *owner = foreign ? NULL : current_sender_or_stub();
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_wait(owner, (uint64_t)timeout);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)rc);
}

/* -- demand query recorder ------------------------------------------------- *
 * Mirrors of the three native demand queries, following the SOURCE's own
 * rules: track_subscriptions returns the mirrored count for an OWNED track
 * and does not test `removed`; has_subscriber is exactly `> 0`; and the
 * aggregate skips removed tracks and non-app-media (catalog/generated) ones.
 *
 * It records which sender and track each query was routed to, and how many of
 * each ran. It is a witness for the binding's mapping and routing; it is not
 * a second publisher, filter engine or subscription scheduler. */

static size_t send_track_subs[FIXTURE_SEND_TRACKS];
static bool send_track_generated[FIXTURE_SEND_TRACKS];
static int dq_subscriptions_calls, dq_has_subscriber_calls, dq_media_calls;
static Py_ssize_t dq_last_track = -1, dq_last_sender = -1;

static Py_ssize_t send_track_slot(const moq_media_track_t *track)
{
    for (size_t i = 0; i < send_track_count; i++)
        if (&send_track_pool[i] == track) return (Py_ssize_t)i;
    return -1;
}

size_t moq_media_sender_track_subscriptions(const moq_media_sender_t *s,
                                            const moq_media_track_t *track)
{
    pthread_mutex_lock(&mutex);
    dq_subscriptions_calls++;
    dq_last_sender = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    dq_last_track = send_track_slot(track);
    sender_log_add("subscriptions");
    size_t n = 0;
    if (s && track) {
        Py_ssize_t i = dq_last_track;
        /* ownership, exactly as the service checks it -- and NO removed test */
        if (i >= 0 && send_track_owner[i] == s) n = send_track_subs[i];
    }
    pthread_mutex_unlock(&mutex);
    return n;
}

bool moq_media_sender_track_has_subscriber(const moq_media_sender_t *s,
                                           const moq_media_track_t *track)
{
    pthread_mutex_lock(&mutex);
    dq_has_subscriber_calls++;
    pthread_mutex_unlock(&mutex);
    return moq_media_sender_track_subscriptions(s, track) > 0;
}

bool moq_media_sender_has_media_subscriber(const moq_media_sender_t *s)
{
    pthread_mutex_lock(&mutex);
    dq_media_calls++;
    dq_last_sender = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    sender_log_add("has_media_subscriber");
    bool any = false;
    if (s) {
        for (size_t i = 0; i < send_track_count; i++) {
            if (send_track_owner[i] != s) continue;
            /* removed and non-app-media tracks are skipped, as in the service */
            if (send_track_removed_flag[i] || send_track_generated[i]) continue;
            if (send_track_subs[i] > 0) { any = true; break; }
        }
    }
    pthread_mutex_unlock(&mutex);
    return any;
}

PyObject *test_demand_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    memset(send_track_subs, 0, sizeof(send_track_subs));
    memset(send_track_generated, 0, sizeof(send_track_generated));
    dq_subscriptions_calls = dq_has_subscriber_calls = dq_media_calls = 0;
    dq_last_track = dq_last_sender = -1;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Script one track's mirrored demand, and whether it is app media. */
PyObject *test_demand_set(PyObject *self, PyObject *args)
{
    (void)self;
    Py_ssize_t index = 0;
    unsigned long long subs = 0;
    int generated = 0;
    if (!PyArg_ParseTuple(args, "nK|i", &index, &subs, &generated))
        return NULL;
    if (index < 0 || (size_t)index >= FIXTURE_SEND_TRACKS) {
        PyErr_SetString(PyExc_ValueError, "track index out of fixture range");
        return NULL;
    }
    if (subs > (unsigned long long)SIZE_MAX) {
        PyErr_SetString(PyExc_OverflowError,
                        "the scripted count exceeds this host's size_t");
        return NULL;
    }
    pthread_mutex_lock(&mutex);
    send_track_subs[index] = (size_t)subs;
    send_track_generated[index] = generated ? true : false;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_demand_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:i,s:n,s:n}",
        "subscriptions", dq_subscriptions_calls,
        "has_subscriber", dq_has_subscriber_calls,
        "has_media_subscriber", dq_media_calls,
        "last_track", dq_last_track,
        "last_sender", dq_last_sender);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* The compiled toolchain's own size_t facts, so a test never assumes a width. */
PyObject *test_size_limits(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    return Py_BuildValue("{s:n,s:K}",
        "size_t_width", (Py_ssize_t)sizeof(size_t),
        "size_t_max", (unsigned long long)SIZE_MAX);
}

/* Drive the ACTUAL native queries without the bridge. */
PyObject *test_demand_simulate(PyObject *self, PyObject *args)
{
    (void)self;
    const char *which;
    Py_ssize_t which_len = 0, index = -1;
    int foreign = 0;
    if (!PyArg_ParseTuple(args, "s#|ni", &which, &which_len, &index, &foreign))
        return NULL;
    pthread_mutex_lock(&mutex);
    moq_media_track_t *track = (index >= 0 && (size_t)index < send_track_count)
                               ? &send_track_pool[index] : NULL;
    moq_media_sender_t *owner = foreign ? &send_track_stub_owner
                                        : current_sender_or_stub();
    if (foreign == 2) owner = NULL;
    pthread_mutex_unlock(&mutex);
    if (strcmp(which, "subscriptions") == 0)
        return PyLong_FromSize_t(
            moq_media_sender_track_subscriptions(owner, track));
    if (strcmp(which, "has_subscriber") == 0)
        return PyBool_FromLong(
            moq_media_sender_track_has_subscriber(owner, track));
    if (strcmp(which, "has_media_subscriber") == 0)
        return PyBool_FromLong(moq_media_sender_has_media_subscriber(owner));
    PyErr_SetString(PyExc_ValueError, "unknown demand query");
    return NULL;
}

/* -- sender stats recorder ------------------------------------------------- *
 * An ABI witness for moq_media_sender_get_stats. It records the caller's REAL
 * output capacity and writes only the scripted stamped prefix, leaving
 * whatever the caller put in the rest of its storage untouched -- so a bridge
 * that reads past the stamp reads the caller's own poison, not a zero this
 * fixture helpfully supplied.
 *
 * It pins mapping and ABI handling. It does not reproduce the native queue
 * policy, and nothing here is evidence about the service's own counters. */

/* The library's frozen v0 floor, computed from the last v0 field exactly as
 * service/src/media_sender.c does, so the fixture answers INVAL on the same
 * boundary the real implementation uses. */
#define SENDER_STATS_V0_SIZE \
    (offsetof(moq_media_sender_stats_t, last_error) + sizeof(moq_result_t))

static moq_media_sender_stats_t scripted_sender_stats;
static uint32_t sender_stats_size;          /* the stamp to write */
static moq_result_t sender_stats_result;
static int sender_stats_entries;
static size_t sender_stats_capacity;        /* the caller's actual out_size */
static Py_ssize_t sender_stats_target;      /* which sender was asked */

moq_result_t moq_media_sender_get_stats(const moq_media_sender_t *s,
                                        moq_media_sender_stats_t *out,
                                        size_t out_size)
{
    pthread_mutex_lock(&mutex);
    sender_stats_entries++;
    sender_stats_capacity = out_size;
    sender_stats_target = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    sender_log_add("get_stats");
    if (!s || !out) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    if (sender_stats_result != MOQ_OK) {
        /* The caller's storage is left exactly as it was, poison included. */
        moq_result_t rc = sender_stats_result;
        pthread_mutex_unlock(&mutex);
        return rc;
    }
    /* The library's own floor: an out_size below the frozen v0 prefix is
     * INVAL, as the real implementation answers. */
    if (out_size < SENDER_STATS_V0_SIZE) {
        pthread_mutex_unlock(&mutex);
        return MOQ_ERR_INVAL;
    }
    /* The copy is bounded INDEPENDENTLY by three real limits, in size_t
     * arithmetic with no truncating cast: the scripted stamp, the source
     * object, and the caller's actual capacity. A dishonest stamp is
     * permission to MIS-REPORT a size, never permission to read or write
     * memory that is not there. */
    size_t n = (size_t)sender_stats_size;
    if (n > sizeof(scripted_sender_stats)) n = sizeof(scripted_sender_stats);
    if (n > out_size) n = out_size;
    memcpy(out, &scripted_sender_stats, n);
    /* The stamp itself is only written when the caller's capacity actually
     * covers that member. */
    if (out_size >= sizeof(out->struct_size))
        out->struct_size = sender_stats_size;   /* scripted, honest or not */
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

/* Call the ACTUAL recorder with real, owned, bounded backing storage and a
 * canary, so its copy boundary is exercised rather than assumed. The caller
 * declares the capacity to offer; everything outside the copied prefix must
 * come back as the poison this driver wrote. */
PyObject *test_sender_stats_drive(PyObject *self, PyObject *args)
{
    (void)self;
    Py_ssize_t capacity = 0;
    int foreign = 0;
    if (!PyArg_ParseTuple(args, "n|i", &capacity, &foreign)) return NULL;
    /* A capacity LARGER than the library's struct is the newer-caller case,
     * and the driver really owns those bytes: the backing block below is the
     * struct plus a canary region, and the offered capacity may reach into
     * that region. A correct recorder still copies at most its own struct. */
    if (capacity < 0 ||
        (size_t)capacity > sizeof(moq_media_sender_stats_t) + 16) {
        PyErr_SetString(PyExc_ValueError,
                        "the bounded driver capacity must be 0..sizeof+16");
        return NULL;
    }
    /* Owned storage: the struct itself plus a trailing canary region that no
     * correct implementation may touch. */
    struct {
        moq_media_sender_stats_t stats;
        unsigned char canary[32];
    } backing;
    memset(&backing, 0xA5, sizeof(backing));

    moq_media_sender_t *owner = foreign ? NULL : current_sender_or_stub();
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_get_stats(owner, &backing.stats, (size_t)capacity);
    Py_END_ALLOW_THREADS

    /* The canary beyond the OFFERED capacity must be untouched. When the
     * caller offered more than the struct, the first bytes of the canary are
     * inside the offered window, so the check starts after it. */
    size_t offered_into_canary =
        (size_t)capacity > sizeof(backing.stats)
        ? (size_t)capacity - sizeof(backing.stats) : 0;
    int canary_intact = 1;
    for (size_t i = offered_into_canary; i < sizeof(backing.canary); i++)
        if (backing.canary[i] != 0xA5) { canary_intact = 0; break; }
    /* Whatever the caller offered beyond the library's own struct must still
     * be poison: a newer caller's extra bytes are never written. */
    int beyond_struct_untouched = 1;
    for (size_t i = 0; i < offered_into_canary; i++)
        if (backing.canary[i] != 0xA5) { beyond_struct_untouched = 0; break; }
    /* How much of the caller's own struct storage is still poison, counted
     * from the end, so a write past the offered capacity is visible. */
    const unsigned char *raw = (const unsigned char *)&backing.stats;
    size_t untouched = 0;
    while (untouched < sizeof(backing.stats) &&
           raw[sizeof(backing.stats) - 1 - untouched] == 0xA5)
        untouched++;
    return Py_BuildValue("{s:i,s:O,s:O,s:n,s:n,s:y#}",
        "rc", (int)rc,
        "canary_intact", canary_intact ? Py_True : Py_False,
        "beyond_struct_untouched",
        beyond_struct_untouched ? Py_True : Py_False,
        "untouched_tail", (Py_ssize_t)untouched,
        "stamp", (Py_ssize_t)((size_t)capacity >= sizeof(backing.stats.struct_size)
                              ? (Py_ssize_t)backing.stats.struct_size : -1),
        "prefix", (const char *)raw,
        (Py_ssize_t)((size_t)capacity < sizeof(backing.stats)
                     ? capacity : (Py_ssize_t)sizeof(backing.stats)));
    /* `prefix` is the caller's own struct storage only, never the canary. */
}

PyObject *test_sender_stats(PyObject *self, PyObject *args)
{
    (void)self;
    unsigned long long v[9], evicted;
    long long last_error;
    if (!PyArg_ParseTuple(args, "KKKKKKKKKLK", &v[0], &v[1], &v[2], &v[3],
                          &v[4], &v[5], &v[6], &v[7], &v[8], &last_error,
                          &evicted))
        return NULL;
    pthread_mutex_lock(&mutex);
    scripted_sender_stats.objects_written = v[0];
    scripted_sender_stats.objects_sent = v[1];
    scripted_sender_stats.objects_queued = v[2];
    scripted_sender_stats.bytes_queued = v[3];
    scripted_sender_stats.objects_dropped = v[4];
    scripted_sender_stats.groups_dropped = v[5];
    scripted_sender_stats.keyframes_dropped = v[6];
    scripted_sender_stats.groups_abandoned = v[7];
    scripted_sender_stats.backpressure_stalls = v[8];
    scripted_sender_stats.last_error = (moq_result_t)last_error;
    scripted_sender_stats.sap_records_evicted = evicted;
    sender_stats_size = (uint32_t)sizeof(scripted_sender_stats);
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_stats_size(PyObject *self, PyObject *value)
{
    (void)self;
    unsigned long size = PyLong_AsUnsignedLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    sender_stats_size = (uint32_t)size;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_stats_result(PyObject *self, PyObject *value)
{
    (void)self;
    long code = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    sender_stats_result = (moq_result_t)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_stats_calls(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:n,s:n}",
        "entries", sender_stats_entries,
        "capacity", (Py_ssize_t)sender_stats_capacity,
        "target", sender_stats_target);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* The SDK's own layout facts, so a test never hardcodes host offsets. */
/* Per-field offset and width, so a test can build an expected IMAGE from its
 * own declared values instead of reading the recorder's buffer back. */
PyObject *test_sender_stats_fields(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
#define FIELD(name) \
    #name, (Py_ssize_t)offsetof(moq_media_sender_stats_t, name), \
    (Py_ssize_t)sizeof(((moq_media_sender_stats_t *)0)->name)
    return Py_BuildValue(
        "{s:(nn),s:(nn),s:(nn),s:(nn),s:(nn),s:(nn),s:(nn),s:(nn),s:(nn),"
        "s:(nn),s:(nn),s:(nn)}",
        FIELD(struct_size), FIELD(objects_written), FIELD(objects_sent),
        FIELD(objects_queued), FIELD(bytes_queued), FIELD(objects_dropped),
        FIELD(groups_dropped), FIELD(keyframes_dropped),
        FIELD(groups_abandoned), FIELD(backpressure_stalls),
        FIELD(last_error), FIELD(sap_records_evicted));
#undef FIELD
}

PyObject *test_sender_stats_layout(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    return Py_BuildValue("{s:n,s:n,s:n,s:n}",
        "sizeof", (Py_ssize_t)sizeof(moq_media_sender_stats_t),
        "v0_end", (Py_ssize_t)(offsetof(moq_media_sender_stats_t, last_error)
                               + sizeof(moq_result_t)),
        "sap_offset",
        (Py_ssize_t)offsetof(moq_media_sender_stats_t, sap_records_evicted),
        "sap_end",
        (Py_ssize_t)(offsetof(moq_media_sender_stats_t, sap_records_evicted)
                     + sizeof(uint64_t)));
}

/* The scripted snapshot as the recorder would deliver it, for a scoped
 * stand-in that has no capsule: it counts as an entry and records a full
 * capacity, exactly as a bridge call would, so the call-inventory rows mean
 * the same thing under the stand-in as under the real bridge. */
PyObject *test_sender_stats_snapshot(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    sender_stats_entries++;
    sender_stats_capacity = sizeof(moq_media_sender_stats_t);
    sender_stats_target = 0;
    sender_log_add("get_stats");
    if (sender_stats_result != MOQ_OK) {
        pthread_mutex_unlock(&mutex);
        Py_RETURN_NONE;
    }
    PyObject *r = Py_BuildValue(
        "{s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:i,s:K,s:I}",
        "objects_written", (unsigned long long)scripted_sender_stats.objects_written,
        "objects_sent", (unsigned long long)scripted_sender_stats.objects_sent,
        "objects_queued", (unsigned long long)scripted_sender_stats.objects_queued,
        "bytes_queued", (unsigned long long)scripted_sender_stats.bytes_queued,
        "objects_dropped", (unsigned long long)scripted_sender_stats.objects_dropped,
        "groups_dropped", (unsigned long long)scripted_sender_stats.groups_dropped,
        "keyframes_dropped", (unsigned long long)scripted_sender_stats.keyframes_dropped,
        "groups_abandoned", (unsigned long long)scripted_sender_stats.groups_abandoned,
        "backpressure_stalls", (unsigned long long)scripted_sender_stats.backpressure_stalls,
        "last_error", (int)scripted_sender_stats.last_error,
        "sap_records_evicted", (unsigned long long)scripted_sender_stats.sap_records_evicted,
        "struct_size", (unsigned int)sender_stats_size);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_sender_stats_last_result(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    long r = (long)sender_stats_result;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong(r);
}

PyObject *test_sender_stats_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    memset(&scripted_sender_stats, 0, sizeof(scripted_sender_stats));
    sender_stats_size = (uint32_t)sizeof(scripted_sender_stats);
    sender_stats_result = MOQ_OK;
    sender_stats_entries = 0;
    sender_stats_capacity = 0;
    sender_stats_target = -1;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* -- end-track recorder ---------------------------------------------------- *
 * A bridge witness for moq_media_sender_end_track. It records WHICH sender and
 * track were passed and how many times, and it keeps a per-track end flag so
 * the accepted precedence -- terminal and interrupt BEFORE idempotence -- can
 * be scripted the way the real service orders it.
 *
 * It does not queue, drain or emit anything: a scripted MOQ_OK here is not
 * evidence that the service emits END_OF_TRACK on the wire. That belongs to
 * the native suite (service/tests/test_media_sender.c drives the public
 * end_track over a real loopback peer). */

static int et_entries;
static moq_result_t et_result;
static bool et_seen;
static bool et_track_null, et_sender_null;
static Py_ssize_t et_track_index, et_sender_index;
/* Parks inside the native end call on the SAME bounded gate the write
 * recorder uses, so _test_write_wait_entered and _test_write_release drive
 * both. One mechanism, one bound, no sleeps. */
static bool et_block_armed;
static bool send_track_end_requested[FIXTURE_SEND_TRACKS];
static int et_ended;

moq_result_t moq_media_sender_end_track(moq_media_sender_t *s,
                                        moq_media_track_t *track)
{
    pthread_mutex_lock(&mutex);
    et_entries++;
    et_seen = true;
    sender_log_add("end_track");
    et_sender_null = (s == NULL);
    et_track_null = (track == NULL);
    et_sender_index = (s && s == current_sender) ? 0 : (s ? 1 : -1);
    et_track_index = -1;
    for (size_t i = 0; i < send_track_count; i++)
        if (&send_track_pool[i] == track) { et_track_index = (Py_ssize_t)i; break; }

    if (et_block_armed) wr_park();

    /* A scripted non-OK answers first, exactly as the service's terminal and
     * interrupt checks precede its idempotent no-op. */
    if (et_result != MOQ_OK) {
        moq_result_t scripted = et_result;
        pthread_mutex_unlock(&mutex);
        return scripted;                 /* nothing is latched on refusal */
    }
    moq_result_t rc = MOQ_ERR_INVAL;
    for (size_t i = 0; i < send_track_count; i++) {
        if (&send_track_pool[i] != track) continue;
        if (send_track_owner[i] != s) { rc = MOQ_ERR_INVAL; break; }   /* foreign */
        if (send_track_removed_flag[i]) { rc = MOQ_ERR_WRONG_STATE; break; }
        if (send_track_end_requested[i]) { rc = MOQ_OK; break; }  /* idempotent */
        send_track_end_requested[i] = true;
        et_ended++;
        rc = MOQ_OK;
        break;
    }
    pthread_mutex_unlock(&mutex);
    return rc;
}

PyObject *test_end_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    et_entries = et_ended = 0;
    et_result = MOQ_OK;
    et_seen = false;
    et_track_null = et_sender_null = false;
    et_track_index = et_sender_index = -1;
    et_block_armed = false;
    memset(send_track_end_requested, 0, sizeof(send_track_end_requested));
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_end_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:O}",
        "entries", et_entries,
        "ended", et_ended,
        "seen", et_seen ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_end_target(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    if (!et_seen) { pthread_mutex_unlock(&mutex); Py_RETURN_NONE; }
    PyObject *r = Py_BuildValue("{s:n,s:n,s:O,s:O}",
        "track_index", et_track_index,
        "sender_index", et_sender_index,
        "track_null", et_track_null ? Py_True : Py_False,
        "sender_null", et_sender_null ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_end_block(PyObject *self, PyObject *value)
{
    (void)self;
    int armed = PyObject_IsTrue(value);
    if (armed < 0) return NULL;
    pthread_mutex_lock(&mutex);
    et_block_armed = (armed != 0);
    if (!et_block_armed) {
        wr_released = true;
        pthread_cond_broadcast(&wr_gate);
    } else {
        wr_released = false;
    }
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_end_result(PyObject *self, PyObject *args)
{
    (void)self;
    int code = 0;
    if (!PyArg_ParseTuple(args, "i", &code)) return NULL;
    pthread_mutex_lock(&mutex);
    et_result = (moq_result_t)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Whether the recorder holds an end request for a declared track, so a row can
 * prove that a refusal latched NOTHING. */
PyObject *test_end_requested(PyObject *self, PyObject *value)
{
    (void)self;
    Py_ssize_t index = PyLong_AsSsize_t(value);
    if (index == -1 && PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    bool requested = (index >= 0 && (size_t)index < send_track_count)
                     ? send_track_end_requested[index] : false;
    pthread_mutex_unlock(&mutex);
    return PyBool_FromLong(requested);
}

/* Drive the recorder's own end_track without the bridge, so the witness is
 * exercised before any product depends on it. */
/* The fixture index of the live track with this declared name, so a scoped
 * stand-in can name its target without a capsule. */
PyObject *test_send_track_index_for(PyObject *self, PyObject *args)
{
    (void)self;
    PyObject *value;
    int include_removed = 0;
    if (!PyArg_ParseTuple(args, "O|i", &value, &include_removed)) return NULL;
    char *name;
    Py_ssize_t len;
    if (PyBytes_AsStringAndSize(value, &name, &len) < 0) return NULL;
    pthread_mutex_lock(&mutex);
    Py_ssize_t found = -1;
    for (size_t i = 0; i < send_track_count; i++) {
        if (send_track_removed_flag[i] && !include_removed) continue;
        if (send_track_name_store[i].len != (size_t)len) continue;
        if (len && memcmp(send_track_name_store[i].data, name, (size_t)len) != 0)
            continue;
        found = (Py_ssize_t)i;
        break;
    }
    pthread_mutex_unlock(&mutex);
    return PyLong_FromSsize_t(found);
}

PyObject *test_end_simulate(PyObject *self, PyObject *args)
{
    (void)self;
    Py_ssize_t index = 0;
    int foreign = 0;
    if (!PyArg_ParseTuple(args, "n|i", &index, &foreign)) return NULL;
    pthread_mutex_lock(&mutex);
    moq_media_track_t *track = (index >= 0 && (size_t)index < send_track_count)
                               ? &send_track_pool[index] : NULL;
    moq_media_sender_t *owner = foreign ? &send_track_stub_owner
                                        : current_sender_or_stub();
    pthread_mutex_unlock(&mutex);
    moq_result_t rc;
    /* The GIL is released around the call, as the bridge must: otherwise a
     * parked call could never be reached by another thread. */
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_end_track(owner, track);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)rc);
}

/* -- completion request recorder ------------------------------------------ *
 * A bridge witness for moq_media_sender_complete. It records WHICH sender was
 * passed and how many times, keeps its own completing latch so the accepted
 * precedence -- interrupt and terminal BEFORE idempotence -- can be scripted
 * the way the real service orders it, and marks the passed sender's live
 * tracks removed on acceptance, which is the one state change the request
 * makes synchronously.
 *
 * It does not publish a terminal catalog, end publisher tracks, drop queued
 * media or account for anything: in the service those happen later, on the
 * network pump. A scripted MOQ_OK here is NOT evidence that the broadcast
 * completed on the wire. That belongs to the native suite. */

static int cp_entries, cp_accepted;
static moq_result_t cp_result;
static bool cp_seen, cp_sender_null;
static Py_ssize_t cp_sender_index;
static bool cp_completing;          /* the recorder's own terminal latch */
static int cp_tracks_removed;       /* tracks THIS recorder marked removed */
/* Parks inside the native call on the same bounded gate the write and end
 * recorders use, so _test_write_wait_entered and _test_write_release drive
 * all three. One mechanism, one bound, no sleeps. */
static bool cp_block_armed;
/* Completion-fixture reset ENTRIES, cumulative for the process and never
 * cleared by the reset itself, so an attempted reset stays visible. It is
 * this recorder's own counter: the envelope's receipt reports WAIT-fixture
 * resets, which are a different family entirely. */
static int cp_reset_entries;

moq_result_t moq_media_sender_complete(moq_media_sender_t *s)
{
    pthread_mutex_lock(&mutex);
    cp_entries++;
    cp_seen = true;
    sender_log_add("complete");
    cp_sender_null = (s == NULL);
    cp_sender_index = (s && s == current_sender) ? 0 : (s ? 1 : -1);

    if (cp_block_armed) wr_park();

    /* A scripted non-OK answers first, exactly as the service's interrupt and
     * terminal checks precede its idempotent no-op: a refusal after the latch
     * is already set is still a refusal, never a courtesy success. */
    if (cp_result != MOQ_OK) {
        moq_result_t scripted = cp_result;
        pthread_mutex_unlock(&mutex);
        return scripted;                 /* nothing is latched on refusal */
    }
    if (cp_completing) {                 /* idempotent: a second request is a no-op */
        pthread_mutex_unlock(&mutex);
        return MOQ_OK;
    }
    cp_completing = true;
    for (size_t i = 0; i < send_track_count; i++) {
        if (send_track_owner[i] != s) continue;
        if (send_track_removed_flag[i]) continue;
        send_track_removed_flag[i] = true;
        cp_tracks_removed++;
    }
    cp_accepted++;
    pthread_mutex_unlock(&mutex);
    return MOQ_OK;
}

PyObject *test_complete_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    /* Counted at ENTRY, before anything is cleared, so an attempted reset is
     * visible even if the rest of this function were to refuse. */
    cp_reset_entries++;
    cp_entries = cp_accepted = cp_tracks_removed = 0;
    cp_result = MOQ_OK;
    cp_seen = cp_sender_null = cp_completing = cp_block_armed = false;
    cp_sender_index = -1;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* How often the completion fixture's reset was ENTERED. Read as a delta
 * against a recorded baseline; the reset does not clear it. */
PyObject *test_complete_reset_entries(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    int entries = cp_reset_entries;
    pthread_mutex_unlock(&mutex);
    return PyLong_FromLong((long)entries);
}

PyObject *test_complete_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:i,s:O,s:O}",
        "entries", cp_entries,
        "accepted", cp_accepted,
        "tracks_removed", cp_tracks_removed,
        "completing", cp_completing ? Py_True : Py_False,
        "seen", cp_seen ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_complete_target(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    if (!cp_seen) { pthread_mutex_unlock(&mutex); Py_RETURN_NONE; }
    PyObject *r = Py_BuildValue("{s:n,s:O}",
        "sender_index", cp_sender_index,
        "sender_null", cp_sender_null ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_complete_result(PyObject *self, PyObject *args)
{
    (void)self;
    int code = 0;
    if (!PyArg_ParseTuple(args, "i", &code)) return NULL;
    pthread_mutex_lock(&mutex);
    cp_result = (moq_result_t)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_complete_block(PyObject *self, PyObject *value)
{
    (void)self;
    int armed = PyObject_IsTrue(value);
    if (armed < 0) return NULL;
    pthread_mutex_lock(&mutex);
    cp_block_armed = (armed != 0);
    if (!cp_block_armed) {
        wr_released = true;
        pthread_cond_broadcast(&wr_gate);
    } else {
        wr_released = false;
    }
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Drive the recorder's own complete without the bridge, so the witness is
 * exercised before any product depends on it. */
PyObject *test_complete_simulate(PyObject *self, PyObject *args)
{
    (void)self;
    int foreign = 0, null_sender = 0;
    if (!PyArg_ParseTuple(args, "|ii", &foreign, &null_sender)) return NULL;
    pthread_mutex_lock(&mutex);
    moq_media_sender_t *owner = null_sender ? NULL
                                : (foreign ? &send_track_stub_owner
                                           : current_sender_or_stub());
    pthread_mutex_unlock(&mutex);
    moq_result_t rc;
    /* The GIL is released around the call, as the bridge must: otherwise a
     * parked call could never be reached by another thread. */
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_complete(owner);
    Py_END_ALLOW_THREADS
    return PyLong_FromLong((long)rc);
}

PyObject *test_send_track_simulate_add(PyObject *self, PyObject *args)
{
    (void)self;
    const char *name = NULL;
    Py_ssize_t name_len = 0;
    unsigned int struct_size = 0;
    if (!PyArg_ParseTuple(args, "y#I", &name, &name_len, &struct_size))
        return NULL;
    moq_media_track_cfg_t cfg;
    moq_media_track_cfg_init(&cfg);
    if (struct_size) cfg.struct_size = struct_size;
    cfg.name = (moq_bytes_t){ (const uint8_t *)name, (size_t)name_len };
    cfg.media_type = MOQ_MEDIA_TYPE_VIDEO;
    cfg.packaging = MOQ_MEDIA_PACKAGING_RAW;
    cfg.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
    cfg.bitrate = 1500000;
    moq_media_track_t *out = NULL;
    moq_result_t rc = moq_media_sender_add_track(current_sender_or_stub(), &cfg,
                                                 &out);
    return PyLong_FromLong((long)rc);
}

PyObject *test_send_track_cfg_shape(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    moq_media_track_cfg_t cfg;
    memset(&cfg, 0xA5, sizeof(cfg));
    moq_media_track_cfg_init(&cfg);
    return Py_BuildValue(
        "{s:n,s:I,s:O,s:i,s:i,s:I,s:K,s:O,s:O,s:O,s:O,s:O,s:n}",
        "cfg_size", (Py_ssize_t)sizeof(cfg),
        "struct_size", (unsigned int)cfg.struct_size,
        "is_live", cfg.is_live ? Py_True : Py_False,
        "media_type", (int)cfg.media_type,
        "packaging", (int)cfg.packaging,
        "timescale", (unsigned int)cfg.timescale,
        "bitrate", (unsigned long long)cfg.bitrate,
        "has_track_duration", cfg.has_track_duration ? Py_True : Py_False,
        "has_max_grp_sap", cfg.has_max_grp_sap ? Py_True : Py_False,
        "emit_sap_timeline", cfg.emit_sap_timeline ? Py_True : Py_False,
        "emit_media_timeline", cfg.emit_media_timeline ? Py_True : Py_False,
        "has_alt_group", cfg.has_alt_group ? Py_True : Py_False,
        "content_protection_ref_id_count",
        (Py_ssize_t)cfg.content_protection_ref_id_count);
}

PyObject *test_send_track_reset(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    memset(send_track_pool, 0, sizeof(send_track_pool));
    memset(send_track_owner, 0, sizeof(send_track_owner));
    memset(send_track_removed_flag, 0, sizeof(send_track_removed_flag));
    memset(send_track_owner_destroyed, 0, sizeof(send_track_owner_destroyed));
    send_track_count = 0;
    tk_add_entries = tk_added = tk_remove_entries = tk_removed = 0;
    tk_add_result = tk_remove_result = MOQ_OK;
    tk_pool_exhausted = false;
    tcfg_seen = false;
    tcfg_clear();
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_send_track_counts(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    Py_ssize_t live_native = 0;
    for (size_t i = 0; i < send_track_count; i++)
        if (!send_track_removed_flag[i] && !send_track_owner_destroyed[i])
            live_native++;
    PyObject *r = Py_BuildValue(
        "{s:i,s:i,s:i,s:i,s:n,s:n,s:O}",
        "add_entries", tk_add_entries,
        "added", tk_added,
        "remove_entries", tk_remove_entries,
        "removed", tk_removed,
        "allocated", (Py_ssize_t)send_track_count,
        "live_native", live_native,
        "pool_exhausted", tk_pool_exhausted ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_send_track_config(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    pthread_mutex_lock(&mutex);
    if (!tcfg_seen) { pthread_mutex_unlock(&mutex); Py_RETURN_NONE; }
    PyObject *r = Py_BuildValue(
        "{s:y#,s:y#,s:y#,s:y#,s:y#,s:y#,s:i,s:i,s:I,s:I,s:I,s:K,s:I,s:K,s:O,s:O,"
        "s:K,s:I,s:O,s:O,s:O,s:O,s:O,s:O,s:n,s:O,s:O,s:z}",
        "name", (const char *)tcfg_name.data, (Py_ssize_t)tcfg_name.len,
        "codec", (const char *)tcfg_codec.data, (Py_ssize_t)tcfg_codec.len,
        "init_data", (const char *)tcfg_init_data.data, (Py_ssize_t)tcfg_init_data.len,
        "role", (const char *)tcfg_role.data, (Py_ssize_t)tcfg_role.len,
        "lang", (const char *)tcfg_lang.data, (Py_ssize_t)tcfg_lang.len,
        "channel_config", (const char *)tcfg_channel_config.data,
        (Py_ssize_t)tcfg_channel_config.len,
        "media_type", tcfg_media_type,
        "packaging", tcfg_packaging,
        "timescale", (unsigned int)tcfg_timescale,
        "width", (unsigned int)tcfg_width,
        "height", (unsigned int)tcfg_height,
        "framerate_millis", (unsigned long long)tcfg_framerate_millis,
        "samplerate", (unsigned int)tcfg_samplerate,
        "bitrate", (unsigned long long)tcfg_bitrate,
        "is_live", tcfg_is_live ? Py_True : Py_False,
        "has_track_duration", tcfg_has_track_duration ? Py_True : Py_False,
        "track_duration_ms", (unsigned long long)tcfg_track_duration_ms,
        "struct_size", (unsigned int)tcfg_struct_size,
        "full_size", tcfg_full_size ? Py_True : Py_False,
        "has_max_grp_sap", tcfg_has_max_grp_sap ? Py_True : Py_False,
        "has_max_obj_sap", tcfg_has_max_obj_sap ? Py_True : Py_False,
        "emit_sap_timeline", tcfg_emit_sap_timeline ? Py_True : Py_False,
        "emit_media_timeline", tcfg_emit_media_timeline ? Py_True : Py_False,
        "has_alt_group", tcfg_has_alt_group ? Py_True : Py_False,
        "content_protection_ref_id_count", (Py_ssize_t)tcfg_cp_ref_count,
        "content_protection_ref_ids_null", tcfg_cp_ptr_null ? Py_True : Py_False,
        "oracle_limit", tcfg_oracle_limit ? Py_True : Py_False,
        "oracle_limit_reason", tcfg_oracle_limit_reason);
    pthread_mutex_unlock(&mutex);
    return r;
}

/* Script the next add/remove outcome. The call and its configuration are still
 * recorded, so a controlled refusal never erases the witness. */
PyObject *test_send_track_results(PyObject *self, PyObject *args)
{
    (void)self;
    int add = 0, remove = 0;
    if (!PyArg_ParseTuple(args, "ii", &add, &remove)) return NULL;
    pthread_mutex_lock(&mutex);
    tk_add_result = (moq_result_t)add;
    tk_remove_result = (moq_result_t)remove;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

/* Per-handle state: whether the slot is still a live (non-removed) track. */
PyObject *test_send_track_state(PyObject *self, PyObject *args)
{
    (void)self;
    Py_ssize_t index = 0;
    if (!PyArg_ParseTuple(args, "n", &index)) return NULL;
    pthread_mutex_lock(&mutex);
    if (index < 0 || (size_t)index >= send_track_count) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_IndexError, "no such fixture send track");
        return NULL;
    }
    PyObject *r = Py_BuildValue("{s:O,s:O,s:O}",
        "allocated", Py_True,
        "removed", send_track_removed_flag[index] ? Py_True : Py_False,
        "owner_destroyed",
        send_track_owner_destroyed[index] ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_sender_cfg_shape(PyObject *self, PyObject *args)
{
    (void)self; (void)args;
    moq_media_sender_cfg_t cfg;
    memset(&cfg, 0xA5, sizeof(cfg));
    moq_media_sender_cfg_init_sized(&cfg, sizeof(cfg));
    bool pointers_null =
        cfg.callbacks.ctx == NULL && cfg.callbacks.on_subscriber_joined == NULL &&
        cfg.callbacks.on_subscriber_left == NULL && cfg.callbacks.on_ready == NULL &&
        cfg.callbacks.on_closed == NULL && cfg.callbacks.on_track_closed == NULL;
    return Py_BuildValue(
        "{s:n,s:n,s:n,s:I,s:I,s:O,s:i,s:O,s:O}",
        "cfg_size", (Py_ssize_t)sizeof(cfg),
        "callbacks_size", (Py_ssize_t)sizeof(cfg.callbacks),
        "callbacks_offset", (Py_ssize_t)offsetof(moq_media_sender_cfg_t, callbacks),
        "cfg_struct_size", (unsigned int)cfg.struct_size,
        "callbacks_struct_size", (unsigned int)cfg.callbacks.struct_size,
        "validate_cmaf", cfg.validate_cmaf ? Py_True : Py_False,
        "backpressure", (int)cfg.backpressure,
        "callbacks_pointers_null", pointers_null ? Py_True : Py_False,
        "content_protections_null",
        (cfg.content_protections == NULL && cfg.content_protection_count == 0)
            ? Py_True : Py_False);
}


PyObject *test_sender_reset(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    if (current_sender) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked a native sender");
        return NULL;
    }
    senders_attached = senders_destroyed = 0;
    sender_attach_entries = sender_destroy_entries = 0;
    sender_attach_result = MOQ_OK;
    sender_log_count = 0;
    sender_log_overflow = false;
    memset(scfg_parts, 0, sizeof(scfg_parts));
    memset(scfg_part_captured, 0, sizeof(scfg_part_captured));
    memset(scfg_part_declared, 0, sizeof(scfg_part_declared));
    scfg_declared_count = scfg_captured_count = 0;
    memset(scfg_catalog, 0, sizeof(scfg_catalog));
    scfg_catalog_captured = scfg_catalog_declared = 0;
    scfg_backpressure = 0;
    scfg_block_timeout_us = scfg_catalog_refresh_interval_us = 0;
    scfg_queue_max_objects = scfg_queue_max_bytes = 0;
    scfg_pre_ready_max_objects = scfg_pre_ready_max_bytes = 0;
    scfg_validate_cmaf = scfg_publish_tracks = scfg_drop_without_demand = false;
    scfg_endpoint_null = scfg_full_size = scfg_callbacks_absent = false;
    scfg_callbacks_struct_size = 0;
    scfg_content_protections_absent = false;
    scfg_seen = scfg_oracle_limit = scfg_shape_refused = false;
    scfg_unsupported_prefix = false;
    scfg_oracle_limit_reason = scfg_shape_refused_reason = NULL;
    scfg_unsupported_reason = NULL;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_counts(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    PyObject *r = Py_BuildValue("{s:i,s:i,s:i,s:i,s:O}",
        "attach_entries", sender_attach_entries,
        "attached", senders_attached,
        "destroy_entries", sender_destroy_entries,
        "destroyed", senders_destroyed,
        "live", current_sender ? Py_True : Py_False);
    pthread_mutex_unlock(&mutex);
    return r;
}

PyObject *test_sender_log(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    if (sender_log_overflow) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "fixture lifecycle log overflowed");
        return NULL;
    }
    PyObject *t = PyTuple_New((Py_ssize_t)sender_log_count);
    if (!t) { pthread_mutex_unlock(&mutex); return NULL; }
    for (int i = 0; i < sender_log_count; ++i) {
        PyObject *item = PyUnicode_FromString(sender_log[i]);
        if (!item || PyTuple_SetItem(t, i, item) < 0) {
            Py_DECREF(t); pthread_mutex_unlock(&mutex); return NULL;
        }
    }
    pthread_mutex_unlock(&mutex);
    return t;
}

PyObject *test_sender_attach_result(PyObject *module, PyObject *value)
{
    (void)module;
    long rc = PyLong_AsLong(value);
    if (rc == -1 && PyErr_Occurred()) return NULL;
    pthread_mutex_lock(&mutex);
    sender_attach_result = (moq_result_t)rc;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_state(PyObject *module, PyObject *args)
{
    (void)module;
    int ready = 0, fatal = 0;
    unsigned long long code = 0;
    if (!PyArg_ParseTuple(args, "ppK", &ready, &fatal, &code)) return NULL;
    pthread_mutex_lock(&mutex);
    if (!current_sender) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "no native sender is attached");
        return NULL;
    }
    current_sender->ready = ready ? true : false;
    current_sender->fatal = fatal ? true : false;
    current_sender->fatal_code = (uint64_t)code;
    pthread_mutex_unlock(&mutex);
    Py_RETURN_NONE;
}

PyObject *test_sender_config(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    if (!scfg_seen) {
        pthread_mutex_unlock(&mutex);
        Py_RETURN_NONE;
    }
    /* Only what was safely retained is ever read back. */
    PyObject *parts = PyTuple_New((Py_ssize_t)scfg_captured_count);
    PyObject *declared = PyTuple_New((Py_ssize_t)(scfg_declared_count < FIXTURE_MAX_PARTS
                                                  ? scfg_declared_count : FIXTURE_MAX_PARTS));
    if (!parts || !declared) {
        Py_XDECREF(parts); Py_XDECREF(declared);
        pthread_mutex_unlock(&mutex); return NULL;
    }
    for (size_t i = 0; i < scfg_captured_count; ++i) {
        PyObject *part = PyBytes_FromStringAndSize((const char *)scfg_parts[i],
                                                   (Py_ssize_t)scfg_part_captured[i]);
        if (!part || PyTuple_SetItem(parts, (Py_ssize_t)i, part) < 0) {
            Py_DECREF(parts); Py_DECREF(declared);
            pthread_mutex_unlock(&mutex); return NULL;
        }
    }
    Py_ssize_t declared_n = (Py_ssize_t)(scfg_declared_count < FIXTURE_MAX_PARTS
                                         ? scfg_declared_count : FIXTURE_MAX_PARTS);
    for (Py_ssize_t i = 0; i < declared_n; ++i) {
        PyObject *len = PyLong_FromSize_t(scfg_part_declared[i]);
        if (!len || PyTuple_SetItem(declared, i, len) < 0) {
            Py_DECREF(parts); Py_DECREF(declared);
            pthread_mutex_unlock(&mutex); return NULL;
        }
    }
    PyObject *result = Py_BuildValue(
        "{s:O,s:O,s:n,s:y#,s:n,s:i,s:K,s:I,s:I,s:I,s:I,s:O,s:O,s:O,s:K,s:O,s:O,s:O,s:I,s:O,s:O,s:z,s:O,s:z,s:O,s:z}",
        "namespace", parts,
        "namespace_declared_lengths", declared,
        "namespace_declared_count", (Py_ssize_t)scfg_declared_count,
        "catalog_track", (const char *)scfg_catalog, (Py_ssize_t)scfg_catalog_captured,
        "catalog_declared_length", (Py_ssize_t)scfg_catalog_declared,
        "backpressure", scfg_backpressure,
        "block_timeout_us", (unsigned long long)scfg_block_timeout_us,
        "queue_max_objects", (unsigned int)scfg_queue_max_objects,
        "queue_max_bytes", (unsigned int)scfg_queue_max_bytes,
        "pre_ready_max_objects", (unsigned int)scfg_pre_ready_max_objects,
        "pre_ready_max_bytes", (unsigned int)scfg_pre_ready_max_bytes,
        "validate_cmaf", scfg_validate_cmaf ? Py_True : Py_False,
        "publish_tracks", scfg_publish_tracks ? Py_True : Py_False,
        "drop_without_demand", scfg_drop_without_demand ? Py_True : Py_False,
        "catalog_refresh_interval_us", (unsigned long long)scfg_catalog_refresh_interval_us,
        "endpoint_null", scfg_endpoint_null ? Py_True : Py_False,
        "full_size", scfg_full_size ? Py_True : Py_False,
        "callbacks_absent", scfg_callbacks_absent ? Py_True : Py_False,
        "callbacks_struct_size", (unsigned int)scfg_callbacks_struct_size,
        "content_protections_absent", scfg_content_protections_absent ? Py_True : Py_False,
        "oracle_limit", scfg_oracle_limit ? Py_True : Py_False,
        "oracle_limit_reason", scfg_oracle_limit_reason,
        "shape_refused", scfg_shape_refused ? Py_True : Py_False,
        "shape_refused_reason", scfg_shape_refused_reason,
        "unsupported_prefix", scfg_unsupported_prefix ? Py_True : Py_False,
        "unsupported_prefix_reason", scfg_unsupported_reason);
    Py_DECREF(parts); Py_DECREF(declared);
    pthread_mutex_unlock(&mutex);
    return result;
}

/* Substrate self-check: drive the fixture's OWN attach with declared values
 * for every supported field. On success the attachment stays LIVE, so the
 * endpoint stop gate can be exercised; release it with the destroy seam.
 * Never used by the binding. */
PyObject *test_sender_simulate_attach(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *ns = NULL;
    const char *catalog = NULL;
    Py_ssize_t catalog_len = 0;
    int backpressure = 0, validate_cmaf = 0, publish_tracks = 0, drop_without_demand = 0;
    unsigned long long block_timeout_us = 0, refresh_us = 0;
    unsigned int qmo = 0, qmb = 0, pmo = 0, pmb = 0;
    /* null_part: index of a part whose POINTER is cleared while its length
     * stays nonzero -- the array itself is fully backed, and that entry is
     * never dereferenced. struct_size: 0 means the full current size. */
    Py_ssize_t null_part = -1;
    unsigned int struct_size = 0;
    if (!PyArg_ParseTuple(args, "Oy#iKIIIIpppKnI", &ns, &catalog, &catalog_len,
                          &backpressure, &block_timeout_us, &qmo, &qmb, &pmo, &pmb,
                          &validate_cmaf, &publish_tracks, &drop_without_demand,
                          &refresh_us, &null_part, &struct_size))
        return NULL;
    if (!PyTuple_Check(ns)) {
        PyErr_SetString(PyExc_TypeError, "namespace must be a tuple of bytes");
        return NULL;
    }
    Py_ssize_t count = PyTuple_Size(ns);
    moq_bytes_t *parts = count ? calloc((size_t)count, sizeof(*parts)) : NULL;
    if (count && !parts) return PyErr_NoMemory();
    for (Py_ssize_t i = 0; i < count; ++i) {
        PyObject *item = PyTuple_GetItem(ns, i);
        char *data = NULL; Py_ssize_t len = 0;
        if (PyBytes_AsStringAndSize(item, &data, &len) < 0) { free(parts); return NULL; }
        parts[i] = (moq_bytes_t){ (const uint8_t *)data, (size_t)len };
    }
    if (null_part >= 0 && null_part < count)
        parts[null_part].data = NULL;          /* coherence probe, never read */
    /* Built here rather than through moq_media_sender_cfg_init_sized: the
     * fixture module links moq::core only, never the real service. */
    moq_media_sender_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.struct_size = struct_size ? struct_size : (uint32_t)sizeof(cfg);
    cfg.namespace_.parts = parts;
    cfg.namespace_.count = (size_t)count;
    cfg.catalog_track = (moq_bytes_t){ (const uint8_t *)catalog, (size_t)catalog_len };
    cfg.backpressure = (moq_media_send_backpressure_t)backpressure;
    cfg.block_timeout_us = block_timeout_us;
    cfg.queue_max_objects = qmo;
    cfg.queue_max_bytes = qmb;
    cfg.pre_ready_max_objects = pmo;
    cfg.pre_ready_max_bytes = pmb;
    cfg.validate_cmaf = validate_cmaf ? true : false;
    cfg.publish_tracks = publish_tracks ? true : false;
    cfg.drop_without_demand = drop_without_demand ? true : false;
    cfg.catalog_refresh_interval_us = refresh_us;
    moq_media_sender_t *s = NULL;
    moq_result_t rc = current ? moq_media_sender_attach(current, &cfg, &s)
                              : MOQ_ERR_INVAL;
    free(parts);
    if (!current) {
        PyErr_SetString(PyExc_RuntimeError, "no fixture endpoint is connected");
        return NULL;
    }
    return PyLong_FromLong((long)rc);
}

/* Reads the fixture's real sender accessors, so the fallback rules are
 * observed rather than assumed. */
PyObject *test_sender_snapshot(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    moq_media_sender_t *s = current_sender;
    pthread_mutex_unlock(&mutex);
    if (!s) {
        PyErr_SetString(PyExc_RuntimeError, "no native sender is attached");
        return NULL;
    }
    return Py_BuildValue("{s:O,s:O,s:O,s:K}",
        "ready", moq_media_sender_is_ready(s) ? Py_True : Py_False,
        "closed", moq_media_sender_is_closed(s) ? Py_True : Py_False,
        "fatal", moq_media_sender_is_fatal(s) ? Py_True : Py_False,
        "fatal_code", (unsigned long long)moq_media_sender_fatal_code(s));
}

PyObject *test_sender_simulate_destroy(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    moq_media_sender_t *s = current_sender;
    pthread_mutex_unlock(&mutex);
    if (s) moq_media_sender_destroy(s);
    Py_RETURN_NONE;
}
