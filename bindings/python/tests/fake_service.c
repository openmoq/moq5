#define _POSIX_C_SOURCE 200809L
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <moq/endpoint.h>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h>
#include <pthread.h>
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

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
static moq_endpoint_t *current;
static int created, stopped, destroyed;
static moq_result_t connect_result, stop_result, wait_result = MOQ_DONE;
static bool block_wait, entered, block_stop, stop_entered;
static bool force_watchdog, watchdog_fired;
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
    stopped++;
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
    out->struct_size = terminal_size;
    out->reason = ep->reason;
    out->detail_code = ep->detail;
    return MOQ_OK;
}

moq_result_t moq_endpoint_wait(moq_endpoint_t *ep, uint64_t timeout_us)
{
    pthread_mutex_lock(&mutex);
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
    block_wait = false;
    wait_result = MOQ_OK;
    pthread_cond_broadcast(&condition);
    pthread_mutex_unlock(&mutex);
}

PyObject *test_reset(PyObject *module, PyObject *args)
{
    (void)module; (void)args;
    pthread_mutex_lock(&mutex);
    if (current) {
        pthread_mutex_unlock(&mutex);
        PyErr_SetString(PyExc_RuntimeError, "previous test leaked a native endpoint");
        return NULL;
    }
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
