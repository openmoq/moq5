#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>
#include <moq/version.h>
#include <stddef.h>

/* The track layer needs the sized current-description copy. An SDK without
 * it is refused here, at compile time, rather than falling back to the
 * mutable borrowed descriptor. */
#ifndef MOQ_MEDIA_TRACK_DESC_V0_SIZE
#error "moq5 requires a LibMoQ service SDK that declares moq_media_receiver_track_desc_copy (MOQ_MEDIA_TRACK_DESC_V0_SIZE)"
#endif
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <process.h>
#define process_id _getpid
#else
#include <unistd.h>
#define process_id getpid
#endif

/* Handles never escape the private module through the public Python API.
 * Ordinary calls are owner-thread confined. Wake/interrupt hold the GIL for
 * their short native call; close detaches the pointer before releasing it. */
typedef struct {
    moq_endpoint_t *endpoint;
    uint64_t thread_id;
    long pid;
    bool closing;
} endpoint_handle_t;

typedef struct {
    PyObject *error;          /* an actual returned native result code */
    PyObject *binding_error;  /* malformed native output: never a C code */
    PyObject *event_lost;
    PyObject *object_lost;     /* a dequeued track event the bridge could not convert */
} module_state_t;

static const char handle_name[] = "moq5._native.endpoint";

static PyObject *native_error(PyObject *module, moq_result_t rc,
                              const char *operation)
{
    module_state_t *state = PyModule_GetState(module);
    PyObject *args = Py_BuildValue("iss", rc, operation, moq_strerror(rc));
    if (!args) return NULL;
    PyErr_SetObject(state->error, args);
    Py_DECREF(args);
    return NULL;
}

/* A malformed native OUTPUT (bad stamp, impossible span) is a binding fault
 * with a field-specific message, distinct from any returned result code. */
static PyObject *binding_fault(PyObject *module, const char *field, const char *what,
                               unsigned long long observed, unsigned long long bound)
{
    module_state_t *state = PyModule_GetState(module);
    PyErr_Format(state->binding_error, "%s: stamped struct_size %llu %s %llu",
                 field, observed, what, bound);
    return NULL;
}

static int uint_value(PyObject *object, uint64_t max, const char *name,
                      uint64_t *out)
{
    if (!PyLong_Check(object) || PyBool_Check(object)) {
        PyErr_Format(PyExc_TypeError, "%s must be an integer, not bool", name);
        return -1;
    }
    unsigned long long value = PyLong_AsUnsignedLongLong(object);
    if (PyErr_Occurred()) {
        if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
            PyErr_Clear();
            PyErr_Format(PyExc_ValueError, "%s is out of range", name);
        }
        return -1;
    }
    if (value > max) {
        PyErr_Format(PyExc_ValueError, "%s is out of range", name);
        return -1;
    }
    *out = (uint64_t)value;
    return 0;
}

static int bytes_value(PyObject *object, const char *name, moq_bytes_t *out)
{
    char *data;
    Py_ssize_t size;
    if (!PyBytes_Check(object)) {
        PyErr_Format(PyExc_TypeError, "%s must be immutable bytes", name);
        return -1;
    }
    if (PyBytes_AsStringAndSize(object, &data, &size) < 0) return -1;
    if (memchr(data, '\0', (size_t)size)) {
        PyErr_Format(PyExc_ValueError, "%s contains a NUL byte", name);
        return -1;
    }
    out->data = (const uint8_t *)data;
    out->len = (size_t)size;
    return 0;
}

/* A length-bearing binary span: any byte, including NUL, is legal. */
static int binary_value(PyObject *object, const char *name, moq_bytes_t *out)
{
    char *data;
    Py_ssize_t size;
    if (!PyBytes_Check(object)) {
        PyErr_Format(PyExc_TypeError, "%s must be immutable bytes", name);
        return -1;
    }
    if (PyBytes_AsStringAndSize(object, &data, &size) < 0) return -1;
    out->data = (const uint8_t *)data;
    out->len = (size_t)size;
    return 0;
}

static endpoint_handle_t *handle_get(PyObject *object, int owner, int live)
{
    endpoint_handle_t *handle = PyCapsule_GetPointer(object, handle_name);
    if (!handle) return NULL;
    if (handle->pid != (long)process_id()) {
        PyErr_SetString(PyExc_RuntimeError, "an inherited endpoint cannot be used after fork");
        return NULL;
    }
    if (owner && handle->thread_id != PyThreadState_GetID(PyThreadState_Get())) {
        PyErr_SetString(PyExc_RuntimeError, "endpoint operation requires its creating thread");
        return NULL;
    }
    if (live && !handle->endpoint) {
        /* Released and close-in-progress are different facts: `closed` reads
         * False while a stop/join is still running on the owner thread. */
        PyErr_SetString(PyExc_RuntimeError,
                        handle->closing ? "endpoint close is in progress" : "endpoint is closed");
        return NULL;
    }
    return handle;
}

static moq_result_t handle_close(endpoint_handle_t *handle)
{
    moq_endpoint_t *endpoint = handle->endpoint;
    if (!endpoint) return MOQ_OK;
    handle->closing = true;
    handle->endpoint = NULL;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_endpoint_stop(endpoint);
    if (rc == MOQ_OK) moq_endpoint_destroy(endpoint);
    Py_END_ALLOW_THREADS
    /* A refused stop must not turn into destroy-before-quiescence. */
    if (rc != MOQ_OK) handle->endpoint = endpoint;
    handle->closing = false;
    return rc;
}

/* Runs from capsule_dealloc with the capsule's refcount already zero, and the
 * capsule is freed unconditionally when this returns. Diagnostics raised here
 * -- a refused stop, or ResourceWarning under warnings-as-errors -- are handed
 * to PyErr_WriteUnraisable with a NULL context on purpose: sys.unraisablehook
 * receives its context object as a normal reference, so passing the dying
 * capsule would resurrect it, release it a second time, and free it twice. The
 * message carries the identity instead. The pending exception is preserved. */
static void handle_destroy(PyObject *capsule)
{
    PyObject *pending = PyErr_GetRaisedException();
    endpoint_handle_t *handle = PyCapsule_GetPointer(capsule, handle_name);
    if (!handle) {
        PyErr_WriteUnraisable(NULL);
        PyErr_SetRaisedException(pending);
        return;
    }
    if (handle->endpoint && handle->pid == (long)process_id()) {
        moq_result_t rc = handle_close(handle);
        if (rc != MOQ_OK) {
            PyErr_SetString(PyExc_RuntimeError, "native endpoint stop refused during finalization; not destroyed");
            PyErr_WriteUnraisable(NULL);
        } else if (PyErr_WarnEx(PyExc_ResourceWarning,
                               "unclosed moq5.Endpoint; use close() or a context manager", 1) < 0) {
            PyErr_WriteUnraisable(NULL);
        }
    }
    /* In a fork child, touching inherited native locks is unsafe. The child
     * discards only its Python handle; native resources remain parent-owned. */
    PyMem_Free(handle);
    PyErr_SetRaisedException(pending);
}

static PyObject *connect_endpoint(PyObject *module, PyObject *args)
{
    PyObject *url, *protocol, *backend, *versions, *sni, *ca, *path, *profile, *timeout;
    if (!PyArg_ParseTuple(args, "OOOOOOOOO:connect", &url, &protocol, &backend,
                          &versions, &sni, &ca, &path, &profile, &timeout)) return NULL;
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    uint64_t p, b, w, t;
    if (bytes_value(url, "url", &cfg.url) < 0 ||
        bytes_value(sni, "sni", &cfg.sni) < 0 ||
        bytes_value(ca, "ca_file", &cfg.ca_file) < 0 ||
        bytes_value(path, "wt_path", &cfg.wt_path) < 0 ||
        uint_value(protocol, MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT, "protocol", &p) < 0 ||
        uint_value(backend, MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC, "backend", &b) < 0 ||
        uint_value(profile, MOQ_WT_PROFILE_D13_14_COMPAT, "wt_profile", &w) < 0 ||
        uint_value(timeout, MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US, "handshake_timeout_us", &t) < 0)
        return NULL;
    cfg.protocol = (moq_transport_protocol_t)p;
    cfg.backend = (moq_transport_backend_t)b;
    cfg.wt_profile = (uint32_t)w;
    cfg.handshake_timeout_us = t;
    cfg.insecure_skip_verify = false;
    if (!PyTuple_Check(versions)) {
        PyErr_SetString(PyExc_TypeError, "versions must be a tuple");
        return NULL;
    }
    Py_ssize_t count = PyTuple_Size(versions);
    if (count < 0) return NULL;
    if ((size_t)count > SIZE_MAX / sizeof(moq_version_t)) return PyErr_NoMemory();
    moq_version_t *offers = count ? PyMem_Calloc((size_t)count, sizeof(*offers)) : NULL;
    if (count && !offers) return PyErr_NoMemory();
    for (Py_ssize_t i = 0; i < count; ++i) {
        uint64_t version;
        if (uint_value(PyTuple_GetItem(versions, i), UINT32_MAX, "version", &version) < 0) {
            PyMem_Free(offers);
            return NULL;
        }
        if (!version) {
            PyMem_Free(offers);
            PyErr_SetString(PyExc_ValueError, "version must be nonzero");
            return NULL;
        }
        offers[i] = (moq_version_t)version;
    }
    cfg.versions.struct_size = sizeof(cfg.versions);
    cfg.versions.policy = count == 0 ? MOQ_VERSION_POLICY_AUTO :
                          count == 1 ? MOQ_VERSION_POLICY_EXACT : MOQ_VERSION_POLICY_LIST;
    cfg.versions.versions = offers;
    cfg.versions.version_count = (size_t)count;
    endpoint_handle_t *handle = PyMem_Calloc(1, sizeof(*handle));
    if (!handle) {
        PyMem_Free(offers);
        return PyErr_NoMemory();
    }
    handle->pid = (long)process_id();
    handle->thread_id = PyThreadState_GetID(PyThreadState_Get());
    PyObject *capsule = PyCapsule_New(handle, handle_name, handle_destroy);
    if (!capsule) {
        PyMem_Free(offers);
        PyMem_Free(handle);
        return NULL;
    }
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_endpoint_connect(&cfg, &handle->endpoint);
    Py_END_ALLOW_THREADS
    PyMem_Free(offers);
    if (rc != MOQ_OK) {
        Py_DECREF(capsule);
        return native_error(module, rc, "connect");
    }
    return capsule;
}

static PyObject *close_endpoint(PyObject *module, PyObject *capsule)
{
    endpoint_handle_t *handle = handle_get(capsule, 1, 0);
    if (!handle) return NULL;
    moq_result_t rc = handle_close(handle);
    if (rc != MOQ_OK) return native_error(module, rc, "close");
    Py_RETURN_NONE;
}

static PyObject *closed_endpoint(PyObject *module, PyObject *capsule)
{
    (void)module;
    endpoint_handle_t *handle = handle_get(capsule, 0, 0);
    if (!handle) return NULL;
    return PyBool_FromLong(handle->endpoint == NULL && !handle->closing);
}

static PyObject *state_endpoint(PyObject *module, PyObject *capsule)
{
    (void)module;
    endpoint_handle_t *handle = handle_get(capsule, 1, 1);
    if (!handle) return NULL;
    return PyLong_FromLong(moq_endpoint_state(handle->endpoint));
}

static PyObject *terminal_endpoint(PyObject *module, PyObject *capsule)
{
    endpoint_handle_t *handle = handle_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_endpoint_terminal_t terminal = {0};
    moq_result_t rc = moq_endpoint_get_terminal(handle->endpoint, &terminal, sizeof(terminal));
    if (rc != MOQ_OK) return native_error(module, rc, "terminal");
    if (terminal.struct_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE) {
        return native_error(module, MOQ_ERR_ABI_MISMATCH, "terminal");
    }
    return Py_BuildValue("iK", (int)terminal.reason, (unsigned long long)terminal.detail_code);
}

static PyObject *version_endpoint(PyObject *module, PyObject *capsule)
{
    (void)module;
    endpoint_handle_t *handle = handle_get(capsule, 1, 1);
    if (!handle) return NULL;
    return PyLong_FromUnsignedLong((uint32_t)moq_endpoint_negotiated_version(handle->endpoint));
}

static PyObject *wait_endpoint(PyObject *module, PyObject *args)
{
    PyObject *capsule, *timeout;
    if (!PyArg_ParseTuple(args, "OO:wait", &capsule, &timeout)) return NULL;
    uint64_t micros;
    if (uint_value(timeout, INT64_MAX, "timeout_us", &micros) < 0) return NULL;
    endpoint_handle_t *handle = handle_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_endpoint_wait(handle->endpoint, micros);
    Py_END_ALLOW_THREADS
    if (PyErr_CheckSignals() < 0) return NULL;
    if (rc != MOQ_OK && rc != MOQ_DONE && rc != MOQ_ERR_INTERRUPTED && rc != MOQ_ERR_CLOSED)
        return native_error(module, rc, "wait");
    return PyLong_FromLong(rc);
}

static PyObject *wake_endpoint(PyObject *module, PyObject *capsule)
{
    (void)module;
    endpoint_handle_t *handle = handle_get(capsule, 0, 1);
    if (!handle) return NULL;
    moq_endpoint_wake(handle->endpoint);
    Py_RETURN_NONE;
}

static PyObject *interrupt_endpoint(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *capsule, *interrupted;
    if (!PyArg_ParseTuple(args, "OO:set_interrupted", &capsule, &interrupted)) return NULL;
    if (!PyBool_Check(interrupted)) {
        PyErr_SetString(PyExc_TypeError, "interrupted must be bool");
        return NULL;
    }
    endpoint_handle_t *handle = handle_get(capsule, 0, 1);
    if (!handle) return NULL;
    moq_endpoint_set_interrupted(handle->endpoint, interrupted == Py_True);
    Py_RETURN_NONE;
}

/* -- media receiver ------------------------------------------------------ *
 * A receiver capsule retains its endpoint capsule (a C-level strong reference)
 * from attach until the native receiver is destroyed, so the native endpoint
 * outlives every attachment regardless of Python release order. Ordinary
 * calls are owner-thread confined like the endpoint's. */
typedef struct {
    moq_media_receiver_t *receiver;
    PyObject *endpoint;      /* the endpoint capsule, retained until destroy */
    uint64_t thread_id;
    long pid;
    bool closing;
} receiver_handle_t;

/* A track capsule names one native handle of one receiver capsule; the
 * pointer is never dereferenced by the bridge, only handed back to the
 * service, which validates ownership itself. */
typedef struct {
    moq_media_track_t *track;
    PyObject *receiver;      /* the owning receiver capsule, retained */
} track_handle_t;

static const char receiver_name[] = "moq5._native.receiver";
static const char track_name[] = "moq5._native.track";

static receiver_handle_t *receiver_get(PyObject *object, int owner, int live)
{
    receiver_handle_t *handle = PyCapsule_GetPointer(object, receiver_name);
    if (!handle) return NULL;
    if (handle->pid != (long)process_id()) {
        PyErr_SetString(PyExc_RuntimeError, "an inherited receiver cannot be used after fork");
        return NULL;
    }
    if (owner && handle->thread_id != PyThreadState_GetID(PyThreadState_Get())) {
        PyErr_SetString(PyExc_RuntimeError, "receiver operation requires its creating thread");
        return NULL;
    }
    if (live && !handle->receiver) {
        PyErr_SetString(PyExc_RuntimeError,
                        handle->closing ? "receiver close is in progress" : "receiver is closed");
        return NULL;
    }
    return handle;
}

/* Destroy the native receiver (void: it cannot refuse). The endpoint
 * retention is released by the caller AFTER this returns, so the endpoint is
 * never destroyed under a live attachment. */
static void receiver_destroy_native(receiver_handle_t *handle)
{
    moq_media_receiver_t *receiver = handle->receiver;
    if (!receiver) return;
    handle->closing = true;
    handle->receiver = NULL;
    Py_BEGIN_ALLOW_THREADS
    moq_media_receiver_destroy(receiver);
    Py_END_ALLOW_THREADS
    handle->closing = false;
}

static void receiver_capsule_destroy(PyObject *capsule)
{
    PyObject *pending = PyErr_GetRaisedException();
    receiver_handle_t *handle = PyCapsule_GetPointer(capsule, receiver_name);
    if (!handle) {
        PyErr_WriteUnraisable(NULL);
        PyErr_SetRaisedException(pending);
        return;
    }
    if (handle->receiver && handle->pid == (long)process_id()) {
        receiver_destroy_native(handle);
        /* The receiver's own diagnostic precedes the endpoint release below,
         * so a forgotten pair reports receiver first, endpoint second -- the
         * same order the native resources go. */
        if (PyErr_WarnEx(PyExc_ResourceWarning,
                         "unclosed moq5.Receiver; use close() or a context manager", 1) < 0) {
            PyErr_WriteUnraisable(NULL);
        }
    }
    /* Release the endpoint retention last. A fork child drops only its Python
     * references; the native receiver and its endpoint stay parent-owned (the
     * endpoint capsule's own finalizer has the same PID guard). */
    Py_CLEAR(handle->endpoint);
    PyMem_Free(handle);
    PyErr_SetRaisedException(pending);
}

static PyObject *receiver_attach_py(PyObject *module, PyObject *args)
{
    PyObject *endpoint_capsule, *namespace, *catalog, *auto_subscribe, *time_mode, *policy;
    PyObject *max_objects, *max_bytes, *max_track_events;
    if (!PyArg_ParseTuple(args, "OOOOOOOOO:receiver_attach", &endpoint_capsule, &namespace,
                          &catalog, &auto_subscribe, &time_mode, &policy,
                          &max_objects, &max_bytes, &max_track_events)) return NULL;
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init(&cfg);
    uint64_t tm, pol, mo, mb, me;
    if (!PyBool_Check(auto_subscribe)) {
        PyErr_SetString(PyExc_TypeError, "auto_subscribe must be bool");
        return NULL;
    }
    if (binary_value(catalog, "catalog_track", &cfg.catalog_track) < 0 ||
        uint_value(time_mode, MOQ_MEDIA_TIME_SHARED_EPOCH, "time_mode", &tm) < 0 ||
        uint_value(policy, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, "overflow", &pol) < 0 ||
        uint_value(max_objects, UINT32_MAX, "max_objects", &mo) < 0 ||
        uint_value(max_bytes, UINT64_MAX, "max_bytes", &mb) < 0 ||
        uint_value(max_track_events, UINT32_MAX, "max_track_events", &me) < 0)
        return NULL;
    if (pol == MOQ_MEDIA_OVERFLOW_UNSET) {
        PyErr_SetString(PyExc_ValueError, "overflow is out of range");
        return NULL;
    }
    if (!PyTuple_Check(namespace)) {
        PyErr_SetString(PyExc_TypeError, "namespace must be a tuple");
        return NULL;
    }
    Py_ssize_t count = PyTuple_Size(namespace);
    if (count < 0) return NULL;
    if (count == 0) {
        PyErr_SetString(PyExc_ValueError, "namespace must not be empty");
        return NULL;
    }
    if ((size_t)count > SIZE_MAX / sizeof(moq_bytes_t)) return PyErr_NoMemory();
    moq_bytes_t *parts = PyMem_Calloc((size_t)count, sizeof(*parts));
    if (!parts) return PyErr_NoMemory();
    for (Py_ssize_t i = 0; i < count; ++i) {
        if (binary_value(PyTuple_GetItem(namespace, i), "namespace part", &parts[i]) < 0) {
            PyMem_Free(parts);
            return NULL;
        }
        if (parts[i].len == 0) {
            PyMem_Free(parts);
            PyErr_SetString(PyExc_ValueError, "namespace parts must not be empty");
            return NULL;
        }
    }
    cfg.endpoint = NULL;   /* attach borrows */
    cfg.namespace_.parts = parts;
    cfg.namespace_.count = (size_t)count;
    cfg.auto_subscribe = auto_subscribe == Py_True;
    cfg.time_mode = (moq_media_time_mode_t)tm;
    cfg.overflow.policy = (moq_media_overflow_policy_t)pol;
    cfg.overflow.max_objects = (uint32_t)mo;
    cfg.overflow.max_bytes = mb;
    cfg.max_track_events = (uint32_t)me;

    endpoint_handle_t *ep = handle_get(endpoint_capsule, 1, 1);
    if (!ep) {
        PyMem_Free(parts);
        return NULL;
    }
    receiver_handle_t *handle = PyMem_Calloc(1, sizeof(*handle));
    if (!handle) {
        PyMem_Free(parts);
        return PyErr_NoMemory();
    }
    handle->pid = (long)process_id();
    handle->thread_id = PyThreadState_GetID(PyThreadState_Get());
    handle->endpoint = Py_NewRef(endpoint_capsule);
    PyObject *capsule = PyCapsule_New(handle, receiver_name, receiver_capsule_destroy);
    if (!capsule) {
        PyMem_Free(parts);
        Py_DECREF(handle->endpoint);
        PyMem_Free(handle);
        return NULL;
    }
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_receiver_attach(ep->endpoint, &cfg, &handle->receiver);
    Py_END_ALLOW_THREADS
    PyMem_Free(parts);
    if (rc != MOQ_OK) {
        handle->receiver = NULL;
        Py_DECREF(capsule);   /* releases the endpoint retention: nothing attached */
        return native_error(module, rc, "attach");
    }
    return capsule;
}

static PyObject *receiver_close_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    receiver_handle_t *handle = receiver_get(capsule, 1, 0);
    if (!handle) return NULL;
    receiver_destroy_native(handle);
    Py_CLEAR(handle->endpoint);
    Py_RETURN_NONE;
}

static PyObject *receiver_closed_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    receiver_handle_t *handle = receiver_get(capsule, 0, 0);
    if (!handle) return NULL;
    return PyBool_FromLong(handle->receiver == NULL && !handle->closing);
}

/* Context entry: the owner-thread, same-process and live checks, with no
 * service query. */
static PyObject *receiver_enter_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    if (!receiver_get(capsule, 1, 1)) return NULL;
    Py_RETURN_NONE;
}

static PyObject *receiver_wait_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *timeout;
    if (!PyArg_ParseTuple(args, "OO:receiver_wait", &capsule, &timeout)) return NULL;
    uint64_t micros;
    if (uint_value(timeout, INT64_MAX, "timeout_us", &micros) < 0) return NULL;
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_receiver_wait(handle->receiver, micros);
    Py_END_ALLOW_THREADS
    if (PyErr_CheckSignals() < 0) return NULL;
    if (rc != MOQ_OK && rc != MOQ_DONE && rc != MOQ_ERR_INTERRUPTED && rc != MOQ_ERR_CLOSED)
        return native_error(module, rc, "wait");
    return PyLong_FromLong(rc);
}

/* The stats v0 prefix is frozen at `paused`; the appended fields are read
 * only when the stamped size covers each of them entirely. */
#define STATS_V0_SIZE (offsetof(moq_media_receiver_stats_t, paused) + sizeof(bool))
#define STATS_FIELD_END(field) \
    (offsetof(moq_media_receiver_stats_t, field) + sizeof(((moq_media_receiver_stats_t *)0)->field))

static PyObject *receiver_stats_py(PyObject *module, PyObject *capsule)
{
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_media_receiver_stats_t st;
    memset(&st, 0xEE, sizeof(st));
    moq_result_t rc = moq_media_receiver_get_stats(handle->receiver, &st, sizeof(st));
    if (rc != MOQ_OK) return native_error(module, rc, "stats");
    /* The service returned OK: a stamp outside [v0, sizeof] is malformed
     * output, reported as a binding fault, never as a native result. */
    if (st.struct_size < STATS_V0_SIZE)
        return binding_fault(module, "stats", "below the v0 prefix", st.struct_size, STATS_V0_SIZE);
    if (st.struct_size > sizeof(st))
        return binding_fault(module, "stats", "exceeds sizeof", st.struct_size, sizeof(st));
    PyObject *catalog_drops = Py_None, *catalog_complete = Py_None;
    if (st.struct_size >= STATS_FIELD_END(catalog_drops))
        catalog_drops = PyLong_FromUnsignedLongLong(st.catalog_drops);
    else
        Py_INCREF(catalog_drops);
    if (!catalog_drops) return NULL;
    if (st.struct_size >= STATS_FIELD_END(catalog_complete))
        catalog_complete = PyBool_FromLong(st.catalog_complete);
    else
        Py_INCREF(catalog_complete);
    if (!catalog_complete) {
        Py_DECREF(catalog_drops);
        return NULL;
    }
    PyObject *result = Py_BuildValue("(KKKKKKKKKOOO)",
        (unsigned long long)st.objects_received, (unsigned long long)st.objects_queued,
        (unsigned long long)st.bytes_queued, (unsigned long long)st.objects_dropped,
        (unsigned long long)st.groups_dropped, (unsigned long long)st.keyframes_dropped,
        (unsigned long long)st.parse_drops, (unsigned long long)st.overflow_events,
        (unsigned long long)st.pause_transitions, st.paused ? Py_True : Py_False,
        catalog_drops, catalog_complete);
    Py_DECREF(catalog_drops);
    Py_DECREF(catalog_complete);
    return result;
}

static PyObject *receiver_terminal_py(PyObject *module, PyObject *capsule)
{
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    endpoint_handle_t *ep = handle_get(handle->endpoint, 0, 1);
    if (!ep) return NULL;
    bool closed = moq_media_receiver_is_closed(handle->receiver);
    bool fatal = moq_media_receiver_is_fatal(handle->receiver);
    uint64_t code = moq_media_receiver_fatal_code(handle->receiver);
    moq_endpoint_terminal_t terminal = {0};
    moq_result_t rc = moq_endpoint_get_terminal(ep->endpoint, &terminal, sizeof(terminal));
    if (rc != MOQ_OK) return native_error(module, rc, "terminal");
    if (terminal.struct_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE)
        return binding_fault(module, "terminal", "below the v0 prefix",
                             terminal.struct_size, MOQ_ENDPOINT_TERMINAL_V0_SIZE);
    return Py_BuildValue("(OOKiK)", closed ? Py_True : Py_False, fatal ? Py_True : Py_False,
                         (unsigned long long)code, (int)terminal.reason,
                         (unsigned long long)terminal.detail_code);
}

/* A track operation needs a live owner receiver and a track that names THAT
 * receiver's capsule; a foreign or closed pairing is refused before the
 * service is called. */
static track_handle_t *track_get(PyObject *object, receiver_handle_t *owner, PyObject *owner_capsule)
{
    track_handle_t *track = PyCapsule_GetPointer(object, track_name);
    if (!track) return NULL;
    (void)owner;
    if (track->receiver != owner_capsule) {
        PyErr_SetString(PyExc_RuntimeError, "track belongs to another receiver");
        return NULL;
    }
    return track;
}

/* Track capsules are minted from a native handle the service published
 * (poll_track, or the test provider). */
static void track_capsule_destroy(PyObject *capsule)
{
    track_handle_t *track = PyCapsule_GetPointer(capsule, track_name);
    if (!track) {
        PyErr_WriteUnraisable(NULL);
        return;
    }
    Py_CLEAR(track->receiver);
    PyMem_Free(track);
}

static PyObject *track_capsule_new(PyObject *receiver_capsule, moq_media_track_t *native)
{
    track_handle_t *track = PyMem_Calloc(1, sizeof(*track));
    if (!track) return PyErr_NoMemory();
    track->track = native;
    track->receiver = Py_NewRef(receiver_capsule);
    PyObject *capsule = PyCapsule_New(track, track_name, track_capsule_destroy);
    if (!capsule) {
        Py_DECREF(track->receiver);
        PyMem_Free(track);
        return NULL;
    }
    return capsule;
}

static PyObject *track_key_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    track_handle_t *track = PyCapsule_GetPointer(capsule, track_name);
    if (!track) return NULL;
    /* An identity key for the wrapper's cache; never a dereferenced address. */
    return PyLong_FromVoidPtr(track->track);
}

static PyObject *receiver_subscribe_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *track_capsule, *start, *priority;
    if (!PyArg_ParseTuple(args, "OOOO:receiver_subscribe", &capsule, &track_capsule, &start, &priority))
        return NULL;
    uint64_t start_value, priority_value = 0;
    if (uint_value(start, MOQ_MEDIA_START_NEXT_GROUP, "start", &start_value) < 0) return NULL;
    if (priority != Py_None && uint_value(priority, UINT8_MAX, "priority", &priority_value) < 0) return NULL;
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    track_handle_t *track = track_get(track_capsule, handle, capsule);
    if (!track) return NULL;
    moq_media_receiver_track_subscribe_cfg_t cfg;
    moq_media_receiver_track_subscribe_cfg_init(&cfg);
    cfg.start = (moq_media_start_mode_t)start_value;
    cfg.has_priority = priority != Py_None;
    cfg.priority = (uint8_t)priority_value;
    moq_result_t rc = moq_media_receiver_subscribe_track(handle->receiver, track->track, &cfg);
    if (rc != MOQ_OK) return native_error(module, rc, "subscribe");
    Py_RETURN_NONE;
}

static PyObject *receiver_unsubscribe_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:receiver_unsubscribe", &capsule, &track_capsule)) return NULL;
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    track_handle_t *track = track_get(track_capsule, handle, capsule);
    if (!track) return NULL;
    moq_result_t rc = moq_media_receiver_unsubscribe_track(handle->receiver, track->track);
    if (rc != MOQ_OK) return native_error(module, rc, "unsubscribe");
    Py_RETURN_NONE;
}

static PyObject *receiver_track_state_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:receiver_track_state", &capsule, &track_capsule)) return NULL;
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    track_handle_t *track = track_get(track_capsule, handle, capsule);
    if (!track) return NULL;
    moq_media_track_state_t state = (moq_media_track_state_t)-1;
    moq_result_t rc = moq_media_receiver_track_state(handle->receiver, track->track, &state);
    if (rc != MOQ_OK) return native_error(module, rc, "track_state");
    return PyLong_FromLong((long)state);
}

/* -- track events ---------------------------------------------------------- *
 * A dequeued event is CONSUMED by the native poll. Conversion completes in
 * full before anything is returned; a malformed native output raises
 * EventLost carrying only the already-validated kind and the failing stage
 * (never an address); an allocation failure propagates the bare MemoryError.
 * Every field is gated by its compiled end within its own stamped prefix:
 * the event's, the description's, and the nested info/init ones. Optional
 * values the prefix does not cover are None, never fabricated. */
#define FIELD_END(type, field) (offsetof(type, field) + sizeof(((type *)0)->field))
/* The event's frozen v0 prefix ends at config_generation (the appended blocks
 * start at the old sizeof, after four reserved bytes). */
#define EVENT_V0_SIZE (offsetof(moq_media_track_event_t, config_generation) + sizeof(uint32_t))
#define COVERS(stamp, type, field) ((size_t)(stamp) >= FIELD_END(type, field))

static PyObject *event_lost(PyObject *module, PyObject *kind, const char *stage)
{
    module_state_t *state = PyModule_GetState(module);
    PyObject *args = Py_BuildValue("(Os)", kind ? kind : Py_None, stage);
    if (!args) return NULL;
    PyErr_SetObject(state->event_lost, args);
    Py_DECREF(args);
    return NULL;
}

#ifdef MOQ_PYTHON_TESTING
/* Test-only: a countdown that makes the Nth gated Python allocation in the
 * bridge fail with MemoryError, so the native allocation-failure paths are
 * exercised deterministically. Never compiled into the production module. */
static long g_fail_allocation_after = -1;
PyObject *test_fail_allocation_after(PyObject *module, PyObject *value)
{
    (void)module;
    long n = PyLong_AsLong(value);
    if (PyErr_Occurred()) return NULL;
    g_fail_allocation_after = n;
    Py_RETURN_NONE;
}
#define ALLOCATION_GATE() \
    do { if (g_fail_allocation_after >= 0 && g_fail_allocation_after-- == 0) return PyErr_NoMemory(); } while (0)

/* Test-only acquisition-site witnesses for the object path: one NAMED site
 * fails once with MemoryError, and the last stage the most recent object
 * conversion completed is readable, so a test can pin what already existed
 * when a failure struck instead of guessing an allocation count. */
/* Implemented by the private test fixture: it records the ownership inventory
 * at the instant a named fault fires. Declared here because this block sits
 * above the fixture header's inclusion, and absent entirely from a production
 * build, where site_fails() is a constant zero. */
void moq5_test_note_fault_site(const char *name);
/* The fixture's per-span observing allocator, used ONLY in a testing build so
 * the declared acquisition sites are the real ones. */
const moq_alloc_t *moq5_test_counting_alloc_for(int span);

static char g_fail_site[64];
static bool g_fail_site_armed;
/* Which armed site actually consumed its fault, so a test can require that the
 * failure it observed came from the stage it named rather than an earlier one. */
static char g_fail_site_fired[64];
static const char *g_object_progress = "none";
PyObject *test_allocation_site_fired(PyObject *module, PyObject *value)
{
    (void)module;
    Py_ssize_t n;
    const char *name = PyUnicode_AsUTF8AndSize(value, &n);
    if (!name) return NULL;
    return PyBool_FromLong(strcmp(g_fail_site_fired, name) == 0);
}
PyObject *test_fail_allocation_at(PyObject *module, PyObject *value)
{
    (void)module;
    if (value == Py_None) {
        g_fail_site_armed = false;   /* the fired witness survives disarming */
        Py_RETURN_NONE;
    }
    Py_ssize_t n;
    const char *name = PyUnicode_AsUTF8AndSize(value, &n);
    if (!name) return NULL;
    if ((size_t)n >= sizeof(g_fail_site)) {
        PyErr_SetString(PyExc_ValueError, "site name too long");
        return NULL;
    }
    memcpy(g_fail_site, name, (size_t)n + 1);
    g_fail_site_armed = true;
    g_fail_site_fired[0] = '\0';
    Py_RETURN_NONE;
}
PyObject *test_object_progress(PyObject *module, PyObject *unused)
{
    (void)module; (void)unused;
    return PyUnicode_FromString(g_object_progress);
}
static int site_fails(const char *name)
{
    if (!g_fail_site_armed || strcmp(g_fail_site, name) != 0) return 0;
    g_fail_site_armed = false;
    snprintf(g_fail_site_fired, sizeof(g_fail_site_fired), "%s", name);
    moq5_test_note_fault_site(name);
    PyErr_NoMemory();
    return 1;
}
/* The fixture's own drivers consume an armed fault through this, so the
 * ordering observer can be exercised without a bridge. */
int moq5_test_site_fails(const char *name)
{
    return site_fails(name);
}
#define PROGRESS(name) (g_object_progress = (name))
#else
#define ALLOCATION_GATE() ((void)0)
#define site_fails(name) 0
#define PROGRESS(name) ((void)0)
#endif

/* Copy a span into owned bytes. A nonzero length with a NULL pointer, or a
 * length beyond Py_ssize_t, is malformed output (NULL result with the
 * event_lost exception set). `optional` turns an empty span into None. */
static PyObject *span_copy(PyObject *module, PyObject *kind, const char *stage,
                           moq_bytes_t span, int optional)
{
    ALLOCATION_GATE();
    if (span.len == 0) {
        if (optional) Py_RETURN_NONE;
        return PyBytes_FromStringAndSize("", 0);
    }
    if (!span.data || span.len > (size_t)PY_SSIZE_T_MAX) return event_lost(module, kind, stage);
    return PyBytes_FromStringAndSize((const char *)span.data, (Py_ssize_t)span.len);
}

static PyObject *span_tuple(PyObject *module, PyObject *kind, const char *stage,
                            const moq_bytes_t *spans, size_t count)
{
    ALLOCATION_GATE();
    if (count == 0) return PyTuple_New(0);
    /* An array whose byte size cannot exist, or that Python cannot index, is
     * malformed native output for THIS field -- refused by arithmetic before
     * any allocation, never reported as an allocation failure. */
    if (!spans || count > SIZE_MAX / sizeof(moq_bytes_t) || count > (size_t)PY_SSIZE_T_MAX)
        return event_lost(module, kind, stage);
    PyObject *tuple = PyTuple_New((Py_ssize_t)count);
    if (!tuple) return NULL;
    for (size_t i = 0; i < count; ++i) {
        PyObject *item = span_copy(module, kind, stage, spans[i], 0);
        if (!item) {
            Py_DECREF(tuple);
            return NULL;
        }
        if (PyTuple_SetItem(tuple, (Py_ssize_t)i, item) < 0) {
            Py_DECREF(tuple);
            return NULL;
        }
    }
    return tuple;
}

/* dict helpers: each returns -1 with an exception set (MemoryError or the
 * event_lost already raised) */
static int put(PyObject *dict, const char *key, PyObject *value)
{
    if (!value) return -1;
    int rc = PyDict_SetItemString(dict, key, value);
    Py_DECREF(value);
    return rc;
}
static PyObject *opt_u64(int present, uint64_t value)
{
    if (!present) Py_RETURN_NONE;
    return PyLong_FromUnsignedLongLong(value);
}
static PyObject *opt_enum(uint64_t value)
{
    /* zero is "unknown/underived" for the structured enums: reported as None */
    if (value == 0) Py_RETURN_NONE;
    return PyLong_FromUnsignedLongLong(value);
}

static PyObject *init_dict(PyObject *module, PyObject *kind, const moq_cmaf_init_info_t *init)
{
    /* No documented floor: the stamp must cover at least struct_size and no
     * more than sizeof; every field is gated on its own compiled end. */
    uint32_t stamp = init->struct_size;
    if (stamp < sizeof(init->struct_size) || stamp > sizeof(*init))
        return event_lost(module, kind, "description.init.struct_size");
    ALLOCATION_GATE();
    PyObject *d = PyDict_New();
    if (!d) return NULL;
#define INIT_FIELD(key, field, conv) \
    if (put(d, key, COVERS(stamp, moq_cmaf_init_info_t, field) ? (conv) : Py_NewRef(Py_None)) < 0) goto fail
    INIT_FIELD("codec_kind", codec_kind, PyLong_FromLong((long)init->codec_kind));
    INIT_FIELD("timescale", timescale, PyLong_FromUnsignedLong(init->timescale));
    INIT_FIELD("width", width, PyLong_FromUnsignedLong(init->width));
    INIT_FIELD("height", height, PyLong_FromUnsignedLong(init->height));
    INIT_FIELD("samplerate", samplerate, PyLong_FromUnsignedLong(init->samplerate));
    INIT_FIELD("channel_count", channel_count, PyLong_FromUnsignedLong(init->channel_count));
    INIT_FIELD("codec_config", codec_config, span_copy(module, kind, "description.init.codec_config", init->codec_config, 0));
    INIT_FIELD("track_id", track_id, PyLong_FromUnsignedLong(init->track_id));
#undef INIT_FIELD
    if (COVERS(stamp, moq_cmaf_init_info_t, has_cenc) && init->has_cenc) {
        /* CENC is PRESENT once has_cenc fits and is true; each of its fields
         * is gated on its own end, so a later absent field never discards an
         * earlier present one. */
        PyObject *scheme = COVERS(stamp, moq_cmaf_init_info_t, scheme)
                               ? span_copy(module, kind, "description.init.scheme", init->scheme, 0)
                               : Py_NewRef(Py_None);
        if (!scheme) goto fail;
        PyObject *protected_ = COVERS(stamp, moq_cmaf_init_info_t, default_is_protected)
                                   ? PyLong_FromLong((long)init->default_is_protected) : Py_NewRef(Py_None);
        if (!protected_) { Py_DECREF(scheme); goto fail; }
        PyObject *iv = COVERS(stamp, moq_cmaf_init_info_t, default_per_sample_iv_size)
                           ? PyLong_FromLong((long)init->default_per_sample_iv_size) : Py_NewRef(Py_None);
        if (!iv) { Py_DECREF(scheme); Py_DECREF(protected_); goto fail; }
        PyObject *kid = COVERS(stamp, moq_cmaf_init_info_t, default_kid)
                            ? span_copy(module, kind, "description.init.default_kid", init->default_kid, 0)
                            : Py_NewRef(Py_None);
        if (!kid) { Py_DECREF(scheme); Py_DECREF(protected_); Py_DECREF(iv); goto fail; }
        PyObject *cenc = Py_BuildValue("(OOOO)", scheme, protected_, iv, kid);
        Py_DECREF(scheme);
        Py_DECREF(protected_);
        Py_DECREF(iv);
        Py_DECREF(kid);
        if (put(d, "cenc", cenc) < 0) goto fail;
    } else if (put(d, "cenc", Py_NewRef(Py_None)) < 0) {
        goto fail;
    }
    return d;
fail:
    Py_DECREF(d);
    return NULL;
}

static PyObject *description_dict(PyObject *module, PyObject *kind, const moq_media_track_desc_t *desc)
{
    uint32_t stamp = desc->struct_size;
    if (stamp < MOQ_MEDIA_TRACK_DESC_V0_SIZE || stamp > sizeof(*desc))
        return event_lost(module, kind, "description.struct_size");
    ALLOCATION_GATE();
    PyObject *d = PyDict_New();
    if (!d) return NULL;
#define SPAN(key, field, optional) \
    if (put(d, key, span_copy(module, kind, "description." key, desc->field, optional)) < 0) goto fail
#define OPT(key, has, field) \
    if (put(d, key, opt_u64(desc->has, desc->field)) < 0) goto fail
    /* the frozen v0 prefix covers every field through is_live */
    SPAN("name", name, 0);
    SPAN("role", role, 1);
    SPAN("codec", codec, 1);
    SPAN("lang", lang, 1);
    SPAN("label", label, 1);
    /* nested info: its own stamp gates transport_version */
    uint32_t info_stamp = desc->info.struct_size;
    if (info_stamp < MOQ_MEDIA_TRACK_INFO_V0_SIZE || info_stamp > sizeof(desc->info))
        { event_lost(module, kind, "description.info.struct_size"); goto fail; }
    if (put(d, "media_type", opt_enum(desc->info.media_type)) < 0) goto fail;
    if (put(d, "packaging", opt_enum(desc->info.packaging)) < 0) goto fail;
    if (put(d, "timescale", opt_enum(desc->info.timescale)) < 0) goto fail;
    if (put(d, "transport_version",
            COVERS(info_stamp, moq_media_track_info_t, transport_version)
                ? opt_enum(desc->info.transport_version) : Py_NewRef(Py_None)) < 0) goto fail;
    if (desc->has_init) {
        if (put(d, "init", init_dict(module, kind, &desc->init)) < 0) goto fail;
    } else if (put(d, "init", Py_NewRef(Py_None)) < 0) {
        goto fail;
    }
    if (site_fails("description.init_data")) goto fail;
    SPAN("init_data", init_data, 0);
    OPT("width", has_width, width);
    OPT("height", has_height, height);
    OPT("samplerate", has_samplerate, samplerate);
    SPAN("channel_config", channel_config, 1);
    OPT("framerate_millis", has_framerate, framerate_millis);
    OPT("bitrate", has_bitrate, bitrate);
    OPT("max_grp_sap", has_max_grp_sap, max_grp_sap);
    OPT("max_obj_sap", has_max_obj_sap, max_obj_sap);
    if (put(d, "content_protection_ref_ids",
            span_tuple(module, kind, "description.content_protection_ref_ids",
                       desc->content_protection_ref_ids, desc->content_protection_ref_id_count)) < 0) goto fail;
    SPAN("packaging_text", packaging, 0);
    SPAN("event_type", event_type, 1);
    SPAN("mime_type", mime_type, 1);
    if (put(d, "depends", span_tuple(module, kind, "description.depends", desc->depends, desc->depends_count)) < 0) goto fail;
    if (desc->has_template) {
        const moq_msf_media_template_t *t = &desc->template_;
        if (put(d, "template", Py_BuildValue("(KKKKKKKK)",
                (unsigned long long)t->start_media_ms, (unsigned long long)t->delta_media_ms,
                (unsigned long long)t->start_group, (unsigned long long)t->start_object,
                (unsigned long long)t->delta_group, (unsigned long long)t->delta_object,
                (unsigned long long)t->start_wallclock_ms, (unsigned long long)t->delta_wallclock_ms)) < 0) goto fail;
    } else if (put(d, "template", Py_NewRef(Py_None)) < 0) {
        goto fail;
    }
    if (put(d, "is_live", PyBool_FromLong(desc->is_live)) < 0) goto fail;
    OPT("track_duration_ms", has_track_duration, track_duration_ms);
#undef SPAN
#undef OPT
    return d;
fail:
    Py_DECREF(d);
    return NULL;
}

static PyObject *receiver_poll_track_py(PyObject *module, PyObject *capsule)
{
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_media_track_event_t ev;
    memset(&ev, 0xEE, sizeof(ev));
    moq_result_t rc = moq_media_receiver_poll_track(handle->receiver, &ev, sizeof(ev));
    if (rc == MOQ_DONE || rc == MOQ_ERR_CLOSED) return PyLong_FromLong(rc);
    if (rc != MOQ_OK) return native_error(module, rc, "poll_track");

    /* From here the event is consumed. */
    if (ev.struct_size < EVENT_V0_SIZE || ev.struct_size > sizeof(ev))
        return event_lost(module, NULL, "event.struct_size");
    PyObject *kind = PyLong_FromLong((long)ev.kind);
    if (!kind) return NULL;
    PyObject *track = NULL, *desc = NULL, *largest = NULL, *expires = NULL, *drop = NULL, *result = NULL;

    /* Shape by KNOWN kind: CATALOG_READY carries no handle; the known
     * track-scoped kinds require one; an unknown kind is preserved either
     * way, its description copied when a handle is present. */
    bool track_scoped, global;
    switch (ev.kind) {
    case MOQ_MEDIA_TRACK_ADDED: case MOQ_MEDIA_TRACK_UPDATED: case MOQ_MEDIA_TRACK_REMOVED:
    case MOQ_MEDIA_TRACK_ENDED: case MOQ_MEDIA_TRACK_UPDATE_OK: case MOQ_MEDIA_TRACK_PARSE_DROP:
        track_scoped = true; global = false; break;
    case MOQ_MEDIA_CATALOG_READY:
        track_scoped = false; global = true; break;
    default:
        track_scoped = false; global = false; break;
    }
    if ((track_scoped && !ev.track) || (global && ev.track)) {
        event_lost(module, kind, "track");
        goto out;
    }
    /* A global event must carry no descriptor either: the pointer is tested
     * for absence only, never dereferenced. */
    if (global && ev.desc) {
        event_lost(module, kind, "description");
        goto out;
    }
    if (ev.track) {
        moq_media_track_desc_t current;
        memset(&current, 0xEE, sizeof(current));
        /* The CURRENT description, under the receiver mutex: never the
         * mutable borrowed ev.desc. */
        moq_result_t crc = moq_media_receiver_track_desc_copy(handle->receiver, ev.track,
                                                              &current, sizeof(current));
        if (crc != MOQ_OK) {
            event_lost(module, kind, "description.copy");
            goto out;
        }
        desc = description_dict(module, kind, &current);
        if (!desc) goto out;
        track = track_capsule_new(capsule, ev.track);
        if (!track) goto out;
    } else {
        track = Py_NewRef(Py_None);
        desc = Py_NewRef(Py_None);
    }

    if (ev.kind == MOQ_MEDIA_TRACK_UPDATE_OK &&
        COVERS(ev.struct_size, moq_media_track_event_t, has_largest) && ev.has_largest &&
        COVERS(ev.struct_size, moq_media_track_event_t, largest_object)) {
        largest = Py_BuildValue("(KK)", (unsigned long long)ev.largest_group, (unsigned long long)ev.largest_object);
    } else {
        largest = Py_NewRef(Py_None);
    }
    if (!largest) goto out;
    if (ev.kind == MOQ_MEDIA_TRACK_UPDATE_OK &&
        COVERS(ev.struct_size, moq_media_track_event_t, has_expires) && ev.has_expires &&
        COVERS(ev.struct_size, moq_media_track_event_t, expires_ms)) {
        expires = PyLong_FromUnsignedLongLong(ev.expires_ms);
    } else {
        expires = Py_NewRef(Py_None);
    }
    if (!expires) goto out;
    if (ev.kind == MOQ_MEDIA_TRACK_PARSE_DROP &&
        COVERS(ev.struct_size, moq_media_track_event_t, parse_drop_class)) {
        PyObject *total = COVERS(ev.struct_size, moq_media_track_event_t, parse_drops_total)
                              ? PyLong_FromUnsignedLongLong(ev.parse_drops_total) : Py_NewRef(Py_None);
        if (!total) goto out;
        PyObject *delta = COVERS(ev.struct_size, moq_media_track_event_t, parse_drops_delta)
                              ? PyLong_FromUnsignedLongLong(ev.parse_drops_delta) : Py_NewRef(Py_None);
        if (!delta) {
            Py_DECREF(total);
            goto out;
        }
        drop = Py_BuildValue("(iOO)", (int)ev.parse_drop_class, total, delta);
        Py_DECREF(total);
        Py_DECREF(delta);
    } else {
        drop = Py_NewRef(Py_None);
    }
    if (!drop) goto out;
    result = Py_BuildValue("(OOOOOO)", kind, track, desc, largest, expires, drop);
out:
    Py_XDECREF(kind);
    Py_XDECREF(track);
    Py_XDECREF(desc);
    Py_XDECREF(largest);
    Py_XDECREF(expires);
    Py_XDECREF(drop);
    return result;
}

/* -- owned media objects ------------------------------------------------ *
 * A successful poll transfers exclusive ownership of the object's buffers;
 * every byte and sample record is copied into Python-owned values, the
 * transferred buffers are released exactly once on this (the polling)
 * thread, and only then does anything return to Python. The ownership refs
 * live in the frozen v0 prefix, so the local object is zero-initialized
 * (absent, never poison) before the poll. */
#define OBJECT_V0_SIZE (offsetof(moq_media_object_t, samples_owned) + sizeof(moq_cmaf_sample_t *))

/* ObjectLost(stage, presentation_time_us, status, packaging): the consumed
 * object could not be converted. Scalars are carried only once they were
 * read (validated) before the failing stage; never a handle or address. */
static PyObject *object_lost(PyObject *module, const char *stage, const moq_media_object_t *o, bool scalars)
{
    module_state_t *state = PyModule_GetState(module);
    PyObject *args = scalars
        ? Py_BuildValue("(sKII)", stage, (unsigned long long)o->presentation_time_us,
                        (unsigned)o->status, (unsigned)o->packaging)
        : Py_BuildValue("(sOOO)", stage, Py_None, Py_None, Py_None);
    if (!args) return NULL;
    PyErr_SetObject(state->object_lost, args);
    Py_DECREF(args);
    return NULL;
}

/* Release the transferred buffers without disturbing a pending exception. */
static void object_release(moq_media_object_t *o)
{
    PyObject *pending = PyErr_GetRaisedException();
    moq_media_object_cleanup(o);
    PyErr_SetRaisedException(pending);
}

/* Validate every span, array and bound BEFORE any read or allocation that
 * depends on it, then copy. Returns the owned result tuple, or NULL with
 * ObjectLost / the original MemoryError set. */
static PyObject *object_convert(PyObject *module, PyObject *capsule, receiver_handle_t *handle,
                                const moq_media_object_t *o)
{
    module_state_t *state = PyModule_GetState(module);
    if (o->struct_size < OBJECT_V0_SIZE || o->struct_size > sizeof(*o))
        return object_lost(module, "struct_size", o, false);
    /* The scalars are plain values: reading them validates them, and every
     * later stage may cite them. Status, never byte length, decides whether
     * an object is media: NORMAL carries a payload ref (zero-length media is
     * legal); END_OF_GROUP / END_OF_TRACK / unknown statuses carry none. */
    bool status_only = o->status != MOQ_OBJECT_NORMAL;
    if (status_only && (o->payload_ref || o->payload.len || o->fragment.len))
        return object_lost(module, "status", o, true);
    if (!status_only && !o->payload_ref)
        return object_lost(module, "payload", o, true);
    if ((o->payload.len && !o->payload.data) || o->payload.len > (size_t)PY_SSIZE_T_MAX)
        return object_lost(module, "payload", o, true);
    if ((o->fragment.len && !o->fragment.data) || o->fragment.len > (size_t)PY_SSIZE_T_MAX)
        return object_lost(module, "fragment", o, true);
    if ((o->sample_count && !o->samples) || o->sample_count > SIZE_MAX / sizeof(moq_cmaf_sample_t) ||
        o->sample_count > (size_t)PY_SSIZE_T_MAX)
        return object_lost(module, "samples", o, true);
    if (o->mdat_offset > o->fragment.len)
        return object_lost(module, "bounds:mdat_offset", o, true);
    if (o->mdat_len > o->fragment.len - o->mdat_offset)
        return object_lost(module, "bounds:mdat_len", o, true);
    if (!o->track)
        return object_lost(module, "track", o, true);
    PROGRESS("validated");

    PyObject *payload = NULL, *fragment = NULL, *samples = NULL, *desc = NULL, *track = NULL;
    PyObject *capture = NULL, *result = NULL;
    if (site_fails("object.payload")) goto out;
    payload = PyBytes_FromStringAndSize(o->payload.len ? (const char *)o->payload.data : "",
                                        (Py_ssize_t)o->payload.len);
    if (!payload) goto out;
    PROGRESS("payload");
    if (site_fails("object.fragment")) goto out;
    fragment = PyBytes_FromStringAndSize(o->fragment.len ? (const char *)o->fragment.data : "",
                                         (Py_ssize_t)o->fragment.len);
    if (!fragment) goto out;
    PROGRESS("fragment");
    samples = PyTuple_New((Py_ssize_t)o->sample_count);
    if (!samples) goto out;
    for (size_t i = 0; i < o->sample_count; ++i) {
        if (site_fails("object.samples")) goto out;
        const moq_cmaf_sample_t *sample = &o->samples[i];
        /* three unsigned 32-bit fields and a SIGNED composition offset, in
         * timescale ticks: the widths and signs are the C ones */
        PyObject *item = Py_BuildValue("(IIIi)", (unsigned)sample->duration, (unsigned)sample->size,
                                       (unsigned)sample->flags, (int)sample->composition_offset);
        if (!item || PyTuple_SetItem(samples, (Py_ssize_t)i, item) < 0) goto out;
    }
    PROGRESS("samples");
    /* The CURRENT description, under the receiver mutex: the same route and
     * the same handle identity as a track event's Track. */
    moq_media_track_desc_t current;
    memset(&current, 0xEE, sizeof(current));
    moq_result_t crc = moq_media_receiver_track_desc_copy(handle->receiver, o->track, &current, sizeof(current));
    if (crc != MOQ_OK) {
        object_lost(module, "track.description", o, true);
        goto out;
    }
    desc = description_dict(module, NULL, &current);
    if (!desc) {
        /* a malformed description copy is a loss of THIS object; an
         * allocation failure stays the original MemoryError */
        if (PyErr_ExceptionMatches(state->event_lost)) {
            PyErr_Clear();
            object_lost(module, "track.description", o, true);
        }
        goto out;
    }
    PROGRESS("description");
    track = track_capsule_new(capsule, o->track);
    if (!track) goto out;
    capture = opt_u64(o->has_capture_time, o->capture_time_us);
    if (!capture) goto out;
    result = Py_BuildValue("(OOIIIOOOOKLKOOnnO)", track, desc,
                           (unsigned)o->config_generation, (unsigned)o->packaging, (unsigned)o->status,
                           o->end_of_group ? Py_True : Py_False, o->datagram ? Py_True : Py_False,
                           o->keyframe ? Py_True : Py_False, capture,
                           (unsigned long long)o->decode_time_us, (long long)o->composition_offset_us,
                           (unsigned long long)o->presentation_time_us, payload, fragment,
                           (Py_ssize_t)o->mdat_offset, (Py_ssize_t)o->mdat_len, samples);
    if (result) PROGRESS("converted");
out:
    Py_XDECREF(payload);
    Py_XDECREF(fragment);
    Py_XDECREF(samples);
    Py_XDECREF(desc);
    Py_XDECREF(track);
    Py_XDECREF(capture);
    return result;
}

static PyObject *receiver_poll_object_py(PyObject *module, PyObject *capsule)
{
    /* owner thread, same process, live: all before anything is consumed */
    receiver_handle_t *handle = receiver_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_media_object_t obj;
    memset(&obj, 0, sizeof(obj));
    moq_result_t rc = moq_media_receiver_poll_object(handle->receiver, &obj, sizeof(obj));
    if (rc == MOQ_DONE || rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_INTERRUPTED) return PyLong_FromLong(rc);
    if (rc != MOQ_OK) return native_error(module, rc, "poll_object");
    /* From here the object is consumed: exactly one cleanup, on this thread,
     * whatever the conversion outcome, before anything returns. */
    PROGRESS("dequeued");
    PyObject *result = object_convert(module, capsule, handle, &obj);
    object_release(&obj);
    return result;
}

#ifdef MOQ_PYTHON_TESTING
/* Test-provider seams: the fixture mints track handles for a receiver it
 * owns and reads back the native pointers the capsules carry. */
moq_media_receiver_t *moq5_test_receiver(PyObject *capsule)
{
    receiver_handle_t *handle = receiver_get(capsule, 0, 1);
    return handle ? handle->receiver : NULL;
}
moq_media_track_t *moq5_test_track(PyObject *capsule)
{
    track_handle_t *track = PyCapsule_GetPointer(capsule, track_name);
    return track ? track->track : NULL;
}
PyObject *moq5_test_track_capsule(PyObject *receiver_capsule, moq_media_track_t *native)
{
    if (!receiver_get(receiver_capsule, 0, 1)) return NULL;
    return track_capsule_new(receiver_capsule, native);
}
#endif

/* -- media sender -------------------------------------------------------- *
 * A sender capsule retains its endpoint capsule (a C-level strong reference)
 * from attach until the native sender is destroyed, so the native endpoint
 * outlives every attachment regardless of Python release order. Ordinary
 * calls are owner-thread confined like the receiver's. There is no track
 * registry: this shell owns configuration, attachment and lifetime only. */
typedef struct {
    moq_media_sender_t *sender;
    PyObject *endpoint;      /* the endpoint capsule, retained until destroy */
    uint64_t thread_id;
    long pid;
    bool closing;
    bool activated;          /* the native attach has been attempted */
    /* An accepted completion request marked every track the sender then had
     * removed. That is a synchronous native fact about REMOVAL, recorded here
     * so a track can report it without a registry; it is not wire completion,
     * and it gates nothing. */
    bool completion_acknowledged;
} sender_handle_t;

static const char sender_name[] = "moq5._native.sender";

static sender_handle_t *sender_get(PyObject *object, int owner, int live)
{
    sender_handle_t *handle = PyCapsule_GetPointer(object, sender_name);
    if (!handle) return NULL;
    if (handle->pid != (long)process_id()) {
        PyErr_SetString(PyExc_RuntimeError, "an inherited sender cannot be used after fork");
        return NULL;
    }
    if (owner && handle->thread_id != PyThreadState_GetID(PyThreadState_Get())) {
        PyErr_SetString(PyExc_RuntimeError, "sender operation requires its creating thread");
        return NULL;
    }
    if (live && !handle->sender) {
        PyErr_SetString(PyExc_RuntimeError,
                        handle->closing ? "sender close is in progress" : "sender is closed");
        return NULL;
    }
    return handle;
}

/* Destroy the native sender (void: it cannot refuse). The endpoint retention
 * is released by the caller AFTER this returns, so the endpoint is never
 * destroyed under a live attachment. */
static void sender_destroy_native(sender_handle_t *handle)
{
    moq_media_sender_t *sender = handle->sender;
    if (!sender) return;
    handle->closing = true;
    handle->sender = NULL;
    Py_BEGIN_ALLOW_THREADS
    moq_media_sender_destroy(sender);
    Py_END_ALLOW_THREADS
    handle->closing = false;
}

static void sender_capsule_destroy(PyObject *capsule)
{
    PyObject *pending = PyErr_GetRaisedException();
    sender_handle_t *handle = PyCapsule_GetPointer(capsule, sender_name);
    if (!handle) {
        PyErr_WriteUnraisable(NULL);
        PyErr_SetRaisedException(pending);
        return;
    }
    if (handle->sender && handle->pid == (long)process_id()) {
        sender_destroy_native(handle);
        if (PyErr_WarnEx(PyExc_ResourceWarning,
                         "unclosed moq5.Sender; use close() or a context manager", 1) < 0) {
            PyErr_WriteUnraisable(NULL);
        }
    }
    /* Release the endpoint retention last. A fork child drops only its Python
     * references; the native sender and its endpoint stay parent-owned. */
    Py_CLEAR(handle->endpoint);
    PyMem_Free(handle);
    PyErr_SetRaisedException(pending);
}

/* Stage one: allocate the handle, retain the endpoint and build the capsule,
 * with NOTHING attached. The caller commits this inert capsule to its wrapper
 * before stage two runs, so no fallible Python work follows native success.
 * Dropping the capsule here releases the endpoint retention and attaches
 * nothing. */
static PyObject *sender_prepare_py(PyObject *module, PyObject *endpoint_capsule)
{
    (void)module;
    endpoint_handle_t *ep = handle_get(endpoint_capsule, 1, 1);
    if (!ep) return NULL;
    if (site_fails("sender_handle")) return PyErr_NoMemory();
    sender_handle_t *handle = PyMem_Calloc(1, sizeof(*handle));
    if (!handle) return PyErr_NoMemory();
    handle->pid = (long)process_id();
    handle->thread_id = PyThreadState_GetID(PyThreadState_Get());
    handle->endpoint = Py_NewRef(endpoint_capsule);
    if (site_fails("sender_capsule")) {
        Py_DECREF(handle->endpoint);
        PyMem_Free(handle);
        return PyErr_NoMemory();
    }
    PyObject *capsule = PyCapsule_New(handle, sender_name, sender_capsule_destroy);
    if (!capsule) {
        Py_DECREF(handle->endpoint);
        PyMem_Free(handle);
        return NULL;
    }
    return capsule;
}

/* Stage two: validate the configuration and perform the native attachment on
 * a capsule the caller already owns. Nothing fallible follows MOQ_OK. */
static PyObject *sender_activate_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *namespace, *catalog, *backpressure;
    PyObject *block_timeout, *queue_objects, *queue_bytes;
    PyObject *pre_ready_objects, *pre_ready_bytes;
    PyObject *validate_cmaf, *publish_tracks, *drop_without_demand, *refresh;
    if (!PyArg_ParseTuple(args, "OOOOOOOOOOOOO:sender_activate", &capsule,
                          &namespace, &catalog, &backpressure, &block_timeout,
                          &queue_objects, &queue_bytes, &pre_ready_objects,
                          &pre_ready_bytes, &validate_cmaf, &publish_tracks,
                          &drop_without_demand, &refresh)) return NULL;
    sender_handle_t *handle = sender_get(capsule, 1, 0);
    if (!handle) return NULL;
    if (handle->activated) {
        PyErr_SetString(PyExc_RuntimeError, "this sender was already attached");
        return NULL;
    }
    for (PyObject *flag = NULL, **each = (PyObject *[]){ validate_cmaf, publish_tracks,
                                                         drop_without_demand, NULL };
         (flag = *each) != NULL; ++each) {
        if (!PyBool_Check(flag)) {
            PyErr_SetString(PyExc_TypeError, "sender flags must be bool");
            return NULL;
        }
    }
    uint64_t bp, timeout, qo, qb, pro, prb, refresh_us;
    moq_bytes_t catalog_bytes;
    if (binary_value(catalog, "catalog_track", &catalog_bytes) < 0 ||
        uint_value(backpressure, MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK, "backpressure", &bp) < 0 ||
        uint_value(block_timeout, UINT64_MAX, "block_timeout_us", &timeout) < 0 ||
        uint_value(queue_objects, UINT32_MAX, "queue_max_objects", &qo) < 0 ||
        uint_value(queue_bytes, UINT32_MAX, "queue_max_bytes", &qb) < 0 ||
        uint_value(pre_ready_objects, UINT32_MAX, "pre_ready_max_objects", &pro) < 0 ||
        uint_value(pre_ready_bytes, UINT32_MAX, "pre_ready_max_bytes", &prb) < 0 ||
        uint_value(refresh, UINT64_MAX, "catalog_refresh_interval_us", &refresh_us) < 0)
        return NULL;
    if (bp == MOQ_MEDIA_SEND_BP_UNSET) {
        PyErr_SetString(PyExc_ValueError, "backpressure is out of range");
        return NULL;
    }
    if (!PyTuple_Check(namespace)) {
        PyErr_SetString(PyExc_TypeError, "namespace must be a tuple");
        return NULL;
    }
    Py_ssize_t count = PyTuple_Size(namespace);
    if (count < 0) return NULL;
    if (count == 0) {
        PyErr_SetString(PyExc_ValueError, "namespace must not be empty");
        return NULL;
    }
    if ((size_t)count > SIZE_MAX / sizeof(moq_bytes_t)) return PyErr_NoMemory();
    moq_bytes_t *parts = PyMem_Calloc((size_t)count, sizeof(*parts));
    if (!parts) return PyErr_NoMemory();
    for (Py_ssize_t i = 0; i < count; ++i) {
        if (binary_value(PyTuple_GetItem(namespace, i), "namespace part", &parts[i]) < 0) {
            PyMem_Free(parts);
            return NULL;
        }
        if (parts[i].len == 0) {
            PyMem_Free(parts);
            PyErr_SetString(PyExc_ValueError, "namespace parts must not be empty");
            return NULL;
        }
    }

    /* The full current configuration, with callbacks and content protections
     * deliberately absent: this shell installs neither. */
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.endpoint = NULL;                 /* attach borrows */
    cfg.namespace_.parts = parts;
    cfg.namespace_.count = (size_t)count;
    cfg.catalog_track = catalog_bytes;
    cfg.backpressure = (moq_media_send_backpressure_t)bp;
    cfg.block_timeout_us = timeout;
    cfg.queue_max_objects = (uint32_t)qo;
    cfg.queue_max_bytes = (uint32_t)qb;
    cfg.pre_ready_max_objects = (uint32_t)pro;
    cfg.pre_ready_max_bytes = (uint32_t)prb;
    cfg.validate_cmaf = validate_cmaf == Py_True;
    cfg.publish_tracks = publish_tracks == Py_True;
    cfg.drop_without_demand = drop_without_demand == Py_True;
    cfg.catalog_refresh_interval_us = refresh_us;

    endpoint_handle_t *ep = handle_get(handle->endpoint, 1, 1);
    if (!ep) {
        PyMem_Free(parts);
        return NULL;
    }
    handle->activated = true;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_attach(ep->endpoint, &cfg, &handle->sender);
    Py_END_ALLOW_THREADS
    PyMem_Free(parts);
    if (rc != MOQ_OK) {
        handle->sender = NULL;
        /* Release the endpoint retention now: nothing is attached, and the
         * refused wrapper may still be held by an exception traceback. */
        Py_CLEAR(handle->endpoint);
        return native_error(module, rc, "attach");
    }
    Py_RETURN_NONE;
}

static PyObject *sender_close_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    sender_handle_t *handle = sender_get(capsule, 1, 0);
    if (!handle) return NULL;
    sender_destroy_native(handle);
    Py_CLEAR(handle->endpoint);
    Py_RETURN_NONE;
}

static PyObject *sender_closed_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    sender_handle_t *handle = sender_get(capsule, 0, 0);
    if (!handle) return NULL;
    return PyBool_FromLong(handle->sender == NULL && !handle->closing);
}

static PyObject *sender_enter_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    if (!sender_get(capsule, 1, 1)) return NULL;
    Py_RETURN_NONE;
}

static PyObject *sender_ready_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    sender_handle_t *handle = sender_get(capsule, 1, 1);
    if (!handle) return NULL;
    return PyBool_FromLong(moq_media_sender_is_ready(handle->sender));
}

static PyObject *sender_terminal_py(PyObject *module, PyObject *capsule)
{
    sender_handle_t *handle = sender_get(capsule, 1, 1);
    if (!handle) return NULL;
    endpoint_handle_t *ep = handle_get(handle->endpoint, 0, 1);
    if (!ep) return NULL;
    bool closed = moq_media_sender_is_closed(handle->sender);
    bool fatal = moq_media_sender_is_fatal(handle->sender);
    uint64_t code = moq_media_sender_fatal_code(handle->sender);
    moq_endpoint_terminal_t terminal = {0};
    moq_result_t rc = moq_endpoint_get_terminal(ep->endpoint, &terminal, sizeof(terminal));
    if (rc != MOQ_OK) return native_error(module, rc, "terminal");
    if (terminal.struct_size < MOQ_ENDPOINT_TERMINAL_V0_SIZE)
        return binding_fault(module, "terminal", "below the v0 prefix",
                             terminal.struct_size, MOQ_ENDPOINT_TERMINAL_V0_SIZE);
    return Py_BuildValue("(OOKiK)", closed ? Py_True : Py_False, fatal ? Py_True : Py_False,
                         (unsigned long long)code, (int)terminal.reason,
                         (unsigned long long)terminal.detail_code);
}

/* -- send tracks ---------------------------------------------------------- *
 * The NATIVE sender owns its tracks until it is destroyed, so this capsule has
 * no per-track destructor: it holds only Python-side ownership. It retains its
 * SENDER capsule from prepare until the track is discarded, which keeps the
 * owner alive under a live track, and it retains the declared name object
 * (bytes are immutable, so ownership is a reference, never a copy).
 *
 * prepare/activate split, as for Sender.attach: everything fallible is done
 * while nothing is registered, and the native add is the final step. */
/* Three states, never conflated. An ATTEMPT is not a registration: a refused
 * or cancelled activation leaves a handle whose retained fields are gone, and
 * every accessor must refuse it rather than read cleared storage. */
typedef enum {
    SEND_TRACK_PREPARED,          /* built, nothing registered yet */
    SEND_TRACK_REGISTERED,        /* the native add succeeded */
    SEND_TRACK_ABORTED            /* refused or cancelled; fields released */
} send_track_state_t;

typedef struct {
    moq_media_track_t *track;     /* non-NULL only while REGISTERED */
    PyObject *sender;             /* the sender capsule, retained until aborted */
    PyObject *name;               /* the declared name, retained by reference */
    uint64_t thread_id;
    long pid;
    send_track_state_t state;
    bool removed;                 /* a native remove succeeded */
} send_track_handle_t;

static const char send_track_name_tag[] = "moq5._native.send_track";

/* Common checked access: the capsule TYPE is the gate, so an endpoint, sender
 * or receiver-track capsule can never reach this boundary. */
/* `need` is the LOWEST state this operation accepts. An aborted handle is
 * refused everywhere except the idempotent abort itself. */
static send_track_handle_t *send_track_get(PyObject *object, int owner,
                                           int need_registered, int allow_aborted)
{
    send_track_handle_t *handle = PyCapsule_GetPointer(object, send_track_name_tag);
    if (!handle) return NULL;
    if (handle->pid != (long)process_id()) {
        PyErr_SetString(PyExc_RuntimeError,
                        "an inherited track cannot be used after fork");
        return NULL;
    }
    if (owner && handle->thread_id != PyThreadState_GetID(PyThreadState_Get())) {
        PyErr_SetString(PyExc_RuntimeError,
                        "track operation requires its creating thread");
        return NULL;
    }
    if (handle->state == SEND_TRACK_ABORTED && !allow_aborted) {
        PyErr_SetString(PyExc_RuntimeError,
                        "this track was never registered: its add was refused "
                        "or cancelled");
        return NULL;
    }
    if (need_registered && handle->state != SEND_TRACK_REGISTERED) {
        PyErr_SetString(PyExc_RuntimeError, "this track is not registered yet");
        return NULL;
    }
    return handle;
}

/* Release the prepared Python-side retention. The native track itself belongs
 * to the sender and is NOT destroyed here. */
static void send_track_release(send_track_handle_t *handle)
{
    Py_CLEAR(handle->sender);
    Py_CLEAR(handle->name);
}

static void send_track_capsule_destroy(PyObject *capsule)
{
    PyObject *pending = PyErr_GetRaisedException();
    send_track_handle_t *handle = PyCapsule_GetPointer(capsule, send_track_name_tag);
    if (!handle) {
        PyErr_WriteUnraisable(NULL);
        PyErr_SetRaisedException(pending);
        return;
    }
    /* No native effect: dropping a track never removes or ends it. */
    send_track_release(handle);
    handle->state = SEND_TRACK_ABORTED;
    PyMem_Free(handle);
    PyErr_SetRaisedException(pending);
}

static PyObject *send_track_prepare_py(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *sender_capsule, *name;
    if (!PyArg_ParseTuple(args, "OO:send_track_prepare", &sender_capsule, &name))
        return NULL;
    if (!PyBytes_Check(name)) {
        PyErr_SetString(PyExc_TypeError, "track name must be bytes");
        return NULL;
    }
    /* owner thread, same process and a live sender, BEFORE any allocation */
    if (!sender_get(sender_capsule, 1, 1)) return NULL;
    if (site_fails("send_track_handle")) return PyErr_NoMemory();
    /* PyMem_Calloc zeroes, and SEND_TRACK_PREPARED is zero. */
    send_track_handle_t *handle = PyMem_Calloc(1, sizeof(*handle));
    if (!handle) return PyErr_NoMemory();
    handle->pid = (long)process_id();
    handle->thread_id = PyThreadState_GetID(PyThreadState_Get());
    handle->sender = Py_NewRef(sender_capsule);
    handle->name = Py_NewRef(name);
    if (site_fails("send_track_capsule")) {
        send_track_release(handle);
        PyMem_Free(handle);
        return PyErr_NoMemory();
    }
    PyObject *capsule = PyCapsule_New(handle, send_track_name_tag,
                                      send_track_capsule_destroy);
    if (!capsule) {
        send_track_release(handle);
        PyMem_Free(handle);
        return NULL;
    }
    return capsule;
}

static PyObject *send_track_activate_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *codec, *init_data, *role, *lang, *channel_config;
    PyObject *media_type, *packaging, *bitrate, *timescale, *width, *height;
    PyObject *framerate, *samplerate, *is_live, *duration;
    if (!PyArg_ParseTuple(args, "OOOOOOOOOOOOOOOO:send_track_activate",
                          &capsule, &media_type, &packaging, &codec, &bitrate,
                          &timescale, &init_data, &role, &lang, &is_live,
                          &width, &height, &framerate, &samplerate,
                          &channel_config, &duration))
        return NULL;
    send_track_handle_t *handle = send_track_get(capsule, 1, 0, 0);
    if (!handle) return NULL;
    if (handle->state != SEND_TRACK_PREPARED) {
        PyErr_SetString(PyExc_RuntimeError, "this track was already registered");
        return NULL;
    }
    if (!PyBool_Check(is_live)) {
        PyErr_SetString(PyExc_TypeError, "is_live must be bool");
        goto abort_prepared;
    }
    /* The prepared owner must still be usable: prepare and activate are two
     * calls, so the sender may have been closed in between. */
    sender_handle_t *sender = sender_get(handle->sender, 1, 1);
    if (!sender) goto abort_prepared;

    uint64_t mt, pk, rate, ts, w, h, fps, sr, dur = 0;
    moq_bytes_t codec_bytes, init_bytes, role_bytes, lang_bytes, channel_bytes;
    moq_bytes_t name_bytes;
    if (binary_value(handle->name, "name", &name_bytes) < 0 ||
        binary_value(codec, "codec", &codec_bytes) < 0 ||
        binary_value(init_data, "init_data", &init_bytes) < 0 ||
        binary_value(role, "role", &role_bytes) < 0 ||
        binary_value(lang, "lang", &lang_bytes) < 0 ||
        binary_value(channel_config, "channel_config", &channel_bytes) < 0 ||
        uint_value(media_type, UINT32_MAX, "media_type", &mt) < 0 ||
        uint_value(packaging, UINT32_MAX, "packaging", &pk) < 0 ||
        uint_value(bitrate, UINT64_MAX, "bitrate", &rate) < 0 ||
        uint_value(timescale, UINT32_MAX, "timescale", &ts) < 0 ||
        uint_value(width, UINT32_MAX, "width", &w) < 0 ||
        uint_value(height, UINT32_MAX, "height", &h) < 0 ||
        uint_value(framerate, UINT64_MAX, "framerate_millis", &fps) < 0 ||
        uint_value(samplerate, UINT32_MAX, "samplerate", &sr) < 0)
        goto abort_prepared;
    bool has_duration = duration != Py_None;
    if (has_duration && uint_value(duration, UINT64_MAX, "track_duration_ms",
                                   &dur) < 0)
        goto abort_prepared;
    /* Defensive re-check. uint_value requires PyLong and binary_value requires
     * bytes, so no foreign __index__ or buffer protocol runs above and nothing
     * can have closed the sender between the two checks inside this function.
     * The public property reads happen BEFORE this call, which is where a
     * config subclass could run code. */
    sender = sender_get(handle->sender, 1, 1);
    if (!sender) goto abort_prepared;

    /* The full current configuration, with every deferred field absent. */
    moq_media_track_cfg_t cfg;
    moq_media_track_cfg_init(&cfg);
    cfg.name = name_bytes;
    cfg.media_type = (moq_media_type_t)mt;
    cfg.packaging = (moq_media_packaging_t)pk;
    cfg.codec = codec_bytes;
    cfg.timescale = (uint32_t)ts;
    cfg.init_data = init_bytes;
    cfg.role = role_bytes;
    cfg.lang = lang_bytes;
    cfg.is_live = is_live == Py_True;
    cfg.width = (uint32_t)w;
    cfg.height = (uint32_t)h;
    cfg.framerate_millis = fps;
    cfg.samplerate = (uint32_t)sr;
    cfg.channel_config = channel_bytes;
    cfg.bitrate = rate;
    cfg.has_track_duration = has_duration;
    cfg.track_duration_ms = dur;

    moq_media_track_t *native = NULL;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_add_track(sender->sender, &cfg, &native);
    Py_END_ALLOW_THREADS
    if (rc != MOQ_OK) {
        /* An ATTEMPT is not a registration. Release the inactive prepared
         * retention now, so a live traceback holding the wrapper cannot keep
         * the owner alive, and mark the handle aborted so every accessor
         * refuses it instead of reading cleared storage. */
        handle->state = SEND_TRACK_ABORTED;
        send_track_release(handle);
        return native_error(module, rc, "add_track");
    }
    handle->track = native;
    handle->state = SEND_TRACK_REGISTERED;
    Py_RETURN_NONE;

abort_prepared:
    /* A conversion or validation failure before the native call leaves nothing
     * registered; the prepared retention goes with it and the original error
     * is preserved. */
    handle->state = SEND_TRACK_ABORTED;
    send_track_release(handle);
    return NULL;
}

/* Cancel an INACTIVE prepared transaction. Idempotent, never removes or ends a
 * native track, and refuses to discard a successful registration. Private
 * bridge machinery: there is no user-facing discard. */
static PyObject *send_track_abort_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    send_track_handle_t *handle = send_track_get(capsule, 1, 0, 1);
    if (!handle) return NULL;
    if (handle->state == SEND_TRACK_REGISTERED) {
        PyErr_SetString(PyExc_RuntimeError,
                        "a registered track cannot be discarded");
        return NULL;
    }
    handle->state = SEND_TRACK_ABORTED;
    send_track_release(handle);
    Py_RETURN_NONE;
}

static PyObject *send_track_name_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    send_track_handle_t *handle = send_track_get(capsule, 1, 0, 0);
    if (!handle || !handle->name) {
        if (!PyErr_Occurred())
            PyErr_SetString(PyExc_RuntimeError, "this track has no name");
        return NULL;
    }
    return Py_NewRef(handle->name);
}

static PyObject *send_track_removed_py(PyObject *module, PyObject *capsule)
{
    (void)module;
    send_track_handle_t *handle = send_track_get(capsule, 1, 1, 0);
    if (!handle) return NULL;
    if (handle->removed) Py_RETURN_TRUE;
    /* An accepted completion request is a removal made through this binding:
     * the owner records that the service marked every track it then had
     * removed. The owner's handle storage outlives the native sender -- only
     * the capsule's release frees it -- so this reads binding-owned state and
     * never a destroyed C sender. */
    sender_handle_t *owner = PyCapsule_GetPointer(handle->sender, sender_name);
    if (!owner) return NULL;
    return PyBool_FromLong(owner->completion_acknowledged);
}

static PyObject *sender_remove_track_py(PyObject *module, PyObject *args)
{
    PyObject *sender_capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:sender_remove_track", &sender_capsule,
                          &track_capsule)) return NULL;
    sender_handle_t *sender = sender_get(sender_capsule, 1, 1);
    if (!sender) return NULL;
    send_track_handle_t *handle = send_track_get(track_capsule, 1, 1, 0);
    if (!handle) return NULL;
    if (handle->sender != sender_capsule) {
        PyErr_SetString(PyExc_RuntimeError, "this track belongs to another sender");
        return NULL;
    }
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_remove_track(sender->sender, handle->track);
    Py_END_ALLOW_THREADS
    if (rc != MOQ_OK) return native_error(module, rc, "remove_track");
    handle->removed = true;
    Py_RETURN_NONE;
}

/* -- media write ---------------------------------------------------------- *
 * One synchronous submission. Ownership is transfer-on-success: the two spans
 * are copied into refcounted buffers here, and on MOQ_OK they belong to the
 * service. On ANY non-OK result they are released HERE, before a Python result
 * or error is allocated and before any pending signal is processed, so a
 * failure to report a refusal can never strand a buffer.
 *
 * The success carrier is allocated BEFORE the native call, so this bridge
 * allocates nothing further for a success once the service has accepted the
 * object, and ownership never depends on a small-integer cache. That is not
 * the same as "nothing can fail after acceptance": the explicit signal check
 * below may still raise, and the Python wrapper runs after this returns. An
 * exception from either does not undo the transfer. */
#ifdef MOQ_PYTHON_TESTING
#define WRITE_SPAN_PAYLOAD 0
#define WRITE_SPAN_PROPERTIES 1
#define write_span_alloc(span) moq5_test_counting_alloc_for(span)
#else
#define WRITE_SPAN_PAYLOAD 0
#define WRITE_SPAN_PROPERTIES 1
#define write_span_alloc(span) ((void)(span), moq_alloc_default())
#endif

static int write_span(PyObject *value, int span, const char *name,
                      const char *site, moq_rcbuf_t **out)
{
    moq_bytes_t bytes;
    (void)site;                    /* the fault gate is testing-only */
    if (binary_value(value, name, &bytes) < 0) return -1;
    if (site_fails(site)) return -1;
    moq_result_t rc = moq_rcbuf_create(write_span_alloc(span), bytes.data,
                                       bytes.len, out);
    if (rc != MOQ_OK) {
        PyErr_NoMemory();
        return -1;
    }
    return 0;
}

static PyObject *sender_write_py(PyObject *module, PyObject *args)
{
    PyObject *sender_capsule, *track_capsule, *payload, *properties;
    PyObject *is_sync, *starts_group, *ends_group;
    PyObject *decode_time, *presentation_time, *capture_time, *sap_type;
    if (!PyArg_ParseTuple(args, "OOOOOOOOOOO:sender_write",
                          &sender_capsule, &track_capsule, &payload,
                          &properties, &is_sync, &starts_group, &ends_group,
                          &decode_time, &presentation_time, &capture_time,
                          &sap_type))
        return NULL;

    /* Owner thread, same process, live sender and a REGISTERED track of THIS
     * sender, all before the native track pointer is read and before anything
     * is acquired. */
    sender_handle_t *sender = sender_get(sender_capsule, 1, 1);
    if (!sender) return NULL;
    send_track_handle_t *track = send_track_get(track_capsule, 1, 1, 0);
    if (!track) return NULL;
    if (track->sender != sender_capsule) {
        PyErr_SetString(PyExc_RuntimeError, "this track belongs to another sender");
        return NULL;
    }

    if (!PyBool_Check(is_sync) || !PyBool_Check(starts_group) ||
        !PyBool_Check(ends_group)) {
        PyErr_SetString(PyExc_TypeError,
                        "is_sync, starts_group and ends_group must be bool");
        return NULL;
    }
    uint64_t decode_us, presentation_us, capture_us = 0, sap = 0;
    if (uint_value(decode_time, UINT64_MAX, "decode_time_us", &decode_us) < 0 ||
        uint_value(presentation_time, UINT64_MAX, "presentation_time_us",
                   &presentation_us) < 0)
        return NULL;
    bool has_capture = capture_time != Py_None;
    if (has_capture && uint_value(capture_time, UINT64_MAX, "capture_time_us",
                                  &capture_us) < 0)
        return NULL;
    bool has_sap = sap_type != Py_None;
    if (has_sap && uint_value(sap_type, UINT32_MAX, "sap_type", &sap) < 0)
        return NULL;
    if (properties != Py_None && !PyBytes_Check(properties)) {
        PyErr_SetString(PyExc_TypeError, "properties must be immutable bytes");
        return NULL;
    }

    /* Nothing above runs foreign Python: uint_value requires PyLong and the
     * spans must be bytes. The owner is rechecked anyway, because prepare-time
     * validity is not call-time validity. */
    sender = sender_get(sender_capsule, 1, 1);
    if (!sender) return NULL;
    track = send_track_get(track_capsule, 1, 1, 0);
    if (!track) return NULL;

    moq_rcbuf_t *span_payload = NULL, *span_properties = NULL;
    if (write_span(payload, WRITE_SPAN_PAYLOAD, "payload",
                   "send_object_payload", &span_payload) < 0)
        return NULL;
    if (properties != Py_None &&
        write_span(properties, WRITE_SPAN_PROPERTIES, "properties",
                   "send_object_properties", &span_properties) < 0) {
        moq_rcbuf_decref(span_payload);
        return NULL;
    }

    /* The success carrier, built while a failure can still be reported. */
    PyObject *accepted = PyLong_FromLong(0);
    if (!accepted) {
        moq_rcbuf_decref(span_payload);
        if (span_properties) moq_rcbuf_decref(span_properties);
        return NULL;
    }

    moq_media_send_object_t object;
    memset(&object, 0, sizeof(object));
    object.struct_size = (uint32_t)sizeof(object);
    object.payload = span_payload;
    object.properties = span_properties;
    object.is_sync = is_sync == Py_True;
    object.starts_group = starts_group == Py_True;
    object.ends_group = ends_group == Py_True;
    object.decode_time_us = decode_us;
    object.presentation_time_us = presentation_us;
    object.has_capture_time = has_capture;
    object.capture_time_us = capture_us;
    object.has_sap_type = has_sap;
    object.sap_type = (moq_sap_type_t)sap;

    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_write(sender->sender, track->track, &object);
    Py_END_ALLOW_THREADS

    if (rc != MOQ_OK) {
        /* No transfer. Ownership is settled FIRST, before a result or error
         * object is allocated and before a pending signal is processed. */
        moq_rcbuf_decref(span_payload);
        if (span_properties) moq_rcbuf_decref(span_properties);
        Py_DECREF(accepted);
        if (PyErr_CheckSignals() < 0) return NULL;
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_INTERRUPTED ||
            rc == MOQ_ERR_CLOSED) {
            if (site_fails("write_result")) return NULL;
            return PyLong_FromLong((long)rc);
        }
        if (site_fails("write_error")) return NULL;
        return native_error(module, rc, "write");
    }
    /* Accepted: both references belong to the service and must NOT be
     * released here. Ownership is already settled, so a pending signal may be
     * processed now -- and is not suppressed. An interrupt seen here does not
     * mean the object was refused. */
    if (PyErr_CheckSignals() < 0) {
        Py_DECREF(accepted);
        return NULL;
    }
    return accepted;
}

/* -- bounded wait ---------------------------------------------------------- *
 * One bounded native wait per call, exactly as the receiver's. The timeout is
 * validated here against the same public domain the wrapper uses, the sender
 * capsule and its owner thread, process and liveness are checked before any
 * native entry, the GIL is released only around the potentially blocking call,
 * and a pending Python signal is processed after it is reacquired.
 *
 * The four agreed codes come back as values for WaitResult; anything else
 * keeps its signed code as a MoqError with the operation "wait". A terminal
 * sender is a CLOSED RESULT; a destroyed Python owner is refused before the
 * service is entered. Neither becomes a silent timeout. */
static PyObject *sender_wait_py(PyObject *module, PyObject *args)
{
    PyObject *capsule, *timeout;
    if (!PyArg_ParseTuple(args, "OO:sender_wait", &capsule, &timeout))
        return NULL;
    uint64_t micros;
    if (uint_value(timeout, INT64_MAX, "timeout_us", &micros) < 0) return NULL;
    sender_handle_t *handle = sender_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_wait(handle->sender, micros);
    Py_END_ALLOW_THREADS
    if (PyErr_CheckSignals() < 0) return NULL;
    if (rc != MOQ_OK && rc != MOQ_DONE && rc != MOQ_ERR_INTERRUPTED &&
        rc != MOQ_ERR_CLOSED)
        return native_error(module, rc, "wait");
    return PyLong_FromLong(rc);
}

/* -- subscriber demand ----------------------------------------------------- *
 * Three INDEPENDENT point-in-time queries over the service's mirrored counts.
 * The C getters are thread-safe and answer 0/false for a NULL or foreign
 * handle, but that fallback is not permission for this API to accept one:
 * the owner thread, the process, a live sender and a REGISTERED track of THIS
 * sender are all checked here, exactly as for write and end_track.
 *
 * Nothing is cached, summed in Python, gated on readiness or latched on
 * removal; the service's own answer is reported. A count is not a promise
 * about a later write. */
static send_track_handle_t *demand_track(PyObject *sender_capsule,
                                         PyObject *track_capsule,
                                         sender_handle_t **owner)
{
    *owner = sender_get(sender_capsule, 1, 1);
    if (!*owner) return NULL;
    send_track_handle_t *track = send_track_get(track_capsule, 1, 1, 0);
    if (!track) return NULL;
    if (track->sender != sender_capsule) {
        PyErr_SetString(PyExc_RuntimeError, "this track belongs to another sender");
        return NULL;
    }
    return track;
}

static PyObject *sender_subscriptions_py(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *sender_capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:sender_subscriptions", &sender_capsule,
                          &track_capsule))
        return NULL;
    sender_handle_t *owner;
    send_track_handle_t *track = demand_track(sender_capsule, track_capsule,
                                              &owner);
    if (!track) return NULL;
    size_t count = moq_media_sender_track_subscriptions(owner->sender,
                                                        track->track);
    /* The whole size_t domain, as an ordinary nonnegative Python int. */
    if (site_fails("sender_subscriptions_result")) return NULL;
    return PyLong_FromSize_t(count);
}

static PyObject *sender_has_subscriber_py(PyObject *module, PyObject *args)
{
    (void)module;
    PyObject *sender_capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:sender_has_subscriber", &sender_capsule,
                          &track_capsule))
        return NULL;
    sender_handle_t *owner;
    send_track_handle_t *track = demand_track(sender_capsule, track_capsule,
                                              &owner);
    if (!track) return NULL;
    /* The service's own convenience query, not a Python recomputation. */
    bool any = moq_media_sender_track_has_subscriber(owner->sender,
                                                     track->track);
    return PyBool_FromLong(any);          /* a singleton: nothing to fail */
}

static PyObject *sender_has_media_subscriber_py(PyObject *module,
                                                PyObject *capsule)
{
    (void)module;
    sender_handle_t *owner = sender_get(capsule, 1, 1);
    if (!owner) return NULL;
    /* One native observation over app-visible media tracks; never a sum over
     * Python wrappers, and never gated on readiness. */
    return PyBool_FromLong(moq_media_sender_has_media_subscriber(owner->sender));
}

/* -- sender statistics ----------------------------------------------------- *
 * A read-only snapshot, taken anew on every call. The frozen v0 prefix ends at
 * last_error; sap_records_evicted is appended after it and is read ONLY when
 * the stamped prefix covers the whole field -- absence is None, never a
 * fabricated zero. The native result is checked FIRST; a malformed stamp on a
 * SUCCESSFUL call is a binding fault, never a native error code. Nothing is
 * recomputed, repaired or cached, and no native pointer is retained. */
#define SENDER_STATS_V0_SIZE \
    (offsetof(moq_media_sender_stats_t, last_error) + sizeof(moq_result_t))
#define SENDER_STATS_FIELD_END(field)                        \
    (offsetof(moq_media_sender_stats_t, field) +             \
     sizeof(((moq_media_sender_stats_t *)0)->field))

static PyObject *sender_stats_py(PyObject *module, PyObject *capsule)
{
    sender_handle_t *handle = sender_get(capsule, 1, 1);
    if (!handle) return NULL;
    moq_media_sender_stats_t st;
    memset(&st, 0xEE, sizeof(st));           /* poison: absence must be absence */
    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_get_stats(handle->sender, &st, sizeof(st));
    Py_END_ALLOW_THREADS
    if (rc != MOQ_OK) return native_error(module, rc, "stats");
    /* The service returned OK: a stamp outside [v0, the capacity we offered]
     * is malformed output, reported as a binding fault. */
    if (st.struct_size < SENDER_STATS_V0_SIZE)
        return binding_fault(module, "stats", "below the v0 prefix",
                             st.struct_size, SENDER_STATS_V0_SIZE);
    if (st.struct_size > sizeof(st))
        return binding_fault(module, "stats", "exceeds sizeof",
                             st.struct_size, sizeof(st));
    PyObject *evicted = Py_None;
    if (st.struct_size >= SENDER_STATS_FIELD_END(sap_records_evicted)) {
        if (site_fails("sender_stats_field")) return NULL;
        evicted = PyLong_FromUnsignedLongLong(
            (unsigned long long)st.sap_records_evicted);
    } else {
        Py_INCREF(evicted);
    }
    if (!evicted) return NULL;
    if (site_fails("sender_stats_result")) {
        Py_DECREF(evicted);
        return NULL;
    }
    PyObject *result = Py_BuildValue("(KKKKKKKKKiO)",
        (unsigned long long)st.objects_written,
        (unsigned long long)st.objects_sent,
        (unsigned long long)st.objects_queued,
        (unsigned long long)st.bytes_queued,
        (unsigned long long)st.objects_dropped,
        (unsigned long long)st.groups_dropped,
        (unsigned long long)st.keyframes_dropped,
        (unsigned long long)st.groups_abandoned,
        (unsigned long long)st.backpressure_stalls,
        (int)st.last_error,                  /* signed: historical DATA */
        evicted);
    Py_DECREF(evicted);
    return result;
}

/* -- broadcast completion --------------------------------------------------- *
 * One asynchronous REQUEST that the broadcast be terminated permanently.
 * Synchronously the service marks every track this sender has removed, which
 * is the fact acknowledged below; the END_OF_TRACKs and the terminal catalog
 * are emitted later, on its own thread. Idempotence stays NATIVE: every call
 * reaches the service, so its interrupt and terminal checks -- which precede
 * that idempotent no-op -- are never bypassed here. */
static PyObject *sender_complete_py(PyObject *module, PyObject *capsule)
{
    sender_handle_t *handle = sender_get(capsule, 1, 1);
    if (!handle) return NULL;

    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_complete(handle->sender);
    Py_END_ALLOW_THREADS

    if (rc != MOQ_OK) {
        if (PyErr_CheckSignals() < 0) return NULL;
        return native_error(module, rc, "request_complete");
    }
    /* The service has marked every track this sender had removed. Record that
     * BEFORE anything else can fail: a signal raised on the way out does not
     * undo what the service already did, so the tracks must still report it.
     * Nothing here allocates, iterates or reads the service again. */
    handle->completion_acknowledged = true;
    if (PyErr_CheckSignals() < 0) return NULL;
    Py_RETURN_NONE;
}

/* -- per-track ending ------------------------------------------------------ *
 * One asynchronous termination REQUEST. The service queues a terminal marker
 * and emits END_OF_TRACK after this track's queued objects drain; returning
 * MOQ_OK here does not mean the terminal was emitted or that prior media was
 * flushed. Ending one track removes nothing, destroys nothing, and touches
 * neither other tracks nor the endpoint.
 *
 * Idempotence stays NATIVE: this bridge keeps no ended state, and a repeat is
 * simply a second call. The service checks its interrupt and terminal
 * conditions BEFORE its idempotent no-op, so a repeat after terminalization
 * is CLOSED rather than a courtesy OK, and that is what is reported.
 *
 * The success carrier is built BEFORE the native call, so a failure to build
 * it cannot leave an end queued that the caller never learns about. A pending
 * signal is processed only after the native result is settled; an exception
 * that follows does NOT undo a committed request. */
static PyObject *sender_end_track_py(PyObject *module, PyObject *args)
{
    PyObject *sender_capsule, *track_capsule;
    if (!PyArg_ParseTuple(args, "OO:sender_end_track", &sender_capsule,
                          &track_capsule))
        return NULL;

    /* Owner thread, same process, live sender, and a REGISTERED track of THIS
     * sender, all before the native track pointer is read. The Python wrapper
     * checks too; it is not what makes this memory-safe. */
    sender_handle_t *sender = sender_get(sender_capsule, 1, 1);
    if (!sender) return NULL;
    send_track_handle_t *track = send_track_get(track_capsule, 1, 1, 0);
    if (!track) return NULL;
    if (track->sender != sender_capsule) {
        PyErr_SetString(PyExc_RuntimeError, "this track belongs to another sender");
        return NULL;
    }

    /* The success carrier, built while a failure can still be reported and
     * before anything is committed natively. */
    if (site_fails("end_track_result")) return NULL;
    PyObject *accepted = PyLong_FromLong(0);
    if (!accepted) return NULL;

    moq_result_t rc;
    Py_BEGIN_ALLOW_THREADS
    rc = moq_media_sender_end_track(sender->sender, track->track);
    Py_END_ALLOW_THREADS

    if (rc != MOQ_OK) {
        Py_DECREF(accepted);
        if (PyErr_CheckSignals() < 0) return NULL;
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_INTERRUPTED ||
            rc == MOQ_ERR_CLOSED)
            return PyLong_FromLong((long)rc);
        return native_error(module, rc, "end_track");
    }
    /* The request is the service's now. A signal may reach the caller here;
     * it does not undo what was committed. */
    if (PyErr_CheckSignals() < 0) {
        Py_DECREF(accepted);
        return NULL;
    }
    return accepted;
}

static PyObject *build_info(PyObject *module, PyObject *unused)
{
    (void)module;
    (void)unused;
#ifdef MOQ_PYTHON_TESTING
    PyObject *testing = Py_True;
#else
    PyObject *testing = Py_False;
#endif
    return Py_BuildValue("{s:s,s:s,s:s,s:O}",
                         "compiled_version", MOQ_VERSION_STRING,
                         "runtime_version", moq_version_string(),
                         "python_abi", "cp312-abi3", "test_backend", testing);
}

#ifdef MOQ_PYTHON_TESTING
#include "fake_service.h"
#endif

static PyMethodDef methods[] = {
    {"connect", connect_endpoint, METH_VARARGS, "Create a verified, asynchronous native connection."},
    {"close", close_endpoint, METH_O, "Stop/join, then destroy, on the owner thread."},
    {"closed", closed_endpoint, METH_O, "Whether this owner released its endpoint."},
    {"state", state_endpoint, METH_O, "Read the native endpoint state."},
    {"terminal", terminal_endpoint, METH_O, "Read the sized terminal snapshot."},
    {"negotiated_version", version_endpoint, METH_O, "Read negotiated version, zero before establishment."},
    {"wait", wait_endpoint, METH_VARARGS, "Wait with the GIL released; return the exact wait outcome."},
    {"wake", wake_endpoint, METH_O, "Schedule native service work."},
    {"set_interrupted", interrupt_endpoint, METH_VARARGS, "Set or clear the sticky interrupt."},
    {"build_info", build_info, METH_NOARGS, "Compiled and loaded LibMoQ versions (not dependency provenance)."},
    {"receiver_attach", receiver_attach_py, METH_VARARGS, "Attach a receiver to a live endpoint, retaining it."},
    {"receiver_close", receiver_close_py, METH_O, "Destroy the receiver, then release the endpoint retention."},
    {"receiver_closed", receiver_closed_py, METH_O, "Whether this owner released its receiver."},
    {"sender_prepare", sender_prepare_py, METH_O, "Build an inert sender capsule that retains its endpoint."},
    {"sender_activate", sender_activate_py, METH_VARARGS, "Attach a prepared sender; nothing fallible follows success."},
    {"sender_close", sender_close_py, METH_O, "Destroy the native sender on the owner thread."},
    {"sender_closed", sender_closed_py, METH_O, "Whether this owner released its sender."},
    {"sender_enter", sender_enter_py, METH_O, "Owner-thread, same-process and live checks."},
    {"sender_ready", sender_ready_py, METH_O, "Announced and catalog published; not demand."},
    {"sender_terminal", sender_terminal_py, METH_O, "Read closed/fatal/fatal_code and the endpoint terminal."},
    {"send_track_prepare", send_track_prepare_py, METH_VARARGS, "Build an inert send-track capsule that retains its sender."},
    {"send_track_activate", send_track_activate_py, METH_VARARGS, "Register a prepared track; the last fallible step."},
    {"send_track_name", send_track_name_py, METH_O, "The declared track name."},
    {"send_track_removed", send_track_removed_py, METH_O, "Whether the track has been removed."},
    {"send_track_abort", send_track_abort_py, METH_O, "Cancel an inactive prepared track."},
    {"sender_remove_track", sender_remove_track_py, METH_VARARGS, "Remove a track this sender owns."},
    {"sender_write", sender_write_py, METH_VARARGS, "Submit one media object; transfer-on-success."},
    {"sender_end_track", sender_end_track_py, METH_VARARGS, "Request per-track termination; idempotence stays native."},
    {"sender_complete", sender_complete_py, METH_O, "Request broadcast completion; the service answers every call."},
    {"sender_stats", sender_stats_py, METH_O, "Copy one sender stats snapshot."},
    {"sender_wait", sender_wait_py, METH_VARARGS, "One bounded native wait for the write level."},
    {"sender_subscriptions", sender_subscriptions_py, METH_VARARGS, "Active subscriptions for one owned track."},
    {"sender_has_subscriber", sender_has_subscriber_py, METH_VARARGS, "Whether one owned track has a subscriber."},
    {"sender_has_media_subscriber", sender_has_media_subscriber_py, METH_O, "Whether any app-visible media track has a subscriber."},
    {"receiver_enter", receiver_enter_py, METH_O, "Owner/process/liveness guard for context entry."},
    {"receiver_wait", receiver_wait_py, METH_VARARGS, "Wait with the GIL released; return the exact wait outcome."},
    {"receiver_stats", receiver_stats_py, METH_O, "Copy the sized stats snapshot."},
    {"receiver_terminal", receiver_terminal_py, METH_O, "Read closed/fatal/fatal_code and the endpoint terminal."},
    {"receiver_subscribe", receiver_subscribe_py, METH_VARARGS, "Record subscribe intent for a track."},
    {"receiver_unsubscribe", receiver_unsubscribe_py, METH_VARARGS, "Record unsubscribe intent for a track."},
    {"receiver_track_state", receiver_track_state_py, METH_VARARGS, "Read a track's delivery state."},
    {"receiver_poll_track", receiver_poll_track_py, METH_O, "Dequeue one track event as owned copies, or EMPTY/CLOSED."},
    {"track_key", track_key_py, METH_O, "Identity key of a track capsule for the wrapper's cache."},
    {"receiver_poll_object", receiver_poll_object_py, METH_O,
     "Dequeue one media object as owned copies (buffers released), or EMPTY/INTERRUPTED/CLOSED."},
#ifdef MOQ_PYTHON_TESTING
    MOQ_PYTHON_TEST_METHODS
#endif
    {NULL, NULL, 0, NULL}
};

static int traverse(PyObject *module, visitproc visit, void *arg)
{
    module_state_t *state = PyModule_GetState(module);
    if (state) {
        Py_VISIT(state->error);
        Py_VISIT(state->binding_error);
        Py_VISIT(state->event_lost);
        Py_VISIT(state->object_lost);
    }
    return 0;
}

static int clear(PyObject *module)
{
    module_state_t *state = PyModule_GetState(module);
    if (state) {
        Py_CLEAR(state->error);
        Py_CLEAR(state->binding_error);
        Py_CLEAR(state->event_lost);
        Py_CLEAR(state->object_lost);
    }
    return 0;
}

static void free_module(void *module)
{
    /* Module deallocation need not pass through the cyclic-GC clear hook. */
    (void)clear((PyObject *)module);
}

static struct PyModuleDef definition = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "moq5._native",
    .m_doc = "Private CPython bridge to the public LibMoQ service API.",
    .m_size = sizeof(module_state_t),
    .m_methods = methods,
    .m_traverse = traverse,
    .m_clear = clear,
    .m_free = free_module,
};

static int constant(PyObject *dict, const char *name, long value)
{
    PyObject *number = PyLong_FromLong(value);
    if (!number) return -1;
    int rc = PyDict_SetItemString(dict, name, number);
    Py_DECREF(number);
    return rc;
}

PyMODINIT_FUNC PyInit__native(void)
{
    PyObject *module = PyModule_Create(&definition);
    if (!module) return NULL;
    module_state_t *state = PyModule_GetState(module);
    state->error = PyErr_NewException("moq5._native.Error", PyExc_RuntimeError, NULL);
    if (!state->error || PyModule_AddObjectRef(module, "Error", state->error) < 0) goto fail;
    state->binding_error = PyErr_NewException("moq5._native.BindingError", PyExc_RuntimeError, NULL);
    if (!state->binding_error || PyModule_AddObjectRef(module, "BindingError", state->binding_error) < 0) goto fail;
    state->event_lost = PyErr_NewException("moq5._native.EventLost", state->binding_error, NULL);
    if (!state->event_lost || PyModule_AddObjectRef(module, "EventLost", state->event_lost) < 0) goto fail;
    state->object_lost = PyErr_NewException("moq5._native.ObjectLost", state->binding_error, NULL);
    if (!state->object_lost || PyModule_AddObjectRef(module, "ObjectLost", state->object_lost) < 0) goto fail;
    PyObject *constants = PyDict_New();
    if (!constants) goto fail;
#define ADD(name) do { if (constant(constants, #name, MOQ_##name) < 0) goto constants_fail; } while (0)
    ADD(OK); ADD(DONE); ADD(ERR_NOMEM); ADD(ERR_INVAL); ADD(ERR_PROTO);
    ADD(ERR_CLOSED); ADD(ERR_WRONG_STATE); ADD(ERR_STALE_HANDLE); ADD(ERR_WRONG_SESSION);
    ADD(ERR_WOULD_BLOCK); ADD(ERR_BUFFER); ADD(ERR_REQUEST_BLOCKED); ADD(ERR_ABI_MISMATCH);
    ADD(ERR_GOAWAY); ADD(ERR_INTERRUPTED); ADD(ERR_UNSUPPORTED); ADD(ERR_INTERNAL);
    ADD(TRANSPORT_PROTOCOL_AUTO); ADD(TRANSPORT_PROTOCOL_RAW_QUIC); ADD(TRANSPORT_PROTOCOL_WEBTRANSPORT);
    ADD(TRANSPORT_BACKEND_AUTO); ADD(TRANSPORT_BACKEND_PICOQUIC); ADD(TRANSPORT_BACKEND_MVFST);
    ADD(TRANSPORT_BACKEND_PROXYGEN); ADD(TRANSPORT_BACKEND_MSQUIC);
    ADD(TRANSPORT_BACKEND_WTQUIC_NETWORK); ADD(TRANSPORT_BACKEND_WTQUIC_MSQUIC);
    ADD(VERSION_DRAFT_16); ADD(VERSION_DRAFT_18); ADD(VERSION_DRAFT_21);
    ADD(WT_PROFILE_BACKEND_DEFAULT); ADD(WT_PROFILE_CURRENT); ADD(WT_PROFILE_D13_14_COMPAT);
    ADD(ENDPOINT_CONNECTING); ADD(ENDPOINT_ESTABLISHED); ADD(ENDPOINT_RECONNECTING);
    ADD(ENDPOINT_DRAINING); ADD(ENDPOINT_CLOSED);
    ADD(ENDPOINT_TERMINAL_NONE); ADD(ENDPOINT_TERMINAL_CLEAN); ADD(ENDPOINT_TERMINAL_PROTOCOL);
    ADD(ENDPOINT_TERMINAL_TLS_CERTIFICATE); ADD(ENDPOINT_TERMINAL_TLS); ADD(ENDPOINT_TERMINAL_TRANSPORT);
    ADD(MEDIA_OVERFLOW_DROP_TO_KEYFRAME); ADD(MEDIA_OVERFLOW_DROP_GROUP); ADD(MEDIA_OVERFLOW_FLOW_CONTROL);
    ADD(MEDIA_SEND_BP_DROP_TO_KEYFRAME); ADD(MEDIA_SEND_BP_DROP_GROUP);
    ADD(MEDIA_SEND_BP_BLOCK_TIMEOUT); ADD(MEDIA_SEND_BP_RETURN_WOULD_BLOCK);
    ADD(MEDIA_TIME_RAW); ADD(MEDIA_TIME_SHARED_EPOCH);
    ADD(MEDIA_START_CURRENT); ADD(MEDIA_START_NEXT_GROUP);
    ADD(MEDIA_TRACK_ADDED); ADD(MEDIA_TRACK_UPDATED); ADD(MEDIA_TRACK_REMOVED); ADD(MEDIA_TRACK_ENDED);
    ADD(MEDIA_CATALOG_READY); ADD(MEDIA_TRACK_UPDATE_OK); ADD(MEDIA_TRACK_PARSE_DROP);
    ADD(MEDIA_TRACK_STATE_DISCOVERED); ADD(MEDIA_TRACK_STATE_PENDING); ADD(MEDIA_TRACK_STATE_ACTIVE);
    ADD(MEDIA_TRACK_STATE_PAUSED_APP); ADD(MEDIA_TRACK_STATE_PAUSED_FLOW); ADD(MEDIA_TRACK_STATE_ENDED);
    ADD(MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE); ADD(MEDIA_RECEIVER_FATAL_EVENT_OVERFLOW);
    ADD(MEDIA_RECEIVER_FATAL_SETUP_FAILED); ADD(MEDIA_RECEIVER_FATAL_CATALOG_REJECTED);
    ADD(MEDIA_PACKAGING_RAW); ADD(MEDIA_PACKAGING_CMAF); ADD(MEDIA_TYPE_VIDEO); ADD(MEDIA_TYPE_AUDIO);
    ADD(OBJECT_NORMAL); ADD(OBJECT_END_OF_GROUP); ADD(OBJECT_END_OF_TRACK);
    ADD(CMAF_CODEC_UNKNOWN); ADD(CMAF_CODEC_AVC); ADD(CMAF_CODEC_HEVC); ADD(CMAF_CODEC_AV1);
    ADD(CMAF_CODEC_AAC); ADD(CMAF_CODEC_OPUS);
    ADD(MEDIA_PARSE_DROP_MEDIA); ADD(MEDIA_PARSE_DROP_SAP); ADD(MEDIA_PARSE_DROP_MEDIA_TIMELINE);
    ADD(SAP_NONE); ADD(SAP_TYPE_1); ADD(SAP_TYPE_2); ADD(SAP_TYPE_3);
#undef ADD
    if (PyModule_AddObject(module, "constants", constants) < 0) goto constants_fail;
    return module;
constants_fail:
    Py_DECREF(constants);
fail:
    Py_DECREF(module);
    return NULL;
}
