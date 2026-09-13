#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <moq/endpoint.h>
#include <moq/version.h>
#include <limits.h>
#include <stdint.h>
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
    PyObject *error;
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
#ifdef MOQ_PYTHON_TESTING
    MOQ_PYTHON_TEST_METHODS
#endif
    {NULL, NULL, 0, NULL}
};

static int traverse(PyObject *module, visitproc visit, void *arg)
{
    module_state_t *state = PyModule_GetState(module);
    if (state) Py_VISIT(state->error);
    return 0;
}

static int clear(PyObject *module)
{
    module_state_t *state = PyModule_GetState(module);
    if (state) Py_CLEAR(state->error);
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
    ADD(VERSION_DRAFT_16); ADD(VERSION_DRAFT_18);
    ADD(WT_PROFILE_BACKEND_DEFAULT); ADD(WT_PROFILE_CURRENT); ADD(WT_PROFILE_D13_14_COMPAT);
    ADD(ENDPOINT_CONNECTING); ADD(ENDPOINT_ESTABLISHED); ADD(ENDPOINT_RECONNECTING);
    ADD(ENDPOINT_DRAINING); ADD(ENDPOINT_CLOSED);
    ADD(ENDPOINT_TERMINAL_NONE); ADD(ENDPOINT_TERMINAL_CLEAN); ADD(ENDPOINT_TERMINAL_PROTOCOL);
    ADD(ENDPOINT_TERMINAL_TLS_CERTIFICATE); ADD(ENDPOINT_TERMINAL_TLS); ADD(ENDPOINT_TERMINAL_TRANSPORT);
#undef ADD
    if (PyModule_AddObject(module, "constants", constants) < 0) goto constants_fail;
    return module;
constants_fail:
    Py_DECREF(constants);
fail:
    Py_DECREF(module);
    return NULL;
}
