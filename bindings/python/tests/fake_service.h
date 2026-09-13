#ifndef MOQ_PYTHON_FAKE_SERVICE_H
#define MOQ_PYTHON_FAKE_SERVICE_H

PyObject *test_reset(PyObject *, PyObject *);
PyObject *test_counts(PyObject *, PyObject *);
PyObject *test_config(PyObject *, PyObject *);
PyObject *test_connect_result(PyObject *, PyObject *);
PyObject *test_stop_result(PyObject *, PyObject *);
PyObject *test_wait_result(PyObject *, PyObject *);
PyObject *test_terminal(PyObject *, PyObject *);
PyObject *test_block_wait(PyObject *, PyObject *);
PyObject *test_wait_entered(PyObject *, PyObject *);
PyObject *test_block_stop(PyObject *, PyObject *);
PyObject *test_stop_entered(PyObject *, PyObject *);
PyObject *test_snapshot(PyObject *, PyObject *);
PyObject *test_terminal_size(PyObject *, PyObject *);
PyObject *test_unblock(PyObject *, PyObject *);
PyObject *test_force_watchdog(PyObject *, PyObject *);
PyObject *test_watchdog_fired(PyObject *, PyObject *);
PyObject *test_force_pthread_error(PyObject *, PyObject *);
PyObject *test_pthread_error(PyObject *, PyObject *);

#define MOQ_PYTHON_TEST_METHODS \
    {"_test_reset", test_reset, METH_NOARGS, NULL}, \
    {"_test_counts", test_counts, METH_NOARGS, NULL}, \
    {"_test_config", test_config, METH_NOARGS, NULL}, \
    {"_test_connect_result", test_connect_result, METH_O, NULL}, \
    {"_test_stop_result", test_stop_result, METH_O, NULL}, \
    {"_test_wait_result", test_wait_result, METH_O, NULL}, \
    {"_test_terminal", test_terminal, METH_VARARGS, NULL}, \
    {"_test_block_wait", test_block_wait, METH_NOARGS, NULL}, \
    {"_test_wait_entered", test_wait_entered, METH_NOARGS, NULL}, \
    {"_test_block_stop", test_block_stop, METH_NOARGS, NULL}, \
    {"_test_stop_entered", test_stop_entered, METH_NOARGS, NULL}, \
    {"_test_snapshot", test_snapshot, METH_VARARGS, NULL}, \
    {"_test_terminal_size", test_terminal_size, METH_O, NULL}, \
    {"_test_unblock", test_unblock, METH_NOARGS, NULL}, \
    {"_test_force_watchdog", test_force_watchdog, METH_NOARGS, NULL}, \
    {"_test_watchdog_fired", test_watchdog_fired, METH_NOARGS, NULL}, \
    {"_test_force_pthread_error", test_force_pthread_error, METH_O, NULL}, \
    {"_test_pthread_error", test_pthread_error, METH_NOARGS, NULL},

#endif
