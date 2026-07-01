#ifndef MOQ_TEST_SUPPORT_H
#define MOQ_TEST_SUPPORT_H

#include <stdio.h>

#define MOQ_TEST_CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define MOQ_TEST_CHECK_EQ_INT(actual, expected) do { \
    int _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %d, expected %s == %d\n", \
                __FILE__, __LINE__, #actual, _a, #expected, _e); \
        failures++; \
    } \
} while (0)

#define MOQ_TEST_CHECK_EQ_U64(actual, expected) do { \
    uint64_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %llu, expected %s == %llu\n", \
                __FILE__, __LINE__, #actual, (unsigned long long)_a, \
                #expected, (unsigned long long)_e); \
        failures++; \
    } \
} while (0)

#define MOQ_TEST_CHECK_EQ_SIZE(actual, expected) do { \
    size_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %zu, expected %s == %zu\n", \
                __FILE__, __LINE__, #actual, _a, #expected, _e); \
        failures++; \
    } \
} while (0)

#define MOQ_TEST_CHECK_EQ_HEX(actual, expected) do { \
    uint64_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == 0x%llx, expected %s == 0x%llx\n", \
                __FILE__, __LINE__, #actual, (unsigned long long)_a, \
                #expected, (unsigned long long)_e); \
        failures++; \
    } \
} while (0)

#define MOQ_TEST_PASS(name) do { \
    if (failures == 0) printf("PASS: %s\n", (name)); \
} while (0)

#endif /* MOQ_TEST_SUPPORT_H */
