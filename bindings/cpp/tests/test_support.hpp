#ifndef MOQ_CPP_TEST_SUPPORT_HPP
#define MOQ_CPP_TEST_SUPPORT_HPP

#include <cstdio>

#define MOQ_CHECK(expr)                                                     \
    do {                                                                    \
        if (!(expr)) {                                                      \
            std::fprintf(stderr, "FAIL: %s:%d: %s\n",                      \
                         __FILE__, __LINE__, #expr);                        \
            failures++;                                                     \
        }                                                                   \
    } while (0)

#define MOQ_PASS(name)                                                      \
    do {                                                                    \
        if (failures == 0)                                                  \
            std::printf("PASS: %s\n", (name));                             \
    } while (0)

#endif // MOQ_CPP_TEST_SUPPORT_HPP
