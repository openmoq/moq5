/*
 * C++ include/link smoke test for <moq/mvfst.hpp>.
 *
 * Proves the C++ header compiles, the adapter class is declared,
 * and the library links against mvfst without unresolved symbols.
 */

#include <moq/mvfst.hpp>
#include <cstdio>

int main()
{
    /* Verify C API is accessible from C++. */
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);

    /* Verify C++ types are complete (sizeof compiles). */
    static_assert(sizeof(moq::mvfst::adapter::config) > 0,
                  "adapter::config must be a complete type");

    /* Verify adapter constructor rejects null session. */
    {
        bool caught = false;
        try {
            auto ac = moq::mvfst::adapter::config::client();
            moq::mvfst::adapter a(ac, nullptr, nullptr);
        } catch (const std::invalid_argument &) {
            caught = true;
        }
        if (!caught) {
            std::fprintf(stderr, "FAIL: null session not rejected\n");
            return 1;
        }
    }

    /* Verify adapter constructor rejects null socket. */
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                              MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *sess = nullptr;
        if (moq_session_create(&scfg, 0, &sess) < 0) {
            std::fprintf(stderr, "FAIL: session create\n");
            return 1;
        }
        bool caught = false;
        try {
            auto ac = moq::mvfst::adapter::config::client();
            moq::mvfst::adapter a(ac, sess, nullptr);
        } catch (const std::invalid_argument &) {
            caught = true;
        }
        moq_session_destroy(sess);
        if (!caught) {
            std::fprintf(stderr, "FAIL: null socket not rejected\n");
            return 1;
        }
    }

    /* Verify adapter rejects perspective mismatch. */
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                              MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *sess = nullptr;
        if (moq_session_create(&scfg, 0, &sess) < 0) {
            std::fprintf(stderr, "FAIL: session create\n");
            return 1;
        }
        bool caught = false;
        try {
            auto ac = moq::mvfst::adapter::config::server();
            moq::mvfst::adapter a(ac, sess, nullptr);
        } catch (const std::invalid_argument &) {
            caught = true;
        }
        moq_session_destroy(sess);
        if (!caught) {
            std::fprintf(stderr, "FAIL: perspective mismatch not rejected\n");
            return 1;
        }
    }

    /* Verify moq_session_perspective. */
    {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                              MOQ_PERSPECTIVE_SERVER);
        moq_session_t *sess = nullptr;
        if (moq_session_create(&scfg, 0, &sess) < 0) {
            std::fprintf(stderr, "FAIL: session create\n");
            return 1;
        }
        if (moq_session_perspective(sess) != MOQ_PERSPECTIVE_SERVER) {
            std::fprintf(stderr, "FAIL: perspective not SERVER\n");
            moq_session_destroy(sess);
            return 1;
        }
        moq_session_destroy(sess);
    }

    std::printf("PASS: test_mvfst_cxx_smoke\n");
    return 0;
}
