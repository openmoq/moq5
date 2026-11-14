/* A program that needs only the protocol core. Built as C and as C++ from
 * `pkg-config --cflags --libs libmoq-core`. Silent on success. */
#include <stdio.h>

#include <moq/session.h>

#define CHECK(expr) do { if (!(expr)) { \
    fprintf(stderr, "tier consumer core:%d: %s\n", __LINE__, #expr); \
    return 1; \
} } while (0)

int main(void)
{
    moq_session_cfg_t cfg;
    moq_session_t *session = NULL;
    moq_done_subscribe_cfg_t done;

    /* A session reaches well past one object of the library: the session
     * machine, its wire profile and the allocator must all link. */
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    CHECK(moq_session_create(&cfg, 0, &session) == MOQ_OK);
    CHECK(session != NULL);
    CHECK(moq_session_perspective(session) == MOQ_PERSPECTIVE_SERVER);
    moq_session_destroy(session);

    /* A sized initializer through its macro, then the exported symbol. */
    moq_done_subscribe_cfg_init(&done);
    CHECK(done.struct_size == sizeof(done));
    (moq_done_subscribe_cfg_init)(&done);
    CHECK(done.struct_size == MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE);
    return 0;
}
