/* Compile-only test: verifies <moq/picoquic.h> is usable by consumers
 * linking only moq::adapter-picoquic. */
#include <moq/picoquic.h>

int main(void) {
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    (void)cfg;
    return 0;
}
