/*
 * Consumer smoke: find_package(libmoq COMPONENTS
 * adapter-msquic-managed) must resolve moq::adapter-msquic-managed AND
 * its transitive attach + msquic::msquic dependencies. The references
 * below touch both public headers and force real symbol resolution
 * against both installed libraries — one attach symbol, one managed
 * symbol.
 */

#include <moq/msquic.h>
#include <moq/msquic_managed.h>

#include <string.h>

#if defined(__linux__)
#include <link.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

static int print_loaded_image(struct dl_phdr_info *info, size_t size, void *data)
{
    const char *name = info->dlpi_name;
    char *path;
    struct stat st;
    int failed;

    (void)size;
    (void)data;
    if (name == NULL)
        return 1;
    /* The main executable has an empty name; these vDSOs have no disk file. */
    if (name[0] == '\0' || strcmp(name, "linux-vdso.so.1") == 0 ||
        strcmp(name, "linux-gate.so.1") == 0)
        return 0;
    path = realpath(name, NULL);
    if (path == NULL) {
        perror("loaded image realpath");
        return 1;
    }
    /* Keep each raw record representable as one CMake list element. */
    if (path[0] != '/' || strpbrk(path, "\t\r\n[]\\") != NULL) {
        fprintf(stderr, "loaded image path cannot be represented\n");
        free(path);
        return 1;
    }
    if (stat(path, &st) != 0) {
        perror("loaded image stat");
        free(path);
        return 1;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "loaded image is not a regular file\n");
        free(path);
        return 1;
    }
    failed = printf("MOQ_LOADED_IMAGE\t%s\n", path) < 0;
    free(path);
    return failed;
}
#endif

int main(int argc, char **argv)
{
    moq_msquic_conn_cfg_t cfg;
    moq_msquic_managed_cfg_t mcfg;
    QUIC_SETTINGS settings;

    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    moq_msquic_settings_init(&settings);
    moq_msquic_managed_cfg_init_sized(&mcfg, sizeof(mcfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (mcfg.struct_size != sizeof(mcfg))
        return 1;
    if (settings.SendBufferingEnabled)
        return 1;
    if (moq_msquic_conn_callback() == NULL)
        return 1;
    if (moq_msquic_managed_port(NULL) != 0)
        return 1;
    if (argc == 1)
        return 0;
#if defined(__linux__)
    if (argc != 2 || strcmp(argv[1], "--list-images") != 0)
        return 1;
    if (dl_iterate_phdr(print_loaded_image, NULL) != 0)
        return 1;
    if (fflush(stdout) != 0 || ferror(stdout))
        return 1;
    return 0;
#else
    (void)argv;
    return 1;
#endif
}
