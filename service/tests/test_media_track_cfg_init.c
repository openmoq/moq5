#include <moq/media_sender.h>
#include <stdio.h>
#include <string.h>

static unsigned failures;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL:%d: %s\n", __LINE__, #x); failures++; } } while (0)

int main(void)
{
    union {
        moq_media_track_cfg_t alignment;
        unsigned char bytes[sizeof(moq_media_track_cfg_t) + 32];
    } storage;
    size_t old_size = (offsetof(moq_media_track_cfg_t, alt_group) +
        sizeof(((moq_media_track_cfg_t *)0)->alt_group) +
        _Alignof(moq_media_track_cfg_t) - 1) &
        ~((size_t)_Alignof(moq_media_track_cfg_t) - 1);
    memset(&storage, 0xa5, sizeof(storage));
    void (*legacy)(moq_media_track_cfg_t *) = &moq_media_track_cfg_init;
    legacy((moq_media_track_cfg_t *)(void *)storage.bytes);
    CHECK(storage.alignment.struct_size == old_size);
    CHECK(storage.alignment.is_live);
    for (size_t i = old_size; i < sizeof(storage.bytes); i++)
        CHECK(storage.bytes[i] == 0xa5);
    CHECK(offsetof(moq_media_track_cfg_t, has_publisher_priority) >= old_size);
    CHECK(MOQ_MEDIA_TRACK_CFG_V0_SIZE ==
          offsetof(moq_media_track_cfg_t, has_track_duration));
    for (size_t size = 0; size <= sizeof(storage.bytes); size++) {
        unsigned char expected[sizeof(storage.bytes)];
        memset(expected, 0xa5, sizeof(expected));
        memset(&storage, 0xa5, sizeof(storage));
        if (size >= sizeof(uint32_t)) {
            size_t n = size < sizeof(moq_media_track_cfg_t) ? size :
                       sizeof(moq_media_track_cfg_t);
            memset(expected, 0, n);
            uint32_t stamp = (uint32_t)n;
            memcpy(expected, &stamp, sizeof(stamp));
            if (n >= offsetof(moq_media_track_cfg_t, is_live) + sizeof(bool))
                expected[offsetof(moq_media_track_cfg_t, is_live)] = 1;
        }
        moq_media_track_cfg_init_sized(&storage.alignment, size);
        CHECK(memcmp(storage.bytes, expected, sizeof(expected)) == 0);
    }
    memset(&storage, 0xa5, sizeof(storage));
    moq_media_track_cfg_init_sized(&storage.alignment, SIZE_MAX);
    CHECK(storage.alignment.struct_size == sizeof(moq_media_track_cfg_t));
    for (size_t i = sizeof(moq_media_track_cfg_t); i < sizeof(storage.bytes); i++)
        CHECK(storage.bytes[i] == 0xa5);
    moq_media_track_cfg_init_sized(NULL, SIZE_MAX);
    moq_media_track_cfg_init(&storage.alignment);
    CHECK(storage.alignment.struct_size == sizeof(moq_media_track_cfg_t));
    printf("track config initializer: %u failures\n", failures);
    return failures ? 1 : 0;
}
