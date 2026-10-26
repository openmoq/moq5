/* Compile against the actual pre-cluster declaration, not a cast of a
 * current struct. On ARM ILP32 this caller is 20 bytes/alignment 4. */
#include <moq/types.h>
#include <string.h>
#include <stdio.h>

typedef struct moq_session moq_session_t;
typedef struct legacy_publish_cfg {
    uint32_t struct_size;
    moq_namespace_t track_namespace;
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
} legacy_publish_cfg_t;
extern void moq_publish_namespace_cfg_init(legacy_publish_cfg_t *cfg);
extern moq_result_t moq_session_publish_namespace(moq_session_t *s,
    const legacy_publish_cfg_t *cfg, uint64_t now, moq_announcement_t *out);

int cluster_legacy_cfg_case(moq_session_t *s, moq_announcement_t *ann)
{
    struct { uint32_t lead; legacy_publish_cfg_t cfg; uint8_t guard[8]; } old;
    memset(&old, 0xa5, sizeof(old));
    moq_publish_namespace_cfg_init(&old.cfg);
    if (old.cfg.struct_size != sizeof(old.cfg) || old.cfg.auth_tokens != NULL ||
        old.cfg.auth_token_count != 0 || old.lead != UINT32_C(0xa5a5a5a5)) return 1;
    for (size_t i = 0; i < sizeof(old.guard); i++)
        if (old.guard[i] != 0xa5) { fprintf(stderr, "legacy cfg overrun\n"); return 1; }
    moq_bytes_t part = {(const uint8_t *)"legacy", 6};
    old.cfg.track_namespace = (moq_namespace_t){&part, 1};
    return moq_session_publish_namespace(s, &old.cfg, 2, ann) != MOQ_OK || ann->_opaque == 0;
}

typedef struct legacy_update_cfg {
    uint32_t struct_size;
    uint32_t flags;
    moq_namespace_t track_namespace_prefix;
    bool forward;
    uint8_t reserved[7];
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
} legacy_update_cfg_t;
_Static_assert(sizeof(legacy_update_cfg_t) == (sizeof(void *) == 8 ? 48 : 32),
               "actual historical update extent");
_Static_assert(_Alignof(legacy_update_cfg_t) == sizeof(void *), "old pointer alignment");
extern void moq_namespace_update_cfg_init(legacy_update_cfg_t *cfg);
extern moq_result_t moq_session_update_namespace(moq_session_t *s, moq_announcement_t ann,
    const legacy_update_cfg_t *cfg, uint64_t now, uint64_t *id);

int cluster_legacy_update_cfg_case(moq_session_t *s, moq_announcement_t ann, uint64_t *id)
{
    /* On ARM ILP32 the actual old object starts four bytes into an eight-
     * aligned allocation. No cast to today's enlarged type supplies alignment. */
    _Alignas(8) struct { uint32_t lead; legacy_update_cfg_t cfg; uint8_t guard[8]; } old;
    memset(&old, 0xa5, sizeof(old));
    moq_namespace_update_cfg_init(&old.cfg);
    if (old.cfg.struct_size != sizeof(old.cfg) || old.cfg.flags != 0 ||
        old.cfg.auth_tokens != NULL || old.cfg.auth_token_count != 0 ||
        old.lead != UINT32_C(0xa5a5a5a5)) return 1;
    if (sizeof(void *) == 4 && ((uintptr_t)&old.cfg & 7u) != 4u) return 1;
    for (size_t i = 0; i < sizeof(old.guard); i++)
        if (old.guard[i] != 0xa5) return 1;
    const uint8_t bytes[] = {'o', 'l', 'd'};
    moq_auth_token_t token = {0, {bytes, sizeof(bytes)}};
    old.cfg.auth_tokens = &token;
    old.cfg.auth_token_count = 1;
    moq_result_t rc = moq_session_update_namespace(s, ann, &old.cfg, 4, id);
    for (size_t i = 0; i < sizeof(old.guard); i++)
        if (old.guard[i] != 0xa5) return 1;
    return rc != MOQ_OK;
}
