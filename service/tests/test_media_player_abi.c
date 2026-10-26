/* The media player's public records, pinned on LP64, i386 ILP32 (4-aligned
 * uint64_t) and ARM ILP32 (8-aligned), and independent of the consumer's
 * enum width. Each record's v0 floor is its complete first layout. */
#include <moq/media_player.h>

#include <stdio.h>
#include <string.h>

typedef struct abi_u64_probe { char c; uint64_t v; } abi_u64_probe_t;
#define ABI_U64_ALIGN offsetof(abi_u64_probe_t, v)
#define ABI_PICK(lp64, ilp32_a4, ilp32_a8) \
    (sizeof(void *) == 8 ? (size_t)(lp64) \
     : ABI_U64_ALIGN == 4 ? (size_t)(ilp32_a4) : (size_t)(ilp32_a8))
#define PIN_AT(T, f, a, b, c) \
    _Static_assert(offsetof(T, f) == ABI_PICK(a, b, c), #T "." #f)
#define PIN_SIZE(T, a, b, c) \
    _Static_assert(sizeof(T) == ABI_PICK(a, b, c), #T ": sizeof")

PIN_AT(moq_media_player_cfg_t, receiver, 8, 8, 8);
PIN_AT(moq_media_player_cfg_t, preferred_lang, 24, 16, 16);
PIN_AT(moq_media_player_cfg_t, end_tail_wait_us, 40, 24, 24);
PIN_AT(moq_media_player_cfg_t, max_held_bytes, 72, 56, 56);
PIN_AT(moq_media_player_cfg_t, assume_shared_epoch, 80, 64, 64);
PIN_SIZE(moq_media_player_cfg_t, 88, 68, 72);
_Static_assert(MOQ_MEDIA_PLAYER_CFG_V0_SIZE ==
               offsetof(moq_media_player_cfg_t, assume_shared_epoch) + 1, "cfg v0");

PIN_AT(moq_media_player_select_cfg_t, shared_epoch, 9, 9, 9);
PIN_SIZE(moq_media_player_select_cfg_t, 12, 12, 12);

PIN_SIZE(moq_media_timeline_t, 40, 40, 40);
PIN_AT(moq_media_player_output_t, configuration_generation, 16, 16, 16);
PIN_AT(moq_media_player_output_t, track, 24, 24, 24);
PIN_AT(moq_media_player_output_t, u, 32, 28, 32);
PIN_AT(moq_media_player_output_t, u.unit.payload, 88, 84, 88);
PIN_AT(moq_media_player_output_t, u.timeline.rate, 64, 60, 64);
PIN_SIZE(moq_media_player_output_t, 144, 140, 144);

PIN_AT(moq_media_player_event_t, track, 16, 16, 16);
PIN_AT(moq_media_player_event_t, u, 24, 20, 24);
PIN_SIZE(moq_media_player_event_t, 72, 68, 72);

PIN_AT(moq_media_player_feedback_t, configuration_generation, 16, 12, 16);
PIN_AT(moq_media_player_feedback_t, u, 24, 20, 24);
PIN_SIZE(moq_media_player_feedback_t, 56, 52, 56);
_Static_assert(MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE == ABI_PICK(56, 52, 56), "feedback v0");

PIN_AT(moq_media_player_stats_t, outputs_queued, 8, 8, 8);
PIN_AT(moq_media_player_stats_t, feedback_stale, 128, 128, 128);
PIN_AT(moq_media_player_stats_t, outputs_bytes, 136, 136, 136);
PIN_AT(moq_media_player_stats_t, selections_failed, 144, 144, 144);
PIN_SIZE(moq_media_player_stats_t, 152, 152, 152);
_Static_assert(MOQ_MEDIA_PLAYER_STATS_V0_SIZE == 152, "stats v0");
_Static_assert(MOQ_MEDIA_PLAYER_MAX_QUEUE == 65536u, "queue bound");

int main(void)
{
    int failures = 0;
    moq_media_player_cfg_t c;
    memset(&c, 0xEE, sizeof(c));
    moq_media_player_cfg_init_sized(&c, MOQ_MEDIA_PLAYER_CFG_V0_SIZE);
    failures += c.struct_size != MOQ_MEDIA_PLAYER_CFG_V0_SIZE;
    failures += c.selection != 0 || c.receiver != NULL || c.max_held_bytes != 0;
    moq_media_player_cfg_init_sized(&c, 2);   /* below a struct_size: nothing */
    failures += c.struct_size != MOQ_MEDIA_PLAYER_CFG_V0_SIZE;
    moq_media_player_select_cfg_t s;
    moq_media_player_select_cfg_init(&s);
    failures += s.struct_size != sizeof(s) || s.start != 0 || s.independent_timing;
    moq_media_player_feedback_t f;
    moq_media_player_feedback_init(&f);
    failures += f.struct_size != sizeof(f) || f.kind != 0;
    if (failures) {
        printf("FAIL: media player abi: %d\n", failures);
        return 1;
    }
    printf("PASS: media player abi\n");
    return 0;
}
