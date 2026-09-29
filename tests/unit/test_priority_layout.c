/* Also compiled freestanding for ARM ILP32: no host runtime required. */
#include <moq/session.h>
#include <moq/transport_bridge.h>

typedef struct {
    moq_stream_ref_t stream_ref;
    uint8_t header[32];
    uint8_t header_len;
    moq_rcbuf_t *payload;
    bool fin;
} legacy_send_data_t;

#define SAME_OFFSET(member) \
    _Static_assert(offsetof(moq_send_data_action_t, member) == \
                   offsetof(legacy_send_data_t, member), #member " moved")
SAME_OFFSET(stream_ref);
SAME_OFFSET(header);
SAME_OFFSET(header_len);
SAME_OFFSET(payload);
SAME_OFFSET(fin);
_Static_assert(offsetof(moq_send_data_action_t, scheduling_priority) >=
               sizeof(legacy_send_data_t), "new field reuses legacy padding");
_Static_assert(sizeof(moq_send_data_action_t) <= MOQ_ACTION_DETAIL_MAX,
               "new field grows the action union");
_Static_assert(sizeof(((moq_action_t *)0)->u) == MOQ_ACTION_DETAIL_MAX,
               "action detail reserve changed");

int main(void)
{
    moq_transport_endpoint_ops_t ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    return ops.set_stream_priority != NULL;
}
