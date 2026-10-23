#ifndef MOQ_TEST_PUBLISH_EVENT_CLIENT_H
#define MOQ_TEST_PUBLISH_EVENT_CLIENT_H

#include <moq/session.h>
#include <stddef.h>

/* An unchanged public-record aggregate from before group-order scheduling.
 * Compile this same client as C and C++: preserving offsets through padding
 * is insufficient if an insertion shifts the positional member sequence. */
static bool unchanged_publish_event_client(void)
{
    moq_publish_request_event_t event = {
        {0}, {NULL, 0}, {NULL, 0}, 0, false, {0}, NULL, 0,
        {NULL, 0}, false, true, 10, 20, true, 30
    };
    return event.has_largest && event.largest_group == 10 &&
           event.largest_object == 20 && event.has_expires && event.expires_ms == 30;
}

#endif
