#ifndef MOQ_INTERNAL_FILL_H
#define MOQ_INTERNAL_FILL_H

#include <moq/session.h>

/* Retain the request-time selection: later updates must not change an older
 * fill's range while its response or data stream is still in flight. */
typedef struct moq_fill_selection {
    moq_fill_request_t fill;
    moq_subscribe_filter_t filter;
    uint64_t start_group, start_object, end_group;
} moq_fill_selection_t;

bool moq_fill_selection_has_content(const moq_fill_selection_t *selection,
    bool has_largest, uint64_t largest_group, uint64_t largest_object);

#endif
