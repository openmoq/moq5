#ifndef MOQ_VERSION_H
#define MOQ_VERSION_H

#include "export.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_VERSION_MAJOR 0
#define MOQ_VERSION_MINOR 1
#define MOQ_VERSION_PATCH 0

#define MOQ_VERSION_NUMBER \
    ((MOQ_VERSION_MAJOR * 10000) + (MOQ_VERSION_MINOR * 100) + MOQ_VERSION_PATCH)

#define MOQ_VERSION_STRING "0.1.0"

MOQ_API uint32_t    moq_version_number(void);
MOQ_API const char *moq_version_string(void);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_VERSION_H */
