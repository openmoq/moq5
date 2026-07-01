#ifndef MOQ_CODEC_H
#define MOQ_CODEC_H

/*
 * Wire-profile codec API.
 *
 * This header exposes draft-specific control message encode/decode
 * functions. It is intended for:
 *   - Session engine internals
 *   - QUIC adapters
 *   - Fuzz targets
 *   - Unit/integration tests
 *   - Diagnostics and interop tools
 *
 * Application code should normally include <moq/moq.h> and use the
 * future moq_session_* APIs instead of calling codec functions directly.
 *
 * Draft-qualified names (moq_d16_*) are expected here and are not
 * the application-level API. They will grow as more control messages
 * are implemented and as future wire profiles (draft-18, RFC v1) are
 * added.
 */

#include "moq.h"
#include "wire.h"
#include "vi64.h"
#include "buf.h"
#include "kvp.h"
#include "control.h"

#endif /* MOQ_CODEC_H */
