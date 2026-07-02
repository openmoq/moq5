#ifndef MOQ_SERVICE_SHIM_H
#define MOQ_SERVICE_SHIM_H

/* The installed C service tier (pkg-config: libmoq-service). The service
 * headers pull types.h/session.h/media_object.h/cmaf.h/msf.h transitively;
 * rcbuf.h is included for the sender's payload buffers. */
#include <moq/endpoint.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#endif
