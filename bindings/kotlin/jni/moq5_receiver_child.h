/* The receiver child's private payload.
 *
 * A receiver is an OWNED registry child of its endpoint root. What the
 * registry holds for it is not the bare `moq_media_receiver_t *` but this
 * wrapper: the receiver, plus the per-receiver map from a native track
 * handle (stable, never recycled, valid until the receiver is destroyed) to
 * the BORROWED child token minted for it on the bridge's first sight of the
 * handle -- from a track event or from an object, whichever the rows meet
 * first. Track tokens are
 * children of the receiver child, so retiring the receiver invalidates every
 * one of them through the registry's ordinary subtree rule; there is no
 * process-global track map. The owned cleanup destroys the receiver and frees
 * the map exactly once.
 *
 * Shared between the product bridge and the host-only test recorder, which
 * needs the receiver behind a token to drive the service's own testing seam.
 * Never installed. */
#ifndef MOQ5_RECEIVER_CHILD_H
#define MOQ5_RECEIVER_CHILD_H

#include <stddef.h>

#include <moq/media_receiver.h>

#include "moq5_registry.h"

typedef struct moq5_track_slot {
    const moq_media_track_t *track;
    moq5_handle_t            token;
} moq5_track_slot_t;

typedef struct moq5_receiver_child {
    moq_media_receiver_t *r;
    moq5_track_slot_t    *tracks;
    size_t                count;
    size_t                cap;
} moq5_receiver_child_t;

#endif /* MOQ5_RECEIVER_CHILD_H */
