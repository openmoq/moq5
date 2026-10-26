package org.openmoq.moq5.samples.player

import org.openmoq.moq5.WireIdentity

/**
 * The diagnostic breadcrumb for one delivered object's public wire identity, built only when the
 * trace is on and read by nothing in the player. The four fields are printed verbatim as the binding
 * delivered them: an absent subgroup (a datagram) prints `subgroup=none`, distinct from `subgroup=0`,
 * and an object the binding delivered without any identity prints `absent`. The offline oracle that
 * compares these lines against the publisher's own decisions depends on exactly this shape.
 */
internal fun identityBreadcrumb(lane: PlaybackLane?, identity: WireIdentity?): String {
    val head = "object identity lane=" + when (lane) { PlaybackLane.Video -> "video"; PlaybackLane.Audio -> "audio"; null -> "other" }
    if (identity == null) return "$head absent"
    val subgroup = identity.subgroup?.toString() ?: "none"
    return "$head group=${identity.group} subgroup=$subgroup object=${identity.objectId} priority=${identity.publisherPriority}"
}
