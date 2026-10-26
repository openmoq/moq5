package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnixMicros

internal data class PlaybackCandidate<T : Any>(val handle: T, val description: TrackDescription, val present: Boolean)
internal data class PlaybackSelected<T : Any>(
    val video: T, val videoFormat: FormatOutcome.Video,
    val audio: T?, val audioFormat: FormatOutcome.Audio?,
)

/** Catalog-order selection, not ABR. Do not silently mute an advertised audio track. */
internal fun <T : Any> selectPlayback(candidates: List<PlaybackCandidate<T>>): PlaybackSelected<T>? {
    fun eligible(type: MediaType) = candidates.filter { it.present && it.description.mediaType == type }
    fun format(candidate: PlaybackCandidate<T>): FormatOutcome {
        val d = candidate.description
        if (d.depends.isNotEmpty()) return FormatOutcome.Unsupported("dependent track")
        if (d.contentProtectionRefIds.isNotEmpty() || d.init?.cenc != null) {
            return FormatOutcome.Unsupported("protected track")
        }
        if ((d.init?.codecConfig ?: d.initData)?.size?.let { it > 1024 * 1024 } == true) {
            return FormatOutcome.Unsupported("codec configuration exceeds the sample bound")
        }
        return DecoderFormats.of(MediaFacts.ofTrack(d))
    }
    val videos = eligible(MediaType.Video)
    if (videos.isEmpty()) return null
    val audios = eligible(MediaType.Audio)
    val audioFormats = audios.mapNotNull { c -> (format(c) as? FormatOutcome.Audio)?.let { c.handle to it } }
    for (candidate in videos) {
        val v = format(candidate) as? FormatOutcome.Video ?: continue
        if (audios.isEmpty()) return PlaybackSelected(candidate.handle, v, null, null)
        val a = audioFormats.firstOrNull { it.second.packaging == v.packaging } ?: continue
        return PlaybackSelected(candidate.handle, v, a.first, a.second)
    }
    throw PlaybackRenderRefused("no present independent unencrypted supported video/audio selection in one timing domain")
}

internal data class PlaybackTimedUnit(val unit: AccessUnitOutcome.Unit, val logicalUs: Long)

/** One mapper per broadcast, shared by both selected tracks. */
internal class PlaybackReceiveMedia(
    private val video: FormatOutcome.Video,
    private val audio: FormatOutcome.Audio?,
    private val maxPayloadBytes: Int = 8 * 1024 * 1024,
    private val maxSamples: Int = 1024,
) {
    init {
        require(maxPayloadBytes > 0 && maxSamples > 0)
        require(audio == null || audio.packaging == video.packaging) { "mixed RAW/CMAF timing needs an explicit timeline mapping" }
    }
    private val clock = PlaybackClock()

    fun map(
        lane: PlaybackLane, payload: MediaPayload, status: ObjectStatus, keyframe: Boolean,
        presentation: MediaMicros, decode: MediaMicros, composition: MediaOffsetMicros,
        capture: UnixMicros?,
    ): List<PlaybackTimedUnit> {
        val format = when (lane) {
            PlaybackLane.Video -> video
            PlaybackLane.Audio -> requireNotNull(audio) { "no selected audio track" }
        }
        if (status != ObjectStatus.Normal) return emptyList()
        val raw = video.packaging == PackagingKind.Raw
        when (payload) {
            is MediaPayload.Raw -> {
                if (!raw) throw PlaybackRenderRefused("RAW payload on a CMAF track")
                if (payload.payload.size > maxPayloadBytes) throw PlaybackRenderRefused("object exceeds the sample byte bound")
            }
            is MediaPayload.Cmaf -> {
                if (raw) throw PlaybackRenderRefused("CMAF payload on a RAW track")
                if (payload.fragment.size > maxPayloadBytes) throw PlaybackRenderRefused("fragment exceeds the sample byte bound")
                if (payload.samples.isEmpty() || payload.samples.size > maxSamples) throw PlaybackRenderRefused("fragment sample count outside the sample bound")
                if (payload.mdat.offset.toLong() + payload.mdat.length > payload.fragment.size.toLong()) {
                    throw PlaybackRenderRefused("mdat outside its fragment")
                }
            }
        }
        // RAW's scalar is capture time, not a signed media PTS. Use zero only
        // for the framing parser, then one shared unsigned-difference clock.
        val facts = MediaFacts.ofObject(payload, status, keyframe,
            if (raw) MediaMicros(0u) else presentation,
            if (raw) MediaMicros(0u) else decode, composition, capture)
        val mapped = AccessUnits.split(format, facts, maxPayloadBytes)
        mapped.filterIsInstance<AccessUnitOutcome.Rejected>().firstOrNull()?.let { throw PlaybackRenderRefused(it.reason) }
        val units = mapped.map { it as AccessUnitOutcome.Unit }
        if (units.any { it.bytes.isEmpty() || it.bytes.size > maxPayloadBytes }) {
            throw PlaybackRenderRefused("framed access unit exceeds the sample byte bound")
        }
        // A malformed object must not commit the broadcast's first timestamp.
        val rawTime = if (raw) when (val time = clock.timeOf(facts)) {
            is PlaybackTiming.At -> time.playbackUs
            is PlaybackTiming.Refused -> throw PlaybackRenderRefused(time.reason)
        } else null
        return units.map { PlaybackTimedUnit(it, rawTime ?: it.presentationTimeUs) }
    }
}
