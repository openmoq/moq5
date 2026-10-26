package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer

/**
 * Explicit, positive limits on one configuration capture: how many units,
 * how large one unit may be, and how many bytes all of them may hold
 * together. Nothing here is a device-wide default.
 */
internal class ConfigBounds(val maxUnits: Int, val maxUnitBytes: Int, val maxTotalBytes: Int) {
    init {
        require(maxUnits > 0) { "the configuration unit limit is positive, not $maxUnits" }
        require(maxUnitBytes > 0) { "the configuration unit byte limit is positive, not $maxUnitBytes" }
        require(maxTotalBytes > 0) { "the configuration byte limit is positive, not $maxTotalBytes" }
    }
}

/**
 * One BORROWED unit of configuration bytes, as the capture sees it: a
 * declared length, then exactly one read of exactly that many bytes. The
 * caller keeps the source's bytes, range and membership stable for the
 * whole synchronous capture; nothing is retained after it returns.
 */
internal interface ConfigByteSource {
    /** The unit's length in bytes, read once, before any unit is copied. A negative value is a contract violation the capture refuses. */
    val length: Int

    /**
     * A FRESH array holding exactly [length] bytes of this unit, handed over
     * EXCLUSIVELY: the source keeps no alias to it, so the capture adopts it
     * without another copy. Called at most once, and only after every length
     * and the aggregate were validated.
     */
    fun read(length: Int): ByteArray
}

/**
 * The borrowed [ByteBuffer] as a [ConfigByteSource]: its remaining range is
 * pinned once, through a duplicate, so the caller's position, limit and mark
 * are never touched. Only a view is made here; bytes are copied in [read].
 */
internal class ByteBufferSource(buffer: ByteBuffer) : ConfigByteSource {
    private val view = buffer.duplicate()

    override val length: Int = view.remaining()

    override fun read(length: Int): ByteArray {
        check(length == this.length) { "a pinned range of ${this.length} bytes was read as $length" }
        val bytes = ByteArray(length)
        view.duplicate().get(bytes)
        return bytes
    }
}

/**
 * OPAQUE configuration bytes, owned and immutable: the ordered units exactly
 * as they were captured. No codec grammar, no normalization, no validation.
 * Raw equality is unit count, order, boundaries and bytes; a semantic
 * equivalence is a separate, later operation.
 */
internal class RawConfigSnapshot private constructor(private val units: Array<ByteArray>, val totalBytes: Int) {
    val unitCount: Int get() = units.size

    /** A copy of unit [index]. */
    fun unit(index: Int): ByteArray = units[index].copyOf()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RawConfigSnapshot || other.units.size != units.size) return false
        return units.indices.all { units[it].contentEquals(other.units[it]) }
    }

    override fun hashCode(): Int = units.fold(units.size) { hash, unit -> 31 * hash + unit.contentHashCode() }

    internal companion object {
        /** Adopts [units]: the arrays were handed over exclusively by their sources. */
        fun adopt(units: Array<ByteArray>, totalBytes: Int): RawConfigSnapshot = RawConfigSnapshot(units, totalBytes)
    }
}

internal sealed interface RawSnapshotResult {
    class Taken(val snapshot: RawConfigSnapshot) : RawSnapshotResult

    /** A bound refused the input; nothing was copied and nothing partial is published. */
    class Refused(val reason: String) : RawSnapshotResult
}

/**
 * The one capture algorithm. Count, every unit's length and the aggregate
 * are checked before ANY unit is read; a refusal copies nothing. A read
 * that raises leaves as itself and no later unit is read.
 */
internal object RawConfigSnapshots {
    /** Real buffers: the count is refused BEFORE any element is adapted, then the same algorithm runs. */
    fun capture(units: List<ByteBuffer>, bounds: ConfigBounds): RawSnapshotResult {
        countRefusal(units.size, bounds)?.let { return it }
        return captureFrom(units.map { ByteBufferSource(it) }, bounds)
    }

    fun captureFrom(sources: List<ConfigByteSource>, bounds: ConfigBounds): RawSnapshotResult {
        countRefusal(sources.size, bounds)?.let { return it }
        // Every length is read ONCE and pinned; allocation below uses only
        // these validated values, never a second query of the source.
        val lengths = IntArray(sources.size)
        var total = 0L
        for (i in sources.indices) {
            val length = sources[i].length
            if (length < 0) return RawSnapshotResult.Refused("configuration unit $i declares a negative length of $length")
            if (length == 0) return RawSnapshotResult.Refused("configuration unit $i is empty")
            if (length > bounds.maxUnitBytes) {
                return RawSnapshotResult.Refused("configuration unit $i of $length bytes exceeds the limit of ${bounds.maxUnitBytes}")
            }
            lengths[i] = length
            total += length
        }
        if (total > bounds.maxTotalBytes) return RawSnapshotResult.Refused("$total configuration bytes exceed the limit of ${bounds.maxTotalBytes}")
        val units = arrayOfNulls<ByteArray>(sources.size)
        for (i in sources.indices) {
            val bytes = sources[i].read(lengths[i])
            check(bytes.size == lengths[i]) { "configuration unit $i was read as ${bytes.size} bytes, not ${lengths[i]}" }
            units[i] = bytes
        }
        return RawSnapshotResult.Taken(RawConfigSnapshot.adopt(units.requireNoNulls(), total.toInt()))
    }

    private fun countRefusal(count: Int, bounds: ConfigBounds): RawSnapshotResult.Refused? =
        when {
            count == 0 -> RawSnapshotResult.Refused("no configuration units")
            count > bounds.maxUnits -> RawSnapshotResult.Refused("$count configuration units exceed the limit of ${bounds.maxUnits}")
            else -> null
        }
}
