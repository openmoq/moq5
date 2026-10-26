package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendCommandResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.EngineLedger
import org.openmoq.moq5.engine.Gate
import org.openmoq.moq5.engine.HookedBackend
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * One owned VOD batch, RED-first.
 *
 * Source-derived: [MediaSender.convertToVod] validates the caller's `List`
 * and hands the SAME list to the engine job, which lowers it to native pairs
 * when the job RUNS (after the caller suspended) and walks it AGAIN after the
 * backend accepted, to mark each item's handle VOD. The caller's list
 * therefore survives two suspensions and is reread twice. The intended
 * correction is one shallow owned snapshot taken before validation and
 * submission, used for both the native lowering and the successful local
 * update, with the C batch's atomic validation untouched.
 *
 * The rows mutate a `CopyOnWriteArrayList` after confirmed queue submission,
 * and separately after backend acceptance but before return, for both
 * replacement and clearing; a backend refusal is covered; and the ordinary
 * cancellation rule of this non-resource-producing operation is pinned as
 * controls: queued cancellation prevents mutation; a claimed successful job
 * updates the snapshotted model even if result delivery is cancelled.
 */
internal class PublicSenderVodBatchTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val TX = SenderConfig(namespace = NAMESPACE, backpressure = SendBackpressure.ReturnWouldBlock)
        fun video(name: String) =
            SendTrackConfig(
                name = TrackName(name.toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
        const val INVAL = -2
    }

    private class Rig(val inner: ScriptedEndpointBackend, val hooked: HookedBackend, val endpoint: MoqEndpoint, val sender: MediaSender, val t1: SendTrack, val t2: SendTrack, val t3: SendTrack) {
        val original: List<Pair<Long, ULong>> get() = listOf(t1.token to 100uL, t2.token to 200uL)

        fun batch(): CopyOnWriteArrayList<VodConversion> = CopyOnWriteArrayList(listOf(VodConversion(t1, MediaMillis(100uL)), VodConversion(t2, MediaMillis(200uL))))

        fun replaceWithThird(batch: CopyOnWriteArrayList<VodConversion>) {
            batch.clear()
            batch.add(VodConversion(t3, MediaMillis(300uL)))
        }
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun rig(): Rig {
        val inner = newBackend()
        val hooked = HookedBackend(inner)
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { hooked }, inner)
        val sender = endpoint.attachSender(TX)
        val t1 = withTimeout(BOUND) { sender.addTrack(video("v1")) }
        val t2 = withTimeout(BOUND) { sender.addTrack(video("v2")) }
        val t3 = withTimeout(BOUND) { sender.addTrack(video("v3")) }
        return Rig(inner, hooked, endpoint, sender, t1, t2, t3)
    }

    private fun Rig.assertOriginalConverted(row: String) {
        val calls = inner.observed().senderVodConversions
        assertEquals(1, calls.size, "$row: exactly one native batch call")
        assertEquals(original, calls.single().items, "$row: the native batch is the ORIGINAL membership, order and durations")
        assertTrue(t1.isVod && t2.isVod, "$row: the original items read VOD")
        assertTrue(!t3.isVod, "$row: an item that was never in the submitted batch does not read VOD")
    }

    /** Occupy the worker with a held stats read so the conversion stays queued behind it. */
    private suspend fun CoroutineScope.holdWorker(rig: Rig, ledger: EngineLedger, gates: MutableList<Gate>): kotlinx.coroutines.Deferred<SenderStats> {
        val hold = gate("hold").also { gates += it }
        rig.hooked.beforeSenderStats = { ledger.started("held"); hold.await() }
        val holder = async(Dispatchers.Default) { rig.sender.statsSnapshot() }
        assertTrue(ledger.await("the preceding job is claimed") { it.started.contains("held") }, "R8: the holding job never ran")
        return holder
    }

    @Test
    fun `the batch is snapshotted before submission -- a replacement after confirmed queueing changes nothing`() =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                val holder = holdWorker(rig, ledger(), gates)
                val batch = rig.batch()
                val wakes = rig.inner.observed().wakeCount
                val caller = async(Dispatchers.Default) { rig.sender.convertToVod(batch) }
                assertTrue(rig.inner.awaitCondition("the conversion is queued") { it.wakeCount == wakes + 1 }, "R8 queued: the conversion was never accepted")
                rig.replaceWithThird(batch)
                gates.forEach { it.open() }
                withTimeout(BOUND) { holder.await() }
                withTimeout(BOUND) { caller.await() }
                rig.assertOriginalConverted("R8 queued-replacement")
            }
        }

    @Test
    fun `the batch is snapshotted before submission -- a clear after confirmed queueing changes nothing`() =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                val holder = holdWorker(rig, ledger(), gates)
                val batch = rig.batch()
                val wakes = rig.inner.observed().wakeCount
                val caller = async(Dispatchers.Default) { rig.sender.convertToVod(batch) }
                assertTrue(rig.inner.awaitCondition("the conversion is queued") { it.wakeCount == wakes + 1 }, "R8 queued-clear: the conversion was never accepted")
                batch.clear()
                gates.forEach { it.open() }
                withTimeout(BOUND) { holder.await() }
                withTimeout(BOUND) { caller.await() }
                rig.assertOriginalConverted("R8 queued-clear")
            }
        }

    @Test
    fun `a replacement after backend acceptance but before return marks the original items, not the replacement`() =
        runTest {
            val rig = rig()
            run {
                val batch = rig.batch()
                rig.hooked.afterSenderConvertToVod = { rig.replaceWithThird(batch) }
                withTimeout(BOUND) { rig.sender.convertToVod(batch) }
                rig.assertOriginalConverted("R8 accepted-replacement")
            }
        }

    @Test
    fun `a clear after backend acceptance but before return still marks the original items`() =
        runTest {
            val rig = rig()
            run {
                val batch = rig.batch()
                rig.hooked.afterSenderConvertToVod = { batch.clear() }
                withTimeout(BOUND) { rig.sender.convertToVod(batch) }
                rig.assertOriginalConverted("R8 accepted-clear")
            }
        }

    @Test
    fun `a backend refusal converts nothing, and the refused native batch is the original even when the list was replaced while queued`() =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                rig.inner.senderCommandResult = BackendCommandResult.Refused(MoqResultCode(INVAL))
                val holder = holdWorker(rig, ledger(), gates)
                val batch = rig.batch()
                val wakes = rig.inner.observed().wakeCount
                val caller = async(Dispatchers.Default) { runCatching { rig.sender.convertToVod(batch) } }
                assertTrue(rig.inner.awaitCondition("the conversion is queued") { it.wakeCount == wakes + 1 }, "R8 refusal: the conversion was never accepted")
                rig.replaceWithThird(batch)
                gates.forEach { it.open() }
                withTimeout(BOUND) { holder.await() }
                val r = withTimeout(BOUND) { caller.await() }
                val refused = assertFailsWith<VodConversionRejectedException>("R8 refusal: the C's refusal is typed") { r.getOrThrow() }
                assertEquals(MoqResultCode(INVAL), refused.code)
                val calls = rig.inner.observed().senderVodConversions
                assertEquals(1, calls.size, "R8 refusal: one native call")
                assertEquals(rig.original, calls.single().items, "R8 refusal: the refused native batch is the ORIGINAL")
                assertTrue(!rig.t1.isVod && !rig.t2.isVod && !rig.t3.isVod, "R8 refusal: nothing converted")
            }
        }

    @Test
    fun `queued cancellation prevents the mutation -- no native call, nothing marked`() =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                val holder = holdWorker(rig, ledger(), gates)
                val batch = rig.batch()
                val wakes = rig.inner.observed().wakeCount
                val caller = launch(Dispatchers.Default) { rig.sender.convertToVod(batch) }
                assertTrue(rig.inner.awaitCondition("the conversion is queued") { it.wakeCount == wakes + 1 }, "R8 cancel-queued: the conversion was never accepted")
                caller.cancel()
                withTimeout(BOUND) { caller.join() }
                gates.forEach { it.open() }
                withTimeout(BOUND) { holder.await() }
                withTimeout(BOUND) { rig.sender.statsSnapshot() } // a later job proves the queue drained
                assertEquals(0, rig.inner.observed().senderVodConversions.size, "R8 cancel-queued: no native call")
                assertTrue(!rig.t1.isVod && !rig.t2.isVod, "R8 cancel-queued: nothing marked")
            }
        }

    @Test
    fun `a claimed successful conversion updates the snapshotted items although the caller's delivery was cancelled`() =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                val ledger = ledger()
                // The conversion runs its native call under the engine lock, so
                // the cancel is applied inside the window, on the worker, where
                // it is deterministic (Job.cancel is thread-agnostic).
                val callerReady = gate("caller-ready").also { gates += it }
                val callerRef = AtomicReference<Job?>(null)
                rig.hooked.beforeSenderConvertToVod = {
                    ledger.started("claimed")
                    if (callerReady.await()) callerRef.get()!!.cancel()
                    ledger.completed("claimed")
                }
                val batch = rig.batch()
                val caller = launch(Dispatchers.Default) { rig.sender.convertToVod(batch) }
                callerRef.set(caller)
                callerReady.open()
                assertTrue(ledger.await("the cancel is applied after the claim, before the native call") { it.completed.contains("claimed") }, "R8 cancel-claimed: the job never reached its native call")
                withTimeout(BOUND) { caller.join() }
                withTimeout(BOUND) { rig.sender.statsSnapshot() } // a later job proves the claimed body finished
                rig.assertOriginalConverted("R8 cancel-claimed")
            }
        }
}
