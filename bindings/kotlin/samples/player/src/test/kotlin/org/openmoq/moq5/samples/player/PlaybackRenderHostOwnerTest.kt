package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackRenderHostOwnerTest {
    private class Rig(scope: TestScope) {
        val log = mutableListOf<String>()
        val gate = PlaybackRuntimeGate()
        val surface = Any()
        val failure = IllegalStateException("work failed")
        var startFailure: Throwable? = null
        var workerFailure: Throwable? = null
        var renderFailure: Throwable? = null
        var displayFailure: Throwable? = null
        var joined = true
        var retained = emptySet<PlaybackRetainedResource>()
        val worker = object : PlaybackRenderWorker {
            override val dispatcher = StandardTestDispatcher(scope.testScheduler)
            override fun start() { log += "worker.start"; workerFailure?.let { throw it } }
            override suspend fun stopAndJoin(): Boolean { log += "worker.join"; return joined }
        }
        val display = object : PlaybackDisplay<Any> {
            override val surface = this@Rig.surface
            override fun close() { log += "display.close"; displayFailure?.let { throw it } }
        }
        val renderer = object : ManagedPlaybackRender {
            override fun start() { log += "render.start"; startFailure?.let { throw it } }
            override fun close(): PlaybackRenderClosed {
                log += "render.close"
                return PlaybackRenderClosed(retained, renderFailure)
            }
            override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long) = PlaybackOffer.Queued
            override fun pump() = false
            override fun endInputs() {}
            override val decoderOutputsEnded = true
            override val videoOutputsReleased = 0
        }
        val answer = PlaybackReadResult(PlaybackReadEnd.ObjectLimit, 2, 3)
        suspend fun run(body: suspend (PlaybackRenderPort, PlaybackFailure) -> PlaybackReadResult = { port, _ ->
            assertSame(renderer, port)
            log += "consume"
            answer
        }): PlaybackReadResult = runOwnedPlayback(gate, { log += "worker.create"; worker }, {
            log += "display.open"; display
        }, { got, _ -> assertSame(surface, got); log += "render.create"; renderer }, body)
    }

    @Test fun cleanRunClosesRendererThenJoinsWorkerThenReturnsDisplay() = runTest {
        val r = Rig(this)
        assertSame(r.answer, r.run())
        assertEquals(listOf("worker.create", "worker.start", "display.open", "render.create", "render.start", "consume", "render.close", "worker.join", "display.close"), r.log)
        assertFalse(r.gate.retained())
        r.run()
        assertEquals(2, r.log.count { it == "display.close" })
    }

    @Test fun failedStartClosesPartiallyStartedRenderer() = runTest {
        for (cause in listOf(IllegalStateException("start"), AssertionError("start"), CancellationException("start"))) {
            val r = Rig(this)
            r.startFailure = cause
            assertSame(cause, runCatching { r.run() }.exceptionOrNull())
            assertEquals(listOf("render.close", "worker.join", "display.close"), r.log.takeLast(3))
            assertFalse("consume" in r.log)
            assertFalse(r.gate.retained())
        }
    }

    @Test fun workerStartFailureStillJoinsWithoutAcquiringDisplay() = runTest {
        val r = Rig(this)
        r.workerFailure = r.failure
        assertSame(r.failure, runCatching { r.run() }.exceptionOrNull())
        assertEquals(listOf("worker.create", "worker.start", "worker.join"), r.log)
        assertFalse(r.gate.retained())
    }

    @Test fun uncertainRendererRetainsAllAndRefusesNextAttemptBeforeEffects() = runTest {
        val r = Rig(this)
        r.retained = setOf(PlaybackRetainedResource.Sync)
        r.renderFailure = r.failure
        assertSame(r.failure, runCatching { r.run() }.exceptionOrNull())
        assertTrue(r.gate.retained())
        assertEquals("render.close", r.log.last())
        val before = r.log.toList()
        assertTrue(runCatching { r.run() }.exceptionOrNull() is IllegalStateException)
        assertEquals(before, r.log)
    }

    @Test fun retainedResourceWithoutFailureIsStillARefusal() = runTest {
        val r = Rig(this)
        r.retained = setOf(PlaybackRetainedResource.VideoCodec)
        assertTrue(runCatching { r.run() }.exceptionOrNull() is IllegalStateException)
        assertTrue(r.gate.retained())
        assertFalse("worker.join" in r.log)
    }

    @Test fun joinMustBeProvedBeforeDisplayReleaseOrReadmission() = runTest {
        val r = Rig(this)
        r.joined = false
        assertTrue(runCatching { r.run() }.exceptionOrNull() is IllegalStateException)
        assertTrue(r.gate.retained())
        assertEquals("worker.join", r.log.last())
    }

    @Test fun displayReleaseFailureRetainsItsOwnerWithoutRetry() = runTest {
        val r = Rig(this)
        r.displayFailure = r.failure
        assertSame(r.failure, runCatching { r.run() }.exceptionOrNull())
        assertTrue(r.gate.retained())
        runCatching { r.run() }
        assertEquals(1, r.log.count { it == "display.close" })
    }

    @Test fun cleanupFailureIsPreservedOnOriginalWorkFailure() = runTest {
        val r = Rig(this)
        val cleanup = AssertionError("release failed")
        r.renderFailure = cleanup
        // No retained resource: a failure in flush may still permit proved release.
        val result = runCatching { r.run { _, _ -> throw r.failure } }
        assertSame(r.failure, result.exceptionOrNull())
        assertEquals(listOf(cleanup), r.failure.suppressed.toList())
        assertFalse(r.gate.retained())
        assertEquals("display.close", r.log.last())
    }

    /** A real registry lease flowing through the owned attempt, with every effect logged. */
    private class Owned(scope: TestScope) {
        val log = mutableListOf<String>()
        val gate = PlaybackRuntimeGate()
        val texture = Any()
        var surfaceReleaseFailure: Throwable? = null
        val registry = PlaybackDisplayRegistry<Any, Any>(
            createSurface = { log += "surface.create"; Any() },
            releaseSurface = { log += "surface.release"; surfaceReleaseFailure?.let { throw it } },
            releaseTransferredTexture = { log += "texture.release" },
        ).also { it.available(texture) }
        val worker = object : PlaybackRenderWorker {
            override val dispatcher = StandardTestDispatcher(scope.testScheduler)
            override fun start() { log += "worker.start" }
            override suspend fun stopAndJoin(): Boolean { log += "worker.join"; return true }
        }
        val renderer = object : ManagedPlaybackRender {
            override fun start() { log += "render.start" }
            override fun close(): PlaybackRenderClosed { log += "render.close"; return PlaybackRenderClosed(emptySet(), null) }
            override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long) = PlaybackOffer.Queued
            override fun pump() = false
            override fun endInputs() {}
            override val decoderOutputsEnded = true
            override val videoOutputsReleased = 0
        }
        val answer = PlaybackReadResult(PlaybackReadEnd.ObjectLimit, 1, 1)
        suspend fun run(owned: (PlaybackDisplay<Any>) -> Unit): PlaybackReadResult = runOwnedPlayback(
            gate, { worker }, { registry.acquire().also { log += "display.acquired" } },
            { _, _ -> log += "render.create"; renderer }, { _, _ -> log += "consume"; answer },
            onDisplayOwned = owned,
        )
    }

    @Test fun ownedHookRunsAfterTheAttemptOwnsTheLeaseAndBeforeTheRenderer() = runTest {
        val o = Owned(this)
        var seen: PlaybackDisplay<Any>? = null
        assertSame(o.answer, o.run { seen = it; o.log += "owned" })
        assertEquals(listOf("worker.start", "surface.create", "display.acquired", "owned", "render.create", "render.start",
            "consume", "render.close", "worker.join", "surface.release"), o.log)
        val lease = seen as PlaybackDisplayLease<*, *>
        assertSame(o.texture, lease.texture)
        assertEquals(1, lease.ordinal)
        assertEquals(null, o.registry.leaseIdFor(o.texture))
        assertFalse(o.gate.retained())
    }

    @Test fun throwingOwnedHookTakesTheOwnedCloseAndReleasePath() = runTest {
        val o = Owned(this)
        val refused = IllegalStateException("observer-refused: reused-surface")
        assertSame(refused, runCatching { o.run { o.log += "owned"; throw refused } }.exceptionOrNull())
        assertEquals(listOf("worker.start", "surface.create", "display.acquired", "owned", "worker.join", "surface.release"), o.log)
        assertFalse("render.create" in o.log, "no renderer after a refused owner hook")
        assertEquals(1, o.log.count { it == "surface.release" }, "the owned lease closes exactly once")
        assertEquals(null, o.registry.leaseIdFor(o.texture))
        assertFalse(o.gate.retained())
        assertTrue(refused.suppressed.isEmpty())
        // The released attempt admits the next one on the same registry; that lease is the entry's second.
        var second: PlaybackDisplayLease<*, *>? = null
        assertSame(o.answer, o.run { second = it as PlaybackDisplayLease<*, *> })
        assertEquals(2, second!!.ordinal)
    }

    @Test fun throwingOwnedHookWithUncertainDisplayCloseRetainsTheAttempt() = runTest {
        val o = Owned(this)
        val refused = IllegalStateException("observer refused")
        val closeFailed = IllegalStateException("native Surface release failed")
        o.surfaceReleaseFailure = closeFailed
        assertSame(refused, runCatching { o.run { throw refused } }.exceptionOrNull())
        assertEquals(1, o.log.count { it == "surface.release" })
        assertTrue(o.gate.retained(), "an uncertain close keeps the attempt")
        assertSame(closeFailed, refused.suppressed.getOrNull(0))
        assertTrue(refused.suppressed.any { it is IllegalStateException && it.message!!.contains("remain retained") })
        val before = o.log.toList()
        assertTrue(runCatching { o.run { } }.exceptionOrNull() is IllegalStateException)
        assertEquals(before, o.log, "the retained attempt refuses the next one before any effect, with no second close")
    }

    @Test fun releaseSignalFiresOnceAfterTheGateIsReleasedAndNeverOnRetention() = runTest {
        val r = Rig(this)
        val seen = mutableListOf<Boolean>()
        assertSame(r.answer, runOwnedPlayback(r.gate, { r.worker }, { r.display }, { _, _ -> r.renderer },
            { _, _ -> r.answer }, onReleased = { seen += r.gate.retained() }))
        assertEquals(listOf(false), seen)
        val held = Rig(this)
        held.displayFailure = held.failure
        val heldSeen = mutableListOf<Boolean>()
        assertSame(held.failure, runCatching {
            runOwnedPlayback(held.gate, { held.worker }, { held.display }, { _, _ -> held.renderer }, { _, _ -> held.answer },
                onReleased = { heldSeen += held.gate.retained() })
        }.exceptionOrNull())
        assertEquals(emptyList(), heldSeen)
        assertTrue(held.gate.retained())
    }

    @Test fun aThrowingReleaseSignalIsACleanupErrorThatNeitherUndoesTheReleaseNorHidesTheWork() = runTest {
        val r = Rig(this)
        val signal = AssertionError("observer signal failed")
        assertSame(signal, runCatching {
            runOwnedPlayback(r.gate, { r.worker }, { r.display }, { _, _ -> r.renderer }, { _, _ -> r.answer }, onReleased = { throw signal })
        }.exceptionOrNull(), "alone, it surfaces")
        assertFalse(r.gate.retained(), "the proved release stands")
        assertEquals(1, r.log.count { it == "display.close" })
        val w = Rig(this)
        val raised = runCatching {
            runOwnedPlayback(w.gate, { w.worker }, { w.display }, { _, _ -> w.renderer }, { _, _ -> throw w.failure }, onReleased = { throw signal })
        }.exceptionOrNull()
        assertSame(w.failure, raised, "the work's failure stays primary")
        assertTrue(w.failure.suppressed.any { it === signal })
        assertFalse(w.gate.retained())
    }

    @Test fun cancellationSettlesConsumerBeforeNativeDisposal() = runTest {
        val r = Rig(this)
        val entered = CompletableDeferred<Unit>()
        val cleanupEntered = CompletableDeferred<Unit>()
        val settle = CompletableDeferred<Unit>()
        val task = async {
            r.run { _, _ ->
                entered.complete(Unit)
                try { awaitCancellation() } finally {
                    withContext(NonCancellable) { cleanupEntered.complete(Unit); settle.await(); r.log += "consume.settled" }
                }
            }
        }
        try {
            entered.await()
            task.cancel()
            cleanupEntered.await()
            assertFalse("render.close" in r.log)
            settle.complete(Unit)
            task.join()
            assertTrue(r.log.indexOf("consume.settled") < r.log.indexOf("render.close"))
            assertFalse(r.gate.retained())
        } finally {
            settle.complete(Unit)
            withContext(NonCancellable) { task.cancel(); task.join() }
        }
    }
}
