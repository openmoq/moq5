package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

/** These failures escape the real session lifetime; no decoder or resource model replaces it. */
class PlaybackLifetimeErrorTest {
    private class Fixture {
        val calls = mutableListOf<String>()
        val endpoint = Any()
        val child = Any()

        suspend fun run(
            body: Throwable? = null,
            childClose: Throwable? = null,
            endpointClose: Throwable? = null,
            describe: Throwable? = null,
            connect: Throwable? = null,
        ): Throwable? = runCatching {
            playbackLifetime(
                connect = {
                    calls += "connect"
                    connect?.let { throw it }
                    endpoint
                },
                describe = {
                    assertSame(endpoint, it)
                    calls += "describe"
                    describe?.let { throw it }
                },
                attach = {
                    assertSame(endpoint, it)
                    calls += "attach"
                    child
                },
                run = { ep, ch ->
                    assertSame(endpoint, ep)
                    assertSame(child, ch)
                    calls += "run"
                    body?.let { throw it }
                },
                closeChild = {
                    assertSame(child, it)
                    calls += "child"
                    yield()
                    childClose?.let { throw it }
                },
                closeEndpoint = {
                    assertSame(endpoint, it)
                    calls += "endpoint"
                    yield()
                    endpointClose?.let { throw it }
                },
            )
        }.exceptionOrNull()

        fun bothClosed() = assertEquals(listOf("connect", "describe", "attach", "run", "child", "endpoint"), calls)
    }

    @Test
    fun bodyErrorKeepsIdentityAndBothCleanupFailures() = runTest {
        val f = Fixture()
        val body = AssertionError("body")
        val child = IllegalStateException("child")
        val endpoint = LinkageError("endpoint")
        val seen = f.run(body, child, endpoint)
        f.bothClosed()
        assertSame(body, seen)
        assertEquals(listOf<Throwable>(child, endpoint), body.suppressed.toList())
    }

    @Test
    fun childErrorCannotSkipEndpointOrReplaceBodyException() = runTest {
        val f = Fixture()
        val body = IllegalStateException("body")
        val child = AssertionError("child")
        val endpoint = IllegalArgumentException("endpoint")
        val seen = f.run(body, child, endpoint)
        f.bothClosed()
        assertSame(body, seen)
        assertEquals(listOf<Throwable>(child, endpoint), body.suppressed.toList())
    }

    @Test
    fun successfulBodyStillAttemptsBothErroringCloses() = runTest {
        val f = Fixture()
        val child = AssertionError("child")
        val endpoint = LinkageError("endpoint")
        val seen = f.run(childClose = child, endpointClose = endpoint)
        f.bothClosed()
        assertSame(child, seen)
        assertEquals(listOf<Throwable>(endpoint), child.suppressed.toList())
    }

    @Test
    fun endpointErrorIsCarriedByTheFirstCleanupFailure() = runTest {
        val f = Fixture()
        val child = IllegalStateException("child")
        val endpoint = AssertionError("endpoint")
        val seen = f.run(childClose = child, endpointClose = endpoint)
        f.bothClosed()
        assertSame(child, seen)
        assertEquals(listOf<Throwable>(endpoint), child.suppressed.toList())
    }

    @Test
    fun errorBeforeAttachClosesOnlyTheOwnedEndpoint() = runTest {
        val f = Fixture()
        val describe = AssertionError("describe")
        val endpoint = IllegalStateException("endpoint")
        val seen = f.run(describe = describe, endpointClose = endpoint)
        assertEquals(listOf("connect", "describe", "endpoint"), f.calls)
        assertSame(describe, seen)
        assertEquals(listOf<Throwable>(endpoint), describe.suppressed.toList())
    }

    @Test
    fun sameBodyAndCleanupErrorIsNotSelfSuppressed() = runTest {
        val f = Fixture()
        val shared = AssertionError("shared")
        val seen = f.run(shared, shared, shared)
        f.bothClosed()
        assertSame(shared, seen)
        assertEquals(emptyList(), shared.suppressed.toList())
    }

    @Test
    fun sameCleanupExceptionIsAttemptedTwiceButCarriedOnce() = runTest {
        val f = Fixture()
        val shared = IllegalStateException("shared")
        val seen = f.run(childClose = shared, endpointClose = shared)
        f.bothClosed()
        assertSame(shared, seen)
        assertEquals(emptyList(), shared.suppressed.toList())
    }

    @Test
    fun repeatedCleanupIdentityIsAttachedToThePrimaryOnlyOnce() = runTest {
        val f = Fixture()
        val body = IllegalStateException("body")
        val shared = IllegalArgumentException("shared")
        val seen = f.run(body, shared, shared)
        f.bothClosed()
        assertSame(body, seen)
        assertEquals(listOf<Throwable>(shared), body.suppressed.toList())
    }

    @Test
    fun bodyCancellationKeepsIdentityWhenBothClosesRaiseErrors() = runTest {
        val f = Fixture()
        val body = CancellationException("body")
        val child = AssertionError("child")
        val endpoint = LinkageError("endpoint")
        val seen = f.run(body, child, endpoint)
        f.bothClosed()
        assertSame(body, seen)
        assertEquals(listOf<Throwable>(child, endpoint), body.suppressed.toList())
    }

    @Test
    fun failedConnectOwnsNothingToClose() = runTest {
        val f = Fixture()
        val failure = AssertionError("connect")
        assertSame(failure, f.run(connect = failure))
        assertEquals(listOf("connect"), f.calls)
        assertEquals(emptyList(), failure.suppressed.toList())
    }

    @Test
    fun bodyErrorWithSuccessfulClosesIsNotWrapped() = runTest {
        val f = Fixture()
        val failure = AssertionError("body")
        val seen = f.run(body = failure)
        f.bothClosed()
        assertSame(failure, seen)
        assertEquals(emptyList(), failure.suppressed.toList())
    }
}
