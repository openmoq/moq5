package org.openmoq.moq5.jni

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * A child JVM that owns the allocation ledger while a SECOND thread of its own
 * reaches into it, and reports what it saw, one line per fact.
 *
 * Once the worker has started, EVERY exit from the owned body -- normal,
 * interrupted or thrown -- first retires that worker within the bound. A
 * worker that cannot be retired is reported (WORKER_LIVE, with proof the
 * inventory is still armed) and the process halts at once: `halt` runs no
 * `finally`, so the scoped observer never reclaims the inventory while the
 * worker can still touch it. The parent reaps this process and judges it; the
 * teardown is not an in-process cleanup and is never reported as one.
 *
 * Scenarios (the first argument):
 * - `isolation`: the worker runs to completion; the ledger is settled.
 * - `held`: the worker waits on a latch nothing releases, so the join bound
 *   expires by construction rather than by timing.
 * - `held-interrupt`: as `held`, and the owner's own join is interrupted.
 * - `held-throw`: as `held`, and the owner throws a declared exception after
 *   the worker started.
 *
 * Exit status: 0 settled normally; [EXIT_WORKER_LIVE] a live worker stopped
 * settlement; [EXIT_WORKER_NOT_STARTED]; [EXIT_BODY_FAILED] the body failed
 * after its worker was retired and the ledger settled.
 */
internal object CodecLedgerChildMain {
    const val EXIT_WORKER_LIVE = 3
    const val EXIT_WORKER_NOT_STARTED = 4
    const val EXIT_BODY_FAILED = 5

    private const val JOIN_MILLIS = 30_000L
    private const val HELD_JOIN_MILLIS = 200L

    private class Declared(message: String) : RuntimeException(message)

    private fun fact(key: String, value: Any?) {
        println("$key $value")
        System.out.flush()
    }

    /**
     * Joins [worker] within [boundMillis] of monotonic time, absorbing any
     * interruption inside that same bound (and restoring it afterwards). A
     * worker still alive at the bound halts the process before the caller can
     * unwind into the observer's settlement.
     */
    private fun retireOrHalt(worker: Thread, boundMillis: Long) {
        val deadline = System.nanoTime() + boundMillis * 1_000_000
        var interrupted = false
        while (worker.isAlive) {
            val leftMillis = (deadline - System.nanoTime()) / 1_000_000
            if (leftMillis <= 0) break
            try {
                worker.join(leftMillis)
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (worker.isAlive) {
            // The inventory is still armed and still this thread's: a further
            // arm is refused. Nothing may settle it while the worker lives.
            fact("WORKER_LIVE", true)
            fact("STILL_ARMED", !TrustRouteProbe.nativeCodecAllocArm(-1))
            Runtime.getRuntime().halt(EXIT_WORKER_LIVE)
        }
        if (interrupted) Thread.currentThread().interrupt()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val scenario = args.firstOrNull() ?: "isolation"
        val held = scenario.startsWith("held")
        val bound = if (held) HELD_JOIN_MILLIS else JOIN_MILLIS
        check(NativeBridgeOwner.initialize() is BridgeInit.Ready) { "setup: the host product image must load" }
        TrustRouteProbe.nativeCodecReset()
        val reached = CountDownLatch(1)
        val release = CountDownLatch(1)
        var foreignDisarm: LongArray? = longArrayOf(-1)
        var foreignArm = true
        var foreignWork = -1
        val o =
            observed(allocFailAt = -1) {
                fact("HOLD", TrustRouteProbe.nativeCodecAllocHold())
                fact("SECOND_ARM", TrustRouteProbe.nativeCodecAllocArm(-1))
                val worker =
                    Thread {
                        foreignDisarm = TrustRouteProbe.nativeCodecAllocDisarm()
                        foreignArm = TrustRouteProbe.nativeCodecAllocArm(-1)
                        reached.countDown()
                        if (held) release.await()
                        foreignWork = TrustRouteProbe.nativeCodecAllocControl(false)
                    }
                worker.start()
                try {
                    if (!reached.await(JOIN_MILLIS, TimeUnit.MILLISECONDS)) {
                        fact("WORKER_NOT_STARTED", true)
                        Runtime.getRuntime().halt(EXIT_WORKER_NOT_STARTED)
                    }
                    if (held) fact("WORKER_HELD", true)
                    when (scenario) {
                        "held-interrupt" -> Thread.currentThread().interrupt()
                        "held-throw" -> {
                            fact("OWNER_THROWING", true)
                            throw Declared("the declared unwind after the worker started")
                        }
                    }
                    try {
                        worker.join(bound)
                    } catch (e: InterruptedException) {
                        fact("OWNER_INTERRUPTED", true)
                        throw e
                    }
                } finally {
                    retireOrHalt(worker, bound)
                }
                fact("FOREIGN_DISARM", foreignDisarm?.joinToString(","))
                fact("FOREIGN_ARM", foreignArm)
                fact("FOREIGN_WORK", foreignWork)
                fact("OWNER_WORK", TrustRouteProbe.nativeCodecAllocControl(false))
            }
        val failure = o.result.exceptionOrNull()
        fact("RESULT", failure ?: "ok")
        fact("LEDGER", o.alloc?.let { l -> (listOf(l.attempts, l.doubleFrees, l.foreignFrees, l.reclaimed) + l.entries.flatMap { listOf(it.first, it.second) }).joinToString(" ") })
        fact("FAULT", TrustRouteProbe.nativeFault())
        exitProcess(if (failure == null) 0 else EXIT_BODY_FAILED)
    }
}
