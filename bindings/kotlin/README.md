> **Work in progress.** This branch preserves the Android/Kotlin development
> snapshot on native baseline `f27d0208`. It is not ready to merge into current
> main or publish as an Android release. Native API/dependency reconciliation,
> A/V playback and physical-device acceptance remain unfinished. Python bindings
> code is unchanged from the branch baseline; the Android-specific Python test
> harness is included because the player unit tests use its marker/launcher inputs.

# MOQ5 for Android

Kotlin coroutine bindings to the MOQ5 C media service tier. Applications use
`MoqEndpoint`, `MediaReceiver`, and `MediaSender` from `org.openmoq.moq5`;
they do not call JNI, the protocol core, or transport adapters directly.

**Development status:** the bindings and sample applications are under active
review. This checkout is not a claim of a published Maven release or completed
device qualification. The publisher launcher offers two paths: "Start
synthetic" publishes synthetic encoded frames, and "Start live camera + mic"
requests the camera and microphone permissions and publishes from the back
camera and the microphone. The player connects its public receiver to Android
video and audio renderers through an owned display and worker. Neither the live
capture path nor device playback has yet been qualified on a device. The
samples' capture, codec and lifecycle contracts are being connected to the
Android platform. Do not use a successful host test or APK build as evidence of
working camera-to-player playback.

## Modules and Requirements

| Module | Purpose |
| --- | --- |
| `moq5-android` | Public Kotlin API and the packaged native implementation |
| `samples/publisher` | Publisher application and capture/publication contracts |
| `samples/player` | Receiver application and encoded-media conversion contracts |
| `bridge-r8-fixture` | Shrinking/obfuscation verification fixture, not application API |

The library declares Android API 21 as its minimum; the samples declare API 23.
Individual sample capture features can require a later API and must refuse
unsupported devices explicitly. The packaged ABIs are `arm64-v8a` and
`x86_64`. These build declarations do not establish the final device support
matrix or 16 KiB-page runtime qualification.

The AAR packages one `libmoq5_jni.so` per ABI, its consumer shrinking rules,
and third-party notices. Resolve the Maven artifact with its metadata rather
than copying only the AAR: Kotlin/coroutines dependencies are part of the
consumer contract. Do not load another copy of MOQ5's native implementation
beside the packaged image.

An application needs `android.permission.INTERNET`. Camera and microphone
permissions belong to an application using capture, not to the transport API.

## Build and Local Consumption

Run commands from `bindings/kotlin`. Tool versions and checksums come from
[gradle/toolchain.properties](gradle/toolchain.properties); use its declared
JDK, SDK platform/build tools, command-line tools, and NDK. Configure `JAVA_HOME`
and `ANDROID_HOME` (or the supported SDK discovery configuration). Use the
verified checked-in wrapper, not a Gradle installation found on `PATH`.

```sh
tools/gradlew-verified verifyToolchain
```

A fresh native stage requires the pinned dependencies, CMake, Ninja, make,
Perl, pkg-config, and Git. The dependency recipe may download pinned sources.
The topology recipe consumes that stage and produces the JNI payload:

```sh
tools/build-native-crossbuild.sh --android-api 21
tools/build-jni-topology.sh
tools/gradlew-verified :moq5-android:verifyJniTopology
tools/gradlew-verified :moq5-android:assembleRelease
```

The recipes own their generated `.native` directories. A stale stage or an
active mutation lock is a refusal, not permission to remove another process's
files. Rebuild under the recipe's explicit ownership rules; do not manually
copy an old `.so` over a failed audit. Coordinate builds in shared worktrees.

The configured publication is `org.openmoq:moq5-android:<version>`, where the
version is read from `MOQ_VERSION_STRING` in
[core/include/moq/version.h](../../core/include/moq/version.h). The default
repository is local build output, not Maven Central or the user's Maven cache:

```sh
tools/gradlew-verified :moq5-android:publishReleasePublicationToProbeRepository
```

Point a consuming Android project at `moq5-android/build/local-maven` using a
Gradle Maven repository and request those coordinates with the actual version.
`-PmoqPublicationRepo=/absolute/path/to/repository` selects a different local
repository. Keep its POM and Gradle module metadata with the AAR.

The aggregate verification entry is `tools/gradlew-verified check`. Focused
host tests include `:moq5-android:testDebugUnitTest`,
`:samples:publisher:testDebugUnitTest`, and `:samples:player:testDebugUnitTest`.
The aggregate also checks native/package evidence and deliberately fails when
required evidence is absent or stale. Device instrumentation, real media,
trust-policy and external-consumer runtime checks remain separate acceptance
requirements. Do not suppress warnings or count an omitted test as passing.

## Connect and Receive

`open` creates an endpoint but does not wait for establishment. `connect`
creates it and waits; establishment failure tears it down before throwing.
`withMoqEndpoint` owns teardown through normal return, failure and cancellation.
It opens without waiting, so explicitly await establishment inside its block:

```kotlin
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.MediaObject
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.withMoqEndpoint

suspend fun receiveEncoded(
    config: EndpointConfig,
    namespace: Namespace,
    consume: suspend (MediaObject) -> Unit,
) {
    withMoqEndpoint(config) { endpoint ->
        endpoint.awaitEstablished(10.seconds)
        val receiver = endpoint.attachReceiver(
            ReceiverConfig(
                namespace = namespace,
                overflow = OverflowPolicy.DropToKeyframe,
            ),
        )
        while (true) {
            val obj = receiver.objects.next() ?: break
            consume(obj)
        }
        // Endpoint teardown also retires its attached receiver.
    }
}
```

This receives encoded objects, not decoded frames. The consumer must handle
`ObjectStatus`, `MediaPayload.Raw` versus `MediaPayload.Cmaf`, codec
configuration, and the appropriate timestamp domains. An end/status object is
not an encoded frame. The application owns decoding and presentation.

The default receiver subscribes eligible tracks automatically. For manual
selection, set `autoSubscribe = false`, observe `receiver.inventory`, and call
`subscribe` on the actual discovered `MediaTrack`. Handles belong to their
receiver; do not reconstruct them from names or share them across receivers.
`unsubscribe` pauses delivery and purges queued objects for that track; it does
not destroy the catalog handle. Subscription acceptance is asynchronous.

`inventory` and endpoint `snapshots` are current state, not lossless event
histories. To request discovery history, choose
`EventHistoryPolicy.Exact(capacity)` at receiver creation and consume
`trackEvents`. A history overflow yields its retained prefix then a stable
failure; it does not silently skip events. Inventory remains available.

Each `ExactStream` has one consumer. `asFlow()` consumes the same stream; it is
not multicast. Object pulls suspend when empty. Normal exhaustion returns
`null`; failures throw rather than becoming end-of-stream. Receiver overflow
policy still governs the native media queue: the type name does not promise
lossless transport or unlimited buffering. Inspect `statsSnapshot()` for
policy drops and overflow. SAP/media-timeline streams also report typed
deferral observations; do not discard those while claiming exact history.

## Publish Encoded Media

Choose backpressure explicitly. `ReturnWouldBlock` does not evict media;
`write(..., timeout)` retries on service progress and returns `WouldBlock` if
it cannot accept before the bound. `DropToKeyframe` and `DropGroup` permit
policy drops, including with the suspending `write` form. `Accepted` means
accepted into the service, not delivered to a subscriber.

This finite example accepts a caller-supplied track configuration and already
encoded, correctly grouped objects. It aborts on refusal rather than silently
discarding a frame. The caller must stop all other writes before its flushes:

```kotlin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.DrainOutcome
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.WriteOutcome
import org.openmoq.moq5.withMoqEndpoint

suspend fun publishEncoded(
    config: EndpointConfig,
    namespace: Namespace,
    trackConfig: SendTrackConfig,
    objects: Flow<SendObject>,
) {
    withMoqEndpoint(config) { endpoint ->
        endpoint.awaitEstablished(10.seconds)
        val sender = endpoint.attachSender(
            SenderConfig(namespace, SendBackpressure.ReturnWouldBlock),
        )
        val track = sender.addTrack(trackConfig)
        withTimeout(10.seconds) { sender.awaitReady() }
        objects.collect { obj ->
            val written = sender.write(track, obj, 2.seconds)
            check(written == WriteOutcome.Accepted) { "media not accepted: $written" }
        }
        val ended = sender.endTrack(track, 2.seconds)
        check(ended == WriteOutcome.Accepted) { "track end not accepted: $ended" }

        // Preserve queued media and its END_OF_TRACK before removing tracks.
        val mediaFlushed = endpoint.drain(5.seconds)
        check(mediaFlushed == DrainOutcome.Flushed) { "media not flushed: $mediaFlushed" }
        sender.complete()
        // complete() records a terminal catalog; it does not flush it.
        val terminalFlushed = endpoint.drain(5.seconds)
        check(terminalFlushed == DrainOutcome.Flushed) { "completion not flushed: $terminalFlushed" }
    }
}
```

The two drains serve different obligations. C completion removes the tracks
and can discard still-queued media; calling `complete` immediately after an
accepted write/end is not a media-preservation barrier. The first flush can
remain pending when demanded output cannot progress. A timeout, interruption,
closed result, or unsupported flush is not a successful delivery claim.
Even `Flushed` proves a local flush, not that a remote decoder presented media.

`endTrack` ends one track reliably; `removeTrack` is lifecycle removal, which
can discard its queued media. `convertToVod` ends the live phase but is not
broadcast completion. `close`/`closeAndJoin` do not implicitly flush.

## Trust, Cancellation and Ownership

- `EndpointConfig` defaults to `TrustPolicy.PlatformTrustManager`. The
  transport checks the reference identity natively and delegates chain/app
  policy to Android's trust manager. Configure application anchors and pins
  through Android Network Security Config. No public insecure, PEM-file or
  application-verifier-callback policy exists in this binding.
- On platforms with the per-host CT requirement query, a host requiring
  certificate transparency is refused before native construction: the current
  transport does not provide the SCT/OCSP material needed to satisfy it.
  `TrustPolicyUnsupportedException` is not permission to bypass verification.
- `https://` selects WebTransport; `moqt://` and `moq://` select native QUIC
  under `TransportProtocol.Automatic`. An explicit protocol overrides that
  selection. Version offers constrain negotiation; unsupported offers fail,
  rather than silently downgrading. The handshake timeout and establishment
  wait are separate bounds.
- Suspending operations use the service worker; applications need no native
  pump loop. Endpoint/receiver/sender state is observable without polling a
  timer. Cancellation of one wait does not close the endpoint or set its
  interrupt latch.
- Attach, track-add and individual write attempts have commit-grade ownership:
  an accepted in-flight operation can deliver its handle/result despite
  cancellation. Retain that result and use structured cleanup. Do not infer
  that cancellation undid a native transfer.
- `interrupt()` sets a sticky endpoint-wide latch; `resume()` clears it. This
  is distinct from coroutine cancellation and endpoint close.
- An endpoint permits one receiver and one sender concurrently. Child close
  releases its attachment slot. Endpoint teardown retires attached children
  before destroying the root. `close()` initiates logical close;
  `closeAndJoin()` is the deterministic teardown boundary. Prefer
  `withMoqEndpoint` when the endpoint belongs to one coroutine operation.
- Namespace parts, track names and media bytes are immutable byte values with
  content equality, not Java strings or array identities. Use `utf8OrNull()`
  only for an explicit textual view. Media time and Unix capture time are
  different types; CMAF durations/composition offsets use their declared ticks.

See the public contracts in [MoqEndpoint.kt](moq5-android/src/main/kotlin/org/openmoq/moq5/MoqEndpoint.kt),
[Receiver.kt](moq5-android/src/main/kotlin/org/openmoq/moq5/Receiver.kt),
[Sender.kt](moq5-android/src/main/kotlin/org/openmoq/moq5/Sender.kt), and
[TrustPolicy.kt](moq5-android/src/main/kotlin/org/openmoq/moq5/TrustPolicy.kt).
Internal `engine`, `jni`, and `trust` packages are implementation details.
