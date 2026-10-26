// MoQ5 Android/Kotlin bindings -- root build.
//
// The root has two jobs: make the toolchain contract runnable THROUGH the
// checked-in wrapper, and actually RESOLVE the Android library plugin at the one
// declared version, so the pin is a build contract rather than a number a script
// compares to itself.
//
// `apply false` is deliberate: the root resolves the plugin -- proving the
// marker and its artifact exist and are reachable -- while the AAR module
// `:moq5-android` applies it.

import java.util.Properties

plugins {
    // Version comes from settings.gradle.kts pluginManagement, which reads the
    // single authority file. No AGP version literal exists in this file.
    id("com.android.library") apply false
}

val toolchainProps =
    Properties().apply {
        layout.projectDirectory.file("gradle/toolchain.properties").asFile
            .inputStream()
            .use { load(it) }
    }

/** One pin, read from the single source of truth; absent is a hard error. */
fun pin(name: String): String =
    toolchainProps.getProperty(name)
        ?: error("gradle/toolchain.properties is missing the pin '$name'")

tasks.register<Exec>("verifyToolchain") {
    group = "verification"
    description =
        "Assert every declared toolchain pin against what is actually installed."
    commandLine(
        layout.projectDirectory.file("tools/verify-toolchain.sh").asFile.absolutePath,
    )
    // Proves to the script that it ran through the pinned wrapper rather than a
    // Gradle that happens to be on PATH; the script fails if these disagree.
    environment("MOQ5_TOOLCHAIN_VIA_WRAPPER", gradle.gradleVersion)
    // A non-zero exit must fail the build.
    isIgnoreExitValue = false
    doFirst {
        val declared = pin("gradle.version")
        check(gradle.gradleVersion == declared) {
            "running Gradle ${gradle.gradleVersion}, declared $declared"
        }
    }
}

/*
 * Cross-parser corpus: proves that for every value the canonical grammar
 * admits, the shell reader and java.util.Properties -- the loader THIS file,
 * Gradle and the wrapper all use -- produce byte-identical content. It runs in
 * the aggregate check so the equivalence claim is tested on every gate run
 * rather than asserted in a comment.
 */
tasks.register<Exec>("verifyPropertiesCorpus") {
    group = "verification"
    description =
        "Compare the shell property reader against java.util.Properties, byte for byte."
    commandLine(
        layout.projectDirectory.file("tools/check-properties-corpus.sh").asFile.absolutePath,
        // Gradle's OWN runtime, passed explicitly. Left to PATH the corpus could
        // compare against an unrelated JDK, so the oracle would not be the
        // parser this build actually uses.
        System.getProperty("java.home"),
    )
    isIgnoreExitValue = false
}

/*
 * The native input-root boundary. A checker nobody invokes is documentation,
 * so it is registered as a real verification task and joined to the aggregate
 * check: a regression in the digest boundary now fails the canonical gate
 * rather than waiting for someone to run the script by hand.
 */
tasks.register<Exec>("verifyNativeInputRoots") {
    group = "verification"
    description =
        "Assert the native DSO source digest covers no Gradle module and no ghost root."
    commandLine(
        layout.projectDirectory.file("tools/check-input-roots.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The classifier self-test. The boundary checker and this self-test source the
 * SAME classifier, so registering only the checker left the evidence for its
 * failure paths outside the canonical gate -- the very registration defect this
 * slice exists to prevent (chats/libmoq/1191 F2).
 */
/*
 * The source-manifest self-test. The manifest is the candidate's IDENTITY, and
 * a hash producer whose failure status was masked by a pipeline still minted a
 * complete, plausible digest (chats/libmoq/1240). Report prose could not keep
 * that discriminator alive, so it is a registered gate: valid-looking failures
 * on each producer path, and digests of the wrong length or alphabet.
 */
tasks.register<Exec>("verifySourceManifestSelfTest") {
    group = "verification"
    description =
        "Run the source-manifest self-test: hash-producer status on both " +
            "invocation paths, digest shape, and no candidate mutation."
    commandLine(
        layout.projectDirectory.file("tools/selftest-source-manifest.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifyInputRootClassifier") {
    group = "verification"
    description =
        "Run the input-root classifier self-test: contract cases, failure-path " +
            "controls and setup neuters."
    commandLine(
        layout.projectDirectory.file("tools/selftest-input-roots.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The verification tasks the aggregate check MUST run.
 *
 * The oracle's expected set is an INDEPENDENT LITERAL, deliberately not derived
 * from `requiredVerificationTasks`. Deriving it from the wiring under test made
 * the contract self-referential: deleting a name removed it from the wiring and
 * from the expectation at once, so the assertion was satisfied by the very
 * mutation it existed to catch (chats/libmoq/1191 F1).
 *
 * Two independent facts are asserted: the configured list is exactly the
 * expected set, and the FINAL dependency graph of `check` reaches every member.
 */
val expectedVerificationTasks =
    setOf(
        "verifyToolchain",
        "verifyPropertiesCorpus",
        "verifyNativeInputRoots",
        "verifyInputRootClassifier",
        "verifyNoCrashResidue",
        "verifyJniDiagnosticSelfTest",
        "verifyRegistryOracle",
        "verifyRegistryFrames",
        "verifyAarInventory",
        "verifySourceManifestSelfTest",
        "verifySampleApps",
        "verifySampleAppsSelfTest",
        "verifySampleDebugTrust",
        "verifySampleDebugTrustSelfTest",
        "verifyMavenPublication",
        "verifyMavenPublicationSelfTest",
        "verifyAndroidRuntimeDeclaration",
        "verifyAndroidRuntimeDeclarationSelfTest",
        "verifyAndroidRuntimeDeviceSelfTest",
    )

/*
 * What the sample trust gates read, as an independent literal: both samples'
 * debug and release artifacts. Each gate must depend on all four assemble
 * tasks, so what it judges is what the current sources build, not whatever a
 * previous invocation left behind.
 */
val expectedSampleTrustAssembly =
    setOf(
        ":samples:player:assembleDebug",
        ":samples:player:assembleRelease",
        ":samples:publisher:assembleDebug",
        ":samples:publisher:assembleRelease",
    )

/*
 * The module's whole verification boundary is part of the canonical gate, and
 * its
 * expectation is likewise an independent literal. Depending on the module's
 * `check` -- which enumerates its own host tests, lint and dependency gate --
 * is what stops root `check` from relying on Gradle's unqualified task-name
 * expansion to pick those up by accident. Module tasks are matched by PATH,
 * since a bare name would also match a same-named task in another project.
 */
val expectedModuleVerificationTasks =
    setOf(
        ":moq5-android:check",
        ":bridge-r8-fixture:check",
        ":samples:player:check",
        ":samples:publisher:check",
    )

val requiredModuleVerificationTasks =
    listOf(
        ":moq5-android:check",
        ":bridge-r8-fixture:check",
        ":samples:player:check",
        ":samples:publisher:check",
    )

val requiredVerificationTasks =
    listOf(
        "verifyToolchain",
        "verifyPropertiesCorpus",
        "verifyNativeInputRoots",
        "verifyInputRootClassifier",
        "verifyNoCrashResidue",
        "verifyJniDiagnosticSelfTest",
        "verifyRegistryOracle",
        "verifyRegistryFrames",
        "verifyAarInventory",
        "verifySourceManifestSelfTest",
        "verifySampleApps",
        "verifySampleAppsSelfTest",
        "verifySampleDebugTrust",
        "verifySampleDebugTrustSelfTest",
        "verifyMavenPublication",
        "verifyMavenPublicationSelfTest",
        "verifyAndroidRuntimeDeclaration",
        "verifyAndroidRuntimeDeclarationSelfTest",
        "verifyAndroidRuntimeDeviceSelfTest",
    )

/*
 * The crash-residue gate. A run that leaves a JVM fatal-error dump in the source
 * tree has not left it clean, and an ignore rule alone would hide that rather
 * than report it.
 */
tasks.register<Exec>("verifyNoCrashResidue") {
    group = "verification"
    description = "Assert no JVM crash or replay artefact survives in the shippable inventory."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-crash-residue.sh").asFile.absolutePath,
    )
}


/*
 * Post-test ordering, asserted against the ACTUAL EXECUTION PLAN.
 *
 * `verifyNoCrashResidue` was declared as a sibling dependency of the module
 * checks, so the canonical graph ran it at position 3 and the JNI tests at 191:
 * the tree was pronounced clean before anything could dirty it
 * (chats/libmoq/1224 F1).
 *
 * The edges are finalizers, which are not dependencies -- a dependency walk
 * cannot see them, and resolving a cross-project TaskDependency from a task
 * action is illegal in Gradle 9. So the oracle reads the scheduled plan itself:
 * whenever a producing task is scheduled, its gates must be scheduled too, and
 * strictly after it. That holds however the edge was declared, which is what
 * makes it independent of the wiring under test.
 */
val expectedPostTestOrder: Map<String, Set<String>> =
    mapOf(
        ":moq5-android:testBridgeJni" to
            setOf(":moq5-android:verifyJniDiagnostics", ":verifyNoCrashResidue"),
        ":moq5-android:testDebugUnitTest" to setOf(":verifyNoCrashResidue"),
    )

gradle.taskGraph.whenReady {
    val order = allTasks.map { it.path }
    expectedPostTestOrder.forEach { (producer, gates) ->
        val pi = order.indexOf(producer)
        if (pi >= 0) {
            gates.forEach { gate ->
                val gi = order.indexOf(gate)
                check(gi >= 0) {
                    "$producer is scheduled without $gate; a task that can leave a JVM " +
                        "artefact or a JNI diagnostic must be followed by the gate that " +
                        "looks for it"
                }
                check(gi > pi) {
                    "$gate is scheduled BEFORE $producer (positions $gi and $pi); a gate " +
                        "that runs first reports the state of the previous run"
                }
            }
        }
    }
}

/*
 * The JNI diagnostic gate's own ledger: its vocabulary comes from the selected
 * runtime and its identity rules refuse inherited evidence, so both are pinned
 * by disposable-fixture cases rather than by inspection.
 */
tasks.register<Exec>("verifyJniDiagnosticSelfTest") {
    group = "verification"
    description = "Assert the JNI diagnostic gate kills every checked-JNI form and no prose."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-jni-diagnostics.sh").asFile.absolutePath,
    )
}

/*
 * The private handle registry's behavioral oracle, in every lane it must
 * survive: normal, ASan/UBSan and TSan. It compiles the production registry
 * source, so what it judges is the shipped implementation.
 */
tasks.register<Exec>("verifyRegistryOracle") {
    group = "verification"
    description = "Run the native handle registry oracle in normal and sanitizer lanes."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/run-registry-oracle.sh").asFile.absolutePath,
    )
}

/*
 * The registry's stack-frame budget, on every compiler that builds it, each
 * lane guarded by a positive control so an ignored flag cannot pass by absence.
 */
tasks.register<Exec>("verifyRegistryFrames") {
    group = "verification"
    description = "Assert no registry frame exceeds 4096 bytes on any target compiler."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-registry-frame.sh").asFile.absolutePath,
    )
}

/*
 * The first-party sample applications.
 *
 * They are API acceptance tests: the assessment calls the player sample "an
 * API acceptance test, not a marketing shell". A gate nobody invokes could
 * not make that true, so the contract -- both modules registered, consuming
 * :moq5-android as ordinary applications, reaching no internal package, no C
 * header and no fixture, and each exercising its own workflow -- is part of
 * the canonical check.
 */
tasks.register<Exec>("verifySampleApps") {
    group = "verification"
    description = "Assert the player and publisher samples exist and consume only the public surface."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-sample-apps.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The sample gate's own ledger. Until the samples land the gate reports one
 * fact -- they are absent -- which leaves its consumer, private-package and
 * workflow rows unexercised. This self-test builds a disposable fixture the
 * gate ACCEPTS and then breaks one fact at a time, so every failure path is
 * evidence rather than an assertion, from the first day the rows exist.
 */
/*
 * The Maven publication contract: one publishable artifact with explicit
 * coordinates and a source-derived version, a declared API floor, an AAR whose
 * JNI payload is byte-identical to the audited topology package, POM and
 * Gradle module metadata that put coroutines on a consumer's compile
 * classpath, and a SEPARATE Gradle root that compiles against the published
 * artifact from an ephemeral repository. Registered now that the contract is
 * green, so a regression fails the canonical gate.
 */
tasks.register<Exec>("verifyMavenPublication") {
    group = "verification"
    description = "Assert the published artifact is resolvable, complete and consumable."
    // The rows read the release archive and the generated metadata, so the
    // producers of both are dependencies rather than an assumption about what
    // a previous invocation left behind.
    dependsOn(
        ":moq5-android:assembleRelease",
        ":moq5-android:generatePomFileForReleasePublication",
        ":moq5-android:generateMetadataFileForReleasePublication",
    )
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-maven-publication.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifyMavenPublicationSelfTest") {
    group = "verification"
    description = "Assert the publication gate kills each contract violation by name."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-maven-publication.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The DEVICE-FREE half of the Android runtime acceptance contract.
 *
 * tools/check-android-runtime.sh is the runtime proof, and it needs a device.
 * This check runs where there is none -- a CI host, a review checkout -- so the
 * device gate is deliberately NOT registered here: a verification task that
 * required hardware would either fail on every such host or be made skippable,
 * and a skippable runtime gate is the silent skip the slice exists to prevent.
 *
 * What IS registered is the substrate that gate depends on: the instrumentation
 * source set and its manifest, the runner declaration, the pinned AndroidX Test
 * artifacts and their confinement to the instrumentation configuration, and the
 * three named observations agreeing between the tests and the device gate.
 * Without this, all of that is deletable on a host where nothing would notice.
 */
tasks.register<Exec>("verifyAndroidRuntimeDeclaration") {
    group = "verification"
    description = "Assert the Android runtime acceptance substrate exists and is declared correctly."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-android-runtime-declaration.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifyAndroidRuntimeDeclarationSelfTest") {
    group = "verification"
    description = "Assert the runtime declaration gate kills each contract violation by name."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-android-runtime-declaration.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * What the device gate may do to a possibly shared device -- refuse package IDs
 * already present, uninstall only its own installs, never clear the crash
 * buffer -- judged on the host by running that gate against a fake adb and a
 * fake Gradle. No device is touched, so this belongs in the canonical check.
 */
tasks.register<Exec>("verifyAndroidRuntimeDeviceSelfTest") {
    group = "verification"
    description = "Assert the runtime device gate's ownership and crash-observation contract on a fake device."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-android-runtime-device.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifySampleAppsSelfTest") {
    group = "verification"
    description = "Assert the sample-app gate kills each contract violation by name."
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-sample-apps.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The samples' debug-only trust overlay: each debug build trusts user-installed
 * CAs for a local relay, and each release build carries no such config. Judged
 * in source, in the merged manifests and in the APKs, which is why both gates
 * depend on every sample assemble task: the artifacts they read are the ones
 * the current sources build.
 */
tasks.register<Exec>("verifySampleDebugTrust") {
    group = "verification"
    description = "Assert each sample's user-CA trust overlay is debug-only and exact, in source and in the built APKs."
    // Written out here, NOT read from expectedSampleTrustAssembly: that literal
    // is the independent expectation this wiring is checked against.
    dependsOn(
        ":samples:player:assembleDebug",
        ":samples:player:assembleRelease",
        ":samples:publisher:assembleDebug",
        ":samples:publisher:assembleRelease",
    )
    environment("MOQ5_TRUST_FRESH_BUILD", "1")
    environment("TMPDIR", temporaryDir.absolutePath)
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-sample-debug-trust.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifySampleDebugTrustSelfTest") {
    group = "verification"
    description = "Assert the sample trust gate kills each overlay violation by name."
    dependsOn(
        ":samples:player:assembleDebug",
        ":samples:player:assembleRelease",
        ":samples:publisher:assembleDebug",
        ":samples:publisher:assembleRelease",
    )
    environment("TMPDIR", temporaryDir.absolutePath)
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/selftest-sample-debug-trust.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The shipped archives must not contain test-only classes. Kotlin `internal` is
 * not JVM encapsulation, so this is asserted against the built AARs.
 */
tasks.register<Exec>("verifyAarInventory") {
    group = "verification"
    description = "Assert no test-only class ships in the debug or release AAR."
    dependsOn(":moq5-android:assembleDebug", ":moq5-android:assembleRelease")
    commandLine(
        "/bin/sh",
        layout.projectDirectory.file("tools/check-aar-inventory.sh").asFile.absolutePath,
    )
}

tasks.register("check") {
    group = "verification"
    description = "Aggregate verification check."
    dependsOn(requiredVerificationTasks)
    dependsOn(requiredModuleVerificationTasks)
}

/*
 * The task-graph contract.
 *
 * It reads the DECLARED dependency set of `check` after configuration and
 * compares it against independent literals. It deliberately does not resolve
 * the dependency graph: `check` now depends on a task in another project, and
 * resolving that -- at configuration time or from a task action -- is illegal
 * in Gradle 9 ("State of build ':' should not be in state ..."). It equally
 * does not consult the whole scheduled graph, because invoking `gradlew check`
 * also runs each subproject's own `check` by name, so a whole-graph query would
 * still see `:moq5-android:check` after the wiring under test had been removed
 * and the mutant would pass.
 */
gradle.projectsEvaluated {
    // Fact 1: the configured root list matches the independent expectation.
    val configuredRoot = requiredVerificationTasks.toSet()
    check(configuredRoot == expectedVerificationTasks) {
        "the configured verification list does not match the expected set; " +
            "missing ${expectedVerificationTasks - configuredRoot}, " +
            "unexpected ${configuredRoot - expectedVerificationTasks}"
    }

    // Fact 2: the configured module list matches its independent expectation.
    val configuredModule = requiredModuleVerificationTasks.toSet()
    check(configuredModule == expectedModuleVerificationTasks) {
        "the configured module verification list does not match the expected set; " +
            "missing ${expectedModuleVerificationTasks - configuredModule}, " +
            "unexpected ${configuredModule - expectedModuleVerificationTasks}"
    }

    // Fact 3: every expected name is genuinely declared on `check`.
    fun flatten(value: Any?): List<String> =
        when (value) {
            is CharSequence -> listOf(value.toString())
            is Iterable<*> -> value.flatMap { flatten(it) }
            is Array<*> -> value.flatMap { flatten(it) }
            else -> emptyList()
        }

    val declared = tasks.named("check").get().dependsOn.flatMap { flatten(it) }.toSet()
    val unreachable = (expectedVerificationTasks + expectedModuleVerificationTasks)
        .filterNot { it in declared }
    check(unreachable.isEmpty()) {
        "aggregate `check` does not depend on $unreachable; a verification task that " +
            "`check` cannot reach is never run by the canonical gate"
    }

    // Fact 4: each sample trust gate depends on every artifact it reads.
    for (gate in listOf("verifySampleDebugTrust", "verifySampleDebugTrustSelfTest")) {
        val assembled = tasks.named(gate).get().dependsOn.flatMap { flatten(it) }.toSet()
        val unbuilt = expectedSampleTrustAssembly - assembled
        check(unbuilt.isEmpty()) {
            "$gate does not depend on $unbuilt; a trust gate that reads an artifact it did " +
                "not build judges whatever a previous invocation left behind"
        }
    }
}
