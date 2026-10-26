// The first-party MoQ5 publisher sample.
//
// It is an API ACCEPTANCE APPLICATION, not a product and not a published
// artifact: it exists to prove the send-side public Kotlin surface is
// sufficient and pleasant from OUTSIDE the library. So it consumes
// `:moq5-android` through an ordinary project dependency and sees exactly
// what an application developer sees -- no private source set, no internal
// engine or JNI package, no C header, no test fixture.
//
// It uses plain `android.widget` views on purpose. Compose, AndroidX
// lifecycle, camera, Media3 and a network library are each absent because
// every framework between the reader and the MoQ calls is something they
// have to discount; `tools/check-sample-apps.sh` enforces that absence.
//
// It sets `minSdk` only because an application module requires one, using the
// already-established experimental cross-build input. THAT IS NOT A PRODUCT
// minSdk PROMISE: the library's supported API floor is an open ruling and is
// not decided here.
import java.util.Properties

plugins {
    id("com.android.application")
}

val toolchainProps =
    Properties().apply {
        rootProject.layout.projectDirectory.file("gradle/toolchain.properties").asFile
            .inputStream()
            .use { load(it) }
    }

fun pin(name: String): String =
    toolchainProps.getProperty(name)
        ?: error("gradle/toolchain.properties is missing the pin '$name'")

val platform = pin("sdk.platform").removePrefix("android-")

// The sample's native-library packaging policy, judged on the EVALUATED
// model rather than on the text: a declaration that is deleted, or swallowed
// by an unterminated comment, leaves the model unset (null), and this gate
// refuses that as firmly as a legacy (true) setting. It sits above every
// region it judges, and `check` depends on it.
var evaluatedLegacyPackaging: Boolean? = null
afterEvaluate {
    evaluatedLegacyPackaging = android.packaging.jniLibs.useLegacyPackaging
}
val verifySamplePackagingDeclaration =
    tasks.register("verifySamplePackagingDeclaration") {
        group = "verification"
        description = "Assert the evaluated packaging model declares useLegacyPackaging = false."
        doLast {
            check(evaluatedLegacyPackaging == false) {
                "the evaluated jniLibs.useLegacyPackaging is $evaluatedLegacyPackaging; " +
                    "this sample's packaging policy is an explicit false (uncompressed native libraries)"
            }
        }
    }
tasks.named("check") {
    dependsOn(verifySamplePackagingDeclaration)
}

android {
    namespace = "org.openmoq.moq5.samples.publisher"
    compileSdk = platform.substringBefore(".").toInt()
    platform.substringAfter(".", "").takeIf { it.isNotEmpty() }?.let {
        compileSdkMinor = it.toInt()
    }
    buildToolsVersion = pin("sdk.buildTools")

    defaultConfig {
        // Experimental build input only; see the header. No product promise.
        //
        // 23, not 21, for one packaging reason: a native library can only be
        // STORED uncompressed and mapped from the APK on API 23 and above, so
        // below it AGP packages `lib/<abi>/*.so` deflated and the platform has
        // to extract a copy at install time. This is the SAMPLE's build input;
        // the library's published floor is unchanged at 21 and is asserted by
        // `check-maven-publication.sh`.
        minSdk = 23
    }

    // Native libraries are STORED, not deflated: an explicit false is this
    // sample's packaging policy. A compressed library cannot be page-aligned
    // or mapped from the APK -- the loader has to extract a copy first, which
    // is what the 16 KiB alignment contract exists to avoid -- and a
    // zipalign check reports a compressed entry as OK because nothing is left
    // to align. What actually ships is judged on the APK itself
    // (tools/check-package-alignment.sh); verifySamplePackagingDeclaration
    // judges that this declaration is evaluated.
    packaging {
        jniLibs {
            useLegacyPackaging = false
        }
    }
}

/*
 * Warnings are failures here for the same reason they are in the library:
 * org.gradle.warning.mode=fail governs Gradle's own warnings, not the Kotlin
 * compiler's, Android lint's, or javac's. A sample that tolerated a warning
 * would be a consumer proving the surface is warning-clean while not being
 * warning-clean itself.
 */
kotlin {
    compilerOptions {
        allWarningsAsErrors.set(true)
    }
}

android {
    lint {
        warningsAsErrors = true
        abortOnError = true
        // Nothing is demoted in a sample.
    }
}

// The escape hatches, refused by literals independent of the configuration
// above, so widening the configuration alone fails here.
check(android.lint.informational.isEmpty()) {
    "lint informational set is ${android.lint.informational}, expected empty; no issue may be demoted in a sample"
}
check(android.lint.disable.isEmpty() && android.lint.ignoreWarnings == false) {
    "lint disable/ignoreWarnings must stay empty in a sample"
}
check(android.lint.baseline == null) {
    "a lint baseline hides existing findings and is not accepted"
}

// Configured for any future Java source, exactly as the library configures it.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

dependencies {
    // The library exports coroutines as an `api` dependency, because its public
    // surface returns `StateFlow` and suspends. A sample therefore USES coroutine
    // APIs without declaring the artifact: declaring it here would teach the
    // wrong consumer contract and hide a regression in that export.
    implementation(project(":moq5-android"))

    // Unit tests for the sample's OWN lifecycle code, in the same two artifacts
    // the library module already uses. They are test-only: the sample still
    // declares no coroutines artifact of its own, and the published surface it
    // consumes is unchanged. `kotlinx-coroutines-test` is what makes the
    // Start/Stop ordering deterministic instead of timing-dependent.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:${pin("kotlin.version")}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${pin("coroutines.version")}")
}

/*
 * The HOST codec lane: the sample's configuration step through the real
 * public binding, with the host bridge image present.
 *
 * It is test orchestration, not a native build or a loader: the image is the
 * library's own verified host build (`:moq5-android:buildBridgeTestDsos`),
 * reached by the JVM's by-name resolution exactly as the library's
 * `testBridgeJni` reaches it, and the sample's code calls only the public
 * `CodecSignaling`. The native owner's decision is process-sticky, so these
 * rows run in their own JVM per class, and the ordinary unit tests -- which
 * include the image-ABSENT owner row -- never see them.
 */
private val hostCodecTestPattern = "org.openmoq.moq5.samples.publisher.host.*"

val verifyHostCodecDiagnostics =
    tasks.register<Exec>("verifyHostCodecDiagnostics") {
        group = "verification"
        description = "Assert no -Xcheck:jni diagnostic and no native recorder report appears in any host codec fork's output."
        argumentProviders.add(
            CommandLineArgumentProvider {
                listOf(
                    layout.buildDirectory.dir("test-results/testHostCodec").get().asFile.absolutePath,
                    layout.buildDirectory.file("test-results/testHostCodec.moq5-token").get().asFile.absolutePath,
                    providers.environmentVariable("MOQ5_RUN_TOKEN").orElse("").get(),
                    providers.environmentVariable("MOQ5_RUN_LOG").orElse("").get(),
                    // These forks expect NO native recorder report: a lost
                    // recorder inventory fails the lane, not just a JNI warning.
                    "--recorder",
                )
            },
        )
        commandLine(
            "/bin/sh",
            rootProject.layout.projectDirectory.file("tools/check-jni-diagnostics.sh").asFile.absolutePath,
        )
    }

val testHostCodec =
    tasks.register<Test>("testHostCodec") {
        group = "verification"
        description = "Run the sample's codec configuration through the real public binding, one JVM per class."
        useJUnit()
        forkEvery = 1
        filter {
            includeTestsMatching(hostCodecTestPattern)
            isFailOnNoMatchingTests = true
        }
        dependsOn(":moq5-android:buildBridgeTestDsos")
        // The image these forks load is a real input: a changed image must
        // never leave this task UP-TO-DATE over it.
        inputs.dir(rootProject.layout.projectDirectory.dir(".native/_bridge"))
            .withPropertyName("bridgeTestDsos")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        systemProperty(
            "java.library.path",
            rootProject.layout.projectDirectory.dir(".native/_bridge").asFile.absolutePath,
        )
        jvmArgs("-Xcheck:jni")
        // Always executes: the diagnostic gate judges this invocation's forks.
        outputs.upToDateWhen { false }
        val tokenFile = layout.buildDirectory.file("test-results/testHostCodec.moq5-token")
        val runToken = providers.environmentVariable("MOQ5_RUN_TOKEN").orElse("")
        doFirst {
            val f = tokenFile.get().asFile
            f.parentFile.mkdirs()
            f.writeText(runToken.get())
        }
        finalizedBy(verifyHostCodecDiagnostics)
        finalizedBy(":verifyNoCrashResidue")
    }

afterEvaluate {
    val unit = tasks.named<Test>("testDebugUnitTest")
    unit.configure {
        filter {
            excludeTestsMatching(hostCodecTestPattern)
            isFailOnNoMatchingTests = false
        }
    }
    testHostCodec.configure {
        testClassesDirs = unit.get().testClassesDirs
        classpath = unit.get().classpath
        dependsOn("compileDebugUnitTestKotlin", "compileDebugKotlin")
    }
}

// The publisher's canonical verification boundary: `check` runs the host
// codec lane (whose own finalizer is its diagnostic gate), so root `check`,
// which depends on this module's `check`, runs it too.
tasks.named("check") {
    dependsOn(testHostCodec)
}

// The boundary contract, read from the DECLARED dependencies of `check` once
// the project is configured and compared with a literal that is independent
// of that wiring: deleting the dependency removes it from the graph but not
// from this set, so every build fails by name instead of silently no longer
// running the lane. The declared set is read, not resolved: resolving it
// would reach another project's tasks, which Gradle refuses to resolve from
// here.
val expectedPublisherCheckDependencies = setOf("testHostCodec", "verifySamplePackagingDeclaration")

afterEvaluate {
    fun flatten(value: Any?): List<String> =
        when (value) {
            is CharSequence -> listOf(value.toString())
            is TaskProvider<*> -> listOf(value.name)
            is Task -> listOf(value.name)
            is Iterable<*> -> value.flatMap { flatten(it) }
            is Array<*> -> value.flatMap { flatten(it) }
            else -> emptyList()
        }
    val declared = tasks.named("check").get().dependsOn.flatMap { flatten(it) }.toSet()
    val missing = expectedPublisherCheckDependencies.filterNot { it in declared }
    check(missing.isEmpty()) {
        "`:samples:publisher:check` does not depend on $missing; a verification task `check` " +
            "does not reach is never run by the canonical gate"
    }
}
