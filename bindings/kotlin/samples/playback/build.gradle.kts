// The playback engine the Android samples share.
//
// Receiving, decoding, A/V sync and presentation for a MoQ5 broadcast, written
// against the published `:moq5-android` surface and nothing private, so it is
// part of what the samples prove about that surface. Two applications use it:
// `samples/player`, the framework-free API-acceptance app, and
// `samples/showcase`, the full Android app. It is never published.
//
// It holds the same line as the acceptance sample: plain platform media APIs
// and coroutines from the library's own export; no Compose, AndroidX
// lifecycle, Media3 or network library. `tools/check-sample-apps.sh` enforces
// that. Its public declarations are what the two applications call; the rest
// stays internal to the module.
import java.util.Properties

plugins {
    id("com.android.library")
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

android {
    namespace = "org.openmoq.moq5.samples.playback"
    compileSdk = platform.substringBefore(".").toInt()
    platform.substringAfter(".", "").takeIf { it.isNotEmpty() }?.let {
        compileSdkMinor = it.toInt()
    }
    buildToolsVersion = pin("sdk.buildTools")

    defaultConfig {
        // The applications' floor (see samples/player): the engine needs no lower.
        minSdk = 23
    }
}

/*
 * Warnings are failures here for the same reason they are in the library and
 * the applications: org.gradle.warning.mode=fail governs Gradle's own warnings,
 * not the Kotlin compiler's, Android lint's, or javac's.
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
    }
}

check(android.lint.informational.isEmpty()) {
    "lint informational set is ${android.lint.informational}, expected empty; no issue may be demoted in a sample"
}
check(android.lint.disable.isEmpty() && android.lint.ignoreWarnings == false) {
    "lint disable/ignoreWarnings must stay empty in a sample"
}
check(android.lint.baseline == null) {
    "a lint baseline hides existing findings and is not accepted"
}

// The marker tests read the declared marker vectors (written from the recipe
// by build_vectors.py). They are a task input, so a change re-runs the tests
// instead of reusing a stale result.
val androidMediaAuthorities = rootDir.resolve("../python/tests/runtime/android_media").normalize().let { dir ->
    mapOf(
        "moq5.player.markerVectors" to dir.resolve("marker_vectors.json"),
    )
}
tasks.withType<Test>().configureEach {
    androidMediaAuthorities.forEach { (property, file) ->
        inputs.file(file).withPropertyName(property).withPathSensitivity(PathSensitivity.NONE)
        systemProperty(property, file.path)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

dependencies {
    // `api`, not `implementation`: the engine's public declarations name the
    // library's types, so an application using the engine compiles against
    // them too. Coroutines arrive through the library's own `api` export.
    api(project(":moq5-android"))

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:${pin("kotlin.version")}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${pin("coroutines.version")}")
}
