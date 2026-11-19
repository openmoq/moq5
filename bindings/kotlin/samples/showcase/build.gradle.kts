// The MoQ5 showcase: a complete Android application over the shared playback
// engine (`samples/playback`) and the published `:moq5-android` surface.
//
// Where `samples/player` proves the receive API with plain views and nothing
// in between, this sample shows what an application built on it looks like:
// Jetpack Compose with Material 3, a ViewModel that survives configuration
// changes, picture-in-picture, audio focus, and settings for the draft offer
// and start mode. Its UI libraries are its own: `tools/check-sample-apps.sh`
// keeps them out of the library, the engine and the other samples.
//
// It sets `minSdk` 26 because picture-in-picture and AudioFocusRequest need
// it; that is this sample's floor, not the library's (21) or a product promise.
import java.util.Properties
import org.gradle.api.artifacts.component.ComponentIdentifier
import org.gradle.api.artifacts.result.ResolvedDependencyResult

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.plugin.compose")
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
    namespace = "org.openmoq.moq5.samples.showcase"
    compileSdk = platform.substringBefore(".").toInt()
    platform.substringAfter(".", "").takeIf { it.isNotEmpty() }?.let {
        compileSdkMinor = it.toInt()
    }
    buildToolsVersion = pin("sdk.buildTools")

    defaultConfig {
        applicationId = "org.openmoq.moq5.samples.showcase"
        minSdk = 26
    }

    buildFeatures {
        compose = true
    }

    // Native libraries are stored, not deflated, as in the player sample.
    packaging {
        jniLibs {
            useLegacyPackaging = false
        }
    }
}

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

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

// The pinned compiler and coroutines decide the runtime: no UI library may
// raise kotlin-stdlib or kotlinx-coroutines above the toolchain pins. Judged on
// the RESOLVED runtime classpath, which is what ships.
val verifyShowcaseRuntimePins =
    tasks.register("verifyShowcaseRuntimePins") {
        group = "verification"
        description = "Assert the resolved runtime classpath keeps kotlin-stdlib and kotlinx-coroutines at the toolchain pins."
        val root = configurations.named("releaseRuntimeClasspath").flatMap { it.incoming.resolutionResult.rootComponent }
        val kotlinPin = pin("kotlin.version")
        val coroutinesPin = pin("coroutines.version")
        inputs.property("kotlinPin", kotlinPin)
        inputs.property("coroutinesPin", coroutinesPin)
        doLast {
            val resolved = mutableMapOf<String, String>()
            val visited = mutableSetOf<ComponentIdentifier>()
            val queue = ArrayDeque(listOf(root.get()))
            while (queue.isNotEmpty()) {
                val component = queue.removeFirst()
                if (!visited.add(component.id)) continue
                component.moduleVersion?.let { resolved["${it.group}:${it.name}"] = it.version }
                component.dependencies.filterIsInstance<ResolvedDependencyResult>().forEach { queue.add(it.selected) }
            }
            val stdlib = resolved["org.jetbrains.kotlin:kotlin-stdlib"] ?: error("kotlin-stdlib is absent from the showcase runtime classpath")
            check(stdlib == kotlinPin) { "kotlin-stdlib resolved to $stdlib; the toolchain pin is $kotlinPin" }
            // Every module of the coroutines family, whichever the UI libraries bring.
            val coroutines = resolved.filterKeys { it.startsWith("org.jetbrains.kotlinx:kotlinx-coroutines") }
            check(coroutines.isNotEmpty()) { "no kotlinx-coroutines module on the showcase runtime classpath" }
            val off = coroutines.filterValues { it != coroutinesPin }
            check(off.isEmpty()) { "kotlinx-coroutines modules off the toolchain pin $coroutinesPin: $off" }
            logger.lifecycle(
                "showcase runtime: ${resolved.size} modules; kotlin-stdlib $stdlib; ${coroutines.keys.sorted()} at $coroutinesPin",
            )
        }
    }
tasks.named("check") {
    dependsOn(verifyShowcaseRuntimePins)
}

dependencies {
    implementation(project(":moq5-android"))
    implementation(project(":samples:playback"))

    implementation(platform("androidx.compose:compose-bom:${pin("androidx.composeBom.version")}"))
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.foundation:foundation")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.material:material-icons-core")
    implementation("androidx.compose.ui:ui-tooling-preview")
    debugImplementation("androidx.compose.ui:ui-tooling")
    implementation("androidx.activity:activity-compose:${pin("androidx.activityCompose.version")}")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:${pin("androidx.lifecycle.version")}")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:${pin("androidx.lifecycle.version")}")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:${pin("kotlin.version")}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${pin("coroutines.version")}")
}
