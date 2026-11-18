// MoQ5 Android/Kotlin bindings -- Gradle root.
//
// The accepted topology is ONE product AAR module, `moq5-android`, carrying the
// public Kotlin API plus its internal engine/SPI/JNI backend; it is declared
// below. The separately published `moq5-api` module that an early draft proposed
// was rejected (a Kotlin `internal` backend SPI is module-scoped, so a second
// published artifact could not implement it) and is not revived here.
//
// Everything this file needs is read from gradle/toolchain.properties, the
// single source of truth for every pin. The AGP version is NOT written here:
// a second literal would let the two drift and would make a version
// perturbation pass by text comparison alone.

pluginManagement {
    val toolchainFile = java.io.File(settingsDir, "gradle/toolchain.properties")
    require(toolchainFile.isFile) { "missing ${toolchainFile.path}" }
    val toolchain = java.util.Properties()
    toolchainFile.inputStream().use { toolchain.load(it) }
    val agpVersion: String =
        toolchain.getProperty("agp.version")
            ?: error("gradle/toolchain.properties is missing the pin 'agp.version'")

    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }

    // Declaring the version here is what makes the root's `apply false` a REAL
    // resolution: Gradle must find the com.android.library plugin marker at
    // exactly this version, so a nonexistent version fails in Gradle's own
    // plugin resolution rather than in a shell string comparison.
    plugins {
        id("com.android.library") version agpVersion
    }
}

rootProject.name = "moq5-bindings"

// The single product library module. The rejected two-artifact split (a
// separately published `moq5-api`) is deliberately not revived: a Kotlin
// `internal` backend SPI is module-scoped, so a second published artifact could
// not implement it.
include(":moq5-android")

// The two first-party sample applications. They are API acceptance apps: each
// consumes the published `:moq5-android` surface through an ordinary project
// dependency, and neither is ever published. `tools/check-sample-apps.sh` holds
// them to that contract, and the root's module verification boundary names both
// `check` tasks explicitly rather than relying on task-name expansion.
include(":samples:player")
include(":samples:publisher")

// A TEST-ONLY R8 fixture: a consuming application used to prove the bridge's
// registration identity survives a minified release build under the library's
// own packaged consumer rules. Never published, and not either planned sample.
include(":bridge-r8-fixture")

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}
