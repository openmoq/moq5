// TEST-ONLY R8 fixture. This module exists to prove one thing: that a consuming
// application's R8 pass, applying the library's OWN packaged consumer rules,
// leaves the JNI bridge's registration identity intact.
//
// It is verification infrastructure. It is never published, it is not either of
// the planned samples, and nothing in it is a product surface.
//
// It sets `minSdk` only because an application module requires one, using the
// already-established experimental cross-build input. THAT IS NOT A PRODUCT
// minSdk PROMISE: the library's supported API floor is an open ruling and is
// not decided here.
//
// It consumes the library's consumer rules through the ordinary project
// dependency -- AGP applies an AAR's packaged rules automatically -- so nothing
// here copies them. A copy could drift and would prove nothing.
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

android {
    namespace = "org.openmoq.moq5.r8fixture"
    compileSdk = platform.substringBefore(".").toInt()
    platform.substringAfter(".", "").takeIf { it.isNotEmpty() }?.let {
        compileSdkMinor = it.toInt()
    }
    buildToolsVersion = pin("sdk.buildTools")

    defaultConfig {
        // Experimental build input only; see the header. No product promise.
        minSdk = 21
    }

    buildTypes {
        release {
            isMinifyEnabled = true
            // Resource shrinking is enabled because lint is right to ask for it
            // beside minification, and because it widens what this fixture
            // proves: the bound class must survive the resource shrinker too,
            // not only the code shrinker. It is resolved, never suppressed.
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                file("fixture-rules.pro"),
            )
        }
    }
}

/*
 * Warnings are failures here for the same reason they are in the library:
 * org.gradle.warning.mode=fail governs Gradle's own warnings, not the Kotlin
 * compiler's, Android lint's, or javac's.
 *
 * This module is a verification fixture, so a warning it tolerates is a warning
 * the canonical gate tolerates. `1211` reported this module as warning-free on
 * the strength of the Gradle console alone; the lint REPORT carried a real
 * NotShrinkingResources finding and `check` still exited 0 (chats/libmoq/1212
 * F1). The policy below is what makes the claim true rather than asserted.
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
        // Nothing is demoted in this module. The library's single freshness
        // advisory is not inherited here, and the emptiness is asserted below.
    }
}

// An escape hatch that nothing enforces is an escape hatch. These literals are
// independent of the configuration above, so widening the configuration alone
// fails here rather than passing quietly.
check(android.lint.informational.isEmpty()) {
    "lint informational set is ${android.lint.informational}, expected empty; " +
        "no issue may be demoted in the R8 fixture"
}
check(android.lint.disable.isEmpty() && android.lint.ignoreWarnings == false) {
    "lint disable/ignoreWarnings must stay empty in the R8 fixture"
}
check(android.lint.baseline == null) {
    "a lint baseline hides existing findings and is not accepted"
}
check(android.lint.warningsAsErrors == true && android.lint.abortOnError == true) {
    "lint must fail the build on warnings in the R8 fixture"
}

// The javac warning policy, and one Java source that EXERCISES it. The list
// used to configure the compiler is deliberately separate from the literal the
// oracle below compares against, so widening the configuration alone fails.
val javacWarningArgs = listOf("-Xlint:all", "-Werror")

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(javacWarningArgs)
}

check(javacWarningArgs.toSet() == setOf("-Xlint:all", "-Werror")) {
    "javac warning policy is $javacWarningArgs, expected exactly " +
        "[-Xlint:all, -Werror]"
}

dependencies {
    implementation(project(":moq5-android"))
}

/*
 * The R8 identity gate.
 *
 * It depends on the minified release build because its evidence IS that build's
 * output: the DEX and the mapping R8 actually produced under the library's own
 * packaged consumer rules.
 */
val verifyR8BridgeIdentity =
    tasks.register<Exec>("verifyR8BridgeIdentity") {
        group = "verification"
        description = "Assert the JNI bridge's registration identity survives R8."
        dependsOn("assembleRelease")
        commandLine(
            "/bin/sh",
            rootProject.layout.projectDirectory.file("tools/check-r8-identity.sh")
                .asFile.absolutePath,
        )
    }

/*
 * The warning policy's own gate.
 *
 * The `check(...)` calls above run at configuration time and catch a WIDENED
 * configuration. They cannot catch a DELETED one: remove the block and there is
 * nothing left to evaluate. This task asserts the policy from the outside, at
 * execution, so deletion is a failure rather than a silent absence -- and it
 * reports how many JavaCompile tasks it actually inspected, so a vacuous pass
 * is visible instead of being mistaken for coverage.
 */
tasks.register("verifyWarningPolicy") {
    group = "verification"
    description = "Assert this fixture fails on Kotlin, lint and javac warnings."
    val kotlinAsErrors = kotlin.compilerOptions.allWarningsAsErrors
    val lintWarnAsErrors = android.lint.warningsAsErrors
    val lintAbort = android.lint.abortOnError
    val lintInformational = android.lint.informational.toSet()
    val lintDisable = android.lint.disable.toSet()
    val lintIgnoreWarnings = android.lint.ignoreWarnings
    val lintBaseline = android.lint.baseline
    val javacArgs =
        tasks.withType(JavaCompile::class.java).map { it.options.compilerArgs.toList() }
    doLast {
        check(kotlinAsErrors.get()) {
            "kotlin allWarningsAsErrors is off; a Kotlin warning would pass the gate"
        }
        check(lintWarnAsErrors && lintAbort) {
            "lint warningsAsErrors/abortOnError are off; a lint warning would pass"
        }
        check(lintInformational.isEmpty() && lintDisable.isEmpty()) {
            "lint demotions present: informational=$lintInformational disable=$lintDisable"
        }
        check(!lintIgnoreWarnings) {
            "lint ignoreWarnings is on; every lint warning would pass the gate"
        }
        check(lintBaseline == null) { "a lint baseline is not accepted" }
        check(javacArgs.isNotEmpty()) {
            "no JavaCompile task carries the warning policy; with no Java source " +
                "the policy would be unexercised and undetectable"
        }
        // The warning-affecting VOCABULARY is pinned, not a list of convenient
        // spellings. Enumerating `-nowarn`, `-Xlint:none` and `-Xlint:-foo`
        // still admitted javac's combined form `-Xlint:all,-rawtypes`, which
        // silences a category while leaving the accepted flags in place for an
        // oracle to find (chats/libmoq/1216 F3).
        //
        // The rule: `-Xlint:all` and `-Werror` must each appear exactly as
        // intended, and NO OTHER `-Xlint` form, no `-nowarn`, and no
        // `-Xmaxwarns` form may accompany them. Options that do not affect
        // warnings -- `-XDstringConcat=inline`, `-source`, `-target`, ... --
        // are unconstrained.
        javacArgs.forEach { args ->
            check(args.containsAll(listOf("-Xlint:all", "-Werror"))) {
                "a JavaCompile task is missing the warning policy: $args"
            }
            val offending =
                args.filter {
                    when {
                        it == "-Xlint:all" || it == "-Werror" -> false
                        it == "-nowarn" -> true
                        it.startsWith("-Xmaxwarns") -> true
                        it == "-Xlint" || it.startsWith("-Xlint:") -> true
                        else -> false
                    }
                }
            check(offending.isEmpty()) {
                "a JavaCompile task carries warning-affecting flag(s) $offending " +
                    "beyond the accepted [-Xlint:all, -Werror]; any other -Xlint " +
                    "form, -nowarn or -Xmaxwarns can silence the compiler: $args"
            }
        }
        logger.lifecycle(
            "PASS[fixture.warning-policy]: kotlin+lint+javac fail on warnings " +
                "(${javacArgs.size} JavaCompile task(s) inspected)",
        )
    }
}

/*
 * This module's canonical verification boundary, asserted by a task that does
 * NOT depend on the suite it describes, for the same reason the library's does:
 * a boundary check buried inside `check`'s own action stops running exactly when
 * one of `check`'s dependencies fails.
 */
val expectedFixtureChecks =
    setOf("assembleRelease", "verifyR8BridgeIdentity", "verifyWarningPolicy", "lintDebug")

tasks.named("check") {
    dependsOn(verifyR8BridgeIdentity)
    dependsOn("verifyWarningPolicy")
    dependsOn("verifyFixtureCheckBoundary")
}

tasks.register("verifyFixtureCheckBoundary") {
    group = "verification"
    description = "Assert `check` reaches the R8 identity gate."
    val ownPath = project.path
    doLast {
        val reachable = mutableSetOf<String>()
        val seen = mutableSetOf<Task>()
        // The walk records every dependency's name but recurses only within this
        // project. This module depends on the library, and asking a task in
        // another project for its dependencies is illegal in Gradle 9 ("Current
        // thread does not hold the state lock for project ..."). Every name this
        // boundary asserts belongs to this module, so the confinement costs the
        // check nothing.
        //
        // Ownership is read from the task PATH rather than from `Task.project`,
        // which Gradle 9 deprecates at execution time. A task path is its
        // owning project's path plus ":" plus its name.
        fun ownerOf(t: Task): String = t.path.substringBeforeLast(':')
        fun walk(t: Task) {
            if (!seen.add(t)) return
            t.taskDependencies.getDependencies(t).forEach {
                reachable += it.name
                if (ownerOf(it) == ownPath) walk(it)
            }
        }
        walk(tasks.named("check").get())
        val missing = expectedFixtureChecks.filterNot { it in reachable }
        check(missing.isEmpty()) {
            "`:bridge-r8-fixture:check` does not reach $missing; this fixture's " +
                "evidence and its warning policy must be part of the canonical " +
                "gate, not a manual step"
        }
    }
}
