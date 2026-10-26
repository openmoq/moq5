// MoQ5 Android library module.
//
// AGP 9 has built-in Kotlin support, so the Kotlin plugin is DELIBERATELY not
// applied here: applying org.jetbrains.kotlin.android alongside it is the
// migration error the Android guidance calls out
// (developer.android.com/build/migrate-to-built-in-kotlin).
//
// The module carries the public Kotlin API, the INTERNAL endpoint engine and
// backend SPI, and the NativeBridge declaration for the single packaged JNI
// image. Android sample applications are intentionally separate modules.

import java.security.MessageDigest
import java.util.Properties

plugins {
    id("com.android.library")
    id("maven-publish")
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

/**
 * The compile platform, parsed from `sdk.platform` with an EXACT grammar.
 *
 * A `substringBefore(".")` would silently compile against android-36 while the
 * declaration said android-36.1 -- and both are installed here, so that build
 * would pass while using the wrong platform. Trailing or malformed text is
 * rejected rather than truncated away.
 */
data class CompileSdkPin(val major: Int, val minor: Int)

fun parseCompileSdkPin(raw: String): CompileSdkPin {
    val m = Regex("""^android-(\d+)(?:\.(\d+))?$""").matchEntire(raw)
        ?: error(
            "sdk.platform must be exactly 'android-<major>' or " +
                "'android-<major>.<minor>'; got '$raw'",
        )
    val major = m.groupValues[1].toIntOrNull()
        ?: error("sdk.platform major is not an integer in '$raw'")
    val minor =
        if (m.groupValues[2].isEmpty()) {
            0
        } else {
            m.groupValues[2].toIntOrNull()
                ?: error("sdk.platform minor is not an integer in '$raw'")
        }
    return CompileSdkPin(major, minor)
}

val compileSdkPin = parseCompileSdkPin(pin("sdk.platform"))

/*
 * The published coordinates.
 *
 * The version is READ from the C library's own authority --
 * `core/include/moq/version.h`, the same `MOQ_VERSION_STRING` the native build
 * compiles against -- rather than written here as a second copy that could
 * drift from the code it describes. For v1 the Android artifact tracks the
 * library version; if the two ever move independently this is the one place
 * that changes.
 */
val versionHeader = rootProject.layout.projectDirectory.file("../../core/include/moq/version.h").asFile

fun libmoqVersion(): String {
    check(versionHeader.isFile) { "no version authority at ${versionHeader.path}" }
    val declared =
        Regex("""^#define\s+MOQ_VERSION_STRING\s+"([^"]+)"\s*$""", RegexOption.MULTILINE)
            .find(versionHeader.readText())
            ?.groupValues?.get(1)
            ?: error("${versionHeader.path} declares no MOQ_VERSION_STRING")
    check(declared.isNotBlank()) { "MOQ_VERSION_STRING is empty in ${versionHeader.path}" }
    return declared
}

group = "org.openmoq"
version = libmoqVersion()

/*
 * The supported ABIs, and where their audited payload comes from.
 *
 * `.native/_topology/<abi>/package/libmoq5_jni.so` is the object the topology
 * gate vouches for by digest. It is STAGED into a generated directory here,
 * never copied into source: the AAR must carry the audited bytes, and the
 * publication gate compares the archive member's SHA-256 against that package.
 */
val supportedAbis = listOf("arm64-v8a", "x86_64")

/**
 * Copies the audited payload into the `<abi>/libmoq5_jni.so` layout an AAR
 * expects. A typed task with a declared output directory, because that is what
 * the variant API can attach as a GENERATED source directory -- the payload is
 * never checked into the source tree.
 */
abstract class StageNativePayload : DefaultTask() {
    @get:InputDirectory
    abstract val topology: DirectoryProperty

    @get:Input
    abstract val abis: ListProperty<String>

    @get:OutputDirectory
    abstract val outputDir: DirectoryProperty

    @TaskAction
    fun stage() {
        val out = outputDir.get().asFile
        out.deleteRecursively()
        val missing = mutableListOf<String>()
        for (abi in abis.get()) {
            val source = topology.get().file("$abi/package/libmoq5_jni.so").asFile
            if (!source.isFile) {
                missing += abi
                continue
            }
            val target = out.resolve(abi).apply { mkdirs() }.resolve("libmoq5_jni.so")
            source.copyTo(target, overwrite = true)
        }
        check(missing.isEmpty()) {
            "no audited topology package for $missing; run tools/build-jni-topology.sh before " +
                "assembling an artifact that claims a native payload"
        }
    }
}

/**
 * Delivers the audited third-party notice closure INTO the artifact.
 *
 * The topology stage collects one notice per declared component per ABI and
 * records component, digest, archive-relative path and pinned source in
 * `<abi>/licenses.manifest`. Collecting them proves nothing to a consumer: the
 * bytes have to be inside the AAR a consumer resolves, and inside the APK that
 * consumer builds. This task copies those exact bytes into the Java-resource
 * layout the notice gate reads, and writes the consumer-facing index beside
 * them.
 *
 * The declaration is the AUTHORITY: every component the manifest declares is
 * delivered, each copy is verified against the declared digest before it is
 * written, and the ABIs must agree -- a component whose two ABIs disagree
 * would leave a consumer holding bytes that describe only half the artifact.
 */
abstract class StageNoticePayload : DefaultTask() {
    @get:InputDirectory
    abstract val topology: DirectoryProperty

    @get:Input
    abstract val abis: ListProperty<String>

    @get:OutputDirectory
    abstract val outputDir: DirectoryProperty

    private fun rowsOf(abi: String): Map<String, Pair<String, String>> {
        val manifest = topology.get().file("$abi/licenses.manifest").asFile
        check(manifest.isFile) {
            "no licence manifest at ${manifest.path}; run tools/build-jni-topology.sh " +
                "before assembling an artifact that claims a notice closure"
        }
        val rows = LinkedHashMap<String, Pair<String, String>>()
        manifest.readLines().filter { it.isNotEmpty() }.forEach { line ->
            val parts = line.split("\t")
            check(parts.size == 4 && parts.none { it.isEmpty() }) {
                "${manifest.path}: a row is not four non-empty tab-separated fields: $line"
            }
            check(rows.put(parts[0], parts[1] to parts[2]) == null) {
                "${manifest.path}: ${parts[0]} is declared more than once"
            }
        }
        check(rows.isNotEmpty()) { "${manifest.path} declares no component" }
        return rows
    }

    @TaskAction
    fun stage() {
        val out = outputDir.get().asFile
        out.deleteRecursively()
        val declared = abis.get().associateWith { rowsOf(it) }
        val first = declared.getValue(abis.get().first())
        declared.forEach { (abi, rows) ->
            check(rows.keys == first.keys) {
                "$abi declares ${rows.keys} but ${abis.get().first()} declares ${first.keys}; " +
                    "the notice closure must be the same for every ABI"
            }
            rows.forEach { (component, row) ->
                check(row.first == first.getValue(component).first) {
                    "$component hashes ${row.first} for $abi but ${first.getValue(component).first} " +
                        "for ${abis.get().first()}; a consumer cannot receive both"
                }
            }
        }
        val dir = out.resolve(NOTICE_DIR).apply { mkdirs() }
        val index = StringBuilder()
        for ((component, row) in first) {
            val (digest, rel) = row
            val source = topology.get().file("${abis.get().first()}/$rel").asFile
            check(source.isFile) { "the collected notice for $component is absent: ${source.path}" }
            val bytes = source.readBytes()
            val got =
                MessageDigest.getInstance("SHA-256").digest(bytes)
                    .joinToString("") { byte -> "%02x".format(byte) }
            check(got == digest) {
                "${source.path} hashes $got; the audited manifest declares $digest"
            }
            val target = dir.resolve("$component.LICENSE")
            target.writeBytes(bytes)
            index.append(component).append('\t').append(digest).append('\t')
                .append("$NOTICE_DIR/$component.LICENSE").append('\n')
        }
        dir.resolve("licenses.manifest").writeText(index.toString())
    }

    companion object {
        // The resource path the notice gate reads, and the one a consumer
        // indexes. It is part of the artifact contract, not a local choice.
        const val NOTICE_DIR = "META-INF/org.openmoq/moq5/notices"
    }
}

val stageNoticePayload =
    tasks.register<StageNoticePayload>("stageNoticePayload") {
        group = "build"
        description = "Deliver the audited third-party notice closure into the artifact's resources."
        topology.set(rootProject.layout.projectDirectory.dir(".native/_topology"))
        abis.set(supportedAbis)
        outputDir.set(layout.buildDirectory.dir("generated/noticeResources"))
    }

val stageNativePayload =
    tasks.register<StageNativePayload>("stageNativePayload") {
        group = "build"
        description = "Stage the audited topology payload into the AAR's jniLibs layout."
        topology.set(rootProject.layout.projectDirectory.dir(".native/_topology"))
        abis.set(supportedAbis)
        outputDir.set(layout.buildDirectory.dir("generated/jniLibs"))
    }

// The variant API attaches it as a GENERATED source directory, so every
// variant's archive carries the audited bytes and nothing is checked in.
androidComponents {
    onVariants { variant ->
        variant.sources.jniLibs?.addGeneratedSourceDirectory(
            stageNativePayload,
            StageNativePayload::outputDir,
        )
        // The notices travel as Java resources, so they reach the AAR and every
        // APK built against it by the ordinary packaging route.
        variant.sources.resources?.addGeneratedSourceDirectory(
            stageNoticePayload,
            StageNoticePayload::outputDir,
        )
    }
}


android {
    defaultConfig {
        consumerProguardFiles("consumer-rules.pro")

        // The published API floor, ruled at chats/libmoq/1519 on the evidence
        // gathered for 1512: the JNI object is linked against the NDK's API-21
        // sysroot, every strong undefined symbol resolves there, the two that
        // do not (getentropy, memfd_create) are weak and null-checked, and no
        // Kotlin in this module reaches above 21 outside an SDK_INT guard.
        minSdk = 21

        // The instrumentation the RUNTIME acceptance gate drives. AGP only
        // builds an androidTest APK when a runner is declared, so this line is
        // what makes `connectedDebugAndroidTest` a real device run rather than
        // an up-to-date no-op that writes no report.
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    namespace = "org.openmoq.moq5"
    compileSdk = compileSdkPin.major
    compileSdkMinor = compileSdkPin.minor

    // ONE published variant. AGP creates the `release` software component only
    // when a variant is declared publishable, and exactly one is declared:
    // debug is a development artifact, not something a consumer resolves.
    publishing {
        singleVariant("release")
    }
}

// The pin is only a contract if something compares it with what AGP actually
// configured. Reading the extension back independently is what makes dropping
// the minor assignment a NAMED failure rather than a silently different build.
check(android.compileSdk == compileSdkPin.major) {
    "compileSdk is ${android.compileSdk} but gradle/toolchain.properties pins " +
        "sdk.platform=${pin("sdk.platform")} (major ${compileSdkPin.major})"
}
check(android.compileSdkMinor == compileSdkPin.minor) {
    "compileSdkMinor is ${android.compileSdkMinor} but gradle/toolchain.properties pins " +
        "sdk.platform=${pin("sdk.platform")} (minor ${compileSdkPin.minor}); " +
        "an installed android-${compileSdkPin.major} would otherwise let this " +
        "build pass against the wrong platform"
}

// The Kotlin version is AGP's, not ours. The pin exists so the test framework
// can be requested at exactly that version; this check makes the pin real, so a
// future AGP bump fails here instead of silently resolving a mismatched
// kotlin-test against a different compiler.
val kotlinInUse: String =
    Class.forName("org.jetbrains.kotlin.config.KotlinCompilerVersion")
        .getField("VERSION").get(null).toString()
check(kotlinInUse == pin("kotlin.version")) {
    "AGP's built-in Kotlin is $kotlinInUse but gradle/toolchain.properties pins " +
        "kotlin.version=${pin("kotlin.version")}"
}

/*
 * Warnings are failures, and org.gradle.warning.mode=fail does NOT cover these:
 * it governs Gradle's own warnings, not the Kotlin compiler's, Android lint's,
 * or javac's. Each is turned into an error explicitly (chats/libmoq/1187 F1).
 */
kotlin {
    compilerOptions {
        allWarningsAsErrors.set(true)
    }
}

/*
 * Exactly ONE lint issue is informational, and it is not a source defect.
 *
 * AndroidGradlePluginVersion is a FRESHNESS advisory: it reports that a newer
 * Gradle than the one in our wrapper exists. Our wrapper version and its
 * distribution SHA-256 are pinned by gradle/toolchain.properties and verified
 * by the toolchain gate, so an older version here is deliberate. Letting an
 * upstream release turn a previously reproducible build red would make the gate
 * a function of release timing rather than of our code
 * (chats/libmoq/1189 R1).
 *
 * warningsAsErrors and abortOnError stay ON. Nothing else is demoted.
 */
val informationalLintIssues = setOf("AndroidGradlePluginVersion")

android {
    lint {
        warningsAsErrors = true
        abortOnError = true
        informational += informationalLintIssues
    }
}

// The exception is only narrow if something enforces its narrowness. This
// asserts the demoted set EXACTLY, so a second issue cannot be added here, and
// it also refuses the other escape hatches: disable, ignore, and a baseline.
check(android.lint.informational == setOf("AndroidGradlePluginVersion")) {
    "lint informational set is ${android.lint.informational}, expected exactly " +
        "[AndroidGradlePluginVersion] -- no other issue may be demoted. This " +
        "literal is deliberately independent of the value used to configure the " +
        "set, so widening the configuration alone fails here"
}
check(android.lint.disable.isEmpty() && android.lint.ignoreWarnings == false) {
    "lint disable/ignoreWarnings must stay empty; the only accepted exception is " +
        "$informationalLintIssues, as informational"
}
check(android.lint.baseline == null) {
    "a lint baseline hides existing findings and is not accepted"
}

// Configured for any future Java source. The product surface is Kotlin today,
// so this remains configured but unexercised until a Java source set appears.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

dependencies {
    // Coroutines is part of this library's PUBLIC API, not an implementation
    // detail: `MoqEndpoint.snapshots`, `MediaReceiver.inventory` and
    // `MediaSender.status` are `StateFlow`s, and most operations are `suspend`.
    // Gradle's rule for that is `api` -- a dependency whose types appear in the
    // public API belongs on the consumer's COMPILE classpath, not only its
    // runtime one.
    //
    // It was `implementation` until a first-party sample tried to consume the
    // AAR and could not compile: `Cannot access class 'StateFlow'. Check your
    // module classpath'. Every third-party consumer would have hit the same
    // wall and been left to rediscover a dependency our own API forces on them
    // (chats/libmoq/1501 F1). `api` also satisfies the runtime requirement that
    // motivated the original declaration -- it puts the artifact on both
    // classpaths -- so nothing is lost by the change.
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:${pin("coroutines.version")}")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:${pin("kotlin.version")}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${pin("coroutines.version")}")

    // Instrumentation only. `androidTestImplementation` is the configuration
    // that never reaches the published AAR: these artifacts exist to run the
    // acceptance tests on a device, and a consumer must not inherit them.
    // androidx.test:runner carries AndroidJUnitRunner (declared above);
    // androidx.test.ext:junit carries AndroidJUnit4 and InstrumentationRegistry.
    androidTestImplementation("androidx.test:runner:${pin("androidx.test.runner.version")}")
    androidTestImplementation("androidx.test.ext:junit:${pin("androidx.test.junit.version")}")
}

/*
 * The dependency SHAPE, asserted at configuration time.
 *
 * `api` is what a consumer of the AAR both COMPILES and runs against, so the
 * contract is stated in every direction that matters: the coroutines artifact
 * must be a declared API dependency, and it must NOT be hidden behind
 * `implementation` (compile-invisible to consumers, which is the defect 1501 F1
 * names) or behind the test configuration (absent from the published AAR).
 *
 * This is a DECLARATION check only. What is actually selected is a separate
 * fact -- a future dependency can drag in another stdlib while every
 * declaration here stays correct -- so resolution is gated independently by
 * `verifyResolvedDependencies` below.
 */
val coroutinesModule = "org.jetbrains.kotlinx:kotlinx-coroutines-core"
val coroutinesTestModule = "org.jetbrains.kotlinx:kotlinx-coroutines-test"
val kotlinStdlibModule = "org.jetbrains.kotlin:kotlin-stdlib"

fun declaredIn(configuration: String, module: String): Boolean =
    configurations.findByName(configuration)?.dependencies?.any {
        "${it.group}:${it.name}" == module
    } ?: false

gradle.projectsEvaluated {
    check(declaredIn("api", coroutinesModule)) {
        "$coroutinesModule must be declared on `api`: its types appear in this library's " +
            "public API (StateFlow, suspend), so an AAR consumer must be able to COMPILE " +
            "against it, not merely resolve it at runtime"
    }
    check(!declaredIn("implementation", coroutinesModule)) {
        "$coroutinesModule is declared on `implementation`; that keeps a dependency our own " +
            "public API exposes off every consumer's compile classpath, so a consumer cannot " +
            "name the types we return"
    }
    check(!declaredIn("testImplementation", coroutinesModule)) {
        "$coroutinesModule is declared on `testImplementation`; a required runtime " +
            "dependency must not be hidden behind the test configuration"
    }
    check(declaredIn("testImplementation", coroutinesTestModule)) {
        "$coroutinesTestModule must be a test-only artifact"
    }
}

/*
 * The RESOLVED dependency gate.
 *
 * Declarations say what was asked for; this asserts what the build actually
 * selects, which is the only thing a consumer ever sees. The pinned Kotlin is
 * authoritative only if nothing upgrades the stdlib underneath it -- exactly
 * the trap that ruled out coroutines 1.11.0, whose own stdlib 2.2.20 would
 * have overridden `kotlin.version`.
 */
fun selectedVersion(configuration: String, module: String): String? =
    configurations
        .getByName(configuration)
        .incoming
        .resolutionResult
        .allComponents
        .mapNotNull { component ->
            val id = component.moduleVersion ?: return@mapNotNull null
            if ("${id.group}:${id.name}" == module) id.version else null
        }
        .singleOrNull()

tasks.register("verifyResolvedDependencies") {
    group = "verification"
    description =
        "Assert the RESOLVED compile and runtime graphs select the pinned coroutine and Kotlin versions."
    doLast {
        val coroutines = pin("coroutines.version")
        val kotlin = pin("kotlin.version")
        val runtime = "debugRuntimeClasspath"
        val compile = "debugCompileClasspath"
        val testRuntime = "debugUnitTestRuntimeClasspath"

        val selectedCoroutines = selectedVersion(runtime, "$coroutinesModule-jvm")
        check(selectedCoroutines == coroutines) {
            "resolved runtime selects kotlinx-coroutines-core-jvm $selectedCoroutines, " +
                "but coroutines.version pins $coroutines"
        }

        // The COMPILE classpath, asserted separately: an `api` dependency has to
        // be resolvable to compile against, and a regression to `implementation`
        // would leave the runtime row above passing while every consumer lost
        // the ability to name the types this library returns.
        val compileCoroutines = selectedVersion(compile, "$coroutinesModule-jvm")
        check(compileCoroutines == coroutines) {
            "resolved COMPILE classpath selects kotlinx-coroutines-core-jvm $compileCoroutines, " +
                "but coroutines.version pins $coroutines; a public API dependency must be on `api`"
        }
        check(selectedVersion(compile, coroutinesTestModule) == null) {
            "$coroutinesTestModule leaked onto the product compile classpath"
        }

        val selectedStdlib = selectedVersion(runtime, kotlinStdlibModule)
        check(selectedStdlib == kotlin) {
            "resolved runtime selects $kotlinStdlibModule $selectedStdlib, but kotlin.version " +
                "pins $kotlin; a dependency is deciding the Kotlin runtime instead of the pin"
        }

        check(selectedVersion(runtime, coroutinesTestModule) == null) {
            "$coroutinesTestModule leaked onto the product runtime classpath"
        }
        val selectedTest = selectedVersion(testRuntime, coroutinesTestModule)
        check(selectedTest == coroutines) {
            "test runtime selects $coroutinesTestModule $selectedTest, expected $coroutines"
        }
    }
}

/*
 * The host shared objects the bridge handshake rows load.
 *
 * They are built from the SHIPPED `jni/moq5_jni.c` (with only the LibMoQ anchor
 * table omitted, since the host has no Android cross-build) and from the
 * fixture that expands the same `MOQ5_ABI_METHODS` authority -- so the boundary
 * the tests exercise is the real one, not a Kotlin stand-in agreeing with a
 * separate parser.
 */
/*
 * The publication.
 *
 * ONE artifact, from AGP's `release` component, so the POM and the Gradle
 * module metadata are generated from the resolved variant rather than
 * hand-written. `api(...)` on coroutines (below) is what puts the artifact on
 * a consumer's COMPILE classpath in both files.
 *
 * The repository is a LOCAL directory, never `~/.m2`: `-PmoqPublicationRepo`
 * points the external-consumer probe at its own ephemeral repository, and the
 * default keeps any accidental publish inside this module's build output.
 */
val publicationRepo: String =
    providers.gradleProperty("moqPublicationRepo").orNull
        ?: layout.buildDirectory.dir("local-maven").get().asFile.path

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])
            }
        }
        repositories {
            maven {
                name = "Probe"
                url = uri(publicationRepo)
            }
        }
    }
}

/*
 * Publishing is bound to the native audit.
 *
 * `check` depended on `verifyJniTopology`, but a publish task did not, so an
 * AAR whose payload no longer corresponded to the audited sources could still
 * be emitted -- the artifact was judged only by the files that happened to be
 * in the tree. Every task that can emit `org.openmoq:moq5-android` therefore
 * runs the audit first, whether it publishes to a repository or to the local
 * Maven cache.
 */
tasks.withType<AbstractPublishToMaven>().configureEach {
    dependsOn("verifyJniTopology")
}

tasks.register<Exec>("buildBridgeTestDsos") {
    group = "verification"
    description = "Build the host shared objects for the JNI bridge handshake tests."
    commandLine(
        rootProject.layout.projectDirectory.file("tools/build-bridge-testdsos.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The Kotlin mirror of `jni/moq5_abi.h` must not drift from it. This is a
 * declaration check only: on its own a parser could agree with a Kotlin fake
 * while the real descriptors disagreed, which is why the handshake rows load
 * actual shared objects as well.
 */
/*
 * The bytecode this gate judges is the DECLARED OUTPUT of the compile tasks it
 * depends on -- both variants -- passed explicitly.
 *
 * It previously let the script discover a class under the module's whole build
 * directory and take the first match. Match order is a filesystem property, not
 * a contract (this tree yields debug first, another yields release first), and
 * that directory also holds outputs of tasks this gate does not depend on, so a
 * freshly rebuilt producer could sit beside the stale class actually judged
 * (chats/libmoq/1214 F1). Resolving the artefact from the producer removes both
 * halves of that hazard, and judging BOTH variants means the release bytecode
 * that R8 will actually process is covered too.
 */
val bridgeClassRelativePath = "org/openmoq/moq5/jni/NativeBridge.class"

fun bridgeClassOf(compileTaskName: String): Provider<File> =
    tasks.named(compileTaskName).map { t ->
        val matches =
            t.outputs.files.files
                .map { it.resolve(bridgeClassRelativePath) }
                .filter { it.isFile }
        check(matches.size == 1) {
            "$compileTaskName declares ${matches.size} candidate(s) for " +
                "$bridgeClassRelativePath; exactly one is required so the gate " +
                "cannot judge an artefact its producer did not just write"
        }
        matches.single()
    }

tasks.register<Exec>("verifyBridgeAbiAuthority") {
    group = "verification"
    // It judges the JVM side on COMPILED bytecode from BOTH variants, so both
    // producers are dependencies and both supply their artefact explicitly.
    dependsOn("compileDebugKotlin", "compileReleaseKotlin", "buildBridgeTestDsos")
    description = "Assert the Kotlin bridge constants match jni/moq5_abi.h."
    // Each producer's artefact is passed under its OWN name, so the script has
    // an exact contract to require rather than a list whose length it can only
    // count -- and so a path containing whitespace cannot be split.
    val debugClass = bridgeClassOf("compileDebugKotlin")
    val releaseClass = bridgeClassOf("compileReleaseKotlin")
    doFirst {
        environment("MOQ5_BRIDGE_CLASS_DEBUG", debugClass.get().absolutePath)
        environment("MOQ5_BRIDGE_CLASS_RELEASE", releaseClass.get().absolutePath)
    }
    commandLine(
        rootProject.layout.projectDirectory.file("tools/check-bridge-abi.sh").asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * Per-class process isolation is confined to the JNI rows.
 *
 * `RegisterNatives` binds per JVM and a later load replaces an earlier binding,
 * so each DSO variant needs its own process. Applying that to EVERY unit test
 * would fork a JVM per class for the whole suite and scale badly as the
 * receiver, sender and session suites arrive -- so the JNI package runs in its
 * own task with `forkEvery = 1`, and the ordinary unit tests keep their single
 * process.
 */
private val jniTestPattern = "org.openmoq.moq5.jni.*"


/*
 * The JNI diagnostic gate, and why it is a FINALIZER of the tests it judges.
 *
 * `-Xcheck:jni` reports a contract violation and lets the VM continue, so the
 * warning lands in a fork's captured output while every assertion passes and the
 * task goes green. Observing that by eye once is not enforcement
 * (chats/libmoq/1224 F2).
 *
 * A finalizer runs even when the task it follows FAILS or its fork dies, which
 * is exactly when a diagnostic is most likely to exist. It reads the results of
 * that run, so it is ordered after them by construction rather than by luck.
 */
val verifyJniDiagnostics =
    tasks.register<Exec>("verifyJniDiagnostics") {
        group = "verification"
        description = "Assert no -Xcheck:jni diagnostic appears in any JNI fork's output."
        // Every identity is passed explicitly: this invocation's token and log,
        // the exact result boundary, and the token the producer stamped into it.
        // The script has no fallback path -- a Gradle run started outside the
        // canonical launcher supplies no token and fails closed.
        argumentProviders.add(
            CommandLineArgumentProvider {
                listOf(
                    layout.buildDirectory.dir("test-results/testBridgeJni").get()
                        .asFile.absolutePath,
                    layout.buildDirectory.file("test-results/testBridgeJni.moq5-token")
                        .get().asFile.absolutePath,
                    providers.environmentVariable("MOQ5_RUN_TOKEN").orElse("").get(),
                    providers.environmentVariable("MOQ5_RUN_LOG").orElse("").get(),
                )
            },
        )
        commandLine(
            "/bin/sh",
            rootProject.layout.projectDirectory.file("tools/check-jni-diagnostics.sh")
                .asFile.absolutePath,
        )
    }

val testBridgeJni =
    tasks.register<Test>("testBridgeJni") {
        group = "verification"
        description = "Run the JNI bridge rows, one JVM per class."
        useJUnit()
        forkEvery = 1
        filter {
            includeTestsMatching(jniTestPattern)
            isFailOnNoMatchingTests = true
        }
        dependsOn("buildBridgeTestDsos")
        // The native objects are a real INPUT of these rows: they are what the
        // forks load. Without declaring them, changing a bridge DSO leaves this
        // task UP-TO-DATE and the suite reports green against a stale object --
        // which silently voided a mutant run of mine.
        inputs.dir(rootProject.layout.projectDirectory.dir(".native/_bridge"))
            .withPropertyName("bridgeTestDsos")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        // The packaged-name product entry -- System.loadLibrary("moq5_jni") --
        // is exercised on a real JVM by pointing the loader at the directory
        // holding the host build of the shipped substrate. This is the loader's
        // own by-name resolution, not a test hook in production code: on device
        // the same call resolves out of the application's library directory.
        systemProperty(
            "java.library.path",
            rootProject.layout.projectDirectory.dir(".native/_bridge").asFile.absolutePath,
        )
        // -Xcheck:jni turns JNI contract violations into loud diagnostics rather
        // than into behaviour that happens to work on this VM. The bridge's
        // failure paths call JNI while a lookup may have left an exception
        // pending, and that sequence is exactly what a permissive HotSpot run
        // can hide from a gate that only asks whether the test passed
        // (chats/libmoq/1222 F1). Every row runs under it, not just the
        // adversarial ones.
        jvmArgs("-Xcheck:jni")

        // These rows always execute.
        //
        // The diagnostic gate judges THIS invocation: a -Xcheck:jni warning is
        // observable only in the output of forks that actually ran. An
        // UP-TO-DATE producer contributes no fork output, so the only honest
        // verdicts available would be "inherited results" (a failure) or a clean
        // report resting on no evidence. Re-running the 15 rows costs a few
        // seconds and makes the evidence real every time.
        outputs.upToDateWhen { false }

        // This invocation's run token is stamped into a boundary file beside the
        // results, in doFirst, so the finalizer can tell THIS run's results from
        // an inherited set. Without it, stale XML plus a fresh log read as a
        // clean run (chats/libmoq/1226 F1).
        val tokenFile = layout.buildDirectory.file("test-results/testBridgeJni.moq5-token")
        val runToken = providers.environmentVariable("MOQ5_RUN_TOKEN").orElse("")
        doFirst {
            val f = tokenFile.get().asFile
            f.parentFile.mkdirs()
            f.writeText(runToken.get())
        }
        // Both gates FINALIZE this task: they must run after it whether it
        // passed, failed, or left a dead fork behind.
        finalizedBy(verifyJniDiagnostics)
        finalizedBy(":verifyNoCrashResidue")
    }

// AGP creates the unit-test task during variant configuration, so the split is
// wired once that has happened.
afterEvaluate {
    val unit = tasks.named<Test>("testDebugUnitTest")
    unit.configure {
        finalizedBy(":verifyNoCrashResidue")
        filter {
            excludeTestsMatching(jniTestPattern)
            isFailOnNoMatchingTests = false
        }
    }
    testBridgeJni.configure {
        testClassesDirs = unit.get().testClassesDirs
        classpath = unit.get().classpath
        // Only the artefacts it actually needs; copying the AGP task's whole
        // dependency set risks a cycle back through .
        dependsOn("compileDebugUnitTestKotlin", "compileDebugKotlin")
    }
}

/*
 * The module's own canonical verification boundary, enumerated independently of
 * how AGP happens to wire `check`. The root aggregate depends on THIS task, so
 * an invocation of root `check` never relies on Gradle's unqualified
 * task-name expansion to pick up module lint by accident.
 */
/*
 * The JNI endpoint bootstrap gate, and why it is registered rather than run by
 * hand.
 *
 * It judges native images, so it must depend on the producer that writes the
 * host image and must fail closed when the Android topology outputs it also
 * judges are absent or stale -- each image is bound to its producer's recorded
 * digest inside the script. An unregistered script that a person remembers to
 * run is not a forcing function; this task is reachable from `check` and is
 * listed in `expectedModuleChecks`, so dropping it fails the boundary gate.
 */
tasks.register<Exec>("verifyJniEndpointPreflightSelfTest") {
    group = "verification"
    description = "Assert the JNI endpoint preflight cannot pass on an unreadable image."
    commandLine(
        "/bin/sh",
        rootProject.layout.projectDirectory.file("tools/selftest-jni-endpoint-preflight.sh")
            .asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

/*
 * The product topology audit, registered so the endpoint gate can DEPEND on it.
 *
 * Matching hashes between a diagnostic image and its own receipt prove the pair
 * is self-consistent, not that either corresponds to the current sources. The
 * topology audit is the check that recomputes the LibMoQ input digest, so the
 * endpoint boundary is only current-source provenance when it runs behind it.
 */
tasks.register<Exec>("verifyJniTopology") {
    group = "verification"
    description = "Audit the one-DSO Android topology against its recorded provenance."
    commandLine(
        "/bin/sh",
        rootProject.layout.projectDirectory.file("tools/check-jni-topology.sh")
            .asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

tasks.register<Exec>("verifyJniEndpointPreflight") {
    group = "verification"
    description = "Assert no endpoint ABI row outruns the host image's endpoint substrate."
    // The host image's producer. The Android topology is produced outside
    // Gradle, so the script binds those images to their own receipts instead of
    // to a task dependency, and names a failure when either is missing.
    dependsOn("buildBridgeTestDsos")
    // ... and behind the audit that binds the Android images to current source.
    dependsOn("verifyJniTopology")
    // The gate is only meaningful against the images it judges; declaring them
    // as inputs stops a stale UP-TO-DATE run reporting green over a new build.
    inputs.dir(rootProject.layout.projectDirectory.dir(".native/_bridge"))
    inputs.file(rootProject.layout.projectDirectory.file("jni/moq5_abi.h"))
    commandLine(
        "/bin/sh",
        rootProject.layout.projectDirectory.file("tools/check-jni-endpoint-preflight.sh")
            .asFile.absolutePath,
    )
    isIgnoreExitValue = false
}

val expectedModuleChecks =
    setOf(
        "testDebugUnitTest",
        "lintDebug",
        "verifyResolvedDependencies",
        "buildBridgeTestDsos",
        "verifyBridgeAbiAuthority",
        "verifyModuleCheckBoundary",
        "testBridgeJni",
        "verifyJniDiagnostics",
        "verifyJniTopology",
        "verifyJniEndpointPreflight",
        "verifyJniEndpointPreflightSelfTest",
    )

tasks.named("check") {
    dependsOn("verifyResolvedDependencies")
    dependsOn("verifyBridgeAbiAuthority")
    dependsOn("verifyModuleCheckBoundary")
    dependsOn("testBridgeJni")
    dependsOn("verifyJniDiagnostics")
    dependsOn("verifyJniTopology")
    dependsOn("verifyJniEndpointPreflight")
    dependsOn("verifyJniEndpointPreflightSelfTest")
}

/*
 * The module's canonical verification boundary, asserted by a task that does
 * NOT depend on the suite it is describing.
 *
 * Putting this in `check`'s own action made it unrunnable exactly when it
 * mattered: while any dependency fails -- an intentional RED, say -- `check`
 * never executes and the boundary goes unchecked. As an independent task it
 * still runs under `--continue` and reports the wiring either way.
 */
tasks.register("verifyModuleCheckBoundary") {
    group = "verification"
    description = "Assert `check` reaches this module's host tests, lint and gates."
    doLast {
        val check = tasks.named("check").get()
        val reachable = mutableSetOf<String>()
        val seen = mutableSetOf<Task>()
        fun walk(t: Task) {
            if (!seen.add(t)) return
            t.taskDependencies.getDependencies(t).forEach {
                reachable += it.name
                walk(it)
            }
        }
        walk(check)
        val missing = expectedModuleChecks.filterNot { it in reachable }
        check(missing.isEmpty()) {
            "`:moq5-android:check` does not reach $missing; the module's canonical " +
                "verification boundary must own host tests, lint and the dependency gate"
        }

    }
}
