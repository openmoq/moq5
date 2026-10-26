package org.openmoq.moq5.samples

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The first-party Android sample applications, as a topology contract.
 *
 * The assessment names exactly two, `bindings/kotlin/samples/player` and
 * `bindings/kotlin/samples/publisher`, and calls them "an API acceptance
 * test, not a marketing shell". These rows pin the structural half of that:
 * each is its own registered Gradle module that consumes `:moq5-android`
 * the way an application developer would, and the library module carries no
 * sample code of its own.
 *
 * What these rows deliberately do NOT claim: that the samples run. The
 * library's scripted backend is `internal` to `:moq5-android`, so a
 * consumer module cannot drive a fake endpoint, and the real one needs a
 * relay or a device. The behavioural claim available to a consumer module
 * is that its code COMPILES against the public surface alone, which the
 * Kotlin module boundary enforces for us and the sample gate audits at the
 * source level.
 */
internal class SampleTopologyTest {
    private companion object {
        /** `bindings/kotlin`, from the module directory the test runs in. */
        val root = File("..").canonicalFile
        val settings = File(root, "settings.gradle.kts")
        val samples = listOf("player", "publisher")

        fun moduleDir(name: String) = File(root, "samples/$name")

        fun buildScript(name: String) = File(moduleDir(name), "build.gradle.kts")
    }

    @Test
    fun `both sample modules exist under bindings-kotlin samples`() {
        val missing = samples.filterNot { moduleDir(it).isDirectory && buildScript(it).isFile }
        assertTrue(
            missing.isEmpty(),
            "the assessment names a player and a publisher sample; missing module(s): $missing " +
                "(each is a directory under bindings/kotlin/samples with its own build.gradle.kts)",
        )
    }

    @Test
    fun `settings registers both samples, so Gradle builds and gates them`() {
        val text = settings.readText()
        for (s in samples) {
            assertTrue(
                Regex("""^\s*include\("[:]samples:$s"\)""", RegexOption.MULTILINE).containsMatchIn(text),
                "settings.gradle.kts does not include \":samples:$s\"; an unregistered directory is never compiled, " +
                    "never linted and never reached by the canonical gate",
            )
        }
    }

    @Test
    fun `each sample is an application that consumes the library through an ordinary project dependency`() {
        for (s in samples) {
            val b = buildScript(s)
            if (!b.isFile) {
                assertTrue(false, "no build script for the $s sample")
                continue
            }
            val text = b.readText()
            assertTrue(
                """id("com.android.application")""" in text,
                "the $s sample must be an application module: a library module could not be installed or run",
            )
            assertTrue(
                Regex("""implementation\(project\("[:]moq5-android"\)\)""").containsMatchIn(text),
                "the $s sample must depend on :moq5-android through implementation(project(...)); " +
                    "consuming the real published surface is the whole point of an acceptance app",
            )
        }
    }

    // ---- the platform-API boundary ----------------------------------------------------------

    private val CAMERA2 = "android.hardware.camera2"
    private val MEDIA = "android.media"

    /** Root-relative source directories, as the shell gate names them. */
    private val PUBLISHER_SRC = "samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher"
    private val PLAYER_SRC = "samples/player/src/main/kotlin/org/openmoq/moq5/samples/player"

    /** The files that alone may name a file-scoped class: full root-relative paths, never a basename. */
    private val CAMERA_DRIVER = "$PUBLISHER_SRC/Camera2Capture.kt"
    private val ENCODER_DRIVER = "$PUBLISHER_SRC/MediaCodecEncoder.kt"
    private val AUDIO_ADAPTER = "$PUBLISHER_SRC/AudioRecordCapture.kt"
    private val PLAYER_CODEC_FILES = setOf("$PLAYER_SRC/AndroidPlaybackCodec.kt", "$PLAYER_SRC/AndroidPlaybackRender.kt")

    /** camera2 metadata lookup: anywhere in the publisher. */
    private val CAMERA2_METADATA = setOf("CameraCharacteristics", "CameraMetadata", "CameraManager", "CameraAccessException")

    /** camera2 capture-session classes: the publisher's Camera2 driver only. */
    private val CAMERA2_SESSION = setOf("CameraDevice", "CameraCaptureSession", "CaptureRequest")

    /** `android.media` by role: what each sample's own sources may name. */
    private val PUBLISHER_MEDIA = setOf("MediaCodec", "MediaFormat", "MediaCodecInfo", "AudioRecord", "AudioFormat", "AudioTimestamp", "MediaRecorder")
    private val PLAYER_MEDIA = setOf("MediaCodec", "MediaFormat", "AudioTrack", "AudioFormat", "AudioAttributes", "MediaSync", "PlaybackParams")

    /** A role's file-scoped `android.media` classes, and the only files that may name each. */
    private val MEDIA_FILE_SCOPES: Map<String, Map<String, Set<String>>> =
        mapOf(
            "publisher" to mapOf("MediaCodec" to setOf(ENCODER_DRIVER), "MediaCodecInfo" to setOf(ENCODER_DRIVER), "AudioRecord" to setOf(AUDIO_ADAPTER)),
            "player" to mapOf("MediaCodec" to PLAYER_CODEC_FILES),
        )

    /** Why a name is refused. */
    private data class Violation(val name: String, val reason: String)

    private fun roleOf(path: String): String =
        samples.firstOrNull { path.startsWith("samples/$it/") } ?: error("not a sample source path: $path")

    /**
     * The executable text of Kotlin source: line comments, nesting block
     * comments, raw strings, string and character literals removed, and the
     * spacing the grammar permits around a dot removed. A name in a comment or
     * a literal is not a use; a name split by spacing is the same name.
     *
     * It is a lexer for exactly those forms, not a Kotlin or Java parser: an
     * expression inside a string template is removed with its string. Block
     * comments follow the source's language, read from [path]: Kotlin's nest,
     * Java's end at the first close.
     */
    private fun executable(path: String, text: String): String {
        val nests = !path.endsWith(".java")
        val out = StringBuilder()
        var i = 0
        var depth = 0
        var raw = false
        while (i < text.length) {
            val c = text[i]
            if (raw) {
                if (text.startsWith("\"\"\"", i)) { raw = false; i += 3 } else i++
                continue
            }
            if (depth > 0) {
                when {
                    text.startsWith("/*", i) -> { if (nests) depth++; i += 2 }
                    text.startsWith("*/", i) -> { depth--; i += 2 }
                    else -> i++
                }
                continue
            }
            when {
                text.startsWith("\"\"\"", i) -> { raw = true; i += 3 }
                text.startsWith("/*", i) -> { depth++; i += 2 }
                text.startsWith("//", i) -> { while (i < text.length && text[i] != '\n') i++ }
                c == '"' || c == '\'' -> {
                    i++
                    while (i < text.length && text[i] != '\n') {
                        if (text[i] == '\\') { i += 2; continue }
                        if (text[i] == c) { i++; break }
                        i++
                    }
                }
                else -> { out.append(c); i++ }
            }
        }
        return out.toString().replace(Regex("[ \\t]*\\.[ \\t]*"), ".")
    }

    private fun wildcard(code: String, pkg: String): Boolean =
        Regex("^[ \\t]*import[ \\t]+${Regex.escape(pkg)}\\.\\*[ \\t]*;?[ \\t]*$", RegexOption.MULTILINE).containsMatchIn(code)

    /** The top-level classes of [pkg] that [code] names, by import or by qualified reference. */
    private fun named(code: String, pkg: String): Set<String> =
        Regex("(?<![A-Za-z0-9_.])${Regex.escape(pkg)}\\.([A-Za-z_][A-Za-z0-9_]*)").findAll(code).map { it.groupValues[1] }.toSet()

    private fun camera2Violations(path: String, text: String): List<Violation> {
        val code = executable(path, text)
        if (wildcard(code, CAMERA2)) return listOf(Violation("$CAMERA2.*", "wildcard"))
        return named(code, CAMERA2).sorted().mapNotNull { cls ->
            when {
                roleOf(path) == "player" -> Violation(cls, "role")
                cls in CAMERA2_METADATA -> null
                cls in CAMERA2_SESSION -> if (path == CAMERA_DRIVER) null else Violation(cls, "file")
                else -> Violation(cls, "unlisted")
            }
        }
    }

    private fun mediaViolations(path: String, text: String): List<Violation> {
        val code = executable(path, text)
        if (wildcard(code, MEDIA)) return listOf(Violation("$MEDIA.*", "wildcard"))
        val role = roleOf(path)
        val own = if (role == "publisher") PUBLISHER_MEDIA else PLAYER_MEDIA
        val other = if (role == "publisher") PLAYER_MEDIA else PUBLISHER_MEDIA
        return named(code, MEDIA).sorted().mapNotNull { cls ->
            val scope = MEDIA_FILE_SCOPES[role]?.get(cls)
            when {
                cls in own && (scope == null || path in scope) -> null
                cls in own -> Violation(cls, "file")
                cls in other -> Violation(cls, "role")
                else -> Violation(cls, "unlisted")
            }
        }
    }

    /** Libraries a sample may not take without a proposal. */
    private val LIBRARIES = listOf("androidx.compose", "androidx.lifecycle", "androidx.camera", "androidx.media3", "okhttp", "okhttp3", "retrofit")

    /** A library is judged like a platform API: by what the executable text names, never by a comment or a literal. */
    private fun libraryViolations(path: String, text: String): List<Violation> {
        roleOf(path) // a sample source path, or this fails
        val code = executable(path, text)
        return LIBRARIES.filter { it in code }.map { Violation(it, "unproposed") }
    }

    /** Every Kotlin and Java source of every sample, by its root-relative path. */
    private fun sampleSources(): List<Pair<String, String>> =
        samples.flatMap { s ->
            val dir = File(moduleDir(s), "src")
            if (!dir.isDirectory) emptyList() else
                dir.walkTopDown().filter { it.isFile && (it.extension == "kt" || it.extension == "java") }
                    .map { it.relativeTo(root).path.replace(File.separatorChar, '/') to it.readText() }
                    .sortedBy { it.first }
                    .toList()
        }

    @Test
    fun `no sample declares or imports an unproposed library, and every source stays inside its platform-API allowance`() {
        // Compose, AndroidX lifecycle, camera and Media3, and network libraries
        // are absent: each would have to be proposed before it appears. The
        // build script is how one is DECLARED, an import is how one arrives.
        for (s in samples) {
            val b = buildScript(s)
            if (b.isFile) {
                // A build script is judged as text: any mention there is a declaration.
                val text = b.readText()
                val found = (LIBRARIES + listOf("android.media.MediaCodec", "android.media.MediaMuxer")).filter { it in text }
                assertTrue(found.isEmpty(), "the $s sample declares unproposed dependency(ies) $found")
                assertTrue(CAMERA2 !in text, "the $s sample declares a camera2 dependency")
            }
        }
        val sources = sampleSources()
        for (s in samples) {
            assertTrue(sources.any { it.first.startsWith("samples/$s/") }, "the $s sample contributes no Kotlin or Java source; an empty inventory proves nothing")
        }
        val refused = mutableMapOf<String, List<Violation>>()
        for ((path, text) in sources) {
            val found = libraryViolations(path, text) + camera2Violations(path, text) + mediaViolations(path, text)
            if (found.isNotEmpty()) refused[path] = found
        }
        assertEquals(emptyMap(), refused, "every sample source stays inside its library and platform-API allowance")
    }

    // Synthetic sources: a root-relative path and its text, judged as one file.
    private val other = "$PUBLISHER_SRC/LiveLaunch.kt"
    private val playerOther = "$PLAYER_SRC/PlaybackSession.kt"

    @Test
    fun `camera2 metadata is the publisher's anywhere, and its session classes only in the exact Camera2 driver`() {
        for (cls in CAMERA2_METADATA) {
            assertEquals(emptyList(), camera2Violations(other, "import $CAMERA2.$cls"), "metadata $cls, any publisher file")
        }
        for (cls in listOf("CameraDevice", "CameraCaptureSession", "CaptureRequest")) {
            assertEquals(emptyList(), camera2Violations(CAMERA_DRIVER, "import $CAMERA2.$cls"), "$cls in the driver")
            assertEquals(listOf(Violation(cls, "file")), camera2Violations(other, "import $CAMERA2.$cls"), "$cls outside the driver")
            // The same basename elsewhere is another file: paths match whole, never by suffix.
            for (lookalike in listOf("samples/publisher/src/main/kotlin/elsewhere/Camera2Capture.kt", "samples/publisher/src/test/kotlin/org/openmoq/moq5/samples/publisher/Camera2Capture.kt")) {
                assertEquals(listOf(Violation(cls, "file")), camera2Violations(lookalike, "import $CAMERA2.$cls"), "$cls in $lookalike")
            }
        }
        for (cls in listOf("CaptureResult", "TotalCaptureResult", "CameraExtensionSession")) {
            assertEquals(listOf(Violation(cls, "unlisted")), camera2Violations(CAMERA_DRIVER, "import $CAMERA2.$cls"), "$cls is no allowance, even in the driver")
        }
        // A qualified reference counts like an import.
        assertEquals(listOf(Violation("CameraDevice", "file")), camera2Violations(other, "val d: $CAMERA2.CameraDevice? = null"))
    }

    @Test
    fun `the player has no camera2 role`() {
        for (cls in CAMERA2_METADATA + listOf("CameraDevice", "CaptureRequest")) {
            assertEquals(listOf(Violation(cls, "role")), camera2Violations(playerOther, "import $CAMERA2.$cls"), "the player may not name $cls")
        }
        assertEquals(listOf(Violation("CameraManager", "role")), camera2Violations(PLAYER_CODEC_FILES.first(), "import $CAMERA2.CameraManager"))
    }

    @Test
    fun `android media is role-specific, and the shared names are each role's own`() {
        for (cls in listOf("MediaFormat", "AudioFormat", "AudioTimestamp", "MediaRecorder")) {
            assertEquals(emptyList(), mediaViolations(other, "import $MEDIA.$cls"), "publisher $cls")
        }
        for (cls in listOf("MediaFormat", "AudioFormat", "AudioTrack", "AudioAttributes", "MediaSync", "PlaybackParams")) {
            assertEquals(emptyList(), mediaViolations(playerOther, "import $MEDIA.$cls"), "player $cls")
        }
        // A class of the other role only is refused by role.
        for (cls in listOf("AudioRecord", "AudioTimestamp", "MediaCodecInfo", "MediaRecorder")) {
            assertEquals(listOf(Violation(cls, "role")), mediaViolations(playerOther, "import $MEDIA.$cls"), "the player captures nothing, so not $cls")
        }
        for (cls in listOf("AudioTrack", "AudioAttributes", "MediaSync", "PlaybackParams")) {
            assertEquals(listOf(Violation(cls, "role")), mediaViolations(other, "import $MEDIA.$cls"), "the publisher plays nothing, so not $cls")
        }
        // No role's: refused by name, wherever it appears.
        for (path in listOf(other, ENCODER_DRIVER, playerOther)) {
            assertEquals(listOf(Violation("MediaMuxer", "unlisted")), mediaViolations(path, "import $MEDIA.MediaMuxer"), "MediaMuxer in $path")
        }
        assertEquals(listOf(Violation("MediaMuxer", "unlisted")), mediaViolations(other, "val m: $MEDIA.MediaMuxer? = null"), "a qualified reference counts")
    }

    @Test
    fun `the file-scoped media classes are named only in their exact files`() {
        assertEquals(emptyList(), mediaViolations(ENCODER_DRIVER, "import $MEDIA.MediaCodec\nimport $MEDIA.MediaCodecInfo"))
        assertEquals(emptyList(), mediaViolations(AUDIO_ADAPTER, "import $MEDIA.AudioRecord"))
        for (path in PLAYER_CODEC_FILES) {
            assertEquals(emptyList(), mediaViolations(path, "import $MEDIA.MediaCodec"), "the player codec file $path")
        }
        assertEquals(listOf(Violation("MediaCodec", "file")), mediaViolations(other, "import $MEDIA.MediaCodec"))
        assertEquals(listOf(Violation("MediaCodecInfo", "file")), mediaViolations(other, "import $MEDIA.MediaCodecInfo"))
        assertEquals(listOf(Violation("AudioRecord", "file")), mediaViolations(ENCODER_DRIVER, "import $MEDIA.AudioRecord"))
        assertEquals(listOf(Violation("MediaCodec", "file")), mediaViolations(playerOther, "import $MEDIA.MediaCodec"))
        assertEquals(listOf(Violation("MediaCodec", "file")), mediaViolations("samples/player/src/main/kotlin/elsewhere/AndroidPlaybackCodec.kt", "import $MEDIA.MediaCodec"), "a basename is not the file")
        assertEquals(listOf(Violation("MediaCodec", "file")), mediaViolations(AUDIO_ADAPTER.replace("AudioRecordCapture", "MediaCodecEncoderX"), "import $MEDIA.MediaCodec"))
    }

    @Test
    fun `a library name in a comment or a string is not a use, and a real import or qualified use is refused by name`() {
        for (text in listOf("// androidx.compose is deliberately absent", "/* okhttp3 */", "val doc = \"retrofit\"", "val raw = \"\"\"androidx.media3\"\"\"")) {
            assertEquals(emptyList(), libraryViolations(other, text), "not a use: $text")
        }
        assertEquals(listOf(Violation("androidx.compose", "unproposed")), libraryViolations(playerOther, "import androidx.compose.runtime.Composable"))
        assertEquals(listOf(Violation("okhttp", "unproposed"), Violation("okhttp3", "unproposed")), libraryViolations(other, "val c: okhttp3.OkHttpClient? = null"))
        assertEquals(listOf(Violation("androidx.camera", "unproposed")), libraryViolations(other, "import androidx . camera . core . Preview"), "spacing around a dot is the same name")
    }

    @Test
    fun `the platform-API allowances are exactly the shell gate's, and cannot be emptied or widened unnoticed`() {
        // Independent literals: the same sets as tools/check-sample-apps.sh, restated here
        // so a change to either list fails one of the two enforcement points.
        assertEquals(setOf("CameraCharacteristics", "CameraMetadata", "CameraManager", "CameraAccessException"), CAMERA2_METADATA)
        assertEquals(setOf("CameraDevice", "CameraCaptureSession", "CaptureRequest"), CAMERA2_SESSION)
        assertEquals(setOf("AudioFormat", "AudioRecord", "AudioTimestamp", "MediaCodec", "MediaCodecInfo", "MediaFormat", "MediaRecorder"), PUBLISHER_MEDIA)
        assertEquals(setOf("AudioAttributes", "AudioFormat", "AudioTrack", "MediaCodec", "MediaFormat", "MediaSync", "PlaybackParams"), PLAYER_MEDIA)
        assertEquals(
            mapOf(
                "publisher" to mapOf(
                    "MediaCodec" to setOf("samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/MediaCodecEncoder.kt"),
                    "MediaCodecInfo" to setOf("samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/MediaCodecEncoder.kt"),
                    "AudioRecord" to setOf("samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/AudioRecordCapture.kt"),
                ),
                "player" to mapOf(
                    "MediaCodec" to setOf(
                        "samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackCodec.kt",
                        "samples/player/src/main/kotlin/org/openmoq/moq5/samples/player/AndroidPlaybackRender.kt",
                    ),
                ),
            ),
            MEDIA_FILE_SCOPES,
        )
        assertEquals("samples/publisher/src/main/kotlin/org/openmoq/moq5/samples/publisher/Camera2Capture.kt", CAMERA_DRIVER)
        // And every file a scope names exists: a scope over a renamed file would admit nothing and refuse everything.
        val scoped = (MEDIA_FILE_SCOPES.values.flatMap { it.values.flatten() } + CAMERA_DRIVER).toSet()
        assertEquals(emptyList(), scoped.filterNot { File(root, it).isFile }, "every scoped file exists")
    }

    /**
     * The nested members the samples' executable text names, by owner.
     *
     * The unit of every allowance is the TOP-LEVEL class, because that is what
     * an import names: admitting an owner admits every member nested in it, and
     * neither gate bounds one nested member apart from another. This inventory
     * is what the samples actually use, derived from their executable text, so
     * a new nested member is a visible change here rather than an unreviewed
     * consequence of an owner being allowed.
     */
    private val NESTED_MEMBERS =
        mapOf(
            "publisher" to setOf(
                "AudioFormat.Builder", "AudioRecord.Builder", "AudioRecord.OnRecordPositionUpdateListener",
                "CameraCaptureSession.StateCallback", "CameraDevice.StateCallback",
                "MediaCodec.BufferInfo", "MediaCodec.Callback", "MediaCodec.CodecException",
                "MediaCodecInfo.CodecCapabilities", "MediaCodecInfo.CodecProfileLevel", "MediaRecorder.AudioSource",
            ),
            "player" to setOf(
                "AudioAttributes.Builder", "AudioFormat.Builder", "AudioTrack.Builder", "MediaCodec.BufferInfo", "MediaSync.Callback",
            ),
        )

    @Test
    fun `an allowance is by top-level class, and the nested members the samples use all belong to allowed owners`() {
        val owners = CAMERA2_METADATA + CAMERA2_SESSION + PUBLISHER_MEDIA + PLAYER_MEDIA
        val member = Regex("(?<![A-Za-z0-9_.])(${owners.joinToString("|")})\\.([A-Z][a-z][A-Za-z0-9]*)\\b")
        val used = samples.associateWith { mutableSetOf<String>() }
        for ((path, text) in sampleSources()) {
            member.findAll(executable(path, text)).forEach { used.getValue(roleOf(path)).add(it.groupValues[1] + "." + it.groupValues[2]) }
        }
        assertEquals(NESTED_MEMBERS, used.mapValues { it.value.toSet() }, "the nested members in use, as reviewed")
        for ((role, members) in NESTED_MEMBERS) {
            val allowed = if (role == "publisher") CAMERA2_METADATA + CAMERA2_SESSION + PUBLISHER_MEDIA else PLAYER_MEDIA
            assertEquals(emptyList(), members.filterNot { it.substringBefore('.') in allowed }, "every $role nested member's owner is one of its allowed classes")
        }
    }

    @Test
    fun `a wildcard import directive is refused in either package`() {
        for ((pkg, judge) in listOf(CAMERA2 to ::camera2Violations, MEDIA to ::mediaViolations)) {
            for (path in listOf(other, CAMERA_DRIVER, ENCODER_DRIVER, playerOther)) {
                assertEquals(listOf(Violation("$pkg.*", "wildcard")), judge(path, "import $pkg.*"), "import $pkg.* in $path")
            }
        }
    }

    @Test
    fun `a wildcard import written with the whitespace the grammar permits is the same violation`() {
        for ((pkg, judge) in listOf(CAMERA2 to ::camera2Violations, MEDIA to ::mediaViolations)) {
            for (form in listOf("\timport   ${pkg.replace(".", " . ")} . *  ;", "import ${pkg.replace(".", ". ")}.*", "  import $pkg .*;")) {
                assertEquals(listOf(Violation("$pkg.*", "wildcard")), judge(other, form), "'$form'")
            }
        }
    }

    @Test
    fun `a wildcard import still counts when a comment follows it on the same line`() {
        for ((pkg, judge) in listOf(CAMERA2 to ::camera2Violations, MEDIA to ::mediaViolations)) {
            for (form in listOf("import $pkg.* // trailing", "import $pkg.*; /* block */", "import $pkg.* /* nested /* comment */ still */")) {
                assertEquals(listOf(Violation("$pkg.*", "wildcard")), judge(playerOther, form), "'$form'")
            }
        }
    }

    @Test
    fun `a Java block comment ends at its first close, so an import after it is live`() {
        val javaPlayer = "samples/player/src/main/java/org/openmoq/moq5/samples/player/TopologyJavaProbe.java"
        val text = "/* explanatory /* text */\nimport android.media.AudioRecord;"
        assertEquals(listOf(Violation("AudioRecord", "role")), mediaViolations(javaPlayer, text), "Java closes the comment at the first */")
        assertEquals(listOf(Violation("CameraDevice", "file")), camera2Violations(javaPlayer.replace("player", "publisher"), "/* a /* b */ import $CAMERA2.CameraDevice;"))
        // The same text in Kotlin is still inside the nested comment, and imports nothing.
        assertEquals(emptyList(), mediaViolations("$PLAYER_SRC/TopologyProbe.kt", "/* explanatory /* text */\nimport android.media.AudioRecord; */"))
        // A Java import accepted for its role stays accepted.
        assertEquals(emptyList(), mediaViolations(javaPlayer, "import android.media.AudioTrack;"))
        assertEquals(emptyList(), camera2Violations(javaPlayer.replace("player", "publisher"), "import $CAMERA2.CameraCharacteristics;"))
    }

    @Test
    fun `a name in a comment or a string is not a use, and a wildcard cannot smuggle an unlisted class`() {
        for (text in listOf(
            "// import $MEDIA.MediaMuxer would be refused",
            "/* import $CAMERA2.CameraDevice /* nested */ still a comment */",
            "val doc = \"import $MEDIA.MediaMuxer\"",
            "val raw = \"\"\"import $CAMERA2.*\"\"\"",
            "val c = 'x' // $MEDIA.AudioTrack",
        )) {
            assertEquals(emptyList(), camera2Violations(other, text), "camera2: $text")
            assertEquals(emptyList(), mediaViolations(other, text), "media: $text")
        }
        val smuggled = "import $CAMERA2.*\nval session: CameraExtensionSession? = null"
        assertEquals(listOf(Violation("$CAMERA2.*", "wildcard")), camera2Violations(CAMERA_DRIVER, smuggled))
    }

    @Test
    fun `the library module carries no sample code`() {
        val strays =
            File(root, "moq5-android/src").walkTopDown()
                .filter { it.isFile && it.extension == "kt" }
                .filter { f ->
                    val p = f.path.replace(File.separatorChar, '/')
                    "/samples/" in p && !p.endsWith("/samples/SampleTopologyTest.kt")
                }
                .map { it.path }
                .toList()
        assertEquals(emptyList(), strays, "sample application code belongs in its own module, never inside the library")
    }
}
