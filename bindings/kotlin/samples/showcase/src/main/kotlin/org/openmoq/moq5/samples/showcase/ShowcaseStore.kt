package org.openmoq.moq5.samples.showcase

import android.util.AtomicFile
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URLDecoder
import java.net.URLEncoder

/** What the app keeps between launches: its settings, the last broadcast typed, and recent ones. */
data class Saved(
    val settings: ShowcaseSettings = ShowcaseSettings(),
    val url: String? = null,
    val namespace: String? = null,
    val recents: List<WatchTarget> = emptyList(),
)

/**
 * [saved] as text: `key=value` lines, one `recent=<url> <namespace>` line per
 * recent broadcast. Every value is %-encoded, so any character it holds --
 * a line break included -- comes back exactly and can never start a record.
 */
fun encodeSaved(saved: Saved): String =
    buildList {
        add("draft=" + saved.settings.draft.name)
        add("start=" + saved.settings.start.name)
        add("stats=" + saved.settings.showStats)
        saved.url?.let { add("url=" + encoded(it)) }
        saved.namespace?.let { add("namespace=" + encoded(it)) }
        saved.recents.forEach { add("recent=" + encoded(it.url) + " " + encoded(it.namespace)) }
    }.joinToString("\n", postfix = "\n")

/** The text back; unknown keys and malformed values fall back to the defaults. */
fun decodeSaved(text: String): Saved {
    var saved = Saved()
    val recents = mutableListOf<WatchTarget>()
    for (line in text.split('\n')) {
        val key = line.substringBefore('=', "")
        val value = line.substringAfter('=', "")
        saved =
            when (key) {
                "draft" -> DraftChoice.entries.firstOrNull { it.name == value }?.let { saved.copy(settings = saved.settings.copy(draft = it)) } ?: saved
                "start" -> StartChoice.entries.firstOrNull { it.name == value }?.let { saved.copy(settings = saved.settings.copy(start = it)) } ?: saved
                "stats" -> saved.copy(settings = saved.settings.copy(showStats = value == "true"))
                "url" -> decoded(value)?.let { saved.copy(url = it) } ?: saved
                "namespace" -> decoded(value)?.let { saved.copy(namespace = it) } ?: saved
                "recent" -> {
                    val parts = value.split(' ').map(::decoded)
                    val url = parts.getOrNull(0)
                    val namespace = parts.getOrNull(1)
                    if (parts.size == 2 && url != null && namespace != null && targetProblem(url, namespace) == null) recents += WatchTarget(url, namespace)
                    saved
                }
                else -> saved
            }
    }
    return saved.copy(recents = recents.distinct().take(RECENTS_KEPT))
}

private fun encoded(value: String): String = URLEncoder.encode(value, "UTF-8")

private fun decoded(value: String): String? =
    try {
        URLDecoder.decode(value, "UTF-8")
    } catch (malformed: IllegalArgumentException) {
        null
    }

/** The saved-state file, as the store uses it. [read] throws FileNotFoundException when there is none. */
interface SavedFile {
    fun read(): ByteArray

    /** Open a write that replaces the file only when committed. */
    fun begin(): SavedWrite
}

/** One write transaction of a [SavedFile]. */
interface SavedWrite {
    fun write(bytes: ByteArray)

    fun commit()

    fun abort()
}

/** [SavedFile] over the platform's AtomicFile. */
class AtomicSavedFile(file: File) : SavedFile {
    private val atomic = AtomicFile(file)

    override fun read(): ByteArray = atomic.readFully()

    override fun begin(): SavedWrite {
        val out = atomic.startWrite()
        return object : SavedWrite {
            override fun write(bytes: ByteArray) = out.write(bytes)

            override fun commit() = atomic.finishWrite(out)

            override fun abort() = atomic.failWrite(out)
        }
    }
}

/**
 * [Saved] in one small file, replaced atomically. Reading happens once, at
 * start; [save] is called off the main thread, one write at a time. A save
 * that fails is reported through [warn] and leaves the previous file.
 */
class ShowcaseStore(private val file: SavedFile, private val warn: (String, Exception) -> Unit) {
    fun load(): Saved =
        try {
            decodeSaved(String(file.read(), Charsets.UTF_8))
        } catch (absent: FileNotFoundException) {
            Saved()
        } catch (unreadable: IOException) {
            warn("the saved state could not be read; starting from defaults", unreadable)
            Saved()
        }

    fun save(saved: Saved) {
        val write =
            try {
                file.begin()
            } catch (unopened: IOException) {
                warn("the saved state could not be written", unopened)
                return
            }
        try {
            write.write(encodeSaved(saved).toByteArray(Charsets.UTF_8))
            write.commit()
        } catch (failed: IOException) {
            write.abort()
            warn("the saved state could not be written", failed)
        }
    }
}
