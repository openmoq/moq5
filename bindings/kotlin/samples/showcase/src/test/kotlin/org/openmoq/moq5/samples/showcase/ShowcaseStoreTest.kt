package org.openmoq.moq5.samples.showcase

import java.io.FileNotFoundException
import java.io.IOException
import kotlin.test.Test
import kotlin.test.assertEquals

class ShowcaseStoreTest {
    private class File(var openFails: Boolean = false, var writeFails: Boolean = false) : SavedFile {
        var content: ByteArray? = null
        val events = mutableListOf<String>()

        override fun read(): ByteArray = content ?: throw FileNotFoundException("absent")

        override fun begin(): SavedWrite {
            if (openFails) throw IOException("open refused")
            events += "begin"
            var pending = ByteArray(0)
            return object : SavedWrite {
                override fun write(bytes: ByteArray) {
                    if (writeFails) throw IOException("disk full")
                    pending = bytes
                }

                override fun commit() {
                    content = pending
                    events += "commit"
                }

                override fun abort() {
                    events += "abort"
                }
            }
        }
    }

    private val saved = Saved(ShowcaseSettings(draft = DraftChoice.Draft16), "https://relay.example.com", "live/a", emptyList())

    @Test fun `a healthy save is read back`() {
        val file = File()
        val store = ShowcaseStore(file) { _, _ -> }
        assertEquals(Saved(), store.load(), "no file yet: defaults")
        store.save(saved)
        assertEquals(listOf("begin", "commit"), file.events)
        assertEquals(saved, store.load())
    }

    @Test fun `a save that cannot open its file is reported and the app carries on saving`() {
        val file = File(openFails = true)
        val warnings = mutableListOf<String>()
        val store = ShowcaseStore(file) { message, _ -> warnings += message }
        store.save(saved)
        assertEquals(listOf("the saved state could not be written"), warnings)
        assertEquals(emptyList(), file.events, "nothing was acquired, so nothing is rolled back")
        file.openFails = false
        store.save(saved)
        assertEquals(saved, store.load())
    }

    @Test fun `a save that fails while writing rolls back only its own transaction and keeps the previous file`() {
        val file = File()
        val store = ShowcaseStore(file) { _, _ -> }
        store.save(saved)
        file.writeFails = true
        store.save(saved.copy(namespace = "live/b"))
        assertEquals(listOf("begin", "commit", "begin", "abort"), file.events)
        assertEquals(saved, store.load())
    }
}
