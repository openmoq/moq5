package org.openmoq.moq5.samples.publisher

/**
 * The screen's four inputs, as entered.
 *
 * Saved explicitly with the Activity's instance state: the views are built in
 * code without IDs, so the platform's own view-state restore does not keep
 * them. A recreated screen shows what was entered, and does nothing else on
 * its account: no request is resumed and nothing starts.
 */
internal data class PublisherInputs(val url: String, val namespace: String, val track: String, val frames: String) {
    /** What a live start uses: the relay URL and the namespace, trimmed. */
    fun live(): LiveInputs = LiveInputs(url.trim(), namespace.trim())

    /** Writes these inputs for `onSaveInstanceState`. */
    fun save(put: (String, String) -> Unit) {
        put(URL, url)
        put(NAMESPACE, namespace)
        put(TRACK, track)
        put(FRAMES, frames)
    }

    companion object {
        val DEFAULT = PublisherInputs(url = "moqt://10.0.2.2:4443", namespace = "live/cam1", track = "video", frames = "300")

        /** The inputs a recreated screen starts with, from what its predecessor saved. */
        /** A field that was not saved is that field's default. */
        fun restored(get: (String) -> String?): PublisherInputs =
            PublisherInputs(
                url = get(URL) ?: DEFAULT.url,
                namespace = get(NAMESPACE) ?: DEFAULT.namespace,
                track = get(TRACK) ?: DEFAULT.track,
                frames = get(FRAMES) ?: DEFAULT.frames,
            )

        private const val URL = "inputs.url"
        private const val NAMESPACE = "inputs.namespace"
        private const val TRACK = "inputs.track"
        private const val FRAMES = "inputs.frames"
    }
}
