package org.openmoq.moq5.samples.player

/*
 * The launch can preset the three inputs, as `am start --es <extra> <value>` does, so a supervised
 * run never has to type into the fields. The launch intent is the Activity's own: a recreated
 * Activity reads the same extras and starts from the same values.
 */

internal const val DEFAULT_URL = "moqt://10.0.2.2:4443"
internal const val DEFAULT_NAMESPACE = "live/cam1"
internal const val DEFAULT_BUDGET = "120"

internal const val RELAY_URL_EXTRA = "org.openmoq.moq5.samples.player.RELAY_URL"
internal const val NAMESPACE_EXTRA = "org.openmoq.moq5.samples.player.NAMESPACE"
internal const val OBJECT_BUDGET_EXTRA = "org.openmoq.moq5.samples.player.OBJECT_BUDGET"

/** The text each field starts with. */
internal data class PlayerInputs(val url: String, val namespace: String, val budget: String)

/**
 * Each launch string extra as given, or the default when it is absent. Nothing is judged here: the
 * fields show exactly what the launch said, and the session judges the inputs at Start, as it does
 * for typed ones.
 */
internal fun playerInputs(extra: (String) -> String?): PlayerInputs =
    PlayerInputs(
        url = extra(RELAY_URL_EXTRA) ?: DEFAULT_URL,
        namespace = extra(NAMESPACE_EXTRA) ?: DEFAULT_NAMESPACE,
        budget = extra(OBJECT_BUDGET_EXTRA) ?: DEFAULT_BUDGET,
    )

/** Harness-only launch switch: the diagnostic breadcrumbs ([PlaybackTrace]), independent of the observer. */
internal const val DIAGNOSTIC_TRACE_EXTRA = "org.openmoq.moq5.samples.player.DIAGNOSTIC_TRACE"

/** The two harness-only launch switches: the acceptance observer and the diagnostic trace. */
internal data class PlayerFlags(val observe: Boolean, val trace: Boolean)

/** Each boolean launch extra as given, or false when it is absent; neither implies the other. */
internal fun playerFlags(extra: (String, Boolean) -> Boolean): PlayerFlags =
    PlayerFlags(observe = extra(OBSERVE_PRESENTATION_EXTRA, false), trace = extra(DIAGNOSTIC_TRACE_EXTRA, false))
