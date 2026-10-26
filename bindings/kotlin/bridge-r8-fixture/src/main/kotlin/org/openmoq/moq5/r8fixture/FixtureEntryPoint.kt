package org.openmoq.moq5.r8fixture

/**
 * The fixture application's own code, kept so the minified APK always contains a
 * DEX file.
 *
 * Without it, stripping the library's keep rules leaves R8 with nothing to emit
 * at all, and "the bound class is gone" becomes indistinguishable from "the
 * archive is empty" -- a weaker observation than the one this fixture exists to
 * make. With it, the bound class's absence is a fact about that class.
 *
 * It deliberately does NOT reference the bridge: the bridge class is `internal`
 * to the library, and the point is that the packaged consumer rules retain it
 * without any consumer reference at all.
 */
object FixtureEntryPoint {
    @JvmStatic
    fun describe(): String = "moq5 r8 fixture"
}
