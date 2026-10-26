# The FIXTURE's own keep rule, for the fixture's own entry point.
#
# This is deliberately not about the bridge: it exists so the minified APK always
# contains a DEX, which makes "the bound class is absent" a fact about that class
# rather than about an empty archive. The library's own rules reach this build
# through the AAR, never through this file.
-keep class org.openmoq.moq5.r8fixture.FixtureEntryPoint { *; }
