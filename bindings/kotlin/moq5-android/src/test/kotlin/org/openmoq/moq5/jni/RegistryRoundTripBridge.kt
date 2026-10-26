package org.openmoq.moq5.jni

/**
 * The class a TEST-ONLY native fixture binds to in order to carry a real
 * registry token across JNI.
 *
 * It lives in TEST source. Kotlin `internal` is not JVM encapsulation -- the
 * class compiles to a public final class with public native methods, and in
 * main source it shipped inside the product classes and the AAR
 * (chats/libmoq/1232 F4). The host unit-test class loader is what the fixture's
 * `FindClass` resolves against, and the packaging gate asserts this class is
 * absent from both the debug and release AARs.
 */
internal object RegistryRoundTripBridge {
    external fun mintToken(): Long

    external fun resolves(token: Long): Boolean

    external fun echo(token: Long): Long

    external fun retire(token: Long): Long
}
