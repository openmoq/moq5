package org.openmoq.moq5.trust

import java.io.ByteArrayInputStream
import java.security.KeyStore
import java.security.cert.X509Certificate

/**
 * Real DER certificates for the conversion rows, generated once by the JDK's
 * own `keytool` into an in-memory keystore.
 *
 * Generated rather than checked in so the rows own what they assert on, and
 * built with the platform's own tools so nothing here is a hand-rolled ASN.1
 * encoder whose bugs could be mistaken for the code under test.
 */
internal object TestCerts {
    /**
     * A self-signed leaf for [dname] with key family [keyAlg] (`"RSA"` or
     * `"EC"`) and, when [san] is given, exactly one subjectAltName in keytool's
     * own `-ext SAN=` grammar (`dns:host` or `ip:literal`).
     *
     * The SAN kind is the caller's and is never inferred from its text, so a
     * dNSName whose text is an IP literal is constructible -- which is what
     * proves an IP reference identity cannot be satisfied by a DNS entry.
     */
    fun selfSigned(dname: String, keyAlg: String = "RSA", san: String? = null): ByteArray {
        val dir = kotlin.io.path.createTempDirectory("moq5-certs").toFile()
        try {
            val store = java.io.File(dir, "ks.p12")
            val keytool =
                java.io.File(java.io.File(System.getProperty("java.home"), "bin"), "keytool")
            check(keytool.canExecute()) { "setup: no keytool at ${keytool.path}" }
            val keySpec =
                when (keyAlg) {
                    "RSA" -> listOf("-keyalg", "RSA", "-keysize", "2048")
                    "EC" -> listOf("-keyalg", "EC", "-groupname", "secp256r1")
                    else -> error("setup: unsupported key family $keyAlg")
                }
            val ext = if (san == null) emptyList() else listOf("-ext", "SAN=$san")
            val proc =
                ProcessBuilder(
                    listOf(keytool.path, "-genkeypair", "-alias", "a") + keySpec + listOf(
                        "-dname", dname, "-validity", "1", "-storetype", "PKCS12",
                        "-keystore", store.path, "-storepass", "changeit",
                    ) + ext,
                ).redirectErrorStream(true).start()
            val out = proc.inputStream.bufferedReader().readText()
            check(proc.waitFor() == 0) { "setup: keytool failed: $out" }
            val ks = KeyStore.getInstance("PKCS12")
            store.inputStream().use { ks.load(it, "changeit".toCharArray()) }
            val cert = ks.getCertificate("a") as X509Certificate
            return cert.encoded
        } finally {
            dir.deleteRecursively()
        }
    }

    /** Round-trips DER through the platform factory; used only by self-checks. */
    fun parse(der: ByteArray): X509Certificate =
        java.security.cert.CertificateFactory.getInstance("X.509")
            .generateCertificate(ByteArrayInputStream(der)) as X509Certificate
}
