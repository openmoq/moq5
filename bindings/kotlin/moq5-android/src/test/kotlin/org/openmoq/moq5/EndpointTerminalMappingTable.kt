package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The endpoint mapping contract, tested against the PRODUCTION mapper.
 *
 * Every row is fed to [EndpointSnapshotMapper] as a native record and compared
 * with the value it must produce. The rules come from
 * `service/include/moq/endpoint.h`:
 *
 *  * the five classified reasons stay five DISTINCT cases;
 *  * `CLOSED` + `NONE` is `Unclassified` -- the service cannot classify the
 *    cause. It is ordinary, not exotic: a local stop before any terminal
 *    condition reports it, and raw picoquic passes `false` for clean-close
 *    (`pq_vt_terminal`), so `CLEAN` is structurally unreachable there and a
 *    cleanly closed picoquic endpoint reports it too;
 *  * a NON-closed snapshot with `NONE` has no terminal at all;
 *  * `Protocol` carries the MoQ wire domain; the TLS and transport cases carry
 *    an [EndpointErrorDetail], which is a backend-native diagnostic and not a
 *    MoQ code;
 *  * `detail_code` keeps all 64 bits. wtquic-Network stores a NEGATIVE
 *    `OSStatus` two's-complement, so `errSecNotTrusted` (-67843) crosses as
 *    0xFFFFFFFFFFFEF6FD with BIT 63 SET. A JNI `jlong` / Kotlin `Long`
 *    preserves all 64 raw bits and `.toULong()` round-trips them, so a `Long`
 *    carrier does NOT corrupt them -- what is wrong is treating that signed
 *    value as the unsigned numeric domain, and narrowing to `Int` is what
 *    actually loses bits;
 *  * the negotiated version is retained after close, and `RECONNECTING` is
 *    representable although v0 never enters it.
 */
internal class EndpointTerminalMappingTable {

    /**
     * The native discriminants, exactly as `moq_endpoint_terminal_reason_t` and
     * `moq_endpoint_state_t` number them. These are SERVICE ABI values -- what
     * the C tier hands the binding -- not MoQ wire fields, and not a second
     * copy of the Kotlin model.
     */
    internal enum class NativeReason(val code: Int) {
        NONE(0), CLEAN(1), PROTOCOL(2), TLS_CERTIFICATE(3), TLS(4), TRANSPORT(5)
    }

    internal enum class NativeState(val code: Int) {
        CONNECTING(0), ESTABLISHED(1), RECONNECTING(2), DRAINING(3), CLOSED(4)
    }

    internal data class Row(
        val state: NativeState,
        val reason: NativeReason,
        val detail: ULong,
        /** The production value the mapper must produce; null = no terminal. */
        val expected: EndpointTerminal?,
    )

    /** Drive the PRODUCTION mapper with one native record. */
    private fun mapRow(r: Row, version: UInt = 0u): EndpointSnapshot =
        EndpointSnapshotMapper.map(r.state.code, version, r.reason.code, r.detail)

    private val table =
        listOf(
            // The five classified reasons, each distinct, each on a CLOSED
            // endpoint, each carrying its native detail verbatim.
            Row(NativeState.CLOSED, NativeReason.CLEAN, 0uL, EndpointTerminal.Clean),
            Row(NativeState.CLOSED, NativeReason.PROTOCOL, 0x5uL,
                EndpointTerminal.Protocol(MoqErrorCode(0x5uL))),
            Row(
                NativeState.CLOSED,
                NativeReason.TLS_CERTIFICATE,
                BIT63_DETAIL,
                EndpointTerminal.TlsCertificate(EndpointErrorDetail(BIT63_DETAIL)),
            ),
            Row(NativeState.CLOSED, NativeReason.TLS, 0x2AuL,
                EndpointTerminal.Tls(EndpointErrorDetail(0x2AuL))),
            Row(NativeState.CLOSED, NativeReason.TRANSPORT, 0uL,
                EndpointTerminal.Transport(EndpointErrorDetail(0uL))),
            // CLOSED + NONE: the service cannot classify the cause.
            Row(NativeState.CLOSED, NativeReason.NONE, 0uL, EndpointTerminal.Unclassified),
            // Not closed + NONE: no terminal at all.
            Row(NativeState.CONNECTING, NativeReason.NONE, 0uL, null),
            Row(NativeState.ESTABLISHED, NativeReason.NONE, 0uL, null),
            Row(NativeState.RECONNECTING, NativeReason.NONE, 0uL, null),
            Row(NativeState.DRAINING, NativeReason.NONE, 0uL, null),
        )

    @Test
    fun `the production mapper produces exactly the frozen value for every row`() {
        for (r in table) {
            assertEquals(
                r.expected,
                mapRow(r).terminal,
                "row ${r.state}/${r.reason} must map to ${r.expected}",
            )
        }
    }

    @Test
    fun `the table covers every reason, every state and both NONE readings`() {
        assertEquals(
            NativeReason.entries.toSet(),
            table.map { it.reason }.toSet(),
            "a native terminal reason is missing from the frozen table",
        )
        assertEquals(
            NativeState.entries.toSet(),
            table.map { it.state }.toSet(),
            "a native endpoint state is missing from the frozen table",
        )
        // The five classified reasons map to five DISTINCT cases, compared by
        // CLASS so two cases differing only in detail code are not counted as
        // distinct. A collapse of any pair would shrink this set.
        val classified =
            table.filter { it.reason != NativeReason.NONE }
                .map { mapRow(it).terminal!!::class }
                .toSet()
        assertEquals(5, classified.size, "two classified reasons collapsed onto one case")
        assertTrue(
            EndpointTerminal.Unclassified::class !in classified,
            "a classified reason must never map to Unclassified",
        )
        // NONE reads two ways, decided by the state in the SAME record.
        val noneRows = table.filter { it.reason == NativeReason.NONE }
        assertEquals(
            setOf<EndpointTerminal?>(EndpointTerminal.Unclassified),
            noneRows.filter { it.state == NativeState.CLOSED }
                .map { mapRow(it).terminal }.toSet(),
            "CLOSED + NONE must be Unclassified, never Clean or Transport",
        )
        assertEquals(
            setOf<EndpointTerminal?>(null),
            noneRows.filter { it.state != NativeState.CLOSED }
                .map { mapRow(it).terminal }.toSet(),
            "a non-closed NONE must have no terminal at all",
        )
    }

    @Test
    fun `every native state maps to its own production state`() {
        val expected =
            mapOf(
                NativeState.CONNECTING to EndpointState.Connecting,
                NativeState.ESTABLISHED to EndpointState.Established,
                NativeState.RECONNECTING to EndpointState.Reconnecting,
                NativeState.DRAINING to EndpointState.Draining,
                NativeState.CLOSED to EndpointState.Closed,
            )
        assertEquals(5, expected.values.toSet().size, "two native states collapsed")
        for ((native, want) in expected) {
            assertEquals(
                want,
                EndpointSnapshotMapper.map(native.code, 0u, NativeReason.NONE.code, 0uL).state,
                "native state ${native.code} must map to $want",
            )
        }
    }

    /**
     * The transport-flavoured cases must NOT carry the MoQ wire-error domain.
     * This fails to compile if any of them is changed back to [MoqErrorCode],
     * and fails at runtime if the mapper builds the wrong domain.
     */
    @Test
    fun `transport diagnostics use their own value domain, not MoqErrorCode`() {
        val tlsCert =
            EndpointSnapshotMapper.map(
                NativeState.CLOSED.code, 0u, NativeReason.TLS_CERTIFICATE.code, 7uL,
            ).terminal as EndpointTerminal.TlsCertificate
        val tls =
            EndpointSnapshotMapper.map(
                NativeState.CLOSED.code, 0u, NativeReason.TLS.code, 7uL,
            ).terminal as EndpointTerminal.Tls
        val transport =
            EndpointSnapshotMapper.map(
                NativeState.CLOSED.code, 0u, NativeReason.TRANSPORT.code, 7uL,
            ).terminal as EndpointTerminal.Transport
        // Statically typed as EndpointErrorDetail: assigning them to that type
        // is the compile-time half of the discriminator.
        val details: List<EndpointErrorDetail> =
            listOf(tlsCert.detail, tls.detail, transport.detail)
        assertEquals(listOf(7uL, 7uL, 7uL), details.map { it.rawBits })

        // Protocol -- and only Protocol -- carries the MoQ wire domain.
        val protocol =
            EndpointSnapshotMapper.map(
                NativeState.CLOSED.code, 0u, NativeReason.PROTOCOL.code, 7uL,
            ).terminal as EndpointTerminal.Protocol
        val wire: MoqErrorCode = protocol.detailCode
        assertEquals(7uL, wire.value)
    }

    @Test
    fun `an unknown native discriminator fails closed`() {
        assertFailsWith<UnknownNativeValueException>("an unknown state must not fall back") {
            EndpointSnapshotMapper.map(99, 0u, NativeReason.NONE.code, 0uL)
        }
        assertFailsWith<UnknownNativeValueException>("an unknown reason must not fall back") {
            EndpointSnapshotMapper.map(NativeState.CLOSED.code, 0u, 99, 0uL)
        }
    }

    @Test
    fun `the high-bit detail code survives the mapper as an unsigned 64-bit value`() {
        // A Long carries these bits unchanged; the error is reading the
        // signed value as the unsigned numeric domain. Int narrowing loses them.
        assertEquals(0xFFFFFFFFFFFEF6FDuL, BIT63_DETAIL)
        assertTrue((BIT63_DETAIL shr 63) == 1uL, "bit 63 must be set for this row")
        assertEquals(-67843L, BIT63_DETAIL.toLong(), "reinterpreting gives the OSStatus back")

        // errSecNotTrusted is NOT a general witness for 64-bit preservation:
        // its high word happens to be the sign extension of its low word, so
        // toInt().toULong() reconstructs it exactly. A second vector outside
        // that coincidence band is what actually pins the full width.
        assertTrue((WIDE_DETAIL shr 63) == 1uL, "the second vector must set bit 63")
        assertTrue(
            WIDE_DETAIL != WIDE_DETAIL.toInt().toULong(),
            "the second vector must lie outside the sign-extension band",
        )
        for (v in listOf(BIT63_DETAIL, WIDE_DETAIL)) {
            assertEquals(
                EndpointTerminal.TlsCertificate(EndpointErrorDetail(v)),
                EndpointSnapshotMapper.map(
                    NativeState.CLOSED.code, 0u,
                    NativeReason.TLS_CERTIFICATE.code, v,
                ).terminal,
                "detail 0x${v.toString(16)} must cross the mapper unchanged",
            )
        }
        assertTrue(
            table.any { it.detail == BIT63_DETAIL },
            "the table must exercise a bit-63 detail code",
        )
        assertEquals(
            EndpointTerminal.TlsCertificate(EndpointErrorDetail(BIT63_DETAIL)),
            EndpointSnapshotMapper.map(
                NativeState.CLOSED.code, 0u,
                NativeReason.TLS_CERTIFICATE.code, BIT63_DETAIL,
            ).terminal,
            "the production mapper must carry all 64 raw bits",
        )
    }

    @Test
    fun `every nonzero version crosses the mapper unchanged and zero is absent`() {
        for (v in listOf(DRAFT_16, DRAFT_18, FUTURE_VERSION, UInt.MAX_VALUE)) {
            assertEquals(
                MoqVersion(v),
                EndpointSnapshotMapper.map(
                    NativeState.ESTABLISHED.code, v, NativeReason.NONE.code, 0uL,
                ).negotiatedVersion,
                "version $v must cross the mapper unchanged",
            )
        }
        assertEquals(
            null,
            EndpointSnapshotMapper.map(
                NativeState.CONNECTING.code, 0u, NativeReason.NONE.code, 0uL,
            ).negotiatedVersion,
            "native zero means ABSENT",
        )
    }

    /** The version half of the snapshot, frozen as its own explicit table. */
    internal data class VersionRow(
        val state: NativeState,
        val native: UInt,
        val expected: UInt?,   // null = ABSENT
    )

    private val versionTable =
        listOf(
            // Native zero means ABSENT, in every state that can carry it.
            VersionRow(NativeState.CONNECTING, 0u, null),
            VersionRow(NativeState.RECONNECTING, 0u, null),
            // Valid and ordinary: a locally stopped or pre-establishment-failed
            // endpoint closes without ever negotiating. ESTABLISHED/0 and
            // DRAINING/0 are NOT added -- they cannot occur.
            VersionRow(NativeState.CLOSED, 0u, null),
            // Retained across draining and close, for both known drafts.
            VersionRow(NativeState.ESTABLISHED, 16u, 16u),
            VersionRow(NativeState.DRAINING, 16u, 16u),
            VersionRow(NativeState.CLOSED, 16u, 16u),
            VersionRow(NativeState.ESTABLISHED, 18u, 18u),
            VersionRow(NativeState.DRAINING, 18u, 18u),
            VersionRow(NativeState.CLOSED, 18u, 18u),
            // An unknown FUTURE numeric version is preserved exactly: not
            // rejected, not collapsed onto a known draft, not narrowed.
            VersionRow(NativeState.ESTABLISHED, 22u, 22u),
            VersionRow(NativeState.DRAINING, 22u, 22u),
            VersionRow(NativeState.CLOSED, 22u, 22u),
        )

    /**
     * The rows the table MUST contain, defined once and required by the
     * validator. Without this a rule like "some zero row exists" is satisfied
     * by any one of them, so deleting `CLOSED/0` or `CLOSED/22` left the
     * canonical table green while the semantics it claims to freeze were gone.
     *
     * It is a MINIMUM: unrelated future rows are not rejected.
     */
    private val requiredRows: List<VersionRow> =
        listOf(
            // Absent, in every state that can carry a never-negotiated version.
            VersionRow(NativeState.CONNECTING, 0u, null),
            VersionRow(NativeState.RECONNECTING, 0u, null),
            VersionRow(NativeState.CLOSED, 0u, null),
        ) +
            // Known drafts and an unknown future version, each unchanged in
            // every state that can carry a negotiated version.
            listOf(DRAFT_16, DRAFT_18, FUTURE_VERSION).flatMap { v ->
                listOf(NativeState.ESTABLISHED, NativeState.DRAINING, NativeState.CLOSED)
                    .map { st -> VersionRow(st, v, v) }
            }

    private fun describe(r: VersionRow): String =
        "${r.state}/${r.native} -> ${r.expected ?: "ABSENT"}"

    /**
     * The ONE version-mapping validator. The canonical table and every
     * perturbed copy are judged by this same function, so a perturbation test
     * cannot pass by merely confirming that it perturbed something -- and
     * deleting a rule here breaks the canonical test too.
     *
     * Returns the exact named violations, empty when the table is sound.
     */
    private fun validateVersions(rows: List<VersionRow>): List<String> {
        val problems = mutableListOf<String>()

        // Every required row is present, named individually so a deletion is
        // attributable to the exact row that went missing.
        for (r in requiredRows) {
            if (r !in rows) problems += "missing row: ${describe(r)}"
        }
        // No row appears twice: a duplicate makes the table's own meaning
        // ambiguous and can mask a deletion.
        for ((row, n) in rows.groupingBy { it }.eachCount()) {
            if (n > 1) problems += "duplicate row: ${describe(row)}"
        }

        // Absent: native zero never becomes a version value.
        val zero = rows.filter { it.native == 0u }
        if (zero.isEmpty()) problems += "missing: native zero is not covered"
        if (zero.any { it.expected != null }) {
            problems += "collapse: native zero maps to a version value instead of ABSENT"
        }

        // Retained: both known drafts survive DRAINING and CLOSED.
        for (v in listOf(DRAFT_16, DRAFT_18)) {
            for (st in listOf(NativeState.DRAINING, NativeState.CLOSED)) {
                if (rows.none { it.state == st && it.native == v && it.expected == v }) {
                    problems += "missing: version $v is not retained across $st"
                }
            }
        }

        // Future: preserved exactly, never collapsed onto a known draft.
        val future = rows.filter { it.native == FUTURE_VERSION }
        if (future.isEmpty()) problems += "missing: no unknown future version is covered"
        if (future.any { it.expected != FUTURE_VERSION }) {
            problems += "collapse: an unknown future version is not preserved exactly"
        }

        // Every nonzero native value crosses unchanged.
        if (rows.any { it.native != 0u && it.expected != it.native }) {
            problems += "collapse: a nonzero version does not cross unchanged"
        }
        return problems
    }

    @Test
    fun `the canonical version table satisfies every mapping rule`() {
        assertEquals(
            emptyList(),
            validateVersions(versionTable),
            "the canonical version table must satisfy its own rules",
        )
    }

    /**
     * Each perturbation is judged by the SAME validator, and must produce its
     * own exact diagnostic while every unrelated rule stays satisfied.
     */
    /**
     * Deleting ANY required row must be caught, by that row's own diagnostic.
     * Parameterised over the same required inventory the validator uses, so a
     * row added there is automatically required here too.
     */
    @Test
    fun `deleting any required row is caught by the shared validator`() {
        for (r in requiredRows) {
            val found = validateVersions(versionTable.filter { it != r }).toSet()
            assertTrue(
                "missing row: ${describe(r)}" in found,
                "deleting ${describe(r)} must be reported by name, found $found",
            )
        }
    }

    @Test
    fun `a duplicated row is caught by the shared validator`() {
        val dup = requiredRows.first { it.state == NativeState.CLOSED && it.native == 0u }
        assertEquals(
            setOf("duplicate row: ${describe(dup)}"),
            validateVersions(versionTable + dup).toSet(),
            "a duplicated row must be reported exactly",
        )
    }

    @Test
    fun `an unrelated additional row is not rejected`() {
        val extra = VersionRow(NativeState.ESTABLISHED, 23u, 23u)
        assertEquals(
            emptySet(),
            validateVersions(versionTable + extra).toSet(),
            "the required inventory is a MINIMUM, not a whitelist",
        )
    }

    /**
     * Each collapse perturbation is judged by the SAME validator, and must
     * produce exactly its own violation set.
     */
    @Test
    fun `each collapse perturbation is caught by the shared validator`() {
        data class Case(val label: String, val rows: List<VersionRow>, val expected: Set<String>)

        val zeroRows = requiredRows.filter { it.native == 0u }
        val futureRows = requiredRows.filter { it.native == FUTURE_VERSION }

        val cases =
            listOf(
                Case(
                    "absent collapsed onto a version",
                    versionTable.map { if (it.native == 0u) it.copy(expected = DRAFT_16) else it },
                    zeroRows.map { "missing row: ${describe(it)}" }.toSet() +
                        "collapse: native zero maps to a version value instead of ABSENT",
                ),
                Case(
                    "future collapsed onto a known draft",
                    versionTable.map {
                        if (it.native == FUTURE_VERSION) it.copy(expected = DRAFT_18) else it
                    },
                    futureRows.map { "missing row: ${describe(it)}" }.toSet() +
                        setOf(
                            "collapse: an unknown future version is not preserved exactly",
                            // Collapsing 22 onto 18 also breaks cross-unchanged:
                            // one edit, two real violations.
                            "collapse: a nonzero version does not cross unchanged",
                        ),
                ),
            )

        for (c in cases) {
            assertEquals(
                c.expected,
                validateVersions(c.rows).toSet(),
                "${c.label}: the shared validator must report exactly this violation set",
            )
        }
    }

    private companion object {
        /** errSecNotTrusted (-67843) as raw bits; bit 63 set. */
        const val BIT63_DETAIL: ULong = 0xFFFFFFFFFFFEF6FDuL

        /**
         * A second bit-63 vector OUTSIDE the sign-extension band, so no 32-bit
         * round trip can reconstruct it.
         */
        const val WIDE_DETAIL: ULong = 0x8000000100000002uL
        const val DRAFT_16: UInt = 16u
        const val DRAFT_18: UInt = 18u

        /** An unknown numeric version: neither draft, and not to be collapsed. */
        const val FUTURE_VERSION: UInt = 22u
    }
}
