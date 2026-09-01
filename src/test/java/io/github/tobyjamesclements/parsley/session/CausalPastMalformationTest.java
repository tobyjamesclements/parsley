package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.util.List;

import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.CausesMalformationVectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the token parser refuses what the engine's codec refuses: every
 * malformation class of the shared battery ({@link CausesMalformationVectors}) is
 * re-pinned here through {@link CausalPast#decode}, because a salvaging token parser is
 * the same hazard as a salvaging codec (issue #96, D99). A token decoded leniently from
 * damaged bytes is a weaker frontier than the one minted, and a weaker frontier silently
 * weakens the session guarantee — so these stay red if decode is ever rewritten around
 * the codec rather than through it.
 *
 * <p>The vectors have one spelling, in the shared catalogue both decoders sweep; the
 * per-family tests below keep each refusal class its own red, and the final sweep pins
 * this parser to the <em>whole</em> catalogue, so a class added there is re-pinned here
 * with no change to this file.
 */
class CausalPastMalformationTest {

    private static void refusesFamily(String family) {
        List<CausesMalformationVectors.Vector> vectors = CausesMalformationVectors.family(family);
        assertFalse(vectors.isEmpty(), () -> "family \"" + family + "\" must exist in the shared battery");
        for (CausesMalformationVectors.Vector vector : vectors) {
            CausesCodec.UndecodableMetadataException thrown = assertThrows(
                    CausesCodec.UndecodableMetadataException.class, () -> CausalPast.decode(vector.bytes()),
                    () -> vector.family() + ": " + vector.label() + " must be undecodable");
            assertTrue(thrown.getMessage().contains(vector.diagnosisFragment()),
                    () -> vector.family() + ": " + vector.label() + " must diagnose \""
                            + vector.diagnosisFragment() + "\": " + thrown.getMessage());
        }
    }

    /** Rejects a null value: an absent token is no token, not an empty one. */
    @Test
    void rejectsNullValue() {
        refusesFamily("null-value");
    }

    /** Rejects unknown versions, the pre-release snapshot grammar's byte included. */
    @Test
    void rejectsUnknownVersion() {
        refusesFamily("unknown-version");
    }

    /**
     * Rejects the snapshot-era flat-grammar shapes, which lead with the released version
     * byte and refuse by grammar rather than by version (D101) — a token parser serving
     * one as the empty frontier would silently weaken the session guarantee.
     */
    @Test
    void rejectsSnapshotEraFlatEncodings() {
        refusesFamily("snapshot-flat");
    }

    /** Rejects truncation at every depth, and a surplus byte, as classified refusals. */
    @Test
    void rejectsTruncationAndTrailingBytes() {
        refusesFamily("truncation");
        refusesFamily("trailing");
    }

    /** Rejects a negative position: a token cannot demand a position no log can hold. */
    @Test
    void rejectsNegativePosition() {
        refusesFamily("negative-position");
    }

    /** Rejects the reserved zero topic id: a forged token must not plant an unanswerable id. */
    @Test
    void rejectsZeroTopicId() {
        refusesFamily("zero-topic-id");
    }

    /** Rejects topics out of order or duplicated: one spelling per past survives transport. */
    @Test
    void rejectsTopicsOutOfOrderOrDuplicate() {
        refusesFamily("topic-order");
    }

    /** Rejects partitions out of order or duplicated within a group. */
    @Test
    void rejectsPartitionsOutOfOrderOrDuplicate() {
        refusesFamily("partition-order");
    }

    /** Rejects a topic group naming zero partitions. */
    @Test
    void rejectsZeroPartitionTopic() {
        refusesFamily("zero-partitions");
    }

    /** Rejects miscounted topic and partition counts against the bytes present. */
    @Test
    void rejectsCountMiscounts() {
        refusesFamily("count-miscount");
    }

    /** Rejects non-minimal varints: two spellings of one token would defeat comparison by bytes. */
    @Test
    void rejectsNonMinimalVarint() {
        refusesFamily("non-minimal-varint");
    }

    /** Rejects varints past the non-negative int range, the silent-alias spellings included. */
    @Test
    void rejectsVarintBeyondTheNonNegativeIntRange() {
        refusesFamily("varint-overflow");
    }

    /**
     * Refuses the whole catalogue, family by family or not: the per-family tests above
     * give each class its own red, and this sweep is the drift fence — a malformation
     * class added to the shared battery is re-pinned through {@link CausalPast#decode}
     * even before anyone writes it a named test here.
     */
    @Test
    void refusesEveryCataloguedMalformation() {
        for (CausesMalformationVectors.Vector vector : CausesMalformationVectors.all()) {
            assertThrows(CausesCodec.UndecodableMetadataException.class,
                    () -> CausalPast.decode(vector.bytes()),
                    () -> vector.family() + ": " + vector.label() + " must be undecodable through the token parser");
        }
    }
}
