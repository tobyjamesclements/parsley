package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyEpoch}: the lower-bounds seam the delivery gate and frontier consult.
 */
class ParsleyEpochTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    /**
     * The {@link ParsleyEpoch#NONE} epoch treats every coordinate as unbounded
     * ({@link ParsleyEpoch#NO_BOUND}), so the strip step is a no-op when epoch bounding is disabled
     * (epoch 0).
     */
    @Test
    void noneReportsEveryCoordinateUnbounded() {
        assertEquals(ParsleyEpoch.NO_BOUND, ParsleyEpoch.NONE.startsAt(T1_ID, 0),
                "the NONE epoch must report every coordinate as unbounded");
    }
}
