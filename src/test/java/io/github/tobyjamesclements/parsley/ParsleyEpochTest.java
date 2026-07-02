package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ParsleyEpoch}: the {@link ParsleyEpoch.View} seam the delivery gate consults.
 */
class ParsleyEpochTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    /**
     * The {@link ParsleyEpoch.View#NONE} view treats every coordinate as unbounded
     * ({@link ParsleyEpoch#NO_BOUND}), so the strip step is a no-op when epoch bounding is disabled
     * (epoch 0).
     */
    @Test
    void noneViewReportsEveryCoordinateUnbounded() {
        assertEquals(ParsleyEpoch.NO_BOUND, ParsleyEpoch.View.NONE.startsAt(T1_ID, 0),
                "the NONE view must report every coordinate as unbounded");
    }
}
