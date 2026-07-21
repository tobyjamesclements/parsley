package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the public {@link CausalDeliveryException} hierarchy — the contract an application's
 * {@code StreamsUncaughtExceptionHandler} decides by. The hierarchy's shape is API: a handler
 * type-matches the concrete types (restartable {@link CausalPendingAckException} vs the
 * never-heal-in-place {@link CausalTopicRecreatedException}), catches the whole family through the
 * root, and reads the source coordinate off {@link CausalCoordinateException}.
 */
class CausalDeliveryExceptionTest {

    private static final Uuid T1_ID = Uuid.randomUuid();

    /** Every concrete Parsley exception is catchable through the single public root. */
    @Test
    void everyConcreteExceptionExtendsTheRoot() {
        assertTrue(CausalDeliveryException.class.isAssignableFrom(CausalBufferDeserializationException.class),
                "a poisoned-buffer failure must be catchable as CausalDeliveryException");
        assertTrue(CausalDeliveryException.class.isAssignableFrom(CausalVectorClockResolutionException.class),
                "an unresolvable-clock failure must be catchable as CausalDeliveryException");
        assertTrue(CausalDeliveryException.class.isAssignableFrom(CausalTopicRecreatedException.class),
                "a topic-recreation failure must be catchable as CausalDeliveryException");
        assertTrue(CausalDeliveryException.class.isAssignableFrom(CausalPendingAckException.class),
                "a pending-ack failure must be catchable as CausalDeliveryException");
    }

    /**
     * The concrete types are public (so a handler can type-match them) and final, while their
     * constructors stay package-private: only Parsley raises these.
     */
    @Test
    void concreteTypesArePublicAndFinalWithNoPublicConstructor() {
        for (Class<?> type : new Class<?>[]{
                CausalBufferDeserializationException.class,
                CausalVectorClockResolutionException.class,
                CausalTopicRecreatedException.class,
                CausalPendingAckException.class}) {
            assertTrue(Modifier.isPublic(type.getModifiers()),
                    type.getSimpleName() + " must be public for handler type-matching");
            assertTrue(Modifier.isFinal(type.getModifiers()),
                    type.getSimpleName() + " must be final; the hierarchy is closed");
            for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                assertFalse(Modifier.isPublic(constructor.getModifiers()),
                        type.getSimpleName() + " constructors must not be public; only Parsley raises it");
            }
        }
    }

    /** A coordinate-anchored failure exposes the full source-coordinate quartet to the handler. */
    @Test
    void coordinateAccessorsExposeTheSourceQuartet() {
        CausalCoordinateException e = new CausalBufferDeserializationException(
                "t1", T1_ID, 3, 42L, 7, "details", new RuntimeException("serde"));
        assertEquals("t1", e.topic(), "topic accessor must expose the source topic");
        assertEquals(T1_ID, e.topicId(), "topicId accessor must expose the stable UUID");
        assertEquals(3, e.partition(), "partition accessor must expose the source partition");
        assertEquals(42L, e.offset(), "offset accessor must expose the record's offset");
    }

    /**
     * {@link CausalVectorClockResolutionException#encodedDependencies()} hands out a copy: the
     * undecodable header bytes are operator forensics, and a public accessor must not let a caller
     * mutate the exception's own record of them.
     */
    @Test
    void encodedDependenciesReturnsADefensiveCopy() {
        byte[] encoded = {1, 2, 3};
        CausalVectorClockResolutionException e = new CausalVectorClockResolutionException(
                "t1", T1_ID, 0, 5L, encoded, "details", new RuntimeException("decode"));
        byte[] first = e.encodedDependencies();
        first[0] = 99;
        assertEquals(1, e.encodedDependencies()[0],
                "mutating the returned array must not affect the exception's stored bytes");
    }
}
