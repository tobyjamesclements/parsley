package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the bootstrap member's session-timeout inheritance accepts exactly what
 * Kafka's own config parser accepts, and names the property when nothing does.
 *
 * <p>The bootstrap parses the value itself, so a divergence from Kafka's parsing rules
 * would refuse a configuration every other client in the process runs happily on — and a
 * genuinely bad value used to surface as a bare NumberFormatException wrapped in a message
 * pointing nowhere near the property (D87).
 */
class SessionTimeoutInheritanceTest {

    /** Kafka's config parser trims string values before parsing; the bootstrap must too. */
    @Test
    void whitespacePaddedValueParsesLikeKafkasOwnParser() {
        Map<String, Object> props = GroupMembershipCommitter.memberProperties(
                Map.of("bootstrap.servers", "b:9092", ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, " 12000 "), "g");

        assertEquals(12_000, props.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                "a whitespace-padded value every Kafka client accepts must not fail only in the bootstrap");
    }

    /** A timeout configured the idiomatic prefixed way reaches the plain consumer config. */
    @Test
    void streamsPrefixedSpellingReachesThePlainConsumer() {
        Map<String, Object> props = GroupMembershipCommitter.memberProperties(
                Map.of("bootstrap.servers", "b:9092", "main.consumer.session.timeout.ms", 2_000), "g");

        assertEquals(2_000, props.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                "the prefixed spelling must reach this plain consumer or the join is rejected outright");
    }

    /** A value nothing accepts fails naming its property, not as a bare parse exception. */
    @Test
    void unparsableValueNamesItsProperty() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> GroupMembershipCommitter.memberProperties(
                        Map.of("bootstrap.servers", "b:9092",
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "ten seconds"), "g"),
                "an unparsable timeout must be refused attributably");
        assertTrue(e.getMessage().contains("session.timeout.ms"),
                "the refusal names the property: " + e.getMessage());
    }

    /** A value outside the int range Kafka accepts is refused rather than truncated. */
    @Test
    void outOfRangeValueIsRefusedNotTruncated() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> GroupMembershipCommitter.memberProperties(
                        Map.of("bootstrap.servers", "b:9092",
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000000000"), "g"),
                "a value beyond Integer.MAX_VALUE cannot reach Kafka and must be refused here");
        assertTrue(e.getMessage().contains("3000000000"),
                "the refusal names the value: " + e.getMessage());
    }
}
