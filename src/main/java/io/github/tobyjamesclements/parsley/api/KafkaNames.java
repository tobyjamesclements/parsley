package io.github.tobyjamesclements.parsley.api;

import java.util.regex.Pattern;

/**
 * The one spelling of Kafka's topic-name rule.
 *
 * <p>Every name that becomes a Kafka topic name or a component of one — channel topics,
 * store names, process names, the application-id prefix — is validated here, and the kafka
 * layer bounds composed changelog names with {@link #MAX_TOPIC_NAME_LENGTH}, so the rule
 * cannot drift between sites. Applications may use {@link #isValidTopicName} to
 * pre-validate names before declaring them.
 *
 * <p>The rule is pinned against {@code kafka-clients}' own
 * {@code org.apache.kafka.common.internals.Topic#isValid} by
 * {@code ApiValidationTest#kafkaNamesAgreesWithKafkaClientsOwnRule}, so a client upgrade
 * that changes what the broker accepts fails the build instead of drifting silently.
 */
public final class KafkaNames {

    /** Kafka's topic-name length limit. */
    public static final int MAX_TOPIC_NAME_LENGTH = 249;

    /** Precompiled once: {@code String.matches} recompiles its pattern on every call. */
    private static final Pattern LEGAL_CHARACTERS = Pattern.compile("[a-zA-Z0-9._-]+");

    /** The rule, spelled once for every refusal message that cites it. */
    static final String RULE =
            "[a-zA-Z0-9._-], at most " + MAX_TOPIC_NAME_LENGTH + " characters, not '.' or '..'";

    private KafkaNames() {
    }

    /**
     * Decides whether a name satisfies Kafka's topic-name rule.
     *
     * @param name a candidate topic name or topic-name component
     * @return {@code true} when {@code name} is non-null, non-empty, within the length
     *         limit, not {@code "."} or {@code ".."}, and uses only legal characters
     */
    public static boolean isValidTopicName(String name) {
        return name != null
                && name.length() <= MAX_TOPIC_NAME_LENGTH
                && !name.equals(".")
                && !name.equals("..")
                && LEGAL_CHARACTERS.matcher(name).matches();
    }
}
