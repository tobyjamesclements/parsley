package io.github.tobyjamesclements.parsley.api;

import java.util.regex.Pattern;

/**
 * The one spelling of Kafka's topic-name rule.
 *
 * <p>Every name that becomes a Kafka topic name or a component of one — channel topics,
 * store names, process names, the application-id prefix — is validated here, so the rule
 * cannot drift between declaration sites. The kafka layer bounds composed changelog names
 * to the same limit at its one composition point ({@code ProcessTopology.changelogName}),
 * which cannot reference this class across packages; the two limits are pinned to agree by
 * the declaration-site and composed-name refusal tests.
 */
final class KafkaNames {

    /** Kafka's topic-name length limit. */
    static final int MAX_TOPIC_NAME_LENGTH = 249;

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
    static boolean isValidTopicName(String name) {
        return name != null
                && name.length() <= MAX_TOPIC_NAME_LENGTH
                && !name.equals(".")
                && !name.equals("..")
                && LEGAL_CHARACTERS.matcher(name).matches();
    }
}
