package io.github.tobyjamesclements.parsley.kafka;

/**
 * The one cause-chain walk the kafka suites assert diagnoses through. Both walks are
 * bounded at 64 links, matching
 * {@link io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException#findIn}'s
 * guard against a cyclic chain, and both start at the thrown throwable itself — Kafka
 * Streams wraps handler failures, so the diagnosis is rarely the outermost throwable.
 */
final class TestChains {

    private TestChains() {
    }

    /**
     * Whether the chain holds a throwable of {@code type} whose message contains
     * {@code messageFragmentOrNull} — pass {@code null} to match on the type alone.
     */
    static boolean chainContains(Throwable thrown, Class<? extends Throwable> type,
                                 String messageFragmentOrNull) {
        Throwable cause = thrown;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (type.isInstance(cause)
                    && (messageFragmentOrNull == null
                            || (cause.getMessage() != null
                                    && cause.getMessage().contains(messageFragmentOrNull)))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the chain holds exactly {@code wanted}, by identity — the pin for a scripted
     * failure instance riding a wrap unaltered, stronger than a type-and-message match.
     */
    static boolean chainContains(Throwable thrown, Throwable wanted) {
        Throwable cause = thrown;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (cause == wanted) {
                return true;
            }
        }
        return false;
    }
}
