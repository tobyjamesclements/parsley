package io.parsley;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Defines what happens to a record on each type of causal violation. Configure a uniform action
 * for all violations via the static factories, or use {@link #builder()} for per-violation-type
 * control:
 *
 * <ul>
 *   <li>{@link #forwardUnsafe} — forward the record downstream out-of-causal-order (lenient)
 *   <li>{@link #drop} — silently discard the record (strict)
 *   <li>{@link #deadLetter} — route the record to the dead-letter sink (strict)
 * </ul>
 *
 * <p>For mixed policies — for example, tolerating records from legacy non-Parsley producers while
 * enforcing strict ordering for Parsley-stamped ones — use the builder:
 *
 * <pre>{@code
 * CausalBufferPolicy.builder()
 *     .onMissing(CausalViolationAction.FORWARD_UNSAFE)   // legacy producers: forward regardless
 *     .onUnresolvable(CausalViolationAction.DROP)         // malformed header: discard
 *     .onLimit(CausalViolationAction.DEAD_LETTER)         // buffer overflow: route to DLQ
 *     .setLimit(CausalBufferLimit.ofSize(1000))
 *     .build()
 * }</pre>
 *
 * <p>A {@link CausalViolationHandler} is always called regardless of which action is taken.
 */
public final class CausalBufferPolicy {

    private final Map<CausalViolationReason, CausalViolationAction> actions;
    private final CausalBufferLimit limit;

    private CausalBufferPolicy(EnumMap<CausalViolationReason, CausalViolationAction> actions, CausalBufferLimit limit) {
        this.actions = Map.copyOf(actions);
        this.limit = limit;
    }

    // --- convenience factories ---

    /**
     * Creates a policy that forwards every violated record downstream out-of-causal-order — the
     * lenient, availability-over-consistency choice.
     *
     * @param limit the buffer eviction trigger
     * @return a new policy
     */
    public static CausalBufferPolicy forwardUnsafe(CausalBufferLimit limit) {
        return builder()
                .onMissing(CausalViolationAction.FORWARD_UNSAFE)
                .onUnresolvable(CausalViolationAction.FORWARD_UNSAFE)
                .onLimit(CausalViolationAction.FORWARD_UNSAFE)
                .setLimit(limit)
                .build();
    }

    /**
     * Creates a policy that silently discards every violated record — the strict,
     * consistency-over-availability choice.
     *
     * @param limit the buffer eviction trigger
     * @return a new policy
     */
    public static CausalBufferPolicy drop(CausalBufferLimit limit) {
        return builder()
                .onMissing(CausalViolationAction.DROP)
                .onUnresolvable(CausalViolationAction.DROP)
                .onLimit(CausalViolationAction.DROP)
                .setLimit(limit)
                .build();
    }

    /**
     * Creates a policy that routes every violated record to the dead-letter sink. Requires
     * {@link CausalProcessors.Builder#deadLetterSink} to be set.
     *
     * @param limit the buffer eviction trigger
     * @return a new policy
     */
    public static CausalBufferPolicy deadLetter(CausalBufferLimit limit) {
        return builder()
                .onMissing(CausalViolationAction.DEAD_LETTER)
                .onUnresolvable(CausalViolationAction.DEAD_LETTER)
                .onLimit(CausalViolationAction.DEAD_LETTER)
                .setLimit(limit)
                .build();
    }

    /**
     * Returns a new builder for constructing a policy with per-violation-type actions.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    // --- engine API ---

    /**
     * Returns the action to take for the given violation reason.
     *
     * @param reason the violation reason; must not be {@code null}
     * @return the configured action
     */
    public CausalViolationAction actionFor(CausalViolationReason reason) {
        return actions.get(reason);
    }

    /**
     * Returns the buffer eviction trigger.
     *
     * @return the limit
     */
    public CausalBufferLimit limit() {
        return limit;
    }

    /**
     * Returns {@code true} if any violation type is configured with {@link CausalViolationAction#DEAD_LETTER},
     * meaning a dead-letter sink must be supplied.
     *
     * @return whether a dead-letter sink is required
     */
    public boolean requiresDeadLetterSink() {
        return actions.containsValue(CausalViolationAction.DEAD_LETTER);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalBufferPolicy other)) return false;
        return actions.equals(other.actions) && limit.equals(other.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions, limit);
    }

    @Override
    public String toString() {
        return "CausalBufferPolicy[" +
                "MISSING_HEADER=" + actions.get(CausalViolationReason.MISSING_HEADER) +
                ", UNRESOLVABLE=" + actions.get(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES) +
                ", LIMIT_REACHED=" + actions.get(CausalViolationReason.LIMIT_REACHED) +
                ", limit=" + limit + ']';
    }

    /**
     * Builder for {@link CausalBufferPolicy}. All four settings must be provided; {@link #build()}
     * throws {@link IllegalStateException} if any are missing.
     */
    public static final class Builder {
        private CausalViolationAction onMissing;
        private CausalViolationAction onUnresolvable;
        private CausalViolationAction onLimit;
        private CausalBufferLimit limit;

        private Builder() {}

        /**
         * Sets the action for {@link CausalViolationReason#MISSING_HEADER} violations — records
         * that carry no {@code parsley-causal-dependencies} header (e.g. from legacy producers).
         *
         * @param action the action; must not be {@code null}
         * @return this builder
         */
        public Builder onMissing(CausalViolationAction action) {
            this.onMissing = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        /**
         * Sets the action for {@link CausalViolationReason#UNRESOLVABLE_DEPENDENCIES} violations —
         * records whose dependency header is present but cannot be deserialised.
         *
         * @param action the action; must not be {@code null}
         * @return this builder
         */
        public Builder onUnresolvable(CausalViolationAction action) {
            this.onUnresolvable = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        /**
         * Sets the action for {@link CausalViolationReason#LIMIT_REACHED} violations — records
         * evicted when the buffer limit fires.
         *
         * @param action the action; must not be {@code null}
         * @return this builder
         */
        public Builder onLimit(CausalViolationAction action) {
            this.onLimit = Objects.requireNonNull(action, "action must not be null");
            return this;
        }

        /**
         * Sets the buffer eviction trigger.
         *
         * @param limit the limit; must not be {@code null}
         * @return this builder
         */
        public Builder setLimit(CausalBufferLimit limit) {
            this.limit = Objects.requireNonNull(limit, "limit must not be null");
            return this;
        }

        /**
         * Returns a new {@link CausalBufferPolicy} from the settings accumulated so far.
         *
         * @return a new policy
         * @throws IllegalStateException if any setting has not been provided
         */
        public CausalBufferPolicy build() {
            if (onMissing == null) throw new IllegalStateException("onMissing action must be set");
            if (onUnresolvable == null) throw new IllegalStateException("onUnresolvable action must be set");
            if (onLimit == null) throw new IllegalStateException("onLimit action must be set");
            if (limit == null) throw new IllegalStateException("setLimit must be called");
            EnumMap<CausalViolationReason, CausalViolationAction> map = new EnumMap<>(CausalViolationReason.class);
            map.put(CausalViolationReason.MISSING_HEADER, onMissing);
            map.put(CausalViolationReason.UNRESOLVABLE_DEPENDENCIES, onUnresolvable);
            map.put(CausalViolationReason.LIMIT_REACHED, onLimit);
            return new CausalBufferPolicy(map, limit);
        }
    }
}
