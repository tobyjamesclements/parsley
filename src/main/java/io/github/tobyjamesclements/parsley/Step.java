package io.github.tobyjamesclements.parsley;

import java.util.Arrays;
import java.util.List;

/**
 * The result of one {@link Fold} application, holding the state to persist for the key and the
 * emissions the message caused.
 *
 * <p>A record, so fold logic is testable with plain equality.
 *
 * @param <S> the stage's per-key state type
 * @param state the state to persist for the key, or {@code null} to delete it
 * @param emissions the emissions the message caused, possibly empty
 */
public record Step<S>(S state, List<Emission> emissions) {

    /** Copies {@code emissions} so the record is immutable. */
    public Step {
        emissions = List.copyOf(emissions);
    }

    /**
     * Returns a step with the given state and emissions.
     *
     * @param <S> the stage's per-key state type
     * @param state the state to persist for the key, or {@code null} to delete it
     * @param emissions the emissions the message caused
     * @return the step
     */
    public static <S> Step<S> of(S state, Emission... emissions) {
        return new Step<>(state, Arrays.asList(emissions));
    }
}
