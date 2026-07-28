package io.github.tobyjamesclements.parsley;

import java.util.Arrays;
import java.util.List;

/**
 * The result of one {@link Fold} application: the state to persist for the key ({@code null}
 * deletes it) and the emissions the message caused. A record, so fold logic is testable with
 * plain equality.
 */
public record Step<S>(S state, List<Emission> emissions) {

    public Step {
        emissions = List.copyOf(emissions);
    }

    public static <S> Step<S> of(S state, Emission... emissions) {
        return new Step<>(state, Arrays.asList(emissions));
    }
}
