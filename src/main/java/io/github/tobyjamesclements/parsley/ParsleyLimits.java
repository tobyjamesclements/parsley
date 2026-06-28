package io.github.tobyjamesclements.parsley;

import java.time.Duration;
import java.util.Optional;

/**
 * Utility methods for resolving the configured limit kinds from a {@link CausalBufferLimit}.
 */
final class ParsleyLimits {

    private ParsleyLimits() {}

    /**
     * Resolves the configured {@link ParsleySizeLimit}, if any, from {@code limit} — including one
     * nested inside a {@link ParsleyFirstLimit}.
     *
     * @param limit the configured buffer limit
     * @return the size limit's message count, or empty if no {@link ParsleySizeLimit} is configured
     */
    static Optional<Integer> sizeLimitOf(CausalBufferLimit limit) {
        return switch (limit) {
            case ParsleyUnboundedLimit ul -> Optional.empty();
            case ParsleySizeLimit sl -> Optional.of(sl.messages());
            case ParsleyDurationLimit dl -> Optional.empty();
            case ParsleyFirstLimit fl -> fl.limits().stream()
                    .map(ParsleyLimits::sizeLimitOf)
                    .flatMap(Optional::stream)
                    .findFirst();
        };
    }

    /**
     * Resolves the configured {@link ParsleyDurationLimit}, if any, from {@code limit} — including
     * one nested inside a {@link ParsleyFirstLimit}.
     *
     * @param limit the configured buffer limit
     * @return the duration limit, or empty if no {@link ParsleyDurationLimit} is configured
     */
    static Optional<Duration> durationLimitOf(CausalBufferLimit limit) {
        return switch (limit) {
            case ParsleyUnboundedLimit ul -> Optional.empty();
            case ParsleyDurationLimit dl -> Optional.of(dl.duration());
            case ParsleySizeLimit sl -> Optional.empty();
            case ParsleyFirstLimit fl -> fl.limits().stream()
                    .map(ParsleyLimits::durationLimitOf)
                    .flatMap(Optional::stream)
                    .findFirst();
        };
    }
}