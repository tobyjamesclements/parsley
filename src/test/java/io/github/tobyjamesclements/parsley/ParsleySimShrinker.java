package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Delta-debugging over a recorded {@link ParsleySimTrace} schedule: given a trace that fails and
 * an oracle that replays a candidate trace and names its failure, produce a (locally) minimal
 * sub-trace that still fails <em>the same way</em>. Two passes: shortest failing prefix by binary
 * search (failures asserted mid-run are prefix-monotone: every prefix past the failing action
 * fails, every prefix before it passes — a candidate that turns out non-monotone, e.g. an
 * end-of-run drain check, is simply not adopted), then classic ddmin with doubling granularity.
 *
 * <p>"The same way" is decided by {@link #signatureOf}: throwable class plus its message with the
 * seed/replay label and every digit normalised away — a shrunken schedule shifts offsets and
 * counts, and those must not make the same defect look like a different one. The flip side is
 * accepted imprecision: two genuinely different failures of the same class whose messages differ
 * only in numbers collapse into one signature.
 *
 * <p>Oracle invocations are capped: shrinking is best-effort and always returns the smallest
 * failing trace found so far, never worse than the input.
 */
final class ParsleySimShrinker {

    private static final int MAX_ORACLE_RUNS = 500;
    private static final long BUDGET_NANOS = 60_000_000_000L;

    private ParsleySimShrinker() {}

    /** Replays a candidate schedule; returns its failure signature, or null if it passes. */
    interface Oracle {
        @Nullable String failureSignature(List<ParsleySimTrace.Action> trace);
    }

    /** The minimised trace, the signature it still fails with, and the oracle budget spent. */
    record Shrunk(List<ParsleySimTrace.Action> trace, String signature, int oracleRuns) {}

    /** Canonical failure identity — strip the run label and all digits, keep class + shape. */
    static String signatureOf(Throwable failure) {
        return failure.getClass().getSimpleName() + ": " + String.valueOf(failure.getMessage())
                .replaceAll("\\[seed \\d+\\] ", "")
                .replace("[replay] ", "")
                .replaceAll("\\d+", "#");
    }

    /**
     * Minimises {@code full} against {@code oracle}. The full trace must fail; its signature is
     * the target every accepted candidate has to reproduce exactly. Bounded by both an oracle-run
     * cap and a wall-clock budget — replaying a very long trace can cost seconds, and a shrink
     * that grinds for minutes is worse than a longer repro.
     */
    static Shrunk shrink(List<ParsleySimTrace.Action> full, Oracle oracle) {
        String target = oracle.failureSignature(full);
        if (target == null) {
            throw new IllegalArgumentException("the full trace does not fail — nothing to shrink");
        }
        long deadline = System.nanoTime() + BUDGET_NANOS;
        int[] runs = {1};
        List<ParsleySimTrace.Action> current = shortestFailingPrefix(full, target, oracle, runs, deadline);
        current = ddmin(current, target, oracle, runs, deadline);
        return new Shrunk(current, target, runs[0]);
    }

    private static boolean budgetLeft(int[] runs, long deadline) {
        return runs[0] < MAX_ORACLE_RUNS && System.nanoTime() < deadline;
    }

    private static List<ParsleySimTrace.Action> shortestFailingPrefix(
            List<ParsleySimTrace.Action> full, String target, Oracle oracle, int[] runs, long deadline) {
        int low = 1;
        int high = full.size();
        while (low < high && budgetLeft(runs, deadline)) {
            int mid = low + (high - low) / 2;
            runs[0]++;
            if (target.equals(oracle.failureSignature(full.subList(0, mid)))) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        // Monotonicity is an assumption, not a guarantee — adopt the prefix only if it verifiably
        // fails with the target (one extra run, unless the search already ended on a full-length
        // prefix, which the caller verified).
        if (high == full.size()) {
            return full;
        }
        runs[0]++;
        List<ParsleySimTrace.Action> prefix = full.subList(0, high);
        return target.equals(oracle.failureSignature(prefix)) ? prefix : full;
    }

    private static List<ParsleySimTrace.Action> ddmin(
            List<ParsleySimTrace.Action> start, String target, Oracle oracle, int[] runs, long deadline) {
        List<ParsleySimTrace.Action> current = start;
        int granularity = 2;
        while (current.size() >= 2 && granularity <= current.size() && budgetLeft(runs, deadline)) {
            int chunk = (current.size() + granularity - 1) / granularity;
            boolean reduced = false;
            for (int from = 0; from < current.size() && budgetLeft(runs, deadline); from += chunk) {
                List<ParsleySimTrace.Action> candidate = new ArrayList<>(current.subList(0, from));
                candidate.addAll(current.subList(Math.min(from + chunk, current.size()), current.size()));
                if (candidate.isEmpty()) {
                    continue;
                }
                runs[0]++;
                if (target.equals(oracle.failureSignature(candidate))) {
                    current = candidate;
                    granularity = Math.max(granularity - 1, 2);
                    reduced = true;
                    break;
                }
            }
            if (!reduced) {
                if (granularity >= current.size()) {
                    break;
                }
                granularity = Math.min(granularity * 2, current.size());
            }
        }
        return current;
    }
}
