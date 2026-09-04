package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that every refusal reason reaches an operator with a documented course of
 * action.
 *
 * <p>{@code docs/runbooks.md} promises one runbook per reason and a row per reason in its
 * triage table, and {@code docs/failing-closed.md} a trigger row per reason. A reason added
 * to {@link Reason} without them would stop a process with a diagnosis nothing tells the
 * operator what to do about (D114). The scan is over the Markdown sources, so it needs no
 * docs toolchain.
 */
class RunbookCoverageTest {

    private static final Path RUNBOOKS = Path.of("docs", "runbooks.md");
    private static final Path FAILING_CLOSED = Path.of("docs", "failing-closed.md");

    /** Every reason has a runbook heading of its own in docs/runbooks.md. */
    @Test
    void everyRefusalReasonHasARunbook() throws IOException {
        String runbooks = Files.readString(RUNBOOKS);
        for (Reason reason : Reason.values()) {
            assertTrue(runbooks.contains("\n#### " + reason + "\n"),
                    "docs/runbooks.md has no runbook heading '#### " + reason + "'");
        }
    }

    /** Every reason has a row in the triage table of docs/runbooks.md. */
    @Test
    void everyRefusalReasonIsInTheTriageTable() throws IOException {
        String runbooks = Files.readString(RUNBOOKS);
        for (Reason reason : Reason.values()) {
            assertTrue(runbooks.contains("\n| `" + reason + "` |"),
                    "docs/runbooks.md's triage table has no row for " + reason);
        }
    }

    /** Every reason has a row in the trigger table of docs/failing-closed.md. */
    @Test
    void everyRefusalReasonHasATriggerRow() throws IOException {
        String triggers = Files.readString(FAILING_CLOSED);
        for (Reason reason : Reason.values()) {
            assertTrue(triggers.contains("\n| `" + reason + "` |"),
                    "docs/failing-closed.md's trigger table has no row for " + reason);
        }
    }
}
