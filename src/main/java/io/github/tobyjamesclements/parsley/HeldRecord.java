package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * Why one record is waiting at the causal gate: the record's coordinate, how long it has gated
 * its channel, and the causes it is still waiting for. Obtained from
 * {@link CausalStreams#explainHolds()} and, for holds that outlive their stage's
 * {@code holdWarningAfter} threshold, written to the log.
 *
 * <p>Only the head of a channel's hold queue appears here. Holding is head-of-line blocking by
 * design, so the head is the whole story: everything behind it on that channel waits on it, not
 * on causes of its own.
 *
 * <p>This is a diagnostic observation, not a query surface. It carries coordinates, claims, and
 * local watermarks — never key or value bytes, on the same principle that keeps payloads out of
 * the log — and it describes the task's in-flight view at the moment it was sampled, which an
 * aborted transaction may roll back. Read it to find out why a record is waiting; read sinks
 * for what the application computed.
 *
 * <p><b>The rule this exists to serve:</b> a held record is the gate doing its job. The fix is
 * always to deliver the missing cause, never to skip, reorder, or time out — see
 * {@code docs/guide/diagnosing-holds.md}.
 *
 * @param stage the stage whose task holds the record
 * @param taskId the task, as Kafka Streams names it
 * @param topic the source topic the held record arrived on
 * @param partition its partition, which is the task's own
 * @param offset the held record's offset
 * @param queueDepth how many records wait on this channel, the head included
 * @param heldMs milliseconds this record has been its channel's head, measured from the first
 *     sample that observed it there — a restored hold ages from the restart, not from its
 *     original arrival
 * @param unmet the causes still missing; never empty, since a record with nothing unmet is
 *     delivered rather than held
 */
public record HeldRecord(String stage, String taskId, String topic, int partition, long offset,
                         int queueDepth, long heldMs, List<Unmet> unmet) {

    public HeldRecord {
        unmet = List.copyOf(unmet);
    }

    /**
     * One cause a held record is still waiting for, with the local watermarks that show the
     * gap. A claim is either an offset claim ({@code claimedOffset} set, {@code sender} null)
     * or a sequence claim ({@code sender} and {@code claimedSequence} set), the two claim kinds
     * of the wire format.
     *
     * @param diagnosis what the shape of the gap indicates
     * @param topic the topic the missing cause sits on
     * @param partition its partition
     * @param claimedOffset the offset claimed, or {@code -1} for a sequence claim
     * @param localFrontier the highest offset delivered locally on that channel, {@code -1} for
     *     none — the watermark the claim is measured against
     * @param localPosition the local consumer position: everything strictly below it has been
     *     fetched here or consumer-skipped
     * @param sender the claimed record's sender identity, or null for an offset claim
     * @param claimedSequence the send sequence claimed, or {@code -1} for an offset claim
     * @param deliveredSequence the highest sequence delivered locally from that sender on that
     *     channel, or {@code -1} if none has been
     */
    public record Unmet(Diagnosis diagnosis, String topic, int partition, long claimedOffset,
                        long localFrontier, long localPosition, String sender,
                        long claimedSequence, long deliveredSequence) {

        /** A one-line, payload-free description, used verbatim in the log line. */
        public String describe() {
            String claim = sender == null
                    ? "offset " + claimedOffset
                    : "sequence " + claimedSequence + " from sender " + sender;
            String local = sender == null
                    ? "local frontier " + localFrontier + ", position " + localPosition
                    : "highest delivered sequence " + deliveredSequence
                            + " from that sender, local position " + localPosition;
            return diagnosis.code() + " waiting on " + topic + ":" + partition + " " + claim
                    + " (" + local + ") — " + diagnosis.remedy()
                    + " — see " + diagnosis.reference();
        }
    }

    /**
     * What the shape of an unmet claim indicates. Each constant carries a stable code, the
     * documentation anchor that explains it, and the action it calls for. None of them is ever
     * "skip the record": a claim names a really-appended offset, so every wait resolves once
     * its cause is delivered ({@code docs/foundations/liveness.md}).
     */
    public enum Diagnosis {

        /**
         * The claimed record has not been fetched here yet — the ordinary case, and the one
         * that resolves itself. The cause channel is behind: check its consumer lag and
         * whether its producer is healthy.
         */
        NOT_FETCHED("vc-hold-not-fetched",
                "the cause has not reached this consumer yet; check lag on that topic",
                "docs/guide/diagnosing-holds.md#the-cause-has-not-arrived"),

        /**
         * The claimed record has been fetched here but not delivered, so it is itself held
         * behind its own channel's head. Follow the chain: that channel's head appears in the
         * same report, with its own unmet causes.
         */
        HELD_UPSTREAM("vc-hold-held-upstream",
                "the cause is itself held on its channel; follow that channel's head",
                "docs/guide/diagnosing-holds.md#the-cause-is-itself-held"),

        /**
         * A sequence claim naming a sender this task has never delivered on that channel. If
         * the sender never writes that partition again, this is the documented late-joiner
         * window: a consumer baselined above the claimed record cannot deliver it.
         */
        SENDER_UNSEEN("vc-hold-sender-unseen",
                "no record from that sender has been delivered on that channel; if this"
                        + " consumer baselined at the log end, see the late-joiner caveat",
                "docs/foundations/liveness.md#the-sequence-claim-caveat-late-joiners"),

        /**
         * A sequence claim ahead of what this task has delivered from that sender: the
         * sender's later records are on the way, or their transaction has not committed.
         */
        SENDER_BEHIND("vc-hold-sender-behind",
                "that sender's later records have not been delivered here yet; check lag and"
                        + " whether its transaction has committed",
                "docs/guide/diagnosing-holds.md#the-senders-later-records-are-behind");

        private final String code;
        private final String remedy;
        private final String reference;

        Diagnosis(String code, String remedy, String reference) {
            this.code = code;
            this.remedy = remedy;
            this.reference = reference;
        }

        /** The stable identifier to alert on; safe to match in log pipelines. */
        public String code() {
            return code;
        }

        /** What to do about it — never to skip or time out the held record. */
        public String remedy() {
            return remedy;
        }

        /** The documentation anchor that explains this diagnosis in full. */
        public String reference() {
            return reference;
        }
    }

    /** A one-line, payload-free summary of the hold and its first unmet cause. */
    public String summary() {
        return "stage '" + stage + "' task " + taskId + " has held " + topic + ":" + partition
                + "@" + offset + " for " + heldMs + "ms (queue depth " + queueDepth + "): "
                + unmet.get(0).describe()
                + (unmet.size() > 1 ? " (+" + (unmet.size() - 1) + " more unmet)" : "");
    }
}
