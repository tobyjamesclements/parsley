package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * Describes why one record is waiting at the causal gate.
 *
 * <p>Obtained from {@link CausalStreams#explainHolds()} and, for holds that outlive their
 * stage's {@code holdWarningAfter} threshold, written to the log.
 *
 * <p>Only the head of a channel's hold queue appears here. Everything behind it on that
 * channel waits on the head, not on causes of its own.
 *
 * <p>Carries coordinates, claims, and local watermarks, never key or value bytes. Describes
 * the task's in-flight view at the moment it was sampled, which an aborted transaction may
 * roll back. Read sinks for what the application computed.
 *
 * <p>A held record is the gate doing its job. Deliver the missing cause. Do not skip, reorder,
 * or time out.
 *
 * @param stage the stage whose task holds the record
 * @param taskId the task, as Kafka Streams names it
 * @param topic the source topic the held record arrived on
 * @param partition its partition, which is the task's own
 * @param offset the held record's offset
 * @param queueDepth how many records wait on this channel, the head included
 * @param heldMs milliseconds this record has been its channel's head, measured from the first
 *     sample that observed it there, so a restored hold ages from the restart rather than from
 *     its original arrival
 * @param unmet the causes still missing, never empty
 */
public record HeldRecord(String stage, String taskId, String topic, int partition, long offset,
                         int queueDepth, long heldMs, List<Unmet> unmet) {

    /** Copies {@code unmet} so the record is immutable. */
    public HeldRecord {
        unmet = List.copyOf(unmet);
    }

    /**
     * Describes one cause a held record is still waiting for, with the local watermarks that
     * show the gap.
     *
     * <p>A claim is either an offset claim, with {@code claimedOffset} set and {@code sender}
     * null, or a sequence claim, with {@code sender} and {@code claimedSequence} set. Those
     * are the two claim kinds of the wire format.
     *
     * @param diagnosis what the shape of the gap indicates
     * @param topic the topic the missing cause sits on
     * @param partition its partition
     * @param claimedOffset the offset claimed, or {@code -1} for a sequence claim
     * @param localFrontier the highest offset delivered locally on that channel and the
     *     watermark an offset claim is measured against, or {@code -1} for none
     * @param localPosition the local consumer position, below which everything has been
     *     fetched here or consumer-skipped
     * @param sender the claimed record's sender identity, or null for an offset claim
     * @param claimedSequence the send sequence claimed, or {@code -1} for an offset claim
     * @param deliveredSequence the highest sequence delivered locally from that sender on that
     *     channel, or {@code -1} if none has been
     */
    public record Unmet(Diagnosis diagnosis, String topic, int partition, long claimedOffset,
                        long localFrontier, long localPosition, String sender,
                        long claimedSequence, long deliveredSequence) {

        /**
         * Returns a one-line, payload-free description, used verbatim in the log line.
         *
         * @return the description, naming the diagnosis code, the claim, the local watermarks,
         *         the remedy and the diagnosis reference
         */
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
     * Indicates what the shape of an unmet claim means.
     *
     * <p>Each constant carries a stable code, a reference to its own Javadoc, and the action
     * it calls for. None of them is ever to skip the record. A claim names a really-appended
     * offset, so every wait resolves once its cause is delivered.
     */
    public enum Diagnosis {

        /**
         * The claimed record has not been fetched here yet. This is the ordinary case, and it
         * resolves itself. The cause channel is behind, so check its consumer lag and whether
         * its producer is healthy.
         */
        NOT_FETCHED("vc-hold-not-fetched",
                "the cause has not reached this consumer yet; check lag on that topic",
                "HeldRecord.Diagnosis#NOT_FETCHED"),

        /**
         * The claimed record has been fetched here but not delivered, so it is itself held
         * behind its own channel's head. Follow the chain. That channel's head appears in the
         * same report, with its own unmet causes.
         */
        HELD_UPSTREAM("vc-hold-held-upstream",
                "the cause is itself held on its channel; follow that channel's head",
                "HeldRecord.Diagnosis#HELD_UPSTREAM"),

        /**
         * A sequence claim naming a sender this task has never delivered on that channel. If
         * the sender never writes that partition again, this is the late-joiner window. A
         * consumer baselined above the claimed record cannot deliver it.
         */
        SENDER_UNSEEN("vc-hold-sender-unseen",
                "no record from that sender has been delivered on that channel; if this"
                        + " consumer baselined at the log end, see the late-joiner caveat",
                "HeldRecord.Diagnosis#SENDER_UNSEEN"),

        /**
         * A sequence claim ahead of what this task has delivered from that sender. The
         * sender's later records are on the way, or their transaction has not committed.
         */
        SENDER_BEHIND("vc-hold-sender-behind",
                "that sender's later records have not been delivered here yet; check lag and"
                        + " whether its transaction has committed",
                "HeldRecord.Diagnosis#SENDER_BEHIND");

        private final String code;
        private final String remedy;
        private final String reference;

        Diagnosis(String code, String remedy, String reference) {
            this.code = code;
            this.remedy = remedy;
            this.reference = reference;
        }

        /**
         * Returns the stable identifier to alert on. Safe to match in log pipelines.
         *
         * @return the diagnosis code
         */
        public String code() {
            return code;
        }

        /**
         * Returns the action this diagnosis calls for, which is never to skip or time out the
         * held record.
         *
         * @return the remedy
         */
        public String remedy() {
            return remedy;
        }

        /**
         * Returns the Javadoc element that explains this diagnosis in full, which is this
         * constant.
         *
         * @return the element reference, as {@code HeldRecord.Diagnosis#<constant>}
         */
        public String reference() {
            return reference;
        }
    }

    /**
     * Returns a one-line, payload-free summary of the hold and its first unmet cause.
     *
     * @return the summary, with a count of any further unmet causes
     */
    public String summary() {
        return "stage '" + stage + "' task " + taskId + " has held " + topic + ":" + partition
                + "@" + offset + " for " + heldMs + "ms (queue depth " + queueDepth + "): "
                + unmet.get(0).describe()
                + (unmet.size() > 1 ? " (+" + (unmet.size() - 1) + " more unmet)" : "");
    }
}
