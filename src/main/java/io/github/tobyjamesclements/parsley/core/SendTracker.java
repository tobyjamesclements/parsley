package io.github.tobyjamesclements.parsley.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The core's window onto the node's own produced records. The broker performs the sender's
 * clock increment (offset assignment), learned asynchronously from acknowledgements, so the
 * core reconstructs its own-outputs clock through this interface.
 */
public interface SendTracker {

    /** One acknowledged own send. */
    record Ack(Channel channel, long offset) {}

    /** Returns and clears every acknowledgement received since the last drain. */
    List<Ack> drainAcks();

    /**
     * The crossing wait: blocks until no own send to a channel outside {@code except} is
     * unacknowledged. Two outputs of one invocation are causally ordered even across sink
     * topics, and the later one's stamp cannot claim the earlier until its offset is known.
     * Sends to {@code except} itself need no claim: the destination channel's own FIFO order
     * already delivers them in order downstream.
     *
     * @throws CausalSendException on an observed send failure or timeout; a stamp that might
     *     under-claim the node's own outputs must die with its transaction, never proceed.
     */
    void awaitQuiescence(Set<Channel> except) throws CausalSendException;

    /**
     * Current end offsets (next offset to be assigned) for every partition of the given sink
     * topics, used for the init-time own-outputs seed. Strict: resolution failure must throw,
     * because starting with the seed silently off would under-claim the node's own outputs for
     * the task's whole lifetime.
     */
    Map<Channel, Long> endOffsets(Set<java.util.UUID> sinkTopics);
}
