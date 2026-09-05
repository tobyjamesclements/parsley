package io.github.tobyjamesclements.parsley.kafka;

import java.util.Set;
import java.util.UUID;

/**
 * Answers, when a task initialises, which of the topics its ordering state names still exist.
 *
 * <p>This is the one question the Kafka Streams host asks the substrate outside delivery,
 * and it is asked at task initialisation rather than on a cadence (D115). A seam, so a
 * topology can be driven without a broker.
 *
 * @see AdminTopicIdentitySource
 */
interface TopicIdentitySource {

    /** A source that reports every topic alive, for topologies driven without a broker. */
    TopicIdentitySource ALL_ALIVE = topicIds -> TopicIdentityVerdicts.NONE;

    /**
     * Classifies topic ids.
     *
     * <p>Absence of evidence is not evidence of deletion: an id whose name the source does
     * not know, whose describe was denied, or whose answer did not arrive is reported as
     * neither deleted nor recreated, and its causes stay expressed (D44, D75).
     *
     * @param topicIds the ids a task's received channels and frontier name
     * @return the ids confirmed deleted, and those confirmed recreated under their name
     * @throws Exception if the substrate cannot be asked at all
     */
    TopicIdentityVerdicts resolve(Set<UUID> topicIds) throws Exception;
}
