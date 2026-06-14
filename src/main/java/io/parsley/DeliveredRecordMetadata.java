package io.parsley;

import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * The source coordinate of the record a {@link ParsleyProcessor} is currently delivering to
 * its delegate. Used by {@link StampingProcessorContext} to answer {@code recordMetadata()} with the
 * delivered record's true origin — which, for a record that was buffered and drained later, differs
 * from the Streams "current" record that triggered the drain.
 *
 * @param topic     the source topic
 * @param partition the source partition
 * @param offset    the source offset
 */
record DeliveredRecordMetadata(String topic, int partition, long offset) implements RecordMetadata {}
