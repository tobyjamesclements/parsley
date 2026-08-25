package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The refuse-everything base for hand-rolled consumer doubles: every {@link Consumer}
 * method throws {@link UnsupportedOperationException} naming itself. A double extends this
 * and overrides only the methods the seam under test scripts, so the deliberate tripwire —
 * the loop under test growing a new consumer dependency fails loudly in the double rather
 * than passing vacuously — lives in exactly one place instead of being restated per suite.
 */
abstract class RefusingConsumer implements Consumer<byte[], byte[]> {

    /** The tripwire: the method is not part of the seam this double scripts. */
    private static UnsupportedOperationException refused(String method) {
        return new UnsupportedOperationException(
                method + " is not scripted by this consumer double; the loop under test grew"
                        + " a dependency its test never meant to allow");
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw refused("assignment");
    }

    @Override
    public Set<String> subscription() {
        throw refused("subscription");
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw refused("subscribe");
    }

    @Override
    public void subscribe(Collection<String> topics,
                          org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
        throw refused("subscribe");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        throw refused("assign");
    }

    @Override
    public void subscribe(java.util.regex.Pattern pattern,
                          org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
        throw refused("subscribe");
    }

    @Override
    public void subscribe(java.util.regex.Pattern pattern) {
        throw refused("subscribe");
    }

    @Override
    public void subscribe(org.apache.kafka.clients.consumer.SubscriptionPattern pattern,
                          org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
        throw refused("subscribe");
    }

    @Override
    public void subscribe(org.apache.kafka.clients.consumer.SubscriptionPattern pattern) {
        throw refused("subscribe");
    }

    @Override
    public void unsubscribe() {
        throw refused("unsubscribe");
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        throw refused("poll");
    }

    @Override
    public void commitSync() {
        throw refused("commitSync");
    }

    @Override
    public void commitSync(Duration timeout) {
        throw refused("commitSync");
    }

    @Override
    public void commitSync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets) {
        throw refused("commitSync");
    }

    @Override
    public void commitSync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets,
                           Duration timeout) {
        throw refused("commitSync");
    }

    @Override
    public void commitAsync() {
        throw refused("commitAsync");
    }

    @Override
    public void commitAsync(org.apache.kafka.clients.consumer.OffsetCommitCallback callback) {
        throw refused("commitAsync");
    }

    @Override
    public void commitAsync(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets,
                            org.apache.kafka.clients.consumer.OffsetCommitCallback callback) {
        throw refused("commitAsync");
    }

    @Override
    public void registerMetricForSubscription(org.apache.kafka.common.metrics.KafkaMetric metric) {
        throw refused("registerMetricForSubscription");
    }

    @Override
    public void unregisterMetricFromSubscription(org.apache.kafka.common.metrics.KafkaMetric metric) {
        throw refused("unregisterMetricFromSubscription");
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw refused("seek");
    }

    @Override
    public void seek(TopicPartition partition,
                     org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata) {
        throw refused("seek");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw refused("seekToBeginning");
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw refused("seekToEnd");
    }

    @Override
    public long position(TopicPartition partition) {
        throw refused("position");
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        throw refused("position");
    }

    @Override
    public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed(
            Set<TopicPartition> partitions) {
        throw refused("committed");
    }

    @Override
    public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed(
            Set<TopicPartition> partitions, Duration timeout) {
        throw refused("committed");
    }

    @Override
    public org.apache.kafka.common.Uuid clientInstanceId(Duration timeout) {
        throw refused("clientInstanceId");
    }

    @Override
    public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
        throw refused("metrics");
    }

    @Override
    public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic) {
        throw refused("partitionsFor");
    }

    @Override
    public List<org.apache.kafka.common.PartitionInfo> partitionsFor(String topic, Duration timeout) {
        throw refused("partitionsFor");
    }

    @Override
    public Map<String, List<org.apache.kafka.common.PartitionInfo>> listTopics() {
        throw refused("listTopics");
    }

    @Override
    public Map<String, List<org.apache.kafka.common.PartitionInfo>> listTopics(Duration timeout) {
        throw refused("listTopics");
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw refused("pause");
    }

    @Override
    public Set<TopicPartition> paused() {
        throw refused("paused");
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw refused("resume");
    }

    @Override
    public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        throw refused("offsetsForTimes");
    }

    @Override
    public Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        throw refused("offsetsForTimes");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        throw refused("beginningOffsets");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        throw refused("beginningOffsets");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        throw refused("endOffsets");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        throw refused("endOffsets");
    }

    @Override
    public java.util.OptionalLong currentLag(TopicPartition topicPartition) {
        throw refused("currentLag");
    }

    @Override
    public org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata() {
        throw refused("groupMetadata");
    }

    @Override
    public void enforceRebalance() {
        throw refused("enforceRebalance");
    }

    @Override
    public void enforceRebalance(String reason) {
        throw refused("enforceRebalance");
    }

    @Override
    public void close() {
        throw refused("close");
    }

    @Override
    public void close(Duration timeout) {
        throw refused("close");
    }

    @Override
    public void close(org.apache.kafka.clients.consumer.CloseOptions option) {
        throw refused("close");
    }

    @Override
    public void wakeup() {
        throw refused("wakeup");
    }
}
