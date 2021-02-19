package com.redhat.emergency.response.source;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Named("mission-source.rebalancer")
public class MissionSourceRebalanceListener implements KafkaConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(MissionSourceRebalanceListener.class);

    @ConfigProperty(name = "mission-source.rebalancer.consumer.resume.delay", defaultValue = "10000")
    long delay;

    @ConfigProperty(name = "mission-source.rebalancer.consumer.resume.max.delay", defaultValue = "300000")
    long maxDelay;

    private final Map<TopicPartition, Pair<Long, Long>> counters = new HashMap<>();

    private final Set<TopicPartition> topicPartitions = new HashSet<>();

    private final Map<TopicPartition, Pair<Long, Boolean>> offsets = new HashMap<>();

    private Consumer<?, ?> consumer;

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        this.topicPartitions.addAll(partitions);
        this.consumer = consumer;
        log.info("Partition assigned. Consuming from " + topicPartitions.size() + " partitions");
        // set offsets
        consumer.endOffsets(partitions).forEach((topicPartition, offset) -> {
            offsets.put(topicPartition, new ImmutablePair<>(offset, false));
            log.info("Partition " + topicPartition.partition() + " : end offset = " + offset);
        });
    }

    @Override
    public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        topicPartitions.removeAll(partitions);
        log.info("Partition revoked. Consuming from " + topicPartitions.size() + " partitions");
        //remove offsets
        partitions.forEach(offsets::remove);
    }

    public Pair<Long, Boolean> setOffset(String topic, int partition, long offset) {
        return setOffset(topic, partition, offset, false);
    }

    public Pair<Long, Boolean> setOffset(String topic, int partition, boolean paused) {
        return setOffset(topic, partition, -1, paused);
    }

    public Pair<Long, Boolean> setOffset(String topic, int partition, long offset, boolean paused) {
        synchronized (this) {
            Pair<Long, Boolean> pair = offsets.computeIfAbsent(new TopicPartition(topic, partition), p -> new ImmutablePair<>(offset, paused));
            if (offset == -1) {
                log.debug("Partition " + partition + " : offset = " + pair.getLeft() + ", paused = " + paused);
                Pair<Long, Boolean> pOffset = new ImmutablePair<>(pair.getLeft(), paused);
                offsets.put(new TopicPartition(topic, partition), pOffset);
                return pOffset;
            } else if (!paused && pair.getRight()) {
                throw new IllegalStateException();
            } else if (!pair.getRight() || (paused && pair.getRight() && offset < pair.getLeft())) {
                log.debug("Partition " + partition + " : offset = " + offset + ", paused = " + paused);
                offsets.put(new TopicPartition(topic, partition), new ImmutablePair<>(offset, paused));
            }
            return new ImmutablePair<>(offset, !pair.getRight());
        }
    }

    public CompletionStage<Void> pause(String topic, int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        boolean paused = setOffset(topic, partition, offset, true).getRight();
        if (paused) {
            consumer.pause(Collections.singleton(topicPartition));
            long totalDelay = calculateDelay(topicPartition);
            Uni.createFrom().nullItem().onItem().delayIt().by(Duration.ofMillis(totalDelay)).onItem().transformToUni(o -> resume(topic, partition))
                    .subscribe().with(unused -> {
            });
            log.warn("Consumer partition " + partition + " paused for " + totalDelay + " milliseconds");
        }
        return CompletableFuture.completedFuture(null);
    }

    public Uni<Void> resume(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        if (!topicPartitions.contains(topicPartition)) {
            log.warn("Consumer not consuming from partition " + partition);
            return Uni.createFrom().nullItem();
        }
        long offset = setOffset(topic, partition, false).getLeft();
        consumer.seek(topicPartition, offset);
        consumer.resume(Collections.singleton(topicPartition));
        log.info("Consumer resuming partition " + partition + " from offset " + offset);
        return Uni.createFrom().nullItem();
    }

    private long calculateDelay(TopicPartition partition) {

        Pair<Long, Long> pair = counters.computeIfAbsent(partition, p -> new ImmutablePair<>(0L, Instant.now().toEpochMilli()));
        long counter = pair.getLeft();
        long timestamp = pair.getRight();
        long expected = counter * delay > maxDelay ? timestamp + maxDelay : timestamp + (counter * delay);
        if (counter != 0 && (Instant.now().toEpochMilli() - expected > delay)) {
            log.info("Reset delay");
            counter = 0;
        }
        timestamp = Instant.now().toEpochMilli();
        counter=counter == 0 ? 1 : counter*2;
        counters.put(partition, new ImmutablePair<>(counter, timestamp));
        return counter * delay > maxDelay ? maxDelay : counter * delay;
    }
}
