package com.redhat.emergency.response.source;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

public class MessageWithAck<T> implements Message<T> {

    private T payload;

    private boolean acked = false;

    private IncomingKafkaRecordMetadata<String, T> metadata;

    private MessageWithAck() {}

    static <T> MessageWithAck<T> of(T payload, String topic, int partition,long offset) {
        MessageWithAck<T> m = new MessageWithAck<>();
        m.payload = payload;
        m.metadata = new IncomingKafkaRecordMetadata<>(new io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord<>(new KafkaConsumerRecord<String, T>(topic, partition, offset)));
        return m;
    }

    static <T> MessageWithAck<T> of(T payload) {
        MessageWithAck<T> m = new MessageWithAck<>();
        m.payload = payload;
        return m;
    }

    @Override
    public CompletionStage<Void> ack() {
        acked = true;
        return getAck().get();
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public <M> Optional<M> getMetadata(Class<? extends M> clazz) {
        return (Optional<M>) Optional.of(metadata);
    }

    public boolean acked() {
        return acked;
    }
}
