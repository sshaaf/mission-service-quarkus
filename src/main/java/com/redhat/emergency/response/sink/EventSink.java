package com.redhat.emergency.response.sink;

import java.time.Instant;
import java.util.UUID;
import javax.enterprise.context.ApplicationScoped;

import com.redhat.emergency.response.model.Mission;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class EventSink {

    private final UnicastProcessor<Pair<String, JsonObject>> processor = UnicastProcessor.create();

    public void missionStarted(Mission mission) {
        processor.onNext(ImmutablePair.of(mission.getIncidentId(),
                messageHeaders(new JsonObject(), "MissionStartedEvent").put("body", JsonObject.mapFrom(mission))));
    }

    @Outgoing("mission-event")
    public Multi<Message<String>> responderEvent() {
        return processor.onItem().apply(this::toMessage);
    }

    private Message<String> toMessage(Pair<String, JsonObject> keyPayloadPair) {
        return KafkaRecord.of(keyPayloadPair.getLeft(), keyPayloadPair.getRight().encode());
    }

    private JsonObject messageHeaders(JsonObject json, String messageType) {

        return json.put("id", UUID.randomUUID().toString())
                .put("invokingService", "MissionService")
                .put("timestamp", Instant.now().toEpochMilli())
                .put("messageType", messageType);
    }

}
