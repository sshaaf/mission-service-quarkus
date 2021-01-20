package com.redhat.emergency.response.sink;

import java.time.OffsetDateTime;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EventSink {

    private static final Logger log = LoggerFactory.getLogger(EventSink.class);

    @Inject
    @Channel("mission-event")
    Emitter<String> missionEventEmitter;

    private final UnicastProcessor<String> missionProcessor = UnicastProcessor.create();

    public Uni<Void> missionStarted(Mission mission) {
        return missionEvent(mission, "MissionStartedEvent");
    }

    public Uni<Void> missionPickedUp(Mission mission) {
        return missionEvent(mission, "MissionPickedUpEvent");
    }

    public Uni<Void> missionCompleted(Mission mission) {
        return missionEvent(mission, "MissionCompletedEvent");
    }

    public Uni<Void> missionEvent(Mission mission, String type) {

        return Uni.createFrom().item(() -> {
            String payload = JsonObject.mapFrom(mission).encode();
            log.debug("Sending message to mission-event channel. Key: " + mission.getIncidentId() + " - Message = " + payload);
            missionEventEmitter.send(toMessage(mission.getIncidentId(), payload, type));
            return null;
        });
    }

    private Message<String> toMessage(String key, String payload, String messageType) {
        log.debug(messageType + ": " + payload);
        OutgoingCloudEventMetadata<String> cloudEventMetadata = OutgoingCloudEventMetadata.<String>builder().withType(messageType)
                .withTimestamp(OffsetDateTime.now().toZonedDateTime()).build();
        return KafkaRecord.of(key, payload).addMetadata(cloudEventMetadata);
    }

}
