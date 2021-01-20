package com.redhat.emergency.response.source;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.map.RoutePlanner;
import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.model.MissionStatus;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MissionCommandSource {

    private static final Logger log = LoggerFactory.getLogger(MissionCommandSource.class);

    static final String CREATE_MISSION_COMMAND = "CreateMissionCommand";
    static final String[] ACCEPTED_MESSAGE_TYPES = {CREATE_MISSION_COMMAND};

    @Inject
    RoutePlanner routePlanner;

    @Inject
    MissionRepository repository;

    @Inject
    EventSink eventSink;

    @Inject
    MissionSourceRebalanceListener rebalanceListener;

    @Incoming("mission-command")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<CompletionStage<Void>> process(Message<String> missionCommandMessage) {

        return Uni.createFrom().item(missionCommandMessage)
                .onItem().transform(mcm -> {
                    IncomingKafkaRecordMetadata<String, String> metadata = metadata(mcm);
                    if (metadata != null) {
                        rebalanceListener.setOffset(metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                    }
                    return mcm;
                })
                .onItem().transform(mcm -> accept(missionCommandMessage))
                .onItem().transform(o -> o.flatMap(this::validate).orElseThrow(MessageIgnoredException::new))
                .onItem().transform(m -> m.status(MissionStatus.CREATED))
                .onItem().transformToUni(this::addRoute)
                .onItem().transformToUni(this::addToRepositoryAsync)
                .onItem().transformToUni(this::publishMissionStartedEventAsync)
                .onItem().transform(m -> missionCommandMessage.ack())
                .onFailure(MessageIgnoredException.class).recoverWithItem(t -> missionCommandMessage.ack())
                .onFailure().recoverWithItem(t -> {
                    log.error(t.getMessage(), t);
                    IncomingKafkaRecordMetadata<String, String> metadata = metadata(missionCommandMessage);
                    if (metadata != null) {
                        return rebalanceListener.pause(metadata.getTopic(), metadata.getPartition(), metadata.getOffset());
                    } else {
                        log.warn("Not able to extract metadata from message. Acking message");
                        return missionCommandMessage.ack();
                    }
                });
    }

    private Uni<Mission> addRoute(Mission mission) {
        return routePlanner.getDirections(mission.responderLocation(), mission.destinationLocation(), mission.incidentLocation())
                .map(missionSteps -> {
                    mission.getSteps().addAll(missionSteps);
                    return mission;
                });
    }

    private Uni<Mission> addToRepositoryAsync(Mission mission) {
        return repository.add(mission).map(v -> mission);
    }

    private Uni<Mission> publishMissionStartedEventAsync(Mission mission) {
        return eventSink.missionStarted(mission).map(v -> mission);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Optional<String> accept(Message<String> message) {

        Optional<IncomingCloudEventMetadata> metadata = message.getMetadata(IncomingCloudEventMetadata.class);
        if (metadata.isEmpty()) {
            log.warn("Incoming message is not a CloudEvent");
            return Optional.empty();
        }
        IncomingCloudEventMetadata<String> cloudEventMetadata = metadata.get();
        String dataContentType = cloudEventMetadata.getDataContentType().orElse("");
        if (!dataContentType.equalsIgnoreCase("application/json")) {
            log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
            return Optional.empty();
        }
        String type = cloudEventMetadata.getType();
        if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(type))) {
            log.debug("CloudEvent with type '" + type + "' is ignored");
            return Optional.empty();
        }
        return Optional.of(message.getPayload());
    }

    private Optional<Mission> validate(String jsonAsString) {
        try {
            JsonObject json = new JsonObject(jsonAsString);
            Optional<Mission> mission = Optional.of(json.mapTo(Mission.class))
                    .filter(m -> m.getIncidentId() != null && !(m.getIncidentId().isBlank()))
                    .filter(m -> m.getResponderId() != null && !(m.getResponderId().isBlank()))
                    .filter(m -> m.getIncidentLat() != null && m.getIncidentLong() != null)
                    .filter(m -> m.getResponderStartLat() != null && m.getResponderStartLong() != null)
                    .filter(m -> m.getDestinationLat() != null && m.getDestinationLong() != null);
            if (mission.isEmpty()) {
                log.warn("Missing data in Mission object. Ignoring.");
            }
            return mission;
        } catch (Exception e) {
            log.error("Exception when deserializing message body into Mission object:", e);
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private IncomingKafkaRecordMetadata<String, String> metadata(Message<String> missionCommandMessage) {
        return missionCommandMessage.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
    }

}
