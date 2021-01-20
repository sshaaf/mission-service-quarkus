package com.redhat.emergency.response.source;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.Optional;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySource;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ResponderUpdateLocationSourceTest {

    @InjectMock
    MissionRepository repository;

    @InjectMock
    EventSink eventSink;

    @Inject
    @Any
    InMemoryConnector connector;

    InMemorySource<Message<String>> source;

    @BeforeEach
    void init() {
        openMocks(this);
        source = connector.source("responder-location-update");
    }

    @Test
    void testProcessMessageMoving() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"incidentId\": \"incident\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        Mission mission = new Mission();
        when(repository.get("incident:responder")).thenReturn(Uni.createFrom().item(Optional.of(mission)));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository).get("incident:responder");
        assertThat(mission.getResponderLocationHistory().size(), equalTo(1));
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository).add(mission);
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessMessagePickedUp() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"incidentId\": \"incident\",\n" +
                "    \"status\": \"PICKEDUP\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        Mission mission = new Mission();
        when(repository.get("incident:responder")).thenReturn(Uni.createFrom().item(Optional.of(mission)));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        when(eventSink.missionPickedUp(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository).get("incident:responder");
        assertThat(mission.getResponderLocationHistory().size(), equalTo(1));
        verify(eventSink).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository).add(mission);
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessMessageDropped() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"incidentId\": \"incident\",\n" +
                "    \"status\": \"DROPPED\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        Mission mission = new Mission();
        when(repository.get("incident:responder")).thenReturn(Uni.createFrom().item(Optional.of(mission)));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        when(eventSink.missionCompleted(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository).get("incident:responder");
        assertThat(mission.getResponderLocationHistory().size(), equalTo(1));
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink).missionCompleted(any());
        verify(repository).add(mission);
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessNotACloudEvent() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, false, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository, never()).get(any());
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessWrongMessageType() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "WrongMessageType");
        source.send(message);

        verify(repository, never()).get(any());
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessWrongDataContentType() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/avro", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository, never()).get(any());
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessNoDataContentType() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, null, "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository, never()).get(any());
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessMessageMissingFields() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository, never()).get(any());
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessMessageMissionNotFoundInCache() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"incidentId\": \"incident\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        when(repository.get("incident:responder")).thenReturn(Uni.createFrom().item(Optional.empty()));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository).get("incident:responder");
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }

    @Test
    void testProcessMessageGetMissionThrowsException() {

        String payload = "{\n" +
                "    \"responderId\": \"responder\",\n" +
                "    \"missionId\": \"mission\",\n" +
                "    \"incidentId\": \"incident\",\n" +
                "    \"status\": \"MOVING\",\n" +
                "    \"lat\": 30.12345,\n" +
                "    \"lon\": -78.98765,\n" +
                "    \"human\": false,\n" +
                "    \"continue\": true\n" +
                "}";

        when(repository.get("incident:responder")).thenThrow(new RuntimeException("Exception!"));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        MessageWithAck<String> message = MessageWithAck.of(payload, "topic", 10, 20, true, "application/json", "ResponderLocationUpdatedEvent");
        source.send(message);

        verify(repository).get("incident:responder");
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any(Mission.class));
        assertThat(message.acked(), is(true));
    }
}
