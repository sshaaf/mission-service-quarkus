package com.redhat.emergency.response.sink;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodeAbsent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.math.BigDecimal;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.ce.impl.DefaultOutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class EventSinkTest {

    @Inject
    EventSink eventSink;

    @Inject @Any
    InMemoryConnector connector;

    @BeforeEach
    void init() {
        connector.sink("mission-event").clear();
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testMissionStarted() {

        InMemorySink<String> results = connector.sink("mission-event");

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "CREATED");

        Mission mission = json.mapTo(Mission.class);

        eventSink.missionStarted(mission).await().indefinitely();

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("incidentId", "incident123"));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("id", "${json-unit.regex}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
        assertThat(value, jsonPartEquals("responderId", "responder123"));
        assertThat(value, jsonPartEquals("incidentLat", 30.12345));
        assertThat(value, jsonPartEquals("incidentLong", -70.98765));
        assertThat(value, jsonPartEquals("responderStartLat", 31.12345));
        assertThat(value, jsonPartEquals("responderStartLong", -71.98765));
        assertThat(value, jsonPartEquals("destinationLat", 32.12345));
        assertThat(value, jsonPartEquals("destinationLong", -72.98765));
        assertThat(value, jsonNodePresent("steps"));
        assertThat(value, jsonNodeAbsent("steps[0]"));
        assertThat(value, jsonNodePresent("responderLocationHistory"));
        assertThat(value, jsonNodeAbsent("responderLocationHistory[0]"));
        assertThat(value, jsonPartEquals("status", "CREATED"));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo("incident123"));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("MissionStartedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testMissionPickedUp() {

        InMemorySink<String> results = connector.sink("mission-event");

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "PICKEDUP");

        Mission mission = json.mapTo(Mission.class);

        eventSink.missionPickedUp(mission).await().indefinitely();

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("incidentId", "incident123"));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("id", "${json-unit.regex}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
        assertThat(value, jsonPartEquals("responderId", "responder123"));
        assertThat(value, jsonPartEquals("incidentLat", 30.12345));
        assertThat(value, jsonPartEquals("incidentLong", -70.98765));
        assertThat(value, jsonPartEquals("responderStartLat", 31.12345));
        assertThat(value, jsonPartEquals("responderStartLong", -71.98765));
        assertThat(value, jsonPartEquals("destinationLat", 32.12345));
        assertThat(value, jsonPartEquals("destinationLong", -72.98765));
        assertThat(value, jsonNodePresent("steps"));
        assertThat(value, jsonNodeAbsent("steps[0]"));
        assertThat(value, jsonNodePresent("responderLocationHistory"));
        assertThat(value, jsonNodeAbsent("responderLocationHistory[0]"));
        assertThat(value, jsonPartEquals("status", "PICKEDUP"));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo("incident123"));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("MissionPickedUpEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));

    }

    @SuppressWarnings("rawtypes")
    @Test
    void testMissionCompleted() {

        InMemorySink<String> results = connector.sink("mission-event");

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue())
                .put("status", "COMPLETED");

        Mission mission = json.mapTo(Mission.class);

        eventSink.missionCompleted(mission).await().indefinitely();

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        String value = message.getPayload();
        assertThat(value, jsonPartEquals("incidentId", "incident123"));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("id", "${json-unit.regex}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
        assertThat(value, jsonPartEquals("responderId", "responder123"));
        assertThat(value, jsonPartEquals("incidentLat", 30.12345));
        assertThat(value, jsonPartEquals("incidentLong", -70.98765));
        assertThat(value, jsonPartEquals("responderStartLat", 31.12345));
        assertThat(value, jsonPartEquals("responderStartLong", -71.98765));
        assertThat(value, jsonPartEquals("destinationLat", 32.12345));
        assertThat(value, jsonPartEquals("destinationLong", -72.98765));
        assertThat(value, jsonNodePresent("steps"));
        assertThat(value, jsonNodeAbsent("steps[0]"));
        assertThat(value, jsonNodePresent("responderLocationHistory"));
        assertThat(value, jsonNodeAbsent("responderLocationHistory[0]"));
        assertThat(value, jsonPartEquals("status", "COMPLETED"));
        OutgoingKafkaRecordMetadata outgoingKafkaRecordMetadata = null;
        DefaultOutgoingCloudEventMetadata outgoingCloudEventMetadata = null;
        for (Object next : message.getMetadata()) {
            if (next instanceof OutgoingKafkaRecordMetadata) {
                outgoingKafkaRecordMetadata = (OutgoingKafkaRecordMetadata) next;
            } else if (next instanceof DefaultOutgoingCloudEventMetadata) {
                outgoingCloudEventMetadata = (DefaultOutgoingCloudEventMetadata) next;
            }
        }
        assertThat(outgoingCloudEventMetadata, notNullValue());
        assertThat(outgoingKafkaRecordMetadata, notNullValue());
        String key = (String) outgoingKafkaRecordMetadata.getKey();
        assertThat(key, equalTo("incident123"));
        assertThat(outgoingCloudEventMetadata.getId(), notNullValue());
        assertThat(outgoingCloudEventMetadata.getSpecVersion(), equalTo("1.0"));
        assertThat(outgoingCloudEventMetadata.getType(), equalTo("MissionCompletedEvent"));
        assertThat(outgoingCloudEventMetadata.getTimeStamp().isPresent(), is(true));
    }

}
