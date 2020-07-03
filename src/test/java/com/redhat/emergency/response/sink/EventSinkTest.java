package com.redhat.emergency.response.sink;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodeAbsent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import javax.enterprise.inject.Any;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.connectors.InMemoryConnector;
import io.smallrye.reactive.messaging.connectors.InMemorySink;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecord;
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

    @Test
    void testMissionStarted() {

        InMemorySink<String> results = connector.sink("mission-event");

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue());

        Mission mission = json.mapTo(Mission.class);

        eventSink.missionStarted(mission);

        assertThat(results.received().size(), equalTo(1));
        Message<String> message = results.received().get(0);
        assertThat(message, instanceOf(OutgoingKafkaRecord.class));
        String value = message.getPayload();
        String key = ((OutgoingKafkaRecord<String, String>)message).getKey();
        assertThat(key, equalTo("incident123"));
        assertThat(value, jsonNodePresent("id"));
        assertThat(value, jsonPartEquals("messageType", "MissionStartedEvent"));
        assertThat(value, jsonPartEquals("invokingService", "MissionService"));
        assertThat(value, jsonNodePresent("timestamp"));
        assertThat(value, jsonNodePresent("body"));
        assertThat(value, jsonPartEquals("body.incidentId", "incident123"));
        assertThat(value, jsonNodePresent("body.id"));
        assertThat(value, jsonPartEquals("body.id", "${json-unit.regex}[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
        assertThat(value, jsonPartEquals("body.responderId", "responder123"));
        assertThat(value, jsonPartEquals("body.incidentLat", 30.12345));
        assertThat(value, jsonPartEquals("body.incidentLong", -70.98765));
        assertThat(value, jsonPartEquals("body.responderStartLat", 31.12345));
        assertThat(value, jsonPartEquals("body.responderStartLong", -71.98765));
        assertThat(value, jsonPartEquals("body.destinationLat", 32.12345));
        assertThat(value, jsonPartEquals("body.destinationLong", -72.98765));
        assertThat(value, jsonNodePresent("body.steps"));
        assertThat(value, jsonNodeAbsent("body.steps[0]"));
        assertThat(value, jsonNodePresent("body.responderLocationHistory"));
        assertThat(value, jsonNodeAbsent("body.responderLocationHistory[0]"));
    }

}
