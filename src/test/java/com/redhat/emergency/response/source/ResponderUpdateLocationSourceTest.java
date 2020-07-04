package com.redhat.emergency.response.source;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import com.redhat.emergency.response.model.ResponderLocationHistory;
import com.redhat.emergency.response.repository.MissionRepository;
import com.redhat.emergency.response.sink.EventSink;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ResponderUpdateLocationSourceTest {

    @Inject
    ResponderUpdateLocationSource source;

    @InjectMock
    MissionRepository repository;

    @InjectMock
    EventSink eventSink;

    private boolean messageAck = false;

    @BeforeEach
    void init() {
        initMocks(this);
        messageAck = false;
    }

    @Test
    void testProcessMessage() {

        String payload = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        String m = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission = Json.decodeValue(m, Mission.class);

        when(repository.get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64")).thenReturn(Optional.of(mission));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        Uni<CompletionStage<Void>> uni = source.process(toRecord("incident12364", payload));
        uni.await().indefinitely();

        assertThat(messageAck, equalTo(true));
        assertThat(mission.getResponderLocationHistory().size(), equalTo(2));
        ResponderLocationHistory rlh = mission.getResponderLocationHistory().get(1);
        assertThat(rlh.getLat(), equalTo(new BigDecimal("34.1701")));
        assertThat(rlh.getLon(), equalTo(new BigDecimal("-77.9482")));
        verify(repository).get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64");
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository).add(mission);
    }

    @Test
    void testProcessMessagePickedUp() {

        String payload = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"PICKEDUP\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        String m = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission = Json.decodeValue(m, Mission.class);

        when(repository.get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64")).thenReturn(Optional.of(mission));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));
        when(eventSink.missionPickedUp(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        Uni<CompletionStage<Void>> uni = source.process(toRecord("incident12364", payload));
        uni.await().indefinitely();

        assertThat(messageAck, equalTo(true));
        assertThat(mission.getResponderLocationHistory().size(), equalTo(2));
        ResponderLocationHistory rlh = mission.getResponderLocationHistory().get(1);
        assertThat(rlh.getLat(), equalTo(new BigDecimal("34.1701")));
        assertThat(rlh.getLon(), equalTo(new BigDecimal("-77.9482")));
        verify(repository).get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64");
        verify(eventSink).missionPickedUp(mission);
        verify(eventSink, never()).missionCompleted(any());
        verify(repository).add(mission);
    }

    @Test
    void testProcessMessageDropped() {

        String payload = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"DROPPED\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"human\": false,\n" +
                "  \"continue\": true\n" +
                "}";

        String m = "{\"id\":\"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\"incidentId\":\"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\"," +
                "\"responderId\":\"64\",\"responderStartLat\":\"40.12345\",\"responderStartLong\":\"-80.98765\"," +
                "\"incidentLat\":\"30.12345\",\"incidentLong\":\"-70.98765\"," +
                "\"destinationLat\":\"50.12345\",\"destinationLong\":\"-90.98765\"," +
                "\"responderLocationHistory\":[{\"lat\":30.78452,\"lon\":-70.85252,\"timestamp\":1593872667576}]," +
                "\"steps\":[],\"status\":\"CREATED\"}";
        Mission mission = Json.decodeValue(m, Mission.class);

        when(repository.get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64")).thenReturn(Optional.of(mission));
        when(repository.add(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));
        when(eventSink.missionCompleted(any(Mission.class))).thenReturn(Uni.createFrom().emitter(emitter -> emitter.complete(null)));

        Uni<CompletionStage<Void>> uni = source.process(toRecord("incident12364", payload));
        uni.await().indefinitely();

        assertThat(messageAck, equalTo(true));
        assertThat(mission.getResponderLocationHistory().size(), equalTo(2));
        ResponderLocationHistory rlh = mission.getResponderLocationHistory().get(1);
        assertThat(rlh.getLat(), equalTo(new BigDecimal("34.1701")));
        assertThat(rlh.getLon(), equalTo(new BigDecimal("-77.9482")));
        verify(repository).get("5d9b2d3a-136f-414f-96ba-1b2a445fee5d:64");
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink).missionCompleted(mission);
        verify(repository).add(mission);
    }

    @Test
    void testProcessMessageMissingFields() {

        String payload = "{\n" +
                "  \"responderId\": \"64\",\n" +
                "  \"missionId\": \"f5a9bc5e-408c-4f86-8592-6f67bb73c5fd\",\n" +
                "  \"incidentId\": \"5d9b2d3a-136f-414f-96ba-1b2a445fee5d\",\n" +
                "  \"status\": \"MOVING\",\n" +
                "  \"lat\": 34.1701,\n" +
                "  \"lon\": -77.9482,\n" +
                "  \"continue\": true\n" +
                "}";

        Uni<CompletionStage<Void>> uni = source.process(toRecord("incident12364", payload));
        uni.await().indefinitely();

        assertThat(messageAck, equalTo(true));
        verify(repository, never()).get(any(String.class));
        verify(eventSink, never()).missionPickedUp(any());
        verify(eventSink, never()).missionCompleted(any());
        verify(repository, never()).add(any());
    }

    private IncomingKafkaRecord<String, String> toRecord(String key, String payload) {

        ResponderUpdateLocationSourceTest.MockKafkaConsumer<String, String> mc = new ResponderUpdateLocationSourceTest.MockKafkaConsumer<>();
        KafkaConsumer<String, String> c = new KafkaConsumer<>(mc);
        ConsumerRecord<String, String> cr = new ConsumerRecord<>("topic", 1, 100, key, payload);
        KafkaConsumerRecord<String, String> kcr = new KafkaConsumerRecord<>(new KafkaConsumerRecordImpl<>(cr));
        return new IncomingKafkaRecord<>(c, kcr);
    }

    private class MockKafkaConsumer<K, V> extends KafkaConsumerImpl<K, V> {

        public MockKafkaConsumer() {
            super(new KafkaReadStreamImpl<K, V>(null, null));
        }

        public MockKafkaConsumer(KafkaReadStream<K, V> stream) {
            super(stream);
        }

        @Override
        public void commit(Handler<AsyncResult<Void>> completionHandler) {
            ResponderUpdateLocationSourceTest.this.messageAck = true;

            Promise<Void> future = Promise.promise();
            future.future().onComplete(completionHandler);
            future.complete(null);
        }
    }

}
