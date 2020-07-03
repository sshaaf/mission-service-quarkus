package com.redhat.emergency.response.repository;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodeAbsent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonNodePresent;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonPartEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.math.BigDecimal;
import javax.inject.Inject;

import com.redhat.emergency.response.model.Mission;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.vertx.core.json.JsonObject;
import org.hamcrest.Matchers;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

@QuarkusTest
public class MissionRepositoryTest {

    @Inject
    MissionRepository repository;

    @InjectMock
    RemoteCacheManager cacheManager;

    @Mock
    RemoteCache<String, String> missionCache;

    @Mock
    RemoteCacheManagerAdmin cacheManagerAdmin;

    @Captor
    ArgumentCaptor<String> stringCaptor;

    @BeforeEach
    void init() {
        initMocks(this);
        when(cacheManager.administration()).thenReturn(cacheManagerAdmin);
        when(cacheManagerAdmin.<String, String>getOrCreateCache(eq("mission"), any(Configuration.class))).thenReturn(missionCache);
    }

    @Test
    void testAdd() {

        JsonObject json = new JsonObject().put("incidentId", "incident123")
                .put("incidentLat", new BigDecimal("30.12345").doubleValue()).put("incidentLong", new BigDecimal("-70.98765").doubleValue())
                .put("responderId", "responder123")
                .put("responderStartLat", new BigDecimal("31.12345").doubleValue()).put("responderStartLong", new BigDecimal("-71.98765").doubleValue())
                .put("destinationLat", new BigDecimal("32.12345").doubleValue()).put("destinationLong", new BigDecimal("-72.98765").doubleValue());

        Mission mission = json.mapTo(Mission.class);

        repository.add(mission).await().indefinitely();

        verify(missionCache).put(eq(mission.getKey()), stringCaptor.capture());
        String value = stringCaptor.getValue();
        assertThat(value, Matchers.notNullValue());
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
    }



}
