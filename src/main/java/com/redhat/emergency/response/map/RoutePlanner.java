package com.redhat.emergency.response.map;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;

import com.mapbox.api.directions.v5.DirectionsCriteria;
import com.mapbox.api.directions.v5.MapboxDirections;
import com.mapbox.api.directions.v5.models.DirectionsResponse;
import com.mapbox.api.directions.v5.models.RouteLeg;
import com.mapbox.core.constants.Constants;
import com.mapbox.geojson.Point;
import com.redhat.emergency.response.model.Location;
import com.redhat.emergency.response.model.MissionStep;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Response;

@ApplicationScoped
public class RoutePlanner {

    private static final Logger log = LoggerFactory.getLogger(RoutePlanner.class);

    @ConfigProperty(name = "mapbox.token")
    String accessToken;

    @ConfigProperty(name = "mapbox.url", defaultValue = Constants.BASE_API_URL)
    String mapboxUrl;

    public Uni<List<MissionStep>> getDirections(Location origin, Location destination, Location waypoint) {
        return Uni.createFrom().item(() -> getDirectionsInternal(origin, destination, waypoint)).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private List<MissionStep> getDirectionsInternal(Location origin, Location destination, Location waypoint) {

        MapboxDirections request = MapboxDirections.builder()
                .baseUrl(mapboxUrl)
                .accessToken(accessToken)
                .origin(Point.fromLngLat(origin.getLongitude().doubleValue(), origin.getLatitude().doubleValue()))
                .destination(Point.fromLngLat(destination.getLongitude().doubleValue(), destination.getLatitude().doubleValue()))
                .addWaypoint(Point.fromLngLat(waypoint.getLongitude().doubleValue(), waypoint.getLatitude().doubleValue()))
                .overview(DirectionsCriteria.OVERVIEW_FULL)
                .profile(DirectionsCriteria.PROFILE_DRIVING)
                .steps(true)
                .build();
        List<MissionStep> missionSteps = new ArrayList<>();
        try {
            Response<DirectionsResponse> response = request.executeCall();

            if (response.body() == null || response.body().routes().isEmpty()) {
                log.warn("No routes found; check access token, rights and coordinates. Origin: " + origin + "; Destination: " + destination + "; Waypoint: " + waypoint);
                throw new RoutePlannerException("No route found");
            } else {
                Optional<List<RouteLeg>> legs = Optional.ofNullable(response.body().routes().get(0).legs());
                legs.orElse(Collections.emptyList()).stream().flatMap(r -> Optional.ofNullable(r.steps()).orElse(Collections.emptyList()).stream())
                .map(l -> {
                    Point p = l.maneuver().location();
                    MissionStep.Builder builder = MissionStep.builder(BigDecimal.valueOf(p.latitude()).setScale(4, RoundingMode.HALF_UP),
                            BigDecimal.valueOf(p.longitude()).setScale(4, RoundingMode.HALF_UP));
                    if ("arrive".equalsIgnoreCase(l.maneuver().type())) {
                        Optional<MissionStep> step = missionSteps.stream().filter(MissionStep::isWayPoint).findFirst();
                        if (step.isEmpty()) {
                            builder.wayPoint(true);
                        } else {
                            builder.destination(true);
                        }
                    }
                    return builder.build();
                }).forEach(missionSteps::add);
            }
        } catch (IOException e) {
            log.error("Exception while calling MapBox API", e);
            throw new RoutePlannerException(e.getMessage(), e);
        }
        return missionSteps;
    }

}
