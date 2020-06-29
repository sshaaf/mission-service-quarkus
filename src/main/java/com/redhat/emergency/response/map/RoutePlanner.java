package com.redhat.emergency.response.map;

import java.util.ArrayList;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;

import com.redhat.emergency.response.model.Location;
import com.redhat.emergency.response.model.MissionStep;

@ApplicationScoped
public class RoutePlanner {

    public List<MissionStep> getDirections(Location origin, Location destination, Location waypoint) {

        return new ArrayList<>();
    }

}
