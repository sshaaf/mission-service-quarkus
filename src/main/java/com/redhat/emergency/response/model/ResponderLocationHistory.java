package com.redhat.emergency.response.model;

import java.math.BigDecimal;

public class ResponderLocationHistory {

    private BigDecimal lat;

    private BigDecimal lon;

    private long timestamp;

    private ResponderLocationHistory() {}

    public ResponderLocationHistory(BigDecimal lat, BigDecimal lon, long timestamp) {
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
    }

    public BigDecimal getLat() {
        return lat;
    }

    public BigDecimal getLon() {
        return lon;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
