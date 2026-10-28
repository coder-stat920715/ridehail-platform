package com.ridehail.trip.domain;

import java.math.BigDecimal;

public enum VehicleType {
    BIKE("20", "6"), AUTO("30", "11"), CAB("50", "14");

    private final BigDecimal baseFare;
    private final BigDecimal perKm;

    VehicleType(String baseFare, String perKm) {
        this.baseFare = new BigDecimal(baseFare);
        this.perKm = new BigDecimal(perKm);
    }

    public BigDecimal baseFare() { return baseFare; }
    public BigDecimal perKm() { return perKm; }
}
