package com.ridehail.location.command;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Latest position per driver (normalised write model, one row per driver).
 * Also the system of record from which the Redis read model can be rebuilt after a flush.
 */
@Entity
@Table(name = "driver_location")
public class DriverLocationEntity {
    @Id
    private String driverId;
    private double lat;
    private double lng;
    private String status;
    private String vehicleType;
    private Instant updatedAt;

    protected DriverLocationEntity() { }

    public String getDriverId() { return driverId; }
    public double getLat() { return lat; }
    public double getLng() { return lng; }
    public String getStatus() { return status; }
    public String getVehicleType() { return vehicleType; }
    public Instant getUpdatedAt() { return updatedAt; }
}
