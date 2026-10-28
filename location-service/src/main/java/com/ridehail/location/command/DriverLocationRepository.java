package com.ridehail.location.command;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface DriverLocationRepository extends JpaRepository<DriverLocationEntity, String> {

    /** Single-statement upsert: no read-before-write on a very hot, write-heavy table. */
    @Modifying
    @Query(nativeQuery = true, value = """
            INSERT INTO driver_location (driver_id, lat, lng, status, vehicle_type, updated_at)
            VALUES (:driverId, :lat, :lng, :status, :vehicleType, :updatedAt)
            ON CONFLICT (driver_id) DO UPDATE SET
              lat = EXCLUDED.lat, lng = EXCLUDED.lng, status = EXCLUDED.status,
              vehicle_type = EXCLUDED.vehicle_type, updated_at = EXCLUDED.updated_at
            """)
    int upsert(@Param("driverId") String driverId, @Param("lat") double lat, @Param("lng") double lng,
               @Param("status") String status, @Param("vehicleType") String vehicleType,
               @Param("updatedAt") Instant updatedAt);
}
