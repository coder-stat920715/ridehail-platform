package com.ridehail.location.query;

import com.ridehail.location.domain.VehicleType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Internal read API (service-to-service only; the gateway denies /internal/**).
 * Read-only: never touches PostgreSQL, so heavy matching traffic can't starve location writes.
 */
@RestController
@RequestMapping("/internal/v1")
public class DriverQueryController {

    private final DriverGeoIndex index;
    private final SurgePricingService surge;

    public DriverQueryController(DriverGeoIndex index, SurgePricingService surge) {
        this.index = index;
        this.surge = surge;
    }

    @GetMapping("/drivers/nearby")
    public List<NearbyDriver> nearby(
            @RequestParam @DecimalMin("-90") @DecimalMax("90") double lat,
            @RequestParam @DecimalMin("-180") @DecimalMax("180") double lng,
            @RequestParam(defaultValue = "3") @DecimalMin("0.1") @DecimalMax("10") double radiusKm,
            @RequestParam(defaultValue = "CAB") VehicleType vehicleType,
            @RequestParam(defaultValue = "10") @Min(1) @Max(50) int limit) {
        return index.nearby(lat, lng, radiusKm, vehicleType, limit);
    }

    @GetMapping("/drivers/{driverId}")
    public ResponseEntity<DriverContext> context(@PathVariable String driverId) {
        return index.context(driverId).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/surge")
    public SurgeQuote surge(@RequestParam @DecimalMin("-90") @DecimalMax("90") double lat,
                            @RequestParam @DecimalMin("-180") @DecimalMax("180") double lng) {
        return surge.quote(lat, lng);
    }
}
