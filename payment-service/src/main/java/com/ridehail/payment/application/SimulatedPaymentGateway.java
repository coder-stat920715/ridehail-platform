package com.ridehail.payment.application;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.UUID;

/**
 * Stand-in for a real PSP (Razorpay/Stripe/...). Deterministic rules so failure paths are testable:
 *
 *   rider name starts with "FAIL"    -> authorisation (hold) is DECLINED   => saga compensation
 *   rider name starts with "CAPFAIL" -> hold succeeds, CAPTURE is rejected => forward recovery path
 *   anything else                    -> approved
 */
@Component
public class SimulatedPaymentGateway {

    public record Result(boolean approved, String reference, String reason) {
        static Result ok(String ref) { return new Result(true, ref, null); }
        static Result declined(String reason) { return new Result(false, null, reason); }
    }

    public Result authorize(String riderName, BigDecimal amount) {
        if (startsWith(riderName, "FAIL")) return Result.declined("CARD_DECLINED (simulated gateway: name starts with FAIL)");
        return Result.ok("hold-" + UUID.randomUUID());
    }

    public Result capture(String riderName, BigDecimal amount) {
        if (startsWith(riderName, "CAPFAIL")) return Result.declined("CAPTURE_REJECTED (simulated gateway: name starts with CAPFAIL)");
        return Result.ok("cap-" + UUID.randomUUID());
    }

    private static boolean startsWith(String name, String prefix) {
        return name != null && name.toUpperCase(Locale.ROOT).startsWith(prefix);
    }
}
