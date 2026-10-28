package com.ridehail.trip.domain;

public class IllegalTripStateException extends RuntimeException {
    public IllegalTripStateException(String message) { super(message); }
}
