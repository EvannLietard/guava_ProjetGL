package com.google.common.math.exception;

public class NegativeValueException extends IllegalArgumentException {
    public NegativeValueException(String role, Object value) {
        super(role + " (" + value + ") must be non-negative");
    }
}
