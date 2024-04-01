package com.google.common.math.exception;
public class NotPositiveException extends IllegalArgumentException {
    public NotPositiveException(String role, Object value) {
        super(role + " (" + value + ") must be > 0");
    }
}
