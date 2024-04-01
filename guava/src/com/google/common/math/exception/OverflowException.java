package com.google.common.math.exception;

public class OverflowException extends ArithmeticException {
    public OverflowException(String methodName, long a, long b) {
        super("overflow: " + methodName + "(" + a + ", " + b + ")");
    }
}
