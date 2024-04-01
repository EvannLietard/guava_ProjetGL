package com.google.common.math.exception;

public class RoundingNecessaryException extends ArithmeticException {
    public RoundingNecessaryException() {
        super("Mode was UNNECESSARY, but rounding was necessary");
    }
}
