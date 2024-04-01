package com.google.common.math.exception;

import java.math.RoundingMode;

public class RoundingOutOfRangeException extends ArithmeticException{
    public RoundingOutOfRangeException(double input, RoundingMode mode) {
        super("Rounded value is out of range for input " + input + " and rounding mode " + mode);
    }
}
