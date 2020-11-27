package com.yashoid.yashodb.exception;

public class ValueTypeException extends Exception {

    public ValueTypeException(Throwable cause) {
        super("Value doesn't match with requested type.", cause);
    }

}
