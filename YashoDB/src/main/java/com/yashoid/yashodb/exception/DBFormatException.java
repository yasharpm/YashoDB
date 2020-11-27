package com.yashoid.yashodb.exception;

public class DBFormatException extends DBException {

    public DBFormatException() {
        super("Bad database format.");
    }

    public DBFormatException(String message) {
        super(message);
    }

}
