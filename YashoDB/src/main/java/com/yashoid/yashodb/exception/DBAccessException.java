package com.yashoid.yashodb.exception;

public class DBAccessException extends DBException {

    public DBAccessException(Throwable cause) {
        super("Failed to read from or write into the database file.", cause);
    }

}
