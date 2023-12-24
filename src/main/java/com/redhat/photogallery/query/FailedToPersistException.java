package com.redhat.photogallery.query;

public final class FailedToPersistException extends RuntimeException {
    public FailedToPersistException(final Throwable cause) {
        super(cause);
    }
}
