package com.redhat.photogallery.query;

public interface PersistenceRaceConditionRetryStrategy {
    void execute(Runnable runnable);
}
