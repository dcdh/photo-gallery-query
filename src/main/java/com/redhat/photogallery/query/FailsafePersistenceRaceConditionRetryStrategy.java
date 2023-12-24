package com.redhat.photogallery.query;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.quarkus.arc.DefaultBean;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@Singleton
@DefaultBean
public final class FailsafePersistenceRaceConditionRetryStrategy implements PersistenceRaceConditionRetryStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FailsafePersistenceRaceConditionRetryStrategy.class);

    private RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
            .handle(FailedToPersistException.class)
            .withDelay(Duration.ofSeconds(1))
            .withMaxRetries(-1)
            .onFailedAttempt(e -> LOG.error("Execution attempt failed", e.getLastException().getCause()))
            .onRetry(e -> LOG.warn("Failure #{}. Retrying.", e.getAttemptCount()))
            .build();

    @Override
    public void execute(final Runnable runnable) {
        LOG.info("Executing using FailsafePersistenceRaceConditionRetryStrategy");
        Failsafe.with(retryPolicy).run(runnable::run);
    }
}
