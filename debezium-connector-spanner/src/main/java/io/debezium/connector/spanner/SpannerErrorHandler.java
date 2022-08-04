/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for Spanner.
 *
 * @author John Graf
 */
public class SpannerErrorHandler extends ErrorHandler {

    public SpannerErrorHandler(SpannerConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(SpannerConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof org.apache.kafka.connect.errors.ConnectException) {
            Throwable cause = throwable.getCause();
            while ((cause != null) && (cause != throwable)) {
                if (cause instanceof com.google.api.gax.rpc.UnavailableException) {
                    return true;
                }
                else {
                    cause = cause.getCause();
                }
            }
        }

        return false;
    }
}
