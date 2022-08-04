/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.lang.Thread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.LoggingContext;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support snapshotting and streaming of multiple
 * partitions.
 */
public class SpannerChangeEventSourceCoordinator
        extends ChangeEventSourceCoordinator<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeEventSourceCoordinator.class);
    private final SpannerConnectorConfig config;

    public SpannerChangeEventSourceCoordinator(
                                               Offsets<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> previousOffsets,
                                               ErrorHandler errorHandler,
                                               Class<? extends SourceConnector> connectorType,
                                               SpannerConnectorConfig connectorConfig,
                                               SpannerChangeEventSourceFactory changeEventSourceFactory,
                                               ChangeEventSourceMetricsFactory<SpannerChangeStreamPartition> changeEventSourceMetricsFactory,
                                               EventDispatcher<SpannerChangeStreamPartition, ?> eventDispatcher,
                                               DatabaseSchema<?> schema) {
        super(
                previousOffsets,
                errorHandler,
                connectorType,
                connectorConfig,
                changeEventSourceFactory,
                changeEventSourceMetricsFactory,
                eventDispatcher,
                schema);
        this.config = connectorConfig;
    }

    @Override
    public synchronized void start(
                                   CdcSourceTaskContext taskContext,
                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider) {
        running = true;
        AtomicReference<LoggingContext.PreviousContext> previousLogContext = new AtomicReference<>();
        try {
            this.streamingMetrics = changeEventSourceMetricsFactory.getStreamingMetrics(
                    taskContext, changeEventQueueMetrics, metadataProvider);

            // run the snapshot source on a separate thread so start() won't block
            executor.submit(
                    () -> {
                        try {
                            previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                            streamingMetrics.register();
                            LOGGER.info("Metrics registered");

                            ChangeEventSourceContext context = new ChangeEventSourceContextImpl();
                            LOGGER.info("Context created");

                            executeChangeEventSources(taskContext, previousOffsets, previousLogContext, context);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOGGER.warn("Change event source executor was interrupted", e);
                        }
                        catch (Throwable e) {
                            errorHandler.setProducerThrowable(e);
                        }
                        finally {
                            streamingConnected(false);
                        }
                    });
        }
        catch (Exception e) {
            LOGGER.error("Caught start exception: " + e.toString());
        }
        finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }

    protected void executeChangeEventSources(
                                             CdcSourceTaskContext taskContext,
                                             Offsets<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext,
                                             ChangeEventSourceContext context)
            throws InterruptedException {
        try {
            final SpannerChangeStreamPartition partition = previousOffsets.getTheOnlyPartition();
            final SpannerChangeStreamOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

            LOGGER.info("Starting streaming: " + partition.toString());
            SpannerChangeStreamOffsetContext offsetContext = previousOffset;
            if (previousOffset == null) {
                offsetContext = new SpannerChangeStreamOffsetContext(
                        config, new TransactionContext(), partition.getTaskIndex());
            }

            streamingSource = ((SpannerChangeEventSourceFactory) changeEventSourceFactory)
                    .getStreamingChangeEventSource(offsetContext);
            while (context.isRunning()) {
                try {
                    Thread.sleep(3000);
                }
                catch (Exception e) {
                    // catching the exception
                    LOGGER.info("Caught exception");
                    System.out.println(e);
                }
                ((SpannerStreamingChangeEventSource) streamingSource).executeIteration(context, partition);
            }
            LOGGER.info("Finished streaming");
        }
        catch (Exception e) {
            LOGGER.error("Streaming exception: " + e.toString());
        }
    }

    @Override
    /** Stops this coordinator. */
    public synchronized void stop() throws InterruptedException {
        running = false;

        try {
            // Clear interrupt flag so the graceful termination is always attempted
            Thread.interrupted();
            executor.shutdown();
            boolean isShutdown = executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (!isShutdown) {
                LOGGER.warn("Coordinator didn't stop in the expected time, shutting down executor now");

                // Clear interrupt flag so the forced termination is always attempted
                Thread.interrupted();
                executor.shutdownNow();
                executor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        finally {
            LOGGER.info("Shut down");
        }
    }
}
