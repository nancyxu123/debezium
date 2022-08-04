/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

/**
 * The main task executing streaming from SQL Server. Responsible for lifecycle management the
 * streaming code.
 *
 * @author Jiri Pechanec
 */
public class SpannerConnectorTask
        extends BaseSourceTask<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorTask.class);
    private static final String CONTEXT_NAME = "spanner-connector-task";

    private volatile SpannerChangeStreamTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SpannerDbSchema schema;
    private volatile ErrorHandler errorHandler;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public SpannerChangeEventSourceCoordinator start(Configuration config) {
        final Clock clock = Clock.system();

        final SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(config);
        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();

        this.schema = new SpannerDbSchema(structSchema);

        taskContext = new SpannerChangeStreamTaskContext(connectorConfig);

        // Set up the task record queue ...
        // The queue should have poll interval, max batch size, max queues size, max queue size in
        // bytes.
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new SpannerErrorHandler(connectorConfig, queue);

        final SpannerEventMetadataProvider metadataProvider = new SpannerEventMetadataProvider();

        final EventDispatcher<SpannerChangeStreamPartition, CollectionId> dispatcher = new EventDispatcher<SpannerChangeStreamPartition, CollectionId>(
                connectorConfig,
                taskContext.topicSelector(),
                schema,
                queue,
                taskContext.filters().collectionFilter()::test,
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        SpannerChangeEventSourceCoordinator coordinator = new SpannerChangeEventSourceCoordinator(
                Offsets.of(
                        Collections.singletonMap(
                                new SpannerChangeStreamPartition(
                                        connectorConfig.getTaskId(), connectorConfig.getNumTasks()),
                                null)),
                errorHandler,
                SpannerConnector.class,
                connectorConfig,
                new SpannerChangeEventSourceFactory(
                        connectorConfig, errorHandler, dispatcher, clock, schema),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
        // if (!sourceRecords.isEmpty()) {
        // long recordMillis = sourceRecords.get(0).timestamp();
        // LOGGER.info(
        // "Outputting record: "
        // + sourceRecords.get(0)
        // + " at timestamp: "
        // + Instant.now().toString()
        // + " with discrepancy: "
        // + (Instant.now().toEpochMilli() - recordMillis));
        // }

        return sourceRecords;
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SpannerConnectorConfig.ALL_FIELDS;
    }
}
