/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Optional;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

/*
 * The main task executing streaming from SQL Server. Responsible for lifecycle management the
 * streaming code.
 */
public class SpannerChangeEventSourceFactory
        implements ChangeEventSourceFactory<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> {

    private final SpannerConnectorConfig configuration;
    private final EventDispatcher<SpannerChangeStreamPartition, CollectionId> dispatcher;
    private final Clock clock;
    private final SpannerDbSchema schema;
    private final ErrorHandler errorHandler;

    public SpannerChangeEventSourceFactory(
                                           SpannerConnectorConfig configuration,
                                           ErrorHandler errorHandler,
                                           EventDispatcher<SpannerChangeStreamPartition, CollectionId> dispatcher,
                                           Clock clock,
                                           SpannerDbSchema schema) {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> getSnapshotChangeEventSource(
                                                                                                                                  SnapshotProgressListener<SpannerChangeStreamPartition> snapshotProgressListener) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    public StreamingChangeEventSource<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> getStreamingChangeEventSource(SpannerChangeStreamOffsetContext offsetContext) {
        return new SpannerStreamingChangeEventSource(configuration, dispatcher, offsetContext, clock);
    }

    @Override
    public StreamingChangeEventSource<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> getStreamingChangeEventSource() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<SpannerChangeStreamPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                                            SpannerChangeStreamOffsetContext offsetContext,
                                                                                                                                                            SnapshotProgressListener<SpannerChangeStreamPartition> snapshotProgressListener,
                                                                                                                                                            DataChangeEventListener<SpannerChangeStreamPartition> dataChangeEventListener) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }
}
