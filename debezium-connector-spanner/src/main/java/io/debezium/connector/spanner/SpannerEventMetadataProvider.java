/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

/**
 * An {@link EventMetadataProvider} implementation for Mongodb to extract metrics data from events.
 *
 * @author Chris Cranford
 */
public class SpannerEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(
                                     DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public Map<String, String> getEventSourcePosition(
                                                      DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public String getTransactionId(
                                   DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }
}
