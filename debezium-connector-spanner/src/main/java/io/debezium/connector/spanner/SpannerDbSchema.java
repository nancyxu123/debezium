/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;

/**
 * @author Chris Cranford
 */
@ThreadSafe
public class SpannerDbSchema implements DatabaseSchema<CollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerDbSchema.class);

    private final ConcurrentMap<CollectionId, SpannerDataCollectionSchema> collections = new ConcurrentHashMap<>();

    private final Schema sourceSchema;

    public SpannerDbSchema(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    @Override
    public void close() {
    }

    @Override
    public DataCollectionSchema schemaFor(CollectionId collectionId) {
        return collections.computeIfAbsent(
                collectionId,
                id -> {
                    final Schema keySchema = SchemaBuilder.struct()
                            .name(collectionId.identifier() + ".Key")
                            .field("id", Schema.STRING_SCHEMA)
                            .build();

                    final Schema valueSchema = SchemaBuilder.struct()
                            .name(collectionId.identifier())
                            .field(FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA)
                            .field(FieldName.BEFORE, Schema.OPTIONAL_STRING_SCHEMA)
                            .field(FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                            // Change Streams field
                            .field(FieldName.SOURCE, sourceSchema)
                            .field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                            .field(FieldName.TRANSACTION, Schema.OPTIONAL_STRING_SCHEMA)
                            .build();

                    final Envelope envelope = Envelope.fromSchema(valueSchema);

                    return new SpannerDataCollectionSchema(id, keySchema, envelope, valueSchema);
                });
    }

    @Override
    public boolean tableInformationComplete() {
        // Mongo does not support HistonizedDatabaseSchema - so no tables are recovered
        return false;
    }

    @Override
    public void assureNonEmptySchema() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public boolean isHistorized() {
        return false;
    }
}
