/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;

/**
 * Defines the Kafka Connect {@link Schema} functionality associated with a given mongodb
 * collection, and which can be used to send documents that match the schema to Kafka Connect.
 *
 * @author Chris Cranford
 */
public class SpannerDataCollectionSchema implements DataCollectionSchema {

    private final CollectionId id;
    private final Schema keySchema;
    private final Envelope envelopeSchema;
    private final Schema valueSchema;

    public SpannerDataCollectionSchema(
                                       CollectionId id, Schema keySchema, Envelope envelopeSchema, Schema valueSchema) {
        this.id = id;
        this.keySchema = keySchema;
        this.envelopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public DataCollectionId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    public Struct keyFromDataChangeRecord(com.google.cloud.spanner.Struct dataChangeRecordMod) {
        return new Struct(keySchema).put("id", dataChangeRecordMod.getString(0));
    }

    public Struct valueFromDataChangeRecord(
                                            com.google.cloud.spanner.Struct dataChangeRecordMod,
                                            Envelope.Operation operation) {
        Struct value = new Struct(valueSchema);
        final String newJsonStr = dataChangeRecordMod.getString(1);
        final String oldJsonStr = dataChangeRecordMod.getString(2);
        switch (operation) {
            case READ:
            case CREATE:
                value.put(FieldName.AFTER, newJsonStr);
                break;
            case UPDATE:
                value.put(FieldName.AFTER, newJsonStr);
                value.put(FieldName.BEFORE, oldJsonStr);
                break;
            case DELETE:
                value.put(FieldName.BEFORE, oldJsonStr);
                break;
        }
        return value;
    }

    @Override
    public int hashCode() {
        return valueSchema().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof SpannerDataCollectionSchema) {
            SpannerDataCollectionSchema that = (SpannerDataCollectionSchema) obj;
            return Objects.equals(this.keySchema(), that.keySchema())
                    && Objects.equals(this.valueSchema(), that.valueSchema());
        }
        return false;
    }

    @Override
    public String toString() {
        return "{ key : "
                + SchemaUtil.asString(keySchema())
                + ", value : "
                + SchemaUtil.asString(valueSchema())
                + " }";
    }
}
