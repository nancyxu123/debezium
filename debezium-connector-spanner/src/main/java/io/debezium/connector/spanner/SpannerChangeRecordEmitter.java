/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.annotation.Immutable;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.util.Clock;

/**
 * Emits change data based on a change stream change.
 *
 * @author Jiri Pechanec
 */
public class SpannerChangeRecordEmitter
        extends AbstractChangeRecordEmitter<SpannerChangeStreamPartition, SpannerDataCollectionSchema> {

    @Immutable
    private static final Map<String, Operation> OPERATION_LITERALS;

    private com.google.cloud.spanner.Struct dataChangeRecordMod;
    private com.google.cloud.spanner.Struct dataChangeRecord;
    private String partitionToken;

    static {
        Map<String, Operation> literals = new HashMap<>();

        literals.put("INSERT", Operation.CREATE);
        literals.put("UPDATE", Operation.UPDATE);
        literals.put("DELETE", Operation.DELETE);

        OPERATION_LITERALS = Collections.unmodifiableMap(literals);
    }

    public SpannerChangeRecordEmitter(
                                      SpannerChangeStreamPartition partition,
                                      SpannerChangeStreamOffsetContext offsetContext,
                                      com.google.cloud.spanner.Struct dataChangeRecord,
                                      com.google.cloud.spanner.Struct dataChangeRecordMod,
                                      String partitionToken,
                                      Clock clock) {
        super(partition, offsetContext, clock);
        this.dataChangeRecord = dataChangeRecord;
        this.dataChangeRecordMod = dataChangeRecordMod;
        this.partitionToken = partitionToken;
    }

    @Override
    public Operation getOperation() {
        return OPERATION_LITERALS.get(dataChangeRecord.getString(7));
    }

    @Override
    protected void emitReadRecord(
                                  Receiver<SpannerChangeStreamPartition> receiver, SpannerDataCollectionSchema schema)
            throws InterruptedException {
        // TODO Handled in MongoDbChangeSnapshotOplogRecordEmitter
        // It might be worthy haveing three classes - one for Snapshot, one for oplog and one for change
        // streams
    }

    @Override
    protected void emitCreateRecord(
                                    Receiver<SpannerChangeStreamPartition> receiver, SpannerDataCollectionSchema schema)
            throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitUpdateRecord(
                                    Receiver<SpannerChangeStreamPartition> receiver, SpannerDataCollectionSchema schema)
            throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    @Override
    protected void emitDeleteRecord(
                                    Receiver<SpannerChangeStreamPartition> receiver, SpannerDataCollectionSchema schema)
            throws InterruptedException {
        createAndEmitChangeRecord(receiver, schema);
    }

    private void createAndEmitChangeRecord(
                                           Receiver<SpannerChangeStreamPartition> receiver, SpannerDataCollectionSchema schema)
            throws InterruptedException {
        final Object newKey = schema.keyFromDataChangeRecord(dataChangeRecordMod);
        assert newKey != null;

        final Struct value = schema.valueFromDataChangeRecord(dataChangeRecordMod, getOperation());
        SpannerChangeStreamOffsetContext context = (SpannerChangeStreamOffsetContext) getOffset();
        value.put(FieldName.SOURCE, context.getSourceInfoForPartition(partitionToken).struct());
        value.put(FieldName.OPERATION, getOperation().code());
        value.put(FieldName.TIMESTAMP, getClock().currentTimeAsInstant().toEpochMilli());

        receiver.changeRecord(getPartition(), schema, getOperation(), newKey, value, getOffset(), null);
    }

    public static boolean isValidOperation(String operation) {
        return OPERATION_LITERALS.containsKey(operation);
    }
}
