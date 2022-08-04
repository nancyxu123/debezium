/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.annotation.Immutable;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;

public class SpannerChangeStreamOffsetContext implements OffsetContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerChangeStreamOffsetContext.class);
    private static final String CONTEXT_NAME = "spanner-offset-context";
    public static final String PARTITION_POSITIONS = "partition_positions";

    private final ConcurrentMap<String, SourceInfo> partitionStates;

    private final TransactionContext transactionContext;

    private final String taskId;

    private final SpannerConnectorConfig connectorConfig;

    @Immutable
    public static final class OffsetPosition {
        private final Timestamp ts;
        private final boolean isFinished;

        public OffsetPosition(Timestamp ts, boolean isFinished) {
            this.ts = ts;
            this.isFinished = isFinished;
        }

        public Timestamp getTimestamp() {
            return this.ts;
        }

        public boolean isFinished() {
            return this.isFinished;
        }

        @Override
        public String toString() {
            return "Position [ts=" + ts + ", isFinished=" + isFinished + "]";
        }
    }

    public SpannerChangeStreamOffsetContext(
                                            SpannerConnectorConfig connectorConfig,
                                            TransactionContext transactionContext,
                                            String taskId) {
        partitionStates = new ConcurrentHashMap<String, SourceInfo>();
        this.transactionContext = transactionContext;
        this.taskId = taskId;
        this.connectorConfig = connectorConfig;
    }

    public SpannerChangeStreamOffsetContext(SpannerConnectorConfig connectorConfig, String taskId) {
        this(connectorConfig, new TransactionContext(), taskId);
    }

    public void dataChangeEvent(
                                String partitionToken,
                                Instant commitInstant,
                                String transactionId,
                                String recordSequence,
                                String tableName) {
        SourceInfo sourceInfo = partitionStates.computeIfAbsent(partitionToken, k -> new SourceInfo(connectorConfig));
        sourceInfo.dataChangeEvent(commitInstant, transactionId, recordSequence, tableName);
        partitionStates.replace(partitionToken, sourceInfo);
    }

    public void heartbeatEvent(String partitionToken, Instant commitInstant) {
        SourceInfo sourceInfo = partitionStates.computeIfAbsent(partitionToken, k -> new SourceInfo(connectorConfig));
        sourceInfo.heartbeatEvent(commitInstant);
        partitionStates.replace(partitionToken, sourceInfo);
    }

    public void childPartitionEvent(String partitionToken) {
        SourceInfo sourceInfo = partitionStates.computeIfAbsent(partitionToken, k -> new SourceInfo(connectorConfig));
        sourceInfo.setIsFinished();
        partitionStates.replace(partitionToken, sourceInfo);
    }

    public void removePartition(String partitionToken) {
        partitionStates.remove(partitionToken);
    }

    public String getTaskId() {
        return taskId;
    }

    public SourceInfo getSourceInfoForPartition(String partitionToken) {
        return partitionStates.computeIfAbsent(partitionToken, k -> new SourceInfo(connectorConfig));
    }

    public Map<String, OffsetPosition> getPartitionPositions() {
        Map<String, OffsetPosition> positions = new HashMap<String, OffsetPosition>();
        for (Map.Entry partitionTimestamp : partitionStates.entrySet()) {
            String partitionToken = (String) partitionTimestamp.getKey();
            SourceInfo sourceInfo = (SourceInfo) partitionTimestamp.getValue();
            positions.put(
                    partitionToken,
                    new OffsetPosition(
                            Timestamp.ofTimeSecondsAndNanos(
                                    sourceInfo.timestamp().getEpochSecond(), sourceInfo.timestamp().getNano()),
                            sourceInfo.isFinished()));
        }
        return positions;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, String> offset = new HashMap<String, String>();
        for (Map.Entry partitionTimestamp : partitionStates.entrySet()) {
            String partitionToken = (String) partitionTimestamp.getKey();
            SourceInfo sourceInfo = (SourceInfo) partitionTimestamp.getValue();
            String isFinished = sourceInfo.isFinished() ? "1" : "0";
            offset.put(
                    partitionToken,
                    Timestamp.ofTimeSecondsAndNanos(
                            sourceInfo.timestamp().getEpochSecond(), sourceInfo.timestamp().getNano())
                            .toString()
                            + ","
                            + isFinished);
        }
        return offset;
        // return Collect.hashMapOf(PARTITION_POSITIONS, "1");
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public Schema getSourceInfoSchema() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public Struct getSourceInfo() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public boolean isSnapshotRunning() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public void preSnapshotStart() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public void preSnapshotCompletion() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public void markLastSnapshotRecord() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public void postSnapshotCompletion() {
        throw new UnsupportedOperationException(
                "Currently unsupported by the Spanner Change Streams connector");
    }

    @Override
    public String toString() {
        return "SpannerChangeStreamOffsetContext ["
                + "partitionStates="
                + partitionStates.toString()
                + "]";
    }
}
