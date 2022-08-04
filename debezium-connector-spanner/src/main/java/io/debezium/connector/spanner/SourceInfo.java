/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the
 * source log position. Maps to {@code source} field in {@code Envelope}.
 *
 * @author Jiri Pechanec
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String TABLE_NAME = "table_name";
    public static final String COMMIT_TIMESTAMP = "commit_timestamp";
    public static final String TRANSACTION_ID = "transaction_id";
    public static final String RECORD_SEQUENCE = "record_sequence";

    private String tableName;
    private Instant commitTimestamp = Instant.EPOCH;
    private String transactionId;
    private String recordSequence;
    private boolean isFinished = false;

    protected SourceInfo(SpannerConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public void setTransactionId(String id) {
        transactionId = id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setRecordSequence(String sequence) {
        recordSequence = sequence;
    }

    public String getRecordSequence() {
        return recordSequence;
    }

    /**
     * @param instant a time at which the transaction commit was executed
     */
    public void setCommitTimestamp(Instant instant) {
        commitTimestamp = instant;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setIsFinished() {
        this.isFinished = true;
    }

    public void dataChangeEvent(
                                Instant commitTimestamp, String transactionId, String recordSequence, String tableName) {
        setCommitTimestamp(commitTimestamp);
        setTransactionId(transactionId);
        setTableName(tableName);
        setRecordSequence(recordSequence);
    }

    public void heartbeatEvent(Instant commitTimestamp) {
        dataChangeEvent(commitTimestamp, null, null, null);
    }

    @Override
    public String toString() {
        return "SourceInfo ["
                + "tableName="
                + getTableName()
                + ", transactionId="
                + getTransactionId()
                + ", recordSequence="
                + getRecordSequence()
                + ", timestamp="
                + timestamp()
                + "]";
    }

    @Override
    protected Instant timestamp() {
        return commitTimestamp;
    }

    @Override
    protected String database() {
        return "";
    }
}
