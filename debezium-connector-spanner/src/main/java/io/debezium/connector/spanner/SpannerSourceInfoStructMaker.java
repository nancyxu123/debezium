/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class SpannerSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;
    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerSourceInfoStructMaker.class);

    public SpannerSourceInfoStructMaker(
                                        String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        LOGGER.info("In constructor for SpannerSourceInfoStructMaker");

        schema = commonSchemaBuilder()
                .name(
                        connectorConfig
                                .schemaNameAdjustmentMode()
                                .createAdjuster()
                                .adjust("io.debezium.connector.spanner.Source"))
                .field(SourceInfo.TABLE_NAME, Schema.STRING_SCHEMA)
                .field(SourceInfo.RECORD_SEQUENCE, Schema.STRING_SCHEMA)
                .field(SourceInfo.TRANSACTION_ID, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMIT_TIMESTAMP, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct struct = super.commonStruct(sourceInfo)
                .put(SourceInfo.TABLE_NAME, sourceInfo.getTableName())
                .put(SourceInfo.TRANSACTION_ID, sourceInfo.getTransactionId())
                .put(SourceInfo.RECORD_SEQUENCE, sourceInfo.getRecordSequence())
                .put(SourceInfo.COMMIT_TIMESTAMP, sourceInfo.timestamp().toString());
        return struct;
    }
}
