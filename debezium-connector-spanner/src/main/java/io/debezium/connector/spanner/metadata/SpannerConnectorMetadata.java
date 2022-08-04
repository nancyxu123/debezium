/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.metadata;

import io.debezium.config.Field;
import io.debezium.connector.spanner.Module;
import io.debezium.connector.spanner.SpannerConnector;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.metadata.ConnectorDescriptor;
import io.debezium.metadata.ConnectorMetadata;

public class SpannerConnectorMetadata implements ConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("spanner", "Spanner Connector", SpannerConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getConnectorFields() {
        return SpannerConnectorConfig.ALL_FIELDS;
    }
}
