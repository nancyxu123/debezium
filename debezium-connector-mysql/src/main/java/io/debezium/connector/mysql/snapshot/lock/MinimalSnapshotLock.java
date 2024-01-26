/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.snapshot.spi.SnapshotLock;

public class MinimalSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, Set<String> tableIds) {
        return Optional.empty();
    }
}
