/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;
import io.debezium.util.LoggingContext;

/** The partition. */
public class SpannerChangeStreamPartition implements Partition {
    private static final String TASK_INDEX_KEY = "task";

    private final String taskIndex;
    private final long numTasks;
    private final int hashCode;
    private final Map<String, String> sourcePartition;

    public SpannerChangeStreamPartition(String taskIndex, long numTasks) {
        this.taskIndex = taskIndex;
        this.sourcePartition = Collect.hashMapOf(TASK_INDEX_KEY, taskIndex);
        this.numTasks = numTasks;

        this.hashCode = Objects.hash(taskIndex);
    }

    public String getTaskIndex() {
        return taskIndex;
    }

    public long numTasks() {
        return numTasks;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public Map<String, String> getLoggingContext() {
        return Collections.singletonMap(LoggingContext.TASK_ID, taskIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SpannerChangeStreamPartition other = (SpannerChangeStreamPartition) obj;
        return Objects.equals(taskIndex, other.taskIndex);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "SpannerPartition [sourcePartition=" + getSourcePartition() + "]";
    }
}
