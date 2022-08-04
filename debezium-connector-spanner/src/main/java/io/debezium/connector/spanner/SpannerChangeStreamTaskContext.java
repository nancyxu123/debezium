/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Collections;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 */
public class SpannerChangeStreamTaskContext extends CdcSourceTaskContext {

    private final SourceInfo source;
    private final TopicSelector<CollectionId> topicSelector;
    private final Filters filters;

    /**
     * @param config the configuration
     */
    public SpannerChangeStreamTaskContext(SpannerConnectorConfig config) {
        super(Module.contextName(), config.getLogicalName(), Collections::emptySet);

        this.topicSelector = SpannerTopicSelector.defaultSelector("regular", "heartbeat");

        this.source = new SourceInfo(config);
        this.filters = new Filters();
    }

    public SourceInfo source() {
        return source;
    }

    public TopicSelector<CollectionId> topicSelector() {
        return topicSelector;
    }

    public Filters filters() {
        return filters;
    }
}
