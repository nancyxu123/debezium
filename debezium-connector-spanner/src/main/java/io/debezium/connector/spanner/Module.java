/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.Properties;

import io.debezium.util.IoUtil;

/**
 * Information about this module.
 *
 * @author Jiri Pechanec
 */
public final class Module {

    private static final Properties INFO = IoUtil.loadProperties(Module.class, "io/debezium/connector/spanner/build.version");

    public static String version() {
        return INFO.getProperty("version");
    }

    /**
     * @return symbolic name of the connector plugin
     */
    public static String name() {
        return "spannerchangestreams";
    }

    /**
     * @return context name used in log MDC and JMX metrics
     */
    public static String contextName() {
        return "Spanner_Change_Streams";
    }
}
