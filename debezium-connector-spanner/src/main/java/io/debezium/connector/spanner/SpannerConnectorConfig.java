/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;

/** The configuration properties. */
public class SpannerConnectorConfig extends CommonConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorConfig.class);

    public static final Field CREDENTIALS = Field.create("google.spanner.credentials")
            .withDisplayName("Credentials")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription(
                    "You can specify a credential file by providing a path to GoogleCredentials."
                            + " Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS"
                            + " environment variable");

    public static final Field PROJECT = Field.create("google.spanner.project")
            .withDisplayName("ProjectId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner project to read from.");

    public static final Field INSTANCE = Field.create("google.spanner.instance")
            .withDisplayName("InstanceId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner instance to read from.");

    public static final Field DATABASE = Field.create("google.spanner.database")
            .withDisplayName("DatabaseId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner database to read from.");

    public static final Field METADATA_INSTANCE = Field.create("google.spanner.metadata_instance")
            .withDisplayName("MetadataInstanceId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner metadata instance.");

    public static final Field METADATA_DATABASE = Field.create("google.spanner.metadata_database")
            .withDisplayName("MetadataInstanceId")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner metadata instance.");

    public static final Field METADATA_TABLE_NAME = Field.create("google.spanner.metadata_table_name")
            .withDisplayName("MetadataTableName")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner metadata table name.");

    public static final Field CHANGE_STREAM_NAME = Field.create("google.spanner.change_stream_name")
            .withDisplayName("ChangeStream")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The Spanner change stream.");

    public static final Field START_TIMESTAMP = Field.create("google.spanner.start_timestamp")
            .withDisplayName("StartTimestamp")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The start timestamp.");

    public static final Field NUM_TASKS = Field.create("google.spanner.num_tasks")
            .withDisplayName("NumTasks")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .withDefault(1L)
            .withDescription("The number of tasks.");

    public static final Field END_TIMESTAMP = Field.create("google.spanner.end_timestamp")
            .withDisplayName("EndTimestamp")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The end timestamp.")
            .withDefault("");

    private static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION
            .edit()
            .name("Spanner")
            .type(CREDENTIALS, PROJECT, INSTANCE, DATABASE, METADATA_INSTANCE, METADATA_DATABASE)
            .events(START_TIMESTAMP, END_TIMESTAMP, NUM_TASKS)
            .create();

    /** The set of {@link Field}s defined as part of this configuration. */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 0;
    private final String credentialsFile;
    private final String projectName;
    private final String instanceId;
    private final String databaseId;
    private final String changeStreamName;
    private final String metadataInstanceId;
    private final String metadataDatabaseId;
    private final String metadataTableName;
    private final String startTimestamp;
    private final String endTimestamp;
    private final long numTasks;

    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

    public SpannerConnectorConfig(Configuration config) {
        super(config, config.getString(CHANGE_STREAM_NAME), DEFAULT_SNAPSHOT_FETCH_SIZE);

        this.credentialsFile = config.getString(CREDENTIALS);
        this.projectName = config.getString(PROJECT);
        this.instanceId = config.getString(INSTANCE);
        this.databaseId = config.getString(DATABASE);
        this.changeStreamName = config.getString(CHANGE_STREAM_NAME);
        this.metadataInstanceId = config.getString(METADATA_INSTANCE);
        this.metadataDatabaseId = config.getString(METADATA_DATABASE);
        this.metadataTableName = config.getString(METADATA_TABLE_NAME);
        this.startTimestamp = config.getString(START_TIMESTAMP);
        this.endTimestamp = config.getString(END_TIMESTAMP);
        LOGGER.info("Getting number of tasks");
        this.numTasks = config.getLong(NUM_TASKS);
    }

    public String getCredentialsFile() {
        return credentialsFile;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public String getChangeStreamName() {
        return changeStreamName;
    }

    public String getMetadataInstanceId() {
        return metadataInstanceId;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public String getMetadataDatabaseId() {
        return metadataDatabaseId;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public String getEndTimestamp() {
        return startTimestamp;
    }

    public long getNumTasks() {
        return numTasks;
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SpannerSourceInfoStructMaker getSourceInfoStructMaker(Version version) {
        LOGGER.info("getting source info struct maker");
        LOGGER.info("Module name: " + Module.name());
        LOGGER.info("Module version: " + Module.version());
        return new SpannerSourceInfoStructMaker(Module.name(), Module.version(), this);
    }
}
