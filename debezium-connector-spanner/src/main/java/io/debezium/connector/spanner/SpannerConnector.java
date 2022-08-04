package io.debezium.connector.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import io.debezium.config.Configuration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SpannerConnector extends SourceConnector {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_PARENT_TOKENS = "ParentTokens";
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  public static final String COLUMN_STATE = "State";
  public static final String COLUMN_WATERMARK = "Watermark";
  public static final String COLUMN_TASK_ID = "TaskId";
  private static final int TIMEOUT_MINUTES = 10;

  private DatabaseAdminClient databaseAdminClient;
  private DatabaseClient databaseClient;
  private Configuration config;
  private SpannerChangeStreamTaskContext taskContext;

  private String projectId;
  private String metadataInstanceId;
  private String metadataDatabaseId;
  private String metadataTableName;
  private Timestamp startTimestamp;
  private Timestamp endTimestamp;

  public SpannerConnector() {}

  @Override
  public String version() {
    return Module.version();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpannerConnectorTask.class;
  }

  private Mutation createInsertMetadataMutationFrom(
      String partitionToken,
      List<String> parentTokens,
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      String state,
      int taskId) {
    return Mutation.newInsertOrUpdateBuilder(metadataTableName)
        .set(COLUMN_PARTITION_TOKEN)
        .to(partitionToken)
        .set(COLUMN_PARENT_TOKENS)
        .toStringArray(parentTokens)
        .set(COLUMN_START_TIMESTAMP)
        .to(startTimestamp)
        .set(COLUMN_END_TIMESTAMP)
        .to(endTimestamp)
        .set(COLUMN_STATE)
        .to(state)
        .set(COLUMN_WATERMARK)
        .to(startTimestamp)
        .set(COLUMN_TASK_ID)
        .to(taskId)
        .build();
  }

  private void insertFakePartition(long maxTasks) {
    Random rand = new Random();
    int randInt = rand.nextInt((int) maxTasks);
    List<String> parentTokens = new ArrayList<String>();
    Mutation m =
        createInsertMetadataMutationFrom(
            "Parent0", parentTokens, startTimestamp, endTimestamp, "CREATED", randInt);
    logger.info("Inserting mutation: " + m.toString());
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(m);
    databaseClient.write(mutations);
  }

  public boolean tableExists() {
    final String checkTableExistsStmt =
        "SELECT t.table_name FROM information_schema.tables AS t "
            + "WHERE t.table_catalog = '' AND "
            + "t.table_schema = '' AND "
            + "t.table_name = '"
            + metadataTableName
            + "'";
    try (ResultSet queryResultSet =
        databaseClient
            .singleUseReadOnlyTransaction()
            .executeQuery(Statement.of(checkTableExistsStmt))) {
      return queryResultSet.next();
    }
  }

  private Mutation createUpdateStateMutation(String partitionToken, String state) {
    return Mutation.newUpdateBuilder(metadataTableName)
        .set(COLUMN_PARTITION_TOKEN)
        .to(partitionToken)
        .set(COLUMN_STATE)
        .to(state)
        .build();
  }

  public void dropPartitionMetadataTable() {
    logger.info("Dropping partition metadata table");
    final String metadataDropStmt = "DROP TABLE " + metadataTableName;
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            metadataInstanceId,
            metadataDatabaseId,
            Collections.singletonList(metadataDropStmt),
            null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (Exception e) {
      logger.error("Exception e: " + e);
    }
  }

  public void createPartitionMetadataTable() {
    final String metadataCreateStmt =
        "CREATE TABLE "
            + metadataTableName
            + " ("
            + COLUMN_PARTITION_TOKEN
            + " STRING(MAX) NOT NULL,"
            + COLUMN_PARENT_TOKENS
            + " ARRAY<STRING(MAX)> NOT NULL,"
            + COLUMN_START_TIMESTAMP
            + " TIMESTAMP NOT NULL,"
            + COLUMN_END_TIMESTAMP
            + " TIMESTAMP NOT NULL,"
            + COLUMN_STATE
            + " STRING(MAX) NOT NULL,"
            + COLUMN_WATERMARK
            + " TIMESTAMP NOT NULL,"
            + COLUMN_TASK_ID
            + " INT64 NOT NULL,"
            + ") PRIMARY KEY (PartitionToken)";
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            metadataInstanceId,
            metadataDatabaseId,
            Collections.singletonList(metadataCreateStmt),
            null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (Exception e) {
      logger.error("Exception e: " + e);
    }
  }

  void setUpSpanner(SpannerConnectorConfig config) {
    logger.info("Creating SpannerOptions");
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    // Set retryable codes for all API methods
    // Setting the timeout for streaming read to 2 hours. This is 1 hour by default
    // after BEAM 2.20.
    ServerStreamingCallSettings.Builder<ExecuteSqlRequest, PartialResultSet>
        executeStreamingSqlSettings =
            builder.getSpannerStubSettingsBuilder().executeStreamingSqlSettings();
    RetrySettings.Builder executeSqlStreamingRetrySettings =
        executeStreamingSqlSettings.getRetrySettings().toBuilder();
    executeStreamingSqlSettings.setRetrySettings(
        executeSqlStreamingRetrySettings
            .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(120))
            .build());
    projectId = config.getProjectName();
    metadataInstanceId = config.getMetadataInstanceId();
    metadataDatabaseId = config.getMetadataDatabaseId();
    metadataTableName = config.getMetadataTableName();
    startTimestamp =
        config.getStartTimestamp() == null
            ? Timestamp.now()
            : Timestamp.parseTimestamp(config.getStartTimestamp());
    endTimestamp =
        config.getEndTimestamp() == null
            ? Timestamp.MAX_VALUE
            : Timestamp.parseTimestamp(config.getEndTimestamp());

    logger.info("Setting up builder");
    builder.setProjectId(projectId);
    SpannerOptions options = builder.build();
    logger.info("Getting service");
    Spanner spanner = options.getService();
    logger.info("Creating database clients");
    databaseAdminClient = spanner.getDatabaseAdminClient();
    databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, metadataInstanceId, metadataDatabaseId));
    logger.info("After Creating database clients");
  }

  @Override
  public void start(Map<String, String> props) {
    logger.info("Starting Spanner connector: " + props.toString());
    config = Configuration.from(props);
    // First we will create the SpannerConnectorConfig
    final SpannerConnectorConfig connectorConfig = new SpannerConnectorConfig(config);

    // Create the Spanner metadata table here.
    // I want to create the Spanner metadata table here.
    setUpSpanner(connectorConfig);
    taskContext = new SpannerChangeStreamTaskContext(connectorConfig);
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Partitioning the replica sets amongst the number of tasks ...
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) {
      int taskId = taskConfigs.size();
      taskConfigs.add(
          config
              .edit()
              .with(SpannerConnectorConfig.TASK_ID, taskId)
              .with(SpannerConnectorConfig.NUM_TASKS, maxTasks)
              .build()
              .asMap());
    }
    logger.info("Creating task configs: " + maxTasks);
    logger.info("Set up Spanner");
    if (!tableExists()) {
      createPartitionMetadataTable();
    } else {
      dropPartitionMetadataTable();
      createPartitionMetadataTable();
    }
    logger.info("Created partition metadata table");

    insertFakePartition(maxTasks);
    logger.info("Inserted fake partition");

    return taskConfigs;
  }

  @Override
  public void stop() {
    this.config = null;
    dropPartitionMetadataTable();
  }

  @Override
  public ConfigDef config() {
    return SpannerConnectorConfig.configDef();
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    throw new UnsupportedOperationException("To be implemented.");
  }
}
