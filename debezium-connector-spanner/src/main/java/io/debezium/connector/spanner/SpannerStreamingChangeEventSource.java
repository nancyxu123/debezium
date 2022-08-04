package io.debezium.connector.spanner;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Threads;

/** */
final class SpannerStreamingChangeEventSource
        implements StreamingChangeEventSource<SpannerChangeStreamPartition, SpannerChangeStreamOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpannerStreamingChangeEventSource.class);
    private final SpannerConnectorConfig connectorConfig;
    private final EventDispatcher<SpannerChangeStreamPartition, CollectionId> dispatcher;
    private Spanner client;
    private String projectId;
    private String instanceId;
    private String databaseId;
    private String changeStream;
    private String metadataInstanceId;
    private String metadataDatabaseId;
    private String metadataTableName;
    private DatabaseClient databaseClient;
    private DatabaseClient metadataDatabaseClient;
    private String startTimestamp;
    private String endTimestamp;
    private final Clock clock;
    private final SpannerChangeStreamOffsetContext offsetContext;
    private Timer timer = new Timer();

    public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
    public static final String COLUMN_PARENT_TOKENS = "ParentTokens";
    public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
    public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
    public static final String COLUMN_STATE = "State";
    public static final String COLUMN_WATERMARK = "Watermark";
    public static final String COLUMN_TASK_ID = "TaskId";

    public SpannerStreamingChangeEventSource(
                                             SpannerConnectorConfig connectorConfig,
                                             EventDispatcher<SpannerChangeStreamPartition, CollectionId> dispatcher,
                                             SpannerChangeStreamOffsetContext offsetContext,
                                             Clock clock) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.offsetContext = offsetContext;
        setUpSpanner(connectorConfig);

        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        long beforeGettingWatermarkMillis = Instant.now().toEpochMilli();
                        Timestamp timestamp = getUnfinishedMinWatermark();
                        long afterGettingWatermarkMillis = Instant.now().toEpochMilli();
                        final Instant watermarkInstant = Instant.ofEpochSecond(
                                timestamp.getSeconds(),
                                timestamp.getNanos());
                        final Instant now = Instant.now();
                        if (timestamp != null && beforeGettingWatermarkMillis % 30 == 0) {
                            LOGGER.info("Time it took to query the low watermark: " + (afterGettingWatermarkMillis - beforeGettingWatermarkMillis));
                            LOGGER.info(
                                    "The minimum watermark is: "
                                            + timestamp.toString()
                                            + " which has millis discrepancy: " + (now.toEpochMilli() - watermarkInstant.toEpochMilli()));
                        }
                    }
                },
                0,
                50);
    }

    public @Nullable Timestamp getUnfinishedMinWatermark() {
        final Statement statement = Statement.newBuilder(
                "SELECT "
                        + COLUMN_WATERMARK
                        + ","
                        + COLUMN_PARTITION_TOKEN
                        + " FROM "
                        + metadataTableName
                        + " WHERE "
                        + COLUMN_STATE
                        + " != @state"
                        + " ORDER BY "
                        + COLUMN_WATERMARK
                        + " ASC LIMIT 1")
                .bind("state")
                .to("FINISHED")
                .build();
        try (ResultSet resultSet = metadataDatabaseClient.singleUse().executeQuery(statement)) {
            if (resultSet.next()) {
                // LOGGER.info(
                // "Partition token for minimum watermark: "
                // + resultSet.getString(COLUMN_PARTITION_TOKEN));
                return resultSet.getTimestamp(COLUMN_WATERMARK);
            }
            return null;
        }
    }

    public @Nullable String getState(String partitionToken) {
        final Statement statement = Statement.newBuilder(
                "SELECT "
                        + COLUMN_STATE
                        + " FROM "
                        + metadataTableName
                        + " WHERE "
                        + COLUMN_PARTITION_TOKEN
                        + " = @partitionToken")
                .bind("partitionToken")
                .to(partitionToken)
                .build();
        try (ResultSet resultSet = metadataDatabaseClient.singleUse().executeQuery(statement)) {
            if (resultSet.next()) {
                return resultSet.getString(COLUMN_STATE);
            }
            return null;
        }
    }

    public class PartitionMetadata implements Serializable {

        private Timestamp queryStartTime;
        private Timestamp queryEndTime;
        private String partitionToken;

        public PartitionMetadata(
                                 String partitionToken, Timestamp queryStartTime, Timestamp queryEndTime) {
            this.partitionToken = partitionToken;
            this.queryStartTime = queryStartTime;
            this.queryEndTime = queryEndTime;
        }

        /** Unique partition identifier, which can be used to perform a change stream query. */
        public String partitionToken() {
            return partitionToken;
        }

        public Timestamp queryStartTime() {
            return queryStartTime;
        }

        public Timestamp queryEndTime() {
            return queryEndTime;
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionMetadata)) {
                return false;
            }
            PartitionMetadata that = (PartitionMetadata) o;
            return Objects.equals(queryStartTime, that.queryStartTime)
                    && Objects.equals(partitionToken, that.partitionToken)
                    && Objects.equals(queryEndTime, that.queryEndTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionToken, queryStartTime, queryEndTime);
        }

        @Override
        public String toString() {
            return "PartitionMetadata{"
                    + "partitionToken='"
                    + partitionToken
                    + '\''
                    + ", queryStartTime="
                    + queryStartTime
                    + ", queryEndTime="
                    + queryEndTime;
        }
    }

    void setUpSpanner(SpannerConnectorConfig config) {
        SpannerOptions.Builder builder = SpannerOptions.newBuilder();
        // Set retryable codes for all API methods
        // Setting the timeout for streaming read to 2 hours. This is 1 hour by default
        // after BEAM 2.20.
        ServerStreamingCallSettings.Builder<ExecuteSqlRequest, PartialResultSet> executeStreamingSqlSettings = builder.getSpannerStubSettingsBuilder()
                .executeStreamingSqlSettings();
        RetrySettings.Builder executeSqlStreamingRetrySettings = executeStreamingSqlSettings.getRetrySettings().toBuilder();
        executeStreamingSqlSettings.setRetrySettings(
                executeSqlStreamingRetrySettings
                        .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
                        .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
                        .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(120))
                        .build());
        projectId = config.getProjectName();
        instanceId = config.getInstanceId();
        databaseId = config.getDatabaseId();
        changeStream = config.getChangeStreamName();
        metadataInstanceId = config.getMetadataInstanceId();
        metadataDatabaseId = config.getMetadataDatabaseId();
        metadataTableName = config.getMetadataTableName();
        startTimestamp = config.getStartTimestamp();
        endTimestamp = config.getEndTimestamp();

        builder.setProjectId(projectId);
        builder.setHost("https://staging-wrenchworks.sandbox.googleapis.com");
        SpannerOptions options = builder.build();
        Spanner spanner = options.getService();
        databaseClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
        metadataDatabaseClient = spanner.getDatabaseClient(
                DatabaseId.of(options.getProjectId(), metadataInstanceId, metadataDatabaseId));
    }

    @Override
    public void execute(
                        ChangeEventSourceContext context,
                        SpannerChangeStreamPartition partition,
                        SpannerChangeStreamOffsetContext offsetContext)
            throws InterruptedException {
        throw new UnsupportedOperationException("Currently unsupported by the SQL Server connector");
    }

    @Override
    public boolean executeIteration(
                                    ChangeEventSourceContext context,
                                    SpannerChangeStreamPartition partition,
                                    SpannerChangeStreamOffsetContext offsetContext)
            throws InterruptedException {
        throw new UnsupportedOperationException("Currently unsupported by the SQL Server connector");
    }

    public boolean executeIteration(
                                    ChangeEventSourceContext context, SpannerChangeStreamPartition partition)
            throws InterruptedException {
        try {
            String taskId = partition.getTaskIndex();
            HashSet<PartitionMetadata> partitionTokens = new HashSet<PartitionMetadata>();
            LOGGER.debug("Executing Single Iteration ");

            long nowMillis4 = Instant.now().toEpochMilli();
            try (ResultSet resultSet = getAllCreatedPartitionsForTask(taskId)) {
                while (resultSet.next()) {
                    Struct row = resultSet.getCurrentRowAsStruct();
                    long nowMillis5 = Instant.now().toEpochMilli();
                    String partitionToken = row.getString("PartitionToken");
                    LOGGER.info("Retrieved row: " + row.toString());
                    LOGGER.info(
                            "The time it took t retrieve row "
                                    + " and the difference in millis for: " + partitionToken + ": "
                                    + (nowMillis5 - nowMillis4));
                    List<String> parentTokens = row.getStringList("ParentTokens");
                    Timestamp queryStartTime = row.getTimestamp("StartTimestamp");
                    Timestamp queryEndTime = row.getTimestamp("EndTimestamp");

                    long startQueryMillis = Instant.ofEpochSecond(
                            queryStartTime.getSeconds(),
                            queryStartTime.getNanos()).toEpochMilli();
                    long nowMillis3 = Instant.now().toEpochMilli();
                    LOGGER.info(
                            "The start  timestamp: "
                                    + queryStartTime.toString()
                                    + " and the difference in millis for: " + partitionToken + ": "
                                    + (nowMillis3 - startQueryMillis));
                    boolean allFinished = true;
                    for (String parentToken : parentTokens) {
                        String state = getState(parentToken);
                        if (!state.equals("FINISHED")) {
                            allFinished = false;
                            LOGGER.info(
                                    "Not executing row : "
                                            + row.toString()
                                            + " because parent is not finished: "
                                            + parentToken);
                        }
                    }
                    if (allFinished) {
                        partitionTokens.add(
                                new PartitionMetadata(partitionToken, queryStartTime, queryEndTime));
                    }
                }
            }
            int threads = partitionTokens.size();
            if (threads > 0) {
                final ExecutorService executor = Threads.newFixedThreadPool(
                        SpannerConnector.class, partition.getTaskIndex(), "streaming", threads);
                for (PartitionMetadata partitionToken : partitionTokens) {
                    List<Mutation> mutations = new ArrayList<Mutation>();
                    Mutation m = createUpdateStateMutation(partitionToken.partitionToken(), "SCHEDULED");
                    mutations.add(m);
                    metadataDatabaseClient.write(mutations);
                    executor.submit(
                            () -> {
                                try {
                                    LOGGER.info("Reading change stream for: " + partitionToken);
                                    readChangeStream(
                                            partition,
                                            offsetContext,
                                            partitionToken.partitionToken(),
                                            partitionToken.queryStartTime(),
                                            partitionToken.queryEndTime());
                                }
                                catch (Exception e) {
                                    LOGGER.error("SQL query Exception e: " + e);
                                }
                            });
                }
            }
        }
        catch (Exception e) {
            LOGGER.error("Execute iteration returned error: " + e);
        }
        return true;
    }

    private ResultSet getAllCreatedPartitionsForTask(String taskId) {
        final Statement statement = Statement.newBuilder(
                "SELECT * FROM "
                        + metadataTableName
                        + " WHERE STATE = 'CREATED' AND TaskId = "
                        + taskId)
                .build();
        return metadataDatabaseClient.singleUse().executeQuery(statement);
    }

    private Mutation createInsertMetadataMutationFrom(
                                                      String partitionToken,
                                                      List<String> parentTokens,
                                                      Timestamp queryStartTime,
                                                      Timestamp queryEndTime,
                                                      String state,
                                                      long taskId) {
        return Mutation.newInsertOrUpdateBuilder(metadataTableName)
                .set(COLUMN_PARTITION_TOKEN)
                .to(partitionToken)
                .set(COLUMN_PARENT_TOKENS)
                .toStringArray(parentTokens)
                .set(COLUMN_START_TIMESTAMP)
                .to(queryStartTime)
                .set(COLUMN_END_TIMESTAMP)
                .to(queryEndTime)
                .set(COLUMN_STATE)
                .to(state)
                .set(COLUMN_WATERMARK)
                .to(queryStartTime)
                .set(COLUMN_TASK_ID)
                .to(taskId)
                .build();
    }

    private Mutation createUpdateStateMutation(String partitionToken, String state) {
        LOGGER.info("Updating state for: " + partitionToken + " to: " + state);
        return Mutation.newUpdateBuilder(metadataTableName)
                .set(COLUMN_PARTITION_TOKEN)
                .to(partitionToken)
                .set(COLUMN_STATE)
                .to(state)
                .build();
    }

    private Mutation createUpdateWatermarkMutation(String partitionToken, Timestamp watermark) {
        return Mutation.newUpdateBuilder(metadataTableName)
                .set(COLUMN_PARTITION_TOKEN)
                .to(partitionToken)
                .set(COLUMN_WATERMARK)
                .to(watermark)
                .build();
    }

    private void readChangeStream(
                                  SpannerChangeStreamPartition partition,
                                  SpannerChangeStreamOffsetContext offsetContext,
                                  String partitionToken,
                                  Timestamp queryStartTime,
                                  Timestamp queryEndTime) {
        try {
            final String partitionTokenOrNull = partitionToken.equals("Parent0") ? null : partitionToken;

            final String query = "SELECT * FROM READ_"
                    + changeStream
                    + "("
                    + "   start_timestamp => @startTimestamp,"
                    + "   end_timestamp => @endTimestamp,"
                    + "   partition_token => @partitionToken,"
                    + "   read_options => null,"
                    + "   heartbeat_milliseconds => @heartbeatMillis"
                    + ")";
            long startMillis = Instant.ofEpochSecond(
                    queryStartTime.getSeconds(),
                    queryStartTime.getNanos()).toEpochMilli();
            long nowMillis2 = Instant.now().toEpochMilli();
            LOGGER.info(
                    "The next start  timestamp: "
                            + queryStartTime.toString()
                            + " and the difference in millis for: " + partitionToken + ": "
                            + (nowMillis2 - startMillis));
            final ResultSet resultSet = databaseClient
                    .singleUse()
                    .executeQuery(
                            Statement.newBuilder(query)
                                    .bind("startTimestamp")
                                    .to(queryStartTime)
                                    .bind("endTimestamp")
                                    .to(queryEndTime)
                                    .bind("partitionToken")
                                    .to(partitionTokenOrNull)
                                    .bind("heartbeatMillis")
                                    .to(30)
                                    .build());
            LOGGER.info("TVF query: " + Statement.newBuilder(query)
                    .bind("startTimestamp")
                    .to(queryStartTime)
                    .bind("endTimestamp")
                    .to(queryEndTime)
                    .bind("partitionToken")
                    .to(partitionTokenOrNull)
                    .bind("heartbeatMillis")
                    .to(100)
                    .build());
            List<Mutation> mutations = new ArrayList<Mutation>();
            while (resultSet.next()) {
                Struct record = resultSet.getStructList(0).get(0);
                Timestamp commitTimestamp;
                for (Struct dataChangeRecord : record.getStructList("data_change_record")) {
                    // If the data change record is null, that means the ChangeRecord is either a heartbeat
                    // or a child partitions record.
                    if (dataChangeRecord.isNull(0)) {
                        continue;
                    }
                    String tableName = dataChangeRecord.getString(4);
                    CollectionId collectionId = new CollectionId(projectId, instanceId, databaseId, tableName);
                    for (Struct mod : dataChangeRecord.getStructList(6)) {
                        // Process each data change record mod.
                        try {
                            final String recordSequence = dataChangeRecord.getString(1);
                            final String transactionId = dataChangeRecord.getString(2);
                            final Instant commitInstant = Instant.ofEpochSecond(
                                    dataChangeRecord.getTimestamp(0).getSeconds(),
                                    dataChangeRecord.getTimestamp(0).getNanos());
                            offsetContext.dataChangeEvent(
                                    partitionTokenOrNull, commitInstant, transactionId, recordSequence, tableName);
                            long commitMillis = commitInstant.toEpochMilli();
                            long nowMillis = Instant.now().toEpochMilli();
                            if (commitMillis % 3000 == 0) {
                                LOGGER.info(
                                        "The latest commit timestamp: "
                                                + dataChangeRecord.getTimestamp(0).toString()
                                                + " and the difference in millis: "
                                                + (nowMillis - commitMillis));
                            }
                            dispatcher.dispatchDataChangeEvent(
                                    partition,
                                    collectionId,
                                    new SpannerChangeRecordEmitter(
                                            partition,
                                            offsetContext,
                                            dataChangeRecord,
                                            mod,
                                            partitionTokenOrNull,
                                            clock));
                        }
                        catch (Exception e) {
                            LOGGER.error("Data change Exception e: " + e);
                            return;
                        }
                    }
                }
                for (Struct childPartitionsRecord : record.getStructList("child_partitions_record")) {
                    if (childPartitionsRecord.isNull(0)) {
                        continue;
                    }
                    LOGGER.info(
                            "Processing event for: "
                                    + partitionTokenOrNull
                                    + " "
                                    + childPartitionsRecord.toString()
                                    + " at timestamp: "
                                    + Timestamp.now().toString());
                    long numTasks = partition.numTasks();
                    Random rand = new Random();
                    int randInt = rand.nextInt((int) numTasks);
                    Timestamp childStartTimestamp = childPartitionsRecord.getTimestamp(0);
                    for (Struct childPartition : childPartitionsRecord.getStructList(2)) {
                        String token = childPartition.getString(0);
                        List<String> parentTokens = childPartition.getStringList(1);
                        Mutation m = createInsertMetadataMutationFrom(
                                token, parentTokens, childStartTimestamp, queryEndTime, "CREATED", randInt);
                        mutations.add(m);
                        LOGGER.info("Before child partition event");
                        offsetContext.childPartitionEvent(partitionToken);
                        LOGGER.info("After child partition event");
                    }
                }
                for (Struct heartbeatRecord : record.getStructList("heartbeat_record")) {
                    if (heartbeatRecord.isNull(0)) {
                        continue;
                    }
                    final Instant commitInstant = Instant.ofEpochSecond(
                            heartbeatRecord.getTimestamp(0).getSeconds(),
                            heartbeatRecord.getTimestamp(0).getNanos());
                    long commitMillis = commitInstant.toEpochMilli();
                    long nowMillis = Instant.now().toEpochMilli();
                    if ((nowMillis - commitMillis) > 1000) {
                        LOGGER.info(
                                "The latest commit timestamp: "
                                        + heartbeatRecord.getTimestamp(0).toString()
                                        + " and the difference in millis for partition token: " + partitionTokenOrNull + ": "
                                        + (nowMillis - commitMillis));
                        LOGGER.info("Afterwards: " + partitionTokenOrNull);
                    }
                    offsetContext.heartbeatEvent(partitionTokenOrNull, commitInstant);
                    dispatcher.alwaysDispatchHeartbeatEvent(partition, offsetContext);
                }
                // Figure out how to update the watermark here.

            }
            metadataDatabaseClient.write(mutations);
        }
        catch (Exception e) {
            LOGGER.error("Change stream Exception: " + e.toString());
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // LOGGER.info("Committing the offset");
        List<Mutation> mutations = new ArrayList<Mutation>();
        List<String> finishedTokens = new ArrayList<String>();
        long instantNow = Instant.now().toEpochMilli();
        for (Map.Entry mapElement : offset.entrySet()) {
            String partitionToken = (String) mapElement.getKey();
            String offsetPosition = (String) mapElement.getValue();
            String[] arrOfStr = offsetPosition.split(",");
            String isFinished = arrOfStr[1];
            // LOGGER.info("Timestamp: " + arrOfStr[0]);
            // LOGGER.info("isFinished: " + arrOfStr[1]);
            // LOGGER.info("isFinishedQuality: " + isFinished + ", " + isFinished.equals("1"));
            if (arrOfStr[1].equals("1")) {
                LOGGER.info("Marking partition as finished: " + partitionToken);
                mutations.add(createUpdateStateMutation(partitionToken, "FINISHED"));
                finishedTokens.add(partitionToken);
            }
            else {
                long behindMillis = instantNow - Instant.parse(arrOfStr[0]).toEpochMilli();
                // LOGGER.info("Updating watermark is behind current time by: " + behindMillis);
                mutations.add(
                        createUpdateWatermarkMutation(partitionToken, Timestamp.parseTimestamp(arrOfStr[0])));
            }
        }
        long finishGettingMillis = Instant.now().toEpochMilli();
        // LOGGER.info("Getting watermarks took: " + (finishGettingMillis - instantNow));
        metadataDatabaseClient.write(mutations);
        long finishUpdatingMillis = Instant.now().toEpochMilli();
        // LOGGER.info("Updating watermark took: " + (finishUpdatingMillis - instantNow) + " millis");
        for (String partitionToken : finishedTokens) {
            offsetContext.removePartition(partitionToken);
        }
    }
}
