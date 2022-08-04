package io.debezium.connector.spanner;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class SpannerConnectorIT extends AbstractConnectorTest {
    private Configuration config;
    // private SpannerChangeStreamTaskContext taskContext;

    @Before
    public void beforeEach() {
        // Testing.Debug.disable();
        // Testing.Print.disable();
        stopConnector();
        initializeConnectorTestFramework();
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            logger.info("Done");
        }
    }

    public SpannerConnectorIT() {
    }

    @Test
    public void shouldConsumeAllEventsFromDatabase() throws InterruptedException, IOException {
        Testing.Print.enable();
        // Use the DB configuration to define the connector's configuration ...
        config = Configuration.create()
                .with(SpannerConnectorConfig.PROJECT, "cloud-spanner-backups-loadtest")
                .with(SpannerConnectorConfig.INSTANCE, "change-stream-load-test-3")
                .with(SpannerConnectorConfig.DATABASE, "load-test-change-stream-enable")
                .with(SpannerConnectorConfig.METADATA_INSTANCE, "change-stream-load-test-3")
                .with(SpannerConnectorConfig.METADATA_DATABASE, "change-stream-metadata")
                .with(SpannerConnectorConfig.METADATA_TABLE_NAME, "metadataTable3")
                .with(SpannerConnectorConfig.CHANGE_STREAM_NAME, "changeStreamAll")
                .with("heartbeat.interval.ms", 25)
                .with("poll.interval.ms", 10)
                .build();

        // Set up the replication context for connections ...
        // taskContext = new SpannerChangeStreamTaskContext(config);

        logger.info("Starting the connector");
        // Start the connector ...
        start(SpannerConnector.class, config);

        SourceRecords records = consumeRecordsByTopic(1000000, 20);
    }
}
