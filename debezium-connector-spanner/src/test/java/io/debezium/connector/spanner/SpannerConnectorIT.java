package io.debezium.connector.spanner;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    } finally {
      logger.info("Done");
    }
  }

  public SpannerConnectorIT() {}

  @Test
  public void shouldConsumeAllEventsFromDatabase() throws InterruptedException, IOException {
    Testing.Print.enable();
    // Use the DB configuration to define the connector's configuration ...
    config =
        Configuration.create()
            .with(SpannerConnectorConfig.PROJECT, "project")
            .with(SpannerConnectorConfig.INSTANCE, "instance")
            .with(SpannerConnectorConfig.DATABASE, "database")
            .with(SpannerConnectorConfig.METADATA_INSTANCE, "instance")
            .with(SpannerConnectorConfig.METADATA_DATABASE, "database")
            .with(SpannerConnectorConfig.METADATA_TABLE_NAME, "table")
            .with(SpannerConnectorConfig.CHANGE_STREAM_NAME, "changestream")
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
