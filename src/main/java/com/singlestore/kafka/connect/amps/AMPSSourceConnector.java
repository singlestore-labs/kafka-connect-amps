package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AMPS Source Connector that creates a single task for one AMPS topic.
 */
public class AMPSSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(AMPSSourceConnector.class);

    private Map<String, String> configProps;
    private AMPSSourceConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting AMPS Source Connector");

        this.configProps = props;
        this.config = new AMPSSourceConnectorConfig(props);

        log.info("AMPS Source Connector configuration:");
        log.info("  AMPS URI: {}", config.ampsUri);
        log.info("  AMPS Client Name: {}", config.ampsClientName);
        log.info("  AMPS Topic: {}", config.ampsTopic);
        log.info("  Kafka Topic: {}", config.getKafkaTopic());
        log.info("  Subscription Type: {}", config.ampsSubscriptionType.configValue());
        log.info("  Position: {}", config.ampsPosition);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AMPSSourceTask.class;
    }

    /**
     * Creates a single task configuration for the configured AMPS topic.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            log.info("Requested {} tasks, but this connector uses a single task per AMPS topic " +
                    "so all records share one sourcePartition. Extra tasks would violate offset uniqueness.",
                    maxTasks);
        }
        log.info("Creating task configuration for AMPS topic '{}' -> Kafka topic '{}'",
                config.ampsTopic, config.getKafkaTopic());
        return Collections.singletonList(configProps);
    }

    @Override
    public void stop() {
        log.info("Stopping AMPS Source Connector");
    }

    @Override
    public ConfigDef config() {
        return AMPSSourceConnectorConfig.configDef();
    }
}
