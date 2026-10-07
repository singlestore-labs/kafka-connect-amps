package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.HAClient;

/**
 * Factory interface for creating AMPS HAClient instances.
 * This abstraction allows for easy mocking in unit tests.
 */
public interface AMPSClientFactory {

    /**
     * Creates and connects an AMPS HAClient.
     *
     * @param config           The connector configuration
     * @param clientNameSuffix Suffix to append to client name (typically the topic name)
     * @return A connected HAClient instance
     */
    HAClient create(AMPSSourceConnectorConfig config, String clientNameSuffix);
}
