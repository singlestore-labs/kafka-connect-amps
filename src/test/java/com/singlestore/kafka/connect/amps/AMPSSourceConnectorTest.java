package com.singlestore.kafka.connect.amps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AMPSSourceConnectorTest {

    private AMPSSourceConnector connector;
    private Map<String, String> props;

    @BeforeEach
    void setUp() {
        connector = new AMPSSourceConnector();
        props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_CLIENT_NAME_CONF, "test-client");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "orders");
        props.put(AMPSSourceConnectorConfig.KAFKA_TOPIC_PREFIX_CONF, "amps-");
    }

    @Test
    void testVersion() {
        assertEquals("0.1.0", connector.version());
    }

    @Test
    void testTaskClass() {
        assertEquals(AMPSSourceTask.class, connector.taskClass());
    }

    @Test
    void testStart() {
        connector.start(props);
        assertNotNull(connector.config());
    }

    @Test
    void testTaskConfigsCreatesSingleTask() {
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        
        assertEquals(1, taskConfigs.size(), "Should create exactly one task");
        assertEquals("orders", taskConfigs.get(0).get(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF));
    }

    @Test
    void testTaskConfigsInheritsConnectorConfig() {
        props.put(AMPSSourceConnectorConfig.AMPS_FILTER_CONF, "/price > 100");
        connector.start(props);
        
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        
        assertEquals(1, taskConfigs.size());
        Map<String, String> taskConfig = taskConfigs.get(0);
        assertEquals("tcp://localhost:9007/amps/json", taskConfig.get(AMPSSourceConnectorConfig.AMPS_URI_CONF));
        assertEquals("test-client", taskConfig.get(AMPSSourceConnectorConfig.AMPS_CLIENT_NAME_CONF));
        assertEquals("/price > 100", taskConfig.get(AMPSSourceConnectorConfig.AMPS_FILTER_CONF));
    }

    @Test
    void testStop() {
        connector.start(props);
        assertDoesNotThrow(() -> connector.stop());
    }

    @Test
    void testServiceLoaderManifestIsPresent() {
        java.net.URL manifest = AMPSSourceConnector.class.getClassLoader()
                .getResource("META-INF/services/org.apache.kafka.connect.source.SourceConnector");
        assertNotNull(manifest, "ServiceLoader manifest is required for SingleStore short-name discovery");
    }
}
