package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTypeTest {

    @Test
    void parsesKnownValuesCaseInsensitively() {
        assertEquals(SubscriptionType.SUBSCRIBE, SubscriptionType.fromString("subscribe"));
        assertEquals(SubscriptionType.SUBSCRIBE, SubscriptionType.fromString("SUBSCRIBE"));
        assertEquals(SubscriptionType.SOW_AND_SUBSCRIBE, SubscriptionType.fromString("sow_and_subscribe"));
        assertEquals(SubscriptionType.SOW, SubscriptionType.fromString("sow"));
    }

    @Test
    void rejectsUnknownValue() {
        assertThrows(ConfigException.class, () -> SubscriptionType.fromString("delta_subscribe"));
    }
}
