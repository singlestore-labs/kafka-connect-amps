package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessageTypeTest {

    @Test
    void parsesKnownValuesCaseInsensitively() {
        assertEquals(MessageType.JSON, MessageType.fromString("json"));
        assertEquals(MessageType.JSON, MessageType.fromString("JSON"));
        assertEquals(MessageType.NVFIX, MessageType.fromString("nvfix"));
        assertEquals(MessageType.NVFIX, MessageType.fromString("NVFIX"));
    }

    @Test
    void defaultsToJsonForBlankInput() {
        assertEquals(MessageType.JSON, MessageType.fromString(null));
        assertEquals(MessageType.JSON, MessageType.fromString(""));
    }

    @Test
    void rejectsUnknownValue() {
        assertThrows(ConfigException.class, () -> MessageType.fromString("xml"));
    }
}
