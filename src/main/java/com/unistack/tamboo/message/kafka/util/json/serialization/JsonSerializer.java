package com.unistack.tamboo.message.kafka.util.json.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 * Serialize Jackson JsonNode tree model objects to UTF-8 JSON.
 */
public class JsonSerializer implements Serializer<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();


    public JsonSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}
