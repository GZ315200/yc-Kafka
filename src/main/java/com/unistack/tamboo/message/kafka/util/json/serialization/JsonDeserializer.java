package com.unistack.tamboo.message.kafka.util.json.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 */
public class JsonDeserializer implements Deserializer {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        JsonNode data;
        try {
            data = objectMapper.readTree(bytes);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
        return data;
    }

    @Override
    public void close() {

    }
}
