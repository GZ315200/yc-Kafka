package com.unistack.tamboo.message.kafka.util.json;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 *
 * The Converter interface provides support for translating between Kafka  runtime data format
 * and byte[]. Internally, this likely includes an intermediate step to the format used by the serialization
 * layer (e.g. JsonNode, GenericRecord, Message).
 */
public interface Converter {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert a Kafka Connect data object to a native object for serialization.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return the serialized value
     */
    byte[] fromConnectData(String topic, Object value);

    /**
     * Convert a native object to a Kafka Connect data object.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link GenericRecord} and the converted value
     */
    GenericRecord toConnectData(String topic, byte[] value);

}
