package com.unistack.tamboo.message.kafka.util.json.serialization;

import java.io.Closeable;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 *
 * An interface for converting objects to bytes.
 */
public interface Serializer<T> extends Closeable {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
    void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with dati
     * @param data typed data
     * @return serialized bytes
     */
    byte[] serialize(String topic, T data);

    /**
     * Close this serializer.
     *
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    void close();

}
