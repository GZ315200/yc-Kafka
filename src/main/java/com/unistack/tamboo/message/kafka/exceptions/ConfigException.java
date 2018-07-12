package com.unistack.tamboo.message.kafka.exceptions;

/**
 * @author Gyges Zean
 * @date 2018/4/24
 * Thrown if the user supplies an invalid configuration
 */
public class ConfigException extends KafkaException {

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String name, Object value) {
        this(name, value, null);
    }

    public ConfigException(String message, Object value, String name) {
        super("Invalid value " + value + "for configuration " + name + (message == null ? "" : ": " + message));
    }

}
