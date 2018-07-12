package com.unistack.tamboo.message.kafka.exceptions;

/**
 * @author Gyges Zean
 * @date 2018/4/24
 * The base class of all other Kafka exceptions
 */
public class KafkaException extends RuntimeException {

    public final static long serialVersionUID = 1L;

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(Throwable cause) {
        super(cause);
    }

    public KafkaException() {
        super();
    }

}
