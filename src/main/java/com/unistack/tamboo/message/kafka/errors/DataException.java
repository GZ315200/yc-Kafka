package com.unistack.tamboo.message.kafka.errors;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 * Base class for all  data API exceptions.
 */
public class DataException extends SerializationException {

    public DataException(String s) {
        super(s);
    }

    public DataException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DataException(Throwable throwable) {
        super(throwable);
    }
}
