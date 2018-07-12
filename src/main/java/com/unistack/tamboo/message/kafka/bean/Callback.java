package com.unistack.tamboo.message.kafka.bean;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 *
 * Generic interface for callbacks
 */
public interface Callback<V> {

    /**
     * Invoked upon completion of the operation.
     *
     * @param error the error that caused the operation to fail, or null if no error occurred
     * @param result the return value, or null if the operation failed
     */
    void onCompletion(Throwable error, V result);
}
