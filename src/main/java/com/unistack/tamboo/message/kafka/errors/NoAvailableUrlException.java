package com.unistack.tamboo.message.kafka.errors;

/**
 * @author Gyges Zean
 * @date 2018/6/4
 *
 */
public class NoAvailableUrlException extends GeneralServiceException {

    public NoAvailableUrlException(String message) {
        super(message);
    }

    public NoAvailableUrlException(String message, Throwable s) {
        super(message, s);
    }

    public NoAvailableUrlException(Throwable e) {
        super(e);
    }
}
