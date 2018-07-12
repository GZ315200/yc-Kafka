package com.unistack.tamboo.message.kafka.errors;

/**
 * @author Gyges Zean
 * @date 2018/6/14
 */
public class TopicAclCreateException extends GeneralServiceException {

    public TopicAclCreateException(String message) {
        super(message);
    }

    public TopicAclCreateException(String message, Throwable s) {
        super(message, s);
    }

    public TopicAclCreateException(Throwable e) {
        super(e);
    }
}
