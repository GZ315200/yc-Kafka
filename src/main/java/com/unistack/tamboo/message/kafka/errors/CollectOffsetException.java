package com.unistack.tamboo.message.kafka.errors;

/**
 * @author Gyges Zean
 * @date 2018/5/26
 * 采集offset时出现的异常
 */
public class CollectOffsetException extends DataException {

    public CollectOffsetException(String s) {
        super(s);
    }

    public CollectOffsetException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public CollectOffsetException(Throwable throwable) {
        super(throwable);
    }
}
