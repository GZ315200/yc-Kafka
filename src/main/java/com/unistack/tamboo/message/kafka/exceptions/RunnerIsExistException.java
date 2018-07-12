package com.unistack.tamboo.message.kafka.exceptions;

/**
 * @author Gyges Zean
 * @date 2018/5/15
 */
public class RunnerIsExistException extends KafkaException {

    public RunnerIsExistException(String s) {
        super(s);
    }

    public RunnerIsExistException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RunnerIsExistException(Throwable throwable) {
        super(throwable);
    }
}
