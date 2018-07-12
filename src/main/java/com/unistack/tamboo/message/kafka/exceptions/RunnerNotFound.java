package com.unistack.tamboo.message.kafka.exceptions;

/**
 * @author Gyges Zean
 * @date 2018/5/15
 */
public class RunnerNotFound extends ConnectException{
    public RunnerNotFound(String s) {
        super(s);
    }

    public RunnerNotFound(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RunnerNotFound(Throwable throwable) {
        super(throwable);
    }
}
