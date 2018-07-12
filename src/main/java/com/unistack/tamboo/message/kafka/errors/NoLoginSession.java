package com.unistack.tamboo.message.kafka.errors;

/**
 * @author Gyges Zean
 * @date 2018/6/6
 * 用于登录会话信息的异常
 */
public class NoLoginSession extends GeneralServiceException {

    public NoLoginSession(String message) {
        super(message);
    }

    public NoLoginSession(String message, Throwable s) {
        super(message, s);
    }

    public NoLoginSession(Throwable e) {
        super(e);
    }
}
