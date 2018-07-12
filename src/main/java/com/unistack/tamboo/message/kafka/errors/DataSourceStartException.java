package com.unistack.tamboo.message.kafka.errors;

/**
 * @program: tamboo-sa
 * @description: 数据源启动异常
 * @author: Asasin
 * @create: 2018-06-07 21:07
 **/
public class DataSourceStartException extends GeneralServiceException{

    public DataSourceStartException(String message) {
        super(message);
    }

    public DataSourceStartException(String message, Throwable s) {
        super(message, s);
    }

    public DataSourceStartException(Throwable e) {
        super(e);
    }
}
    