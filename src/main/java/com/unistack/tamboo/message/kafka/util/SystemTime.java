package com.unistack.tamboo.message.kafka.util;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author Gyges Zean
 * @date 2018/4/25
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public Date getDate() {
        return new Date();
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
//            just wake up early
            Thread.currentThread().interrupt();
        }
    }
}
