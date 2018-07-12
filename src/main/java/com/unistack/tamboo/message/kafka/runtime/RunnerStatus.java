package com.unistack.tamboo.message.kafka.runtime;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public class RunnerStatus extends AbstractStatus<String> {


    public RunnerStatus(String runnerId, State state, String msg) {
        super(runnerId, state, msg);
    }

}
