package com.unistack.tamboo.message.kafka.runtime.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.unistack.tamboo.message.kafka.runtime.RunnerStatus;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 */
public class RunnerStateInfo {


    private String runnerHost;

    private RunnerStatus runnerStatus;


    @JsonCreator
    public RunnerStateInfo(@JsonProperty(value = "runner_host") String runnerHost,
                           @JsonProperty(value = "runner_status") RunnerStatus runnerStatus) {
        this.runnerHost = runnerHost;
        this.runnerStatus = runnerStatus;
    }

    @JsonProperty
    public String runnerHost() {
        return runnerHost;
    }

    @JsonProperty
    public RunnerStatus runnerStatus() {
        return runnerStatus;
    }

    public void setRunnerStatus(RunnerStatus runnerStatus) {
        this.runnerStatus = runnerStatus;
    }

    public void setRunnerHost(String runnerHost) {
        this.runnerHost = runnerHost;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("runnerHost", runnerHost)
                .add("runnerStatus", runnerStatus)
                .toString();
    }
}
