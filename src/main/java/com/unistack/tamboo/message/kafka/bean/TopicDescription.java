package com.unistack.tamboo.message.kafka.bean;

import com.google.common.base.MoreObjects;

import java.util.List;

/**
 * @author Gyges Zean
 * @date 2018/5/3
 * A detailed description of a single topic in the cluster.
 */
public class TopicDescription {

    private boolean internal;
    private List<TopicPartitionInfo> partitions;


    public TopicDescription() {
    }


    public TopicDescription(boolean internal, List<TopicPartitionInfo> partitions) {
        this.internal = internal;
        this.partitions = partitions;
    }


    public boolean isInternal() {
        return internal;
    }

    public List<TopicPartitionInfo> getPartitions() {
        return partitions;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
    }

    public void setPartitions(List<TopicPartitionInfo> partitions) {
        this.partitions = partitions;
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("internal", internal)
                .add("partitions", partitions)
                .toString();
    }
}
