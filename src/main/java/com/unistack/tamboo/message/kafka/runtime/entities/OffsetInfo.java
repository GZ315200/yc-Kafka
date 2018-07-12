package com.unistack.tamboo.message.kafka.runtime.entities;

import com.google.common.base.MoreObjects;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 * offset info
 */
public class OffsetInfo {

    private String topic;

    private int partition;

    private int offset;

    private String value;

    private long timestamp;

    public OffsetInfo(String topic, int partition, int offset, String value, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.value = value;
        this.timestamp = timestamp;
    }


    public OffsetInfo() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("partition", partition)
                .add("offset", offset)
                .add("value", value)
                .add("timestamp", timestamp)
                .toString();
    }
}
