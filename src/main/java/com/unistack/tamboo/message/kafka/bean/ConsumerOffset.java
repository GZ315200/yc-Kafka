package com.unistack.tamboo.message.kafka.bean;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Gyges Zean
 * @date 2018/5/24
 */
public class ConsumerOffset {

    private String group;
    private String topic;
    private int partition;
    private long offset;
    private long logEndOffset;
    private long logStartOffset;
    private long timestamp;

    private String consumerId;
    // expanded fields from consumerId
    private String ip;
    private String pid;
    private String tid;

    public ConsumerOffset(String group, String topic, int partition, long offset, long logEndOffset,
                          long logStartOffset, String consumerId) {
        super();
        this.group = group;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.logEndOffset = logEndOffset;
        this.logStartOffset = logStartOffset;
        this.timestamp = System.currentTimeMillis();
        setConsumerId(consumerId);
    }


    public String getIp() {
        return ip;
    }

    public String getPid() {
        return pid;
    }

    public String getTid() {
        return tid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
        try {
            String regex = "(.*)_([0-9]+)@(.*)_([0-9]+)-(.*)_(.*)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(consumerId);
            if (matcher.find()) {
                ip = matcher.group(6).replaceAll("\\/", "");
                pid = matcher.group(2);
                tid = matcher.group(4);
            }
        } catch (Exception e) {
        }

    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    public void setLogEndOffset(long logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public long getLogStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String toString() {
        return "group = " + group + ", topic = " + topic + ", partition = " + partition + ", offset = " + offset
                + ", logEndOffset = " + logEndOffset + ", logStartOffset = " + logStartOffset + ", consumerId = "
                + consumerId;
    }
}
