package com.unistack.tamboo.message.kafka.util.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unistack.tamboo.message.kafka.errors.DataException;


import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenericRecord implements Serializable, Comparable<GenericRecord> {

    private static final long serialVersionUID = -166701310989888706L;

    private ObjectMapper o = new ObjectMapper();

    private String topic;

    private Map<String, String> metaInfo;

    private byte[] content;

    @JsonIgnore
    private String entityName;

    private Long timestamp;


    public GenericRecord() {
    }

    public GenericRecord(String topic, Map<String, String> metaInfo, byte[] content, String entityName,
                         Long timestamp) {
        this.topic = topic;
        this.metaInfo = metaInfo;
        this.content = content;
        this.entityName = entityName;
        this.timestamp = timestamp == null ? System.currentTimeMillis() : timestamp;
    }


    public String getTopic() {
        return topic;
    }

    public Map<String, String> getMetaInfo() {
        return metaInfo;
    }

    public byte[] getContent() {
        return content;
    }

    public Object content() {
        try {
           return o.readValue(getContent(),Object.class);
        } catch (IOException e) {
            throw new DataException("Converting string to object failed.", e);
        }
    }

    public String getEntityName() {
        return entityName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static class GenericRecordBuilder {

        private String topic;

        private Map<String, String> metaInfo;

        private byte[] content;

        private String entityName;

        private Long timestamp;

        GenericRecordBuilder(String topic) {
            this.topic = topic;
        }


        public GenericRecordBuilder metaInfo(Map<String, String> o) {
            this.metaInfo = o;
            return this;
        }

        public GenericRecordBuilder topic(String o) {
            this.topic = o;
            return this;
        }

        public GenericRecordBuilder content(Object o) {
            this.content = (byte[]) o;
            return this;
        }


        public GenericRecordBuilder entityName(String o) {
            this.entityName = o;
            return this;
        }


        public GenericRecordBuilder timestamp(Long o) {
            this.timestamp = o;
            return this;
        }


        public GenericRecord build() {
            return new GenericRecord(topic, metaInfo, content, entityName, timestamp);
        }

    }


    public static GenericRecordBuilder instance(String topic) {
        return new GenericRecordBuilder(topic);
    }


    public String toString() {
        String contentStr;
        if (content == null) contentStr = "";
        else contentStr = new String(content);
        return "topic: " + topic + ", content: " + contentStr + ", metaInfo = " + metaInfo;
    }

    @JsonIgnore
    private long size;

    public long getSize() {
        long contentSize = this.getContent() == null ? 0 : this.getContent().length;
        long metaSize = 0;
        Map<String, String> metaInfo = this.getMetaInfo();
        if (metaInfo != null) {
            for (Map.Entry<String, String> entry : metaInfo.entrySet()) {
                metaSize += entry.getValue().length() + entry.getKey().length();
            }
        }
        return contentSize + metaSize;
    }

    public void setSize(long size) {
        this.size = size;
    }


    public int compareTo(GenericRecord data) {
        return new String(this.topic).compareTo(new String(data.topic));
    }


    public static void main(String[] args) {
        ObjectMapper o = new ObjectMapper();
        String s = "hhhhhhhhhhhhh";
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        GenericRecord genericRecord = GenericRecord.instance("test")
                .build();

        try {
            System.out.println(o.writeValueAsString(genericRecord));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
