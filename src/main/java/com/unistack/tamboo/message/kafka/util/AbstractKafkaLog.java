package com.unistack.tamboo.message.kafka.util;


import com.unistack.tamboo.commons.utils.Callback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Gyges Zean
 * @date 2018/4/27
 */
public abstract class AbstractKafkaLog<K, V> {


    public static final Logger logger = LoggerFactory.getLogger(AbstractKafkaLog.class);


    public static final long CREATE_TOPIC_TIMEOUT_MS = 30000;


    protected Consumer<K, V> consumer;

    protected Map<String, Object> producerConfigs;

    protected Producer<K, V> producer;

    protected final Map<String, Object> consumerConfigs;

    protected Callback<ConsumerRecord<K, V>> consumerCallback;

    protected final String topic;

    protected org.apache.kafka.common.utils.Time time;

    protected Thread thread;

    protected boolean stopRequested;


    protected Runnable initializer;

    protected Map<TopicPartition, Long> endOffsets;


    public AbstractKafkaLog(Map<String, Object> consumerConfigs,
                            String topic, Time time,
                            Runnable initializer) {
        this.topic = topic;
        this.consumerConfigs = consumerConfigs;
        this.stopRequested = false;
        this.time = time;
        this.initializer = initializer != null ? initializer : () -> {

        };
    }


    public AbstractKafkaLog(Map<String, Object> producerConfigs, Map<String, Object> consumerConfigs,
                            Callback<ConsumerRecord<K, V>> consumerCallback,
                            String topic, org.apache.kafka.common.utils.Time time,
                            Runnable initializer) {
        this.topic = topic;
        this.consumerConfigs = consumerConfigs;
        this.producerConfigs = producerConfigs;
        this.consumerCallback = consumerCallback;
        this.stopRequested = false;
        this.time = time;
        this.initializer = initializer != null ? initializer : () -> {

        };
    }

    /**
     * 构建producer实例
     *
     * @return
     */
    protected Producer<K, V> createProducer() {
//        ack=all,保证数据能够写入耐久写入
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
//        不允许多个进行中的请求阻止重新排序（如果启用）
        producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new KafkaProducer<K, V>(producerConfigs);
    }


    /**
     * 构建consumer实例
     *
     * @return
     */
    protected Consumer<K, V> createConsumer() {
//      始终强制重置到日志的开头，因为此类想要使用所有可用的日志数据
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//      关闭自动提交，因为我们总是想要使用完整的日志
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<K, V>(consumerConfigs);
    }


    public void readToLogEnd() {
        logger.trace("Reading to end of offset log");

        Set<TopicPartition> assignment = consumer.assignment();
        endOffsets = consumer.endOffsets(assignment);
        logger.trace("Reading to end of offsets {}", endOffsets);

        while (!endOffsets.isEmpty()) {
            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                /*
                 * 最后一个消息offset为：offset + 1
                 * 获取将被提取的下一个记录的偏移量（如果存在具有该偏移量的记录）。
                 * 如果给定分区没有当前位置，则此方法可能会向服务器发出远程调用。
                 * 此通话将阻止，直到可以确定位置或发生不可恢复的错误
                 * 遇到（在这种情况下，它被抛出给调用者）。
                 */
                if (consumer.position(entry.getKey()) >= entry.getValue())
                    it.remove();
                else {
                    poll(Integer.MAX_VALUE);
                    break;
                }
            }
        }


    }


    public void poll(long timeoutMs) {

    }

    public void start() throws Exception {

    }


    public void stop() {

    }


}
