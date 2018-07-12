package com.unistack.tamboo.message.kafka.util;

import com.google.common.collect.Lists;

import com.unistack.tamboo.commons.utils.Callback;
import com.unistack.tamboo.message.kafka.exceptions.ConnectException;
import com.unistack.tamboo.message.kafka.exceptions.KafkaException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/27
 */
public class KafkaTopicBaseLog<K, V> extends AbstractKafkaLog<K, V> {

    public static final Logger log = LoggerFactory.getLogger(KafkaTopicBaseLog.class);

    public KafkaTopicBaseLog(Map<String, Object> producerConfigs, Map<String, Object> consumerConfigs,
                             Callback<ConsumerRecord<K, V>> consumerCallback, String topic,
                             Time time, Runnable initializer) {
        super(producerConfigs, consumerConfigs, consumerCallback, topic, time, initializer);
        this.producerConfigs = producerConfigs;
    }


    @Override
    public void start() {
        logger.info("Start KafkaBaseLog with topic" + topic);

        initializer.run();
        producer = createProducer();
        consumer = createConsumer();

        List<TopicPartition> partitions = Lists.newArrayList();

        List<PartitionInfo> partitionInfos = null;
        long started = time.milliseconds();
        while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
            partitionInfos = consumer.partitionsFor(topic);
            Utils.sleep(Math.min(time.milliseconds() - started, 1000));
        }
        if (partitionInfos == null)
            throw new ConnectException("Could not look up partition metadata for offset backing store topic in" +
                    " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if" +
                    " this is your first use of the topic it may have taken too long to create.");
        for (PartitionInfo partitionInfo : partitionInfos) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(partitions);

        readToLogEnd();

        thread = new RunnerThread();
        thread.start();

        logger.info("Finished reading KafkaBasedLog for topic " + topic);
        logger.info("Started KafkaBasedLog for topic " + topic);
    }


    @Override
    public void stop() {
        log.info("Stopping KafkaBasedLog for topic " + topic);

        synchronized (this) {
            stopRequested = true;
        }

        consumer.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new ConnectException("Failed to stop KafkaBasedLog. Exiting without cleanly shutting " +
                    "down it's producer and consumer.", e);
        }
        try {
            producer.close();
        } catch (org.apache.kafka.common.KafkaException e) {
            log.error("Failed to stop KafkaBasedLog producer", e);
        }

        try {
            consumer.close();
        } catch (org.apache.kafka.common.KafkaException e) {
            log.error("Failed to stop KafkaBasedLog consumer", e);
        }

        log.info("Stopped KafkaBasedLog for topic " + topic);

    }


    /**
     * Flush the underlying producer to ensure that all pending writes have been sent.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * send status to kafka status topic
     * @param key
     * @param value
     */
    public void send(K key, V value) {
        send(key, value, null);
    }

    /**
     * end status to kafka status topic
     *
     * @param key
     * @param value
     * @param callback
     */
    public void send(K key, V value, org.apache.kafka.clients.producer.Callback callback) {
        producer.send(new ProducerRecord<>(topic, key, value), callback);
    }


    @Override
    public void poll(long timeoutMs) {
        try {
            ConsumerRecords<K, V> records = consumer.poll(timeoutMs);
            commit();
            for (ConsumerRecord<K, V> record : records)
                consumerCallback.onCompletion(null, record);
        } catch (WakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            logger.error("Error polling: " + e);
        }
    }


    public void commit() {
        if (consumer != null) {
            consumer.commitSync();
        }
    }


    private class RunnerThread extends Thread {
        public RunnerThread() {
            super("kafkaBaseLog Runner Thread - " + topic);
        }

        @Override
        public void run() {
            try {
                logger.trace("{} started execution", this);
                while (true) {
                    int numCallbacks;
                    synchronized (KafkaTopicBaseLog.class) {
                        if (stopRequested)
                            break;
                    }

                    try {
                        poll(Integer.MAX_VALUE);
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                }

            } catch (Throwable t) {
                logger.error("Unexpected exception in {}", this, t);
            }
        }
    }

}
