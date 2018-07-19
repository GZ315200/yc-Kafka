package com.unistack.tamboo.message.kafka.storage;


import com.google.common.collect.Maps;
import com.unistack.tamboo.message.kafka.bean.Callback;
import com.unistack.tamboo.message.kafka.exceptions.ConfigException;
import com.unistack.tamboo.message.kafka.runtime.AbstractStatus;
import com.unistack.tamboo.message.kafka.runtime.RunnerConfig;
import com.unistack.tamboo.message.kafka.runtime.RunnerStatus;
import com.unistack.tamboo.message.kafka.util.KafkaTopicBaseLog;
import com.unistack.tamboo.message.kafka.util.TopicAdmin;
import com.unistack.tamboo.message.kafka.util.json.GenericRecord;
import com.unistack.tamboo.message.kafka.util.json.JsonConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public class KafkaStatusBackingStore implements StatusBackingStore {


    public static final String RUNNER_STATUS_PREFIX = "status-runner-";

    private Time time;

    private final Map<String, CacheEntry<RunnerStatus>> runners;

    public static final String STATE_KEY_NAME = "state";
    public static final String TRACE_KEY_NAME = "trace";
    public static final String RUNNER_ID_KEY_NAME = "runner_id";

    private String topic;

    private KafkaTopicBaseLog<String, byte[]> kafkaTopicBaseLog;

    private JsonConverter converter = new JsonConverter();

    public static final Logger log = LoggerFactory.getLogger(KafkaStatusBackingStore.class);


    public KafkaStatusBackingStore(Time time) {
        this.time = time;
//        this.tasks = new Table<>();
        this.runners = new HashMap<>();
    }

    @Override
    public void configure(RunnerConfig config) {
        this.topic = config.getString(RunnerConfig.STATUS_STORAGE_TOPIC);

        if (StringUtils.isBlank(topic)) {
            throw new ConfigException("Status storage topic name must be specified.");
        }

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(config.originals());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // we handle retries in this class


        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(config.originals());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());


        Map<String, Object> adminProps = new HashMap<>(config.originals());
        NewTopic topicDescription = TopicAdmin.defineTopic(topic).
                compacted().
                partitions(config.getInt(RunnerConfig.STATUS_STORAGE_PARTITIONS_CONFIG)).
                replicationFactor(config.getShort(RunnerConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG)).
                build();

        Callback<ConsumerRecord<String, byte[]>> readCallback = new Callback<ConsumerRecord<String, byte[]>>() {
            @Override
            public void onCompletion(Throwable error, ConsumerRecord<String, byte[]> record) {
                read(record);
            }
        };

        this.kafkaTopicBaseLog = createKafkaBasedLog(topic, producerProps, consumerProps, readCallback, topicDescription, adminProps);
    }


    private KafkaTopicBaseLog<String, byte[]> createKafkaBasedLog(String topic, Map<String, Object> producerProps,
                                                                  Map<String, Object> consumerProps,
                                                                  Callback<ConsumerRecord<String, byte[]>> consumedCallback,
                                                                  final NewTopic topicDescription, final Map<String, Object> adminProps) {
        Runnable createTopics = () -> {
            log.debug("Creating admin client to manage Runner internal status topic.");
            try {
                try (TopicAdmin admin = new TopicAdmin(adminProps)) {
                    admin.createTopics(topicDescription);
                }
            } catch (Exception e) {
                log.error("Failed to create the ", e);
            }
        };
        return new KafkaTopicBaseLog<String, byte[]>(producerProps, consumerProps, consumedCallback, topic, time, createTopics);
    }


    private void read(ConsumerRecord<String, byte[]> record) {
        String key = record.key();
        if (key.startsWith(RUNNER_STATUS_PREFIX)) {
            readRunnerStatue(key, record.value());
        } else {
            log.warn("Discarding record with invalid key {}", key);
        }
    }


    private void readRunnerStatue(String key, byte[] value) {
        String runner = parseConnectorStatusKey(key);
        if (StringUtils.isBlank(runner)) {
            log.warn("Discarding record with invalid runner status key {}", key);
            return;
        }
        if (value == null) {
            log.trace("no status for this runner");
            return;
        }

        RunnerStatus status = parseRunnerStatus(runner, value);
        if (status == null) {
            return;
        }

        synchronized (this) {
            log.trace("Received runner {} status update {}", runner, status);
            CacheEntry<RunnerStatus> entry = getOrAdd(runner);

            entry.put(status);
        }

    }


    private RunnerStatus parseRunnerStatus(String runner, byte[] value) {
        try {
            GenericRecord record = converter.toConnectData(topic, value);
            if (!(record.content() instanceof Map)) {
                log.error("Invalid connector status type {}", record.content().getClass());
                return null;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> statusMap = (Map<String, Object>) record.content();
            RunnerStatus.State status = RunnerStatus.State.valueOf((String) statusMap.get(STATE_KEY_NAME));
            String trace = (String) statusMap.get(TRACE_KEY_NAME);
            return new RunnerStatus(runner, status, trace);
        } catch (Exception e) {
            log.error("Failed to deserialize connector status", e);
            return null;
        }
    }


    private String parseConnectorStatusKey(String key) {
        return key.substring(RUNNER_STATUS_PREFIX.length());
    }

    @Override
    public void start() {
        kafkaTopicBaseLog.start();

        // read to the end on startup to ensure that api requests see the most recent states
        kafkaTopicBaseLog.readToLogEnd();
    }

    @Override
    public void stop() {
        kafkaTopicBaseLog.stop();
    }

    @Override
    public void put(RunnerStatus status) {
        sendRunnerStatus(status, false);
    }

    @Override
    public void flush() {
        kafkaTopicBaseLog.flush();
    }


    private void sendRunnerStatus(final RunnerStatus runnerStatus, boolean safeWrite) {
        String runner = runnerStatus.runnerId();
        CacheEntry<RunnerStatus> entry = getOrAdd(runner);
        String key = RUNNER_STATUS_PREFIX + runner;
        send(key, runnerStatus, entry, safeWrite);
    }


    private synchronized CacheEntry<RunnerStatus> getOrAdd(String runner) {
        CacheEntry<RunnerStatus> entry = runners.get(runner);
        if (entry == null) {
            entry = new CacheEntry<>();
            runners.put(runner, entry);
        }
        return entry;
    }


    @Override
    public synchronized RunnerStatus get(String runner) {
        CacheEntry<RunnerStatus> entry = runners.get(runner);
        return entry == null ? null : entry.get();
    }


    private <V extends AbstractStatus> void send(final String key, final V status,
                                                 final CacheEntry<V> entry, final boolean safeWrite) {
        final int sequence;
        synchronized (this) {
            if (safeWrite && !entry.canWriteSafely(status)) {
                return;
            }
            sequence = entry.increment();
        }

        final byte[] value = status.state() == RunnerStatus.State.DESTROYED ? null : serialize(status);

        kafkaTopicBaseLog.send(key, value, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    if (exception instanceof RetriableException) {
                        synchronized (KafkaStatusBackingStore.this) {
                            if ((safeWrite && !entry.canWriteSafely(status, sequence))) {
                                return;
                            }
                        }
                        kafkaTopicBaseLog.send(key, value, this);
                    } else {
                        log.error("Failed to write status update", exception);
                    }
                }
            }
        });

    }


    private static class CacheEntry<T extends AbstractStatus> {
        private T value = null;
        private int sequence = 0;

        public int increment() {
            return ++sequence;
        }

        public void put(T value) {
            this.value = value;
        }

        public T get() {
            return value;
        }

        public boolean canWriteSafely(T status) {
            return value == null || value.runnerId().equals(status.runnerId());
        }

        public boolean canWriteSafely(T status, int sequence) {
            return canWriteSafely(status) && this.sequence == sequence;
        }

    }

    private byte[] serialize(AbstractStatus state) {
        Map<String, Object> data = Maps.newHashMap();
        data.put(STATE_KEY_NAME, state.state());
        data.put(TRACE_KEY_NAME, state.trace());
        data.put(RUNNER_ID_KEY_NAME, state.runnerId());
        return converter.fromConnectData(topic, data.toString().getBytes());
    }

}
