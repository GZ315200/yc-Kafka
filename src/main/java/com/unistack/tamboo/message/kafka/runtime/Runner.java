package com.unistack.tamboo.message.kafka.runtime;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unistack.tamboo.message.kafka.bean.ConsumerOffset;
import com.unistack.tamboo.message.kafka.storage.ConsumerGroupBackingStore;
import com.unistack.tamboo.message.kafka.storage.KafkaStatusBackingStore;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.apache.kafka.common.utils.Time;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 */
public class Runner {

    private KafkaStatusBackingStore kafkaStatusBackingStore;

    private String runnerId;

    private RunnerConfig runnerConfig;

    private Time time;

    private Map<String, Object> config;

    public Runner() {
    }

    public Runner(Map<String, Object> config) {
        this.config = config;
        runnerConfig = new RunnerConfig(this.config);
        this.runnerId = runnerConfig.getString(RunnerConfig.RUNNER_ID);
        time = Time.SYSTEM;
        this.kafkaStatusBackingStore = new KafkaStatusBackingStore(time);
    }


    /**
     * 获取所有group下所有consumer的消息消费信息
     *
     * @param bootstrapServers
     * @return
     */
    public List<ConsumerOffset> getConsumerGroups(String bootstrapServers, List<String> topicNames) {
        List<ConsumerOffset> list = Lists.newArrayList();
        ConsumerGroupBackingStore client = new ConsumerGroupBackingStore(bootstrapServers, topicNames);
        list.addAll(client.getConsumerGroups());
        client.close();
        return list;
    }


    public String getRunnerId() {
        return runnerId;
    }

    public void setConfig(String key, String value) {
        Map<String, Object> config = Maps.newHashMap();
        config.put(key, value);
        this.config = config;
    }

    public long getCurrentTimestamp() {
        return time.milliseconds();
    }

    public void startStoreStatus() {
        kafkaStatusBackingStore.configure(runnerConfig);
        kafkaStatusBackingStore.start();
    }

    public void stopStoreStatus() {
        kafkaStatusBackingStore.stop();
    }

    public void putStatus(RunnerStatus runnerStatus) {
        kafkaStatusBackingStore.put(runnerStatus);
    }

    public RunnerStatus getStatus(String runnerId) {
        return kafkaStatusBackingStore.get(runnerId);
    }


    public static void main(String[] args) {
        Map<String, Object> config = new HashMap<>();
        while (true) {
            config.put(RunnerConfig.RUNNER_ID, "zean.ma");
            config.put(RunnerConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, 1);
            config.put(RunnerConfig.STATUS_STORAGE_PARTITIONS_CONFIG, 1);
            Runner runner = new Runner(config);
//            System.out.println(runner.getConsumerGroups("192.168.1.101:9092", Arrays.asList("class-a")));

        }
    }

}
