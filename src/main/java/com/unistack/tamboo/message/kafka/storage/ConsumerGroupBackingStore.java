package com.unistack.tamboo.message.kafka.storage;

import com.google.common.collect.Lists;
import com.unistack.tamboo.message.kafka.bean.ConsumerOffset;
import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static com.unistack.tamboo.message.kafka.util.CommonUtils.getSecurityProps;


/**
 * @author Gyges Zean
 * @date 2018/5/24
 */
public class ConsumerGroupBackingStore implements OffsetBackingStore {


    public static final Logger logger = LoggerFactory.getLogger(ConsumerGroupBackingStore.class);


    private AdminClient adminClient = null;
    private String bootstrapServers;
    private List<String> topicList;

    public ConsumerGroupBackingStore(String bootstrapServers, List<String> topicNames) {
        Properties props = new Properties();
        this.bootstrapServers = bootstrapServers;

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(getSecurityProps(bootstrapServers));
        this.adminClient = AdminClient.create(props);
        topicList = topicNames;
    }


    /**
     * 获取消息信息
     */
    @Override
    public List<ConsumerOffset> getConsumerGroups() {
        List<ConsumerOffset> offsetList = Lists.newArrayList();
        for (GroupOverview go : JavaConversions.asJavaCollection(adminClient.listAllConsumerGroupsFlattened())) {
            offsetList.addAll(getOffsets(go.groupId()));
        }
        return offsetList;
    }

    @Override
    public void close() {
        adminClient.close();
    }

//    private List<String> getTopicList(String bootstrapServers, List<String> groupIds) {
//        List<String> topicList = Lists.newArrayList();
//        KafkaConsumer<byte[], byte[]> consumer = null;
//        try {
//            for (String g : groupIds) {
//                consumer = getKafkaConsumer(bootstrapServers, g);
//                topicList.addAll(consumer.listTopics().keySet());
//            }
//        } finally {
//            if (consumer != null) {
//                consumer.close();
//            }
//        }
//        return topicList;
//    }


    /**
     * 返回consumer实例
     *
     * @param bootstrapServers
     * @param groupId
     * @return
     */
    private KafkaConsumer<byte[], byte[]> getKafkaConsumer(String bootstrapServers, String groupId) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.putAll(getSecurityProps(bootstrapServers));

        return new KafkaConsumer<byte[], byte[]>(properties);
    }


    private boolean preCheck(TopicPartition tp) {
        boolean passed = true;

        if (!topicList.contains(tp.topic())) {
            logger.warn(tp.topic() + " is deleted, skip to get it's offsets.");
            passed = false;
        }
        return passed;
    }

    /**
     * 获取指定group中的consumer消费信息的offset
     *
     * @param group
     * @return
     */
    private List<ConsumerOffset> getOffsets(String group) {
        List<ConsumerOffset> offsetList = Lists.newArrayList();
        KafkaConsumer<?, ?> consumer = null;

        AdminClient.ConsumerGroupSummary consumerSummaries = adminClient
                .describeConsumerGroup(group, 100);

        Collection<AdminClient.ConsumerSummary> summaries = JavaConversions.asJavaCollection(consumerSummaries.consumers().get());

        if (summaries == null) {
            System.out.println(("Consumer group " + group + " does not exist or is rebalancing."));
        } else {
            consumer = getKafkaConsumer(bootstrapServers, group);
            for (AdminClient.ConsumerSummary summary : summaries) {
                for (TopicPartition tp : JavaConversions.asJavaCollection(summary.assignment())) {
                    if (!preCheck(tp)) {
                        continue;
                    }
                    //获取Offset的元数据信息
                    OffsetAndMetadata oam = consumer.committed(tp);
                    if (oam == null) {
                        System.out.println(group + " has no offset on " + tp + ", consumer = " + summary.consumerId());
                        continue;
                    }
                    //当前topic消费的offset
                    long offset = oam.offset();
                    consumer.assign(Lists.newArrayList(tp));

                    long logEndOffset = consumer.endOffsets(Lists.newArrayList(tp)).get(tp);
                    long logStartOffset = consumer.beginningOffsets(Lists.newArrayList(tp)).get(tp);
//                    consumer.seekToEnd(Lists.newArrayList(tp));
//                    //topic的LEO
//                    long logEndOffset = consumer.position(tp);
//                    consumer.seekToBeginning(Lists.newArrayList(tp));
//                    //topic的LSO
//                    long logStartOffset = consumer.position(tp);
                    ConsumerOffset consumerOffset = new ConsumerOffset(group, tp.topic(), tp.partition(),
                            offset, logEndOffset, logStartOffset, summary.consumerId() + "_" + summary.host());
                    offsetList.add(consumerOffset);
                }
            }
            consumer.close();
        }
        return offsetList;
    }


}
