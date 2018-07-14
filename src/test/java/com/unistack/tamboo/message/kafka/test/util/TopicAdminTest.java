package com.unistack.tamboo.message.kafka.test.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unistack.tamboo.message.kafka.util.TopicAdmin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.unistack.tamboo.message.kafka.util.ConfigHelper.jaasConfigProperty;

/**
 * @author Gyges Zean
 * @date 2018/5/8
 */
public class TopicAdminTest {

    private Map<String, Object> map = new HashMap<>();

    private TopicAdmin topicAdmin = null;

    private ObjectMapper o = null;

    @Before
    public void init() {
        map.put("bootstrap.servers", "192.168.1.110:9093");
//        map.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin123\";");
        map.put("sasl.mechanism","PLAIN");
        map.put("security.protocol","SASL_PLAINTEXT");
        map.put("sasl.jaas.config", jaasConfigProperty("PLAIN", "admin", "admin123").value());
        this.topicAdmin = new TopicAdmin(map);
        this.o = new ObjectMapper();
    }

    @Test
    public void createTopics() {

//        NewTopic newTopic = TopicAdmin
//                .defineTopic("zean-test")
//                .minInSyncReplica((short) 1)
//                .partitions(1)
//                .replicationFactor((short) 1)
//                .build();

        NewTopic newTopic1 = TopicAdmin
                .defineTopic("test")
                .minInSyncReplica((short) 1)
                .partitions(1)
                .replicationFactor((short) 1)
                .deleted()
                .build();

        Set<String> set = topicAdmin.createTopics(newTopic1);
        System.out.println(set);
    }

    @Test
    public void createTopic() {

        NewTopic newTopic = TopicAdmin
                .defineTopic("yh2")
                .minInSyncReplica((short) 1)
                .partitions(1)
                .replicationFactor((short) 1)
                .build();

        boolean isCreate = topicAdmin.createTopic(newTopic);
        System.out.println(isCreate);
    }


    @Test
    public void describeAclTopic() {
        List<AclBinding> aclBindingList = topicAdmin.describeAcl("yh");
        Assert.assertNotNull(aclBindingList);
        System.out.println(aclBindingList.toString());
    }


    @Test
    public void describeTopic() {
        ObjectMapper o = new ObjectMapper();
        try {
            System.out.println(o.writeValueAsString(topicAdmin.describeTopic("test")));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTopicAcl() {
        boolean a = topicAdmin.deleteTopicAcl("tamboo_consumer_metrics");
        System.out.println(a);
    }

    /**
     * [tamboo_consumer_metrics, tamboo_producer_metrics, test, tamboo_config, unistack, test1]

     */
    @Test
    public void deleteTopic() {
        boolean a = topicAdmin.deleteTopic("tamboo_config");
        System.out.println(a);
    }


    @Test
    public void listCluster() {
        System.out.println(topicAdmin.listCluster().getClusterId());
    }

    @Test
    public void createTopicAcl() {
        System.out.println(topicAdmin.createTopicForAcl("tamboo_sanitycheck", "admin"));
    }

    @Test
    public void listAllTopics() {
        Set<String> topics = topicAdmin.listAllTopics();
        System.out.println(topics.toString());
    }

    @Test
    public void bootstrapServer() {
        try {
            System.out.println(o.writeValueAsString(topicAdmin.brokers));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }


    @Test
    public  void alterBroker() {
        topicAdmin.alertBrokersSaslConfig(Arrays.asList("0"),"zean","zean123");
    }

    @Test
    public void createBrokerAcl() {
        System.out.println(topicAdmin.createBrokerForAcl("15FexXCBTlir7-lrNVBUvw"));
    }

    @Test
    public void aleterConfig() {
        topicAdmin.alertTopicSaslConfig("class-a","admin","admin123");
    }


    @Test
    public void describeAcl() {
        System.out.println(topicAdmin.describeBrokerAcl("192.168.1.101:9092,192.168.1.101:9093"));
    }

    @Test
    public void describeLogDir() {
        Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfo = topicAdmin.describeLogDir(Arrays.asList("1")).get(1);
        System.out.println(logDirInfo.get("tamboo_sanitycheck-0"));
        try {
            System.out.println(o.writeValueAsString(logDirInfo));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}
