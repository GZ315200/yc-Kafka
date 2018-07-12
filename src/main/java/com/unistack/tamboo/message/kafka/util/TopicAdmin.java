package com.unistack.tamboo.message.kafka.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.unistack.tamboo.message.kafka.bean.ClusterDescription;
import com.unistack.tamboo.message.kafka.bean.TopicDescription;
import com.unistack.tamboo.message.kafka.bean.TopicPartitionInfo;
import com.unistack.tamboo.message.kafka.exceptions.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.unistack.tamboo.message.kafka.util.ConfigHelper.jaasConfigProperty;


/**
 * @author Gyges Zean
 * @date 2018/4/24
 * Utility to simplify create and managing topics via the {@link org.apache.kafka.clients.admin.AdminClient}.
 */
public class TopicAdmin implements AutoCloseable {

    public static final Logger log = LoggerFactory.getLogger(TopicAdmin.class);

    public static final int OPERATION_TIMEOUT = 60 * 1000;

    public static final String PRINCIPAL_PREFIX = "User:";

    public static Map<Integer, String> brokers = new HashMap<>();


    /**
     * A builder of {@link org.apache.kafka.clients.admin.NewTopic} instances.
     */
    public static class NewTopicBuilder {
        private String name;//topic name
        private int numPartitions = 0; //topic partition number
        private short replicationFactor;// topic replication number.
        private Map<String, String> configs = Maps.newHashMap();//topic configuration

        /**
         * constructor with topic name
         *
         * @param name
         */
        public NewTopicBuilder(String name) {
            this.name = name;
        }

        /**
         * Specify the desired number of partitions for the topic.
         *
         * @param numPartitions the desired number of partitions; must be positive
         * @return this builder to allow methods to be chained;never null.
         */
        public NewTopicBuilder partitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Specify the desired replication factor for the topic.
         *
         * @param replicationFactor the desired number of replicationFactor;must be positive.
         * @return this builder to allow methods to be chained;never null.
         */
        public NewTopicBuilder replicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        /**
         * Specify the clean.policy configuration for topic, should be compacted
         *
         * @return
         */
        public NewTopicBuilder compacted() {
            this.configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            return this;
        }


        public NewTopicBuilder deleted() {
            this.configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
            return this;
        }


        /**
         * Specify the min in-sync replicas required for topic.
         *
         * @param minInSyncReplicas
         * @return
         */
        public NewTopicBuilder minInSyncReplica(short minInSyncReplicas) {
            this.configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minInSyncReplicas));
            return this;
        }

        /**
         * Specify the whether the broker is allowed to elect a leader
         * that was not an in-sync replica when no ISRs
         *
         * @param allow true if unclean leaders can be elected, or false if they are not allowed
         * @return
         */
        public NewTopicBuilder uncleanLeaderElection(boolean allow) {
            this.configs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Boolean.toString(allow));
            return this;
        }

        /**
         * Specify the configuration properties for the topic.
         * overwriting any previously-set properties.
         *
         * @param configs
         * @return
         */
        public NewTopicBuilder config(Map<String, Object> configs) {
            if (!Objects.isNull(configs)) {
                for (Map.Entry<String, Object> entry : configs.entrySet()) {
                    Object value = entry.getValue();
                    this.configs.put(entry.getKey(), value != null ? value.toString() : null);
                }
            } else {
                this.configs.clear();
            }
            return this;
        }

        public NewTopic build() {
            return new NewTopic(
                    name, numPartitions == 0 ? 1 : numPartitions, replicationFactor == 0 ? 1 : replicationFactor).configs(configs);
        }
    }

    /**
     * Obtain a {@link NewTopicBuilder builder} to define a {@link NewTopic}
     *
     * @param topicName the name of the topic
     * @return
     */
    public static NewTopicBuilder defineTopic(String topicName) {
        return new NewTopicBuilder(topicName);
    }

    private final Map<String, Object> adminConfig;

    private final AdminClient admin;

    /**
     * create a TopicAdmin instance by adminConfig
     *
     * @param adminConfig
     */
    public TopicAdmin(Map<String, Object> adminConfig) {
        this(adminConfig, AdminClient.create(adminConfig));
    }

    /**
     * create a TopicAdmin instance by adminConfig & adminClient.
     *
     * @param adminConfig
     * @param adminClient
     */
    public TopicAdmin(Map<String, Object> adminConfig, AdminClient adminClient) {
        this.admin = adminClient;
        this.adminConfig = adminConfig != null ? adminConfig : Collections.emptyMap();
    }

    /**
     * if topic is created successfully or not.
     *
     * @param topic
     * @return
     */
    public boolean createTopic(NewTopic topic) {
        if (topic == null) return false;
        Set<String> newTopicNames = createTopics(topic);
        return newTopicNames.contains(topic.name());
    }

    /**
     * if topic is deleted successfully or not.
     *
     * @param topic
     * @return
     */
    public boolean deleteTopic(String topic) {
        if (StringUtils.isBlank(topic)) return false;
        Set<String> newTopicNames = deleteTopics(topic);
        return newTopicNames.contains(topic);
    }

    /**
     * if topic acl is created successfully or not.
     *
     * @param topic
     * @param user
     * @return
     */
    public boolean createTopicForAcl(String topic, String user) {
        if (StringUtils.isBlank(topic)) return false;
        createGroupForAcl();//默认创建所有group都可以访问
        String topicName = createTopicForAcls(topic, "", null, null, user);
        return topicName.equalsIgnoreCase(topic);
    }


    public boolean createBrokerForAcl(String... users) {
        String cluster = createBrokersForAcls("", null, null, users);
        if (StringUtils.isBlank(cluster)) {
            return false;
        }
        return true;
    }


    /**
     * Attempt to create the topics described by the given definitions, returning all of the names of
     * those topics that were created by this request.
     *
     * @param topics
     * @return
     */
    public Set<String> createTopics(NewTopic... topics) {
        Map<String, NewTopic> topicByName = Maps.newHashMap();
        if (topics != null) {
            for (NewTopic topic : topics) {
                if (topic != null) {
                    topicByName.put(topic.name(), topic);
                }
            }
        }
        if (topicByName.isEmpty()) return Collections.emptySet();
        String bootstrapServers = bootstrapServers();
        String topicNameList = Utils.join(topicByName.keySet(), ",");

        // Attempt to create any missing topics
        CreateTopicsOptions args = new CreateTopicsOptions().
                validateOnly(false);
        Map<String, KafkaFuture<Void>> newResults = admin.createTopics(topicByName.values(), args).values();

        //遍历future，对每个错误拦截处理
        Set<String> newlyCreateTopicNames = Sets.newHashSet();
        for (Map.Entry<String, KafkaFuture<Void>> entry : newResults.entrySet()) {
            String topic = entry.getKey();
            try {
                entry.getValue().get();
                log.info("Create topic {} on brokers at {}", topic, bootstrapServers);
                newlyCreateTopicNames.add(topic);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TopicExistsException) {
                    log.debug("Found existing topic '{}' on the brokers at {}", topic, bootstrapServers);
                    continue;
                }
                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to create topic(s) '{}' since the brokers at {} " +
                                    "do not support the CreateTopics API.", "Falling back assume topic(s) exist or will be auto-created by the broker.",
                            topicNameList, bootstrapServers);
                    return Collections.emptySet();
                }

                if (cause instanceof ClusterAuthorizationException) {
                    log.debug("Not authorized to create topic(s) '{}'." +
                                    "Falling back to assume topic(s) exist or will be auto-created by broker."
                            , topicNameList, bootstrapServers);
                    return Collections.emptySet();
                }

                if (cause instanceof TimeoutException) {
                    throw new ConnectException("Timed out while checking for or creating topic(s) '" + topicNameList + "'." +
                            " This could indicate a connectivity issue, unavailable topic partitions, or if" +
                            " this is your first use of the topic it may have taken too long to create.", cause);
                }
                throw new ConnectException("Error while attempting to create/find topic(s) '" + topicNameList + "'", e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create/find topics(s) " + topicNameList + "", e);
            }
        }
        return newlyCreateTopicNames;
    }


    /**
     * Attempt to delete the topics,returning all of the names of
     * those topics that were deleted by this request.
     *
     * @param topics
     * @return
     */
    public Set<String> deleteTopics(String... topics) {

        Set<String> topicNames = Sets.newHashSet();
        if (topics != null) {
            for (String topic : topics) {
                if (topic != null) {
                    topicNames.add(topic);
                }
            }
        }
        if (topicNames.isEmpty()) return Collections.emptySet();
        String bootstrapServers = bootstrapServers();
        String topicNameList = Utils.join(topicNames, ",");

        DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions()
                .timeoutMs(OPERATION_TIMEOUT);
        Set<String> newDeleteTopicNames = Sets.newHashSet();
        Map<String, KafkaFuture<Void>> newResults = admin.deleteTopics(topicNames, deleteTopicsOptions).values();

        for (Map.Entry<String, KafkaFuture<Void>> entry : newResults.entrySet()) {
            String topic = entry.getKey();
            try {
                entry.getValue().get();
                log.info("delete topic {} on brokers at {}", topic, bootstrapServers);
                newDeleteTopicNames.add(topic);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to delete topic(s) '{}' since the brokers at {} " +
                                    "do not support the CreateTopics API.",
                            topicNameList, bootstrapServers);
                    return Collections.emptySet();
                }

                if (cause instanceof TopicAuthorizationException) {
                    log.debug("Not authorized to delete topic(s) '{}' on brokers at '{}'."
                            , topicNameList, bootstrapServers);
                    return Collections.emptySet();
                }

                if (cause instanceof UnknownTopicOrPartitionException) {
                    log.debug("This topic(s) '{}' doesn't exist on brokers at '{}'.", topicNameList, bootstrapServers);
                    return Collections.emptySet();
                }

                if (cause instanceof TimeoutException) {
                    throw new ConnectException("Timed out while checking for or deleting topic(s) '" + topicNameList + "'.", cause);
                }

                throw new ConnectException("Error while attempting to delete topic(s) '" + topicNameList + "'", e);

            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to delete topics(s) " + topicNameList + "", e);
            }
        }
        return newDeleteTopicNames;
    }


    /**
     * Attempt to describe the topics,returning all of the names and description of
     * those topics that were described by this request.
     *
     * @param topics
     * @return
     */
    public Map<String, TopicDescription> describeTopic(String... topics) {
        Set<String> topicNames = Sets.newHashSet();
        if (topics != null) {
            for (String topic : topics) {
                if (topic != null) {
                    topicNames.add(topic);
                }
            }
        }
        if (topicNames.isEmpty()) return Collections.emptyMap();
        String bootstrapServers = bootstrapServers();
        String topicNameList = Utils.join(topicNames, ",");

        DescribeTopicsOptions describeTopicsOptions = new DescribeTopicsOptions().timeoutMs(OPERATION_TIMEOUT);

        Map<String, TopicDescription> descriptionTopic = Maps.newHashMap();
        TopicDescription td = new TopicDescription();

        Map<String, KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>>
                result = admin.describeTopics(topicNames, describeTopicsOptions).values();

        result.forEach((key, value) -> {
            List<TopicPartitionInfo> partitionInfos = Lists.newArrayList();
            try {
                td.setInternal(value.get().isInternal());

                value.get().partitions().forEach(topicPartitionInfo -> {
                    TopicPartitionInfo info = TopicPartitionInfo
                            .defineTopicForPartition(topicPartitionInfo.partition())
                            .leader(topicPartitionInfo.leader())
                            .replicas(topicPartitionInfo.replicas())
                            .isr(topicPartitionInfo.isr())
                            .build();
                    partitionInfos.add(info);
                });
                td.setPartitions(partitionInfos);
                descriptionTopic.put(value.get().name(), td);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to delete topic(s) '{}' since the brokers at {} " +
                                    "do not support the CreateTopics API.",
                            topicNameList, bootstrapServers);
                }
                if (cause instanceof UnknownTopicOrPartitionException) {
                    log.debug("This topic(s) '{}' doesn't exist on brokers at '{}'.", topicNameList, bootstrapServers);
                }

                if (cause instanceof InvalidTopicException) {
                    log.debug("The client has attempted to perform an operation on an invalid topic '{}' on brokers at '{}'.", topicNameList, bootstrapServers);
                }

                if (cause instanceof TimeoutException) {
                    throw new ConnectException("Timed out while checking for or describing topic(s) '" + topicNameList + "'.", cause);
                }
                throw new ConnectException("Error while attempting to describe topic(s) '" + topicNameList + "'", e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to describe topics(s) " + topicNameList + "", e);
            }
        });
        return descriptionTopic;
    }


    /**
     * Attempt to list the topics,returning all of the names of
     * those topics that were listed by this request.
     *
     * @return
     */
    public Set<String> listAllTopics() {
        Set<String> newTopicNames = null;
        ListTopicsOptions topicsOptions = new ListTopicsOptions()
                .listInternal(false).timeoutMs(OPERATION_TIMEOUT);
        try {
            newTopicNames = admin.listTopics(topicsOptions).names()
                    .get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new ConnectException("Timed out while checking for or listAllTopics'.", cause);
            }
            throw new ConnectException("Error while attempting to list topic(s)", e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to list topics", e);
        }
        return newTopicNames;
    }


    /**
     * Attempt to list the clusters,returning all of the names and their node of
     * those clusters that were listed by this request.
     *
     * @return
     */
    public ClusterDescription listCluster() {
        ClusterDescription clusterDescription = null;
        DescribeClusterOptions options = new DescribeClusterOptions()
                .timeoutMs(OPERATION_TIMEOUT);
        DescribeClusterResult result = admin.describeCluster(options);
        try {
            clusterDescription = new ClusterDescription()
                    .defineForClusterId(result.clusterId().get())
                    .nodes(result.nodes().get())
                    .controller(result.controller().get())
                    .build();

        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new ConnectException("Timed out while checking for or listCluster'.", cause);
            }
            throw new ConnectException("Error while attempting to list clusters", e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to list clusters", e);
        }
        return clusterDescription;
    }


    public List<String> listBootstrapServers() {
        List<String> servers = Lists.newArrayList();
        ClusterDescription description = listCluster();
        description.getClusterNode().forEach(clusterNode -> {
            servers.add(clusterNode.getHosts());
        });
        return servers;
    }


    public List<String> listBrokerIds() {
        List<String> brokerIds = Lists.newArrayList();
        ClusterDescription description = listCluster();
        description.getClusterNode().forEach(clusterNode -> {
            brokerIds.add(clusterNode.getIdString());
        });
        return brokerIds;
    }


    public Map<String, Integer> getHostToIdInfo() {
        ClusterDescription description = listCluster();
        Map<String, Integer> data = Maps.newHashMap();
        description.getClusterNode().forEach(clusterNode -> {
            data.put(clusterNode.getHosts(), Integer.valueOf(clusterNode.getIdString()));
        });
        return data;
    }

    public Map<Integer, String> getIdToHostInfo() {
        ClusterDescription description = listCluster();
        Map<Integer, String> data = Maps.newHashMap();
        description.getClusterNode().forEach(clusterNode -> {
            data.put(Integer.valueOf(clusterNode.getIdString()), clusterNode.getHosts());
        });
        return data;
    }


    /**
     * Attempt to describe the logDir,returning all of the names and their logDir info that were listed by this request.
     *
     * @param brokerIds
     * @return
     */
    public Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> describeLogDir(List<String> brokerIds) {
        Set<Integer> ids = Sets.newHashSet();

        brokerIds.forEach(s -> {
            ids.add(Integer.valueOf(s));
        });

        DescribeLogDirsOptions logDirsOptions = new DescribeLogDirsOptions()
                .timeoutMs(OPERATION_TIMEOUT);

        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirInfoMap = Maps.newHashMap();

        Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> result = admin.
                describeLogDirs(ids, logDirsOptions).values();

        for (Map.Entry<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> entry : result.entrySet()) {
            Integer brokerId = entry.getKey();
            try {
                log.debug("describe the LogDir for this broker {}", brokers.get(brokerId));
                logDirInfoMap.put(brokerId, entry.getValue().get());
            } catch (ExecutionException e) {
                log.error("", e);
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    throw new ConnectException("Timed out while checking for or describeLogDir'.", cause);
                }
                log.debug("Error while attempting to describe logDir", e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to describe logDir", e);
            }
        }
        return logDirInfoMap;
    }


    /**
     * Attempt to describe the acl list,returning all of the names and their acl info
     * that were listed by this request.
     *
     * @param topic
     * @return
     */
    public List<AclBinding> describeAcl(String topic) {
        ResourceFilter resourceFilter = new ResourceFilter(ResourceType.TOPIC, topic);
        AccessControlEntryFilter accessControlEntryFilter =
                AccessControlEntryFilter.ANY;
        AclBindingFilter aclBindingFilter = new AclBindingFilter(resourceFilter, accessControlEntryFilter);
        DescribeAclsOptions options = new DescribeAclsOptions()
                .timeoutMs(OPERATION_TIMEOUT);
        List<AclBinding> aclBindings = Lists.newArrayList();
        KafkaFuture<Collection<AclBinding>> future = admin.describeAcls(aclBindingFilter, options).values();
        try {
            aclBindings.addAll(future.get());
        } catch (ExecutionException e) {
            log.error("", e);
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new ConnectException("Timed out while checking for or describeAcl'.", cause);
            }
            log.debug("Error while attempting to describe acl", e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to describe acl", e);
        }
        return aclBindings;
    }

    public List<AclBinding> describeBrokerAcl(String clusters) {
        ResourceFilter resourceFilter = new ResourceFilter(ResourceType.CLUSTER, clusters);
        AccessControlEntryFilter accessControlEntryFilter =
                AccessControlEntryFilter.ANY;
        AclBindingFilter aclBindingFilter = new AclBindingFilter(resourceFilter, accessControlEntryFilter);
        DescribeAclsOptions options = new DescribeAclsOptions()
                .timeoutMs(OPERATION_TIMEOUT);
        List<AclBinding> aclBindings = Lists.newArrayList();
        KafkaFuture<Collection<AclBinding>> future = admin.describeAcls(aclBindingFilter, options).values();
        try {
            aclBindings.addAll(future.get());
        } catch (ExecutionException e) {
            log.error("", e);
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new ConnectException("Timed out while checking for or describeAcl'.", cause);
            }
            log.debug("Error while attempting to describe acl", e);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to describe acl", e);
        }
        return aclBindings;
    }


    public String createBrokersForAcls(String host, AclOperation aclOperation, AclPermissionType permissionType, String... users) {
        List<AclBinding> aclBindings = Lists.newArrayList();
        String bootstrapServers = bootstrapServers();
        if (StringUtils.isNotBlank(bootstrapServers)) {
            for (String user : users) {
                Resource resource1 = new Resource(ResourceType.CLUSTER, bootstrapServers);
                AccessControlEntry accessControlEntry1 = new AccessControlEntry(PRINCIPAL_PREFIX + user, "*",
                        aclOperation == null ? AclOperation.WRITE : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                AclBinding aclBinding1 = new AclBinding(resource1, accessControlEntry1);

                Resource resource2 = new Resource(ResourceType.CLUSTER, bootstrapServers);
                AccessControlEntry accessControlEntry2 = new AccessControlEntry(PRINCIPAL_PREFIX + user, "*",
                        aclOperation == null ? AclOperation.READ : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                AclBinding aclBinding2 = new AclBinding(resource2, accessControlEntry2);

                aclBindings.add(aclBinding1);
                aclBindings.add(aclBinding2);
            }
            CreateAclsOptions aclsOptions = new CreateAclsOptions().timeoutMs(OPERATION_TIMEOUT);
            CreateAclsResult result = admin.createAcls(aclBindings, aclsOptions);
            Map<AclBinding, KafkaFuture<Void>> futureMap = result.values();
            for (Map.Entry<AclBinding, KafkaFuture<Void>> entry : futureMap.entrySet()) {
                try {
                    entry.getValue().get();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new ConnectException("Interrupted while attempting to create acl", e);
                } catch (ExecutionException e) {
                    log.error("", e);
                    Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException) {
                        throw new ConnectException("Timed out while checking for or  create acl'.", cause);
                    }
                    if (cause instanceof UnsupportedVersionException) {
                        log.debug("Unable to create acl for topic(s) '{}' since the brokers at {} " +
                                        "do not support the CreateTopics API.",
                                bootstrapServers, bootstrapServers);
                    }
                }
            }
        }
        return bootstrapServers;
    }


    public boolean createGroupForAcl() {
        List<AclBinding> aclBindings = Lists.newArrayList();
        Resource resource = new Resource(ResourceType.GROUP, "*");

        AccessControlEntry accessControlEntry1 = new AccessControlEntry(PRINCIPAL_PREFIX + "*", "*",
                AclOperation.WRITE, AclPermissionType.ALLOW);
        AclBinding aclBinding1 = new AclBinding(resource, accessControlEntry1);

        AccessControlEntry accessControlEntry2 = new AccessControlEntry(PRINCIPAL_PREFIX + "*", "*",
                AclOperation.READ, AclPermissionType.ALLOW);
        AclBinding aclBinding2 = new AclBinding(resource, accessControlEntry2);

        aclBindings.add(aclBinding1);
        aclBindings.add(aclBinding2);

        CreateAclsOptions aclsOptions = new CreateAclsOptions().timeoutMs(OPERATION_TIMEOUT);
        CreateAclsResult result = admin.createAcls(aclBindings, aclsOptions);
        Map<AclBinding, KafkaFuture<Void>> futureMap = result.values();
        for (Map.Entry<AclBinding, KafkaFuture<Void>> entry : futureMap.entrySet()) {
            try {
                entry.getValue().get();
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create acl", e);
            } catch (ExecutionException e) {

                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    throw new ConnectException("Timed out while checking for or  create acl'.", cause);
                }
                log.error("", e);
                return false;
            }
        }
        return true;
    }


    /**
     * Attempt to create the acl for a topic,returning the name of topic
     * that by this request.
     * <p>
     * support multi-users to create, specify the host to create. & AclPermissionType = ALLOW
     *
     * @param topic
     * @param users
     * @return
     */

    public String createTopicForAcls(String topic, String host, AclOperation aclOperation, AclPermissionType permissionType, String... users) {

        List<AclBinding> aclBindings = Lists.newArrayList();
        String topicName = null;
        String bootstrapServers = bootstrapServers();
        if (StringUtils.isNotBlank(topic)) {
            for (String user : users) {
                if (StringUtils.isBlank(host)) {
                    Resource resource1 = new Resource(ResourceType.TOPIC, topic);
                    AccessControlEntry accessControlEntry1 = new AccessControlEntry(PRINCIPAL_PREFIX + user, "*",
                            aclOperation == null ? AclOperation.WRITE : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                    AclBinding aclBinding1 = new AclBinding(resource1, accessControlEntry1);

                    Resource resource2 = new Resource(ResourceType.TOPIC, topic);
                    AccessControlEntry accessControlEntry2 = new AccessControlEntry(PRINCIPAL_PREFIX + user, "*",
                            aclOperation == null ? AclOperation.READ : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                    AclBinding aclBinding2 = new AclBinding(resource2, accessControlEntry2);

                    aclBindings.add(aclBinding1);
                    aclBindings.add(aclBinding2);

                } else {
                    Resource resource1 = new Resource(ResourceType.TOPIC, topic);
                    AccessControlEntry accessControlEntry1 = new AccessControlEntry(PRINCIPAL_PREFIX + user, host,
                            aclOperation == null ? AclOperation.WRITE : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                    AclBinding aclBinding1 = new AclBinding(resource1, accessControlEntry1);

                    Resource resource2 = new Resource(ResourceType.TOPIC, topic);
                    AccessControlEntry accessControlEntry2 = new AccessControlEntry(PRINCIPAL_PREFIX + user, host,
                            aclOperation == null ? AclOperation.READ : aclOperation, permissionType == null ? AclPermissionType.ALLOW : permissionType);
                    AclBinding aclBinding2 = new AclBinding(resource2, accessControlEntry2);

                    aclBindings.add(aclBinding1);
                    aclBindings.add(aclBinding2);
                }
            }
            CreateAclsOptions aclsOptions = new CreateAclsOptions().timeoutMs(OPERATION_TIMEOUT);
            CreateAclsResult result = admin.createAcls(aclBindings, aclsOptions);
            Map<AclBinding, KafkaFuture<Void>> futureMap = result.values();
            for (Map.Entry<AclBinding, KafkaFuture<Void>> entry : futureMap.entrySet()) {
                AclBinding aclBinding = entry.getKey();
                topicName = aclBinding.resource().name();
                try {
                    entry.getValue().get();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new ConnectException("Interrupted while attempting to create acl", e);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException) {
                        throw new ConnectException("Timed out while checking for or  create acl'.", cause);
                    }
                    if (cause instanceof InvalidTopicException) {
                        log.debug("The client has attempted to perform an operation on an invalid topic '{}' on brokers at '{}'.", topic, bootstrapServers);

                    }
                    if (cause instanceof UnsupportedVersionException) {
                        log.debug("Unable to create acl for topic(s) '{}' since the brokers at {} " +
                                        "do not support the CreateTopics API.",
                                topic, bootstrapServers);
                    }
                    log.error("", e);
                }
            }
        }
        return topicName;
    }


    public ConfigResource alertBrokersSaslConfig(List<String> brokerIds, String username, String privateKey) {
        ConfigResource key = null;
        String bootstrapServers = bootstrapServers();
        Map<ConfigResource, Config> configs = Maps.newHashMap();
        List<ConfigEntry> entries = Lists.newArrayList();
        ConfigEntry entry = new ConfigEntry(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigProperty("PLAIN", username, privateKey).value());
        entries.add(entry);

        brokerIds.forEach(s -> {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, s);
            Config config = new Config(entries);
            configs.put(resource, config);
        });

        AlterConfigsOptions options = new AlterConfigsOptions().validateOnly(false);

        Map<ConfigResource, KafkaFuture<Void>> result = admin.alterConfigs(configs, options).values();

        for (Map.Entry<ConfigResource, KafkaFuture<Void>> mapEntry : result.entrySet()) {
            key = mapEntry.getKey();
            try {
                mapEntry.getValue().get();
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create acl", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to alter the b broker config for the brokers at {} " +
                            "do not support the CreateTopics API.", bootstrapServers);
                }

            }
        }
        return key;
    }



    public ConfigResource alertTopicSaslConfig(String topic, String username, String privateKey) {
        ConfigResource key = null;
        String bootstrapServers = bootstrapServers();
        Map<ConfigResource, Config> configs = Maps.newHashMap();
        List<ConfigEntry> entries = Lists.newArrayList();
        ConfigEntry entry = new ConfigEntry(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigProperty("PLAIN", username, privateKey).value());
        entries.add(entry);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = new Config(entries);
        configs.put(resource, config);
        AlterConfigsOptions options = new AlterConfigsOptions().validateOnly(false);

        Map<ConfigResource, KafkaFuture<Void>> result = admin.alterConfigs(configs, options).values();

        for (Map.Entry<ConfigResource, KafkaFuture<Void>> mapEntry : result.entrySet()) {
            key = mapEntry.getKey();
            try {
                mapEntry.getValue().get();
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while attempting to create acl", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnsupportedVersionException) {
                    log.debug("Unable to alter the b broker config for the brokers at {} " +
                            "do not support the CreateTopics API.", bootstrapServers);
                }

                if (cause instanceof InvalidTopicException) {
                    log.debug("The client has attempted to perform an operation on an invalid topic '{}' on brokers at '{}'.", topic, bootstrapServers);
                }
            }
        }
        return key;
    }


    public boolean deleteTopicAcl(String topic) {
        int result = 0;
        ResourceFilter resourceFilter = new ResourceFilter(ResourceType.TOPIC, topic);
        AclBindingFilter filter = new AclBindingFilter(resourceFilter, AccessControlEntryFilter.ANY);
        DeleteAclsOptions d = new DeleteAclsOptions().timeoutMs(OPERATION_TIMEOUT);
        DeleteAclsResult deleteAclsResult = admin.deleteAcls(Arrays.asList(filter), d);

        for (Map.Entry<AclBindingFilter, KafkaFuture<DeleteAclsResult.FilterResults>> entry : deleteAclsResult.values().entrySet()) {
            try {
                result = entry.getValue().get().values().size();
            } catch (InterruptedException | ExecutionException e) {
                log.error("", e);
            }
        }
        if (result > 0) {
            return true;
        } else return false;
    }


    /**
     * get a bootstrap servers list.
     *
     * @return
     */
    private String bootstrapServers() {
        Object servers = adminConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        return servers != null ? servers.toString() : "<unknown>";
    }


    @Override
    public void close() throws Exception {
        admin.close();
    }


    public static void main(String[] args) {
        List<Integer> a = Lists.newArrayList(1, 2, 3, 4, 5, 6);
        System.out.println(Utils.join(a, ","));
    }

}
