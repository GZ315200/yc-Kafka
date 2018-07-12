package com.unistack.tamboo.message.kafka.runtime;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Type;
import static org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 * 用于kafka
 */
public class RunnerConfig extends AbstractConfig {


    public static final String RUNNER_ID = "runner.id";

    public static final String STATUS_STORAGE_TOPIC = "status.storage.topic";
    public static final String STATUS_STORAGE_TOPIC_DEFAULT = "runner_status";
    public static final String STATUS_STORAGE_TOPIC_DOC = "storage for runner status";


    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = "status.storage.partitions.config";
    public static final int STATUS_STORAGE_PARTITIONS_CONFIG_DEFAULT = 1;
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG_DOC = "status storage topic partition";


    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG = "status.storage.replication.factor.config";
    public static final int STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DEFAULT = 1;
    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "status storage topic replication";


    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(RUNNER_ID, Type.STRING, Importance.LOW, "")
                .define(STATUS_STORAGE_TOPIC, Type.STRING, STATUS_STORAGE_TOPIC_DEFAULT, Importance.HIGH, STATUS_STORAGE_TOPIC_DOC)
                .define(STATUS_STORAGE_PARTITIONS_CONFIG, Type.INT, STATUS_STORAGE_PARTITIONS_CONFIG_DEFAULT, Importance.MEDIUM, STATUS_STORAGE_PARTITIONS_CONFIG_DOC)
                .define(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, Type.INT, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DEFAULT, Importance.MEDIUM, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
                ;
    }

    public RunnerConfig(ConfigDef definition, Map<String, Object> props) {
        super(definition, props);
    }

    public RunnerConfig(Map<String, Object> props) {
        this(baseConfigDef(), props);
    }

    public static void main(String[] args) {
        System.out.println(baseConfigDef().toHtmlTable());
    }
}
