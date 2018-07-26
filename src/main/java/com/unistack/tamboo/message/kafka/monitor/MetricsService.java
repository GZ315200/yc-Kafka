package com.unistack.tamboo.message.kafka.monitor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unistack.tamboo.message.kafka.bean.BrokerInfo;
import com.unistack.tamboo.message.kafka.util.JMXHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * @author Gyges Zean
 * @date 2018/7/17
 * <p>
 * Supply the collection metrics method for monitoring the zookeeper and Brokers
 */

public class MetricsService {

    public static final Logger logger = LoggerFactory.getLogger(MetricsService.class);

    /**
     * 采集zookeeper的metrics信息
     *
     * @param timestamp
     */
    public void getZookeeperMetrics(long timestamp) {
        JSONObject result = new JSONObject();
        result.put("timestamp", timestamp);
        try {
            ObjectName latencyName = new ObjectName("kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs");
            ObjectName stateName = new ObjectName("kafka.server:type=SessionExpireListener,name=SessionState");

            List<BrokerInfo> brokerInfoList = ZkHelper.getBrokerInfoList();
            ;
            Map<BrokerInfo, Map<ObjectName, Object>> latencyMetrics = concurrentlyScanJMX(brokerInfoList, latencyName, null, "Count");
            Map<BrokerInfo, Map<ObjectName, Object>> stateMetrics = concurrentlyScanJMX(brokerInfoList, stateName, null, "Value");

            JSONArray array = new JSONArray(2);
            assembleTheMetricsData(array, latencyMetrics, "name");
            assembleTheMetricsData(array, stateMetrics, "name");

            result.put("zookeeperMetrics", array);

            logger.info("get zookeeper metrics: " + result.toJSONString());
        } catch (Exception e) {
            logger.error("Failed to collect zookeeper metrics", e);
        }
    }


    /**
     * 采集brokers的metrics的信息
     *
     * @param timestamp
     */
    public void getBrokersMetrics(long timestamp) {
        JSONObject result = new JSONObject();
        result.put("timestamp", timestamp);
        try {
            ObjectName brokersName = new ObjectName("kafka.server:type=KafkaServer,name=*,*");
            ObjectName controllerName = new ObjectName("kafka.controller:type=KafkaController,name=*,*");

            List<BrokerInfo> brokerInfoList = ZkHelper.getBrokerInfoList();
            Map<BrokerInfo, Map<ObjectName, Object>> brokersMetrics = concurrentlyScanJMX(brokerInfoList, brokersName, null, "Value");
            Map<BrokerInfo, Map<ObjectName, Object>> controllerMetrics = concurrentlyScanJMX(brokerInfoList, controllerName, null, "Value");
            JSONArray array = new JSONArray(2);
            assembleTheMetricsData(array, brokersMetrics, "name");
            assembleTheMetricsData(array, controllerMetrics, "name");

            result.put("brokersMetrics", array);
            logger.info("get brokers metrics: " + result.toJSONString());
        } catch (Exception e) {
            logger.error("Failed to collect brokers metrics", e);
        }
    }


    /**
     * 组装metrics的数据
     *
     * @param metrics
     * @param exps
     */
    private JSONArray assembleTheMetricsData(JSONArray array, Map<BrokerInfo, Map<ObjectName, Object>> metrics, String... exps) {
        for (Map.Entry<BrokerInfo, Map<ObjectName, Object>> entry : metrics.entrySet()) {
            JSONObject data = new JSONObject();
            JSONObject result = new JSONObject();
            String host = entry.getKey().getHost();
            int port = entry.getKey().getPort();
            int brokerId = entry.getKey().getBrokerId();
            String hostPort = host + ":" + port;
            result.put("host", host + "-" + brokerId);
            result.put("brokerId", brokerId);
            Map<ObjectName, Object> objectMap = entry.getValue();
            for (Map.Entry<ObjectName, Object> objectEntry : objectMap.entrySet()) {
                ObjectName name = objectEntry.getKey();
                for (String exp : exps) {
                    Object attr = name.getKeyProperty(exp);
                    Object value = objectEntry.getValue();
                    result.put(String.valueOf(attr), value);
                }
            }
            data.put(hostPort, result);
            array.add(data);
        }
        return array;
    }


    private Map<BrokerInfo, Map<ObjectName, Object>> concurrentlyScanJMX(final List<BrokerInfo> brokerInfoList, final ObjectName objectName,
                                                                         final QueryExp exp, final String attr) {
        Map<BrokerInfo, Map<ObjectName, Object>> result = Maps.newHashMap();
        Map<BrokerInfo, Future<Map<ObjectName, Object>>> taskList = Maps.newHashMap();
        for (final BrokerInfo info : brokerInfoList) {
            FutureTask<Map<ObjectName, Object>> task = new FutureTask<Map<ObjectName, Object>>(
                    () -> {
                        long start = System.currentTimeMillis();
                        Map<ObjectName, Object> brokerResult = Maps.newHashMap();
                        JMXHelper helper = JMXHelper.getInstance(info.getHost().trim(), info.getJmxPort().toString().trim());
                        for (ObjectName obj : helper.queryNames(objectName, exp)) {
                            brokerResult.put(obj, helper.getAttribute(obj, attr));
                        }
                        logger.info("got kafka JMX Metrics: type = {}, Broker = {}, ObjectCount = {}, cost = {}",
                                objectName.getKeyProperty("type"), info.getHost(), brokerResult.size(),
                                System.currentTimeMillis() - start);
                        return brokerResult;
                    });
            taskList.put(info, task);
            new Thread(task).start();
        }
        for (BrokerInfo info : taskList.keySet()) {
            try {
                Map<ObjectName, Object> value = taskList.get(info).get();
                result.put(info, value);
            } catch (Exception e) {
                logger.error("failed to get broker metrics. broker_info = " + info, e);
            }
        }
        return result;
    }


    public static void main(String[] args) {
        try {
            MetricsService service = new MetricsService();
            ObjectName latencyName = new ObjectName("kafka.server:type=KafkaServer,name=*,*");

            List<BrokerInfo> brokerInfoList = Lists.newArrayList();

            BrokerInfo brokerInfo = new BrokerInfo();
            brokerInfo.setBrokerId(0);
            brokerInfo.setHost("192.168.1.111");
            brokerInfo.setPort(9093);
            brokerInfo.setJmxPort(9999);
            brokerInfoList.add(brokerInfo);

            Map<BrokerInfo, Map<ObjectName, Object>> latencyMetrics = service.concurrentlyScanJMX(brokerInfoList, latencyName, null, "Value");

            System.out.println(latencyMetrics);

        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
    }
}
