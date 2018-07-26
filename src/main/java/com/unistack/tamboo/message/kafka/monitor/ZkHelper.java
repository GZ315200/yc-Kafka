package com.unistack.tamboo.message.kafka.monitor;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.unistack.tamboo.message.kafka.bean.BrokerInfo;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.List;

/**
 * @author Gyges Zean
 * @date 2018/7/24
 */
public class ZkHelper {

    public static final String ZOOKEEPER_URL = "192.168.1.110:2181";

    /**
     * 获取ZK连接
     *
     * @param zkUrl
     * @return
     */
    public static ZkClient getZkClient(String zkUrl) {
        final ZkClient zkClient = new ZkClient(zkUrl, 30000, 30000, new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
                return data.toString().getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                return new String(bytes);
            }
        });
        return zkClient;
    }

    public static void close(ZkClient zkClient) {
        if (zkClient != null) {
            zkClient.close();
        }
    }


    /**
     * get broker info list from zookeeper
     *
     * @return
     */
    public static List<BrokerInfo> getBrokerInfoList() {
        List<BrokerInfo> list = Lists.newArrayList();

        ZkClient zkClient = null;
        try {
            zkClient = getZkClient(ZOOKEEPER_URL);
            List<String> children = zkClient.getChildren("/brokers/ids");
            for (String child : children) {
                JSONObject json = JSON.parseObject(zkClient.readData("/brokers/ids/" + child).toString());

                BrokerInfo brokerInfo = new BrokerInfo();
                brokerInfo.setBrokerId(Integer.parseInt(child));
                JSONArray jsonArray = json.getJSONArray("endpoints");
                for (int i = 0; i < jsonArray.size(); i++) {
                    if (jsonArray.getString(i).contains("SASL_PLAINTEXT")) {
                        String servers = jsonArray.getString(i).replace("SASL_PLAINTEXT://", " ");
                        String[] listeners = servers.split(":");
                        String host = listeners[0];
                        int port = Integer.parseInt(listeners[1]);
                        brokerInfo.setHost(host.trim());
                        brokerInfo.setPort(port);
                        brokerInfo.setLinuxServerInfo(servers.trim());
                    }
                }
                brokerInfo.setJmxPort(Integer.parseInt(json.get("jmx_port").toString().trim()));
                list.add(brokerInfo);
            }
        } finally {
            close(zkClient);
        }

        return list;
    }


}
