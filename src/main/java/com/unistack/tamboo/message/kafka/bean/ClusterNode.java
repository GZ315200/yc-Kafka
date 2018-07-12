package com.unistack.tamboo.message.kafka.bean;

import com.google.common.base.MoreObjects;

/**
 * @author Gyges Zean
 * @date 2018/5/7
 *
 * the detail of all of cluster node,hosts & nodeId
 */
public class ClusterNode {

    private String hosts; // ip和端口号

    private String idString; // 属于主机的id

    private String rack; //使用 rack来平衡副本，broker 配置example : broker.rack = "RACK1"

    public ClusterNode() {

    }

    public ClusterNode(String hosts, String idString, String rack) {
        this.hosts = hosts;
        this.idString = idString;
        this.rack = rack;
    }

    public String getRack() {
        return rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getIdString() {
        return idString;
    }

    public void setIdString(String idString) {
        this.idString = idString;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("hosts", hosts)
                .add("idString", idString)
                .add("rack", rack)
                .toString();
    }
}
