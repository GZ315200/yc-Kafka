package com.unistack.tamboo.message.kafka.bean;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

/**
 * @author Gyges Zean
 * @date 2018/5/28
 */
public class BrokerInfo implements Serializable {

    private int brokerId;
    private String clusterName;
    private Integer jmxPort;
    private String host;
    private Integer port;
    private String linuxServerInfo;

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Integer getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(Integer jmxPort) {
        this.jmxPort = jmxPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getLinuxServerInfo() {
        return linuxServerInfo;
    }

    public void setLinuxServerInfo(String linuxServerInfo) {
        this.linuxServerInfo = linuxServerInfo;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerInfo that = (BrokerInfo) o;

        if (brokerId != that.brokerId) return false;
        if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
        if (jmxPort != null ? !jmxPort.equals(that.jmxPort) : that.jmxPort != null) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (port != null ? !port.equals(that.port) : that.port != null) return false;
        return linuxServerInfo != null ? linuxServerInfo.equals(that.linuxServerInfo) : that.linuxServerInfo == null;
    }

    @Override
    public int hashCode() {
        int result = brokerId;
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        result = 31 * result + (jmxPort != null ? jmxPort.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (port != null ? port.hashCode() : 0);
        result = 31 * result + (linuxServerInfo != null ? linuxServerInfo.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("brokerId", brokerId)
                .add("clusterName", clusterName)
                .add("jmxPort", jmxPort)
                .add("host", host)
                .add("port", port)
                .add("linuxServerInfo", linuxServerInfo)
                .toString();
    }
}
