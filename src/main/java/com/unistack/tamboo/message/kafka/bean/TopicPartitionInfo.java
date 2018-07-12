package com.unistack.tamboo.message.kafka.bean;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.kafka.common.Node;

import java.util.List;

/**
 * @author Gyges Zean
 * @date 2018/5/7
 * the detail of topic partition info and description.
 */
public class TopicPartitionInfo {

    private int partition;

    private ClusterNode leader;// 主节点

    private List<ClusterNode> replicas; // 副本数

    private List<ClusterNode> isr;// in-sync 副本

    public TopicPartitionInfo(int partition,ClusterNode leader, List<ClusterNode> replicas, List<ClusterNode> isr) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
    }


    public int getPartition() {
        return partition;
    }

    public ClusterNode getLeader() {
        return leader;
    }

    public List<ClusterNode> getReplicas() {
        return replicas;
    }

    public List<ClusterNode> getIsr() {
        return isr;
    }



    public static class TopicBuilder {

        public int partition;

        public ClusterNode leader;// 主节点

        public List<ClusterNode> replicas; // 副本数

        public List<ClusterNode> isr;// in-sync 副本

        public TopicBuilder(int partition) {
            this.partition = partition;
        }


        public TopicBuilder leader(Node leader) {
            ClusterNode clusterNode = new ClusterNode();
            if (!leader.isEmpty()) {
                clusterNode.setHosts(leader.host() + ":" + leader.port());
                clusterNode.setIdString(leader.idString());
                if (leader.hasRack()) {
                    clusterNode.setRack(leader.rack());
                }
            }
            this.leader = clusterNode;
            return this;
        }

        public TopicBuilder replicas(List<Node> replicas) {
            List<ClusterNode> clusterNodes = Lists.newArrayList();
            for (Node node : replicas) {
                ClusterNode clusterNode = new ClusterNode();
                if (!node.isEmpty()) {
                    clusterNode.setIdString(node.idString());
                    clusterNode.setHosts(node.host() + ":" + node.port());
                    if (node.hasRack()) {
                        clusterNode.setRack(node.rack());
                    }
                }
                clusterNodes.add(clusterNode);
            }
            this.replicas = clusterNodes;
            return this;
        }

        public TopicBuilder isr(List<Node> isr) {
            List<ClusterNode> clusterNodes = Lists.newArrayList();
            for (Node node : isr) {
                ClusterNode clusterNode = new ClusterNode();
                if (!node.isEmpty()) {
                    clusterNode.setIdString(node.idString());
                    clusterNode.setHosts(node.host() + ":" + node.port());
                    if (node.hasRack()) {
                        clusterNode.setRack(node.rack());
                    }
                }
                clusterNodes.add(clusterNode);
            }
            this.isr = clusterNodes;
            return this;
        }

        public TopicPartitionInfo build(){
            return new TopicPartitionInfo(partition,leader,replicas,isr);
        }

    }

    public static TopicBuilder defineTopicForPartition(int partition) {
        return new TopicBuilder(partition);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("partition", partition)
                .add("leader", leader)
                .add("replicas", replicas)
                .add("isr", isr)
                .toString();
    }
}
