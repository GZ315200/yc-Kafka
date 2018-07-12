package com.unistack.tamboo.message.kafka.bean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.Set;

/**
 * @author Gyges Zean
 * @date 2018/5/3
 * the detail of single cluster description.
 */
public class ClusterDescription {

    private String clusterId;

    private Set<ClusterNode> clusterNode;

    private ClusterNode controllerHost;


    public ClusterDescription() {
    }


    public ClusterDescription(String clusterId, Set<ClusterNode> clusterNode, ClusterNode controllerHost) {
        this.clusterId = clusterId;
        this.clusterNode = clusterNode;
        this.controllerHost = controllerHost;
    }


    public class ClusterBuilder {

        private String clusterId;

        private Set<ClusterNode> clusterNodes;

        private ClusterNode controllerHost;


        public ClusterBuilder(String clusterId) {
            this.clusterId = clusterId;
        }


        public ClusterBuilder nodes(Collection<Node> nodes) {
            Set<ClusterNode> clusterNodes = Sets.newHashSet();
            for (Node node : nodes) {
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
            this.clusterNodes = clusterNodes;
            return this;
        }


        public ClusterBuilder controller(Node controller) {
            ClusterNode controllerHost = new ClusterNode();
            if (!controller.isEmpty()) {
                controllerHost.setHosts(controller.host() + ":" + controller.port());
                controllerHost.setIdString(controller.idString());
                if (controller.hasRack()) {
                    controllerHost.setRack(controller.rack());
                }
            }
            this.controllerHost = controllerHost;
            return this;
        }

        public ClusterDescription build() {
            return new ClusterDescription(clusterId, clusterNodes, controllerHost);
        }


    }

    /**
     * @param clusterId
     * @return
     */
    public ClusterBuilder defineForClusterId(String clusterId) {
        return new ClusterBuilder(clusterId);
    }


    public String getClusterId() {
        return clusterId;
    }

    public Set<ClusterNode> getClusterNode() {
        return clusterNode;
    }
    public ClusterNode getControllerHost() {
        return controllerHost;
    }



    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("clusterId", clusterId)
                .add("clusterNode", clusterNode)
                .add("controllerHost", controllerHost)
                .toString();
    }

    public static void main(String[] args) throws JsonProcessingException {

        Set<Node> collection = Sets.newHashSet();

        Node n = new Node(1, "192.168.1.193", 9092, "RACK-1");
        Node n1 = new Node(2,"192.168.1.194",9092,"RACK-1");
        collection.add(n);
        collection.add(n1);
        ClusterDescription clusterDescription =
                new ClusterDescription()
                        .defineForClusterId("1")
                        .nodes(collection)
                        .controller(n)
                        .build();
        ObjectMapper o = new ObjectMapper();

//        System.out.println(o.writeValueAsString(clusterDescription));
        System.out.println(clusterDescription.getClusterId());
//        System.out.printf(clusterDescription.getClusterNode().toString());
        System.out.println(clusterDescription.getControllerHost());
    }


}
