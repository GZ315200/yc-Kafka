package com.unistack.tamboo.message.kafka.util;

import com.google.common.collect.Lists;
import com.unistack.tamboo.message.kafka.errors.InvalidValueException;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/23
 */
public enum Protocol {


    PLAINTEXT(9092), SASL_PLAINTEXT(9093);

    public final int port;

    private static final Map<Integer,Protocol> PORT_TO_SECURITY_PROTOCOL;

    static {
        Map<Integer, Protocol> portToSecurityProtocol = new HashMap<Integer, Protocol>();
        for (Protocol protocol : values()) {
            portToSecurityProtocol.put(protocol.port, protocol);
        }
        PORT_TO_SECURITY_PROTOCOL = Collections.unmodifiableMap(portToSecurityProtocol);
    }

    Protocol(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return this.name();
    }

    public static List<Integer> getSupportedPorts() {
        List<Integer> ports = Lists.newArrayList();
        for (Protocol protocol : values()) {
            ports.add(protocol.port);
        }
        return ports;
    }

    public static Protocol getProtocolByPort(int port) {
        if (!PORT_TO_SECURITY_PROTOCOL.containsKey(port)) {
            throw new InvalidValueException("no protocol matches the port " + port + ", supported ports: "
                    + Protocol.getSupportedPorts());
        }
        return PORT_TO_SECURITY_PROTOCOL.get(port);
    }




}
