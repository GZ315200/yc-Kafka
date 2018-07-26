package com.unistack.tamboo.message.kafka.util;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;

import java.util.Map;
import java.util.Properties;

/**
 * @author Gyges Zean
 * @date 2018/5/23
 */
public class ConfigHelper {



    public static final String BOOTSTRAP_SERVERS = "192.168.1.110:9093,192.168.1.111:9093,192.168.1.112:9093";

    public static final String MECHANISM = "PLAIN";
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "admin123";



    public static Map<String, Object> getAdminProperties() {
        Properties properties = new Properties();
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigProperty(MECHANISM, USERNAME, PASSWORD).value());
        properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return toMap(properties);
    }


    public static Map<String, Object> toMap(Properties properties) {
        Map<String, Object> result = Maps.newHashMap();
        properties.forEach((o, o2) -> {
            result.put((String) o, o2);
        });
        return result;
    }



    public static Password jaasConfigProperty(String mechanism, Map<String, Object> options) {
        StringBuilder builder = new StringBuilder();
        builder.append(loginModule(mechanism));
        builder.append(" required");
        for (Map.Entry<String, Object> option : options.entrySet()) {
            builder.append(' ');
            builder.append(option.getKey());
            builder.append('=');
            builder.append(option.getValue());
        }
        builder.append(';');
        return new Password(builder.toString());
    }


    public static Password jaasConfigProperty(String mechanism, String username, String password) {
        return new Password(loginModule(mechanism) + " required username=" + username + " password=" + password + ";");
    }



    private static String loginModule(String mechanism) {
        String loginModule;
        switch (mechanism) {
            case "PLAIN":
                loginModule = PlainLoginModule.class.getName();
                break;
            default:
                if (ScramMechanism.isScram(mechanism)) {
                    loginModule = ScramLoginModule.class.getName();
                } else {
                    throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
                }
        }
        return loginModule;
    }


}
