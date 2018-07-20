package com.unistack.tamboo.message.kafka.util;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/23
 */
public class ConfigHelper {


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
