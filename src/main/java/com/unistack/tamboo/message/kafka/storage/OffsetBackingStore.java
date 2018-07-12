package com.unistack.tamboo.message.kafka.storage;

import com.unistack.tamboo.message.kafka.bean.ConsumerOffset;
import org.apache.kafka.common.config.types.Password;

import java.util.List;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 * storage for topic offset in sqlite
 */
public interface OffsetBackingStore {

    List<ConsumerOffset> getConsumerGroups();

    void close();
}
