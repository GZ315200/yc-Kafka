package com.unistack.tamboo.message.kafka.storage;

import com.unistack.tamboo.message.kafka.bean.ConsumerOffset;
import org.apache.kafka.common.config.types.Password;

import java.util.List;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 * storage of topic offset
 */
public interface OffsetBackingStore {

    /**
     * 获取所有group下所有consumer的消息消费信息
     *
     * @return
     */
    List<ConsumerOffset> getConsumerGroups();

    /**
     * adminClient连接关闭
     */
    void close();
}
