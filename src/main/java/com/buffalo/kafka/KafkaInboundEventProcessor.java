package com.buffalo.kafka;

import java.util.Map;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public interface KafkaInboundEventProcessor {

    void beforeMessageSent(Object message, Map<String, String> meta);

    void onMessageSent(Object message, Map<String, String> meta);

}
