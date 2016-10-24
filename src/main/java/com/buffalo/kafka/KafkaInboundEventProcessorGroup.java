package com.buffalo.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class KafkaInboundEventProcessorGroup implements KafkaInboundEventProcessor {

    private List<KafkaInboundEventProcessor> group = new CopyOnWriteArrayList<>();

    @Override
    public void beforeMessageSent(Object message, Map<String, String> meta) {
        for (KafkaInboundEventProcessor processor : group) {
            processor.beforeMessageSent(message, meta);
        }
    }

    @Override
    public void onMessageSent(Object message, Map<String, String> meta) {
        for (KafkaInboundEventProcessor processor : group) {
            processor.onMessageSent(message, meta);
        }
    }

    public void add(KafkaInboundEventProcessor processor) {
        this.group.add(processor);
    }
}
