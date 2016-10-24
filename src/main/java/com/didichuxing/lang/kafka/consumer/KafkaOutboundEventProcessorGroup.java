package com.didichuxing.lang.kafka.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class KafkaOutboundEventProcessorGroup implements KafkaOutboundEventProcessor {

    private List<KafkaOutboundEventProcessor> group = new CopyOnWriteArrayList<>();

    @Override
    public void afterMessageSent(Object message, Map<String, String> meta) {
        for (KafkaOutboundEventProcessor processor : group) {
            processor.afterMessageSent(message, meta);
        }
    }

    public void add(KafkaOutboundEventProcessor processor) {
        this.group.add(processor);
    }

}
