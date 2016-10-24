package com.didichuxing.lang.kafka.consumer;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class KafkaEventProcessorRegister {

    private static KafkaInboundEventProcessorGroup inboundGroup = new KafkaInboundEventProcessorGroup();

    private static KafkaOutboundEventProcessorGroup outboundGroup = new KafkaOutboundEventProcessorGroup();

    public static void registerInboundEventProcessor(KafkaInboundEventProcessor inbound) {
        inboundGroup.add(inbound);
    }

    public static void registerOutboundEventProcessor(KafkaOutboundEventProcessor outbound) {
        outboundGroup.add(outbound);
    }

    public static KafkaInboundEventProcessor getInbounds() {
        return inboundGroup;
    }

    public static KafkaOutboundEventProcessor getOutbounds() {
        return outboundGroup;
    }
}
