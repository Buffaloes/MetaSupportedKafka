package com.buffalo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class KafkaEventProcessorRegister {

    private static Logger LOGGER = LoggerFactory.getLogger(KafkaEventProcessorRegister.class);

    private static KafkaInboundEventProcessorGroup inboundGroup = new KafkaInboundEventProcessorGroup();

    private static KafkaOutboundEventProcessorGroup outboundGroup = new KafkaOutboundEventProcessorGroup();

    public static void registerInboundEventProcessor(KafkaInboundEventProcessor inbound) {
        inboundGroup.add(inbound);
        LOGGER.info("Register inbound processor");
    }

    public static void registerOutboundEventProcessor(KafkaOutboundEventProcessor outbound) {
        outboundGroup.add(outbound);
        LOGGER.info("Register outbound processor");
    }

    public static KafkaInboundEventProcessor getInbounds() {
        return inboundGroup;
    }

    public static KafkaOutboundEventProcessor getOutbounds() {
        return outboundGroup;
    }
}
