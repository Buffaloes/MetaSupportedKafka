package com.didichuxing.lang.kafka.consumer

import com.didichuxing.lang.kafka.consumer.protocol.Protocol.Transport
import kafka.consumer.ConsumerIterator
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import scala.collection.JavaConversions._


/**
  * Created by 张皆浩 on 16/10/20.
  * DIDI CORPORATION
  */
class MetaSupportedConsumerIterator[K, V](val iter: ConsumerIterator[K, Transport],
                                          val valueDecoder: Decoder[V])
  extends ConsumerIterator[K, V](null, 0, null, null, iter.clientId) {

  override def next(): MessageAndMetadata[K, V] = {

    val item = iter.next()

    new MessageAndMetadata[K, V](item.topic, item.partition, null, item.offset, item.keyDecoder, valueDecoder) {

      override def key(): K = item.key()

      override def message(): V = {
        val message = item.message()
        val b: Array[Byte] = new Array[Byte](message.getData.size)
        message.getData.asReadOnlyByteBuffer().get(b)
        val realMessage = valueDecoder.fromBytes(b)
        val meta: java.util.Map[String, String] = new java.util.HashMap[String, String]()
        for (metaItem <- message.getMetaList) {
          meta.put(metaItem.getKey, metaItem.getValue)
        }
        KafkaEventProcessorRegister.getOutbounds.afterMessageSent(realMessage, meta)
        realMessage
      }
    }
  }

  override def peek(): MessageAndMetadata[K, V] = throw new UnsupportedOperationException()

  override def hasNext(): Boolean = iter.hasNext

  override def clearCurrentChunk() = iter.clearCurrentChunk()

}
