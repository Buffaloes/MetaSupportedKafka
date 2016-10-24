package com.didichuxing.lang.kafka.consumer

import com.didichuxing.lang.kafka.protocol.Protocol.Transport
import kafka.consumer.{ConsumerIterator, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

class MetaSupportedKafkaStream[K, V](
                                      val stream: KafkaStream[K, Transport],
                                      val valueDecoder: Decoder[V])
    extends KafkaStream[K, V](null, 0, null, null, stream.clientId) {

  val iter = new MetaSupportedConsumerIterator(stream.iterator, valueDecoder)

  override def iterator(): ConsumerIterator[K, V] = iter

  override def clear() = iter.clearCurrentChunk()

}