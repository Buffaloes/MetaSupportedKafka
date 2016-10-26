package com.buffalo.kafka.producer;

import com.buffalo.kafka.KafkaEventProcessorRegister;
import com.buffalo.kafka.protocol.Protocol;
import com.buffalo.kafka.protocol.TransportEncoder;
import com.google.protobuf.ByteString;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.*;

/**
 * @author longyaokun
 * @date 2016年10月23日
 * @param <K>
 * @param <V>
 */
public class MetaSupportedProducer<K, V> {

	private final Producer<K, Protocol.Transport> delegate;

	private final Encoder<V> serializer;

	private static final Logger LOGGER = LoggerFactory.getLogger(MetaSupportedProducer.class);

	public MetaSupportedProducer(ProducerConfig config) {
		VerifiableProperties vp = config.props();
		Properties properties = vp.props();
		String serializerClass = properties.getProperty("serializer.class");
		List<Object> args = new ArrayList<>();
		args.add(vp);
		this.serializer = Utils.createObject(serializerClass,
				JavaConverters.asScalaBufferConverter(args).asScala().toSeq());
		properties.put("serializer.class", TransportEncoder.class.getName());
		properties.put("key.serializer.class", config.keySerializerClass());
		ProducerConfig newConfig = new ProducerConfig(properties);
		this.delegate = new Producer<K, Protocol.Transport>(newConfig);
	}

	public void send(KeyedMessage<K, V> message) {
		this.delegate.send(wrapMessage(message));
	}

	public void send(List<KeyedMessage<K, V>> messages) {
		for (KeyedMessage<K, V> message : messages) {
			this.delegate.send(wrapMessage(message));
		}

	}

	private KeyedMessage<K, Protocol.Transport> wrapMessage(KeyedMessage<K, V> message) {
		if (message == null) {
			return null;
		}
		final V value = message.message();

		Map<String, String> meta = new HashMap<>();
		try {
			KafkaEventProcessorRegister.getInbounds().beforeMessageSent(value, meta);
		} catch (Throwable e) {
			LOGGER.error(e.getMessage(), e);
		}

		final Protocol.Transport.Builder builder = Protocol.Transport.newBuilder();
		builder.setData(ByteString.copyFrom(this.serializer.toBytes(value)));
		for (String metaKey : meta.keySet()) {
			builder.addMeta(Protocol.Transport.MapFieldEntry.newBuilder().setKey(metaKey).setValue(meta.get(metaKey)).build());
		}
		final Protocol.Transport newVal = builder.build();
		return new KeyedMessage<>(message.topic(), message.key(), message.partKey(), newVal);
	}

	public void close() {
		this.delegate.close();
	}

}
