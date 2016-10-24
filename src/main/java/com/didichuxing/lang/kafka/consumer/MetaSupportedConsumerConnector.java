package com.didichuxing.lang.kafka.consumer;

import com.didichuxing.lang.kafka.protocol.Protocol;
import com.didichuxing.lang.kafka.protocol.TransportDecoder;
import com.google.common.base.Function;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;

public class MetaSupportedConsumerConnector implements ConsumerConnector {

    private final TransportDecoder transportDecoder = new TransportDecoder();
    private ConsumerConnector delegate;

    public MetaSupportedConsumerConnector(ConsumerConnector consumerConnector) {
        this.delegate = consumerConnector;
    }

    public static ConsumerConnector createJavaConsumerConnector(ConsumerConfig config) {
        return new MetaSupportedConsumerConnector(new kafka.javaapi.consumer.ZookeeperConsumerConnector(config));
    }

    @Override
    public void shutdown() {
        this.delegate.shutdown();
    }

    @Override
    public <K, V> java.util.Map<String, java.util.List<KafkaStream<K, V>>> createMessageStreams(
            java.util.Map<String, Integer> topicCountMap, final Decoder<K> keyDecoder, final Decoder<V> valueDecoder) {
        Map<String, List<KafkaStream<K, Protocol.Transport>>> streams = this.delegate.createMessageStreams(topicCountMap,
                keyDecoder, transportDecoder);
        Map<String, List<KafkaStream<K, V>>> results = new HashMap<>();
        for (String topic : streams.keySet()) {
            List<KafkaStream<K, Protocol.Transport>> streamList = streams.get(topic);
            results.put(topic, newArrayList(transform(streamList, new Function<KafkaStream<K, Protocol.Transport>, KafkaStream<K, V>>() {

                @Override
                public KafkaStream<K, V> apply(KafkaStream<K, Protocol.Transport> input) {
                    return transformStream(input, keyDecoder, valueDecoder);
                }
            })));
        }
        return results;
    }

    @Override
    public java.util.Map<String, java.util.List<KafkaStream<byte[], byte[]>>> createMessageStreams(
            java.util.Map<String, Integer> topicCountMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> java.util.List<KafkaStream<K, V>> createMessageStreamsByFilter(
            TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        List<KafkaStream<K, V>> transformed = new ArrayList<>();
        List<KafkaStream<K, Protocol.Transport>> streams = this.delegate.createMessageStreamsByFilter(topicFilter, numStreams,
                keyDecoder, transportDecoder);
        for (KafkaStream<K, Protocol.Transport> stream : streams) {
            transformed.add(transformStream(stream, keyDecoder, valueDecoder));
        }
        return transformed;
    }

    @Override
    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) {
        throw new UnsupportedOperationException();
    }

    private <K, V> KafkaStream<K, V> transformStream(final KafkaStream<K, Protocol.Transport> stream,
                                                     Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        return new MetaSupportedKafkaStream<K, V>(stream, valueDecoder);
    }

    @Override
    public void commitOffsets() {
        this.delegate.commitOffsets();
    }

    @Override
    public void commitOffsets(boolean retryOnFailure) {
        this.delegate.commitOffsets(retryOnFailure);
    }

}
