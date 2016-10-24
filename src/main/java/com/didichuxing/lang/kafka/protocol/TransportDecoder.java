package com.didichuxing.lang.kafka.protocol;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class TransportDecoder implements Decoder<Protocol.Transport> {

    private final Logger LOG = LoggerFactory.getLogger(TransportDecoder.class);

    @Override
    public Protocol.Transport fromBytes(byte[] bytes) {
        if (bytes.length < MagicNumber.MAGIC_NUMBER_SIZE || ByteBuffer.wrap(bytes).getInt() !=
                MagicNumber.MAGIC_NUMBER) {
            return fallback(bytes);
        } else {
            return decode(bytes);
        }
    }

    private Protocol.Transport decode(byte[] bytes) {
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(bytes, MagicNumber.MAGIC_NUMBER_SIZE, bytes.length -
                    MagicNumber.MAGIC_NUMBER_SIZE);
            Protocol.Transport transport = Protocol.Transport.parseFrom(is);
            return transport;
        } catch (InvalidProtocolBufferException e) {
            LOG.error("[kafka decode error][" + e.getMessage() + "]", e);
            return fallback(bytes);
        } catch (IOException e) {
            LOG.error("[kafka decode error][" + e.getMessage() + "]", e);
            return fallback(bytes);
        }
    }

    private Protocol.Transport fallback(byte[] bytes) {
        return Protocol.Transport.newBuilder().setData(ByteString.copyFrom(bytes)).build();
    }
}
