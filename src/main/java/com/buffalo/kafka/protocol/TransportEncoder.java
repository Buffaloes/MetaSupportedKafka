package com.buffalo.kafka.protocol;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by 张皆浩 on 16/10/20.
 * DIDI CORPORATION
 */
public class TransportEncoder implements Encoder<Protocol.Transport> {

    private static final int MAGIC_NUMBER_SIZE = 4;

    private final Logger LOG = LoggerFactory.getLogger(TransportEncoder.class);

    public TransportEncoder(VerifiableProperties props) {

    }

    @Override
    public byte[] toBytes(Protocol.Transport transport) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream(transport.getSerializedSize() +
                MAGIC_NUMBER_SIZE) {

            @Override
            public byte[] toByteArray() {
                return super.buf;
            }

        };
        DataOutputStream os = new DataOutputStream(bos);

        try {
            os.writeInt(MagicNumber.MAGIC_NUMBER);
            transport.writeTo(os);
        } catch (IOException e) {
            LOG.error("[kafka encode error][" + e.getMessage() + "]", e);
        }
        return bos.toByteArray();
    }
}
