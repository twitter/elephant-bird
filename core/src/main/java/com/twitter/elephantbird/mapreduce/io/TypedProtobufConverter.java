package com.twitter.elephantbird.mapreduce.io;

import java.nio.charset.Charset;

import com.google.protobuf.Message;
import org.apache.commons.lang.ArrayUtils;

/**
 * {@link BinaryConverter} for TypedProtobufs
 */
public class TypedProtobufConverter<M extends Message> implements BinaryConverter<M> {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public byte[] toBytes(M message) {
        Class clazz = message.getClass();
        byte[] classBytes = clazz.getName().getBytes(UTF8);
        if (classBytes.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Class name is too long: "
                    + clazz.getName());
        }
        byte[] classSerialization = ArrayUtils.addAll(
                new byte[]{(byte)classBytes.length}, classBytes);
        ProtobufConverter converter = ProtobufConverter.newInstance(clazz);
        return ArrayUtils.addAll(classSerialization, converter.toBytes(message));
    }

    @Override
    public M fromBytes(byte[] messageBuffer) {
        Class<M> clazz;
        byte classNameLength = messageBuffer[0];
        byte[] classNameBytes = ArrayUtils.subarray(messageBuffer, 1,
                                                    1 + classNameLength);
        String className = new String(classNameBytes, UTF8);
        try {
            clazz = (Class<M>) Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("Byte stream does not have a "
                    + "valid class identifier", ex);
        }
        ProtobufConverter<M> converter = ProtobufConverter.newInstance(clazz);
        return converter.fromBytes(ArrayUtils.subarray(messageBuffer,
                1 + classNameLength, messageBuffer.length));
    }
}
