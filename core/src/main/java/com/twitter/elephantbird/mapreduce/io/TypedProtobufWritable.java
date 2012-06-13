package com.twitter.elephantbird.mapreduce.io;

import com.google.protobuf.Message;

/**
 * A Hadoop Writable wrapper around a protocol buffer of type M with support for
 * polymorphism. This class writes the type of M to the output which makes it
 * unnecessary to pass in the correct Class<M> when creating the object and also
 * allows reading/writing heterogeneous data:
 * <code>
 * for (TypedProtobufWritable<Message> value in values) {
 *     Message message = value.get();
 *     if (message instanceof MyProtocMessage1) {
 *         MyProtocMessage1 protoc1 = (MyProtocMessage1) message;
 *         // do something with the protoc1
 *     }
 * }
 * </code>
 * 
 * Since this class writes the type of M to the output, it is recommended to use
 * some kind of compression on the output.
 */
public class TypedProtobufWritable<M extends Message> extends BinaryWritable<M> { 
    public TypedProtobufWritable() {
        super(null, new TypedProtobufConverter<M>());
    }

    public TypedProtobufWritable(M obj) {
        super(obj, new TypedProtobufConverter<M>());
    }

    @Override
    protected BinaryConverter<M> getConverterFor(Class<M> clazz) {
        return new TypedProtobufConverter();
    }
}
