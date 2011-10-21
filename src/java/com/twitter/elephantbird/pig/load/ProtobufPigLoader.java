package com.twitter.elephantbird.pig.load;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.ProtobufFileInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Loader for uncompressed Protocol Buffer files.
 *
 * <p>Initialize with a String argument that represents the full classpath of the protocol
 * buffer class to be loaded.
 *
 * <p>The no-arg constructor will not work and is there only for internal Pig reasons.
 *
 * <p>Note: This Loader requires the files to be written in a format which is compliant with
 * the requirements of {@link ProtobufFileInputFormat}. Specifically, the format must be:
 * [{@link com.twitter.elephantbird.util.Protobufs.KNOWN_GOOD_POSITION_MARKER} <size of next message>
 *     <serialized message>]. An example code snippet for writing such a file is:
 *
 * <pre>
 *     Message message = buildYourMessage();
 *     OutputStream os = getOutputStream();
 *     os.write(Protobufs.KNOWN_GOOD_POSITION_MARKER);
 *     message.writeDelimitedTo(os);
 * </pre>
 *
 * <p>Note that nested Protocol Buffers are only automatically converted to Tuples if the
 * internal Protobuf is part of the same outer class. Otherwise, ProtobufBytesToTuple should be
 * used.
 *
 * <p>Usage of this is identical to that of {@link LzoProtobufBlockPigLoader}.
 *
 */
public class ProtobufPigLoader<M extends Message> extends LzoBaseLoadFunc{
    private TypeRef<M> typeRef_ = null;

    private final ProtobufToPig protoToPig_ = new ProtobufToPig();
    /**
     * Default constructor. Do not use for actual loading.
     */
    public ProtobufPigLoader() {}

    public ProtobufPigLoader(TypeRef<M> typeRef_) {
        this.typeRef_ = typeRef_;
    }

    public ProtobufPigLoader(String protoClassname) {
        this.typeRef_ = PigUtil.getProtobufTypeRef(protoClassname);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new ProtobufFileInputFormat<M>(typeRef_);
    }

    @Override
    public Tuple getNext() throws IOException {
        M value = super.getNextBinaryValue(typeRef_);
        return value!=null? new ProtobufTuple(value):null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        return new ResourceSchema(protoToPig_.toSchema(Protobufs.getMessageDescriptor(typeRef_.getRawClass())));
    }
}
