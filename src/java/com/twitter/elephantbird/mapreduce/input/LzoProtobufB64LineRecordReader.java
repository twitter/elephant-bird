package com.twitter.elephantbird.mapreduce.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Reads line from an lzo compressed text file, base64 decodes it, and then
 * deserializes that into the templatized protobuf object.
 * Returns <position, protobuf> pairs.
 */
public class  LzoProtobufB64LineRecordReader<M extends Message> extends LzoBinaryB64LineRecordReader<M, ProtobufWritable<M>> {
  private static final Logger LOG = LoggerFactory.getLogger(LzoProtobufB64LineRecordReader.class);

  public LzoProtobufB64LineRecordReader(TypeRef<M> typeRef) {
    this(typeRef, null);
  }

  public LzoProtobufB64LineRecordReader(TypeRef<M> typeRef, ExtensionRegistry extensionRegistry) {
    super(typeRef, new ProtobufWritable<M>(typeRef, extensionRegistry),
        ProtobufConverter.newInstance(typeRef, extensionRegistry));
    LOG.info("LzoProtobufB64LineRecordReader, type args are " + typeRef.getRawClass());
  }
}

