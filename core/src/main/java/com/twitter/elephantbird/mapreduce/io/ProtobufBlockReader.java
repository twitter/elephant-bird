package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* A class to read blocks of protobuf data of type M.  To use, just instantiate
 * with an InputStream and a TypeRef, call readProtobuf until it returns false, and
 * then close the protobuf.  For example,
 * <code>
 * TypeRef<Person> personRef = new TypeRef<Person>();
 * ProtobufBlockReader<Person> reader = new ProtobufBlockReader<Person>(
 *     new FileInputStream("person_data"), personRef);
 * ProtobufWritable<Person> writable = new ProtobufWritable<Person>(personRef);
 * while (reader.readProtobuf(writable)) {
 *   Person person = writable.get();
 *   // do something with the protobuf.
 * }
 * reader.close();
 * </code>
 *
 * See the ProtobufBlockWriter for how to write data files like "person_data" above.
 */

public class ProtobufBlockReader<M extends Message> extends BinaryBlockReader<M> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufBlockReader.class);

  public ProtobufBlockReader(InputStream in, TypeRef<M> typeRef) {
    super(in, ProtobufConverter.newInstance(typeRef));
    LOG.info("ProtobufReader, my typeClass is " + typeRef.getRawClass());
  }

  // for backward compatibility :

  public boolean readProtobuf(ProtobufWritable<M> message) throws IOException {
    return readNext(message);
  }

  public boolean readProtobufBytes(BytesWritable message) throws IOException {
    return readNextProtoBytes(message);
  }
}
