package com.twitter.elephantbird.mapreduce.io;

import java.io.OutputStream;

import com.google.protobuf.Message;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A class to write blocks of protobuf data of type M.  To use, just instantiate
 * with an OutputStream and a TypeRef, call write until you're done, call finish, and
 * then close the writer.  For example,
 * <code>
 * ProtobufBlockWriter<Person> writer = new ProtobufBlockWriter<Person>(
 *     new FileOutputStream("person_data"), Person.class);
 * writer.write(person1);
 * ...
 * writer.write(person100000);
 * writer.finish();
 * writer.close();
 * </code>
 * To make an output stream for an lzo-compressed file in a Path named lzoPath in HDFS,
 * use the following code:
 * <code>
 * Configuration conf = new Configuration();
 * FileSystem fs = lzoPath.getFileSystem(conf);
 * FSDataOutputStream outputStream = fs.create(lzoPath, true);
 * LzopCodec codec = new LzopCodec();
 * codec.setConf(conf);
 * OutputStream lzopOutputStream = codec.createOutputStream(outputStream);
 * </code>
 *
 * See the ProtobufBlockReader for how to read data files like "person_data" above.
 */
public class ProtobufBlockWriter<M extends Message> extends BinaryBlockWriter<M> {

  public ProtobufBlockWriter(OutputStream out, Class<M> protoClass) {
    super(out, protoClass, ProtobufConverter.newInstance(protoClass), DEFAULT_NUM_RECORDS_PER_BLOCK);
  }

  public ProtobufBlockWriter(OutputStream out, Class<M> protoClass, int numRecordsPerBlock) {
    super(out, protoClass, ProtobufConverter.newInstance(protoClass), numRecordsPerBlock);
  }
}
