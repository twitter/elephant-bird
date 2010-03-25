package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.data.proto.BlockStorage.SerializedBlock;
import com.twitter.elephantbird.util.Protobufs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* A class to write blocks of protobuf data of type M.  To use, just instantiate
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
 * FileSystem fs = FileSystem.get(conf);
 * FSDataOutputStream outputStream = fs.create(lzoPath, true);
 * LzopCodec codec = new LzopCodec();
 * codec.setConf(conf);
 * OutputStream lzopOutputStream = codec.createOutputStream(outputStream);
 * </code>
 *
 * See the ProtobufBlockReader for how to read data files like "person_data" above.
 */
public class ProtobufBlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufBlockWriter.class);
  protected static final int DEFAULT_NUM_RECORDS_PER_BLOCK = 100;

  protected final OutputStream out_;
  protected final int numRecordsPerBlock_;
  protected final Class<? extends Message> protobufClass_;
  protected final Descriptors.Descriptor msgDescriptor_;

  protected int numRecordsWritten_ = 0;
  protected SerializedBlock.Builder builder_;

  public ProtobufBlockWriter(OutputStream out, Class<? extends Message> protoClass) {
    this(out, protoClass, DEFAULT_NUM_RECORDS_PER_BLOCK);
  }

  public ProtobufBlockWriter(OutputStream out, Class<? extends Message> protoClass, int numRecordsPerBlock) {
    out_ = out;
    numRecordsPerBlock_ = numRecordsPerBlock;
    protobufClass_ = protoClass;
    msgDescriptor_ = Protobufs.getMessageDescriptor(protobufClass_);

    builder_ = reinitializeBlockBuilder();
  }

  public void write(Message message) throws IOException {
    builder_.addProtoBlobs(message.toByteString());
    numRecordsWritten_++;

    if (builder_.getProtoBlobsCount() == numRecordsPerBlock_) {
      serialize();
      builder_ = reinitializeBlockBuilder();
    }
  }

  public SerializedBlock.Builder reinitializeBlockBuilder() {
    return SerializedBlock.newBuilder()
                          .setVersion(1)
                          .setProtoClassName(protobufClass_.getCanonicalName());
  }


  public void finish() throws IOException {
    if (builder_.getProtoBlobsCount() > 0) {
      serialize();
    }
  }

  public void close() throws IOException {
    out_.close();
  }

  protected void serialize() throws IOException {
    SerializedBlock block = builder_.build();
    out_.write(Protobufs.KNOWN_GOOD_POSITION_MARKER);
    writeRawLittleEndian32(block.getSerializedSize());
    block.writeTo(out_);
  }

  private void writeRawLittleEndian32(int size) throws IOException {
     out_.write((size) & 0xFF);
     out_.write((size >> 8) & 0xFF);
     out_.write((size >> 16) & 0xFF);
     out_.write((size >> 24) & 0xFF);
   }
}
