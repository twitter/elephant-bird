package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.twitter.data.proto.BlockStorage.SerializedBlock;
import com.twitter.elephantbird.util.Protobufs;

/** 
 * A class to write blocks of serialized objects.
 */
public abstract class BinaryBlockWriter<M> {
  protected static final int DEFAULT_NUM_RECORDS_PER_BLOCK = 100;

  private final OutputStream out_;
  private final int numRecordsPerBlock_;
  private final Class<M> protobufClass_;
  private final BinaryConverter<M> protoConverter_;
  private int numRecordsWritten_ = 0;
  private SerializedBlock.Builder builder_;

  protected BinaryBlockWriter(OutputStream out, Class<M> protoClass, BinaryConverter<M> protoConverter, int numRecordsPerBlock) {
    out_ = out;
    numRecordsPerBlock_ = numRecordsPerBlock;
    protobufClass_ = protoClass;
    protoConverter_ = protoConverter;
    
    builder_ = reinitializeBlockBuilder();
  }

  public void write(M message) throws IOException {
    if (message instanceof Message) {
      //a small hack to avoid extra copy, since we need a ByteString anyway.
      builder_.addProtoBlobs(((Message)message).toByteString());
    } else {
      builder_.addProtoBlobs(ByteString.copyFrom(protoConverter_.toBytes(message)));
    }
    
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
