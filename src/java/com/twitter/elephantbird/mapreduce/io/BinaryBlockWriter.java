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
  protected final Class<M> innerClass_;
  private final BinaryConverter<M> binaryConverter_;
  private int numRecordsWritten_ = 0;
  private SerializedBlock.Builder builder_;

  protected BinaryBlockWriter(OutputStream out, Class<M> protoClass, BinaryConverter<M> binaryConverter, int numRecordsPerBlock) {
    out_ = out;
    numRecordsPerBlock_ = numRecordsPerBlock;
    innerClass_ = protoClass;
    binaryConverter_ = binaryConverter;
    builder_ = reinitializeBlockBuilder();
  }

  public void write(M message) throws IOException {
    if (message instanceof Message) {
      //a small hack to avoid extra copy, since we need a ByteString anyway.
      builder_.addProtoBlobs(((Message)message).toByteString());
    } else {
      builder_.addProtoBlobs(ByteString.copyFrom(binaryConverter_.toBytes(message)));
    }

    numRecordsWritten_++;

    if (builder_.getProtoBlobsCount() == numRecordsPerBlock_) {
      serialize();
    }
  }

  public SerializedBlock.Builder reinitializeBlockBuilder() {
    return SerializedBlock.newBuilder()
                          .setVersion(1)
                          .setProtoClassName(innerClass_.getCanonicalName());
  }

  public void finish() throws IOException {
    if (builder_.getProtoBlobsCount() > 0) {
      serialize();
    }
  }

  public void close() throws IOException {
    finish();
    out_.close();
  }

  protected void serialize() throws IOException {
    SerializedBlock block = builder_.build();
    builder_ = reinitializeBlockBuilder();
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
