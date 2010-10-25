package com.twitter.elephantbird.mapreduce.io;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.twitter.data.proto.BlockStorage.SerializedBlock;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.StreamSearcher;
import com.twitter.elephantbird.util.TypeRef;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
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

public class ProtobufBlockReader<M extends Message> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufBlockReader.class);

  private final InputStream in_;
  private final StreamSearcher searcher_;
  private final Function<byte[], M> protoConverter_;
  private SerializedBlock curBlock_;
  private int numLeftToReadThisBlock_ = 0;
  private boolean readNewBlocks_ = true;

  public ProtobufBlockReader(InputStream in, TypeRef<M> typeRef) {
    LOG.info("ProtobufReader, my typeClass is " + typeRef.getRawClass());
    in_ = in;
    protoConverter_ = Protobufs.getProtoConverter(typeRef.getRawClass());
    searcher_ = new StreamSearcher(Protobufs.KNOWN_GOOD_POSITION_MARKER);
  }

  public void close() throws IOException {
    in_.close();
  }

  public boolean readProtobuf(ProtobufWritable<M> message) throws IOException {
    message.clear();

    if (!setupNewBlockIfNeeded()) {
      return false;
    }

    int blobIndex = curBlock_.getProtoBlobsCount() - numLeftToReadThisBlock_;
    byte[] blob = curBlock_.getProtoBlobs(blobIndex).toByteArray();
    message.set(protoConverter_.apply(blob));
    numLeftToReadThisBlock_--;
    return true;
  }

  public boolean readProtobufBytes(BytesWritable message) throws IOException {
    if (!setupNewBlockIfNeeded()) {
      return false;
    }

    int blobIndex = curBlock_.getProtoBlobsCount() - numLeftToReadThisBlock_;
    byte[] blob = curBlock_.getProtoBlobs(blobIndex).toByteArray();
    message.set(blob, 0, blob.length);
    numLeftToReadThisBlock_--;
    return true;
  }

  public void markNoMoreNewBlocks() {
    readNewBlocks_ = false;
  }

  public boolean skipToNextSyncPoint() throws IOException {
    return searcher_.search(in_);
  }

  public SerializedBlock parseNextBlock() throws IOException {
    LOG.debug("ProtobufReader: none left to read, skipping to sync point");
    if (!skipToNextSyncPoint()) {
      LOG.debug("ProtobufReader: SYNC point eof");
      // EOF if there are no more sync markers.
      return null;
    }

    int blockSize = readInt();
    LOG.debug("ProtobufReader: found sync point, next block has size " + blockSize);
    if (blockSize < 0) {
      LOG.debug("ProtobufReader: reading size after sync point eof");
      // EOF if the size cannot be read.
      return null;
    }

    byte[] byteArray = new byte[blockSize];
    IOUtils.readFully(in_, byteArray, 0, blockSize);
    SerializedBlock block = SerializedBlock.parseFrom(byteArray);

    numLeftToReadThisBlock_ = block.getProtoBlobsCount();
    LOG.debug("ProtobufReader: number in next block is " + numLeftToReadThisBlock_);
    return block;
  }

  private boolean setupNewBlockIfNeeded() throws IOException {
    if (numLeftToReadThisBlock_ == 0) {
      if (!readNewBlocks_) {
        // If the reader has been told not to read more blocks, stop.
        // This happens when a map boundary has been crossed in a map job, for example.
        // The goal then is to finsh reading what has been parsed, but let the next split
        // handle everything starting at the next sync point.
        return false;
      }
      curBlock_ = parseNextBlock();
      if (curBlock_ == null) {
        // If there is nothing, it likely means EOF. Signal that processing is done.
        return false;
      }
    }

    return true;
  }

  private int readInt() throws IOException {
    int b = in_.read();
    if (b == -1) {
      return -1;
    }

    return b | (in_.read() << 8) | (in_.read() << 16) | (in_.read() << 24);
  }
}
