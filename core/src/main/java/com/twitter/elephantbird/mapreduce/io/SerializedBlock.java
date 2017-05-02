package com.twitter.elephantbird.mapreduce.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;

import org.apache.hadoop.io.IOUtils;

/**
 * This is a {@link DynamicMessage} equivalent of following protobuf : <pre>
 *
 * message SerializedBlock {
 *  optional int32 version              = 1;
 *  optional string proto_class_name    = 2;
 *  repeated bytes proto_blobs          = 3;
 * };       </pre>
 *
 * This protobuf is required for BinaryBlock format used to store to binary records.
 * See {@link BinaryBlockReader} and {@link BinaryBlockWriter}. The file layout
 * is described in a comment at the bottom of this file. The format is an alternative
 * to a SequenceFile. <p>
 *
 * The primary purpose of DynamicMessage is to avoid any dependence on
 * pre-generated protobuf classes, since generated files are not compatible between
 * protobuf versions (2.4.1 and 2.5.0).This class works with either of these
 * versions at runtime.<p>
 *
 * Developer Note: More documentation on protobuf fields and file format
 * is included at the bottom of this file.
 */
public class SerializedBlock {

  private final Message message;

  // use newInstance() to create a new message
  private SerializedBlock(Message message) {
    this.message = message;
  }

  public Message getMessage() {
    return message;
  }

  public int getVersion() {
    return (Integer) message.getField(versionDesc);
  }

  public String getProtoClassName() {
    return (String) message.getField(protoClassNameDesc);
  }

  public List<ByteString> getProtoBlobs() {
    return (List<ByteString>) message.getField(protoBlobsDesc);
  }

  public static SerializedBlock newInstance(String protoClassName, List<ByteString> protoBlobs) {
    return new SerializedBlock(
        DynamicMessage.newBuilder(messageDescriptor)
            .setField(versionDesc, Integer.valueOf(1))
            .setField(protoClassNameDesc, protoClassName)
            .setField(protoBlobsDesc, protoBlobs)
            .build());
  }

  public static SerializedBlock parseFrom(InputStream in, int maxSize)
                                          throws InvalidProtocolBufferException, IOException {
    // create a CodedInputStream so that protobuf can enforce the configured max size
    // instead of using the default which may not be large enough for this data
    CodedInputStream codedInput = CodedInputStream.newInstance(in);
    codedInput.setSizeLimit(maxSize);
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(messageDescriptor)
      .mergeFrom(codedInput);
    // verify we've read to the end
    codedInput.checkLastTagWas(0);
    return new SerializedBlock(messageBuilder.build());
  }

  public static SerializedBlock parseFrom(byte[] messageBuffer)
                                          throws InvalidProtocolBufferException {
    return new SerializedBlock(
        DynamicMessage.newBuilder(messageDescriptor)
            .mergeFrom(messageBuffer)
            .build());
  }

  private static final Descriptors.Descriptor messageDescriptor;
  private static final Descriptors.FieldDescriptor versionDesc;
  private static final Descriptors.FieldDescriptor protoClassNameDesc;
  private static final Descriptors.FieldDescriptor protoBlobsDesc;

  static {
    // initialize messageDescriptor and the three field descriptors

    DescriptorProtos.FieldDescriptorProto version =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("version")
        .setNumber(1)
        .setType(Type.TYPE_INT32)
        .setLabel(Label.LABEL_OPTIONAL)
        .build();

    DescriptorProtos.FieldDescriptorProto protoClassName =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("proto_class_name")
        .setNumber(2)
        .setType(Type.TYPE_STRING)
        .setLabel(Label.LABEL_OPTIONAL)
        .build();

    DescriptorProtos.FieldDescriptorProto protoBlobs =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName("proto_blobs")
        .setNumber(3)
        .setType(Type.TYPE_BYTES)
        .setLabel(Label.LABEL_REPEATED)
        .build();

    try {
      messageDescriptor = Protobufs.makeMessageDescriptor(
              DescriptorProtos.DescriptorProto.newBuilder()
                .setName("SerializedBlock")
                .addField(version)
                .addField(protoClassName)
                .addField(protoBlobs)
                .build());
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }

    versionDesc        = messageDescriptor.findFieldByName("version");
    protoClassNameDesc = messageDescriptor.findFieldByName("proto_class_name");
    protoBlobsDesc     = messageDescriptor.findFieldByName("proto_blobs");
  }
}

/*
  Contents of old protobuf spec file block_storage.proto
    which included detailed documentation :

  package com.twitter.data.proto;

  message SerializedBlock {
    // The version of the block format we are writing. Always set to 1.
    optional int32 version              = 1;
    // The class_name of the message, e.g. "com.twitter.data.proto.Tables.Status"
    optional string proto_class_name    = 2;
    // A list of serialized byte blobs of the contained protocol buffers.
    // Generally there should be no more than 1000 or so blobs per block,
    // because all of them get parsed into memory at once during analysis.
    // The number of blobs per block can vary arbitrarily, and can even be
    // just 1 if necessary (somewhat space-inefficient).
    repeated bytes proto_blobs          = 3;
  };

  // The serialization format for all protobuf data consists of repeated sequences that look like

  // 16-byte GUID | 4-byte size, little-endian | that many bytes of a serialized SerializedBlock data structure
  // 16-byte GUID | 4-byte size, little-endian | that many bytes of a serialized SerializedBlock data structure
  // ...
  // 16-byte GUID | 4-byte size, little-endian | that many bytes of a serialized SerializedBlock data structure

  // The 16-byte GUID is ALWAYS the following:
  // 0x29, 0xd8, 0xd5, 0x06, 0x58, 0xcd, 0x4c, 0x29, 0xb2, 0xbc, 0x57, 0x99, 0x21, 0x71, 0xbd, 0xff
  //

  // Pseudocode for serializing the block is the following (approximately in Java):
  // SerializedBlock block = SerializedBlock.newBuilder().setVersion(1)
  //                                                     .setProtoClassName(Status.class.getName())
  //                                                     .addProtoBlobs(status1.toByteString())
  //                                                     .addProtoBlobs(status2.toByteString())
  //                                                     .build();
  // Now write to disk, with os being an OutputStream:
  // os.write(<bytes of the guid above>)
  // os.write(<raw little endian of block.getSerializedSize())
  // os.write(block.toByteArray());
  //
  // and repeat.
  // Note that the description above implies that all files start with the GUID.
*/
