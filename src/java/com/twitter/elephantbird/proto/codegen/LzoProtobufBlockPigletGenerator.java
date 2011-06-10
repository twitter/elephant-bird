package com.twitter.elephantbird.proto.codegen;

import com.google.protobuf.Descriptors.Descriptor;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.load.LzoProtobufBlockPigLoader;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.proto.util.ProtogenHelper;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.Strings;

public class LzoProtobufBlockPigletGenerator extends ProtoCodeGenerator {
  ProtobufToPig protoToPig_ = new ProtobufToPig();

  @Override
  public String getFilename() {
    return String.format("pig/%s/%s.piglet",
        packageName_.replaceAll("\\.", "/"), Strings.tableize(descriptorProto_.getName()));
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    Descriptor msgDescriptor = Protobufs.getMessageDescriptor(
        ProtogenHelper.getProtoClass(packageName_, protoFilename_, descriptorProto_.getName()));

    sb.append(protoToPig_.toPigScript(msgDescriptor, LzoProtobufBlockPigLoader.class.getCanonicalName(),
        String.format("%s.%s.%s", packageName_, protoFilename_, descriptorProto_.getName())
    )).endl();
    sb.endl();

    return sb.toString();
  }
}
