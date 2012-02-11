package com.twitter.elephantbird.proto.codegen;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.proto.util.ProtogenHelper;
import com.twitter.elephantbird.util.TypeRef;

public class XProtobufWritableGenerator extends ProtoCodeGenerator {
  @Override
  public String getFilename() {
    return String.format("%s/mapreduce/io/XProtobuf%sWritable.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.mapreduce.io;", packageName_).endl().endl();

    String fullProtoClassName = ProtogenHelper.getProtoClassFullName(
        packageName_, protoFilename_, descriptorProto_.getName());

    sb.append("import %s;", fullProtoClassName).endl();
    sb.append("import %s;", ProtobufWritable.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl();
    sb.append("import %s;", List.class.getName());
    sb.append("import %s;", GeneratedExtension.class.getName());
    sb.endl();

    String protoExtensions = "null";
    if(!protoExtensionNames_.isEmpty()) {
      sb.append("new List<GeneratedExtension<%s, ?>>", descriptorProto_.getName());
    }

    sb.append("public class XProtobuf%sWritable extends ProtobufWritable<%s> {",
        descriptorProto_.getName(), descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Protobuf%sWritable() {", descriptorProto_.getName()).endl();
    sb.append("    super(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();

    sb.append("  public Protobuf%sWritable(%s m) {",
        descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("    super(m, new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();

    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }



  private <M extends Message> String generateExtensionListCode(String listName,
      String protoName, Collection<String> extensionNames) {

    List<GeneratedExtension<M, ?>> ret = new ArrayList<GeneratedExtension<M, ?>>();
    for(String e: extensionNames) {

    }

    FormattingStringBuffer sb = new FormattingStringBuffer();
    sb.append("List<GeneratedExtenson<%s, ?>> %s = " +
        "new ArrayList<GeneratedExtension<%s, ?>>(%d)",
        protoName, listName, protoName, extensionNames.size()).endl();
    for(String e: extensionNames) {
      sb.append("%s.add(%s)", listName, e).endl();
    }

    return sb.toString();
  }
}
