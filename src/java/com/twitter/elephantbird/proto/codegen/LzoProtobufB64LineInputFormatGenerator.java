package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineInputFormat;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class LzoProtobufB64LineInputFormatGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/mapreduce/input/Lzo%sProtobufB64LineInputFormat.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.mapreduce.input;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", LzoProtobufB64LineInputFormat.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl().endl();

    sb.append("public class Lzo%sProtobufB64LineInputFormat extends LzoProtobufB64LineInputFormat<%s> {", descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Lzo%sProtobufB64LineInputFormat() {", descriptorProto_.getName()).endl();
    sb.append("    super(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
