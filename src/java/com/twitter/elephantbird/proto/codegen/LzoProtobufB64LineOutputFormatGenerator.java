package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.mapreduce.output.LzoProtobufB64LineOutputFormat;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class LzoProtobufB64LineOutputFormatGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/mapreduce/output/Lzo%sProtobufB64LineOutputFormat.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.mapreduce.output;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", LzoProtobufB64LineOutputFormat.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl().endl();

    sb.append("public class Lzo%sProtobufB64LineOutputFormat extends LzoProtobufB64LineOutputFormat<%s> {", descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Lzo%sProtobufB64LineOutputFormat() {", descriptorProto_.getName()).endl();
    sb.append("    setTypeRef(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
