package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.pig.load.LzoProtobufB64LinePigLoader;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class LzoProtobufB64LinePigLoaderGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/pig/load/Lzo%sProtobufB64LinePigLoader.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.pig.load;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s.mapreduce.input.Lzo%sProtobufB64LineInputFormat;", packageName_, descriptorProto_.getName()).endl();
    sb.append("import %s;", LzoProtobufB64LinePigLoader.class.getName()).endl();
    sb.append("import org.apache.hadoop.mapreduce.InputFormat;").endl();
    sb.append("import %s;", TypeRef.class.getName()).endl().endl();

    sb.append("public class Lzo%sProtobufB64LinePigLoader extends LzoProtobufB64LinePigLoader<%s> {", descriptorProto_.getName(), descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Lzo%sProtobufB64LinePigLoader() {", descriptorProto_.getName()).endl();
    sb.append("    setTypeRef(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("  public InputFormat getInputFormat() {").endl();
    sb.append("    return new Lzo%sProtobufB64LineInputFormat();", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
