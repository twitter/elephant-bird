package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class ProtobufWritableGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/mapreduce/io/Protobuf%sWritable.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.mapreduce.io;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", ProtobufWritable.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl();
    if(codeGenOptions_.isSupportExtension()) {
      sb.append("import %s;", ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, true)).endl();
    }
    sb.endl();

    String extensionRegistry = "null";
    if(codeGenOptions_.isSupportExtension()) {
      String extRegClassName = ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, false);
      extensionRegistry = extRegClassName + ".getInstance()";
    }

    sb.append("public class Protobuf%sWritable extends ProtobufWritable<%s> {",
        descriptorProto_.getName(), descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Protobuf%sWritable() {", descriptorProto_.getName()).endl();
    sb.append("    super(new TypeRef<%s>(){}, %s);",
        descriptorProto_.getName(), extensionRegistry).endl();
    sb.append("  }").endl();

    sb.append("  public Protobuf%sWritable(%s m) {",
        descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("    super(m, new TypeRef<%s>(){}, %s);",
        descriptorProto_.getName(), extensionRegistry).endl();
    sb.append("  }").endl();

    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
