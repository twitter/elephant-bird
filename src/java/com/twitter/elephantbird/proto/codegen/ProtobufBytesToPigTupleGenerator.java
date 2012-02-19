package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.pig.piggybank.ProtobufBytesToTuple;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class ProtobufBytesToPigTupleGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/pig/piggybank/%sProtobufBytesToTuple.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.pig.piggybank;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", ProtobufBytesToTuple.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl();
    if(codeGenOptions_.isSupportExtension()) {
      sb.append("import %s;", ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, true)).endl();
    }
    sb.endl();

    sb.append("public class %sProtobufBytesToTuple extends ProtobufBytesToTuple<%s> {", descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public %sProtobufBytesToTuple() {", descriptorProto_.getName()).endl();

    if(codeGenOptions_.isSupportExtension()) {
      String extRegClassName = ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, false);
      sb.append("    super(new TypeRef<%s>(){}, %s.getInstance());",
          descriptorProto_.getName(), extRegClassName).endl();
    } else {
      sb.append("    super(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    }
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
