package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.mapred.input.DeprecatedMultiInputFormat;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class DeprecatedLzoProtobufBlockInputFormatGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/mapred/input/DeprecatedLzo%sProtobufBlockInputFormat.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.mapred.input;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", DeprecatedMultiInputFormat.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl().endl();

    sb.append("public class DeprecatedLzo%sProtobufBlockInputFormat extends DeprecatedMultiInputFormat<%s> {", descriptorProto_.getName(), descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public DeprecatedLzo%sProtobufBlockInputFormat() {", descriptorProto_.getName()).endl();
    sb.append("    super(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
