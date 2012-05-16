package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.pig.store.LzoProtobufB64LinePigStorage;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class LzoProtobufB64LinePigStorageGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/pig/store/Lzo%sProtobufB64LinePigStorage.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.pig.store;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", LzoProtobufB64LinePigStorage.class.getName()).endl();
    sb.append("import %s;", TypeRef.class.getName()).endl().endl();

    sb.append("public class Lzo%sProtobufB64LinePigStorage extends LzoProtobufB64LinePigStorage<%s> {", descriptorProto_.getName(), descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Lzo%sProtobufB64LinePigStorage() {", descriptorProto_.getName()).endl();
    sb.append("    setTypeRef(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
