package com.twitter.elephantbird.proto.codegen;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;

public class ProtobufExtensionRegistryGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    String className = ProtobufExtensionRegistryGenerator.getProtobufExtensionRegistryClassName(
        packageName_, descriptorProto_);
    return  String.format("%s.java", className.replaceAll("\\.", "/"));
  }


  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.proto;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s;", ProtobufExtensionRegistry.class.getName()).endl();
//    sb.append("import %s;", ExtensionRegistry.class.getName()).endl().endl();
    sb.endl();

    sb.append("public class Protobuf%sExtensionRegistry extends ProtobufExtensionRegistry<%s> {",
        descriptorProto_.getName(), descriptorProto_.getName()).endl();

    sb.append("  public Protobuf%sExtensionRegistry() {", descriptorProto_.getName()).endl();
    if(protoExtensionNames_ != null) {
      for(String e: protoExtensionNames_) {
        sb.append("    addExtensions(%s);", e).endl();
      }
    }
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }

  public static String getProtobufExtensionRegistryClassName(
      String packageName, DescriptorProto descriptorProto) {
    return String.format("%s.proto.Protobuf%sExtensionRegistry",
        packageName, descriptorProto.getName());
  }
}
