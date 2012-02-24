package com.twitter.elephantbird.proto.codegen;

import java.util.Set;

import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;

public class ProtobufExtensionRegistryGenerator extends ProtoCodeGenerator {
  private Set<String> extensionNames_;

  public ProtobufExtensionRegistryGenerator(Set<String> extensionNames) {
    extensionNames_ = extensionNames;
  }

  @Override
  public String getFilename() {
    return ProtobufExtensionRegistryGenerator.getGenClassName(packageName_,
        protoFilename_, true).replaceAll("\\.", "/") + ".java";
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s;", packageName_).endl().endl();
    sb.append("import %s;", ProtobufExtensionRegistry.class.getName()).endl();
    sb.endl();

    String className = ProtobufExtensionRegistryGenerator.getGenClassName(
        packageName_, protoFilename_, false);
    sb.append("public class %s extends ProtobufExtensionRegistry {",
        className, protoFilename_).endl();
    sb.append("public static final ProtobufExtensionRegistry INSTANCE = new %s();",
        className).endl().endl();

    sb.append("  public %s() {", className).endl();
    for(String e: extensionNames_) {
      sb.append("    addExtension(%s);", e).endl();
    }
    sb.append("  }").endl().endl();

    sb.append("  public static ProtobufExtensionRegistry getInstance() {").endl();
    sb.append("    return %s.INSTANCE;", className).endl();
    sb.append("  }").endl();

    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }

  public static String getGenClassName(String packageName, String protoName,
      boolean fullName) {
    String className = String.format("Protobuf%sExtensionRegistry", protoName);
    if(!fullName) {
      return className;
    }

    return String.format("%s.%s", packageName, className);
  }
}
