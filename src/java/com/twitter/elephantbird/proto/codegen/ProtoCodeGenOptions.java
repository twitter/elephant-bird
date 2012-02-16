package com.twitter.elephantbird.proto.codegen;

import java.util.Map;

public class ProtoCodeGenOptions {
  public static final String OPTION_PROTOBUF_EXTENSION_SUPPORT =
    "protobuf_extension_support";
  public static final String OPTION_PROTOBUF_EXTENSION_CLASSNAME =
    "protobuf_extension_class_name";

  private boolean supportProtobufExtension_ = false;
  private String protobufExtensionClassName_;

  public ProtoCodeGenOptions() {
  }

  public void setOptions(Map<String, String> options) {
    if(options.containsKey(OPTION_PROTOBUF_EXTENSION_SUPPORT)) {
      supportProtobufExtension_ = options.get(OPTION_PROTOBUF_EXTENSION_SUPPORT)=="true";
      protobufExtensionClassName_ = options.get(OPTION_PROTOBUF_EXTENSION_CLASSNAME);
    }
  }

  public void setSupportProtobufExtension(boolean supportProtobufExtension) {
    supportProtobufExtension_ = supportProtobufExtension;
  }

  public boolean isSupportProtobufExtension() {
    return supportProtobufExtension_;
  }

  public String getProtobufExtensionRegistryClassName() {
    return protobufExtensionClassName_;
  }
}
