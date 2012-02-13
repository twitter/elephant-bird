package com.twitter.elephantbird.proto.codegen;

import java.util.Map;

public class ProtoCodeGenOptions {
  private static final String OPTION_PROTOBUF_EXTENSION_SUPPORT =
    "protobuf_extension_support";

  private boolean supportProtobufExtension_ = false;

  public ProtoCodeGenOptions() {
  }

  public void setOptions(Map<String, String> options) {
    if(options.containsKey(OPTION_PROTOBUF_EXTENSION_SUPPORT)) {
      supportProtobufExtension_ = options.get(OPTION_PROTOBUF_EXTENSION_SUPPORT)=="true";
    }
  }

  public void setSupportProtobufExtension(boolean supportProtobufExtension) {
    supportProtobufExtension_ = supportProtobufExtension;
  }

  public boolean isSupportProtobufExtension() {
    return supportProtobufExtension_;
  }
}
