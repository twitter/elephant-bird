package com.twitter.elephantbird.proto.codegen;

import java.util.Map;

public class ProtoCodeGenOptions {
  public static final String EXTENSION_SUPPORT =
    "protobuf_extension_support";

  private boolean supportExtension_ = false;

  public ProtoCodeGenOptions() {
  }

  public void setOptions(Map<String, String> options) {
    if(options.containsKey(EXTENSION_SUPPORT)) {
      supportExtension_ = options.get(EXTENSION_SUPPORT).equals("true");
    }
  }

  public void setSupportExtension(boolean supportExtension) {
    supportExtension_ = supportExtension;
  }

  public boolean isSupportExtension() {
    return supportExtension_;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(EXTENSION_SUPPORT + ": " + supportExtension_);
    return sb.toString();
  }
}
