package com.twitter.elephantbird.proto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;

public class ProtobufExtensionRegistry<M extends Message> {
  private List<GeneratedExtension<M, ?>> extensions_ = new ArrayList<GeneratedExtension<M,?>>();
  private ExtensionRegistry extensionRegistry_ = ExtensionRegistry.newInstance();

  public void addExtension(GeneratedExtension<M, ?> extension) {
    extensionRegistry_.add(extension);
    extensions_.add(extension);
  }

  public List<GeneratedExtension<M, ?>> getExtensions() {
    return Collections.unmodifiableList(extensions_);
  }

  public ExtensionRegistry getRealExtensionRegistry() {
    return extensionRegistry_.getUnmodifiable();
  }
}
