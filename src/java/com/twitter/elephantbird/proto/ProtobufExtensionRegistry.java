package com.twitter.elephantbird.proto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;

public class ProtobufExtensionRegistry {
  private Map<String, LinkedHashSet<GeneratedExtension<?, ?>>> extensionsByExtendeeTypeName_ =
    new HashMap<String, LinkedHashSet<GeneratedExtension<?, ?>>>();
  private Map<String, Class<? extends Message>> extClassesByExtensionTypeName_ =
    new HashMap<String, Class<? extends Message>>();

  private ExtensionRegistry extensionRegistry_ = ExtensionRegistry.newInstance();

  public static ProtobufExtensionRegistry emptyExtensionRegistry() {
    return new ProtobufExtensionRegistry();
  }

  public ProtobufExtensionRegistry() {
  }

  public void addExtension(GeneratedExtension<?, ?> extension) {
    String extendeeTypeName = extension.getDescriptor().getContainingType().getFullName();

    LinkedHashSet<GeneratedExtension<?, ?>> extensions =
      extensionsByExtendeeTypeName_.get(extendeeTypeName);
    if(extensions == null) {
      extensions = new LinkedHashSet<GeneratedExtension<?,?>>();
      extensionsByExtendeeTypeName_.put(extendeeTypeName, extensions);
    }
    extensions.add(extension);

    extensionRegistry_.add(extension);

    if(extension.getDescriptor().getType() == FieldDescriptor.Type.MESSAGE) {
      String extendeeClassname = Protobufs.getProtoClassName(
          extension.getDescriptor().getMessageType());
      extClassesByExtensionTypeName_.put(
          extension.getDescriptor().getMessageType().getFullName(),
          Protobufs.getProtobufClass(extendeeClassname));
    }
  }

  public Set<GeneratedExtension<?, ?>> getExtensions(String protoTypeName) {
    Set<GeneratedExtension<?, ?>> ret = extensionsByExtendeeTypeName_.get(protoTypeName);
    if(ret != null) {
      return Collections.unmodifiableSet(ret);
    }
    return Collections.emptySet();
  }

  public Set<GeneratedExtension<?, ?>> getExtensions(Descriptor descriptor) {
    return getExtensions(descriptor.getFullName());
  }

  public Set<GeneratedExtension<?, ?>> getExtensions(FieldDescriptor fieldDescriptor) {
    Preconditions.checkArgument(fieldDescriptor.getType()==FieldDescriptor.Type.MESSAGE,
        fieldDescriptor + " must be message descriptor");
    return getExtensions(fieldDescriptor.getMessageType().getFullName());
  }

  public List<FieldDescriptor> getExtensionDescriptorFields(String protoFullName) {
    return new ArrayList<FieldDescriptor>(Collections2.transform(getExtensions(protoFullName),
        new Function<GeneratedExtension<?, ?>, FieldDescriptor>() {
      @Override
      public FieldDescriptor apply(GeneratedExtension<?, ?> extension) {
        return extension.getDescriptor();
      }
    }));
  }

  public List<FieldDescriptor> getExtensionDescriptorFields(Descriptor descriptor) {
    return getExtensionDescriptorFields(descriptor.getFullName());
  }

  public List<FieldDescriptor> getExtensionDescriptorFields(FieldDescriptor fieldDescriptor) {
    Preconditions.checkArgument(fieldDescriptor.getType()==FieldDescriptor.Type.MESSAGE,
        fieldDescriptor + " must be message descriptor");
    return getExtensionDescriptorFields(fieldDescriptor.getMessageType().getFullName());
  }

  public Class<? extends Message> getExtensionClass(Descriptor extensionFd) {
    return extClassesByExtensionTypeName_.get(extensionFd.getFullName());
  }

  public ExtensionRegistry getExtensionRegistry() {
    return extensionRegistry_.getUnmodifiable();
  }
}
