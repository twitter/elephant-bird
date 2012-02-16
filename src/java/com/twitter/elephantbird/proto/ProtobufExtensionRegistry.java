package com.twitter.elephantbird.proto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;

public class ProtobufExtensionRegistry {
  private HashMap<String, List<GeneratedExtension<?, ?>>> protoExtensions_ =
    new HashMap<String, List<GeneratedExtension<?, ?>>>();
  private Map<String, ExtensionRegistry> protoExtensionRegistries_ =
    new HashMap<String, ExtensionRegistry>();
  private Map<String, Class<? extends Message>> extensionMessageClasses_ =
    new HashMap<String, Class<? extends Message>>();

  public static ProtobufExtensionRegistry emptyExtensionRegistry() {
    return new ProtobufExtensionRegistry();
  }

  public ProtobufExtensionRegistry() {
  }

  public void addExtension(GeneratedExtension<?, ?> extension) {
    String protoFullName = extension.getDescriptor().getContainingType().getFullName();
    List<GeneratedExtension<?, ?>> extensions = protoExtensions_.get(protoFullName);
    if(extensions == null) {
      extensions = new ArrayList<GeneratedExtension<?,?>>();
      protoExtensions_.put(protoFullName, extensions);
    }
    extensions.add(extension);

    ExtensionRegistry extReg = protoExtensionRegistries_.get(protoFullName);
    if(extReg == null) {
      extReg = ExtensionRegistry.newInstance();
      protoExtensionRegistries_.put(protoFullName, extReg);
    }
    extReg.add(extension);

    if(extension.getDescriptor().getType() == FieldDescriptor.Type.MESSAGE) {
    String extendeeClassname = Protobufs.getProtoClassName(
        extension.getDescriptor().getMessageType());
    extensionMessageClasses_.put(
        extension.getDescriptor().getMessageType().getFullName(),
        Protobufs.getProtobufClass(extendeeClassname));
    }

  }

  public List<GeneratedExtension<?, ?>> getExtensions(String protoFullName) {
    List<GeneratedExtension<?, ?>> ret = protoExtensions_.get(protoFullName);
    if(ret != null) {
      return Collections.unmodifiableList(ret);
    }
    return Collections.emptyList();
  }

  public List<GeneratedExtension<?, ?>> getExtensions(Descriptor descriptor) {
    return getExtensions(descriptor.getFullName());
  }

  public List<GeneratedExtension<?, ?>> getExtensions(FieldDescriptor fieldDescriptor) {
    Preconditions.checkArgument(fieldDescriptor.getType()==FieldDescriptor.Type.MESSAGE,
        fieldDescriptor + " must be message descriptor");
    return getExtensions(fieldDescriptor.getMessageType().getFullName());
  }

  public List<FieldDescriptor> getExtensionDescriptorFields(String protoFullName) {
    return Lists.transform(getExtensions(protoFullName),
        new Function<GeneratedExtension<?, ?>, FieldDescriptor>() {
      @Override
      public FieldDescriptor apply(GeneratedExtension<?, ?> extension) {
        return extension.getDescriptor();
      }
    });
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
    return extensionMessageClasses_.get(extensionFd.getFullName());
  }

  public ExtensionRegistry getExtensionRegistry(String protoFullName) {
    ExtensionRegistry extReg = protoExtensionRegistries_.get(protoFullName);
    if(extReg != null) {
      return extReg.getUnmodifiable();
    }
    return null;
  }

  public ExtensionRegistry getExtensionRegistry(Descriptor descriptor) {
    return getExtensionRegistry(descriptor.getFullName());
  }

  public ExtensionRegistry getExtensionRegistry(FieldDescriptor fieldDescriptor) {
    Preconditions.checkArgument(fieldDescriptor.getType()==FieldDescriptor.Type.MESSAGE,
        fieldDescriptor + " must be message descriptor");
    return getExtensionRegistry(fieldDescriptor.getFullName());
  }
}
