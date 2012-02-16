package com.twitter.elephantbird.pig.util;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;

@SuppressWarnings("serial")
/**
 * This class wraps a protocol buffer message and attempts to delay parsing until individual
 * fields are requested.
 */
public class ProtobufTuple extends AbstractLazyTuple {

  private final Message msg_;
  private final List<FieldDescriptor> fieldDescriptors_;
  private final ProtobufToPig protoConv_;
  private final int protoSize_;
  private final ProtobufExtensionRegistry extensionRegistry_;

  public ProtobufTuple(Message msg) {
    this(msg, null);
  }

  public ProtobufTuple(Message msg, ProtobufExtensionRegistry extensionRegistry) {
    msg_ = msg;
    extensionRegistry_ = extensionRegistry;

    fieldDescriptors_ = new ArrayList<FieldDescriptor>(msg.getDescriptorForType().getFields());
    if(extensionRegistry_ != null) {
      for(GeneratedExtension<?, ?> e: extensionRegistry_.getExtensions(msg.getDescriptorForType())) {
        fieldDescriptors_.add(e.getDescriptor());
      }
    }
    protoSize_ = fieldDescriptors_.size();
    protoConv_ = new ProtobufToPig();
    initRealTuple(protoSize_);
  }

  @Override
  protected Object getObjectAt(int idx) {
    FieldDescriptor fieldDescriptor = fieldDescriptors_.get(idx);
    Object fieldValue = msg_.getField(fieldDescriptor);
    return protoConv_.fieldToPig(fieldDescriptor, fieldValue, extensionRegistry_);
  }

  @Override
  public long getMemorySize() {
    // The protobuf estimate is obviously inaccurate.
    return msg_.getSerializedSize() + realTuple.getMemorySize();
  }
}
