package com.twitter.elephantbird.pig.util;

import java.util.List;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

@SuppressWarnings("serial")
/**
 * This class wraps a protocol buffer message and attempts to delay parsing until individual
 * fields are requested.
 */
public class ProtobufTuple extends AbstractLazyTuple {

  private final Message msg_;
  private final Descriptor descriptor_;
  private final List<FieldDescriptor> fieldDescriptors_;
  private final ProtobufToPig protoConv_;
  private final int protoSize_;

  public ProtobufTuple(Message msg) {
    msg_ = msg;
    descriptor_ = msg.getDescriptorForType();
    fieldDescriptors_ = descriptor_.getFields();
    protoSize_ = fieldDescriptors_.size();
    protoConv_ = new ProtobufToPig();
    initRealTuple(protoSize_);
  }

  protected Object getObjectAt(int idx) {
    FieldDescriptor fieldDescriptor = fieldDescriptors_.get(idx);
    Object fieldValue = msg_.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
      return protoConv_.messageToTuple(fieldDescriptor, fieldValue);
    } else {
      return protoConv_.singleFieldToTuple(fieldDescriptor, fieldValue);
    }
  }

  @Override
  public long getMemorySize() {
    // The protobuf estimate is obviously inaccurate.
    return msg_.getSerializedSize() + realTuple.getMemorySize();
  }
}
