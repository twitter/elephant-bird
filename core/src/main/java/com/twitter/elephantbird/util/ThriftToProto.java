package com.twitter.elephantbird.util;

import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.protocol.TType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;

/**
 * Translates a Thrift object into a Protocol Buffer message.
 * <p>
 * Expects 1-1 correspondence in field names and types.
 * Missing fields and mismatched types will result in exceptions being thrown.
 * <p>
 * TODO: handle complex fields. Only primitives are mapped right now.
 *
 * @author dmitriy
 *
 * @param <T> Source thrift class
 * @param <P> Target Protocol Buffer Message class
 */
@SuppressWarnings("rawtypes")
public class ThriftToProto<T extends TBase, P extends Message> {

  Message.Builder protoBuilder_;
  P protoObj_;
  TypeRef<P> typeRef_;

  public ThriftToProto(T thriftObj, P protoObj) {
    protoBuilder_ = Protobufs.getMessageBuilder(protoObj.getClass());
    protoObj_ = protoObj;
  }

  public static <T extends TBase<?, ?>, P extends Message> ThriftToProto<T, P>
      newInstance(T thriftObj, P protoObj) {
    return new ThriftToProto<T, P>(thriftObj, protoObj);
  }

  @SuppressWarnings("unchecked")
  public  P convert(T thriftObj) {

    Descriptor protoDesc = protoObj_.getDescriptorForType();

    Map<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> fieldMap =
      FieldMetaData.getStructMetaDataMap(thriftObj.getClass());

    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> e : fieldMap.entrySet()) {
      final TFieldIdEnum tFieldIdEnum = e.getKey();
      final FieldValueMetaData thriftMetadata = e.getValue().valueMetaData;

      FieldDescriptor protoFieldDesc = protoDesc.findFieldByName(tFieldIdEnum.getFieldName());
      if ( protoFieldDesc == null ) {
        throw new RuntimeException("Field " + tFieldIdEnum.getFieldName() +
            " not found in " + protoObj_.getClass().getCanonicalName());
      } else if (!typesMatch(protoFieldDesc, thriftMetadata)) {
        throw new RuntimeException("Field " + tFieldIdEnum.getFieldName() + " type does not match: " +
            "thrift " + thriftMetadata.type + "vs " + protoFieldDesc.getType());
      }

      Object fieldValue = thriftObj.getFieldValue(tFieldIdEnum);
      if (protoFieldDesc.getType() == Type.BYTES) {
        protoBuilder_.setField(protoFieldDesc, (ByteString.copyFrom((byte [])fieldValue)));
      } else {
        protoBuilder_.setField(protoFieldDesc, fieldValue);
      }
    }
    return (P) protoBuilder_.build();
  }

  private boolean typesMatch(FieldDescriptor protoFieldDesc, FieldValueMetaData thriftMetadata) {
    if (thriftMetadata.isStruct()) {
      // TODO: Handle structs
      return false;
    }

    byte thriftType = thriftMetadata.type;

    // TODO: Handle Lists that are more than 1 level deep. Be all pro-style with the recursion.
    if (thriftMetadata.type == TType.LIST && protoFieldDesc.isRepeated())  {
      FieldValueMetaData listMeta = ((ListMetaData) thriftMetadata).elemMetaData;
      if (listMeta.isStruct() || listMeta.isContainer()) {
        return false;
      }
      thriftType = listMeta.type;
    }
    return (protoFieldDesc.getType().equals(thriftTypeToProtoType(thriftType)) ||
        thriftBinSucks(protoFieldDesc.getType(), thriftType));
  }

  private boolean thriftBinSucks(Type protoType, byte thriftType) {
    return (thriftType == TType.STRING && protoType == Type.BYTES || protoType == Type.STRING);
  }

  private Type thriftTypeToProtoType(byte thriftType) {
    switch (thriftType)  {
      case TType.BOOL:
        return Type.BOOL;
      case TType.BYTE:
        return Type.INT32;
      case TType.DOUBLE:
        return Type.DOUBLE;
      case TType.I16:
        return Type.INT32;
      case TType.I32:
        return Type.INT32;
      case TType.I64:
        return Type.INT64;
      case TType.STRING:
        return Type.STRING;
        // EXCEPT WHEN IT'S A BINARY BLOB. THANKS, THRIFT.
      default:
        throw new IllegalArgumentException("Can't map Thrift type " + thriftType + " to a ProtoBuf type");
    }
  }

}
