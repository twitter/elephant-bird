package com.twitter.elephantbird.pig.util;

import java.util.List;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A tuple factory to create protobuf tuples where
 * only a subset of fields are required.
 */
public class ProjectedProtobufTupleFactory<M extends Message> {

  private static TupleFactory tf  = TupleFactory.getInstance();

  private final List<FieldDescriptor> requiredFields;
  private final ProtobufToPig protoConv;
  private final ProtobufExtensionRegistry extensionRegistry;


  public ProjectedProtobufTupleFactory(TypeRef<M> typeRef, RequiredFieldList requiredFieldList) {
    this(typeRef, requiredFieldList, null);
  }

  public ProjectedProtobufTupleFactory(TypeRef<M> typeRef, RequiredFieldList requiredFieldList,
      ProtobufExtensionRegistry extensionRegistry) {
    this.extensionRegistry = extensionRegistry;

    List<FieldDescriptor> protoFields =
      Protobufs.getMessageAllFields(Protobufs.getMessageDescriptor(typeRef.getRawClass()),
          this.extensionRegistry);
    protoConv = new ProtobufToPig();

    if (requiredFieldList != null) {
      List<RequiredField> tupleFields = requiredFieldList.getFields();
      requiredFields = Lists.newArrayListWithCapacity(tupleFields.size());

      // should we handle nested projections?
      for(RequiredField f : tupleFields) {
        requiredFields.add(protoFields.get(f.getIndex()));
      }
    } else {
      requiredFields = protoFields;
    }
  }

  public Tuple newTuple(M msg) throws ExecException {
    int size = requiredFields.size();
    Tuple tuple = tf.newTuple(size);

    for(int i=0; i < size; i++) {
      FieldDescriptor fdesc = requiredFields.get(i);
      Object value = msg.getField(fdesc);
      tuple.set(i, protoConv.fieldToPig(fdesc, value, extensionRegistry));
    }
    return tuple;
  }
}
