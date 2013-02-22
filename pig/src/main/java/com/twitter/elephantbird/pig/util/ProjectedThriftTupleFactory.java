package com.twitter.elephantbird.pig.util;

import java.util.List;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TBase;

import com.google.common.base.Preconditions;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.TypeRef;

/**
 * A tuple factory to create thrift tuples where
 * only a subset of fields are required.
 */
public class ProjectedThriftTupleFactory<T extends TBase<?, ?>> {

  private static TupleFactory tf  = TupleFactory.getInstance();

  private int[] requiredFields;
  private final TStructDescriptor tStructDesc;

  public ProjectedThriftTupleFactory(TypeRef<T> typeRef, RequiredFieldList requiredFieldList) {
    tStructDesc = TStructDescriptor.getInstance(typeRef.getRawClass());
    int numFields = tStructDesc.getFields().size();

    if (requiredFieldList != null) {
      List<RequiredField> tupleFields = requiredFieldList.getFields();
      requiredFields = new int[tupleFields.size()];

      // should we handle nested projections? not yet.

      int i = 0;
      for(RequiredField f : tupleFields) {
        Preconditions.checkState(f.getIndex() < numFields,
                                 "Projected index is out of range");
        requiredFields[i++] = f.getIndex();
      }
    } else { // all the fields are required
      requiredFields = new int[numFields];
      for (int i=0; i < numFields; i++) {
        requiredFields[i] = i;
      }
    }
  }

  public Tuple newTuple(T tObject) throws ExecException {
    int size = requiredFields.length;
    List<Field> tFields = tStructDesc.getFields();

    Tuple tuple = tf.newTuple(size);

    for(int i=0; i < size; i++) {
      int idx = requiredFields[i];
      Object value = tStructDesc.getFieldValue(idx, tObject);
      tuple.set(i, ThriftToPig.toPigObject(tFields.get(idx), value, true));
    }

    return tuple;
  }
}
