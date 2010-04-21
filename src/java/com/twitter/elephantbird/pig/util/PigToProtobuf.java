package com.twitter.elephantbird.pig.util;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.twitter.data.proto.Misc.CountedMap;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for turning codegen'd protos into Pig Tuples and Schemas
 * for custom Pig LoadFuncs.
 * @author Kevin Weil
 */
public class PigToProtobuf {
  private static final Logger LOG = LoggerFactory.getLogger(PigToProtobuf.class);

  public PigToProtobuf() {}

  /**
   * Turn a generic message into a Tuple.  Individual fields that are enums
   * are converted into their string equivalents.  Fields that are not filled
   * out in the protobuf are set to null, unless there is a default field value in
   * which case that is used instead.
   * @param msg the protobuf message
   * @return a pig tuple representing the message.
   */
  public Message tupleToMessage(TypeRef typeRef, Tuple tuple) {
	List<FieldDescriptor> fieldDescriptors = Protobufs.getMessageDescriptor(typeRef.getRawClass()).getFields();
	Builder builder = Protobufs.getMessageBuilder(typeRef.getRawClass());

    if (tuple == null) {
	  return builder.build();
    }

	for (int i = 0; i < fieldDescriptors.size() && i < tuple.size(); i++) {
	  Object tupleField = null;
	  FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
	  
	  try {
		tupleField = tuple.get(i);
	  } catch (ExecException e) {
		LOG.warn("Could not convert tuple field " + tupleField + " to field with descriptor " + fieldDescriptor);
		continue;
	  }
	  
	  if (tupleField != null) {
		if (fieldDescriptor.isRepeated()) { 
		  // Repeated fields are set with Lists containing objects of the fields' Java type.
		  builder.setField(fieldDescriptor, dataBagToProtobufList(fieldDescriptor, (DataBag)tupleField));
		} else {
		  if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
			throw new IllegalArgumentException("Can't serialize nested messages");
			//builder.setField(fieldDescriptor, tupleToMessage(?, tupleField));
		  } else {
			builder.setField(fieldDescriptor, tupleField);
		  }
		}
	  }
	}

	return builder.build();
  }

  public List dataBagToProtobufList(FieldDescriptor fieldDescriptor, DataBag bag) {
	ArrayList bagContents = new ArrayList((int)bag.size());
	Iterator<Tuple> bagIter = bag.iterator();

	while (bagIter.hasNext()) {
	  Tuple tuple = bagIter.next();
	  if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
		throw new IllegalArgumentException("Can't serialize nested messages");
		//bagContents.add(?, tupleToMessage(tuple));
	  } else {
		try {
		  bagContents.add(tuple.get(0));
		} catch (ExecException e) {
		  LOG.warn("Could not add a value for repeated field with descriptor " + fieldDescriptor);
		}		
	  }
	}
	  
	return bagContents;
  }
}
