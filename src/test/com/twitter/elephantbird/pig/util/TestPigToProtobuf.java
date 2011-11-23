package com.twitter.elephantbird.pig.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Verifies that <code>PigToProtobuf</code> converts from Pig schemas to Protobuf descriptors properly
 *
 * @author billg
 */
public class TestPigToProtobuf {

  @Test
  public void testConvertValidTypes() throws Descriptors.DescriptorValidationException {
    Schema schema = new Schema();

    schema.add(new Schema.FieldSchema("chararray", DataType.CHARARRAY));
    schema.add(new Schema.FieldSchema("bytearray", DataType.BYTEARRAY));
    schema.add(new Schema.FieldSchema("boolean", DataType.BOOLEAN));
    schema.add(new Schema.FieldSchema("integer", DataType.INTEGER));
    schema.add(new Schema.FieldSchema("long", DataType.LONG));
    schema.add(new Schema.FieldSchema("float", DataType.FLOAT));
    schema.add(new Schema.FieldSchema("double", DataType.DOUBLE));

    Descriptors.Descriptor descriptor = PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(schema));

    Assert.assertEquals("Incorrect data size", 7, descriptor.getFields().size());
    Iterator<Descriptors.FieldDescriptor> fieldIterator = descriptor.getFields().iterator();
    assetFieldDescriptor(fieldIterator.next(), "chararray", Descriptors.FieldDescriptor.Type.STRING);
    assetFieldDescriptor(fieldIterator.next(), "bytearray", Descriptors.FieldDescriptor.Type.BYTES);
    assetFieldDescriptor(fieldIterator.next(), "boolean", Descriptors.FieldDescriptor.Type.BOOL);
    assetFieldDescriptor(fieldIterator.next(), "integer", Descriptors.FieldDescriptor.Type.INT32);
    assetFieldDescriptor(fieldIterator.next(), "long", Descriptors.FieldDescriptor.Type.INT64);
    assetFieldDescriptor(fieldIterator.next(), "float", Descriptors.FieldDescriptor.Type.FLOAT);
    assetFieldDescriptor(fieldIterator.next(), "double", Descriptors.FieldDescriptor.Type.DOUBLE);
  }

  @Test
  public void testConvertExtraFields() throws Descriptors.DescriptorValidationException {
    Schema schema = new Schema();

    schema.add(new Schema.FieldSchema("chararray", DataType.CHARARRAY));
    schema.add(new Schema.FieldSchema("bytearray", DataType.BYTEARRAY));

    List<Pair<String, DescriptorProtos.FieldDescriptorProto.Type>> extraFields =
        new ArrayList<Pair<String, DescriptorProtos.FieldDescriptorProto.Type>>();
    extraFields.add(new Pair<String, DescriptorProtos.FieldDescriptorProto.Type>(
        "extra_string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
    extraFields.add(new Pair<String, DescriptorProtos.FieldDescriptorProto.Type>(
        "extra_int", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));

    Descriptors.Descriptor descriptor = PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(schema), extraFields);

    Assert.assertEquals("Incorrect data size", 4, descriptor.getFields().size());
    Iterator<Descriptors.FieldDescriptor> fieldIterator = descriptor.getFields().iterator();
    assetFieldDescriptor(fieldIterator.next(), "chararray", Descriptors.FieldDescriptor.Type.STRING);
    assetFieldDescriptor(fieldIterator.next(), "bytearray", Descriptors.FieldDescriptor.Type.BYTES);
    assetFieldDescriptor(fieldIterator.next(), "extra_string", Descriptors.FieldDescriptor.Type.STRING);
    assetFieldDescriptor(fieldIterator.next(), "extra_int", Descriptors.FieldDescriptor.Type.INT32);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConvertInvalidTypeBag() throws Descriptors.DescriptorValidationException {
    Schema schema = new Schema();
    schema.add(new Schema.FieldSchema("bag", DataType.BAG));
    PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(schema));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConvertInvalidTypeMap() throws Descriptors.DescriptorValidationException {
    Schema schema = new Schema();
    schema.add(new Schema.FieldSchema("map", DataType.MAP));
    PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(schema));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConvertInvalidTypeTuple() throws Descriptors.DescriptorValidationException {
    Schema schema = new Schema();
    schema.add(new Schema.FieldSchema("tuple", DataType.TUPLE));
    PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(schema));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConvertInvalidSchemaEmpty() throws Descriptors.DescriptorValidationException {
    PigToProtobuf.schemaToProtoDescriptor(new ResourceSchema(new Schema()));
  }

  private static void assetFieldDescriptor(Descriptors.FieldDescriptor fieldDescriptor,
                                           String name, Descriptors.FieldDescriptor.Type type) {
    Assert.assertEquals("Incorrect field name", name, fieldDescriptor.getName());
    Assert.assertEquals("Incorrect field type", type, fieldDescriptor.getType());
  }
}
