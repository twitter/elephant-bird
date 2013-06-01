package com.twitter.elephantbird.pig.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.thrift.Fixtures;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import thrift.test.HolyMoley;
import thrift.test.Nesting;
import thrift.test.OneOfEach;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ProjectedThriftTupleFactory;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.thrift.test.TestMap;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.thrift.test.TestUnion;
import com.twitter.elephantbird.util.TypeRef;

public class TestThriftToPig {

  private static final TupleFactory tupleFactory  = TupleFactory.getInstance();

  private static enum TestType {
    THRIFT_TO_PIG,
    BYTES_TO_TUPLE,
    TUPLE_TO_THRIFT,
  }

  static <M extends TBase<?, ?>> Tuple thriftToPig(M obj) throws TException {
    // it is very inefficient to create one ThriftToPig for each Thrift object,
    // but good enough for unit testing.

    TypeRef<M> typeRef = new TypeRef<M>(obj.getClass()){};
    ThriftToPig<M> thriftToPig = ThriftToPig.newInstance(typeRef);

    Tuple t = thriftToPig.getPigTuple(obj);

    // test projected tuple. project a subset of fields based on field name.

    List<Field> tFields = thriftToPig.getTStructDescriptor().getFields();
    List<Integer> idxList = Lists.newArrayList();
    RequiredFieldList reqFieldList = new RequiredFieldList();
    for (int i=0; i < tFields.size(); i++) {
      String name = tFields.get(i).getName();
      if (name.hashCode()%2 == 0) {
        RequiredField rf = new RequiredField();
        rf.setAlias(name);
        rf.setIndex(i);
        reqFieldList.add(rf);

        idxList.add(i);
      }
    }

    try {
      Tuple pt = new ProjectedThriftTupleFactory<M>(typeRef, reqFieldList).newTuple(obj);
      int pidx=0;
      for(int idx : idxList) {
        if (t.get(idx) != pt.get(pidx)) { // if both are not nulls
          assertEquals(t.get(idx).toString(), pt.get(pidx).toString());
        }
        pidx++;
      }
    } catch (ExecException e) { // not expected
      throw new TException(e);
    }

    // return the full tuple
    return t;
  }

  static <M extends TBase<?, ?>> Tuple thriftToLazyTuple(M obj) throws TException {
    return ThriftToPig.newInstance(new TypeRef<M>(obj.getClass()){}).getLazyTuple(obj);
  }

  static <M extends TBase<?, ?>> Tuple bytesToTuple(M obj)
                                throws TException, ExecException, IOException {
    // test ThriftBytesToTuple UDF.
    // first serialize obj and then invoke the UDF.
    TypeRef<M> typeRef = new TypeRef<M>(obj.getClass()){};
    ThriftConverter<M> converter = ThriftConverter.newInstance(typeRef);
    ThriftBytesToTuple<M> tTuple = new ThriftBytesToTuple<M>(obj.getClass().getName());
    Tuple tuple = tupleFactory.newTuple(1);
    tuple.set(0, new DataByteArray(converter.toBytes(obj)));
    return tTuple.exec(tuple);
  }

  static <M extends TBase<?, ?>> Tuple pigToThrift(M obj) throws TException {
    // Test PigToThrift using the tuple returned by thriftToPig.
    // also use LazyTuple
    Tuple t = thriftToLazyTuple(obj);
    PigToThrift<M> p2t = PigToThrift.newInstance(new TypeRef<M>(obj.getClass()){});
    assertEquals(obj, p2t.getThriftObject(t));
    return t;
  }

  static <M extends TBase<?, ?>> Tuple toTuple(TestType type, M obj) throws Exception {
    switch (type) {
    case THRIFT_TO_PIG:
      return thriftToPig(obj);
    case BYTES_TO_TUPLE:
      return bytesToTuple(obj);
    case TUPLE_TO_THRIFT:
      return pigToThrift(obj);
    default:
      return null;
    }
  }

  @Test
  public void test() throws Exception {
    tupleTest(TestType.THRIFT_TO_PIG);
    tupleTest(TestType.BYTES_TO_TUPLE);
    tupleTest(TestType.TUPLE_TO_THRIFT);
  }

  private void tupleTest(TestType type) throws Exception {
    OneOfEach ooe = Fixtures.oneOfEach;
    Nesting n = Fixtures.nesting;

    ThriftConverter<HolyMoley> hmConverter = ThriftConverter.newInstance(HolyMoley.class);
    // use a deserialized hm object so that hm.contains HashSet iteration is a bit more predictable
    HolyMoley hm = hmConverter.fromBytes(hmConverter.toBytes(Fixtures.holyMoley));

    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"-"+ooe.zomg_unicode+"-0-base64-{(1),(2),(3)}-{(1),(2),(3)}-{(1),(2),(3)}",
        toTuple(type, ooe).toDelimitedString("-"));

    assertEquals("(31337,I am a bonk... xor!)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \","+n.my_ooe.zomg_unicode+",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})",
        toTuple(type, n).toDelimitedString("-"));

    assertEquals("{(1,0,34,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," + ooe.zomg_unicode +
        ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)}),(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \"," +
        ooe.zomg_unicode + ",0,base64,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})}-{({}),({(and a one),(and a two)}),({(then a one, two),(three!),(FOUR!!)})}-{zero={}, three={}, two={(1,Wait.),(2,What?)}}",
        (toTuple(type, hm).toDelimitedString("-")));

    // Test null fields. Pick the fields that have defaults of null
    // so that extra round of seralization and deserialization does not affect it.
    OneOfEach mostly_ooe = new OneOfEach(ooe);
    mostly_ooe.setBase64((ByteBuffer)null);
    mostly_ooe.setZomg_unicode(null);
    assertEquals(
        "1-0-35-27000-16777216-6000000000-3.141592653589793-JSON THIS! \"--0--{(1),(2),(3)}-{(1),(2),(3)}-{(1),(2),(3)}",
        toTuple(type, mostly_ooe).toDelimitedString("-"));

    Nesting n2 = new Nesting(n);
    n2.getMy_bonk().setMessage(null);
    n2.setMy_ooe(mostly_ooe);
    assertEquals("(31337,)-(1,0,35,27000,16777216,6000000000,3.141592653589793,JSON THIS! \",,0,,{(1),(2),(3)},{(1),(2),(3)},{(1),(2),(3)})",
        toTuple(type, n2).toDelimitedString("-"));

    // test enum.
    PhoneNumber ph = new PhoneNumber();
    ph.setNumber("415-555-5555");
    ph.setType(PhoneType.HOME);
    assertEquals("415-555-5555,HOME", toTuple(type, ph).toDelimitedString(","));

    Person person = new Person(new Name("bob", "jenkins"), 42, "foo@bar.com", Lists.newArrayList(ph));
    assertEquals("(bob,jenkins),42,foo@bar.com,{(415-555-5555,HOME)}", toTuple(type, person).toDelimitedString(","));

    // test Enum map
    TestPerson testPerson = new TestPerson(
                                  new TestName("bob", "jenkins"),
                                  ImmutableMap.of(
                                      TestPhoneType.HOME,   "408-555-5555",
                                      TestPhoneType.MOBILE, "650-555-5555",
                                      TestPhoneType.WORK,   "415-555-5555"));
    String tupleString = toTuple(type, testPerson).toDelimitedString("-");
    assertTrue( // the order of elements in map could vary because of HashMap
        tupleString.equals("(bob,jenkins)-{MOBILE=650-555-5555, WORK=415-555-5555, HOME=408-555-5555}") ||
        tupleString.equals("(bob,jenkins)-{MOBILE=650-555-5555, HOME=408-555-5555, WORK=415-555-5555}"));

    // Test Union:
    TestUnion unionInt = new TestUnion();
    unionInt.setI32Type(10);
    assertEquals(",10,,,", toTuple(type, unionInt).toDelimitedString(","));

    TestUnion unionStr = new TestUnion();
    unionStr.setI32Type(-1); // is overridden below.
    unionStr.setStringType("abcd");
    assertEquals("abcd,,,,", toTuple(type, unionStr).toDelimitedString(","));
  }

  //test a list of a struct
  @Test
  public void nestedStructInListTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestRecipe","name:chararray,ingredients:bag{t:tuple(name:chararray,color:chararray)}");
  }

  //test a set of a struct
  @Test
  public void nestedStructInSetTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestUniqueRecipe","name:chararray,ingredients:bag{t:tuple(name:chararray,color:chararray)}");
  }

  @Test
  public void stringsInSetTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestNameList","name:chararray,names:bag{t:tuple(names_tuple:chararray)}");
  }

  @Test
  public void stringsInListTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestNameSet","name:chararray,names:bag{t:tuple(names_tuple:chararray)}");
  }

  @Test
  public void listInListTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestListInList","name:chararray,names:bag{t:tuple(names_bag:bag{t:tuple(names_tuple:chararray)})}");
  }

  @Test
  public void setInListTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestSetInList","name:chararray,names:bag{t:tuple(names_bag:bag{t:tuple(names_tuple:chararray)})}");
  }

  @Test
  public void listInSetTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestListInSet","name:chararray,names:bag{t:tuple(names_bag:bag{t:tuple(names_tuple:chararray)})})");
  }

  @Test
  public void setInSetTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestSetInSet","name:chararray,names:bag{t:tuple(names_bag:bag{t:tuple(names_tuple:chararray)})}");
  }

  @Test
  public void testUnionSchema() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestUnion", "stringType:chararray,i32:int,bufferType:bytearray,structType:tuple(first_name:chararray,last_name:chararray),boolType: int");
  }

  @Test
  public void basicMapTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestMap","name:chararray,names:map[chararray]");
  }

  @Test
  public void listInMapTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestListInMap","name:chararray,names:map[{(names_tuple:chararray)}]");
  }

  @Test
  public void setInMapTest() throws FrontendException {
      nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestSetInMap","name:chararray,names:map[{(names_tuple:chararray)}]");
  }

  @Test
  public void structInMapTest() throws FrontendException {
      nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestStructInMap","name:chararray,names:map[(name: (first_name: chararray,last_name: chararray),phones: map[chararray])]");
  }

  @Test
  public void mapInListTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestMapInList","name:chararray,names:bag{t:tuple(names_tuple:map[chararray])}");
  }

  @Test
  public void mapInSetTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestMapInSet","name:chararray,names:bag{t:tuple(names_tuple:map[chararray])}");
  }

  @Test
  public void binaryInListMap() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestBinaryInListMap",
        "count:int,binaryBlobs:bag{t:tuple(binaryBlobs_tuple:map[bytearray])}");
  }

  @Test
  public void exceptionInMapTest() throws FrontendException {
    nestedInListTestHelper("com.twitter.elephantbird.thrift.test.TestExceptionInMap","name:chararray,names:map[(descreption: chararray)]");
  }

  public void nestedInListTestHelper(String s, String expSchema) throws FrontendException {
    TypeRef<? extends TBase<?, ?>> typeRef_ = PigUtil.getThriftTypeRef(s);
    Schema schema=ThriftToPig.toSchema(typeRef_.getRawClass());
    Schema oldSchema = Schema.getPigSchema(new ResourceSchema(schema));
    assertTrue(Schema.equals(schema, oldSchema, false, true));
    Schema expectedSchema=Utils.getSchemaFromString(expSchema);
    assertTrue("expected : " + expSchema + " got " +  schema.toString(),
               Schema.equals(schema, expectedSchema, false, true));
  }

  /**
   * Tests that thrift map field value has no field schema alias.
   * @throws FrontendException
   */
  @Test
  public void testMapValueFieldAlias() throws FrontendException {
    ThriftToPig<TestMap> thriftToPig = new ThriftToPig<TestMap>(TestMap.class);
    Schema schema = thriftToPig.toSchema();
    Assert.assertEquals("{name: chararray,names: map[chararray]}", schema.toString());
    Assert.assertNull(schema.getField(1).schema.getField(0).alias);
    schema = ThriftToPig.toSchema(TestMap.class);
    Assert.assertEquals("{name: chararray,names: map[chararray]}", schema.toString());
    Assert.assertNull(schema.getField(1).schema.getField(0).alias);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSetConversionProperties() throws ExecException {
    PhoneNumber pn = new PhoneNumber();
    pn.setNumber("1234");
    pn.setType(PhoneType.HOME);

    ThriftToPig ttp = ThriftToPig.newInstance(PhoneNumber.class);
    Tuple tuple = ttp.getPigTuple(pn);
    assertEquals(DataType.CHARARRAY, tuple.getType(1));
    assertEquals(PhoneType.HOME.toString(), tuple.get(1));

    Configuration conf = new Configuration();
    conf.setBoolean(ThriftToPig.USE_ENUM_ID_CONF_KEY, true);
    ThriftToPig.setConversionProperties(conf);
    tuple = ttp.getPigTuple(pn);
    assertEquals(DataType.INTEGER, tuple.getType(1));
    assertEquals(PhoneType.HOME.getValue(), tuple.get(1));
  }
}
