package com.twitter.elephantbird.pig.util;

import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;

/**
 * Unit tests for {@link PigToThrift}.
 *
 * @author Andy Schlaikjer
 */
public class TestPigToThrift {
  private static Person personMessage(String name, int id, String email, String phoneNumber,
      String phoneType) {
    return new Person(new Name(name, null), id, email, Lists.newArrayList(new PhoneNumber(
        phoneNumber).setType(PhoneType.valueOf(phoneType))));
  }

  private static Tuple personTuple(String name, int id, String email, String phoneNumber,
      String phoneType) {
    TupleFactory tf = TupleFactory.getInstance();
    return tf.newTupleNoCopy(
        Lists.<Object>newArrayList(
            tf.newTupleNoCopy(Lists.<Object>newArrayList(name, null)),
            id,
            email,
            new NonSpillableDataBag(
                Lists.<Tuple>newArrayList(
                    tf.newTupleNoCopy(
                        Lists.<Object>newArrayList(phoneNumber, phoneType))))));
  }

  @Test
  public void testPerson() {
    Person expected = personMessage("Joe", 1, null, "123-456-7890", "HOME");
    Person actual = PigToThrift.newInstance(Person.class).getThriftObject(
        personTuple("Joe", 1, null, "123-456-7890", "HOME"));
    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test//(expected = RuntimeException.class)
  public void testPersonBadEnumValue() {
    PigToThrift.newInstance(Person.class).getThriftObject(
        personTuple("Joe", 1, null, "123-456-7890", "ASDF"));
  }
}
