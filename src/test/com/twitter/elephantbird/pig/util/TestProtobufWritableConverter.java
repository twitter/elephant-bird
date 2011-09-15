package com.twitter.elephantbird.pig.util;

import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.TestProtobufWritableConverter.PersonWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author Andy Schlaikjer
 */
public class TestProtobufWritableConverter extends
    AbstractTestProtobufWritableConverter<Person, PersonWritable> {
  public static class PersonWritable extends ProtobufWritable<Person> {
    public PersonWritable() {
      super(TYPE_REF);
    }

    public PersonWritable(Person value) {
      super(value, TYPE_REF);
    }
  }

  private static final TypeRef<Person> TYPE_REF = Protobufs.getTypeRef(Person.class.getName());
  private static final Person V1 = Person.newBuilder().setId(1).setName("Jon Smith").build();
  private static final Person V2 = Person.newBuilder().setId(2).setName("John Doe").build();
  private static final Person V3 = Person.newBuilder().setId(3).setName("Mary Jane").build();
  private static final PersonWritable[] DATA = { new PersonWritable(V1), new PersonWritable(V2),
          new PersonWritable(V3) };
  private static final String[] EXPECTED = { "(Jon Smith,1,,{})", "(John Doe,2,,{})",
          "(Mary Jane,3,,{})" };

  public TestProtobufWritableConverter() {
    super(Person.class, PersonWritable.class, DATA, EXPECTED, "()");
  }
}
