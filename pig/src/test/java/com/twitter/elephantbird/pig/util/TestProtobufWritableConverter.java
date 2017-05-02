package com.twitter.elephantbird.pig.util;

import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * @author Andy Schlaikjer
 */
public class TestProtobufWritableConverter extends AbstractTestProtobufWritableConverter<Person> {
  private static final TypeRef<Person> TYPE_REF = Protobufs.getTypeRef(Person.class.getName());
  private static final Person V1 = Person.newBuilder().setId(1).setName("Jon Smith").build();
  private static final Person V3 = Person.newBuilder().setId(3).setName("Mary Jane").build();
  private static final Person V2 = Person.newBuilder().setId(2).setName("John Doe").build();
  private static final ProtobufWritable<?>[] DATA = { new ProtobufWritable<Person>(V1, TYPE_REF),
          new ProtobufWritable<Person>(V2, TYPE_REF), new ProtobufWritable<Person>(V3, TYPE_REF) };
  private static final String[] EXPECTED = { "(Jon Smith,1,,{})", "(John Doe,2,,{})",
          "(Mary Jane,3,,{})" };

  @SuppressWarnings("unchecked")
  public TestProtobufWritableConverter() {
    super(Person.class, (ProtobufWritable<Person>[]) DATA, EXPECTED, "()");
  }
}
