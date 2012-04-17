package com.twitter.elephantbird.pig.load;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.data.proto.tutorial.AddressBookProtos.JustPersonExtExtEnclosingType;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.data.proto.tutorial.AddressBookProtos.PersonExt;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryB64LineRecordWriter;
import com.twitter.elephantbird.pig.piggybank.Fixtures;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.proto.ProtobufExtensionRegistry;
import com.twitter.elephantbird.util.UnitTestUtil;

/**
 * Test {@link MultiFormatLoader} using a Protobuf.
 */
public class TestProtobufMultiFormatLoader {
  // create a directory with two lzo files, one in Base64Line format
  // and the other in Serialized blocks, and load them using
  // MultiFormatLoader

  private PigServer pigServer;
  private final String testDir =
    System.getProperty("test.build.data") + "/TestProtobufMultiFormatLoader";
  private final File inputDir = new File(testDir, "in");
  private final Person[] records = new Person[]{  makePerson(0, false),
                                                  makePerson(1, false),
                                                  makePerson(2, false) };

  private final Person[] recordsWithExt = new Person[]{  makePerson(0, true),
                                                  makePerson(1, true),
                                                  makePerson(2, true) };
  @Before
  public void setUp() throws Exception {

    Configuration conf = new Configuration();

    if (!LzoCodec.isNativeLzoLoaded(conf)) {
      // TODO: Consider using @RunWith / @SuiteClasses
      return;
    }

    pigServer = UnitTestUtil.makePigServer();

    inputDir.mkdirs();

    // write to block file
    ProtobufBlockWriter<Person> blk_writer =
      new ProtobufBlockWriter<Person>(createLzoOut("1-block-without-ext.lzo", conf), Person.class);
    for (Person rec : records) {
      blk_writer.write(rec);
    }
    blk_writer.close();

    ProtobufBlockWriter<Person> blk_writer_with_ext =
      new ProtobufBlockWriter<Person>(createLzoOut("1-block-with-ext.lzo", conf),
          Person.class, Fixtures.buildExtensionRegistry());
    for (Person rec : recordsWithExt) {
      blk_writer_with_ext.write(rec);
    }
    blk_writer_with_ext.close();

    // write tb64 lines
    ProtobufWritable<Person> protoWritable = ProtobufWritable.newInstance(Person.class);
    LzoBinaryB64LineRecordWriter<Person, ProtobufWritable<Person>> b64_writer =
      LzoBinaryB64LineRecordWriter.newProtobufWriter(Person.class, createLzoOut("2-b64-without-ext.lzo", conf));
    for (Person rec: records) {
      protoWritable.set(rec);
      b64_writer.write(null, protoWritable);
    }
    b64_writer.close(null);

    ProtobufWritable<Person> protoWritableWithExt = ProtobufWritable.newInstance(
        Person.class, Fixtures.buildExtensionRegistry());
    LzoBinaryB64LineRecordWriter<Person, ProtobufWritable<Person>> b64_writer_with_ext =
      LzoBinaryB64LineRecordWriter.newProtobufWriter(Person.class, createLzoOut("2-b64-with-ext.lzo", conf));
    for (Person rec: recordsWithExt) {
      protoWritableWithExt.set(rec);
      b64_writer_with_ext.write(null, protoWritableWithExt);
    }
    b64_writer_with_ext.close(null);

  }

  @Test
  public void testMultiFormatLoader() throws Exception {
    if (pigServer == null) {
      //setUp didn't run because of missing lzo native libraries
      return;
    }

    pigServer.registerQuery(String.format(
        "A = load '%s/*-without-ext.lzo' using %s('%s');\n",
        inputDir.toURI().toString(),
        ProtobufPigLoader.class.getName(),
        Person.class.getName()));

    Iterator<Tuple> rows = pigServer.openIterator("A");
    // verify:
    for (int i=0; i<2; i++) {
      for(Person person : records) {
        String expected = personToString(person, null);
        Assert.assertEquals(expected, rows.next().toString());
      }
    }

    ProtobufExtensionRegistry extensionRegistry = Fixtures.buildExtensionRegistry();
    pigServer.registerQuery(String.format(
        "A = load '%s/*-with-ext.lzo' using %s('%s', '%s');\n",
        inputDir.toURI().toString(),
        ProtobufPigLoader.class.getName(),
        Person.class.getName(),
        extensionRegistry.getClass().getName()));

    rows = pigServer.openIterator("A");
    // verify:
    for (int i=0; i<2; i++) {
      for(Person person : recordsWithExt) {
        String expected = personToString(person, extensionRegistry);
        Assert.assertEquals(expected, rows.next().toString());
      }
    }

    FileUtil.fullyDelete(inputDir);
  }

  private DataOutputStream createLzoOut(String name, Configuration conf) throws IOException {
    File file = new File(inputDir, name);
    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    if (file.exists()) {
      file.delete();
    }
    return new DataOutputStream(codec.createOutputStream(new FileOutputStream(file)));
  }

  // return a Person object
  private static Person makePerson(int index, boolean withExtension) {
    Person.Builder builder= Person.newBuilder()
      .setName("bob_" + index + " jenkins")
      .setId(index)
      .setEmail("bob_" + index + "@example.com")
      .addPhone(
          PhoneNumber.newBuilder()
              .setNumber("408-555-" + (5555 + index))
              .setType(PhoneType.MOBILE));

    if(withExtension) {
      builder.setExtension(PersonExt.extInfo, PersonExt.newBuilder().setAddress(
          "Rd. Foo " + index).build())
          .setExtension(JustPersonExtExtEnclosingType.extExtInfo, "bar " + index);
    }

    return builder.build();
  }

  private String personToString(Person person, ProtobufExtensionRegistry extensionRegistry) {
    return new ProtobufToPig().toTuple(person, extensionRegistry).toString();
  }
}
