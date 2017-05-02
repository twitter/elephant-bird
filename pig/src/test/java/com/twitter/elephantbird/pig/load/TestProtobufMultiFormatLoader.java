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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.elephantbird.mapreduce.io.ProtobufBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryB64LineRecordWriter;
import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.util.CoreTestUtil;

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
  private final Person[] records = new Person[]{  makePerson(0),
                                                  makePerson(1),
                                                  makePerson(2) };
  @Before
  public void setUp() throws Exception {

    Configuration conf = new Configuration();
    Assume.assumeTrue(CoreTestUtil.okToRunLzoTests(conf));

    pigServer = PigTestUtil.makePigServer();

    inputDir.mkdirs();

    // write to block file
    ProtobufBlockWriter<Person> blk_writer =
      new ProtobufBlockWriter<Person>(createLzoOut("1-block.lzo", conf), Person.class);
    for (Person rec : records) {
      blk_writer.write(rec);
    }
    blk_writer.close();

    ProtobufWritable<Person> protoWritable = ProtobufWritable.newInstance(Person.class);

    // write tb64 lines
    LzoBinaryB64LineRecordWriter<Person, ProtobufWritable<Person>> b64_writer =
      LzoBinaryB64LineRecordWriter.newProtobufWriter(Person.class, createLzoOut("2-b64.lzo", conf));
    for (Person rec: records) {
      protoWritable.set(rec);
      b64_writer.write(null, protoWritable);
    }
    b64_writer.close(null);
  }

  @Test
  public void testMultiFormatLoader() throws Exception {
    //setUp might not have run because of missing lzo native libraries
    Assume.assumeTrue(pigServer != null);

    pigServer.registerQuery(String.format(
        "A = load '%s' using %s('%s');\n",
        inputDir.toURI().toString(),
        ProtobufPigLoader.class.getName(),
        Person.class.getName()));

    Iterator<Tuple> rows = pigServer.openIterator("A");
    // verify:
    for (int i=0; i<2; i++) {
      for(Person person : records) {
        String expected = personToString(person);
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
  private static Person makePerson(int index) {
    return Person.newBuilder()
      .setName("bob_" + index + " jenkins")
      .setId(index)
      .setEmail("bob_" + index + "@example.com")
      .addPhone(
          PhoneNumber.newBuilder()
              .setNumber("408-555-" + (5555 + index))
              .setType(PhoneType.MOBILE))
      .build();
  }

  private static String personToString(Person person) {
    return new ProtobufToPig().toTuple(person).toString();
  }

}
