package com.twitter.elephantbird.pig.load;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple;
import com.twitter.elephantbird.pig.store.RCFileThriftPigStorage;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.util.Codecs;

/**
 * Test RCFile loader and storage with Thrift objects
 */
public class TestRCFileThriftStorage {

  private PigServer pigServer;
  private final String testDir =
    System.getProperty("test.build.data") + "/TestRCFileThriftStorage";
  private final File inputDir = new File(testDir, "in");
  private final File rcfile_in = new File(testDir, "rcfile_in");

  private ThriftToPig<TestPerson> thriftToPig = ThriftToPig.newInstance(TestPerson.class);
  private ThriftConverter<TestPerson> thriftConverter = ThriftConverter.newInstance(TestPerson.class);

  private final TestPerson[] records = new TestPerson[]{  makePerson(0),
      makePerson(1),
      makePerson(2) };

  private static final Base64 base64 = Codecs.createStandardBase64();

  public static class B64ToTuple extends ThriftBytesToTuple<TestPerson> {
    public B64ToTuple(String className) {
      super(className);
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
      byte[] bytes = ((DataByteArray)input.get(0)).get();
      input.set(0, new DataByteArray(base64.decode(bytes)));
      return super.exec(input);
    }
  }

  @Before
  public void setUp() throws Exception {

    FileUtil.fullyDelete(new File(testDir));

    pigServer = new PigServer(ExecType.LOCAL);

    pigServer.getPigContext().getProperties().setProperty(
        "mapred.output.compress", "true"); //default codec

    inputDir.mkdirs();

    // create an text file with b64 encoded thrift objects.

    FileOutputStream out = new FileOutputStream(new File(inputDir, "persons_b64.txt"));
    for (TestPerson rec : records) {
      out.write(base64.encode(thriftConverter.toBytes(rec)));
      out.write('\n');
    }
    out.close();
  }

  @Test
  public void testRCFileSThrifttorage() throws Exception {

    // write to rcFile using RCFileThriftPigStorage
    for(String line : String.format(
            "DEFINE b64ToTuple %s('%s');\n" +
            "A = load '%s' as (line);\n" +
            "A = foreach A generate b64ToTuple(line) as t;\n" +
            "A = foreach A generate FLATTEN(t);\n" +
            "STORE A into '%s' using %s('%s');\n"

            , B64ToTuple.class.getName()
            , TestPerson.class.getName()
            , inputDir.toURI().toString()
            , rcfile_in.toURI().toString()
            , RCFileThriftPigStorage.class.getName()
            , TestPerson.class.getName()

            ).split("\n")) {

      pigServer.registerQuery(line + "\n");
    }

    // unknown fields are not yet supported for Thrift.

    // load using RCFileThriftPigLoader
    pigServer.registerQuery(String.format(
        "A = load '%s' using %s('%s');\n"
        , rcfile_in.toURI().toString()
        , RCFileThriftPigLoader.class.getName()
        , TestPerson.class.getName()));

    // verify the result:
    Iterator<Tuple> rows = pigServer.openIterator("A");
    for(TestPerson person : records) {
      String expected = personToString(person);
      Assert.assertEquals(expected, rows.next().toString());
    }

    // clean up on successful run
    FileUtil.fullyDelete(new File(testDir));
  }

  // return a Person thrift object
  private TestPerson makePerson(int index) {
    return new TestPerson(
              new TestName("bob " + index, "jenkins"),
              ImmutableMap.of(TestPhoneType.HOME,
                              "408-555-5555" + "ex" + index));
  }

  private String personToString(TestPerson person) {
    return thriftToPig.getPigTuple(person).toString();
  }

}
