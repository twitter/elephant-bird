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

import com.google.common.collect.ImmutableMap;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.ThriftBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.mapreduce.output.LzoBinaryB64LineRecordWriter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.util.CoreTestUtil;

/**
 * Test {@link MultiFormatLoader} using a Thrift struct.
 */
public class TestThriftMultiFormatLoader {
  // create a directory with two lzo files, one in Base64Line format
  // and the other in Serialized blocks, and load them using
  // MultiFormatLoader

  private PigServer pigServer;
  private final String testDir =
    System.getProperty("test.build.data") + "/TestMultiFormatLoader";
  private final File inputDir = new File(testDir, "in");
  private final TestPerson[] records = new TestPerson[]{  makePerson(0),
                                                          makePerson(1),
                                                          makePerson(2) };
  @Before
  public void setUp() throws Exception {

    Configuration conf = new Configuration();

    Assume.assumeTrue(CoreTestUtil.okToRunLzoTests(conf));

    pigServer = PigTestUtil.makePigServer();

    inputDir.mkdirs();

    // write to block file
    ThriftBlockWriter<TestPerson> blk_writer =
      new ThriftBlockWriter<TestPerson>(createLzoOut("1-block.lzo", conf),
                                        TestPerson.class);
    for (TestPerson rec : records) {
      blk_writer.write(rec);
    }
    blk_writer.close();

    // write tb64 lines
    LzoBinaryB64LineRecordWriter<TestPerson, ThriftWritable<TestPerson>> b64_writer =
      LzoBinaryB64LineRecordWriter.newThriftWriter(TestPerson.class,
                                                   createLzoOut("2-b64.lzo", conf));
    for (TestPerson rec: records) {
      thriftWritable.set(rec);
      b64_writer.write(null, thriftWritable);
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
        MultiFormatLoader.class.getName(),
        TestPerson.class.getName()));

    Iterator<Tuple> rows = pigServer.openIterator("A");
    // verify:
    for (int i=0; i<2; i++) {
      for(TestPerson person : records) {
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

  // thrift class related :
  private ThriftToPig<TestPerson> thriftToPig = ThriftToPig.newInstance(TestPerson.class);
  private ThriftWritable<TestPerson> thriftWritable = ThriftWritable.newInstance(TestPerson.class);

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
