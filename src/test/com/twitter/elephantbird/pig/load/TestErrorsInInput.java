package com.twitter.elephantbird.pig.load;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.hadoop.compression.lzo.GPLNativeCodeLoader;
import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.io.RawBlockWriter;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.Protobufs;

/**
 * Test to ensure that empty records in B64Line and Block formats are
 * correctly skipped. Uses thrift records loaded using MultiFormatLoader.
 */
public class TestEmptyRecords {
  // create a directory with two lzo files, one in Base64Line format
  // and the other in Serialized blocks, and load them using
  // MultiFormatLoader

  private PigServer pigServer;
  private final String testDir =
    System.getProperty("test.build.data") + "/TestEmptyRecords";
  private final File inputDir = new File(testDir, "in");
  private final TestPerson[] records = new TestPerson[]{  makePerson(0),
                                                          makePerson(1),
                                                          makePerson(2) };
  @Before
  public void setUp() throws Exception {

    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      // TODO: Consider using @RunWith / @SuiteClasses
      return;
    }

    pigServer = new PigServer(ExecType.LOCAL);
    // set lzo codec:
    pigServer.getPigContext().getProperties().setProperty(
        "io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
    pigServer.getPigContext().getProperties().setProperty(
        "io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");

    Configuration conf = new Configuration();
    inputDir.mkdirs();

    // block writer
    RawBlockWriter blk_writer = new RawBlockWriter(createLzoOut("1-block.lzo", conf));
    //b64 writer
    OutputStream b64_writer = createLzoOut("2-b64.lzo", conf);

    Base64 base64 = Codecs.createStandardBase64();
    ThriftConverter<TestPerson> tConverter = ThriftConverter.newInstance(TestPerson.class);

    for (TestPerson rec : records) {
      //write a regular record and an empty record
      byte[] bytes = tConverter.toBytes(rec);
      blk_writer.write(bytes);
      blk_writer.write(new byte[0]);
      b64_writer.write(base64.encode(bytes));
      b64_writer.write(Protobufs.NEWLINE_UTF8_BYTE);
      b64_writer.write(Protobufs.NEWLINE_UTF8_BYTE); // empty line.
    }
    blk_writer.close();
    b64_writer.close();
  }

  @Test
  public void testMultiFormatLoaderWithEmptyRecords() throws Exception {
    if (pigServer == null) {
      //setUp didn't run because of missing lzo native libraries
      return;
    }

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
