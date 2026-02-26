package com.twitter.elephantbird.pig.load;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.CoreTestUtil;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.mapreduce.output.RCFileThriftOutputFormat;
import com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple;
import com.twitter.elephantbird.pig.store.RCFileThriftPigStorage;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPersonExtended;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.ThriftUtils;

/**
 * Test RCFile loader and storage with Thrift objects
 */
public class TestRCFileThriftStorage {

  private PigServer pigServer;
  private final String testDir =
                    CoreTestUtil.getTestDataDir(TestRCFileThriftStorage.class);
  private final File inputDir = new File(testDir, "in");
  private final File rcfile_in = new File(testDir, "rcfile_in");

  private ThriftToPig<TestPersonExtended> thriftToPig = ThriftToPig.newInstance(TestPersonExtended.class);
  private ThriftConverter<TestPersonExtended> thriftConverter = ThriftConverter.newInstance(TestPersonExtended.class);

  private final TestPersonExtended[] records = new TestPersonExtended[]{
                                                    makePerson(0),
                                                    makePerson(1),
                                                    makePerson(2) };

  private static final Base64 base64 = Codecs.createStandardBase64();

  public static class B64ToTuple extends ThriftBytesToTuple<TestPersonExtended> {
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

    pigServer = PigTestUtil.makePigServer();

    pigServer.getPigContext().getProperties().setProperty(
        "mapred.output.compress", "true"); //default codec

    inputDir.mkdirs();

    // create an text file with b64 encoded thrift objects.

    FileOutputStream out = new FileOutputStream(new File(inputDir, "persons_b64.txt"));
    for (TestPersonExtended rec : records) {
      out.write(base64.encode(thriftConverter.toBytes(rec)));
      out.write('\n');
    }
    out.close();
  }

  @Test
  public void testRCFileSThrifttorage() throws Exception {
    /* Create a directory with two files:
     *  - one created with TestPersonExtended objects using RCFileThriftPigStorage
     *  - one created with TestPerson using serialized TestPersonExtended objects
     *         to test handling of unknown fields.
     *
     *  Then load both files using RCFileThriftPigLoader.
     */

    // write to rcFile using RCFileThriftPigStorage
    for(String line : String.format(
            "DEFINE b64ToTuple %s('%s');\n" +
            "A = load '%s' as (line);\n" +
            "A = foreach A generate b64ToTuple(line) as t;\n" +
            "A = foreach A generate FLATTEN(t);\n" +
            "STORE A into '%s' using %s('%s');\n"

            , B64ToTuple.class.getName()
            , TestPersonExtended.class.getName()
            , inputDir.toURI().toString()
            , rcfile_in.toURI().toString()
            , RCFileThriftPigStorage.class.getName()
            , TestPersonExtended.class.getName()

            ).split("\n")) {

      pigServer.registerQuery(line + "\n");
    }
    // the RCFile created above has 5 columns : 4 fields in extended Person
    // and one for unknown fields (this column is empty in this case).

    // store another file with unknowns, by writing the person object with
    // TestPerson rather than with TestPersionExtended, but using
    // serialized TestPersionExtended objects.

    RecordWriter<Writable, Writable> thriftWriter =
      createThriftWriter(TestPerson.class, new File(rcfile_in, "persons_with_unknows.rc"));
    for(TestPersonExtended person : records) {
      // write the bytes from TestPersonExtened
      thriftWriter.write(null, new BytesWritable(thriftConverter.toBytes(person)));
    }
    thriftWriter.close(null);
    // this RCFile has 3 columns : 2 fields in TestPerson and one for unknown
    // fields. The unknowns-columns contains 2 fields from TestPersonExtended
    // that are not understood by TestPerson.

    // load using RCFileThriftPigLoader
    pigServer.registerQuery(String.format(
        "A = load '%s' using %s('%s');\n"
        , rcfile_in.toURI().toString()
        , RCFileThriftPigLoader.class.getName()
        , TestPersonExtended.class.getName()));

    // verify the result:
    Iterator<Tuple> rows = pigServer.openIterator("A");
    for (int i=0; i<2; i++) {
      for(TestPersonExtended person : records) {
        String expected = personToString(person);
        Assert.assertEquals(expected, rows.next().toString());
      }
    }

    // clean up on successful run
    FileUtil.fullyDelete(new File(testDir));
  }

  // return a Person thrift object
  private TestPersonExtended makePerson(int index) {
    return new TestPersonExtended(
              new TestName("bob " + index, "jenkins"),
              ImmutableMap.of(TestPhoneType.HOME,
                              "408-555-5555" + "ex" + index),
              "bob_" + index + "@examle.com",
              new TestName("alice " + index, "smith"));
  }

  @SuppressWarnings("unchecked")
  private static RecordWriter<Writable, Writable>
  createThriftWriter(Class<?> thriftClass, final File file)
                    throws IOException, InterruptedException {

    OutputFormat outputFormat = (
      new RCFileThriftOutputFormat(ThriftUtils.getTypeRef(thriftClass.getName())) {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context,
            String extension) throws IOException {
          return new Path(file.toURI().toString());
        }
    });

    Configuration conf = new Configuration();
    // TODO: figure out why Gzip or BZip2 compression fails on OSX
    //conf.setBoolean("mapred.output.compress", true);
    //conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");


    return outputFormat.getRecordWriter(
        HadoopCompat.newTaskAttemptContext(conf, new TaskAttemptID()));
  }

  private String personToString(TestPersonExtended person) {
    return thriftToPig.getPigTuple(person).toString();
  }

}
