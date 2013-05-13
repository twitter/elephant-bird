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

import com.google.protobuf.Message;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.data.proto.tutorial.AddressBookProtos.PersonWithoutEmail;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneNumber;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person.PhoneType;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.mapreduce.output.RCFileProtobufOutputFormat;
import com.twitter.elephantbird.pig.piggybank.ProtobufBytesToTuple;
import com.twitter.elephantbird.pig.store.RCFileProtobufPigStorage;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.util.Codecs;
import com.twitter.elephantbird.util.Protobufs;

/**
 * Test RCFile loader and storage with Protobufs.
 */
public class TestRCFileProtobufStorage {

  private PigServer pigServer;
  private final String testDir =
          CoreTestUtil.getTestDataDir(TestRCFileProtobufStorage.class);
  private final File inputDir = new File(testDir, "in");
  private final File rcfile_in = new File(testDir, "rcfile_in");

  private final Person[] records = new Person[]{
                                          makePerson(0),
                                          makePerson(1),
                                          makePerson(2),
                                          makePersonWithDefaults(3, true),
                                          makePersonWithDefaults(4, false),
                                          makePersonWithDefaults(4, true) };

  private static final Base64 base64 = Codecs.createStandardBase64();

  public static class B64ToTuple extends ProtobufBytesToTuple<Message> {
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

    inputDir.mkdirs();

    // create an text file with b64 encoded protobufs

    FileOutputStream out = new FileOutputStream(new File(inputDir, "persons_b64.txt"));
    for (Person rec : records) {
      out.write(base64.encode(rec.toByteArray()));
      out.write('\n');
    }
    out.close();
  }

  @Test
  public void testRCFileStorage() throws Exception {
    /* create a directory with three rcfiles :
     *  - one created with normal Person objects using RCFileProtobufPigStorage.
     *  - one created with Person objects where the optional fields are not set.
     *  - other with PersonWithoutEmail (for testing unknown fields)
     *    using the same objects as the first one.
     *
     * Then load both files using RCFileProtobufPigLoader
     */

    // write to rcFile using RCFileProtobufStorage
    for(String line : String.format(
            "DEFINE b64ToTuple %s('%s');\n" +
            "A = load '%s' as (line);\n" +
            "A = foreach A generate b64ToTuple(line) as t;\n" +
            "A = foreach A generate FLATTEN(t);\n" +
            "STORE A into '%s' using %s('%s');\n"

            , B64ToTuple.class.getName()
            , Person.class.getName()
            , inputDir.toURI().toString()
            , rcfile_in.toURI().toString()
            , RCFileProtobufPigStorage.class.getName()
            , Person.class.getName()

            ).split("\n")) {

      pigServer.registerQuery(line + "\n");
    }

    // create an rcfile with Person objects directly with out converting to a
    // tuple so that optional fields that are not set are null in RCFile

    ProtobufWritable<Person> personWritable = ProtobufWritable.newInstance(Person.class);

    RecordWriter<Writable, Writable> protoWriter =
            createProtoWriter(Person.class,
                              new File(rcfile_in, "persons_with_unset_fields.rc"));

    for(Person person : records) {
      personWritable.set(person);
      protoWriter.write(null, personWritable);
    }
    protoWriter.close(null);

    // create an rcFile with PersonWithoutEmail to test unknown fields

    ProtobufWritable<PersonWithoutEmail> pweWritable =
            ProtobufWritable.newInstance(PersonWithoutEmail.class);

    protoWriter = createProtoWriter(PersonWithoutEmail.class,
                                    new File(rcfile_in, "persons_with_unknows.rc"));

    for(Person person : records) {
      pweWritable.set(PersonWithoutEmail.newBuilder()
                        .mergeFrom(person.toByteArray()).build());
      protoWriter.write(null, pweWritable);
    }
    protoWriter.close(null);

    // load all the files
    pigServer.registerQuery(String.format(
        "A = load '%s' using %s('%s');\n"
        , rcfile_in.toURI().toString()
        , RCFileProtobufPigLoader.class.getName()
        , Person.class.getName()));

    // verify the result:
    Iterator<Tuple> rows = pigServer.openIterator("A");
    for (int i=0; i<3; i++) {
      for(Person person : records) {
        String expected = personToString(person);
        Assert.assertEquals(expected, rows.next().toString());
      }
    }

    // clean up on successful run
    FileUtil.fullyDelete(new File(testDir));
  }

  @SuppressWarnings("unchecked")
  private static RecordWriter<Writable, Writable>
  createProtoWriter(Class<?> protoClass, final File file)
                    throws IOException, InterruptedException {

    OutputFormat outputFormat = (
      new RCFileProtobufOutputFormat(Protobufs.getTypeRef(protoClass.getName())) {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context,
            String extension) throws IOException {
          return new Path(file.toURI().toString());
        }
    });

    Configuration conf = new Configuration();
    // TODO: figure out why Gzip or BZip2 compression fails on OSX
    // conf.setBoolean("mapred.output.compress", true);
    // conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");

    return outputFormat.getRecordWriter(
        HadoopCompat.newTaskAttemptContext(conf, new TaskAttemptID()));
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

  // return a Person object. don't set optional fields
  private static Person makePersonWithDefaults(int index, boolean add_phone) {
    Person.Builder builder =
            Person.newBuilder()
            .setName("bob_" + index + " jenkins")
            .setId(index);
    if (add_phone) {
      builder.addPhone(PhoneNumber.newBuilder()
                                  .setNumber("408-555-" + (5555 + index)));
    }
    return builder.build();
  }

  private static String personToString(Person person) {
    return new ProtobufToPig().toTuple(person).toString();
  }
}
