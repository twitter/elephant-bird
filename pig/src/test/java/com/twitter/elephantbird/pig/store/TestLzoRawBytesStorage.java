package com.twitter.elephantbird.pig.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.hadoop.compression.lzo.GPLNativeCodeLoader;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.load.LzoRawBytesLoader;
import com.twitter.elephantbird.pig.test.thrift.Name;
import com.twitter.elephantbird.pig.test.thrift.Person;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.UnitTestUtil;

/**
 * Unit tests for {@link LzoRawBytesLoader} and {@link LzoRawBytesStorage}.
 *
 * @author Andy Schlaikjer
 */
public class TestLzoRawBytesStorage {
  private final Person message = new Person(new Name("A", "B"), 1, "a@b.com", null);
  private final ThriftConverter<Person> converter = new ThriftConverter<Person>(
      ThriftUtils.<Person>getTypeRef(Person.class));
  private PigServer pigServer;
  private String tempFilename;

  public DataOutputStream getTempOutputStream() throws IOException {
    File tempPath = new File(System.getProperty("test.build.data"),
        TestLzoRawBytesStorage.class.getName());
    tempPath.mkdirs();
    File tempFile = File.createTempFile("test", ".dat", tempPath);
    tempFilename = tempFile.getAbsolutePath();
    Path path = new Path("file:///" + tempFilename);
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    return fs.create(path);
  }

  public void setUp() throws Exception {
    // create temp data
    PrintWriter writer = new PrintWriter(getTempOutputStream());
    try {
      writer.println("(A,B)\t1\ta@b.com");
    } finally {
      IOUtils.closeStream(writer);
    }

    // create local Pig server
    pigServer = UnitTestUtil.makePigServer();
    pigServer.setBatchOn();
    pigServer.registerQuery(String.format(
        "A = LOAD 'file:%s' AS (name: (first: chararray, last: chararray)" +
            ", id: int, email: chararray);", tempFilename));
    pigServer.registerQuery(String.format(
        "At = FOREACH A GENERATE name, id, email, null AS phones;"));
    pigServer.registerQuery(String.format(
        "STORE At INTO 'file:%s-thrift' USING %s('%s');", tempFilename,
        LzoThriftBlockPigStorage.class.getName(), Person.class.getName()));
    pigServer.executeBatch();
  }

  @Test
  public void testLzoRawBytesLoader() throws Exception {
    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      return;
    }
    setUp();
    pigServer.registerQuery(String.format(
        "B = LOAD 'file:%s-thrift' USING %s() AS (thrift: bytearray);", tempFilename,
        LzoRawBytesLoader.class.getName()));
    validate(pigServer.openIterator("B"));
  }

  @Test
  public void testLzoRawBytesStorage() throws Exception {
    if (!GPLNativeCodeLoader.isNativeCodeLoaded()) {
      return;
    }
    setUp();
    pigServer.registerQuery(String.format(
        "B = LOAD 'file:%s-thrift' USING %s() AS (thrift: bytearray);", tempFilename,
        LzoRawBytesLoader.class.getName()));
    pigServer.registerQuery(String.format(
        "STORE B INTO 'file:%s-bytes' USING %s();", tempFilename,
        LzoRawBytesStorage.class.getName()));
    pigServer.executeBatch();
    pigServer.registerQuery(String.format(
        "B2 = LOAD 'file:%s-bytes' USING %s() AS (thrift: bytearray);", tempFilename,
        LzoRawBytesLoader.class.getName()));
    validate(pigServer.openIterator("B2"));
  }

  public void validate(Iterator<Tuple> itr) throws ExecException {
    assertNotNull(itr);
    assertTrue(itr.hasNext());
    Tuple t = itr.next();
    assertNotNull(t);
    assertEquals(1, t.size());
    DataByteArray data = (DataByteArray) t.get(0);
    assertNotNull(data);
    assertEquals(message, converter.fromBytes(data.get()));
  }
}
