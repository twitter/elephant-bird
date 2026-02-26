package com.twitter.elephantbird.pig.load;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import com.google.common.primitives.Bytes;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.IdentityBinaryConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.util.CoreTestUtil;

/**
 * Tests lzo loader with many splits (about 8K) to trigger corner cases.
 * Creates an indexed lzo file with 1M 1-byte records.
 *
 * The lzo buffer size is set to 512 bytes (default 256KB) so that we end up
 * with lots of lzo blocks.
 *
 * While loading in pig, set max split size to 1K. This verifies a bug fix
 * where the binary block reader was reading some records twice (pull #429).
 */
public class TestBinaryLoaderWithManySplits {

  private PigServer pigServer;
  private final String testDir =
      System.getProperty("test.build.data") + "/TestBinvaryLoaderWithManySplits";
  private final File inputDir = new File(testDir, "in");

  private final int NUM_RECORDS = 1000 * 1000;
  private final byte[] expectedRecords = new byte[NUM_RECORDS];

  @Before
  public void setUp() throws Exception {

    Configuration conf = new Configuration();
    Assume.assumeTrue(CoreTestUtil.okToRunLzoTests(conf));

    pigServer = PigTestUtil.makePigServer();

    inputDir.mkdirs();

    // write to block file.
    // use just one record for each protobuf block so that we have lots of records at
    // lzo level
    BinaryBlockWriter<byte[]> blk_writer = new BinaryBlockWriter<byte[]>(
        createLzoOut("many-blocks.lzo", conf), byte[].class, new IdentityBinaryConverter(), 1) {};

    Random rand = new Random(20150107L);
    for (int i=0; i<NUM_RECORDS; i++) {
      expectedRecords[i] = (byte) rand.nextInt();
    }

    for (byte b : expectedRecords) {
      blk_writer.write(new byte[]{ b });
    }
    blk_writer.close();
  }

  @Test
  public void testLoaderWithMultiplePartitions() throws Exception {
    //setUp might not have run because of missing lzo native libraries
    Assume.assumeTrue(pigServer != null);

    pigServer.getPigContext().getProperties().setProperty(
        "mapred.max.split.size", "1024");

    pigServer.registerQuery(String.format(
        "A = load '%s' using %s as (bytes);\n",
        inputDir.toURI().toString(),
        LzoRawBytesLoader.class.getName()));

    Iterator<Tuple> rows = pigServer.openIterator("A");

    // verify:
    // read all the records and sort them since the splits are not processed in order
    ArrayList<Byte> actual = new ArrayList(expectedRecords.length);
    while (rows.hasNext()) {
      actual.add(((DataByteArray)rows.next().get(0)).get()[0]);
    }
    byte[] actualRecords = Bytes.toArray(actual);

    Assert.assertEquals(expectedRecords.length, actual.size());

    Arrays.sort(expectedRecords);
    Arrays.sort(actualRecords);

    Assert.assertArrayEquals(expectedRecords, actualRecords);

    FileUtil.fullyDelete(inputDir);
  }

  private DataOutputStream createLzoOut(String name, Configuration conf) throws IOException {
    File file = new File(inputDir, name);
    File indexFile = new File(inputDir, name + ".index");

    LzopCodec codec = new LzopCodec();
    // set very small lzo blocks size so that we have lots of them.
    conf.setInt("io.compression.codec.lzo.buffersize", 512);
    codec.setConf(conf);

    if (file.exists()) {
      file.delete();
    }

    return new DataOutputStream(codec.createIndexedOutputStream(new FileOutputStream(file),
        new DataOutputStream(new FileOutputStream(indexFile))));
  }
}
