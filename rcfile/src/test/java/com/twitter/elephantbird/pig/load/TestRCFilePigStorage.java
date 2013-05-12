package com.twitter.elephantbird.pig.load;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.pig.store.RCFilePigStorage;
import com.twitter.elephantbird.pig.util.PigTestUtil;
import com.twitter.elephantbird.util.CoreTestUtil;
import org.apache.hadoop.fs.FileUtil;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.StorageUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;

/**
 * Test to make sure PigStorage and RCFilePigStorage return the same tuples.
 */
public class TestRCFilePigStorage {

  private PigServer pigServer;
  private final String testDir =
                       CoreTestUtil.getTestDataDir(TestRCFilePigStorage.class);
  private final File pigDir = new File(testDir, "pig_in");
  private final File rcfileDir = new File(testDir, "rcfile_in");
  private final int numRecords = 5;

  private final String schema = "name : chararray, "
                              + "age: int, "
                              + "phone:(number: chararray, type: chararray),"
                              + "occupation: chararray";

  @Before
  public void setUp() throws Exception {

    FileUtil.fullyDelete(new File(testDir));

    pigServer = PigTestUtil.makePigServer();

    pigDir.mkdirs();

    // write same data using PigStorage() and RCFileStorage() and
    OutputStream out = new FileOutputStream(new File(pigDir, "part-1.txt"));
    for(int i=0; i<numRecords; i++) {
      writePersonTuple(out, i);
    }
    out.close();

    // rewrite the tuples using RCFilePigStorage()

    for(String line : String.format(
                    "A = load '%s' as (%s);\n" +
                    "STORE A into '%s' using %s();\n"
            , pigDir.toURI().toString()
            , schema
            , rcfileDir.toURI().toString()
            , RCFilePigStorage.class.getName()
         ).split("\n")) {
      pigServer.registerQuery(line + "\n");
    }
  }

  @Test
  public void testRCFilePigStorage() throws IOException {
    // make sure both PigStorage & RCFilePigStorage read the same data

    for(String line : String.format(
                      "A = load '%s' as (%s);\n"                      +
                      "B = load '%s' using %s() as (%s);\n"           +
                      "-- projection \n"                              +
                      "C = foreach A generate name, phone.number;\n"  +
                      "D = foreach B generate name, phone.number;\n"
            , pigDir.toURI().toString()
            , schema
            , rcfileDir.toURI().toString()
            , RCFilePigStorage.class.getName()
            , schema
    ).split("\n")) {
      pigServer.registerQuery(line + "\n");
    }

    Iterator<Tuple> rowsA = pigServer.openIterator("A");
    Iterator<Tuple> rowsB = pigServer.openIterator("B");

    // compare.
    for (int i=0; i<numRecords; i++) {
      Assert.assertEquals(rowsA.next().toString(), rowsB.next().toString());
    }

    Iterator<Tuple> rowsC = pigServer.openIterator("C");
    Iterator<Tuple> rowsD = pigServer.openIterator("D");
    for (int i=0; i<numRecords; i++) {
      Assert.assertEquals(rowsC.next().toString(), rowsD.next().toString());
    }

    FileUtil.fullyDelete(new File(testDir));
  }

  // write a person tuple using StorageUtil.putField()
  private static void writePersonTuple(OutputStream out, int index) throws IOException {
    final TupleFactory tf = TupleFactory.getInstance();

    // should use Pig's mock loader when we move to Pig 11

    // see schema above
    StorageUtil.putField(out, "bob " + index + " jenkins");
    StorageUtil.putField(out, "\t");
    StorageUtil.putField(out, 20 + index);
    StorageUtil.putField(out, "\t");
    StorageUtil.putField(out, tf.newTuple(Lists.newArrayList(
                                            "415-555-" + (1234 + index),
                                            "HOME")));
    StorageUtil.putField(out, "\t");
    StorageUtil.putField(out, "engineer " + index);
    out.write('\n');
  }

}
