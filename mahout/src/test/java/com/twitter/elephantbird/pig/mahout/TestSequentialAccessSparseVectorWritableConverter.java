package com.twitter.elephantbird.pig.mahout;

import java.io.IOException;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

import com.twitter.elephantbird.pig.util.AbstractTestWritableConverter;

/**
 * Unit tests for {@link VectorWritableConverter} and {@link SequentialAccessSparseVector}.
 *
 * @author Andy Schlaikjer
 */
public class TestSequentialAccessSparseVectorWritableConverter extends
    AbstractTestWritableConverter<VectorWritable, VectorWritableConverter> {
  private static final Vector V1 = new DenseVector(new double[] { 1, 2, 3 });
  private static final Vector V2 = new SequentialAccessSparseVector(V1);
  private static final VectorWritable[] DATA = { new VectorWritable(V2) };
  private static final String[] EXPECTED = { "(3,{(0,1.0),(1,2.0),(2,3.0)})" };
  private static final String SCHEMA =
      "(cardinality: int, entries: {entry: (index: int, value: double)})";

  public TestSequentialAccessSparseVectorWritableConverter() {
    super(VectorWritable.class, VectorWritableConverter.class, "-- -sequential", DATA, EXPECTED,
        SCHEMA);
  }

  @Test
  public void testLoadValidSchema01() throws IOException {
    registerReadQuery("-- -sparse", null);
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void testLoadValidSchema02() throws IOException {
    registerReadQuery("-- -sparse -cardinality 3", null);
    validate(new String[] { "({(0,1.0),(1,2.0),(2,3.0)})" }, pigServer.openIterator("A"));
  }

  @Test
  public void testLoadConversionSchema() throws IOException {
    registerReadQuery("-- -dense -cardinality 3", null);
    validate(new String[] { "(1.0,2.0,3.0)" }, pigServer.openIterator("A"));
  }

  @Test(expected = Exception.class)
  public void testLoadInvalidSchema() throws IOException {
    registerReadQuery(tempFilename, "-- -sparse -cardinality 2", null);
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void testDenseToSparse() throws IOException {
    registerReadQuery("-- -dense -cardinality 3", null);
    registerWriteQuery(tempFilename + "-2", "-- -sparse");
    registerReadQuery(tempFilename + "-2");
    pigServer.registerQuery("A = FOREACH A GENERATE key, FLATTEN(value);");
    pigServer.registerQuery(String.format("A = FOREACH A {\n" +
        "entries_sorted = ORDER entries BY index ASC;\n" +
        "GENERATE key, TOTUPLE(cardinality, entries_sorted) AS value;\n" +
        "}"));
    validate(pigServer.openIterator("A"));
  }
}
