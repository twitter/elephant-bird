package com.twitter.elephantbird.pig.mahout;

import java.io.IOException;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.elephantbird.pig.util.AbstractTestWritableConverter;

/**
 * Unit tests for {@link VectorWritableConverter} and {@link DenseVector}.
 *
 * @author Andy Schlaikjer
 */
public class TestDenseVectorWritableConverter extends
    AbstractTestWritableConverter<VectorWritable, VectorWritableConverter> {
  private static final Vector V1 = new DenseVector(new double[] { 1, 2, 3 });
  private static final VectorWritable[] DATA = { new VectorWritable(V1) };
  private static final String[] EXPECTED = { "(1.0,2.0,3.0)" };
  private static final String SCHEMA = "(v1: double, v2: double, v3: double)";

  public TestDenseVectorWritableConverter() {
    super(VectorWritable.class, VectorWritableConverter.class, "", DATA, EXPECTED, SCHEMA);
  }

  protected void testLoadSchema(String writableConverterClassArgs, String expectedSchema)
      throws IOException {
    registerReadQuery(tempFilename, writableConverterClassArgs, null);
    Assert.assertEquals(String.format("{key: int,value: %s}", expectedSchema),
        String.valueOf(pigServer.dumpSchema("A")));
  }

  @Test
  public void testLoadSchema01() throws IOException {
    testLoadSchema("", "bytearray");
  }

  @Test
  public void testLoadSchema011() throws IOException {
    // need both -dense and -cardinality n to define useful schema
    testLoadSchema("-- -dense", "bytearray");
  }

  @Test
  public void testLoadSchema02() throws IOException {
    testLoadSchema("-- -dense -cardinality 3", "(double,double,double)");
  }

  @Test
  public void testLoadSchema021() throws IOException {
    testLoadSchema("-- -dense -cardinality 3 -floatPrecision", "(float,float,float)");
  }

  @Test
  public void testLoadSchema03() throws IOException {
    testLoadSchema("-- -sparse", "(cardinality: int,entries: {t: (index: int,value: double)})");
  }

  @Test
  public void testLoadSchema031() throws IOException {
    testLoadSchema("-- -sparse -floatPrecision",
        "(cardinality: int,entries: {t: (index: int,value: float)})");
  }

  @Test
  public void testLoadSchema04() throws IOException {
    testLoadSchema("-- -sparse -cardinality 3", "(entries: {t: (index: int,value: double)})");
  }

  @Test
  public void testLoadSchema041() throws IOException {
    testLoadSchema("-- -sparse -cardinality 3 -floatPrecision",
        "(entries: {t: (index: int,value: float)})");
  }

  @Test
  public void testLoadValidSchema() throws IOException {
    registerReadQuery("-- -dense -cardinality 3", null);
    validate(pigServer.openIterator("A"));
  }

  @Test
  public void testLoadConversionSchema() throws IOException {
    registerReadQuery("-- -sparse", null);
    validate(new String[] { "(3,{(0,1.0),(1,2.0),(2,3.0)})" }, pigServer.openIterator("A"));
  }

  @Test
  public void testSparseToDense() throws IOException {
    registerReadQuery("-- -sparse", null);
    registerWriteQuery(tempFilename + "-2", "-- -dense");
    registerReadQuery(tempFilename + "-2");
    validate(pigServer.openIterator("A"));
  }
}
