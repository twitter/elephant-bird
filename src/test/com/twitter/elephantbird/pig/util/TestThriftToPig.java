package com.twitter.elephantbird.pig.util;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.elephantbird.thrift.test.TestMap;

/**
 * Unit tests for {@link ThriftToPig}.
 */
public class TestThriftToPig {
  /**
   * Tests that thrift map field value has no field schema alias.
   * @throws FrontendException
   */
  @Test
  public void testMapValueFieldAlias() throws FrontendException {
    ThriftToPig<TestMap> thriftToPig = new ThriftToPig<TestMap>(TestMap.class);
    Schema schema = thriftToPig.toSchema();
    Assert.assertEquals("{name: chararray,names: map[chararray]}", schema.toString());
    Assert.assertNull(schema.getField(1).schema.getField(0).alias);
    schema = ThriftToPig.toSchema(TestMap.class);
    Assert.assertEquals("{name: chararray,names: map[chararray]}", schema.toString());
    Assert.assertNull(schema.getField(1).schema.getField(0).alias);
  }
}
