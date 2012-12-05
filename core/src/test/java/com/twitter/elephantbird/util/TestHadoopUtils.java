package com.twitter.elephantbird.util;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Alex Levenson
 */
public class TestHadoopUtils {

  @Test
  public void testReadWriteToConfAsBase64() throws Exception {
    Map<Integer, String> anObject = Maps.newHashMap();
    anObject.put(7, "seven");
    anObject.put(8, "eight");

    Configuration conf = new Configuration();

    HadoopUtils.writeObjectToConfAsBase64("anobject", anObject, conf);
    Map<Integer, String> copy = HadoopUtils.readObjectFromConfAsBase64("anobject", conf);
    assertEquals(anObject, copy);

    try {
      Set<String> bad = HadoopUtils.readObjectFromConfAsBase64("anobject", conf);
      fail("This should throw a ClassCastException");
    } catch (ClassCastException e) {

    }
  }

  @Test
  public void testReadWriteNullToConfAsBase64() throws Exception {
    Object nullObj = null;
    Configuration conf = new Configuration();

    HadoopUtils.writeObjectToConfAsBase64("anobject", null, conf);
    Object copy = HadoopUtils.readObjectFromConfAsBase64("anobject", conf);
    assertEquals(nullObj, copy);

    assertNull(HadoopUtils.readObjectFromConfAsBase64("non-existant-key", conf));
  }
}
