package com.twitter.elephantbird.util;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Alex Levenson
 */
public class TestHadoopUtils {

  @Test
  public void testReadWriteToConfig() throws Exception {
    Map<Integer, String> anObject = Maps.newHashMap();
    anObject.put(7, "seven");
    anObject.put(8, "eight");

    Configuration conf = new Configuration();

    HadoopUtils.writeObjectToConfig("anobject", anObject, conf);
    Map<Integer, String> copy = HadoopUtils.readObjectFromConfig("anobject", conf);
    assertEquals(anObject, copy);

    try {
      Set<String> bad = HadoopUtils.readObjectFromConfig("anobject", conf);
      fail("This should throw a ClassCastException");
    } catch (ClassCastException e) {

    }

  }
}
