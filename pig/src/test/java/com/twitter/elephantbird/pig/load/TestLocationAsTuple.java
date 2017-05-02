package com.twitter.elephantbird.pig.load;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.easymock.EasyMock;
import org.junit.Test;

public class TestLocationAsTuple {

  @Test
  public void testSimpleLoad() throws IOException {
    Configuration conf = new Configuration();
    Job job = EasyMock.createMock(Job.class);
    EasyMock.expect(HadoopCompat.getConfiguration(job)).andStubReturn(conf);
    EasyMock.replay(job);
    LoadFunc loader = new LocationAsTuple();
    loader.setUDFContextSignature("foo");
    loader.setLocation("a\tb", job);

    RecordReader reader = EasyMock.createMock(RecordReader.class);
    PigSplit split = EasyMock.createMock(PigSplit.class);
    EasyMock.expect(split.getConf()).andStubReturn(conf);
    loader.prepareToRead(reader, split);
    Tuple next = loader.getNext();
    assertEquals("a", next.get(0));
    assertEquals("b", next.get(1));

  }

  @Test
  public void testTokenizedLoad() throws IOException {
    Configuration conf = new Configuration();
    Job job = EasyMock.createMock(Job.class);
    EasyMock.expect(HadoopCompat.getConfiguration(job)).andStubReturn(conf);
    EasyMock.replay(job);
    LoadFunc loader = new LocationAsTuple(",");
    loader.setUDFContextSignature("foo");
    loader.setLocation("a,b\tc", job);

    RecordReader reader = EasyMock.createMock(RecordReader.class);
    PigSplit split = EasyMock.createMock(PigSplit.class);
    EasyMock.expect(split.getConf()).andStubReturn(conf);
    loader.prepareToRead(reader, split);
    Tuple next = loader.getNext();
    assertEquals("a", next.get(0));
    assertEquals("b\tc", next.get(1));

  }
}
