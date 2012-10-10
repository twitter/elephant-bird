package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;

import com.twitter.elephantbird.mapreduce.input.MultiInputFormat;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Loads raw bytes.
 */
public class LzoRawBytesLoader extends LzoBaseLoadFunc {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  private TypeRef<byte[]> typeRef = new TypeRef<byte[]>(byte[].class){};

  @Override
  public InputFormat<LongWritable, BinaryWritable<byte[]>> getInputFormat() throws IOException {
    return new MultiInputFormat<byte[]>(typeRef);
  }

  @Override
  public Tuple getNext() throws IOException {
    byte[] bytes = getNextBinaryValue(typeRef);
    return bytes != null ?
        tupleFactory.newTuple(new DataByteArray(bytes)) : null;
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    return new ResourceSchema(Utils.getSchemaFromString("bytes : bytearray"));
  }
}
