package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.mapreduce.input.MapReduceInputFormatWrapper;
import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;

/**
 * RCFile version of PigStorage.
 *
 * Usage: <pre>
 * register 'libs/*hive-common*.jar; -- hive-common for RCFile
 * register 'libs/*hive-exec*.jar;  -- hive-exec for RCFile
 * register 'libs/*protobuf-java*.jar; -- protobuf-java should not be required, but for now it is.
 *
 * a = load 'input' using RCFileStorage() as (a:int, b:chararray, c:long);
 *
 * b = foreach a generate a, TOTUPLE(a, c);
 * store b into 'output' using RCFilePigStorage();
 *
 * </pre>
 */
public class RCFilePigStorage extends PigStorage {

  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private int numColumns = -1;
  private int[] requiredColumns = null;

  public RCFilePigStorage() {
    super();
  }

  private Properties getUDFProperties() {
    return UDFContext.getUDFContext()
              .getUDFProperties(this.getClass(), new String[] { signature });
  }

  @Override
  public InputFormat<LongWritable, BytesRefArrayWritable> getInputFormat() {
    return new MapReduceInputFormatWrapper<LongWritable, BytesRefArrayWritable>
            (new RCFileInputFormat<LongWritable, BytesRefArrayWritable>());
  }

  @Override
  public OutputFormat<NullWritable, Writable> getOutputFormat() {
    return new TupleOutputFormat();
  }

  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);

    // sets columnIds config for RCFile

    String obj = getUDFProperties().getProperty("requiredFieldList");
    if (obj == null) {
      // front end or there is no projection set
      ColumnProjectionUtils.setFullyReadColumns(job.getConfiguration());
      return ;
    }

    RequiredFieldList fieldList = (RequiredFieldList)
                                  ObjectSerializer.deserialize(obj);

    ArrayList<Integer> ids = new ArrayList<Integer>();
    requiredColumns = new int[fieldList.getFields().size()];
    int i = 0;
    for (RequiredField rf : fieldList.getFields()) {
      requiredColumns[i++] = rf.getIndex();
      ids.add(rf.getIndex());
    }

    ColumnProjectionUtils.setReadColumnIDs(job.getConfiguration(), ids);
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    super.checkSchema(s);
    getUDFProperties().setProperty("numColumns",
                                   Integer.toString(s.getFields().length));
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    super.setStoreLocation(location, job);
    // set number of columns if this is set in context.
    Properties p = getUDFProperties();
    if (p != null) {
      numColumns = Integer.parseInt(p.getProperty("numColumns", "-1"));
    }

    if (numColumns > 0) {
      RCFileOutputFormat.setColumnNumber(job.getConfiguration(), numColumns);
    }
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
                                             throws FrontendException {
    // don't need to invoke super.pushProjection();
    try {
      getUDFProperties().setProperty("requiredFieldList",
                                     ObjectSerializer.serialize(requiredFieldList));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new RequiredFieldResponse(true);
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!in.nextKeyValue()) {
        return null;
      }

      BytesRefArrayWritable byteRefs = (BytesRefArrayWritable) in.getCurrentValue();

      boolean isProjected = requiredColumns != null;
      int inputSize = byteRefs.size();
      int tupleSize = isProjected ? requiredColumns.length : inputSize;

      Tuple tuple = tupleFactory.newTuple(tupleSize);
      int tupleIdx = 0;

      for (int i=0; i<inputSize; i++) {
        BytesRefWritable ref = byteRefs.get(i);
        if (ref != null && (!isProjected || i == requiredColumns[tupleIdx])) {
          tuple.set(tupleIdx++, new DataByteArray(ref.getBytesCopy()));
        }
      }

      return tuple;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * converts output tuple to set of byte arrays and writes them to the RCFile.
   * It converts the tuple fields to bytes the same way as PigStorage does.
   */
  private class TupleOutputFormat extends RCFileOutputFormat {

    @Override
    public RecordWriter<NullWritable, Writable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {

      if (numColumns < 1) {
        throw new IOException("number of columns is not set");
      }

      final RecordWriter<NullWritable, Writable> writer = super.getRecordWriter(job);

      final ByteStream.Output byteStream = new ByteStream.Output();
      final BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
      final BytesRefWritable[] colValRefs = new BytesRefWritable[numColumns];

      for (int i = 0; i < numColumns; i++) {
        colValRefs[i] = new BytesRefWritable();
        rowWritable.set(i, colValRefs[i]);
      }

      return new RecordWriter<NullWritable, Writable>() {

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
          writer.close(context);
        }

        @Override
        public void write(NullWritable key, Writable value) throws IOException,
                                                        InterruptedException {
          byteStream.reset();

          // write each field as a text (just like PigStorage.
          Tuple tuple = (Tuple)value;
          int sz = tuple.size();
          int startPos = 0;

          for (int i = 0; i < sz && i < numColumns; i++) {

            StorageUtil.putField(byteStream, tuple.get(i));
            colValRefs[i].set(byteStream.getData(),
                              startPos,
                              byteStream.getCount() - startPos);
             startPos = byteStream.getCount();
          }

          writer.write(null, rowWritable);
        }
      };
    }
  }
}
