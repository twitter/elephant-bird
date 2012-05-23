package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;

/**
 * TODO:
 * RCFile version of PigStorage. Stores each top level field as a text field.
 * Loading is not supported yet.
 *
 * Usage: <pre>
 * register 'libs/*hive-common*.jar; -- hive-common for RCFile
 * register 'libs/*hive-exec*.jar;  -- hive-exec for RCFile
 * register 'libs/*protobuf-java*.jar; -- protobuf-java should not be required, but for now it is.
 *
 * a = load 'x.txt' as (a, b);
 * store a into 'output' using RCFilePigStorage();
 * </pre>
 */
public class RCFilePigStorage extends PigStorage {

  private int numColumns = -1;

  public RCFilePigStorage() {
    super();
  }

  @Override
  public InputFormat<LongWritable, Text> getInputFormat() {
    throw new RuntimeException("LoadFunc is not supported yet. on TODO list.");
  }

  @Override
  public OutputFormat<NullWritable, Writable> getOutputFormat() {
    return new TupleOutputFormat();
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    super.checkSchema(s);
    UDFContext.getUDFContext()
      .getUDFProperties(this.getClass(), new String[]{signature})
      .setProperty("numColumns", Integer.toString(s.getFields().length));
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    super.setStoreLocation(location, job);
    // set number of columns if this is set in context.
    Properties p = UDFContext.getUDFContext()
                  .getUDFProperties(this.getClass(), new String[]{signature});
    if (p != null) {
      numColumns = Integer.parseInt(p.getProperty("numColumns", "-1"));
    }

    if (numColumns > 0) {
      RCFileOutputFormat.setColumnNumber(job.getConfiguration(), numColumns);
    }
  }


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
