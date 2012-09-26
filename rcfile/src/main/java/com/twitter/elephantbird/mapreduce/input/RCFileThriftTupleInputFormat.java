package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.IOException;

/**
 * This is a wrapper over RCFileThriftInputFormat and provides a method
 * to create a Tuple directly from RCFile bytes, skipping building a Thrift
 * object.
 */
public class RCFileThriftTupleInputFormat extends RCFileThriftInputFormat {

  // for MR
  public RCFileThriftTupleInputFormat() {}

  public RCFileThriftTupleInputFormat(TypeRef<TBase<?, ?>> typeRef) {
    super(typeRef);
  }

  @Override
  public RecordReader<LongWritable, Writable>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
                                     throws IOException, InterruptedException {
    return new TupleReader(createUnwrappedRecordReader(split, taskAttempt));
  }

  public class TupleReader extends RCFileThriftInputFormat.ThriftReader {

    private final TupleFactory tf = TupleFactory.getInstance();

    /**
     * The reader is expected to be a
     * <code>RecordReader< LongWritable, BytesRefArrayWritable ></code>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TupleReader(RecordReader reader) {
      super(reader);
    }

    /**
     * Returns a Tuple consisting of required fields with out creating
     * a Thrift message at the top level.
     */
    public Tuple getCurrentTupleValue() throws IOException, InterruptedException, TException {

      BytesRefArrayWritable byteRefs = getCurrentBytesRefArrayWritable();
      if (byteRefs == null) {
        return null;
      }

      Tuple tuple = tf.newTuple(knownRequiredFields.size());

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        if (buf.getLength() > 0) {
          memTransport.reset(buf.getData(), buf.getStart(), buf.getLength());

          TStructDescriptor.Field field = knownRequiredFields.get(i);
          Object value = ThriftUtils.readFieldNoTag(tProto, field);
          tuple.set(i, ThriftToPig.toPigObject(field, value, false));
        }
      }

      if (isReadingUnknonwsColumn()) {
        throw new IOException("getCurrentTupleValue() is not supported when 'readUnknownColumns' is set");
      }

      return tuple;
    }
  }
}
