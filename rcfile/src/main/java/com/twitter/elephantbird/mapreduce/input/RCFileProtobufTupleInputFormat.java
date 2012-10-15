package com.twitter.elephantbird.mapreduce.input;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.util.Protobufs;
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

import java.io.IOException;

/**
 * This is a wrapper over RCFileProtobufInputFormat and provides a method
 * to create a Tuple directly from RCFile bytes, skipping building a protobuf
 * first.
 */
public class RCFileProtobufTupleInputFormat extends RCFileProtobufInputFormat {

  // for MR
  public RCFileProtobufTupleInputFormat() {}

  public RCFileProtobufTupleInputFormat(TypeRef<Message> typeRef) {
    super(typeRef);
  }

  @Override
  public RecordReader<LongWritable, Writable>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
                                 throws IOException, InterruptedException {
    return new TupleReader(createUnwrappedRecordReader(split, taskAttempt));
  }

  public class TupleReader extends ProtobufReader {

    private final TupleFactory tf = TupleFactory.getInstance();
    private final ProtobufToPig protoToPig = new ProtobufToPig();

    /**
     * The reader is expected to be a
     * <code>RecordReader< LongWritable, BytesRefArrayWritable ></code>.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TupleReader(RecordReader reader) {
      super(reader);
    }

    /**
     * Returns a Tuple consisting of required fields with out creating
     * a Protobuf message at the top level.
     */
    public Tuple getCurrentTupleValue() throws IOException, InterruptedException {

      BytesRefArrayWritable byteRefs = getCurrentBytesRefArrayWritable();

      if (byteRefs == null) {
        return null;
      }

      Tuple tuple = tf.newTuple(knownRequiredFields.size());

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        Descriptors.FieldDescriptor fd = knownRequiredFields.get(i);
        Object value = null;
        if (buf.getLength() > 0) {
          value = Protobufs.readFieldNoTag(
                  CodedInputStream.newInstance(buf.getData(), buf.getStart(), buf.getLength()),
                  knownRequiredFields.get(i),
                  msgBuilder);
        } else { // use the value from default instance
          value = msgInstance.getField(fd);
        }
        tuple.set(i, protoToPig.fieldToPig(fd, value));
      }

      if (isReadingUnknonwsColumn()) {
        // we can handle this if needed.
        throw new IOException("getCurrentTupleValue() is not supported when 'readUnknownColumns' is set");
      }

      return tuple;
    }
  }

}
