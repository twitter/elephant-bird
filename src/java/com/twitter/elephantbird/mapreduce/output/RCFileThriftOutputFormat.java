package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * OutputFormat for storing Thrift objects in RCFile.<p>
 *
 * Each of the top level fields is stored in a separate column.
 * Thrift field ids are stored in RCFile metadata.
 * TODO: handle "unknown fields"
 */
public class RCFileThriftOutputFormat extends RCFileOutputFormat {

  // typeRef is only required for setting metadata for the RCFile
  private TypeRef<? extends TBase<?, ?>> typeRef;
  private TStructDescriptor tDesc;
  private List<Field> tFields;
  private int numColumns;

  private BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
  private BytesRefWritable[] colValRefs;
  private ByteStream.Output byteStream = new ByteStream.Output();
  private TBinaryProtocol tProto = new TBinaryProtocol(
                                      new TIOStreamTransport(byteStream));

  /** internal, for MR use only. */
  public RCFileThriftOutputFormat() {
  }

  public RCFileThriftOutputFormat(TypeRef<? extends TBase<?, ?>> typeRef) { // for PigLoader etc.
    this.typeRef = typeRef;
    init();
  }

  private void init() {
    tDesc = TStructDescriptor.getInstance(typeRef.getRawClass());
    tFields = tDesc.getFields();
    numColumns = tFields.size() + 1; // known fields + 1 for unknown fields
    colValRefs = new BytesRefWritable[numColumns];

    for (int i = 0; i < numColumns; i++) {
      colValRefs[i] = new BytesRefWritable();
      rowWritable.set(i, colValRefs[i]);
    }
  }

  protected ColumnarMetadata makeColumnarMetadata() {
    ColumnarMetadata.Builder metadata = ColumnarMetadata.newBuilder();

    metadata.setClassname(typeRef.getRawClass().getName());
    for(Field fd : tDesc.getFields()) {
      metadata.addFieldId(fd.getFieldId());
    }
    metadata.addFieldId(-1); // -1 for unknown fields

    return metadata.build();
  }

  private class ProtobufWriter extends RCFileOutputFormat.Writer {

    ProtobufWriter(TaskAttemptContext job) throws IOException {
      super(RCFileThriftOutputFormat.this, job, makeColumnarMetadata());
    }

    @Override @SuppressWarnings("unchecked")
    public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
      TBase tObj = ((ThriftWritable<TBase>)value).get();

      byteStream.reset(); // reinitialize the byteStream if buffer is too large?
      int startPos = 0;

      // top level fields are split across the columns.
      for (int i=0; i < numColumns; i++) {

        if (i < (numColumns - 1)) {

          Field fd = tFields.get(i);
          if (tObj.isSet(fd.getFieldIdEnum())) {
            try {
              ThriftUtils.writeFieldNoTag(tProto, fd, tDesc.getFieldValue(i, tObj));
            } catch (TException e) {
              throw new IOException(e);
            }
          }

        } else { // last column : write unknown fields
          // TODO: we need to deserialize thrift buffer ourselves to handle
          // unknown fields.
        }

        colValRefs[i].set(byteStream.getData(),
                          startPos,
                          byteStream.getCount() - startPos);
        startPos = byteStream.getCount();
      }

      super.write(null, rowWritable);
    }
  }

  /**
   * In addition to setting OutputFormat class to {@link RCFileThriftOutputFormat},
   * sets an internal configuration in jobConf so that remote tasks
   * instantiate appropriate object for the correct Thrift class.
   */
  public static <T extends TBase<?, ?>> void setOutputFormatClass(Class<T> thriftClass, Job job) {

    ThriftUtils.setClassConf(job.getConfiguration(), RCFileThriftOutputFormat.class, thriftClass);
    job.setOutputFormatClass(RCFileThriftOutputFormat.class);
  }

  @Override
  public RecordWriter<NullWritable, Writable>
    getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    if (typeRef == null) {
      typeRef = ThriftUtils.getTypeRef(job.getConfiguration(), RCFileProtobufOutputFormat.class);
      init();
    }

    RCFileOutputFormat.setColumnNumber(job.getConfiguration(), numColumns);
    return new ProtobufWriter(job);
  }
}
