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

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * OutputFormat or storing protobufs in RCFile.<p>
 *
 * Each of the top level fields is stored in a separate column.
 * An extra column at the end is added for "unknown fields" in the protobuf.
 * The protobuf field numbers stored in metadata in RCFiles.
 */
public class RCFileProtobufOutputFormat extends RCFileOutputFormat {

  /* typeRef is only required for setting metadata for the RCFile
   * if we delay file creation until the first row is written,
   * this info could be derived from protobuf being written.
   */
  private TypeRef<? extends Message> typeRef;
  private List<FieldDescriptor> msgFields;
  private int numColumns;

  private BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
  private BytesRefWritable[] colValRefs;
  private ByteStream.Output byteStream = new ByteStream.Output();
  private CodedOutputStream protoStream = CodedOutputStream.newInstance(byteStream);

  /** internal, for MR use only. */
  public RCFileProtobufOutputFormat() {
  }

  public RCFileProtobufOutputFormat(TypeRef<? extends Message> typeRef) { // for PigLoader etc.
    this.typeRef = typeRef;
    init();
  }

  private void init() {
    Builder msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
    msgFields = msgBuilder.getDescriptorForType().getFields();
    numColumns = msgFields.size() + 1; // known fields + 1 for unknown fields
    colValRefs = new BytesRefWritable[numColumns];

    for (int i = 0; i < numColumns; i++) {
      colValRefs[i] = new BytesRefWritable();
      rowWritable.set(i, colValRefs[i]);
    }
  }

  protected ColumnarMetadata makeColumnarMetadata() {
    ColumnarMetadata.Builder metadata = ColumnarMetadata.newBuilder();

    metadata.setClassname(typeRef.getRawClass().getName());
    for(FieldDescriptor fd : msgFields) {
      metadata.addFieldId(fd.getNumber());
    }
    metadata.addFieldId(-1); // -1 for unknown fields

    return metadata.build();
  }

  private class ProtobufWriter extends RCFileOutputFormat.Writer {

    ProtobufWriter(TaskAttemptContext job) throws IOException {
      super(RCFileProtobufOutputFormat.this, job, makeColumnarMetadata());
    }

    @Override
    public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
      @SuppressWarnings("unchecked")
      Message msg = ((ProtobufWritable<Message>)value).get();

      protoStream.flush();
      byteStream.reset(); // reinitialize the byteStream if buffer is too large?
      int startPos = 0;

      // top level fields are split across the columns.
      for (int i=0; i < numColumns; i++) {

        if (i < (numColumns - 1)) {

          FieldDescriptor fd = msgFields.get(i);
          if (msg.hasField(fd)) {
            Protobufs.writeFieldNoTag(protoStream, fd, msg.getField(fd));
          }

        } else { // last column : write unknown fields
          msg.getUnknownFields().writeTo(protoStream); // could be empty
        }

        protoStream.flush();
        colValRefs[i].set(byteStream.getData(),
                          startPos,
                          byteStream.getCount() - startPos);
        startPos = byteStream.getCount();
      }

      super.write(null, rowWritable);
    }
  }

  /**
   * In addition to setting OutputFormat class to {@link RCFileProtobufOutputFormat},
   * sets an internal configuration in jobConf so that remote tasks
   * instantiate appropriate object for the protobuf class.
   */
  public static <M extends Message> void setOutputFormatClass(Class<M> protoClass, Job job) {

    Protobufs.setClassConf(job.getConfiguration(), RCFileProtobufOutputFormat.class, protoClass);
    job.setOutputFormatClass(RCFileProtobufOutputFormat.class);
  }

  @Override
  public RecordWriter<NullWritable, Writable>
    getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(job.getConfiguration(), RCFileProtobufOutputFormat.class);
      init();
    }

    RCFileOutputFormat.setColumnNumber(job.getConfiguration(), numColumns);
    return new ProtobufWriter(job);
  }

}

