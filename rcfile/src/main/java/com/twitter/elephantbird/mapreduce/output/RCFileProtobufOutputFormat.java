package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;
import java.util.List;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
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
 * OutputFormat for storing protobufs in RCFile.<p>
 *
 * Each of the top level fields is stored in a separate column.
 * The protobuf field numbers are stored in RCFile metadata.<p>
 *
 * A protobuf message can contain <a href="https://developers.google.com/protocol-buffers/docs/proto#updating">
 * "unknown fields"</a>. These fields are preserved and stored
 * in the last column. e.g. if protobuf A with 4 fields (a, b, c, d) is
 * serialized and when it is deserialized A has only 3 fields (a, c, d),
 * then 'b' is carried over as an unknown field.
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
      super(RCFileProtobufOutputFormat.this, job, Protobufs.toText(makeColumnarMetadata()));
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
          if (fd.isRepeated() || msg.hasField(fd)) {
            // match protobuf's serialization (write only if hasField() is true)
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
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the output format correctly.
   */
  public static void setClassConf(Class<? extends Message> protoClass, Configuration conf) {
    Protobufs.setClassConf(conf, RCFileProtobufOutputFormat.class, protoClass);
  }

  @Override
  public RecordWriter<NullWritable, Writable>
    getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(HadoopCompat.getConfiguration(job), RCFileProtobufOutputFormat.class);
      init();
    }

    RCFileOutputFormat.setColumnNumber(HadoopCompat.getConfiguration(job), numColumns);
    return new ProtobufWriter(job);
  }

}

