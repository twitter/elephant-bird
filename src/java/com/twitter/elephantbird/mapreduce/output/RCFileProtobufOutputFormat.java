package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;
import java.util.List;

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
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * TODO:
 *   - javadoc
 *   - stats
 */
public class RCFileProtobufOutputFormat extends RCFileOutputFormat {

  /*
   * typeRef is only required for setting metadata for the RCFile
   * if we are ok to delay file creation until the first row is created,
   * this info could be derived from object
   */
  private TypeRef<? extends Message> typeRef;
  private List<FieldDescriptor> msgFields;

  private BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
  private BytesRefWritable[] colValRefs;
  private ByteStream.Output byteStream = new ByteStream.Output();
  private CodedOutputStream protoStream = CodedOutputStream.newInstance(byteStream);

  public RCFileProtobufOutputFormat() { // for MR
  }

  public RCFileProtobufOutputFormat(TypeRef<? extends Message> typeRef) { // for PigLoader etc.
    this.typeRef = typeRef;
    init();
  }

  private void init() {
    Builder msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
    msgFields = msgBuilder.getDescriptorForType().getFields();
    colValRefs = new BytesRefWritable[msgFields.size()];

    for (int i = 0; i < msgFields.size(); i++) {
      colValRefs[i] = new BytesRefWritable();
      rowWritable.set(i, colValRefs[i]);
    }
  }

  private class ProtobufWriter extends RCFileOutputFormat.Writer{

    ProtobufWriter(TaskAttemptContext job) throws IOException {
      super(RCFileProtobufOutputFormat.this, job);
    }

    @Override
    public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
      @SuppressWarnings("unchecked")
      Message msg = ((ProtobufWritable<Message>)value).get();

      protoStream.flush();
      byteStream.reset(); // reinitialize the byteStream if buffer is too large?
      int startPos = 0;

      // top level fields are split across the columns.
      for (int i=0; i < msgFields.size(); i++) {
        FieldDescriptor fd = msgFields.get(i);
        if (msg.hasField(fd)) {
          Protobufs.writeFieldNoTag(protoStream, fd, msg.getField(fd));
          protoStream.flush();
        }

        colValRefs[i].set(byteStream.getData(), startPos,
            byteStream.getCount() - startPos);
        startPos = byteStream.getCount();
      }

      super.write(null, rowWritable);
    }
  }

  /**
   * Returns {@link RCFileProtobufOutputFormat} class.
   * Sets an internal configuration in jobConf so that remote asks
   * instantiate appropriate object for this generic class based on protoClass
   */
  public static <M extends Message> Class<RCFileProtobufOutputFormat>
  getOutputFormatClass(Class<M> protoClass, Configuration jobConf) {

    Protobufs.setClassConf(jobConf, RCFileProtobufOutputFormat.class, protoClass);
    return RCFileProtobufOutputFormat.class;
  }


  @Override
  public RecordWriter<NullWritable, Writable>
    getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(job.getConfiguration(), RCFileProtobufOutputFormat.class);
      init();
    }
    RCFileOutputFormat.setColumnNumber(job.getConfiguration(), msgFields.size());
    return new ProtobufWriter(job);
  }

}

