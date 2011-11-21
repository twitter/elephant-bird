package com.twitter.elephantbird.pig.store;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message.Builder;
import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;
import com.twitter.elephantbird.pig.util.PigToProtobuf;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * TODO
 */
public class RCFileProtobufPigStorage extends StoreFunc {
  private static final Logger LOG = LoggerFactory.getLogger(RCFileProtobufPigStorage.class);

  private TypeRef<? extends Message> typeRef;
  private Builder msgBuilder;
  private List<FieldDescriptor> msgFields;
  private List<Builder> columnBuilders; // one builder for each of the non-primitive fields.
  private List<FieldDescriptor> dynamicFields;

  RecordWriter<NullWritable, Writable> writer = null;

  private BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
  private BytesRefWritable[] colValRefs;
  private ByteStream.Output byteStream = new ByteStream.Output();
  private CodedOutputStream protoStream = CodedOutputStream.newInstance(byteStream);

  public RCFileProtobufPigStorage(String protoClassName) {
    typeRef = Protobufs.getTypeRef(protoClassName);
    msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
    msgFields = msgBuilder.getDescriptorForType().getFields();
    columnBuilders = Lists.newArrayListWithCapacity(msgFields.size());
    dynamicFields = Lists.newArrayListWithCapacity(msgFields.size());
    colValRefs = new BytesRefWritable[msgFields.size()];

    for (FieldDescriptor fd : msgFields) {

      if (fd.getType() == Type.MESSAGE && !fd.isRepeated()) {
        columnBuilders.add(msgBuilder.newBuilderForField(fd));
        dynamicFields.add(null);

      } else { // create a dyanamic message: TODO write primitive types directly.
        Descriptor desc;
        try {
          desc = Protobufs.makeMessageDescriptor(DescriptorProto.newBuilder()
                                                  .setName("proto_idx" + fd.getIndex())
                                                  .addField(fd.toProto())
                                                  .build());
          Builder dynBuilder = DynamicMessage.newBuilder(desc);
          columnBuilders.add(dynBuilder);
          dynamicFields.add(dynBuilder.getDescriptorForType().getFields().get(0));
        } catch (DescriptorValidationException e) {
          throw new RuntimeException(e); // not expected
        }
      }
    }

    for (int i = 0; i < msgFields.size(); i++) {
      colValRefs[i] = new BytesRefWritable();
      rowWritable.set(i, colValRefs[i]);
    }
  }

  @Override @SuppressWarnings("unchecked")
  public OutputFormat getOutputFormat() throws IOException {
    return new RCFileOutputFormat();
  }


  @Override @SuppressWarnings("unchecked")
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    Message msg = PigToProtobuf.tupleToMessage(msgBuilder.clone(), t);

    protoStream.flush();
    byteStream.reset(); // TODO : resize the array it if is too large.
    int startPos = 0;

    for (int i=0; i < msgFields.size(); i++) {
      FieldDescriptor fd = msgFields.get(i);
      if (msg.hasField(fd)) {
        Message colMsg;
        Object val = msg.getField(fd);

        if (dynamicFields.get(i) != null) {
          colMsg = columnBuilders.get(i).clone()
                      .setField(dynamicFields.get(i), val).build();
        } else { // a message
          colMsg = (Message) val;
        }
        colMsg.writeTo(protoStream);
        protoStream.flush();
      }

      colValRefs[i].set(byteStream.getData(), startPos,
                        byteStream.getCount() - startPos);
      startPos = byteStream.getCount();
    }

    try {
      writer.write(null, rowWritable);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    // set compression, columns etc.
    FileOutputFormat.setOutputPath(job, new Path(location));
    RCFileOutputFormat.setColumnNumber(job.getConfiguration(), msgFields.size());
  }

  public static class RCFileProtobufOutputFormat<M> extends RCFileOutputFormat {

  }

}
