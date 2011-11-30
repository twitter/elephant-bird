package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.pig.piggybank.storage.hiverc.HiveRCInputFormat;
import org.apache.pig.piggybank.storage.hiverc.HiveRCRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.mapreduce.output.RCFileOutputFormat;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileProtobufInputFormat extends HiveRCInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileProtobufInputFormat.class);

  /**
   * comma separated list of indices of the fields. This is not a list of field
   * numbers in the protobuf.
   *
   * If this configuration is not set or is empty, all the fields for
   * current protobuf are read ("unknown fields" are not carried over).
   */
  public static String REQUIRED_PROTO_FIELD_INDICES_CONF =
                           "elephantbird.protobuf.rcfile.input.required.fields";

  private TypeRef<Message> typeRef;

  /** internal, for MR use only. */
  public RCFileProtobufInputFormat() {
    super();
  }

  public RCFileProtobufInputFormat(TypeRef<Message> typeRef) {
    super();
    this.typeRef = typeRef;
  }

  /**
   * In addition to setting InputFormat class to {@link RCFileProtobufInputFormat},
   * sets an internal configuration in jobConf so that remote tasks
   * instantiate appropriate object for the protobuf class.
   */
  public static <M extends Message> void
      setInputFormatClass(Class<M> protoClass, Job job) {
    Protobufs.setClassConf(job.getConfiguration(), RCFileProtobufInputFormat.class, protoClass);
    job.setInputFormatClass(RCFileProtobufInputFormat.class);
  }

  public class ProtobufReader extends HiveRCRecordReader {

    private Builder               msgBuilder;
    private List<FieldDescriptor> msgFields;
    private boolean               readUnknownFields = false;
    private ColumnarMetadata      storedColInfo;
    private List<FieldDescriptor> fieldsBeingRead = Lists.newArrayList();
    private ArrayList<Integer>    columnsBeingRead = Lists.newArrayList();

    private Message currentValue;

    ProtobufReader() throws IOException {
      super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx)
                           throws IOException, InterruptedException {
      /* set up columns that needs to read from the RCFile.
       * if one of the required fields is one of the stored columns,
       * read the the "unknowns" column (the last column).
       *
       * TODO: move parts of this to another utility method so that
       * it can be used by Thrift as well.
       */
      msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
      Descriptor msgDesc = msgBuilder.getDescriptorForType();
      msgFields = msgDesc.getFields();

      // set up conf to read all the columns
      Configuration conf = new Configuration(ctx.getConfiguration());
      ColumnProjectionUtils.setFullyReadColumns(conf);

      Path file = ((FileSplit)split).getPath();

      // read metadata from the file

      Metadata metadata = null;
      RCFile.Reader reader = new RCFile.Reader(file.getFileSystem(conf), file, conf);

      //ugly hack to get metadata. RCFile has to provide access to metata,
      try {
        Field f = RCFile.Reader.class.getDeclaredField("metadata");
        f.setAccessible(true);
        metadata = (Metadata)f.get(reader);
      } catch (Throwable t) {
        throw new IOException("Could not access metadata fiedl in RCFile reader", t);
      }

      reader.close();

      Text metadataKey = new Text(RCFileOutputFormat.COLUMN_METADATA_PROTOBUF_KEY);

      if (metadata == null || metadata.get(metadataKey) == null) {
        throw new IOException("could not find ColumnarMetadata in " + file);
      }

      storedColInfo = Protobufs.mergeFromText(ColumnarMetadata.newBuilder(),
                                              metadata.get(metadataKey)
                                             ).build();

      // the actual columns that are read is the intersection
      // of currently required columns and columns written to the file
      // If any required column does not exist in the file, we need to read
      // the "unknown fields" column, which is the last one.

      // first find the required fields
      ArrayList<Integer> requiredFieldIds = Lists.newArrayList();
      String reqFieldStr = conf.get(REQUIRED_PROTO_FIELD_INDICES_CONF, "");

      if (reqFieldStr == null || reqFieldStr.equals("")) {
        for(FieldDescriptor fd : msgFields) {
          requiredFieldIds.add(fd.getNumber());
        }
      } else {
        for (String str : reqFieldStr.split(",")) {
          int idx = Integer.valueOf(str);
          if (idx < 0 || idx >= msgFields.size()) {
            throw new IOException("idx " + idx + " is out of range for fields in "
                + typeRef.getRawClass().getName());
          }
          requiredFieldIds.add(msgFields.get(idx).getNumber());
        }
      }

      List<Integer> storedFieldIds = storedColInfo.getFieldIdList();

      int idx = 0;
      for(int sid : storedFieldIds) {
        if (sid > 0 && requiredFieldIds.contains(sid)) {
          columnsBeingRead.add(idx++);
          fieldsBeingRead.add(msgDesc.findFieldByNumber(sid));
        }
      }

      // "unknown fields" column is required if any of the required fields
      // is not in stored fields.
      for(int rid : requiredFieldIds) {
        if (!storedFieldIds.contains(rid)) {
          int last = storedFieldIds.size()-1;
          if (storedFieldIds.get(last) < 0) { // normally true
            LOG.info("will read unknown fields as " + rid + " is not one of the stored columns");
            readUnknownFields = true;
            columnsBeingRead.add(last);
          }
        }
      }

      LOG.info(String.format(
          "reading %d out of %d stored columns (%s including unknowns column) "
          + "from %s. Number of required columns is %d.",
          columnsBeingRead.size(),
          storedColInfo.getFieldIdCount(),
          (readUnknownFields ? "" : "not"),
          file,
          requiredFieldIds.size()));

      ColumnProjectionUtils.setReadColumnIDs(ctx.getConfiguration(), columnsBeingRead);

      // finally!
      super.initialize(split, ctx);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentValue = null;
      return super.nextKeyValue();
    }

    /**
     * Builds protobuf message from the raw bytes from RCFile reader.
     */
    public Message getCurrentProtobufValue() throws IOException, InterruptedException {
      /* getCurrentValue() returns a BytesRefArrayWritable since this class
       * extends HiveRCRecordReader. Other option is to extend
       * RecordReader directly and explicitly delegate the methods to
       * HiveRCRecordReader
       */
      if (currentValue != null) {
        return currentValue;
      }

      BytesRefArrayWritable byteRefs = getCurrentValue();
      if (byteRefs == null) {
        return null;
      }

      Builder builder = msgBuilder.clone();
      for (int i=0; i<fieldsBeingRead.size(); i++) {
        BytesRefWritable buf = byteRefs.get(i);
        if (buf.getLength() > 0) {
          Protobufs.setFieldValue(
              CodedInputStream.newInstance(buf.getData(), buf.getStart(), buf.getLength()),
              fieldsBeingRead.get(i),
              builder);
        }
      }

      if (readUnknownFields) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.size()-1);
        builder.setUnknownFields(
            UnknownFieldSet.newBuilder().mergeFrom(
                buf.getData(), buf.getStart(), buf.getLength()
            ).build());
      }

      currentValue = builder.build();
      return currentValue;
    }
  }

  @Override @SuppressWarnings("unchecked")
  public RecordReader createRecordReader(InputSplit split,
                                         TaskAttemptContext taskAttempt)
                                    throws IOException, InterruptedException {
    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(taskAttempt.getConfiguration(), RCFileProtobufInputFormat.class);
    }
    return new ProtobufReader();
  }
}
