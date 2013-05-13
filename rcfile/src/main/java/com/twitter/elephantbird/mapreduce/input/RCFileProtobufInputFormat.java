package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message.Builder;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.util.RCFileUtil;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileProtobufInputFormat extends RCFileBaseInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileProtobufInputFormat.class);

  private TypeRef<Message> typeRef;

  // for MR
  public  RCFileProtobufInputFormat() {}

  public RCFileProtobufInputFormat(TypeRef<Message> typeRef) {
    this.typeRef = typeRef;
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setClassConf(Class<? extends Message> protoClass, Configuration conf) {
    Protobufs.setClassConf(conf, RCFileProtobufInputFormat.class, protoClass);
  }

  @Override
  public RecordReader<LongWritable, Writable>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
          throws IOException, InterruptedException {
    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(HadoopCompat.getConfiguration(taskAttempt),
                                     RCFileProtobufInputFormat.class);
    }
    return new ProtobufReader(createUnwrappedRecordReader(split, taskAttempt));
  }

  public class ProtobufReader extends FilterRecordReader<LongWritable, Writable> {

    protected Message               msgInstance;
    protected Builder               msgBuilder;
    protected boolean               readUnknownsColumn = false;
    protected List<FieldDescriptor> knownRequiredFields = Lists.newArrayList();
    protected ArrayList<Integer>    columnsBeingRead = Lists.newArrayList();

    protected ProtobufWritable<Message> protoWritable;

    /**
     * The reader is expected to be a
     * <code>RecordReader< LongWritable, BytesRefArrayWritable ></code>.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ProtobufReader(RecordReader reader) {
      super(reader);
    }

    /** is valid only after initialize() is called */
    public boolean isReadingUnknonwsColumn() {
      return readUnknownsColumn;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx)
                           throws IOException, InterruptedException {
      /* set up columns that needs to be read from the RCFile.
       * if any of the required fields is not among stored columns,
       * read the the "unknowns" column (the last column).
      */
      msgBuilder = Protobufs.getMessageBuilder(typeRef.getRawClass());
      msgInstance = msgBuilder.getDefaultInstanceForType();

      protoWritable = ProtobufWritable.newInstance(typeRef.getRawClass());

      Descriptor msgDesc = msgBuilder.getDescriptorForType();
      final List<FieldDescriptor> msgFields = msgDesc.getFields();

      // set up conf to read all the columns
      Configuration conf = new Configuration(HadoopCompat.getConfiguration(ctx));
      ColumnProjectionUtils.setFullyReadColumns(conf);

      FileSplit fsplit = (FileSplit)split;
      Path file = fsplit.getPath();

      LOG.info(String.format("reading %s from %s:%d:%d"
          , typeRef.getRawClass().getName()
          , file.toString()
          , fsplit.getStart()
          , fsplit.getStart() + fsplit.getLength()));

      ColumnarMetadata storedInfo = RCFileUtil.readMetadata(conf, file);

      // list of field numbers
      List<Integer> msgFieldIds = Lists.transform(msgFields,
                                    new Function<FieldDescriptor, Integer>() {
                                       public Integer apply(FieldDescriptor fd) {
                                         return fd.getNumber();
                                       }
                                    });

      columnsBeingRead = RCFileUtil.findColumnsToRead(conf, msgFieldIds, storedInfo);

      for(int idx : columnsBeingRead) {
        int fid = storedInfo.getFieldId(idx);
        if (fid >= 0) {
          knownRequiredFields.add(msgFields.get(msgFieldIds.indexOf(fid)));
        } else {
          readUnknownsColumn = true;
        }
      }

      ColumnProjectionUtils.setReadColumnIDs(conf, columnsBeingRead);

      // finally!
      super.initialize(split, ctx);
    }

    @Override
    public Writable getCurrentValue() throws IOException, InterruptedException {
      protoWritable.set(getCurrentProtobufValue());
      return protoWritable;
    }

    /** returns <code>super.getCurrentValue()</code> */
    public BytesRefArrayWritable getCurrentBytesRefArrayWritable() throws IOException, InterruptedException {
      return (BytesRefArrayWritable) super.getCurrentValue();
    }

    /**
     * Builds protobuf message from the raw bytes returned by RCFile reader.
     */
    public Message getCurrentProtobufValue() throws IOException, InterruptedException {
      BytesRefArrayWritable byteRefs = getCurrentBytesRefArrayWritable();
      if (byteRefs == null) {
        return null;
      }

      Builder builder = msgInstance.newBuilderForType();

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        if (buf.getLength() > 0) {
          Protobufs.setFieldValue(
              CodedInputStream.newInstance(buf.getData(), buf.getStart(), buf.getLength()),
              knownRequiredFields.get(i),
              builder);
        }
      }

      // parse unknowns column if required
      if (readUnknownsColumn) {
        int last = columnsBeingRead.get(columnsBeingRead.size() - 1);
        BytesRefWritable buf = byteRefs.get(last);
        if (buf.getLength() > 0) {
          builder.mergeFrom(buf.getData(), buf.getStart(), buf.getLength());
        }
      }

      return builder.build();
    }
  }

 }
