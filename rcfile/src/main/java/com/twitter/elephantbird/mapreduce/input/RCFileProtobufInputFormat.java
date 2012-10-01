package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.RCFileUtil;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileProtobufInputFormat extends MapReduceInputFormatWrapper<LongWritable, Writable> {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileProtobufInputFormat.class);

  private TypeRef<Message> typeRef;

  /** internal, for MR use only. */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public RCFileProtobufInputFormat() {
    super(new RCFileInputFormat());
  }

  public RCFileProtobufInputFormat(TypeRef<Message> typeRef) {
    this();
    this.typeRef = typeRef;
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setClassConf(Class<? extends Message> protoClass, Configuration conf) {
    Protobufs.setClassConf(conf, RCFileProtobufInputFormat.class, protoClass);
  }

  public class ProtobufReader extends FilterRecordReader<LongWritable, Writable> {

    private final TupleFactory tf = TupleFactory.getInstance();
    private final ProtobufToPig protoToPig = new ProtobufToPig();

    private Builder               msgBuilder;
    private boolean               readUnknownsColumn = false;
    private List<FieldDescriptor> knownRequiredFields = Lists.newArrayList();
    private ArrayList<Integer>    columnsBeingRead = Lists.newArrayList();

    private Message               currentValue;
    private ProtobufWritable<Message> protoWritable;

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
      protoWritable = ProtobufWritable.newInstance(typeRef.getRawClass());
      Descriptor msgDesc = msgBuilder.getDescriptorForType();
      final List<FieldDescriptor> msgFields = msgDesc.getFields();

      // set up conf to read all the columns
      Configuration conf = new Configuration(ctx.getConfiguration());
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

      ColumnProjectionUtils.setReadColumnIDs(ctx.getConfiguration(), columnsBeingRead);

      // finally!
      super.initialize(split, ctx);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      currentValue = null;
      return super.nextKeyValue();
    }

    @Override
    public Writable getCurrentValue() throws IOException, InterruptedException {
      protoWritable.set(getCurrentProtobufValue());
      return protoWritable;
    }

    /**
     * Builds protobuf message from the raw bytes returned by RCFile reader.
     */
    public Message getCurrentProtobufValue() throws IOException, InterruptedException {
      if (currentValue != null) {
        return currentValue;
      }

      BytesRefArrayWritable byteRefs = (BytesRefArrayWritable) super.getCurrentValue();
      if (byteRefs == null) {
        return null;
      }

      Builder builder = msgBuilder.clone();

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

      currentValue = builder.build();
      return currentValue;
    }

    /**
     * Returns a Tuple consisting of required fields with out creating
     * a Protobuf message at the top level.
     */
    public Tuple getCurrentTupleValue() throws IOException, InterruptedException {

      BytesRefArrayWritable byteRefs = (BytesRefArrayWritable) super.getCurrentValue();
      if (byteRefs == null) {
        return null;
      }

      Tuple tuple = tf.newTuple(knownRequiredFields.size());

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        FieldDescriptor fd = knownRequiredFields.get(i);
        Object value = null;
        if (buf.getLength() > 0) {
          value = Protobufs.readFieldNoTag(
              CodedInputStream.newInstance(buf.getData(), buf.getStart(), buf.getLength()),
              knownRequiredFields.get(i),
              msgBuilder);
        } else if (fd.getType() != FieldDescriptor.Type.MESSAGE) {
          value = fd.getDefaultValue();
        }
        tuple.set(i, protoToPig.fieldToPig(fd, value));
      }

      if (readUnknownsColumn) {
        // we can handle this if needed.
        throw new IOException("getCurrentTupleValue() is not supported when 'readUnknownColumns' is set");
      }

      return tuple;
    }
  }

  @Override
  public RecordReader<LongWritable, Writable>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
                                    throws IOException, InterruptedException {
    if (typeRef == null) {
      typeRef = Protobufs.getTypeRef(taskAttempt.getConfiguration(), RCFileProtobufInputFormat.class);
    }
    return new ProtobufReader(super.createRecordReader(split, taskAttempt));
  }
}
