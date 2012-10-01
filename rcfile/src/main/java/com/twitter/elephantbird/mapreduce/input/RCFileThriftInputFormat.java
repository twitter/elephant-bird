package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.util.RCFileUtil;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileThriftInputFormat extends MapReduceInputFormatWrapper<LongWritable, Writable> {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileThriftInputFormat.class);

  private TypeRef<? extends TBase<?, ?>> typeRef;

  /** internal, for MR use only. */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public RCFileThriftInputFormat() {
    super(new RCFileInputFormat());
  }

  public RCFileThriftInputFormat(TypeRef<? extends TBase<?, ?>> typeRef) {
    this();
    this.typeRef = typeRef;
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setClassConf(Class<? extends TBase<?, ?> > thriftClass, Configuration conf) {
    ThriftUtils.setClassConf(conf, RCFileThriftInputFormat.class, thriftClass);
  }

  @Override
  public RecordReader<LongWritable, Writable>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
                                    throws IOException, InterruptedException {
    if (typeRef == null) {
      typeRef = ThriftUtils.getTypeRef(taskAttempt.getConfiguration(), RCFileThriftInputFormat.class);
    }
    return new ThriftReader(super.createRecordReader(split, taskAttempt));
  }

  public class ThriftReader extends FilterRecordReader<LongWritable, Writable> {

    private final TupleFactory tf = TupleFactory.getInstance();

    private TStructDescriptor     tDesc;
    private boolean               readUnknownsColumn = false;
    private List<Field>           knownRequiredFields = Lists.newArrayList();
    private ArrayList<Integer>    columnsBeingRead = Lists.newArrayList();

    private TMemoryInputTransport memTransport = new TMemoryInputTransport();
    private TBinaryProtocol tProto = new TBinaryProtocol(memTransport);

    private TBase<?, ?>           currentValue;
    private ThriftWritable<TBase<?, ?>> thriftWritable;

    /**
     * The reader is expected to be a
     * <code>RecordReader< LongWritable, BytesRefArrayWritable ></code>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ThriftReader(RecordReader reader) {
      super(reader);
    }

    /** is valid only after initialize() is called */
    public boolean isReadingUnknonwsColumn() {
      return readUnknownsColumn;
    }

    @Override @SuppressWarnings("unchecked")
    public void initialize(InputSplit split, TaskAttemptContext ctx)
                           throws IOException, InterruptedException {
      // set up columns that needs to read from the RCFile.

      tDesc = TStructDescriptor.getInstance(typeRef.getRawClass());
      thriftWritable = ThriftWritable.newInstance((Class<TBase<?, ?>>)typeRef.getRawClass());
      final List<Field> tFields = tDesc.getFields();

      FileSplit fsplit = (FileSplit)split;
      Path file = fsplit.getPath();

      LOG.info(String.format("reading %s from %s:%d:%d"
          , typeRef.getRawClass().getName()
          , file.toString()
          , fsplit.getStart()
          , fsplit.getStart() + fsplit.getLength()));

      ColumnarMetadata storedInfo = RCFileUtil.readMetadata(ctx.getConfiguration(),
                                                           file);

      // list of field numbers
      List<Integer> tFieldIds = Lists.transform(tFields,
                                    new Function<Field, Integer>() {
                                       public Integer apply(Field fd) {
                                         return Integer.valueOf(fd.getFieldId());
                                       }
                                    });

      columnsBeingRead = RCFileUtil.findColumnsToRead(ctx.getConfiguration(),
                                                      tFieldIds,
                                                      storedInfo);

      for(int idx : columnsBeingRead) {
        int fid = storedInfo.getFieldId(idx);
        if (fid >= 0) {
          knownRequiredFields.add(tFields.get(tFieldIds.indexOf(fid)));
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

    @Override @SuppressWarnings({ "unchecked", "rawtypes" })
    public Writable getCurrentValue() throws IOException, InterruptedException {
      try {
        thriftWritable.set(getCurrentThriftValue());
        return thriftWritable;
      } catch (TException e) {
        //TODO : add error tracking
        throw new IOException(e);
      }
    }

    /**
     * Builds Thrift object from the raw bytes returned by RCFile reader.
     * @throws TException
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TBase<?, ?> getCurrentThriftValue() throws IOException, InterruptedException, TException {
      if (currentValue != null) {
        return currentValue;
      }

      BytesRefArrayWritable byteRefs = (BytesRefArrayWritable) super.getCurrentValue();
      if (byteRefs == null) {
        return null;
      }

      TBase tObj = tDesc.newThriftObject();

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        if (buf.getLength() > 0) {
          memTransport.reset(buf.getData(), buf.getStart(), buf.getLength());

          Field field = knownRequiredFields.get(i);
          tObj.setFieldValue(field.getFieldIdEnum(),
                             ThriftUtils.readFieldNoTag(tProto, field));
        }
        // else no need to set default value since any default value
        // would have been serialized when this record was written.
      }

      // parse unknowns column if required
      if (readUnknownsColumn) {
        int last = columnsBeingRead.get(columnsBeingRead.size() - 1);
        BytesRefWritable buf = byteRefs.get(last);
        if (buf.getLength() > 0) {
          memTransport.reset(buf.getData(), buf.getStart(), buf.getLength());
          tObj.read(tProto);
        }
      }

      currentValue = tObj;
      return currentValue;
    }

    /**
     * Returns a Tuple consisting of required fields with out creating
     * a Thrift message at the top level.
     */
    public Tuple getCurrentTupleValue() throws IOException, InterruptedException, TException {

      BytesRefArrayWritable byteRefs = (BytesRefArrayWritable) super.getCurrentValue();
      if (byteRefs == null) {
        return null;
      }

      Tuple tuple = tf.newTuple(knownRequiredFields.size());

      for (int i=0; i < knownRequiredFields.size(); i++) {
        BytesRefWritable buf = byteRefs.get(columnsBeingRead.get(i));
        if (buf.getLength() > 0) {
          memTransport.reset(buf.getData(), buf.getStart(), buf.getLength());

          Field field = knownRequiredFields.get(i);
          Object value = ThriftUtils.readFieldNoTag(tProto, field);
          tuple.set(i, ThriftToPig.toPigObject(field, value, false));
        }
      }

      if (readUnknownsColumn) {
        throw new IOException("getCurrentTupleValue() is not supported when 'readUnknownColumns' is set");
      }

      return tuple;
    }
  }
}
