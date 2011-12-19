package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.piggybank.storage.hiverc.HiveRCInputFormat;
import org.apache.pig.piggybank.storage.hiverc.HiveRCRecordReader;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.pig.util.RCFileUtil;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

public class RCFileThriftInputFormat extends HiveRCInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileThriftInputFormat.class);

  private TypeRef<? extends TBase<?, ?>> typeRef;

  /** internal, for MR use only. */
  public RCFileThriftInputFormat() {
  }

  public RCFileThriftInputFormat(TypeRef<? extends TBase<?, ?>> typeRef) {
    this.typeRef = typeRef;
  }

  /**
   * In addition to setting InputFormat class to {@link RCFileThriftInputFormat},
   * sets an internal configuration in jobConf so that remote tasks
   * instantiate appropriate object for the correct Thrift class.
   */
  public static <T extends TBase<?, ?>> void
      setInputFormatClass(Class<T> thriftClass, Job job) {
    ThriftUtils.setClassConf(job.getConfiguration(), RCFileThriftInputFormat.class, thriftClass);
    job.setInputFormatClass(RCFileThriftInputFormat.class);
  }


  @Override @SuppressWarnings("unchecked")
  public RecordReader createRecordReader(InputSplit split,
                                         TaskAttemptContext taskAttempt)
                                    throws IOException, InterruptedException {
    if (typeRef == null) {
      typeRef = ThriftUtils.getTypeRef(taskAttempt.getConfiguration(), RCFileThriftInputFormat.class);
    }
    return new ThriftReader();
  }

  public class ThriftReader extends HiveRCRecordReader {


    private TStructDescriptor     tDesc;
    private boolean               readUnknownsColumn = false;
    private List<Field>           knownRequiredFields = Lists.newArrayList();
    private ArrayList<Integer>    columnsBeingRead = Lists.newArrayList();

    private TMemoryInputTransport memTransport = new TMemoryInputTransport();
    private TBinaryProtocol tProto = new TBinaryProtocol(memTransport);

    private TBase<?, ?>     currentValue;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx)
                           throws IOException, InterruptedException {
      // set up columns that needs to read from the RCFile.

      tDesc = TStructDescriptor.getInstance(typeRef.getRawClass());
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

    /**
     * Builds Thrift object from the raw bytes returned by RCFile reader.
     * @throws TException
     */
    @SuppressWarnings("unchecked")
    public TBase<?, ?> getCurrentThriftValue() throws IOException, InterruptedException, TException {
      /* getCurrentValue() returns a BytesRefArrayWritable since this class
       * extends HiveRCRecordReader. Other option is to extend
       * RecordReader directly and explicitly delegate each of the methods to
       * HiveRCRecordReader
       */
      if (currentValue != null) {
        return currentValue;
      }

      BytesRefArrayWritable byteRefs = getCurrentValue();
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
  }
}
