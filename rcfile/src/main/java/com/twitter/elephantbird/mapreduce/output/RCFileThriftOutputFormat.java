package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.twitter.elephantbird.util.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.twitter.data.proto.Misc.ColumnarMetadata;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * OutputFormat for storing Thrift objects in RCFile.<p>
 *
 * Each of the top level fields is stored in a separate column.
 * Thrift field ids are stored in RCFile metadata.<p>
 *
 * The user can write either a {@link ThriftWritable} with the Thrift object
 * or a {@link BytesWritable} with serialized Thrift bytes. The latter
 * ensures that all the fields are preserved even if the current Thrift
 * definition does not match the definition represented in the serialized bytes.
 * Any fields not recognized by current Thrift class are stored in the last
 * column.
 */
public class RCFileThriftOutputFormat extends RCFileOutputFormat {

  // typeRef is only required for setting metadata for the RCFile
  private TypeRef<? extends TBase<?, ?>> typeRef;
  private TStructDescriptor tDesc;
  private List<Field> tFields;
  private int numColumns;

  private BytesRefArrayWritable rowWritable = new BytesRefArrayWritable();
  private BytesRefWritable[] colValRefs;

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

    private ByteStream.Output byteStream = new ByteStream.Output();
    private TBinaryProtocol tProto = new TBinaryProtocol(
                                        new TIOStreamTransport(byteStream));

    // used when deserializing thrift bytes
    private Map<Short, Integer> idMap;
    private TMemoryInputTransport mTransport;
    private TBinaryProtocol skipProto;

    ProtobufWriter(TaskAttemptContext job) throws IOException {
      super(RCFileThriftOutputFormat.this, job, Protobufs.toText(makeColumnarMetadata()));
    }

    @Override @SuppressWarnings("unchecked")
    public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
      try {
        if (value instanceof BytesWritable) {
          // TODO: handle errors
          fromBytes((BytesWritable)value);
        } else {
          fromObject((TBase<?, ?>)((ThriftWritable)value).get());
        }
      } catch (TException e) {
        // might need to tolerate a few errors.
        throw new IOException(e);
      }

      super.write(null, rowWritable);
    }

    @SuppressWarnings("unchecked")
    private void fromObject(TBase tObj)
                    throws IOException, InterruptedException, TException {

      byteStream.reset(); // reinitialize the byteStream if buffer is too large?
      int startPos = 0;

      // top level fields are split across the columns.
      for (int i=0; i < numColumns; i++) {

        if (i < (numColumns - 1)) {

          Field fd = tFields.get(i);
          ThriftUtils.writeFieldNoTag(tProto, fd, tDesc.getFieldValue(i, tObj));

        } // else { }  : no 'unknown fields' in thrift object

        colValRefs[i].set(byteStream.getData(),
                          startPos,
                          byteStream.getCount() - startPos);
        startPos = byteStream.getCount();
      }
    }

    /**
     * extract serialized bytes for each field, including unknown fields and
     * store those byes in columns.
     */
    private void fromBytes(BytesWritable bytesWritable)
                       throws IOException, InterruptedException, TException {

      if (mTransport == null) {
        initIdMap();
        mTransport = new TMemoryInputTransport();
        skipProto = new TBinaryProtocol(mTransport);
      }

      byte[] bytes = bytesWritable.getBytes();
      mTransport.reset(bytes, 0, bytesWritable.getLength());
      byteStream.reset();

      // set all the fields to null
      for(BytesRefWritable ref : colValRefs) {
        ref.set(bytes, 0, 0);
      }

      skipProto.readStructBegin();

      while (true) {
        int start = mTransport.getBufferPosition();

        TField field = skipProto.readFieldBegin();
        if (field.type == TType.STOP) {
          break;
        }

        int fieldStart = mTransport.getBufferPosition();

        // skip still creates and copies primitive objects (String, buffer, etc)
        // skipProto could override readString() and readBuffer() to avoid that.
        TProtocolUtil.skip(skipProto, field.type);

        int end = mTransport.getBufferPosition();

        Integer idx = idMap.get(field.id);

        if (idx != null && field.type == tFields.get(idx).getType()) {
          // known field
          colValRefs[idx].set(bytes, fieldStart, end-fieldStart);
        } else {
          // unknown field, copy the bytes to last column (with field id)
          byteStream.write(bytes, start, end-start);
        }
      }

      if (byteStream.getCount() > 0) {
        byteStream.write(TType.STOP);
        colValRefs[colValRefs.length-1].set(byteStream.getData(),
                                            0,
                                            byteStream.getCount());
      }
    }

    private void initIdMap() {
      idMap = Maps.newHashMap();
      for(int i=0; i<tFields.size(); i++) {
        idMap.put(tFields.get(i).getFieldId(), i);
      }
      idMap = ImmutableMap.copyOf(idMap);
    }
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the output format correctly.
   */
  public static void setClassConf(Class<? extends TBase<?, ?> > thriftClass, Configuration conf) {
    ThriftUtils.setClassConf(conf, RCFileThriftOutputFormat.class, thriftClass);
  }

  @Override
  public RecordWriter<NullWritable, Writable>
    getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

    if (typeRef == null) {
      typeRef = ThriftUtils.getTypeRef(HadoopCompat.getConfiguration(job), RCFileProtobufOutputFormat.class);
      init();
    }

    RCFileOutputFormat.setColumnNumber(HadoopCompat.getConfiguration(job), numColumns);
    return new ProtobufWriter(job);
  }
}
