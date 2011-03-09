package com.twitter.elephantbird.pig8.util;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig8.load.LzoThriftB64LinePigLoader;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * <li> converts a Thrift struct to a Pig tuple
 * <li> utilities to provide schema for Pig loaders and Pig scripts
 */
public class ThriftToPig<M extends TBase<?, ?>> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftToPig.class);

  /* TODO :
   * 1. Add lazy deserialization like ProtobufTuple does. Not sure if it can be done
   *    efficiently for Thrift.
   * 2. Converting Enum to names (strings) is supported only in the common
   *    case where Enum is part of a struct. Enum used directly in containers
   *    (e.g. list<SomeEnum>) are still integers. The issue is that Thrift
   *    does not explicitly tell that it is writing an Enum. We need to
   *    deduce that from the context. In the case of Structs, we already
   *    maintain this contexts.
   *
   *    In order to support enums-to-strings correctly we need to maintain more
   *    state and we should always know exact context/recursion of Thrift
   *    struct's write() method.
   *
   *    This is certainly do-able. Once we keep track of serialization
   *    so closely, we not far from implementing our own generic write() method.
   *    implementing generic write method will let us deserialize thrift buffer
   *    directly to a Pig Tuple and there is no need to use a Thrift object
   *    as intermediate step. This will also let us support
   *    lazy-deserialization and projections efficiently since we direclty
   *    access the thrift buffer.
   */
  private static BagFactory bagFactory_ = BagFactory.getInstance();
  private static TupleFactory tupleFactory_  = TupleFactory.getInstance();

  /** for some reason there is no TType.BINARY. */
  private static final byte TTYPE_BINARY = 83;

  private Class<? extends TBase<?, ?>> tClass_;
  private ThriftProtocol tProtocol_ = new ThriftProtocol();
  private Deque<PigContainer> containerStack_ = new ArrayDeque<PigContainer>();
  private PigContainer curContainer_;
  private Tuple curTuple_;

  // We want something that provides a generic interface for populating
  // Pig Tuples, Bags, and Maps. This does the trick.

  private abstract class PigContainer {
    StructDescriptor structDesc; // The current thrift struct being written
    FieldDescriptor curFieldDesc;
    public abstract Object getContents();
    public abstract void add(Object o) throws TException;

    /** set curFieldDesc if the container is is Thrift Struct. */
    public void setCurField(TField tField) throws TException {
      if (structDesc != null) {
        curFieldDesc = structDesc.fieldMap.get(tField.id);
        if (curFieldDesc == null) {
          throw new TException("Unexpected TField " + tField + " for " + tClass_.getName());
        }
      }
    }
  }

  private class TupleWrap extends PigContainer {

    private final Tuple t;

    public TupleWrap(int size) {
      t = tupleFactory_.newTuple(size);
    }

    @Override
    public Object getContents() { return t; }

    @Override
    public void add(Object o) throws TException {
      if (curFieldDesc == null) {
        throw new TException("Internal Error. curFieldDesc is not set");
      }
      if (curFieldDesc.enumMap != null && // map enum to string
          (o = curFieldDesc.enumMap.get(o)) == null) {
        throw new TException("cound not find Enum string");
      }
      try {
        t.set(curFieldDesc.tupleIdx, o);
       } catch (ExecException e) {
          throw new TException(e);
       }
    }
  }

  private class BagWrap extends PigContainer {
    List<Tuple> tuples;

    public BagWrap(int size) {
      tuples =  Lists.newArrayListWithCapacity(size);
    }

    @Override
    public void add(Object o) throws TException {
      // Pig bags contain tuples of objects, so we must wrap a tuple around
      // everything we get.
      if (o instanceof Tuple) {
        tuples.add((Tuple) o);
      } else {
        tuples.add(tupleFactory_.newTuple(o));
      }
    }

    @Override
    public Object getContents() {
      return bagFactory_.newDefaultBag(tuples);
    }
  }

  private class MapWrap extends PigContainer {
    private final Map<String, Object> map;
    String currKey = null;

    public MapWrap(int size) {
      map = new HashMap<String, Object>(size);
    }

    @Override
    public void add(Object o) throws TException {
      //we alternate between String keys and (converted) DataByteArray values.
      if (currKey == null) {
        try {
          currKey = (String) o;
        } catch (ClassCastException e) {
          throw new TException("Only String keys are allowed in maps.");
        }
      } else {
        map.put(currKey, o);
        currKey = null;
      }
    }

    @Override
    public Object getContents() {
      return map;
    }
  }


  private void pushContainer(PigContainer c) {
    containerStack_.addLast(c);
    curContainer_ = c;
  }

  private PigContainer popContainer() throws TException {
    PigContainer c = containerStack_.removeLast();
    curContainer_ = containerStack_.peekLast();
    if (curContainer_ == null) { // All done!
      curTuple_ = (Tuple) c.getContents();
    } else {
      curContainer_.add(c.getContents());
    }
    return c;
  }

  public static <M extends TBase<?, ?>> ThriftToPig<M> newInstance(Class<M> tClass) {
    return new ThriftToPig<M>(tClass);
  }

  public static <M extends TBase<?, ?>> ThriftToPig<M> newInstance(TypeRef<M> typeRef) {
    return new ThriftToPig<M>(typeRef.getRawClass());
  }

  public ThriftToPig(Class<M> tClass) {
    this.tClass_ = tClass;
    structMap = Maps.newHashMap();
    updateStructMap(tClass_);
    structMap = ImmutableMap.copyOf(structMap);
    reset();
  }

  /**
   * The protocol should be reset before each object that is serialized.
   * This is important since, the protocol itself can not reliably
   * realize if it at the beginning of a new object. It can not always
   * rely on the last object being correct written because of
   * any exceptions while processing previous object.
   */
  public void reset() {
    containerStack_.clear();
    curContainer_ = null;
    curTuple_ = null;
  }

  /**
   * Converts a thrift object to Pig tuple.
   * Throws TException in case of any errors.
   */
  public Tuple getPigTuple(M thriftObj) throws TException {
    reset();
    thriftObj.write(tProtocol_);
    if (curTuple_ != null) {
      return curTuple_;
    }
    // unexpected
    throw new TException("Internal error. tuple is not set");
  }

  /**
   * returns 'enum int -> enum name' mapping
   */
  static private Map<Integer, String> extractEnumMap(FieldValueMetaData field) {
    MetaData f = new MetaData(field);
    if (!f.isEnum()) {
      return null;
    }
    Map<Integer, String> map = Maps.newHashMap();
    for(TEnum e : f.getEnumClass().getEnumConstants()) {
      map.put(e.getValue(), e.toString());
    }
    return ImmutableMap.copyOf(map);
  }

  /**
   * holds relevant info for a field in a Thrift Struct including
   * index into tuple array.
   */
  private static class FieldDescriptor {
    TFieldIdEnum fieldEnum;
    int tupleIdx;
    Map<Integer, String> enumMap = null; // set for enums
  }

  /**
   * describes a Thrift struct. Contains following info :
   * <li> Thrift field descriptor map
   * <li> ...
   */
  private static class StructDescriptor {
    Map<Short, FieldDescriptor> fieldMap;

    public StructDescriptor(Class<? extends TBase<?, ?>> tClass) {
      fieldMap = Maps.newHashMap();
      int idx = 0;
      for (Entry<? extends TFieldIdEnum, FieldMetaData> e : FieldMetaData.getStructMetaDataMap(tClass).entrySet()) {
        FieldDescriptor desc = new FieldDescriptor();
        desc.fieldEnum = e.getKey();
        desc.tupleIdx = idx++;
        if (e.getValue().valueMetaData.type == TType.ENUM) {
          desc.enumMap = extractEnumMap(e.getValue().valueMetaData);
        }
        fieldMap.put(desc.fieldEnum.getThriftFieldId(), desc);
      }
      fieldMap = ImmutableMap.copyOf(fieldMap);
    }
  }

  private Map<TStruct, StructDescriptor> structMap;

  private void updateStructMap(Class<? extends TBase<?, ?>> tClass) {
    final TStruct tStruct = getStructDesc(tClass);

    if (structMap.get(tStruct) != null) {
      return;
    }

    StructDescriptor desc = new StructDescriptor(tClass);
    LOG.debug("adding struct descriptor for " + tClass.getName()
        + " with " + desc.fieldMap.size() + " fields");
    structMap.put(tStruct, desc);
    // recursively add any referenced classes.
    for (FieldMetaData field : FieldMetaData.getStructMetaDataMap(tClass).values()) {
      updateStructMap(field.valueMetaData);
    }
  }

  /**
   * Look for any class embedded in the in the container or struct fields
   * and update the struct map with them.
   */
  private void updateStructMap(FieldValueMetaData field) {
    MetaData f = new MetaData(field);

    if (f.isStruct()) {
      updateStructMap(f.getStructClass());
    }

    if (f.isList()) {
      updateStructMap(f.getListElem());
    }

    if (f.isMap()) {
      if (f.getMapKey().type != TType.STRING) {
        throw new IllegalArgumentException("Pig does not support maps with non-string keys "
            + "while initializing ThriftToPig for " + tClass_.getName());
      }
      updateStructMap(f.getMapKey());
      updateStructMap(f.getMapValue());
    }

    if (f.isSet()) {
      updateStructMap(f.getSetElem());
    }
  }

  private class ThriftProtocol extends TProtocol {

    ThriftProtocol() {
      super(null); // this protocol is not used for transport.
    }

    @Override
    public void writeBinary(ByteBuffer bin) throws TException {
      byte[] buf = new byte[bin.remaining()];
      bin.mark();
      bin.get(buf);
      bin.reset();
      curContainer_.add(new DataByteArray(buf));
      /* We could use DataByteArray(byte[], start, end) and avoid a
       * copy here.  But the constructor will make a (quite inefficient) copy.
       */
    }

    @Override
    public void writeBool(boolean b) throws TException {
      curContainer_.add(Integer.valueOf(b ? 1 : 0));
    }

    @Override
    public void writeByte(byte b) throws TException {
      curContainer_.add(Integer.valueOf(b));
    }

    @Override
    public void writeDouble(double dub) throws TException {
      curContainer_.add(Double.valueOf(dub));
    }

    @Override
    public void writeFieldBegin(TField field) throws TException {
      curContainer_.setCurField(field);
    }

    @Override
    public void writeFieldEnd() throws TException {
    }

    @Override
    public void writeFieldStop() throws TException {
    }

    @Override
    public void writeI16(short i16) throws TException {
      curContainer_.add(Integer.valueOf(i16));
    }

    @Override
    public void writeI32(int i32) throws TException {
      curContainer_.add(i32);
    }

    @Override
    public void writeI64(long i64) throws TException {
      curContainer_.add(i64);
    }

    @Override
    public void writeListBegin(TList list) throws TException {
      pushContainer(new BagWrap(list.size));
    }

    @Override
    public void writeListEnd() throws TException {
      popContainer();
    }

    @Override
    public void writeMapBegin(TMap map) throws TException {
      pushContainer(new MapWrap(map.size));
    }

    @Override
    public void writeMapEnd() throws TException {
      popContainer();
    }

    @Override
    public void writeSetBegin(TSet set) throws TException {
      pushContainer(new BagWrap(set.size));
    }

    @Override
    public void writeSetEnd() throws TException {
      popContainer();
    }

    @Override
    public void writeString(String str) throws TException {
      curContainer_.add(str);
    }

    @Override
    public void writeStructBegin(TStruct struct) throws TException {
      StructDescriptor desc = structMap.get(struct);
      if (desc == null) {
        throw new TException("Unexpected TStruct " + struct.name + " for " + tClass_.getName());
      }
      PigContainer c = new TupleWrap(desc.fieldMap.size());
      c.structDesc = desc;
      pushContainer(c);
    }

    @Override
    public void writeStructEnd() throws TException {
      popContainer();
    }

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      throw new TException("method not implemented.");
    }
    @Override
    public void writeMessageEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public ByteBuffer readBinary() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public boolean readBool() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public byte readByte() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public double readDouble() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TField readFieldBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readFieldEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public short readI16() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public int readI32() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public long readI64() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TList readListBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readListEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TMap readMapBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readMapEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TMessage readMessageBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readMessageEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TSet readSetBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readSetEnd() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public String readString() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public TStruct readStructBegin() throws TException {
      throw new TException("method not implemented.");
    }

    @Override
    public void readStructEnd() throws TException {
      throw new TException("method not implemented.");
    }
  }

  /**
   * A utility class to help with type checking of a ThriftField.
   * Avoids checking type and not-so-readable casting in many places.
   */
  static class MetaData {
    final FieldValueMetaData field;

    MetaData(FieldValueMetaData field) {
      this.field = field;
    }

    FieldValueMetaData getField() {
      return field;
    }

    // List
    boolean isList() {
      return field instanceof ListMetaData;
    }

    FieldValueMetaData getListElem() {
      return ((ListMetaData)field).elemMetaData;
    }

    // Enum
    boolean isEnum() {
      return field instanceof EnumMetaData;
    }

    Class<? extends TEnum> getEnumClass() {
      return ((EnumMetaData)field).enumClass;
    }

    // Map
    boolean isMap() {
      return field instanceof MapMetaData;
    }

    FieldValueMetaData getMapKey() {
      return ((MapMetaData)field).keyMetaData;
    }

    FieldValueMetaData getMapValue() {
      return ((MapMetaData)field).valueMetaData;
    }

    // Set
    boolean isSet() {
      return field instanceof SetMetaData;
    }

    FieldValueMetaData getSetElem() {
      return ((SetMetaData)field).elemMetaData;
    }

    // Struct
    boolean isStruct() {
      return field instanceof StructMetaData;
    }

    @SuppressWarnings("unchecked")
    Class<? extends TBase<?, ?>> getStructClass() {
      return (Class <? extends TBase<?, ?>>)((StructMetaData)field).structClass;
    }
  }

  /**
   * Returns Pig schema for the Thrift struct.
   */
  public static Schema toSchema(Class<? extends TBase<?, ?>> tClass) {
    Schema schema = new Schema();

    try {
      for (Entry<? extends TFieldIdEnum, FieldMetaData> e : FieldMetaData.getStructMetaDataMap(tClass).entrySet()) {
        FieldMetaData meta = e.getValue();
        FieldValueMetaData field = e.getValue().valueMetaData;
        MetaData fm = new MetaData(field);
        if (fm.isStruct()) {
          schema.add(new FieldSchema(meta.fieldName, toSchema(fm.getStructClass()), DataType.TUPLE));
        } else if (fm.isEnum()) { // enums in Structs are strings (enums in containers are not, yet)
          schema.add(new FieldSchema(meta.fieldName, null, DataType.CHARARRAY));
        } else {
          if (field.type == TType.STRING) {
            // A hack to get around the fact that Thrift uses TType.STRING
            // for both binary and string.
            Class<?> fieldType = ThriftUtils.getFiedlType(tClass, meta.fieldName);
            if (fieldType == ByteBuffer.class) {
              field = new FieldValueMetaData(TTYPE_BINARY);
            }
            // This a partition work around. still need to fix the case
            // when 'binary' is used in containers.
            // This is fixed in Thrift 0.6 (field.isBinary()).
          }
          schema.add(singleFieldToFieldSchema(meta.fieldName, field));
        }
      }
    } catch (FrontendException t) {
      throw new RuntimeException(t);
    }

    return schema;
  }

  private static FieldSchema singleFieldToFieldSchema(String fieldName, FieldValueMetaData field) throws FrontendException {

    MetaData fm = new MetaData(field);

    switch (field.type) {
      case TType.LIST:
        return new FieldSchema(fieldName, singleFieldToTupleSchema(fieldName + "_tuple", fm.getListElem()), DataType.BAG);
      case TType.SET:
        return new FieldSchema(fieldName, singleFieldToTupleSchema(fieldName + "_tuple", fm.getSetElem()), DataType.BAG);
      case TType.MAP:
        // can not specify types for maps in Pig.
        return new FieldSchema(fieldName, null, DataType.MAP);
      default:
        return new FieldSchema(fieldName, null, getPigDataType(field));
    }
  }

  /**
   * Returns a schema with single tuple (for Pig bags).
   */
  private static Schema singleFieldToTupleSchema(String fieldName, FieldValueMetaData field) throws FrontendException {
    MetaData fm = new MetaData(field);
    FieldSchema fieldSchema = null;

    switch (field.type) {
      case TType.STRUCT:
        fieldSchema = new FieldSchema(fieldName, toSchema(fm.getStructClass()), DataType.TUPLE);
        break;
      case TType.LIST:
        fieldSchema = singleFieldToFieldSchema(fieldName, fm.getListElem());
        break;
      case TType.SET:
        fieldSchema = singleFieldToFieldSchema(fieldName, fm.getSetElem());
        break;
      default:
        fieldSchema = new FieldSchema(fieldName, null, getPigDataType(fm.getField()));
    }

    Schema schema = new Schema();
    schema.add(fieldSchema);
    return schema;
  }

  private static byte getPigDataType(FieldValueMetaData field) {
    switch (field.type) {
      case TType.BOOL:
      case TType.BYTE:
      case TType.I16:
      case TType.I32:
      case TType.ENUM: // will revisit this once Enums in containers are also strings.
        return DataType.INTEGER;
      case TType.I64:
        return DataType.LONG;
      case TType.DOUBLE:
        return DataType.DOUBLE;
      case TType.STRING:
        return DataType.CHARARRAY;
      case TTYPE_BINARY:
        return DataType.BYTEARRAY;
      default:
        throw new IllegalArgumentException("Unexpected type where a simple type is expected : " + field.type);
    }
  }

  /**
   * Turn a Thrift Struct into a loading schema for a pig script.
   */
  public static String toPigScript(Class<? extends TBase<?, ?>> thriftClass,
                                   Class<? extends LoadFunc> pigLoader) {
    StringBuilder sb = new StringBuilder();
    /* we are commenting out explicit schema specification. The schema is
     * included mainly to help the readers of the pig script. Pig learns the
     * schema directly from the loader.
     * If explicit schema is not commented, we might have surprising results
     * when a Thrift class (possibly in control of another team) changes,
     * but the Pig script is not updated. Commenting it out work around this.
     */
    StringBuilder prefix = new StringBuilder("       --  ");
    sb.append("raw_data = load '$INPUT_FILES' using ")
      .append(pigLoader.getName())
      .append("('")
      .append(thriftClass.getName())
      .append("');\n")
      .append(prefix)
      .append("as ");
    prefix.append("   ");

    try {
      stringifySchema(sb, toSchema(thriftClass), DataType.TUPLE, prefix);
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }

    sb.append("\n");
    return sb.toString();
  }

  /**
   * Print formatted schema. This is a modified version of
   * {@link Schema#stringifySchema(StringBuilder, Schema, byte)}
   * with support for (indented) pretty printing.
   */
  // This is used for building up output string
  // type can only be BAG or TUPLE
  public static void stringifySchema(StringBuilder sb,
                                     Schema schema,
                                     byte type,
                                     StringBuilder prefix)
                                          throws FrontendException{
      // this is a modified version of {@link Schema#stringifySchema(StringBuilder, Schema, byte)}
      if (type == DataType.TUPLE) {
          sb.append("(") ;
      }
      else if (type == DataType.BAG) {
          sb.append("{") ;
      }

      prefix.append("  ");
      sb.append("\n").append(prefix);

      if (schema == null) {
          sb.append("null") ;
      }
      else {
          boolean isFirst = true ;
          for (int i=0; i< schema.size() ;i++) {

              if (!isFirst) {
                  sb.append(",\n").append(prefix);
              }
              else {
                  isFirst = false ;
              }

              FieldSchema fs = schema.getField(i) ;

              if(fs == null) {
                  sb.append("null");
                  continue;
              }

              if (fs.alias != null) {
                  sb.append(fs.alias);
                  sb.append(": ");
              }

              if (DataType.isAtomic(fs.type)) {
                  sb.append(DataType.findTypeName(fs.type)) ;
              }
              else if ( (fs.type == DataType.TUPLE) ||
                        (fs.type == DataType.BAG) ) {
                  // safety net
                  if (schema != fs.schema) {
                      stringifySchema(sb, fs.schema, fs.type, prefix) ;
                  }
                  else {
                      throw new AssertionError("Schema refers to itself "
                                               + "as inner schema") ;
                  }
              } else if (fs.type == DataType.MAP) {
                  sb.append(DataType.findTypeName(fs.type) + "[ ]") ;
              } else {
                  sb.append(DataType.findTypeName(fs.type)) ;
              }
          }
      }

      prefix.setLength(prefix.length()-2);
      sb.append("\n").append(prefix);

      if (type == DataType.TUPLE) {
          sb.append(")") ;
      }
      else if (type == DataType.BAG) {
          sb.append("}") ;
      }
  }

  private TStruct getStructDesc(Class<? extends TBase<?, ?>> tClass) {
    // hack to get hold of STRUCT_DESC of a thrift class:
    // Access 'private static final' field STRUCT_DESC using reflection.
    // Bad practice, but not sure if there is a better way.
    try {
      Field f = tClass.getDeclaredField("STRUCT_DESC");
      f.setAccessible(true);
      return (TStruct) f.get(null);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      Class<? extends TBase<?, ?>> tClass = ThriftUtils.getTypeRef(args[0]).getRawClass();
      System.out.println(args[0] + " : " + toSchema(tClass).toString());
      System.out.println(toPigScript(tClass, LzoThriftB64LinePigLoader.class));
    }
  }
}
