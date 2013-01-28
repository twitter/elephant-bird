package com.twitter.elephantbird.pig.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.pig.load.ThriftPigLoader;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * <li> converts a Thrift struct to a Pig tuple
 * <li> utilities to provide schema for Pig loaders and Pig scripts
 */
public class ThriftToPig<M extends TBase<?, ?>> {

  public static final Logger LOG = LoggerFactory.getLogger(ThriftToPig.class);

  static final String USE_ENUM_ID_CONF_KEY = "elephantbird.pig.thrift.load.enum.as.int";

  // static because it is used in toSchema and toPigTuple which are static
  private static Boolean useEnumId = false;
  private static TupleFactory tupleFactory  = TupleFactory.getInstance();

  private TStructDescriptor structDesc;

  public static <M extends TBase<?, ?>> ThriftToPig<M> newInstance(Class<M> tClass) {
    return new ThriftToPig<M>(tClass);
  }

  public static <M extends TBase<?, ?>> ThriftToPig<M> newInstance(TypeRef<M> typeRef) {
    return new ThriftToPig<M>(typeRef.getRawClass());
  }

  public ThriftToPig(Class<M> tClass) {
    structDesc = TStructDescriptor.getInstance(tClass);
  }

  public TStructDescriptor getTStructDescriptor() {
    return structDesc;
  }

  /**
   * Converts a thrift object to Pig tuple.
   * All the fields are deserialized.
   * It might be better to use getLazyTuple() if not all fields
   * are required.
   */
  public Tuple getPigTuple(M thriftObj) {
    return toTuple(structDesc, thriftObj);
  }

  /**
   * Similar to {@link #getPigTuple(TBase)}. This delays
   * serialization of tuple contents until they are requested.
   * @param thriftObj
   * @return
   */
  public Tuple getLazyTuple(M thriftObj) {
    return new LazyTuple(structDesc, thriftObj);
  }

  /**
   * Set the flags that can be used by the conversion.
   * @param conf usually the Hadoop job conf
   */
  public static void setConversionProperties(Configuration conf) {
    if (conf != null) {
      useEnumId = conf.getBoolean(USE_ENUM_ID_CONF_KEY, false);
      LOG.debug("useEnumId is set to " + useEnumId);
    }
  }

  @SuppressWarnings("rawtypes")
  private static <T extends TBase>Tuple toTuple(TStructDescriptor tDesc, T tObj) {
    int size = tDesc.getFields().size();
    Tuple tuple = tupleFactory.newTuple(size);
    for (int i=0; i<size; i++) {
      Field field = tDesc.getFieldAt(i);
      Object value = tDesc.getFieldValue(i, tObj);
      try {
        tuple.set(i, toPigObject(field, value, false));
      } catch (ExecException e) { // not expected
        throw new RuntimeException(e);
      }
    }
    return tuple;
  }

  @SuppressWarnings("unchecked")
  public static Object toPigObject(Field field, Object value, boolean lazy) {
    if (value == null) {
      return null;
    }

    switch (field.getType()) {
    case TType.BOOL:
      return Integer.valueOf((Boolean)value ? 1 : 0);
    case TType.BYTE :
      return Integer.valueOf((Byte)value);
    case TType.I16 :
      return Integer.valueOf((Short)value);
    case TType.STRING:
      return stringTypeToPig(value);
    case TType.STRUCT:
      if (lazy) {
        return new LazyTuple(field.gettStructDescriptor(), (TBase<?, ?>)value);
      } else {
        return toTuple(field.gettStructDescriptor(), (TBase<?, ?>)value);
      }
    case TType.MAP:
      return toPigMap(field, (Map<Object, Object>)value, lazy);
    case TType.SET:
      return toPigBag(field.getSetElemField(), (Collection<Object>)value, lazy);
    case TType.LIST:
      return toPigBag(field.getListElemField(), (Collection<Object>)value, lazy);
    case TType.ENUM:
      if (useEnumId) {
        return field.getEnumValueOf(value.toString()).getValue();
      } else {
        return value.toString();
      }
    default:
      // standard types : I32, I64, DOUBLE, etc.
      return value;
    }
  }

  /**
   * TType.STRING is a mess in Thrift. It could be byte[], ByteBuffer,
   * or even a String!.
   */
  private static Object stringTypeToPig(Object value) {
    if (value instanceof String) {
      return value;
    }
    if (value instanceof byte[]) {
      byte[] buf = (byte[])value;
      return new DataByteArray(Arrays.copyOf(buf, buf.length));
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer bin = (ByteBuffer)value;
      byte[] buf = new byte[bin.remaining()];
      bin.mark();
      bin.get(buf);
      bin.reset();
      return new DataByteArray(buf);
    }
    return null;
  }

  private static Map<String, Object> toPigMap(Field field,
                                              Map<Object, Object> map,
                                              boolean lazy) {
    // PIG map's key always a String. just use toString() and hope
    // things would work out ok.
    HashMap<String, Object> out = new HashMap<String, Object>(map.size());
    Field valueField = field.getMapValueField();
    for(Entry<Object, Object> e : map.entrySet()) {
      String key = e.getKey() == null ? null : e.getKey().toString();
      Object prev = out.put(key, toPigObject(valueField, e.getValue(), lazy));
      if (prev != null) {
        String msg = "Duplicate keys while converting to String while "
          + " processing map " + field.getName() + " (key type : "
          + field.getMapKeyField().getType() + " value type : "
          + field.getMapValueField().getType() + ")";
        LOG.warn(msg);
        throw new RuntimeException(msg);
      }
    }
    return out;
  }

  private static DataBag toPigBag(Field field,
                                  Collection<Object> values,
                                  boolean lazy) {
    List<Tuple> tuples = Lists.newArrayListWithExpectedSize(values.size());
    for(Object value : values) {
      Object pValue = toPigObject(field, value, lazy);
      if (pValue instanceof Tuple) { // DataBag should contain Tuples
        tuples.add((Tuple)pValue);
      } else {
        tuples.add(tupleFactory.newTuple(pValue));
      }
    }
    return new NonSpillableDataBag(tuples);
  }

  @SuppressWarnings("serial")
  /**
   * Delays serialization of Thrift fields until they are requested.
   */
  private static class LazyTuple extends AbstractLazyTuple {
    /* NOTE : This is only a partial optimization. The other part
     * is to avoid deserialization of the Thrift fields from the
     * binary buffer.
     *
     * Currently TDeserializer allows deserializing just one field,
     * psuedo-skipping over the fields before it.
     * But if we are going deserialize 5 fields out of 20, we will be
     * skipping over same set of fields multiple times. OTOH this might
     * still be better than a full deserialization.
     *
     * We need to write our own version of TBinaryProtocol that truly skips.
     * Even TDeserializer 'skips'/ignores only after deserializing fields.
     * (e.g. Strings, Integers, buffers etc).
     */
    private TBase<?, ?> tObject;
    private TStructDescriptor desc;

    LazyTuple(TStructDescriptor desc, TBase<?, ?> tObject) {
      initRealTuple(desc.getFields().size());
      this.tObject = tObject;
      this.desc = desc;
    }

    @Override
    protected Object getObjectAt(int index) {
      Field field = desc.getFieldAt(index);
      return toPigObject(field, desc.getFieldValue(index, tObject), true);
    }
  }

  /**
   * Returns Pig schema for the Thrift struct.
   */
  public static Schema toSchema(Class<? extends TBase<?, ?>> tClass) {
    return toSchema(TStructDescriptor.getInstance(tClass));
  }

  public Schema toSchema() {
    return toSchema(structDesc);
  }

  public static Schema toSchema(TStructDescriptor tDesc) {
    Schema schema = new Schema();

    try {
      for(Field field : tDesc.getFields()) {
        schema.add(singleFieldToFieldSchema(field.getName(), field));
      }
    } catch (FrontendException t) {
      throw new RuntimeException(t);
    }

    return schema;
  }

  /**
   * return {@link FieldSchema} for a given field.
   */
  private static FieldSchema singleFieldToFieldSchema(String fieldName, Field field) throws FrontendException {
    //TODO we should probably implement better naming, the current system is pretty nonsensical now
    switch (field.getType()) {
      case TType.STRUCT:
        return new FieldSchema(fieldName, toSchema(field.gettStructDescriptor()), DataType.TUPLE);
      case TType.LIST:
        return new FieldSchema(fieldName, singleFieldToTupleSchema(fieldName + "_tuple", field.getListElemField()), DataType.BAG);
      case TType.SET:
        return new FieldSchema(fieldName, singleFieldToTupleSchema(fieldName + "_tuple", field.getSetElemField()), DataType.BAG);
      case TType.MAP:
        if (field.getMapKeyField().getType() != TType.STRING
            && field.getMapKeyField().getType() != TType.ENUM) {
          LOG.warn("Using a map with non-string key for field " + field.getName()
              + ". while converting to PIG Tuple, toString() is used for the key."
              + " It could result in incorrect maps.");
        }
        return new FieldSchema(fieldName, new Schema(singleFieldToFieldSchema(null, field.getMapValueField())), DataType.MAP);
      default:
        return new FieldSchema(fieldName, null, getPigDataType(field));
    }
  }

  /**
   * A helper function which wraps a Schema in a tuple (for Pig bags) if our version of pig makes it necessary
   */
  private static Schema wrapInTupleIfPig9(Schema schema) throws FrontendException {
      if (PigUtil.Pig9orNewer) {
          return new Schema(new FieldSchema("t",schema,DataType.TUPLE));
      } else {
          return schema;
      }
  }

  /**
   * Returns a schema with single tuple (for Pig bags).
   */
  private static Schema singleFieldToTupleSchema(String fieldName, Field field) throws FrontendException {
    switch (field.getType()) {
      case TType.STRUCT:
        return wrapInTupleIfPig9(toSchema(field.gettStructDescriptor()));
      case TType.LIST:
      case TType.SET:
      case TType.MAP:
        return wrapInTupleIfPig9(new Schema(singleFieldToFieldSchema(fieldName, field)));
      default:
        return wrapInTupleIfPig9(new Schema(new FieldSchema(fieldName, null, getPigDataType(field))));
    }
  }

  private static byte getPigDataType(Field field) {
    switch (field.getType()) {
      case TType.BOOL:
      case TType.BYTE:
      case TType.I16:
      case TType.I32:
        return DataType.INTEGER;
      case TType.ENUM:
        if (useEnumId) {
          return DataType.INTEGER;
        } else {
          return DataType.CHARARRAY;
        }
      case TType.I64:
        return DataType.LONG;
      case TType.DOUBLE:
        return DataType.DOUBLE;
      case TType.STRING:
        return field.isBuffer() ? DataType.BYTEARRAY : DataType.CHARARRAY;
      default:
        throw new IllegalArgumentException("Unexpected type where a simple type is expected : " + field.getType());
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
     * but the Pig script is not updated. Commenting it out avoids this.
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
                sb.append(DataType.findTypeName(fs.type) + "[");
                if (fs.schema!=null)
                    stringifySchema(sb, fs.schema, fs.type, prefix);
                sb.append("]");
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

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      Class<? extends TBase<?, ?>> tClass = ThriftUtils.getTypeRef(args[0]).getRawClass();
      System.out.println(args[0] + " : " + toSchema(tClass).toString());
      System.out.println(toPigScript(tClass, ThriftPigLoader.class));
    }
  }
}
