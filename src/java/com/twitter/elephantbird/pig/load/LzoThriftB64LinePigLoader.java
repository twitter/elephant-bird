package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TBase;

import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.ThriftConverter;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.util.ThriftUtils;
import com.twitter.elephantbird.util.TypeRef;

/**
 * Loader for LZO files with line-oriented base64 encoded Thrift objects.
 */
public class LzoThriftB64LinePigLoader<M extends TBase<?, ?>> extends LzoBinaryB64LinePigLoader {

  private final TypeRef<M> typeRef_;
  private ThriftConverter<M> converter_;
  private final ThriftToPig<M> thriftToPig_;

  public LzoThriftB64LinePigLoader(String thriftClassName) {
    typeRef_ = ThriftUtils.getTypeRef(thriftClassName);
    converter_ = ThriftConverter.newInstance(typeRef_);
    thriftToPig_ =  ThriftToPig.newInstance(typeRef_);

    BinaryConverter<Tuple> tupleConverter = new BinaryConverter<Tuple>() {
      public Tuple fromBytes(byte[] messageBuffer) {
        M value = converter_.fromBytes(messageBuffer);
        if (value != null) {
          return thriftToPig_.getLazyTuple(value);
        }
        return null;
      }

      public byte[] toBytes(Tuple message) {
        throw new RuntimeException("not implemented");
      }
    };

    init(thriftClassName, tupleConverter);
    setLoaderSpec(getClass(), new String[]{thriftClassName});
  }

  @Override
  public Schema determineSchema(String filename, ExecType execType, DataStorage store) throws IOException {
    return ThriftToPig.toSchema(typeRef_.getRawClass());
  }
}
