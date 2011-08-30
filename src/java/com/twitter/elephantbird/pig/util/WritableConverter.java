package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;

import com.twitter.elephantbird.pig.store.SequenceFileStorage;

/**
 * Extends LoadStoreCaster to add initialization and schema validation routines useful when dealing
 * with conversion between Pig and Writable types.
 *
 * @author Andy Schlaikjer
 * @see SequenceFileStorage
 * @param <W> the Writable type to and from which conversion/casting is supported.
 */
public interface WritableConverter<W extends Writable> extends LoadStoreCaster {
  /**
   * Called before Pig {@code LOAD} or {@code STORE} expression evaluation by owning
   * {@link SequenceFileStorage} instance.
   *
   * @param writableClass implementation Class.
   */
  public void initialize(Class<? extends W> writableClass);

  /**
   * Called during evaluation of Pig {@code LOAD} expressions, this method should return the
   * expected Pig type of data loaded using this WritableConverter, or {@code null} if it can't be
   * determined before reading data.
   *
   * @return
   * @throws IOException
   */
  public ResourceFieldSchema getLoadSchema() throws IOException;

  /**
   * Called during evaluation of Pig {@code LOAD} expressions, this method decodes Writable instance
   * data from raw bytes and then converts the Writable to an appropriate Pig value. Implementations
   * should override this method and delegate to an appropriate {@code LoadCaster#bytesTo*()}
   * method.
   *
   * @param dataByteArray raw bytes which encode an instance of type {@code W}.
   * @return Pig value.
   * @throws IOException
   * @see LoadCaster#bytesToCharArray(byte[])
   */
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException;

  /**
   * Called during evaluation of Pig {@code STORE} expressions, this method validates the
   * expression's schema. Implementations should throw an exception if conversion from the specified
   * Pig type to {@code W} is unsupported.
   *
   * @param schema
   * @throws IOException if conversion from the Pig type specified by {@code schema} is unsupported.
   */
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException;

  /**
   * Called during evaluation of Pig {@code STORE} expressions, this method converts Pig value to
   * Writable value. Implementations should try and minimize creation of W instances by returning
   * the same instance (updated to reflect value) from each call.
   *
   * @param value Pig object to convert.
   * @return {@code writable} or a new W instance containing data from {@code value}.
   * @throws IOException
   */
  public W toWritable(Object value) throws IOException;
}
