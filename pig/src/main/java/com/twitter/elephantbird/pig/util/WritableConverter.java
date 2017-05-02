package com.twitter.elephantbird.pig.util;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;

import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.store.SequenceFileStorage;

/**
 * Extends LoadStoreCaster to add initialization and schema validation routines useful when dealing
 * with conversion between Pig and Writable types.
 *
 * @author Andy Schlaikjer
 * @see SequenceFileStorage
 * @param <W> the Writable type to and from which conversion is supported.
 */
public interface WritableConverter<W extends Writable> extends LoadStoreCaster {
  /**
   * Called during evaluation of both Pig {@code LOAD} and {@code STORE} expressions by an owning
   * {@link SequenceFileLoader} or {@link SequenceFileStorage} instance. This method is called on
   * the Pig front end as well as back end. Implementations should allow repeated calls to this
   * method without adverse side effects.
   *
   * @param writableClass the Writable class specified by the user via
   *          {@link SequenceFileStorage#SequenceFileStorage(String, String)}, if any. Otherwise,
   *          {@code null}.
   */
  public void initialize(Class<? extends W> writableClass) throws IOException;

  /**
   * Called during evaluation of Pig {@code LOAD} expressions (after {@link #initialize(Class)}),
   * this method returns the expected Pig type of data loaded using this WritableConverter, or
   * {@code null} if it can't be determined before reading data.
   *
   * @return expected schema of loaded data, or {@code null} if it can't be determined before
   *         reading data.
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
   * Called during evaluation of Pig {@code STORE} expressions on the front end during planning
   * (after {@link #initialize(Class)}), this method validates the expression's schema. An
   * IOException is thrown if conversion from the specified Pig type to {@code W} is unsupported.
   *
   * @param schema
   * @throws IOException if conversion from the Pig type specified by {@code schema} is unsupported.
   * @see StoreFunc#checkSchema(org.apache.pig.ResourceSchema)
   */
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException;

  /**
   * Called during evaluation of Pig {@code STORE} expressions (after {@link #initialize(Class)}),
   * this method returns the Writable class of instances returned by {@link #toWritable(Object)}, or
   * {@code null} if the type must be specified by via
   * {@link SequenceFileStorage#SequenceFileStorage(String, String)}.
   *
   * @return implementation class of Writable instances returned from {@link #toWritable(Object)},
   *         or {@code null} if the type must be manually specified by user via
   *         {@link SequenceFileStorage#SequenceFileStorage(String, String)}.
   * @throws IOException
   */
  public Class<W> getWritableClass() throws IOException;

  /**
   * Called during evaluation of Pig {@code STORE} expressions, this method converts Pig value to
   * Writable value. Implementations should try and minimize creation of Writable instances by
   * returning the same instance (updated to reflect new value) from each call.
   *
   * @param value Pig object to convert.
   * @return instance containing data from {@code value}, or {@code null} if {@code value} is
   *         {@code null}.
   * @throws IOException
   */
  public W toWritable(Object value) throws IOException;
}
