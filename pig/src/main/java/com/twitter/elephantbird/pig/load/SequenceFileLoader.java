package com.twitter.elephantbird.pig.load;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.mapreduce.input.RawSequenceFileInputFormat;
import com.twitter.elephantbird.pig.util.SequenceFileConfig;
import com.twitter.elephantbird.pig.util.WritableConverter;

/**
 * Pig LoadFunc supporting conversion from key and value {@link Writable}s stored within
 * {@link SequenceFile}s to Pig tuples. Example usage:
 *
 * <pre>
 * pairs = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 *   '-c com.twitter.elephantbird.pig.util.IntWritableConverter',
 *   '-c com.twitter.elephantbird.pig.util.TextConverter'
 * ) as (
 *   key: int,
 *   value: chararray
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 * @see SequenceFileConfig
 * @see WritableConverter
 * @see SequenceFileStorage
 */
public class SequenceFileLoader<K extends Writable, V extends Writable> extends LzoBaseLoadFunc
    implements LoadPushDown, LoadMetadata {
  /**
   * Error conditions.
   */
  public static enum Error {
    /**
     * {@link EOFException}s encountered while reading input.
     */
    EOFException
  };

  /**
   * Refinement of {@link SequenceFileConfig} which adds generic "skipEOFErrors" option.
   */
  private class Config extends SequenceFileConfig<K, V> {
    public static final String SKIP_EOF_ERRORS_PARAM = "skipEOFErrors";

    public Config(String keyArgs, String valueArgs, String genericArgs) throws ParseException,
        IOException {
      super(keyArgs, valueArgs, genericArgs);
    }

    @Override
    protected Options getGenericOptions() {
      @SuppressWarnings("static-access")
      Option skipEOFOption =
          OptionBuilder
              .withLongOpt(SKIP_EOF_ERRORS_PARAM)
              .withDescription(
                  "Skip EOFExceptions if they occur while reading data." +
                      " Useful for reading sequence files while they are being created."
              ).create();
      return super.getGenericOptions().addOption(skipEOFOption);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileLoader.class);
  private final Config config;
  private final DataByteArray keyDataByteArray = new DataByteArray();
  private final DataByteArray valueDataByteArray = new DataByteArray();
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private boolean readKey = true;
  private boolean readValue = true;

  /**
   * Parses key, value and generic options from argument strings. See {@link SequenceFileConfig} for
   * details. One extra generic option is supported:
   *
   * <dl>
   * <dt>--skipEOFErrors</dt>
   * <dd>Skip {@link EOFExceptions} if they occur while reading data. Useful for reading sequence
   * files while they are being created.</dd>
   * </dl>
   *
   * @param keyArgs argument string containing key options.
   * @param valueArgs argument string containing value options.
   * @param otherArgs argument string containing other options.
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileLoader(String keyArgs, String valueArgs, String otherArgs)
      throws ParseException, IOException {
    super();
    this.config = new Config(keyArgs, valueArgs, otherArgs);
  }

  /**
   * Constructor without generic arguments (backwards compatible).
   *
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileLoader(String keyArgs, String valueArgs) throws ParseException, IOException {
    this(keyArgs, valueArgs, "");
  }

  /**
   * Default constructor. Defaults used for all options.
   *
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileLoader() throws ParseException, IOException {
    this("", "");
  }

  @Override
  public InputFormat<DataInputBuffer, DataInputBuffer> getInputFormat() throws IOException {
    return new RawSequenceFileInputFormat();
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    /*
     * We have two LoadCasters--one for the key type, another for the value type. Unfortunately,
     * LoadCaster doesn't allow clients to specify which field it's casting (nor schema of field),
     * so we're out of luck here. No casting supported.
     */
    return null;
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
      throws FrontendException {
    return pushProjectionHelper(requiredFieldList);
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    // determine key field schema
    ResourceFieldSchema keySchema = config.keyConverter.getLoadSchema();
    if (keySchema == null) {
      return null;
    }
    keySchema.setName("key");

    // determine value field schema
    ResourceFieldSchema valueSchema = config.valueConverter.getLoadSchema();
    if (valueSchema == null) {
      return null;
    }
    valueSchema.setName("value");

    // return tuple schema
    ResourceSchema resourceSchema = new ResourceSchema();
    resourceSchema.setFields(new ResourceFieldSchema[] { keySchema, valueSchema });
    return resourceSchema;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    super.setLocation(location, job);
    if (requiredFieldList != null) {
      readKey = readValue = false;
      for (RequiredField field : requiredFieldList.getFields()) {
        int i = field.getIndex();
        switch (i) {
          case 0:
            readKey = true;
            break;
          case 1:
            readValue = true;
            break;
          default:
            // TODO fix Pig's silent ignorance of FrontendExceptions thrown from here
            throw new FrontendException("Expected field index in [0, 1] but found index " + i);
        }
      }
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
      if (readKey) {
        if (readValue) {
          return tupleFactory.newTupleNoCopy(Lists.newArrayList(getCurrentKeyObject(),
              getCurrentValueObject()));
        } else {
          return tupleFactory.newTupleNoCopy(Lists.newArrayList(getCurrentKeyObject()));
        }
      } else if (readValue) {
        return tupleFactory.newTupleNoCopy(Lists.newArrayList(getCurrentValueObject()));
      } else {
        throw new IllegalStateException("Cowardly refusing to read zero fields per record");
      }
    } catch (EOFException e) {
      if (!config.genericArguments.hasOption(Config.SKIP_EOF_ERRORS_PARAM)) {
        throw e;
      }
      LOG.warn("EOFException encountered while reading input", e);
      incrCounter(Error.EOFException, 1L);
    } catch (InterruptedException e) {
      throw new ExecException("Error while reading input", 6018, PigException.REMOTE_ENVIRONMENT, e);
    }

    return null;
  }

  private Object getCurrentKeyObject() throws IOException, InterruptedException {
    DataInputBuffer ibuf = (DataInputBuffer) reader.getCurrentKey();
    keyDataByteArray.set(Arrays.copyOf(ibuf.getData(), ibuf.getLength()));
    return config.keyConverter.bytesToObject(keyDataByteArray);
  }

  private Object getCurrentValueObject() throws IOException, InterruptedException {
    DataInputBuffer ibuf = (DataInputBuffer) reader.getCurrentValue();
    valueDataByteArray.set(Arrays.copyOf(ibuf.getData(), ibuf.getLength()));
    return config.valueConverter.bytesToObject(valueDataByteArray);
  }
}
