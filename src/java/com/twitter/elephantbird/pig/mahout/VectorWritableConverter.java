package com.twitter.elephantbird.pig.mahout;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.util.AbstractWritableConverter;
import com.twitter.elephantbird.pig.util.PigCounterHelper;
import com.twitter.elephantbird.pig.util.PigUtil;

/**
 * Supports conversion between Pig {@link Tuple} and Mahout {@link VectorWritable}. See
 * {@link #VectorWritableConverter(String[])} for recognized options.
 *
 * The Pig Tuple schema used to represent vector data is determined by the specified options. For
 * instance, if options {@code -dense} and {@code -cardinality 2} are specified, the following
 * schema is assumed:
 *
 * <pre>
 * (double, double)
 * </pre>
 *
 * If option {@code -sparse} is specified without option {@code -cardinality}, the following schema
 * is assumed:
 *
 * <pre>
 * (cardinality: int, entries: {entry: (index: int, value: double)})
 * </pre>
 *
 * If options {@code -sparse} and {@code -cardinality} are both specified, the following schema is
 * assumed:
 *
 * <pre>
 * (entries: {entry: (index: int, value: double)})
 * </pre>
 *
 * Otherwise, no schema is assumed, and organization of vector Tuple data is inspected at runtime.
 *
 * <p>
 * Example usage:
 *
 * <pre>
 * %declare SEQFILE_LOADER 'com.twitter.elephantbird.pig.load.SequenceFileLoader';
 * %declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';
 * %declare INT_CONVERTER 'com.twitter.elephantbird.pig.util.IntWritableConverter';
 * %declare VECTOR_CONVERTER 'com.twitter.elephantbird.pig.mahout.VectorWritableConverter';
 *
 * -- store DenseVector
 * pair = LOAD '$data' AS (key: int, val: (v1: double, v2: double));
 * STORE pair INTO '$output' USING $SEQFILE_STORAGE (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER'
 * );
 *
 * -- store DenseVector with floats
 * pair = LOAD '$data' AS (key: int, val: (v1: float, v2: float));
 * STORE pair INTO '$output' USING $SEQFILE_STORAGE (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -floatPrecision'
 * );
 *
 * -- store RandomAccessSparseVector data
 * pair = LOAD '$data' AS (key: int, val: (cardinality: int, entries: {entry: (index: int, value: double)}));
 * STORE pair INTO '$output' USING $SEQFILE_STORAGE (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER'
 * );
 *
 * -- store SequentialAccessSparseVector data with the -sequential flag
 * pair = LOAD '$data' AS (key: int, val: (cardinality: int, entries: {entry: (index: int, value: double)}));
 * STORE pair INTO '$output' USING $SEQFILE_STORAGE (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -sequential'
 * );
 *
 * -- load DenseVector data, specifying schema manually
 * pair = LOAD '$data' USING $SEQFILE_LOADER (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER'
 * ) AS (key: int, val: (f1: double, f2: double, f3: double));
 *
 * -- load DenseVector data with known cardinality; Schema defined by SequenceFileLoader
 * pair = LOAD '$data' USING $SEQFILE_LOADER (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -dense -cardinality 2'
 * );
 *
 * -- load *SparseVector data; Schema defined by SequenceFileLoader
 * pair = LOAD '$data' USING $SEQFILE_LOADER (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -sparse'
 * );
 *
 * -- load *SparseVector data with known cardinality; Schema defined by SequenceFileLoader
 * pair = LOAD '$data' USING $SEQFILE_LOADER (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -sparse -cardinality 2'
 * );
 * </pre>
 *
 * Note that conversion of vector data is also supported in cases where it may be desirable to
 * process data in sparse Pig vector format, but input Mahout vector data is actually dense:
 *
 * <pre>
 * pair = LOAD '$dense_vectors' USING $SEQFILE_LOADER (
 *   '-c $INT_CONVERTER', '-c $VECTOR_CONVERTER -- -sparse'
 * );
 * </pre>
 *
 * @author Andy Schlaikjer
 */
public class VectorWritableConverter extends AbstractWritableConverter<VectorWritable> {
  /**
   * Hadoop job counters used by {@link VectorWritableConverter}.
   *
   * @author Andy Schlaikjer
   */
  public static enum Counter {
    /**
     * The number of input sparse vector entry indices found to be out-of-bounds during Tuple to
     * Vector conversion.
     */
    INDEX_OUT_OF_BOUNDS;
  }

  private static final String CARDINALITY_PARAM = "cardinality";
  private static final String DENSE_PARAM = "dense";
  private static final String SPARSE_PARAM = "sparse";
  private static final String SEQUENTIAL_PARAM = "sequential";
  private static final String FLOAT_PRECISION_PARAM = "floatPrecision";
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private final BagFactory bagFactory = BagFactory.getInstance();
  private final PigCounterHelper counterHelper = new PigCounterHelper();
  private final boolean dense;
  private final boolean sparse;
  private final Integer cardinality;
  private final boolean sequential;
  private final boolean floatPrecision;

  /**
   * Default options used.
   *
   * @throws ParseException
   */
  public VectorWritableConverter() throws ParseException {
    this(new String[] {});
  }

  /**
   * The following options are recognized:
   *
   * <dl>
   * <dt>{@code -cardinality n}</dt>
   * <dd>Vectors are expected to have cardinality {@code n}.</dd>
   * <dt>{@code -dense}</dt>
   * <dd>Vectors are expected to be dense. This option and options {@code -sparse},
   * {@code -sequential} are mutually exclusive.</dd>
   * <dt>{@code -sparse}</dt>
   * <dd>Vectors are expected to be sparse. This option and option {@code -dense} are mutually
   * exclusive.</dd>
   * <dt>{@code -sequential}</dt>
   * <dd>Sparse vector data should be stored using {@link SequentialAccessSparseVector}. This option
   * and option {@code -dense} are mutually exclusive.</dd>
   * <dt>{@code -floatPrecision}</dt>
   * <dd>Vector data should be loaded/stored using float precision.</dd>
   * </dl>
   *
   * @param args options passed in from {@link SequenceFileLoader}.
   * @throws ParseException
   */
  public VectorWritableConverter(String[] args) throws ParseException {
    super(new VectorWritable());
    Preconditions.checkNotNull(args);
    CommandLine cmdline = parseArguments(args);
    cardinality =
        cmdline.hasOption(CARDINALITY_PARAM) ? new Integer(
            cmdline.getOptionValue(CARDINALITY_PARAM)) : null;
    dense = cmdline.hasOption(DENSE_PARAM);
    sequential = cmdline.hasOption(SEQUENTIAL_PARAM);
    Preconditions.checkState(!(dense && sequential),
        "Options '-dense' and '-sequential' are mutually exclusive");
    sparse = cmdline.hasOption(SPARSE_PARAM) || sequential;
    Preconditions.checkState(!(dense && sparse),
        "Options '-dense' and '-sparse' are mutually exclusive");
    floatPrecision = cmdline.hasOption(FLOAT_PRECISION_PARAM);
    writable.setWritesLaxPrecision(floatPrecision);
  }

  private CommandLine parseArguments(String[] args) throws ParseException {
    return new GnuParser().parse(getOptions(), args);
  }

  /**
   * @return Options to parse from {@code String[] args} on construction.
   */
  @SuppressWarnings("static-access")
  protected Options getOptions() {
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt(CARDINALITY_PARAM).hasArg().withArgName("n")
        .withDescription("Expected cardinality of vector data.").create());
    options
        .addOption(OptionBuilder
            .withLongOpt(DENSE_PARAM)
            .withDescription(
                "If specified along with cardinality, reported LOAD schema will be dense.")
            .create());
    options.addOption(OptionBuilder.withLongOpt(SPARSE_PARAM)
        .withDescription("If specified, reported LOAD schema will be sparse.").create());
    options.addOption(OptionBuilder
        .withLongOpt(SEQUENTIAL_PARAM)
        .withDescription(
            "If specified, Pig vector data will be converted to"
                + " SequentialAccessSparseVector data on STORE."
                + " Otherwise, RandomAccessSparseVector is used.").create());
    options.addOption(OptionBuilder.withLongOpt(FLOAT_PRECISION_PARAM)
        .withDescription("If specified, float precision will be used when writing output data.")
        .create());
    return options;
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    Byte valueType = floatPrecision ? DataType.FLOAT : DataType.DOUBLE;
    if (sparse) {
      FieldSchema entriesFieldSchema;
      if (PigUtil.Pig9orNewer) {
        Schema tupleSchema = new Schema(Lists.newArrayList(new FieldSchema("index",
            DataType.INTEGER), new FieldSchema("value", valueType)));
        Schema bagSchema = new Schema(Lists.newArrayList(new FieldSchema("t", tupleSchema,
            DataType.TUPLE)));
        entriesFieldSchema = new FieldSchema("entries", bagSchema, DataType.BAG);
      } else {
        entriesFieldSchema =
            new FieldSchema("entries", new Schema(Lists.newArrayList(new FieldSchema("index",
                DataType.INTEGER), new FieldSchema("value", valueType))), DataType.BAG);
      }
      if (cardinality != null) {
        return new ResourceFieldSchema(new FieldSchema(null, new Schema(
            Lists.newArrayList(entriesFieldSchema))));
      }
      return new ResourceFieldSchema(new FieldSchema(null, new Schema(Lists.newArrayList(
          new FieldSchema("cardinality", DataType.INTEGER), entriesFieldSchema))));
    }
    if (dense && cardinality != null) {
      return new ResourceFieldSchema(new FieldSchema(null, new Schema(Collections.nCopies(
          cardinality, new FieldSchema(null, valueType)))));
    }
    return new ResourceFieldSchema(new FieldSchema(null, DataType.BYTEARRAY));
  }

  @Override
  public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
    return bytesToTuple(dataByteArray.get(), null);
  }

  @Override
  protected Tuple toTuple(VectorWritable writable, ResourceFieldSchema schema) throws IOException {
    Preconditions.checkNotNull(writable, "VectorWritable is null");
    Vector v = writable.get();
    Preconditions.checkNotNull(v, "Vector is null");

    // check cardinality
    int size = v.size();
    if (cardinality != null) {
      Preconditions.checkState(cardinality == size,
          "Expecting cardinality %s but found cardinality %s", cardinality, size);
    }

    // create Tuple
    Tuple out = null;
    if (v.isDense()) {
      // dense vector found
      if (sparse) {
        // client requested sparse tuple rep
        out = toSparseVectorTuple(v);
      } else {
        out = toDenseVectorTuple(v);
      }
    } else {
      // sparse vector found
      if (dense) {
        // client requested dense tuple rep
        out = toDenseVectorTuple(v);
      } else {
        out = toSparseVectorTuple(v);
      }
    }
    return out;
  }

  protected Tuple toDenseVectorTuple(Vector v) {
    List<Number> values = Lists.newArrayListWithCapacity(v.size());
    for (Element e : v) {
      values.add(floatPrecision ? (float) e.get() : e.get());
    }
    return tupleFactory.newTupleNoCopy(values);
  }

  protected Tuple toSparseVectorTuple(Vector v) {
    DataBag bag = bagFactory.newDefaultBag();
    Iterator<Element> itr = v.iterateNonZero();
    while (itr.hasNext()) {
      Element e = itr.next();
      bag.add(tupleFactory.newTupleNoCopy(Lists.<Number> newArrayList(e.index(),
          floatPrecision ? (float) e.get() : e.get())));
    }
    return cardinality != null ? tupleFactory.newTupleNoCopy(ImmutableList.of(bag)) : tupleFactory
        .newTupleNoCopy(ImmutableList.of(v.size(),
            bag));
  }

  @Override
  public void checkStoreSchema(ResourceFieldSchema schema) throws IOException {
    assertFieldTypeEquals(DataType.TUPLE, schema.getType(), "tuple");
    ResourceSchema vectorSchema = schema.getSchema();
    assertNotNull(vectorSchema, "ResourceSchema for tuple is null");
    ResourceFieldSchema[] vectorFieldSchemas = vectorSchema.getFields();
    assertNotNull(vectorFieldSchemas, "Tuple field schemas are null");
    if (vectorFieldSchemas.length == 1 && vectorFieldSchemas[0].getType() == DataType.BAG) {

      // has to be sparse format
      Preconditions.checkNotNull(cardinality, "Cardinality undefined");
      checkSparseVectorEntriesSchema(vectorFieldSchemas[0].getSchema());

    } else if (vectorFieldSchemas.length == 2 && vectorFieldSchemas[1].getType() == DataType.BAG) {

      // has to be sparse format
      Preconditions.checkState(cardinality == null, "Cardinality already defined");
      // check tuple[0] == cardinality:int
      assertFieldTypeEquals(DataType.INTEGER, vectorFieldSchemas[0].getType(), "tuple[0]");
      checkSparseVectorEntriesSchema(vectorFieldSchemas[1].getSchema());

    } else {

      // has to be dense format
      for (int i = 0; i < vectorFieldSchemas.length; ++i) {
        assertFieldTypeIsNumeric(vectorFieldSchemas[i].getType(), "tuple[" + i + "]");
      }

    }
  }

  private void checkSparseVectorEntriesSchema(ResourceSchema entriesSchema) throws IOException {
    // check entries.length == 1
    assertNotNull(entriesSchema, "ResourceSchema of entries is null");
    ResourceFieldSchema[] entriesFieldSchemas = entriesSchema.getFields();
    assertNotNull(entriesFieldSchemas, "Tuple field schemas are null");
    assertTupleLength(1, entriesFieldSchemas.length, "entries");

    // check entries[0] == entry:tuple
    assertFieldTypeEquals(DataType.TUPLE, entriesFieldSchemas[0].getType(), "entries[0]");

    // check entries[0].length == 2
    ResourceSchema entriesTupleSchema = entriesFieldSchemas[0].getSchema();
    assertNotNull(entriesTupleSchema, "ResourceSchema of entries[0] is null");
    ResourceFieldSchema[] entriesTupleFieldSchemas = entriesTupleSchema.getFields();
    assertNotNull(entriesTupleFieldSchemas, "Tuple field schemas are null");
    assertTupleLength(2, entriesTupleFieldSchemas.length, "entries[0]");

    // check entries[0][0] == index:int
    assertFieldTypeEquals(DataType.INTEGER, entriesTupleFieldSchemas[0].getType(), "entries[0][0]");

    // check entries[0][1] == value:double
    assertFieldTypeIsNumeric(entriesTupleFieldSchemas[1].getType(), "entries[0][1]");
  }

  @Override
  protected VectorWritable toWritable(Tuple value) throws IOException {
    Preconditions.checkNotNull(value, "Tuple is null");
    Vector v = null;
    if (isSparseVectorData(value)) {
      v = convertSparseVectorDataToVector(value);
    } else {
      validateDenseVectorData(value);
      v = convertDenseVectorDataToVector(value);
    }
    writable.set(v);
    return writable;
  }

  private static boolean isSparseVectorData(Tuple value) throws IOException {
    assertNotNull(value, "Tuple is null");
    if ((1 == value.size() && DataType.BAG == value.getType(0))
        || (2 == value.size() && DataType.INTEGER == value.getType(0) && DataType.BAG == value
            .getType(1)))
      return true;
    return false;
  }

  private Vector convertSparseVectorDataToVector(Tuple value) throws IOException {
    Vector v;

    // determine output vector size and fetch bag containing entries from input
    int size = 0;
    DataBag entries = null;
    if (value.size() == 2) {
      // cardinality defined by input
      size = (Integer) value.get(0);
      if (cardinality != null) {
        // cardinality defined by VectorWritableConverter instance
        size = cardinality;
      }
      entries = (DataBag) value.get(1);
    } else {
      Preconditions.checkNotNull(cardinality, "Cardinality is undefined");
      size = cardinality;
      entries = (DataBag) value.get(0);
    }

    // create vector, allowing conversion of sparse input vector data to dense output vector
    if (dense) {
      // TODO(Andy Schlaikjer): Test for OOM before it happens
      v = new DenseVector(size);
    } else {
      // more efficient to build sparse vector with this impl
      v = new RandomAccessSparseVector(size);
    }

    // populate vector
    for (Tuple entry : entries) {
      validateSparseVectorEntryData(entry);
      int i = (Integer) entry.get(0);
      // check index bounds
      if (i < 0 || i >= size) {
        counterHelper.incrCounter(Counter.INDEX_OUT_OF_BOUNDS, 1);
        continue;
      }
      double n = ((Number) entry.get(1)).doubleValue();
      v.setQuick(i, n);
    }

    // convert to (sparse) sequential vector if requested
    if (sequential) {
      v = new SequentialAccessSparseVector(v);
    }

    return v;
  }

  private static void validateSparseVectorEntryData(Tuple value) throws IOException {
    assertNotNull(value, "Tuple is null");
    assertTupleLength(2, value.size(), "tuple");
    assertFieldTypeEquals(DataType.INTEGER, value.getType(0), "tuple[0]");
    assertFieldTypeIsNumeric(value.getType(1), "tuple[1]");
  }

  private static void validateDenseVectorData(Tuple value) throws IOException {
    assertNotNull(value, "Tuple is null");
    for (int i = 0; i < value.size(); ++i) {
      assertFieldTypeIsNumeric(value.getType(i), "tuple[" + i + "]");
    }
  }

  private Vector convertDenseVectorDataToVector(Tuple value) throws IOException {
    Vector v;

    // determine output vector size
    int size = value.size();
    int minSize = size;
    if (cardinality != null && cardinality != size) {
      // cardinality specified on construction overrides instance cardinality
      size = cardinality;
      if (minSize > size) {
        minSize = size;
      }
    }

    // allow conversion of dense vector data to sparse vector
    if (sparse) {
      // this ctor used to pre-alloc space for entries
      v = new RandomAccessSparseVector(size, size);
      for (int i = 0; i < minSize; ++i) {
        v.setQuick(i, ((Number) value.get(i)).doubleValue());
      }
    } else {
      double[] values = new double[size];
      for (int i = 0; i < minSize; ++i) {
        values[i] = ((Number) value.get(i)).doubleValue();
      }
      // this ctor uses values directly, no copying performed
      v = new DenseVector(values, true);
    }

    return v;
  }

  private static void assertNotNull(Object value, String msg, Object... values) throws IOException {
    if (value == null) {
      throw new IOException(String.format(msg, values));
    }
  }

  private static void assertFieldTypeEquals(byte expected, byte observed, String fieldName)
      throws IOException {
    if (expected != observed) {
      throw new IOException(String.format("Expected %s of type '%s' but found type '%s'",
          fieldName, DataType.findTypeName(expected), DataType.findTypeName(observed)));
    }
  }

  private static void assertFieldTypeIsNumeric(byte observed, String fieldName) throws IOException {
    switch (observed) {
      case DataType.INTEGER:
      case DataType.LONG:
      case DataType.FLOAT:
      case DataType.DOUBLE:
        break;
      default:
        throw new IOException(String.format("Expected %s of numeric type but found type '%s'",
            fieldName, DataType.findTypeName(observed)));
    }
  }

  private static void assertTupleLength(int expected, int observed, String fieldName)
      throws IOException {
    if (expected != observed) {
      throw new IOException(String.format("Expected %s of length %s but found length %s",
          fieldName, expected, observed));
    }
  }
}
